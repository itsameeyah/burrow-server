//! Raw Minecraft TCP ingress & hostname multiplexer (e.g., port 25565).
//!
//! Peeks the initial Minecraft Handshake packet and routes by hostname:
//! 1. Explicit per-hostname routes (`BURROW_TCP_ROUTES=smp.meeyah.dev=192.168.0.50:25565,...`)
//!    are transparently proxied to the configured upstream Minecraft server.
//! 2. Burrow join codes (`<CODE>.<BURROW_TCP_DOMAIN>`, e.g. `ABC123.burrow.meeyah.dev`):
//!    - On Status (`next_state == 1`): replies with a live vanilla MOTD/status JSON + Ping/Pong.
//!    - On Login (`next_state == 2` or Transfer `3`): opens a host-side relay room and splices
//!      raw TCP bytes directly into the host's WebSocket relay (forwarding the exact initial
//!      handshake untouched so Mojang auth, encryption, compression, and permissions run end-to-end).
//! 3. Catch-all fallback (`BURROW_TCP_FALLBACK=192.168.0.50:25565`): any non-Burrow hostname is
//!    transparently proxied so regular Minecraft servers and Burrow cleanly share port 25565.

use crate::cloudflare_srv::normalize_domain;
use crate::ws::{self, App};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HANDSHAKE_BYTES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TcpRoute {
    /// Lowercase hostname pattern — either exact (`smp.meeyah.dev`) or wildcard (`*.smp.meeyah.dev`).
    pub host_pattern: String,
    /// Upstream `host:port` target (e.g. `192.168.0.50:25565`).
    pub target: String,
}

impl TcpRoute {
    fn matches(&self, host: &str) -> bool {
        if let Some(suffix) = self.host_pattern.strip_prefix("*.") {
            // Matches `<anything>.<suffix>` (at least one label before suffix).
            host.len() > suffix.len() + 1
                && host.ends_with(suffix)
                && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
        } else {
            host == self.host_pattern
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TcpIngressConfig {
    /// Base domain for Burrow join codes (e.g. `burrow.meeyah.dev`).
    pub domain: Option<String>,
    /// Explicit `hostname=target` routes evaluated before Burrow join-code matching.
    pub routes: Vec<TcpRoute>,
    /// Optional catch-all `host:port` for any hostname that isn't a Burrow join code.
    pub fallback: Option<String>,
}

impl TcpIngressConfig {
    pub fn from_env() -> Self {
        let domain = std::env::var("BURROW_TCP_DOMAIN")
            .ok()
            .map(|s| normalize_domain(&s))
            .filter(|s| !s.is_empty());

        let routes = std::env::var("BURROW_TCP_ROUTES")
            .ok()
            .map(|s| parse_routes(&s))
            .unwrap_or_default();

        let fallback = std::env::var("BURROW_TCP_FALLBACK")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Self {
            domain,
            routes,
            fallback,
        }
    }
}

pub fn parse_routes(raw: &str) -> Vec<TcpRoute> {
    let mut out = Vec::new();
    for entry in raw.split(|c| c == ',' || c == ';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((lhs, rhs)) = entry.split_once('=') else {
            tracing::warn!("Ignoring malformed BURROW_TCP_ROUTES entry '{}'", entry);
            continue;
        };
        let host_pattern = lhs.trim().trim_end_matches('.').to_ascii_lowercase();
        let target = rhs.trim().to_string();
        if !host_pattern.is_empty() && !target.is_empty() {
            out.push(TcpRoute {
                host_pattern,
                target,
            });
        }
    }
    out
}

#[derive(Debug, PartialEq, Eq)]
enum RouteDecision {
    BurrowCode(String),
    Proxy(String),
    NotFound,
}

pub async fn serve(app: Arc<App>, bind: String, config: TcpIngressConfig) {
    let listener = match TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind Burrow TCP ingress on {}: {}", bind, e);
            return;
        }
    };
    tracing::info!(
        "Burrow vanilla TCP ingress listening on {} (domain={:?}, routes={}, fallback={:?})",
        bind,
        config.domain,
        config.routes.len(),
        config.fallback
    );

    let config = Arc::new(config);
    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            continue;
        };
        let app = app.clone();
        let config = config.clone();
        tokio::spawn(async move {
            handle_client(app, config, stream).await;
        });
    }
}

struct Handshake {
    protocol_version: i32,
    server_address: String,
    next_state: i32,
    packet_total_len: usize,
}

async fn handle_client(app: Arc<App>, config: Arc<TcpIngressConfig>, mut stream: TcpStream) {
    let _ = stream.set_nodelay(true);

    let mut buf = Vec::with_capacity(256);
    let mut tmp = [0u8; 512];
    let handshake = loop {
        if let Some(hs) = try_parse_handshake(&buf) {
            break hs;
        }
        if buf.len() >= MAX_HANDSHAKE_BYTES {
            return;
        }
        match tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.read(&mut tmp)).await {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => return,
            Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
        }
    };

    let code = match resolve_route(&config, &handshake.server_address) {
        RouteDecision::Proxy(target) => {
            proxy_to_backend(stream, &buf, &target, &handshake).await;
            return;
        }
        RouteDecision::BurrowCode(code) => code,
        RouteDecision::NotFound => {
            let domain_hint = config.domain.as_deref().unwrap_or("burrow.meeyah.dev");
            if handshake.next_state == 1 {
                let _ = send_status_and_ping(
                    &mut stream,
                    &buf[handshake.packet_total_len..],
                    handshake.protocol_version,
                    "Burrow",
                    &format!("Use <CODE>.{} to join a hosted world", domain_hint),
                    0,
                    0,
                )
                .await;
            } else {
                let _ = send_login_disconnect(
                    &mut stream,
                    &format!("Enter a Burrow join address like <CODE>.{}", domain_hint),
                )
                .await;
            }
            return;
        }
    };

    let matched = {
        let sessions = app.sessions.lock().await;
        sessions
            .iter()
            .find(|(_, s)| s.join_code.as_deref() == Some(code.as_str()))
            .map(|(h, s)| (*h, s.clone()))
    };

    let Some((host_uuid, sess)) = matched else {
        if handshake.next_state == 1 {
            let _ = send_status_and_ping(
                &mut stream,
                &buf[handshake.packet_total_len..],
                handshake.protocol_version,
                "Burrow",
                "World not found or join code disabled",
                0,
                0,
            )
            .await;
        } else {
            let _ = send_login_disconnect(&mut stream, "Invalid or expired Burrow join code").await;
        }
        return;
    };

    let host_name = ws::resolve_user_name(&app, &host_uuid).await;

    if handshake.next_state == 1 {
        let version_label = if sess.game_version.is_empty() {
            "Burrow".to_string()
        } else {
            sess.game_version.clone()
        };
        let motd = if sess.locked {
            format!("{}'s World — {} (Locked)", host_name, sess.world_name)
        } else {
            format!("{}'s World — {}", host_name, sess.world_name)
        };
        let _ = send_status_and_ping(
            &mut stream,
            &buf[handshake.packet_total_len..],
            handshake.protocol_version,
            &version_label,
            &motd,
            sess.player_count,
            sess.max_players,
        )
        .await;
        return;
    }

    if sess.locked {
        let _ = send_login_disconnect(&mut stream, "This Burrow world is currently locked").await;
        return;
    }
    if sess.player_count >= sess.max_players {
        let _ = send_login_disconnect(&mut stream, "This Burrow world is full").await;
        return;
    }

    let Some(host_tx) = app.hub.tx(&host_uuid).await else {
        let _ = send_login_disconnect(&mut stream, "That host is currently offline").await;
        return;
    };

    let Some((room_id, host_token)) = app.relay.create_tcp_room(&app, host_uuid, stream, buf) else {
        return;
    };

    {
        let conn = app.db.lock().unwrap();
        crate::db::log_event(&conn, "join_begin", None);
    }

    let msg = json!({
        "op": "relay.host",
        "roomId": room_id.to_string(),
        "token": host_token,
        "relayPath": "/relay/ws",
        "joinerUuid": "",
        "joinerName": "Vanilla Player",
        "joinSource": "code",
        "viaName": "",
    })
    .to_string();
    let _ = host_tx.send(msg);
}

async fn proxy_to_backend(
    mut client: TcpStream,
    initial_bytes: &[u8],
    target: &str,
    handshake: &Handshake,
) {
    let upstream = match tokio::time::timeout(UPSTREAM_CONNECT_TIMEOUT, TcpStream::connect(target))
        .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::debug!("TCP proxy upstream {} unreachable: {}", target, e);
            reply_upstream_offline(&mut client, initial_bytes, handshake).await;
            return;
        }
        Err(_) => {
            tracing::debug!("TCP proxy upstream {} timed out", target);
            reply_upstream_offline(&mut client, initial_bytes, handshake).await;
            return;
        }
    };
    let mut upstream = upstream;
    let _ = upstream.set_nodelay(true);
    if upstream.write_all(initial_bytes).await.is_err() {
        return;
    }
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
}

async fn reply_upstream_offline(
    client: &mut TcpStream,
    initial_bytes: &[u8],
    handshake: &Handshake,
) {
    if handshake.next_state == 1 {
        let _ = send_status_and_ping(
            client,
            &initial_bytes[handshake.packet_total_len..],
            handshake.protocol_version,
            "Offline",
            "Server is currently offline",
            0,
            0,
        )
        .await;
    } else {
        let _ = send_login_disconnect(client, "Upstream Minecraft server is currently offline").await;
    }
}

fn clean_handshake_host(raw: &str) -> String {
    // Strip Forge/FML null-delimited suffixes (e.g. `host\0FML3\0`), whitespace, and trailing dot.
    raw.split('\0')
        .next()
        .unwrap_or("")
        .trim()
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn resolve_route(config: &TcpIngressConfig, raw_address: &str) -> RouteDecision {
    let host = clean_handshake_host(raw_address);

    // 1. Explicit per-hostname routes always win.
    for route in &config.routes {
        if route.matches(&host) {
            return RouteDecision::Proxy(route.target.clone());
        }
    }

    // 2. Burrow join-code match (`<6-char-CODE>.<domain>`).
    if let Some(code) = extract_code_for_config(config, &host) {
        return RouteDecision::BurrowCode(code);
    }

    // 3. Catch-all fallback to another Minecraft server (e.g. SMP on another machine/port).
    if let Some(ref fallback) = config.fallback {
        return RouteDecision::Proxy(fallback.clone());
    }

    RouteDecision::NotFound
}

fn extract_code_for_config(config: &TcpIngressConfig, host: &str) -> Option<String> {
    if let Some(ref domain) = config.domain {
        // Require exact `<CODE>.<domain>` (single label before `.<domain>`).
        let prefix = host.strip_suffix(domain)?.strip_suffix('.')?;
        if prefix.contains('.') {
            return None;
        }
        return ws::normalize_join_code(prefix);
    }

    // No explicit BURROW_TCP_DOMAIN configured:
    // If a fallback proxy is set, require `<CODE>.burrow.<tld...>` so unrelated
    // 6-letter subdomains (e.g. `gaming.meeyah.dev`) still fall through to fallback.
    let mut parts = host.split('.');
    let first = parts.next()?;
    if config.fallback.is_some() {
        let second = parts.next()?;
        if second != "burrow" {
            return None;
        }
    }
    ws::normalize_join_code(first)
}

fn read_varint(bytes: &[u8]) -> Option<(i32, usize)> {
    let mut num_read = 0usize;
    let mut result = 0i32;
    loop {
        if num_read >= bytes.len() || num_read >= 5 {
            return None;
        }
        let read = bytes[num_read];
        let val = (read & 0x7F) as i32;
        result |= val << (7 * num_read);
        num_read += 1;
        if (read & 0x80) == 0 {
            return Some((result, num_read));
        }
    }
}

fn write_varint(out: &mut Vec<u8>, mut value: i32) {
    let mut v = value as u32;
    loop {
        if (v & !0x7F) == 0 {
            out.push(v as u8);
            return;
        }
        out.push(((v & 0x7F) | 0x80) as u8);
        v >>= 7;
        let _ = &mut value;
    }
}

fn write_packet(packet_id: i32, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(payload.len() + 5);
    write_varint(&mut body, packet_id);
    body.extend_from_slice(payload);
    let mut framed = Vec::with_capacity(body.len() + 5);
    write_varint(&mut framed, body.len() as i32);
    framed.extend_from_slice(&body);
    framed
}

fn write_mc_string(out: &mut Vec<u8>, s: &str) {
    write_varint(out, s.len() as i32);
    out.extend_from_slice(s.as_bytes());
}

fn try_parse_handshake(buf: &[u8]) -> Option<Handshake> {
    let (pkt_len, len_bytes) = read_varint(buf)?;
    if pkt_len <= 0 || pkt_len as usize > MAX_HANDSHAKE_BYTES {
        return None;
    }
    let total = len_bytes + pkt_len as usize;
    if buf.len() < total {
        return None;
    }
    let body = &buf[len_bytes..total];
    let mut pos = 0usize;

    let (pkt_id, n) = read_varint(&body[pos..])?;
    pos += n;
    if pkt_id != 0 {
        return None;
    }

    let (protocol_version, n) = read_varint(&body[pos..])?;
    pos += n;

    let (str_len, n) = read_varint(&body[pos..])?;
    pos += n;
    if str_len < 0 || pos + str_len as usize + 2 > body.len() {
        return None;
    }
    let server_address =
        String::from_utf8_lossy(&body[pos..pos + str_len as usize]).to_string();
    pos += str_len as usize;

    // Skip 2-byte unsigned port.
    pos += 2;

    let (next_state, _) = read_varint(&body[pos..])?;

    Some(Handshake {
        protocol_version,
        server_address,
        next_state,
        packet_total_len: total,
    })
}

async fn send_login_disconnect(stream: &mut TcpStream, reason: &str) -> std::io::Result<()> {
    let json_text = json!({ "text": reason }).to_string();
    let mut payload = Vec::new();
    write_mc_string(&mut payload, &json_text);
    let pkt = write_packet(0x00, &payload);
    stream.write_all(&pkt).await?;
    stream.flush().await
}

async fn send_status_and_ping(
    stream: &mut TcpStream,
    leftover: &[u8],
    protocol: i32,
    version_name: &str,
    motd: &str,
    online: u32,
    max: u32,
) -> std::io::Result<()> {
    // Wait briefly for the Status Request (0x00) if not already in `leftover`,
    // then reply with Status Response (0x00).
    let mut buf = leftover.to_vec();
    let mut tmp = [0u8; 64];
    if buf.is_empty() {
        if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut tmp)).await {
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    let status_json = json!({
        "version": {
            "name": version_name,
            "protocol": protocol
        },
        "players": {
            "max": max,
            "online": online
        },
        "description": {
            "text": motd
        }
    })
    .to_string();

    let mut payload = Vec::new();
    write_mc_string(&mut payload, &status_json);
    stream.write_all(&write_packet(0x00, &payload)).await?;
    stream.flush().await?;

    // Handle optional Ping packet (0x01 + 8-byte payload) so the client computes ping bars.
    let mut ping_buf = Vec::new();
    if let Some((req_len, req_lb)) = read_varint(&buf) {
        let consumed = req_lb + req_len.max(0) as usize;
        if buf.len() > consumed {
            ping_buf.extend_from_slice(&buf[consumed..]);
        }
    }
    if ping_buf.len() < 10 {
        if let Ok(Ok(n)) = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut tmp)).await {
            ping_buf.extend_from_slice(&tmp[..n]);
        }
    }
    if let Some((p_len, p_lb)) = read_varint(&ping_buf) {
        let end = p_lb + p_len.max(0) as usize;
        if ping_buf.len() >= end && p_len >= 9 {
            let p_body = &ping_buf[p_lb..end];
            if let Some((1, id_len)) = read_varint(p_body) {
                if p_body.len() >= id_len + 8 {
                    let pong = write_packet(0x01, &p_body[id_len..id_len + 8]);
                    let _ = stream.write_all(&pong).await;
                    let _ = stream.flush().await;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_handshake_packet(host: &str, next_state: i32) -> Vec<u8> {
        let mut payload = Vec::new();
        write_varint(&mut payload, 770); // protocol
        write_mc_string(&mut payload, host);
        payload.extend_from_slice(&25565u16.to_be_bytes());
        write_varint(&mut payload, next_state);
        write_packet(0x00, &payload)
    }

    #[test]
    fn parses_vanilla_handshake_and_routes_correctly() {
        let cfg = TcpIngressConfig {
            domain: Some("burrow.meeyah.dev".into()),
            routes: parse_routes("smp.meeyah.dev=192.168.0.50:25565,*.test.meeyah.dev=127.0.0.1:25566"),
            fallback: Some("192.168.0.100:25565".into()),
        };

        let pkt = make_handshake_packet("ab12cd.burrow.meeyah.dev", 2);
        let hs = try_parse_handshake(&pkt).expect("valid handshake");
        assert_eq!(hs.protocol_version, 770);
        assert_eq!(hs.next_state, 2);
        assert_eq!(
            resolve_route(&cfg, &hs.server_address),
            RouteDecision::BurrowCode("AB12CD".into())
        );

        // Explicit route wins (even with trailing dot & Forge FML suffix).
        assert_eq!(
            resolve_route(&cfg, "SMP.meeyah.dev.\0FML3\0"),
            RouteDecision::Proxy("192.168.0.50:25565".into())
        );

        // Wildcard route works.
        assert_eq!(
            resolve_route(&cfg, "alpha.test.meeyah.dev"),
            RouteDecision::Proxy("127.0.0.1:25566".into())
        );

        // 6-letter non-Burrow subdomain must NOT be mistaken for a Burrow code; goes to fallback.
        assert_eq!(
            resolve_route(&cfg, "gaming.meeyah.dev"),
            RouteDecision::Proxy("192.168.0.100:25565".into())
        );
    }

    #[tokio::test]
    async fn proxies_initial_handshake_and_followup_bytes_verbatim() {
        let upstream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap().to_string();

        let echo_task = tokio::spawn(async move {
            let (mut sock, _) = upstream_listener.accept().await.unwrap();
            let mut received = vec![0u8; 64];
            let n = sock.read(&mut received).await.unwrap();
            sock.write_all(&received[..n]).await.unwrap();
        });

        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();

        let upstream_addr_clone = upstream_addr.clone();
        let proxy_task = tokio::spawn(async move {
            let (client, _) = proxy_listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 128];
            let mut client = client;
            let hs = loop {
                if let Some(h) = try_parse_handshake(&buf) {
                    break h;
                }
                let n = client.read(&mut tmp).await.unwrap();
                buf.extend_from_slice(&tmp[..n]);
            };
            proxy_to_backend(client, &buf, &upstream_addr_clone, &hs).await;
        });

        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        let mut wire = make_handshake_packet("smp.meeyah.dev", 1);
        wire.extend_from_slice(b"EXTRA");
        client.write_all(&wire).await.unwrap();

        let mut echoed = vec![0u8; wire.len()];
        client.read_exact(&mut echoed).await.unwrap();
        assert_eq!(echoed, wire);

        drop(client);
        let _ = proxy_task.await;
        let _ = echo_task.await;
    }
}
