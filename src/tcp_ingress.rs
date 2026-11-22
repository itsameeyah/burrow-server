//! Raw Minecraft TCP ingress (e.g., port 25565) for vanilla players without
//! Burrow installed.
//!
//! Players connect to `<CODE>.<domain>` (e.g., `ABC123.burrow.meeyah.dev`).
//! We peek the initial Minecraft Handshake packet, extract the 6-character
//! join code from the leading DNS label, and:
//! - On Status (`next_state == 1`): reply directly with a vanilla MOTD/status
//!   JSON showing the host, world name, and player count (+ Ping/Pong).
//! - On Login (`next_state == 2` or Transfer `3`): open a host-side relay room
//!   (`relay.host`) and splice raw TCP bytes directly into the host's WebSocket
//!   relay, forwarding the exact initial handshake bytes untouched so Mojang
//!   auth, encryption, compression, and permissions run end-to-end.

use crate::ws::{self, App};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HANDSHAKE_BYTES: usize = 4096;

pub async fn serve(app: Arc<App>, bind: String) {
    let listener = match TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("Failed to bind Burrow TCP ingress on {}: {}", bind, e);
            return;
        }
    };
    tracing::info!("Burrow vanilla TCP ingress listening on {}", bind);

    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            continue;
        };
        let app = app.clone();
        tokio::spawn(async move {
            handle_client(app, stream).await;
        });
    }
}

struct Handshake {
    protocol_version: i32,
    server_address: String,
    next_state: i32,
    packet_total_len: usize,
}

async fn handle_client(app: Arc<App>, mut stream: TcpStream) {
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

    let Some(code) = extract_code_from_address(&handshake.server_address) else {
        if handshake.next_state == 1 {
            let _ = send_status_and_ping(
                &mut stream,
                &buf[handshake.packet_total_len..],
                handshake.protocol_version,
                "Burrow",
                "Use <CODE>.burrow.meeyah.dev to join a hosted world",
                0,
                0,
            )
            .await;
        } else {
            let _ = send_login_disconnect(
                &mut stream,
                "Enter a Burrow join address like <CODE>.burrow.meeyah.dev",
            )
            .await;
        }
        return;
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

fn extract_code_from_address(raw: &str) -> Option<String> {
    // Strip Forge/FML null-delimited suffixes and trailing dot.
    let clean = raw.split('\0').next().unwrap_or("").trim().trim_end_matches('.');
    let first_label = clean.split('.').next().unwrap_or("");
    ws::normalize_join_code(first_label)
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

    #[test]
    fn parses_vanilla_handshake_and_extracts_code() {
        let mut payload = Vec::new();
        write_varint(&mut payload, 770); // protocol
        write_mc_string(&mut payload, "ab12cd.burrow.meeyah.dev");
        payload.extend_from_slice(&25565u16.to_be_bytes());
        write_varint(&mut payload, 2); // login
        let pkt = write_packet(0x00, &payload);

        let hs = try_parse_handshake(&pkt).expect("valid handshake");
        assert_eq!(hs.protocol_version, 770);
        assert_eq!(hs.next_state, 2);
        assert_eq!(
            extract_code_from_address(&hs.server_address).as_deref(),
            Some("AB12CD")
        );
    }
}
