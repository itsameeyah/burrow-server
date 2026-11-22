//! Relay rooms: a host and a joiner each connect one websocket carrying a
//! room id + token; the broker pairs them and then blindly splices binary
//! frames both ways. The relay never interprets the bytes — vanilla
//! Minecraft encrypts its own traffic end-to-end, so all it ever sees is
//! ciphertext.

use crate::db;
use crate::ws::App;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

pub const MAX_ROOMS_PER_HOST: usize = 64;

pub struct Room {
    pub host: Option<oneshot::Sender<WebSocket>>,
    pub joiner: Option<oneshot::Sender<WebSocket>>,
    pub host_token: String,
    pub joiner_token: String,
}

#[derive(Default)]
pub struct Relay {
    pub rooms: std::sync::Mutex<HashMap<Uuid, Room>>,
    host_counts: std::sync::Mutex<HashMap<Uuid, usize>>,
    active_tunnels: AtomicUsize,
}

impl Relay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pending (awaiting pairing) plus currently spliced relay tunnels.
    pub fn active_count(&self) -> usize {
        self.rooms.lock().unwrap().len() + self.active_tunnels.load(Ordering::Relaxed)
    }

    /// Create a room and spawn its broker. The broker waits (bounded) for
    /// both parties, splices their sockets, then removes the room.
    /// Returns `None` if `host` already has `MAX_ROOMS_PER_HOST` pending+active rooms.
    pub fn create_room(
        &self,
        app: &Arc<App>,
        host: Uuid,
        _joiner: Uuid,
    ) -> Option<(Uuid, String, String)> {
        {
            let mut counts = self.host_counts.lock().unwrap();
            let count = counts.entry(host).or_insert(0);
            if *count >= MAX_ROOMS_PER_HOST {
                return None;
            }
            *count += 1;
        }

        let host_guard = HostRoomGuard { app: app.clone(), host };
        let room_id = Uuid::new_v4();
        let host_token = Uuid::new_v4().to_string();
        let joiner_token = Uuid::new_v4().to_string();
        let (host_tx, host_rx) = oneshot::channel::<WebSocket>();
        let (joiner_tx, joiner_rx) = oneshot::channel::<WebSocket>();
        self.rooms.lock().unwrap().insert(
            room_id,
            Room {
                host: Some(host_tx),
                joiner: Some(joiner_tx),
                host_token: host_token.clone(),
                joiner_token: joiner_token.clone(),
            },
        );
        tokio::spawn(broker(app.clone(), room_id, host_rx, joiner_rx, host_guard));
        Some((room_id, host_token, joiner_token))
    }

    /// Create a host-only relay room bridged directly to a raw vanilla Minecraft
    /// TCP connection (for non-Burrow players connecting via `<CODE>.<domain>`).
    pub fn create_tcp_room(
        &self,
        app: &Arc<App>,
        host: Uuid,
        tcp: tokio::net::TcpStream,
        initial_bytes: Vec<u8>,
    ) -> Option<(Uuid, String)> {
        {
            let mut counts = self.host_counts.lock().unwrap();
            let count = counts.entry(host).or_insert(0);
            if *count >= MAX_ROOMS_PER_HOST {
                return None;
            }
            *count += 1;
        }

        let host_guard = HostRoomGuard { app: app.clone(), host };
        let room_id = Uuid::new_v4();
        let host_token = Uuid::new_v4().to_string();
        let (host_tx, host_rx) = oneshot::channel::<WebSocket>();
        self.rooms.lock().unwrap().insert(
            room_id,
            Room {
                host: Some(host_tx),
                joiner: None,
                host_token: host_token.clone(),
                joiner_token: String::new(),
            },
        );
        tokio::spawn(broker_tcp(
            app.clone(),
            room_id,
            host_rx,
            tcp,
            initial_bytes,
            host_guard,
        ));
        Some((room_id, host_token))
    }
}

struct HostRoomGuard {
    app: Arc<App>,
    host: Uuid,
}

impl Drop for HostRoomGuard {
    fn drop(&mut self) {
        let mut counts = self.app.relay.host_counts.lock().unwrap();
        if let Some(c) = counts.get_mut(&self.host) {
            if *c <= 1 {
                counts.remove(&self.host);
            } else {
                *c -= 1;
            }
        }
    }
}

struct ActiveTunnelGuard(Arc<App>);

impl ActiveTunnelGuard {
    fn enter(app: Arc<App>) -> Self {
        app.relay.active_tunnels.fetch_add(1, Ordering::Relaxed);
        Self(app)
    }
}

impl Drop for ActiveTunnelGuard {
    fn drop(&mut self) {
        self.0.relay.active_tunnels.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn broker(
    app: Arc<App>,
    room_id: Uuid,
    host_rx: oneshot::Receiver<WebSocket>,
    joiner_rx: oneshot::Receiver<WebSocket>,
    _host_guard: HostRoomGuard,
) {
    let paired = tokio::time::timeout(
        Duration::from_secs(60),
        async { tokio::try_join!(host_rx, joiner_rx).ok() },
    )
    .await
    .unwrap_or(None);

    app.relay.rooms.lock().unwrap().remove(&room_id);

    match paired {
        Some((host, joiner)) => {
            let _guard = ActiveTunnelGuard::enter(app.clone());
            {
                let conn = app.db.lock().unwrap();
                db::log_event(&conn, "relay_open", None);
            }
            tracing::info!("relay room {room_id} paired, splicing");
            splice(host, joiner).await;
            {
                let conn = app.db.lock().unwrap();
                db::log_event(&conn, "relay_close", None);
            }
            tracing::info!("relay room {room_id} closed");
        }
        None => {
            {
                let conn = app.db.lock().unwrap();
                db::log_event(&conn, "relay_expired", None);
            }
            tracing::info!("relay room {room_id} expired unmatched");
        }
    }
}

async fn broker_tcp(
    app: Arc<App>,
    room_id: Uuid,
    host_rx: oneshot::Receiver<WebSocket>,
    tcp: tokio::net::TcpStream,
    initial_bytes: Vec<u8>,
    _host_guard: HostRoomGuard,
) {
    let paired = tokio::time::timeout(Duration::from_secs(20), host_rx)
        .await
        .ok()
        .and_then(|r| r.ok());

    app.relay.rooms.lock().unwrap().remove(&room_id);

    match paired {
        Some(host) => {
            let _guard = ActiveTunnelGuard::enter(app.clone());
            {
                let conn = app.db.lock().unwrap();
                db::log_event(&conn, "relay_open", None);
            }
            tracing::info!("tcp relay room {room_id} paired, splicing");
            splice_ws_tcp(host, tcp, initial_bytes).await;
            {
                let conn = app.db.lock().unwrap();
                db::log_event(&conn, "relay_close", None);
            }
            tracing::info!("tcp relay room {room_id} closed");
        }
        None => {
            {
                let conn = app.db.lock().unwrap();
                db::log_event(&conn, "relay_expired", None);
            }
            tracing::info!("tcp relay room {room_id} expired waiting for host");
        }
    }
}

/// Forward binary frames both directions until either side closes.
async fn splice(a: WebSocket, b: WebSocket) {
    let (mut a_sink, mut a_stream) = a.split();
    let (mut b_sink, mut b_stream) = b.split();

    let mut a_to_b = tokio::spawn(async move {
        use futures_util::SinkExt;
        while let Some(Ok(msg)) = a_stream.next().await {
            match msg {
                Message::Binary(data) => {
                    if b_sink.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => {
                    let _ = b_sink.send(Message::Close(None)).await;
                    break;
                }
                _ => {}
            }
        }
    });

    let mut b_to_a = tokio::spawn(async move {
        use futures_util::SinkExt;
        while let Some(Ok(msg)) = b_stream.next().await {
            match msg {
                Message::Binary(data) => {
                    if a_sink.send(Message::Binary(data)).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => {
                    let _ = a_sink.send(Message::Close(None)).await;
                    break;
                }
                _ => {}
            }
        }
    });

    // When either direction ends, tear down the other so both clients see
    // the tunnel close at the same time.
    tokio::select! {
        _ = &mut a_to_b => { b_to_a.abort(); }
        _ = &mut b_to_a => { a_to_b.abort(); }
    }
}

/// Splice a host WebSocket relay with a raw vanilla Minecraft TCP stream.
async fn splice_ws_tcp(ws: WebSocket, tcp: tokio::net::TcpStream, initial_bytes: Vec<u8>) {
    use futures_util::SinkExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let _ = tcp.set_nodelay(true);
    let (mut ws_sink, mut ws_stream) = ws.split();
    let (mut tcp_read, mut tcp_write) = tcp.into_split();

    if !initial_bytes.is_empty() && ws_sink.send(Message::Binary(initial_bytes.into())).await.is_err() {
        return;
    }

    let mut tcp_to_ws = tokio::spawn(async move {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            match tcp_read.read(&mut buf).await {
                Ok(0) | Err(_) => {
                    let _ = ws_sink.send(Message::Close(None)).await;
                    break;
                }
                Ok(n) => {
                    if ws_sink.send(Message::Binary(buf[..n].to_vec().into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut ws_to_tcp = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_stream.next().await {
            match msg {
                Message::Binary(data) => {
                    if tcp_write.write_all(&data).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => {
                    let _ = tcp_write.shutdown().await;
                    break;
                }
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = &mut tcp_to_ws => { ws_to_tcp.abort(); }
        _ = &mut ws_to_tcp => { tcp_to_ws.abort(); }
    }
}

/// /relay/ws?room=<uuid>&token=<token> — upgrade and hand the socket to
/// the room's broker. Tokens are single-use and know which side they are.
pub async fn relay_handler(
    State(app): State<Arc<App>>,
    Query(params): Query<HashMap<String, String>>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Some(room_str) = params.get("room") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(room_id) = Uuid::parse_str(room_str) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Some(token) = params.get("token") else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    let slot = {
        let mut rooms = app.relay.rooms.lock().unwrap();
        match rooms.get_mut(&room_id) {
            None => return StatusCode::NOT_FOUND.into_response(),
            Some(room) => {
                if token == &room.host_token {
                    room.host.take()
                } else if token == &room.joiner_token {
                    room.joiner.take()
                } else {
                    None
                }
            }
        }
    };

    match slot {
        Some(sender) => upgrade.on_upgrade(move |socket| async move {
            let _ = sender.send(socket);
        }),
        None => StatusCode::CONFLICT.into_response(),
    }
}

use futures_util::StreamExt;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn per_host_room_cap_enforced() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let app = Arc::new(App {
            db: std::sync::Mutex::new(conn),
            hub: crate::hub::Hub::new(),
            relay: Relay::new(),
            sessions: tokio::sync::Mutex::new(HashMap::new()),
            activities: tokio::sync::Mutex::new(HashMap::new()),
            dev_mode: false,
            mojang_session_url: "https://sessionserver.mojang.com".into(),
            admin_token: None,
            discord_client_id: None,
            discord_client_secret: None,
            discord_redirect_uri: None,
            discord_allowed_user_id: None,
            oauth_states: tokio::sync::Mutex::new(HashMap::new()),
            http: reqwest::Client::new(),
        });

        let host_a = Uuid::new_v4();
        let host_b = Uuid::new_v4();
        let joiner = Uuid::new_v4();

        for _ in 0..MAX_ROOMS_PER_HOST {
            assert!(app.relay.create_room(&app, host_a, joiner).is_some());
        }
        // 65th room for host_a must be rejected.
        assert!(app.relay.create_room(&app, host_a, joiner).is_none());
        // Different host_b is unaffected.
        assert!(app.relay.create_room(&app, host_b, joiner).is_some());
    }
}
