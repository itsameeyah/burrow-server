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
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::oneshot;
use uuid::Uuid;

pub struct Room {
    pub host: Option<oneshot::Sender<WebSocket>>,
    pub joiner: Option<oneshot::Sender<WebSocket>>,
    pub host_token: String,
    pub joiner_token: String,
}

#[derive(Default)]
pub struct Relay {
    pub rooms: std::sync::Mutex<HashMap<Uuid, Room>>,
}

impl Relay {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a room and spawn its broker. The broker waits (bounded) for
    /// both parties, splices their sockets, then removes the room.
    pub fn create_room(&self, app: &Arc<App>, _joiner: Uuid) -> (Uuid, String, String) {
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
        tokio::spawn(broker(app.clone(), room_id, host_rx, joiner_rx));
        (room_id, host_token, joiner_token)
    }
}

async fn broker(
    app: Arc<App>,
    room_id: Uuid,
    host_rx: oneshot::Receiver<WebSocket>,
    joiner_rx: oneshot::Receiver<WebSocket>,
) {
    let paired = tokio::time::timeout(Duration::from_secs(60), async {
        let Ok(host) = host_rx.await else {
            return None;
        };
        let Ok(joiner) = joiner_rx.await else {
            return None;
        };
        Some((host, joiner))
    })
    .await
    .unwrap_or(None);

    app.relay.rooms.lock().unwrap().remove(&room_id);
    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "relay_close", None);
    }

    match paired {
        Some((host, joiner)) => {
            tracing::info!("relay room {room_id} paired, splicing");
            splice(host, joiner).await;
        }
        None => tracing::info!("relay room {room_id} expired unmatched"),
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
