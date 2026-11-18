//! The signaling websocket: hello/auth, friends CRUD, presence, sessions,
//! and relay room handout. Every message is a JSON object with an "op" field,
//! mirroring the mod's Protocol class exactly.

use crate::{db, hub, relay};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct App {
    pub db: Mutex<rusqlite::Connection>,
    pub hub: hub::Hub,
    pub relay: relay::Relay,
    pub sessions: tokio::sync::Mutex<HashMap<Uuid, String>>,
    pub dev_mode: bool,
    pub admin_token: Option<String>,
}

type Tx = mpsc::Sender<String>;

pub async fn ws_handler(State(app): State<Arc<App>>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| run_client(app, socket))
}

async fn send(tx: &Tx, text: String) {
    let _ = tx.send(text).await;
}

fn error_op(message: impl std::fmt::Display) -> String {
    json!({"op": "error", "message": message.to_string()}).to_string()
}

async fn run_client(app: Arc<App>, socket: WebSocket) {
    let (tx, mut rx) = mpsc::channel::<String>(64);
    let (mut sink, mut stream) = socket.split();

    let writer = tokio::spawn(async move {
        use futures_util::SinkExt;
        while let Some(msg) = rx.recv().await {
            if sink.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    let mut identity: Option<Uuid> = None;

    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let Some(op) = value.get("op").and_then(Value::as_str) else {
                    continue;
                };
                handle_message(&app, &mut identity, &tx, op, &value).await;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    if let Some(me) = identity {
        app.hub.remove(&me).await;
        {
            let conn = app.db.lock().unwrap();
            db::log_event(&conn, "disconnect", None);
        }
        broadcast_presence(&app, &me).await;
    }

    drop(tx);
    let _ = writer.await;
}

async fn handle_message(app: &Arc<App>, identity: &mut Option<Uuid>, tx: &Tx, op: &str, v: &Value) {
    match op {
        "hello" => hello(app, identity, tx, v).await,
        "friends.sync" => {
            if let Some(me) = identity {
                send(tx, build_sync(app, me).await).await;
            }
        }
        "friend.add" => match identity {
            Some(me) => friend_add(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "friend.accept" => match identity {
            Some(me) => friend_accept(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "friend.decline" => match identity {
            Some(me) => friend_decline(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "friend.remove" => match identity {
            Some(me) => friend_remove(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "session.open" => match identity {
            Some(me) => session_open(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "session.close" => match identity {
            Some(me) => session_close(app, me).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "join.begin" => match identity {
            Some(me) => join_begin(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        other => send(tx, error_op(format!("unknown op '{other}'"))).await,
    }
}

async fn hello(app: &Arc<App>, identity: &mut Option<Uuid>, tx: &Tx, v: &Value) {
    let mode = v.get("mode").and_then(Value::as_str).unwrap_or("anonymous");

    let (uuid, name) = match mode {
        "dev" => {
            if !app.dev_mode {
                send(tx, error_op("this server does not accept dev identities")).await;
                return;
            }
            let name = v
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("dev")
                .to_string();
            let Some(uuid) = v
                .get("uuid")
                .and_then(Value::as_str)
                .and_then(|s| Uuid::parse_str(s).ok())
            else {
                send(tx, error_op("dev hello requires a uuid")).await;
                return;
            };
            (uuid, name)
        }
        // Real Microsoft/Xbox authentication lands with the next milestone.
        // Until then, non-dev clients get a fresh identity under their chosen
        // name, which is enough to exercise the UI against a real server.
        _ => {
            let name = v
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("Guest")
                .to_string();
            (Uuid::new_v4(), name)
        }
    };

    {
        let conn = app.db.lock().unwrap();
        db::upsert_user(&conn, uuid, &name).ok();
        db::log_event(&conn, "hello", None);
    }

    app.hub
        .insert(uuid, hub::HubClient { name: name.clone(), tx: tx.clone() })
        .await;
    *identity = Some(uuid);

    send(tx, json!({"op": "welcome", "uuid": uuid.to_string(), "name": name}).to_string()).await;
    send(tx, build_sync(app, &uuid).await).await;
    broadcast_presence(app, &uuid).await;
}

/// Build the friends.sync payload for one user: friends (with live
/// presence), incoming requests, outgoing requests.
async fn build_sync(app: &App, me: &Uuid) -> String {
    let (friend_pairs, incoming, outgoing) = {
        let conn = app.db.lock().unwrap();
        let friends: Vec<(Uuid, String)> = db::friend_uuids(&conn, me)
            .iter()
            .filter_map(|u| db::user_by_uuid(&conn, u))
            .collect();
        let incoming = db::incoming_requests(&conn, me);
        let outgoing = db::outgoing_requests(&conn, me);
        (friends, incoming, outgoing)
    };

    let world_names: HashMap<Uuid, String> = app.sessions.lock().await.clone();

    let mut friends: Vec<Value> = Vec::new();
    for (u, name) in &friend_pairs {
        let online = app.hub.is_online(u).await;
        let world = world_names.get(u).cloned().unwrap_or_default();
        friends.push(json!({
            "uuid": u.to_string(),
            "name": name,
            "presence": if online { "online" } else { "offline" },
            "hosting": world_names.contains_key(u),
            "worldName": world,
        }));
    }
    let incoming: Vec<Value> = incoming
        .iter()
        .map(|(u, name)| json!({"uuid": u.to_string(), "name": name}))
        .collect();
    let outgoing: Vec<Value> = outgoing
        .iter()
        .map(|(u, name)| json!({"uuid": u.to_string(), "name": name}))
        .collect();

    json!({"op": "friends.sync", "friends": friends, "incoming": incoming, "outgoing": outgoing}).to_string()
}

async fn friend_add(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let Some(name) = v.get("name").and_then(Value::as_str) else {
        send(tx, error_op("friend.add requires a name")).await;
        return;
    };

    let target = {
        let conn = app.db.lock().unwrap();
        db::user_by_name(&conn, name)
    };

    let Some((target_uuid, target_name)) = target else {
        send(tx, error_op(format!("no profile named '{name}' exists on this server yet"))).await;
        return;
    };
    if target_uuid == *me {
        send(tx, error_op("that's you 🙂")).await;
        return;
    }

    // If they already sent us a request, this accept-path completes the
    // friendship instead of creating a duplicate.
    let reverse_pending = {
        let conn = app.db.lock().unwrap();
        db::request_exists(&conn, &target_uuid, me)
    };

    if reverse_pending {
        {
            let conn = app.db.lock().unwrap();
            db::delete_request(&conn, &target_uuid, me);
            db::add_friendship(&conn, &target_uuid, me);
        }
        send(tx, build_sync(app, me).await).await;
        if let Some(other) = app.hub.tx(&target_uuid).await {
            send(&other, build_sync(app, &target_uuid).await).await;
        }
        push_presence_to_friends(app, me).await;
        return;
    }

    let (already_friends, already_sent) = {
        let conn = app.db.lock().unwrap();
        (
            db::are_friends(&conn, me, &target_uuid),
            db::request_exists(&conn, me, &target_uuid),
        )
    };

    if already_friends {
        send(tx, error_op(format!("you and {target_name} are already friends"))).await;
        return;
    }
    if already_sent {
        send(tx, error_op("request already pending")).await;
        return;
    }

    {
        let conn = app.db.lock().unwrap();
        db::add_request(&conn, me, &target_uuid);
        db::log_event(&conn, "friend_request_sent", None);
    }
    send(tx, build_sync(app, me).await).await;
    if let Some(other) = app.hub.tx(&target_uuid).await {
        send(&other, build_sync(app, &target_uuid).await).await;
    }
}

async fn friend_accept(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let Some(requester) = uuid_field(v) else {
        send(tx, error_op("friend.accept requires a uuid")).await;
        return;
    };
    let exists = {
        let conn = app.db.lock().unwrap();
        db::request_exists(&conn, &requester, me)
    };
    if !exists {
        send(tx, error_op("no such pending request")).await;
        return;
    }
    {
        let conn = app.db.lock().unwrap();
        db::delete_request(&conn, &requester, me);
        db::add_friendship(&conn, &requester, me);
        db::log_event(&conn, "friend_added", None);
    }
    send(tx, build_sync(app, me).await).await;
    if let Some(other) = app.hub.tx(&requester).await {
        send(&other, build_sync(app, &requester).await).await;
    }
    push_presence_to_friends(app, me).await;
}

async fn friend_decline(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let Some(other_uuid) = uuid_field(v) else {
        send(tx, error_op("friend.decline requires a uuid")).await;
        return;
    };
    // Covers both "decline their incoming request" and "cancel my outgoing
    // request" — whichever direction the row exists in is removed.
    {
        let conn = app.db.lock().unwrap();
        db::delete_request(&conn, &other_uuid, me);
        db::delete_request(&conn, me, &other_uuid);
    }
    send(tx, build_sync(app, me).await).await;
    if let Some(other) = app.hub.tx(&other_uuid).await {
        send(&other, build_sync(app, &other_uuid).await).await;
    }
}

async fn friend_remove(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let Some(other_uuid) = uuid_field(v) else {
        send(tx, error_op("friend.remove requires a uuid")).await;
        return;
    };
    {
        let conn = app.db.lock().unwrap();
        db::remove_friendship(&conn, me, &other_uuid);
    }
    send(tx, build_sync(app, me).await).await;
    if let Some(other) = app.hub.tx(&other_uuid).await {
        send(&other, build_sync(app, &other_uuid).await).await;
    }
}

async fn session_open(app: &Arc<App>, me: &Uuid, _tx: &Tx, v: &Value) {
    let world_name = v
        .get("worldName")
        .and_then(Value::as_str)
        .unwrap_or("a world")
        .to_string();
    app.sessions.lock().await.insert(*me, world_name);
    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "session_open", None);
    }
    push_session_state(app, me).await;
}

async fn session_close(app: &Arc<App>, me: &Uuid) {
    app.sessions.lock().await.remove(me);
    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "session_close", None);
    }
    push_session_state(app, me).await;
}

async fn join_begin(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let Some(host) = uuid_field(v) else {
        send(tx, error_op("join.begin requires a host uuid")).await;
        return;
    };
    let host_world = app.sessions.lock().await.get(&host).cloned();
    let Some(world) = host_world else {
        send(tx, error_op("that friend is not hosting right now")).await;
        return;
    };
    let Some(host_tx) = app.hub.tx(&host).await else {
        send(tx, error_op("that friend is not online")).await;
        return;
    };

    let (room_id, host_token, joiner_token) = app.relay.create_room(app, *me);
    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "join_begin", None);
    }

    send(
        tx,
        json!({
            "op": "join.ready",
            "host": host.to_string(),
            "worldName": world,
            "roomId": room_id.to_string(),
            "token": joiner_token,
            "relayPath": "/relay/ws"
        })
        .to_string(),
    )
    .await;
    send(
        &host_tx,
        json!({"op": "relay.host", "roomId": room_id.to_string(), "token": host_token, "relayPath": "/relay/ws"})
            .to_string(),
    )
    .await;
}

/// Tell all my online friends what my presence currently looks like.
async fn broadcast_presence(app: &Arc<App>, me: &Uuid) {
    push_presence_to_friends(app, me).await;
}

async fn push_presence_to_friends(app: &Arc<App>, me: &Uuid) {
    let friends = {
        let conn = app.db.lock().unwrap();
        db::friend_uuids(&conn, me)
    };
    let online = app.hub.is_online(me).await;
    let hosting_world = app.sessions.lock().await.get(me).cloned();
    let update = json!({
        "op": "presence.batch",
        "updates": [{
            "uuid": me.to_string(),
            "presence": if online { "online" } else { "offline" },
            "hosting": hosting_world.is_some(),
            "worldName": hosting_world.clone().unwrap_or_default(),
        }]
    })
    .to_string();
    for friend in friends {
        if friend == *me {
            continue;
        }
        if let Some(tx) = app.hub.tx(&friend).await {
            send(&tx, update.clone()).await;
        }
    }
}

async fn push_session_state(app: &Arc<App>, me: &Uuid) {
    let friends = {
        let conn = app.db.lock().unwrap();
        db::friend_uuids(&conn, me)
    };
    let world = app.sessions.lock().await.get(me).cloned();
    let state = json!({
        "op": "session.state",
        "sessions": [{
            "host": me.to_string(),
            "worldName": world.clone().unwrap_or_default(),
            "active": world.is_some(),
        }]
    })
    .to_string();
    for friend in friends {
        if let Some(tx) = app.hub.tx(&friend).await {
            send(&tx, state.clone()).await;
        }
    }
    // The host itself also receives its own session state.
    if let Some(tx) = app.hub.tx(me).await {
        send(&tx, state.clone()).await;
    }
}

fn uuid_field(v: &Value) -> Option<Uuid> {
    v.get("uuid")
        .or_else(|| v.get("host"))
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
}
