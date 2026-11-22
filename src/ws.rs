//! The signaling websocket: hello/auth, friends CRUD, presence, sessions,
//! and relay room handout. Every message is a JSON object with an "op" field,
//! mirroring the mod's Protocol class exactly.

use crate::{db, hub, relay};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessMode {
    InviteOnly,
    Friends,
    FriendsOfFriends,
}

impl AccessMode {
    pub fn parse(s: &str) -> Self {
        match s {
            "invite_only" => Self::InviteOnly,
            // Legacy "open" (former Friend Network) safely falls back to FriendsOfFriends.
            "friends_of_friends" | "open" => Self::FriendsOfFriends,
            _ => Self::Friends,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::InviteOnly => "invite_only",
            Self::Friends => "friends",
            Self::FriendsOfFriends => "friends_of_friends",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvitePerm {
    HostOnly,
    Operators,
    Anyone,
}

impl InvitePerm {
    pub fn parse(s: &str) -> Self {
        match s {
            "operators" => Self::Operators,
            "anyone" => Self::Anyone,
            _ => Self::HostOnly,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::HostOnly => "host_only",
            Self::Operators => "operators",
            Self::Anyone => "anyone",
        }
    }
}

const INVITE_TTL_SECS: i64 = 30;
const FOF_RECONNECT_GRACE_SECS: i64 = 120;
const AUTH_CHALLENGE_TTL_SECS: i64 = 30;
const AUTH_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(45);
const WS_PING_INTERVAL: Duration = Duration::from_secs(30);
const MOJANG_VERIFY_TIMEOUT: Duration = Duration::from_secs(10);
const RATE_LIMIT_MAX: usize = 40;
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(5);
const RATE_LIMIT_CLOSE_STRIKES: u32 = 10;
const MAX_SIGNALING_MSG_BYTES: usize = 65_536;

#[derive(Clone, Debug)]
pub struct HostedSession {
    pub world_name: String,
    pub game_version: String,
    pub access: AccessMode,
    pub invite_perm: InvitePerm,
    pub locked: bool,
    pub max_players: u32,
    pub player_count: u32,
    /// invitee uuid -> (inviter uuid, inviter name, expires_at unix secs)
    pub invites: HashMap<Uuid, (Uuid, String, i64)>,
    pub operators: HashSet<Uuid>,
    pub banned: HashSet<Uuid>,
    /// Players who have already opened a game join room for this session
    /// (allowed to open/reconnect auxiliary voice tunnels even if world fills/locks).
    pub active_joiners: HashSet<Uuid>,
    /// Live set of player UUIDs currently inside the hosted world (reported by host).
    pub current_players: HashSet<Uuid>,
    /// Short reconnect grace window (2 minutes) for players who were already in
    /// the world and disconnected (e.g. FoF guest whose mutual friend already left).
    pub grace_joiners: HashMap<Uuid, i64>,
    /// Optional 6-character uppercase alphanumeric (A-Z, 0-9) join code (off by default).
    pub join_code: Option<String>,
}

impl HostedSession {
    pub fn active_invite(&self, uuid: &Uuid) -> Option<&(Uuid, String, i64)> {
        let now = db::now();
        self.invites.get(uuid).filter(|(_, _, exp)| *exp > now)
    }

    pub fn prune_expired_invites(&mut self) {
        let now = db::now();
        self.invites.retain(|_, (_, _, exp)| *exp > now);
    }

    pub fn has_reconnect_grace(&self, uuid: &Uuid) -> bool {
        let now = db::now();
        self.grace_joiners.get(uuid).is_some_and(|exp| *exp > now)
    }

    pub fn prune_expired_grace(&mut self) {
        let now = db::now();
        self.grace_joiners.retain(|_, exp| *exp > now);
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClientActivity {
    pub kind: String,        // "idle" | "singleplayer" | "server" | "local" | "multiplayer" | "burrow"
    pub server_name: String, // saved server name (empty if hidden/direct-connect/incognito)
    pub server_ip: String,   // joinable IP (empty if hidden/direct-connect/local/incognito)
    pub label: String,       // extra display label (e.g. host name when in a Burrow world)
}

pub struct App {
    pub db: Mutex<rusqlite::Connection>,
    pub hub: hub::Hub,
    pub relay: relay::Relay,
    pub sessions: tokio::sync::Mutex<HashMap<Uuid, HostedSession>>,
    pub activities: tokio::sync::Mutex<HashMap<Uuid, ClientActivity>>,
    pub dev_mode: bool,
    /// Base URL for Mojang sessionserver (overridable via BURROW_MOJANG_SESSION_URL for tests).
    pub mojang_session_url: String,
    pub admin_token: Option<String>,
    /// Discord OAuth gate for the dashboard (all optional until configured).
    pub discord_client_id: Option<String>,
    pub discord_client_secret: Option<String>,
    pub discord_redirect_uri: Option<String>,
    pub discord_allowed_user_id: Option<String>,
    /// Short-lived CSRF states for OAuth round-trips.
    pub oauth_states: tokio::sync::Mutex<HashMap<String, i64>>,
    /// Shared HTTP client for Discord OAuth & Cloudflare DNS API calls.
    pub http: reqwest::Client,
    /// Optional Cloudflare dynamic SRV manager for non-25565 vanilla join codes.
    pub cloudflare_srv: Option<Arc<crate::cloudflare_srv::CloudflareSrv>>,
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

struct PendingAuth {
    server_id: String,
    claimed_name: String,
    expires_at: i64,
}

async fn run_client(app: Arc<App>, socket: WebSocket) {
    let (tx, mut rx) = mpsc::channel::<String>(64);
    let (mut sink, mut stream) = socket.split();
    let conn_id = hub::mint_conn_id();

    let writer = tokio::spawn(async move {
        use futures_util::SinkExt;
        let mut ping_tick = tokio::time::interval(WS_PING_INTERVAL);
        ping_tick.tick().await; // skip immediate first tick
        loop {
            tokio::select! {
                maybe_msg = rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            if sink.send(Message::Text(msg.into())).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            let _ = sink.send(Message::Close(None)).await;
                            break;
                        }
                    }
                }
                _ = ping_tick.tick() => {
                    if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    let mut identity: Option<Uuid> = None;
    let mut pending_auth: Option<PendingAuth> = None;
    let mut msg_times: VecDeque<Instant> = VecDeque::with_capacity(RATE_LIMIT_MAX);
    let mut rate_strikes: u32 = 0;
    let auth_deadline = tokio::time::Instant::now() + AUTH_HANDSHAKE_TIMEOUT;

    loop {
        let next_frame = if identity.is_none() {
            match tokio::time::timeout_at(auth_deadline, stream.next()).await {
                Ok(frame) => frame,
                Err(_) => {
                    tracing::info!("closing unauthenticated signaling connection {conn_id} after handshake timeout");
                    break;
                }
            }
        } else {
            stream.next().await
        };

        let Some(Ok(msg)) = next_frame else {
            break;
        };

        match msg {
            Message::Text(text) => {
                let now_inst = Instant::now();
                while msg_times
                    .front()
                    .is_some_and(|t| now_inst.duration_since(*t) >= RATE_LIMIT_WINDOW)
                {
                    msg_times.pop_front();
                }
                if msg_times.len() >= RATE_LIMIT_MAX {
                    rate_strikes += 1;
                    send(&tx, error_op("rate limit exceeded — slow down")).await;
                    if rate_strikes >= RATE_LIMIT_CLOSE_STRIKES {
                        tracing::warn!(
                            "closing signaling connection {conn_id} for sustained rate-limit abuse"
                        );
                        break;
                    }
                    continue;
                }
                msg_times.push_back(now_inst);
                rate_strikes = 0;

                if text.len() > MAX_SIGNALING_MSG_BYTES {
                    send(&tx, error_op("message too large")).await;
                    continue;
                }
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                let Some(op) = value.get("op").and_then(Value::as_str) else {
                    continue;
                };
                if handle_message(&app, &mut identity, &mut pending_auth, &tx, conn_id, op, &value)
                    .await
                {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    // Cleanup only if OUR generation still owns the hub entry — a newer
    // reconnect must not be evicted by this dying connection.
    if let Some(me) = identity {
        if app.hub.remove_owned(&me, conn_id).await.is_some() {
            // A host that vanished takes its hosted-world session & activity with it.
            let prev_session = app.sessions.lock().await.remove(&me);
            app.activities.lock().await.remove(&me);
            {
                let conn = app.db.lock().unwrap();
                db::log_event(&conn, "disconnect", None);
            }
            if let Some(ref prev) = prev_session {
                if let Some(ref cf) = app.cloudflare_srv {
                    cf.spawn_sync(prev.join_code.clone(), None);
                }
                push_session_state(&app, &me, Some(prev)).await;
            }
            broadcast_presence(&app, &me).await;
        }
    }

    drop(tx);
    let _ = writer.await;
}

/// Returns `true` when the connection should be closed after flushing queued messages.
async fn handle_message(
    app: &Arc<App>,
    identity: &mut Option<Uuid>,
    pending_auth: &mut Option<PendingAuth>,
    tx: &Tx,
    conn_id: u64,
    op: &str,
    v: &Value,
) -> bool {
    match op {
        "hello" => hello(app, identity, pending_auth, tx, conn_id, v).await,
        "auth.verify" => return auth_verify(app, identity, pending_auth, tx, conn_id, v).await,
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
        "session.update" => match identity {
            Some(me) => session_update(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "session.close" => match identity {
            Some(me) => session_close(app, me).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "session.invite" => match identity {
            Some(me) => session_invite(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "session.code" => match identity {
            Some(me) => session_code(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "code.lookup" => match identity {
            Some(me) => code_lookup(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "presence.update" => match identity {
            Some(me) => presence_update(app, me, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "invite.request" => match identity {
            Some(me) => invite_request(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "users.check" => match identity {
            Some(_) => users_check(app, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "join.begin" => match identity {
            Some(me) => join_begin(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        "join.code" => match identity {
            Some(me) => join_code(app, me, tx, v).await,
            None => send(tx, error_op("say hello first")).await,
        },
        other => send(tx, error_op(format!("unknown op '{other}'"))).await,
    }
    false
}

async fn hello(
    app: &Arc<App>,
    identity: &mut Option<Uuid>,
    pending_auth: &mut Option<PendingAuth>,
    tx: &Tx,
    conn_id: u64,
    v: &Value,
) {
    if identity.is_some() {
        // One identity per websocket: a second hello would orphan the first
        // hub entry without ever cleaning it up.
        send(tx, error_op("this connection is already identified")).await;
        return;
    }
    let mode = v.get("mode").and_then(Value::as_str).unwrap_or("");

    match mode {
        "mojang" => {
            let Some(claimed_name) = v
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty() && s.len() <= 32)
            else {
                send(tx, error_op("mojang hello requires a name")).await;
                return;
            };
            let server_id = Uuid::new_v4().simple().to_string();
            *pending_auth = Some(PendingAuth {
                server_id: server_id.clone(),
                claimed_name: claimed_name.to_string(),
                expires_at: db::now() + AUTH_CHALLENGE_TTL_SECS,
            });
            send(tx, json!({"op": "auth.challenge", "serverId": server_id}).to_string()).await;
        }
        "dev" if app.dev_mode => {
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
            *pending_auth = None;
            complete_login(app, identity, tx, conn_id, uuid, name).await;
        }
        _ => {
            send(tx, error_op("authentication required")).await;
        }
    }
}

/// Returns `true` when verification failed and the socket should be closed so
/// the client cleanly triggers reconnect/backoff instead of idling unauthenticated.
async fn auth_verify(
    app: &Arc<App>,
    identity: &mut Option<Uuid>,
    pending_auth: &mut Option<PendingAuth>,
    tx: &Tx,
    conn_id: u64,
    v: &Value,
) -> bool {
    const FAIL_MSG: &str = "Mojang verification failed — retrying...";
    const BUSY_MSG: &str = "Mojang auth servers are busy — retrying...";

    if identity.is_some() {
        send(tx, error_op("this connection is already identified")).await;
        return false;
    }
    // Consume the pending challenge immediately so it is strictly single-use.
    let Some(pending) = pending_auth.take() else {
        send(tx, error_op(FAIL_MSG)).await;
        return true;
    };
    let Some(server_id) = v.get("serverId").and_then(Value::as_str) else {
        send(tx, error_op(FAIL_MSG)).await;
        return true;
    };
    if server_id != pending.server_id || db::now() > pending.expires_at {
        send(tx, error_op(FAIL_MSG)).await;
        return true;
    }

    let url = format!(
        "{}/session/minecraft/hasJoined?username={}&serverId={}",
        app.mojang_session_url,
        url_encode(&pending.claimed_name),
        url_encode(&pending.server_id),
    );
    let res = match app.http.get(&url).timeout(MOJANG_VERIFY_TIMEOUT).send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("mojang hasJoined request failed for {}: {}", pending.claimed_name, e);
            send(tx, error_op(BUSY_MSG)).await;
            return true;
        }
    };
    if res.status() != reqwest::StatusCode::OK {
        let status = res.status();
        tracing::info!(
            "mojang hasJoined rejected {} with status {}",
            pending.claimed_name,
            status
        );
        let msg = if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            BUSY_MSG
        } else {
            FAIL_MSG
        };
        send(tx, error_op(msg)).await;
        return true;
    }
    let Ok(body) = res.json::<Value>().await else {
        tracing::warn!("mojang hasJoined returned invalid JSON for {}", pending.claimed_name);
        send(tx, error_op(FAIL_MSG)).await;
        return true;
    };
    let Some(uuid) = body
        .get("id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
    else {
        tracing::warn!("mojang hasJoined missing valid id for {}", pending.claimed_name);
        send(tx, error_op(FAIL_MSG)).await;
        return true;
    };
    let Some(name) = body
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
    else {
        tracing::warn!("mojang hasJoined missing valid name for {}", pending.claimed_name);
        send(tx, error_op(FAIL_MSG)).await;
        return true;
    };

    complete_login(app, identity, tx, conn_id, uuid, name).await;
    false
}

async fn complete_login(
    app: &Arc<App>,
    identity: &mut Option<Uuid>,
    tx: &Tx,
    conn_id: u64,
    uuid: Uuid,
    name: String,
) {
    {
        let mut conn = app.db.lock().unwrap();
        if let Err(e) = db::upsert_user(&mut conn, uuid, &name) {
            tracing::warn!("failed to upsert user {}: {}", name, e);
        }
        db::log_event(&conn, "hello", None);
    }

    app.hub
        .insert(uuid, hub::HubClient { name: name.clone(), tx: tx.clone(), conn_id })
        .await;
    *identity = Some(uuid);

    send(tx, json!({"op": "welcome", "uuid": uuid.to_string(), "name": name}).to_string()).await;
    send(tx, build_sync(app, &uuid).await).await;
    broadcast_presence(app, &uuid).await;
}

fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                use std::fmt::Write;
                let _ = write!(out, "%{:02X}", b);
            }
        }
    }
    out
}

/// Can `viewer` see `session` (owned by `host`) in their friends / discoverable list?
fn visible_to_friend(session: &HostedSession, viewer: &Uuid) -> bool {
    if session.banned.contains(viewer) {
        return false;
    }
    session.access != AccessMode::InviteOnly || session.active_invite(viewer).is_some()
}

/// Build the friends.sync payload for one user: friends (with live
/// presence), incoming requests, outgoing requests, and discoverable
/// non-friend worlds (live friends-of-friends or explicit invites).
async fn build_sync(app: &App, me: &Uuid) -> String {
    let (friend_pairs, incoming, outgoing) = {
        let conn = app.db.lock().unwrap();
        let friends = db::friends_with_names(&conn, me);
        let incoming = db::incoming_requests(&conn, me);
        let outgoing = db::outgoing_requests(&conn, me);
        (friends, incoming, outgoing)
    };

    let sessions: HashMap<Uuid, HostedSession> = app.sessions.lock().await.clone();
    let activities: HashMap<Uuid, ClientActivity> = app.activities.lock().await.clone();
    let friend_set: HashSet<Uuid> = friend_pairs.iter().map(|(u, _)| *u).collect();

    let mut friends: Vec<Value> = Vec::new();
    for (u, name) in &friend_pairs {
        let online = app.hub.is_online(u).await;
        let visible_sess = sessions.get(u).filter(|s| visible_to_friend(s, me));
        let act = if online {
            activities.get(u).cloned().unwrap_or_default()
        } else {
            ClientActivity::default()
        };
        friends.push(json!({
            "uuid": u.to_string(),
            "name": name,
            "presence": if online { "online" } else { "offline" },
            "hosting": visible_sess.is_some(),
            "worldName": visible_sess.map(|s| s.world_name.as_str()).unwrap_or(""),
            "gameVersion": visible_sess.map(|s| s.game_version.as_str()).unwrap_or(""),
            "access": visible_sess.map(|s| s.access.as_str()).unwrap_or(""),
            "locked": visible_sess.map(|s| s.locked).unwrap_or(false),
            "playerCount": visible_sess.map(|s| s.player_count).unwrap_or(0),
            "maxPlayers": visible_sess.map(|s| s.max_players).unwrap_or(0),
            "activity": act.kind,
            "serverName": act.server_name,
            "serverIp": act.server_ip,
            "activityLabel": act.label,
        }));
    }

    // Non-friend sessions this user can see & join (explicit invite, or
    // Friends-of-Friends where a mutual friend is currently in the world, or
    // within the 2-minute reconnect grace window).
    let mut discoverable: Vec<Value> = Vec::new();
    for (host_uuid, sess) in &sessions {
        if host_uuid == me || friend_set.contains(host_uuid) || sess.banned.contains(me) {
            continue;
        }
        let via = if let Some((_, inviter_name, _)) = sess.active_invite(me) {
            Some(("invite", inviter_name.clone()))
        } else if sess.access == AccessMode::FriendsOfFriends {
            let active_mutual = {
                let conn = app.db.lock().unwrap();
                db::mutual_friend_in_set(&conn, host_uuid, me, &sess.current_players)
            };
            if let Some((_, via_name)) = active_mutual {
                Some(("friend_of_friend", via_name))
            } else if sess.has_reconnect_grace(me) {
                Some(("friend_of_friend", String::new()))
            } else {
                None
            }
        } else {
            None
        };
        if let Some((source, via_name)) = via {
            let host_name = match app.hub.name(host_uuid).await {
                Some(n) => n,
                None => {
                    let conn = app.db.lock().unwrap();
                    db::user_name_by_uuid(&conn, host_uuid).unwrap_or_else(|| "?".into())
                }
            };
            discoverable.push(json!({
                "host": host_uuid.to_string(),
                "hostName": host_name,
                "worldName": sess.world_name,
                "gameVersion": sess.game_version,
                "access": sess.access.as_str(),
                "source": source,
                "viaName": via_name,
                "locked": sess.locked,
                "playerCount": sess.player_count,
                "maxPlayers": sess.max_players,
            }));
        }
    }

    let incoming: Vec<Value> = incoming
        .iter()
        .map(|(u, name)| json!({"uuid": u.to_string(), "name": name}))
        .collect();
    let outgoing: Vec<Value> = outgoing
        .iter()
        .map(|(u, name)| json!({"uuid": u.to_string(), "name": name}))
        .collect();

    json!({
        "op": "friends.sync",
        "friends": friends,
        "discoverable": discoverable,
        "incoming": incoming,
        "outgoing": outgoing
    })
    .to_string()
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
            db::log_event(&conn, "friend_added", None);
        }
        let my_name = resolve_user_name(app, me).await;
        send(tx, build_sync(app, me).await).await;
        if let Some(other) = app.hub.tx(&target_uuid).await {
            send(&other, build_sync(app, &target_uuid).await).await;
            send(
                &other,
                json!({
                    "op": "friend.accepted",
                    "uuid": me.to_string(),
                    "name": my_name,
                })
                .to_string(),
            )
            .await;
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
    let my_name = resolve_user_name(app, me).await;
    send(tx, build_sync(app, me).await).await;
    if let Some(other) = app.hub.tx(&requester).await {
        send(&other, build_sync(app, &requester).await).await;
        send(
            &other,
            json!({
                "op": "friend.accepted",
                "uuid": me.to_string(),
                "name": my_name,
            })
            .to_string(),
        )
        .await;
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

fn parse_uuid_set(v: &Value, key: &str) -> Option<HashSet<Uuid>> {
    let arr = v.get(key)?.as_array()?;
    Some(
        arr.iter()
            .filter_map(Value::as_str)
            .filter_map(|s| Uuid::parse_str(s).ok())
            .collect(),
    )
}

pub(crate) async fn resolve_user_name(app: &App, uuid: &Uuid) -> String {
    match app.hub.name(uuid).await {
        Some(n) => n,
        None => {
            let conn = app.db.lock().unwrap();
            db::user_name_by_uuid(&conn, uuid).unwrap_or_else(|| "?".into())
        }
    }
}

pub(crate) fn normalize_join_code(raw: &str) -> Option<String> {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect();
    if cleaned.len() == 6 {
        Some(cleaned)
    } else {
        None
    }
}

fn generate_join_code(sessions: &HashMap<Uuid, HostedSession>) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    loop {
        let bytes = Uuid::new_v4();
        let raw = bytes.as_bytes();
        let mut code = String::with_capacity(6);
        for i in 0..6 {
            let idx = (raw[i] as usize) % ALPHABET.len();
            code.push(ALPHABET[idx] as char);
        }
        let collision = sessions
            .values()
            .any(|s| s.join_code.as_deref() == Some(code.as_str()));
        if !collision {
            return code;
        }
    }
}

fn apply_session_fields(sess: &mut HostedSession, host: &Uuid, v: &Value) {
    if let Some(world) = v.get("worldName").and_then(Value::as_str) {
        if !world.is_empty() {
            sess.world_name = world.to_string();
        }
    }
    if let Some(ver) = v.get("gameVersion").and_then(Value::as_str) {
        let trimmed: String = ver.trim().chars().take(32).collect();
        if !trimmed.is_empty() {
            sess.game_version = trimmed;
        }
    }
    if let Some(access) = v.get("access").and_then(Value::as_str) {
        sess.access = AccessMode::parse(access);
    }
    if let Some(perm) = v.get("invitePerm").and_then(Value::as_str) {
        sess.invite_perm = InvitePerm::parse(perm);
    }
    if let Some(locked) = v.get("locked").and_then(Value::as_bool) {
        sess.locked = locked;
    }
    if let Some(max_p) = v.get("maxPlayers").and_then(Value::as_u64) {
        sess.max_players = (max_p as u32).clamp(1, 64);
    }
    if let Some(cnt) = v.get("playerCount").and_then(Value::as_u64) {
        sess.player_count = cnt as u32;
    }
    if let Some(ops) = parse_uuid_set(v, "operators") {
        sess.operators = ops;
    }
    if let Some(banned) = parse_uuid_set(v, "banned") {
        sess.banned = banned;
    }
    if let Some(mut players) = parse_uuid_set(v, "players") {
        players.insert(*host);
        let now = db::now();
        // Grant a 2-minute reconnect grace window to any guest who just disconnected
        // from the world (so a FoF guest whose mutual friend already left can reconnect
        // briefly after a crash/network blip).
        for prev_player in &sess.current_players {
            if prev_player != host && !players.contains(prev_player) && !sess.banned.contains(prev_player) {
                sess.grace_joiners
                    .insert(*prev_player, now + FOF_RECONNECT_GRACE_SECS);
            }
        }
        // Clear grace entries for players currently back inside or banned.
        for p in &players {
            sess.grace_joiners.remove(p);
        }
        sess.grace_joiners.retain(|u, exp| *exp > now && !sess.banned.contains(u));
        sess.current_players = players;
    } else {
        sess.current_players.insert(*host);
        sess.prune_expired_grace();
    }
}

async fn session_open(app: &Arc<App>, me: &Uuid, _tx: &Tx, v: &Value) {
    let prev = {
        let mut sessions = app.sessions.lock().await;
        let prev = sessions.get(me).cloned();
        let mut sess = prev.clone().unwrap_or_else(|| {
            let mut initial_players = HashSet::new();
            initial_players.insert(*me);
            HostedSession {
                world_name: "a world".into(),
                game_version: String::new(),
                access: AccessMode::Friends,
                invite_perm: InvitePerm::HostOnly,
                locked: false,
                max_players: 8,
                player_count: 1,
                invites: HashMap::new(),
                operators: HashSet::new(),
                banned: HashSet::new(),
                active_joiners: HashSet::new(),
                current_players: initial_players,
                grace_joiners: HashMap::new(),
                join_code: None,
            }
        });
        apply_session_fields(&mut sess, me, v);
        sessions.insert(*me, sess);
        prev
    };
    if prev.is_none() {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "session_open", None);
    }
    push_session_state(app, me, prev.as_ref()).await;
}

async fn session_update(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let prev = {
        let mut sessions = app.sessions.lock().await;
        let Some(existing) = sessions.get_mut(me) else {
            send(tx, error_op("no active hosted session to update")).await;
            return;
        };
        let prev = existing.clone();
        apply_session_fields(existing, me, v);
        prev
    };
    push_session_state(app, me, Some(&prev)).await;
}

async fn session_code(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let action = v.get("action").and_then(Value::as_str).unwrap_or("generate");
    let code_result: Result<(Option<String>, Option<String>), &'static str> = {
        let mut sessions = app.sessions.lock().await;
        if !sessions.contains_key(me) {
            Err("start hosting before generating a join code")
        } else {
            let prev_code = sessions.get(me).and_then(|s| s.join_code.clone());
            let next_code = match action {
                "disable" | "off" | "clear" => None,
                "regenerate" => Some(generate_join_code(&sessions)),
                _ => {
                    if let Some(existing) = prev_code.clone() {
                        Some(existing)
                    } else {
                        Some(generate_join_code(&sessions))
                    }
                }
            };
            if let Some(sess) = sessions.get_mut(me) {
                sess.join_code = next_code.clone();
            }
            Ok((prev_code, next_code))
        }
    };
    match code_result {
        Ok((prev_code, code)) => {
            if let Some(ref cf) = app.cloudflare_srv {
                cf.spawn_sync(prev_code, code.clone());
            }
            send(
                tx,
                json!({
                    "op": "session.code.state",
                    "code": code.as_deref().unwrap_or(""),
                    "enabled": code.is_some(),
                })
                .to_string(),
            )
            .await;
        }
        Err(msg) => {
            send(tx, error_op(msg)).await;
        }
    }
}

async fn code_lookup(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let raw = v.get("code").and_then(Value::as_str).unwrap_or("");
    let Some(code) = normalize_join_code(raw) else {
        send(
            tx,
            json!({
                "op": "code.info",
                "code": raw.trim().to_uppercase(),
                "found": false,
                "error": "Enter a 6-character join code",
            })
            .to_string(),
        )
        .await;
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
        send(
            tx,
            json!({
                "op": "code.info",
                "code": code,
                "found": false,
                "error": "No world found for that code",
            })
            .to_string(),
        )
        .await;
        return;
    };

    if sess.banned.contains(me) {
        send(
            tx,
            json!({
                "op": "code.info",
                "code": code,
                "found": false,
                "error": "You are banned from this world",
            })
            .to_string(),
        )
        .await;
        return;
    }

    let host_name = resolve_user_name(app, &host_uuid).await;
    send(
        tx,
        json!({
            "op": "code.info",
            "code": code,
            "found": true,
            "host": host_uuid.to_string(),
            "hostName": host_name,
            "worldName": sess.world_name,
            "gameVersion": sess.game_version,
            "playerCount": sess.player_count,
            "maxPlayers": sess.max_players,
            "locked": sess.locked,
            "isSelf": host_uuid == *me,
        })
        .to_string(),
    )
    .await;
}

async fn join_code(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let raw = v.get("code").and_then(Value::as_str).unwrap_or("");
    let Some(code) = normalize_join_code(raw) else {
        send(tx, error_op("enter a valid 6-character join code")).await;
        return;
    };
    let matched = {
        let sessions = app.sessions.lock().await;
        sessions
            .iter()
            .find(|(_, s)| s.join_code.as_deref() == Some(code.as_str()))
            .map(|(h, s)| (*h, s.clone()))
    };
    let Some((host, sess)) = matched else {
        send(tx, error_op("invalid or expired join code")).await;
        return;
    };
    if host == *me {
        send(tx, error_op("that's your own world")).await;
        return;
    }
    let Some(host_tx) = app.hub.tx(&host).await else {
        send(tx, error_op("that host is not online")).await;
        return;
    };
    if sess.banned.contains(me) {
        send(tx, error_op("you are banned from this world")).await;
        return;
    }
    if sess.locked {
        send(tx, error_op("this world is currently locked")).await;
        return;
    }
    if sess.player_count >= sess.max_players {
        send(tx, error_op("this world is full")).await;
        return;
    }

    let host_name = resolve_user_name(app, &host).await;
    let joiner_name = resolve_user_name(app, me).await;
    let Some((room_id, host_token, joiner_token)) = app.relay.create_room(app, host, *me) else {
        send(tx, error_op("that host has too many active connections right now")).await;
        return;
    };

    if let Some(live) = app.sessions.lock().await.get_mut(&host) {
        live.active_joiners.insert(*me);
    }
    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "join_begin", None);
    }

    send(
        tx,
        json!({
            "op": "join.ready",
            "host": host.to_string(),
            "hostName": host_name,
            "worldName": sess.world_name,
            "gameVersion": sess.game_version,
            "roomId": room_id.to_string(),
            "token": joiner_token,
            "relayPath": "/relay/ws"
        })
        .to_string(),
    )
    .await;
    send(
        &host_tx,
        json!({
            "op": "relay.host",
            "roomId": room_id.to_string(),
            "token": host_token,
            "relayPath": "/relay/ws",
            "joinerUuid": me.to_string(),
            "joinerName": joiner_name,
            "joinSource": "code",
            "viaName": "",
        })
        .to_string(),
    )
    .await;
}

async fn session_close(app: &Arc<App>, me: &Uuid) {
    let prev = app.sessions.lock().await.remove(me);
    if let (Some(cf), Some(p)) = (&app.cloudflare_srv, &prev) {
        cf.spawn_sync(p.join_code.clone(), None);
    }
    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "session_close", None);
    }
    push_session_state(app, me, prev.as_ref()).await;
}

async fn session_invite(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    // Target may be specified by uuid or by profile name.
    let target = if let Some(u) = v
        .get("uuid")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
    {
        Some(u)
    } else if let Some(name) = v.get("name").and_then(Value::as_str) {
        let conn = app.db.lock().unwrap();
        db::user_by_name(&conn, name).map(|(u, _)| u)
    } else {
        None
    };
    let Some(target_uuid) = target else {
        send(tx, error_op("session.invite requires a target uuid or name")).await;
        return;
    };
    let host_uuid = v
        .get("host")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or(*me);
    if target_uuid == host_uuid {
        send(tx, error_op("that player is already the host")).await;
        return;
    }

    let inviter_name = match app.hub.name(me).await {
        Some(n) => n,
        None => {
            let conn = app.db.lock().unwrap();
            db::user_name_by_uuid(&conn, me).unwrap_or_else(|| "?".into())
        }
    };
    let host_name = match app.hub.name(&host_uuid).await {
        Some(n) => n,
        None => {
            let conn = app.db.lock().unwrap();
            db::user_name_by_uuid(&conn, &host_uuid).unwrap_or_else(|| "?".into())
        }
    };

    let world_name = {
        let mut sessions = app.sessions.lock().await;
        let Some(sess) = sessions.get_mut(&host_uuid) else {
            send(tx, error_op("no active hosted world to invite to")).await;
            return;
        };
        let allowed = host_uuid == *me
            || match sess.invite_perm {
                InvitePerm::HostOnly => false,
                InvitePerm::Operators => sess.operators.contains(me),
                InvitePerm::Anyone => true,
            };
        if !allowed {
            send(tx, error_op("you don't have permission to invite to this world")).await;
            return;
        }
        if sess.banned.contains(&target_uuid) {
            send(tx, error_op("that player is banned from this world")).await;
            return;
        }
        sess.prune_expired_invites();
        sess.invites.insert(target_uuid, (*me, inviter_name.clone(), db::now() + INVITE_TTL_SECS));
        sess.world_name.clone()
    };

    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "session_invite", None);
    }

    if let Some(target_tx) = app.hub.tx(&target_uuid).await {
        send(
            &target_tx,
            json!({
                "op": "invite.received",
                "host": host_uuid.to_string(),
                "hostName": host_name,
                "worldName": world_name,
                "invitedBy": me.to_string(),
                "invitedByName": inviter_name,
                "ttlSeconds": INVITE_TTL_SECS,
            })
            .to_string(),
        )
        .await;
        // Also refresh the invitee's view of the host session so it becomes joinable immediately.
        send(&target_tx, build_sync(app, &target_uuid).await).await;
    }
}

async fn join_begin(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let Some(host) = uuid_field(v) else {
        send(tx, error_op("join.begin requires a host uuid")).await;
        return;
    };
    let is_voice = v.get("channel").and_then(Value::as_str) == Some("voice");
    if host == *me {
        send(tx, error_op("that's your own world 🙂")).await;
        return;
    }
    let session = app.sessions.lock().await.get(&host).cloned();
    let Some(sess) = session else {
        send(tx, error_op("that player is not hosting right now")).await;
        return;
    };
    let Some(host_tx) = app.hub.tx(&host).await else {
        send(tx, error_op("that host is not online")).await;
        return;
    };
    if sess.banned.contains(me) {
        send(tx, error_op("you are banned from this world")).await;
        return;
    }
    // Voice tunnels for players who already opened a game room on this session
    // bypass lock/cap/invite-expiry checks so SVC works even in a full or
    // freshly locked world.
    let already_joined = is_voice && sess.active_joiners.contains(me);
    if !already_joined {
        if sess.locked {
            send(tx, error_op("this world is currently locked")).await;
            return;
        }
        if !is_voice && sess.player_count >= sess.max_players {
            send(tx, error_op("this world is full")).await;
            return;
        }
    }

    let (join_source, via_name): (&str, String) = if already_joined {
        ("voice", String::new())
    } else {
        let (direct_friend, active_mutual) = {
            let conn = app.db.lock().unwrap();
            let direct = db::are_friends(&conn, me, &host);
            let mutual = if direct {
                None
            } else {
                db::mutual_friend_in_set(&conn, &host, me, &sess.current_players)
            };
            (direct, mutual)
        };

        let invite_info = sess.active_invite(me).cloned();
        if let Some((_, inviter_name, _)) = invite_info {
            ("invite", inviter_name)
        } else {
            match sess.access {
                AccessMode::InviteOnly => {
                    send(tx, error_op("this world is invite-only")).await;
                    return;
                }
                AccessMode::Friends => {
                    if !direct_friend {
                        send(tx, error_op("you are not friends with that host")).await;
                        return;
                    }
                    ("friend", String::new())
                }
                AccessMode::FriendsOfFriends => {
                    if direct_friend {
                        ("friend", String::new())
                    } else if let Some((_, mutual_name)) = active_mutual {
                        ("friend_of_friend", mutual_name)
                    } else if sess.has_reconnect_grace(me) {
                        ("friend_of_friend", String::new())
                    } else {
                        send(
                            tx,
                            error_op("a mutual friend must currently be in this world to join"),
                        )
                        .await;
                        return;
                    }
                }
            }
        }
    };

    let host_name = resolve_user_name(app, &host).await;
    let joiner_name = resolve_user_name(app, me).await;

    let Some((room_id, host_token, joiner_token)) = app.relay.create_room(app, host, *me) else {
        send(tx, error_op("that host has too many active connections right now")).await;
        return;
    };

    if is_voice {
        {
            let conn = app.db.lock().unwrap();
            db::log_event(&conn, "voice_begin", None);
        }
        send(
            tx,
            json!({
                "op": "voice.ready",
                "host": host.to_string(),
                "roomId": room_id.to_string(),
                "token": joiner_token,
                "relayPath": "/relay/ws"
            })
            .to_string(),
        )
        .await;
        send(
            &host_tx,
            json!({
                "op": "relay.voice",
                "roomId": room_id.to_string(),
                "token": host_token,
                "relayPath": "/relay/ws",
                "joinerUuid": me.to_string(),
                "joinerName": joiner_name,
            })
            .to_string(),
        )
        .await;
        return;
    }

    if let Some(live) = app.sessions.lock().await.get_mut(&host) {
        live.active_joiners.insert(*me);
    }
    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "join_begin", None);
    }

    send(
        tx,
        json!({
            "op": "join.ready",
            "host": host.to_string(),
            "hostName": host_name,
            "worldName": sess.world_name,
            "gameVersion": sess.game_version,
            "roomId": room_id.to_string(),
            "token": joiner_token,
            "relayPath": "/relay/ws"
        })
        .to_string(),
    )
    .await;
    send(
        &host_tx,
        json!({
            "op": "relay.host",
            "roomId": room_id.to_string(),
            "token": host_token,
            "relayPath": "/relay/ws",
            "joinerUuid": me.to_string(),
            "joinerName": joiner_name,
            "joinSource": join_source,
            "viaName": via_name,
        })
        .to_string(),
    )
    .await;
}

async fn presence_update(app: &Arc<App>, me: &Uuid, v: &Value) {
    let clamp_str = |key: &str, max_len: usize| -> String {
        v.get(key)
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .chars()
            .take(max_len)
            .collect()
    };
    let kind = clamp_str("activity", 24);
    let server_name = clamp_str("serverName", 64);
    let server_ip = clamp_str("serverIp", 128);
    let label = clamp_str("label", 64);

    app.activities.lock().await.insert(
        *me,
        ClientActivity { kind, server_name, server_ip, label },
    );
    push_presence_to_friends(app, me).await;
}

async fn invite_request(app: &Arc<App>, me: &Uuid, tx: &Tx, v: &Value) {
    let Some(target) = uuid_field(v) else {
        send(tx, error_op("invite.request requires a uuid")).await;
        return;
    };
    if target == *me {
        return;
    }
    let friends = {
        let conn = app.db.lock().unwrap();
        db::are_friends(&conn, me, &target)
    };
    if !friends {
        send(tx, error_op("you can only request invites from friends")).await;
        return;
    }
    let Some(target_tx) = app.hub.tx(&target).await else {
        send(tx, error_op("that friend is offline")).await;
        return;
    };
    let my_name = match app.hub.name(me).await {
        Some(n) => n,
        None => {
            let conn = app.db.lock().unwrap();
            db::user_name_by_uuid(&conn, me).unwrap_or_else(|| "?".into())
        }
    };
    {
        let conn = app.db.lock().unwrap();
        db::log_event(&conn, "invite_request", None);
    }
    send(
        &target_tx,
        json!({
            "op": "invite.requested",
            "from": me.to_string(),
            "fromName": my_name,
        })
        .to_string(),
    )
    .await;
}

/// Check which of a bounded list of players on the caller's Minecraft server
/// have registered Burrow accounts, so the mod can highlight addable players
/// and grey out non-Burrow players.
async fn users_check(app: &Arc<App>, tx: &Tx, v: &Value) {
    let uuids: Vec<Uuid> = v
        .get("uuids")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .take(128)
                .filter_map(Value::as_str)
                .filter_map(|s| Uuid::parse_str(s).ok())
                .collect()
        })
        .unwrap_or_default();
    let names: Vec<String> = v
        .get("names")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .take(128)
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty() && s.len() <= 32)
                .collect()
        })
        .unwrap_or_default();

    let (found_uuids, found_names) = {
        let conn = app.db.lock().unwrap();
        let mut fu: Vec<String> = Vec::new();
        for u in &uuids {
            if db::user_name_by_uuid(&conn, u).is_some() {
                fu.push(u.to_string());
            }
        }
        let mut fn_: Vec<String> = Vec::new();
        for n in &names {
            if db::user_by_name(&conn, n).is_some() {
                fn_.push(n.to_lowercase());
            }
        }
        (fu, fn_)
    };

    send(
        tx,
        json!({
            "op": "users.check.result",
            "uuids": found_uuids,
            "names": found_names,
        })
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
    let sess = app.sessions.lock().await.get(me).cloned();
    let act = if online {
        app.activities.lock().await.get(me).cloned().unwrap_or_default()
    } else {
        ClientActivity::default()
    };
    for friend in friends {
        if friend == *me {
            continue;
        }
        let visible = sess.as_ref().filter(|s| visible_to_friend(s, &friend));
        let update = json!({
            "op": "presence.batch",
            "updates": [{
                "uuid": me.to_string(),
                "presence": if online { "online" } else { "offline" },
                "hosting": visible.is_some(),
                "worldName": visible.map(|s| s.world_name.as_str()).unwrap_or(""),
                "activity": act.kind,
                "serverName": act.server_name,
                "serverIp": act.server_ip,
                "activityLabel": act.label,
            }]
        })
        .to_string();
        if let Some(tx) = app.hub.tx(&friend).await {
            send(&tx, update).await;
        }
    }
}

async fn push_session_state(app: &Arc<App>, me: &Uuid, prev: Option<&HostedSession>) {
    let curr = app.sessions.lock().await.get(me).cloned();
    let (friends, prev_fof_map, curr_fof_map, host_name) = {
        let conn = app.db.lock().unwrap();
        let friends = db::friend_uuids(&conn, me);
        let prev_fof = match prev {
            Some(p) if p.access == AccessMode::FriendsOfFriends => {
                db::friends_of_friends_via_players(&conn, me, &p.current_players)
            }
            _ => HashMap::new(),
        };
        let curr_fof = match &curr {
            Some(c) if c.access == AccessMode::FriendsOfFriends => {
                db::friends_of_friends_via_players(&conn, me, &c.current_players)
            }
            _ => HashMap::new(),
        };
        let name = db::user_name_by_uuid(&conn, me);
        (friends, prev_fof, curr_fof, name)
    };
    let host_name = match app.hub.name(me).await {
        Some(n) => n,
        None => host_name.unwrap_or_else(|| "?".into()),
    };
    let friend_set: HashSet<Uuid> = friends.iter().copied().collect();

    // Determine who should receive an update: all friends, plus anyone reachable
    // via live FoF, reconnect grace, or invites in either the previous or current state.
    let mut recipients: HashSet<Uuid> = friends.into_iter().collect();
    recipients.extend(prev_fof_map.keys().copied());
    recipients.extend(curr_fof_map.keys().copied());
    if let Some(p) = prev {
        recipients.extend(p.invites.keys().copied());
        recipients.extend(p.grace_joiners.keys().copied());
    }
    if let Some(c) = &curr {
        recipients.extend(c.invites.keys().copied());
        recipients.extend(c.grace_joiners.keys().copied());
    }
    recipients.insert(*me);

    for recipient in recipients {
        let Some(tx) = app.hub.tx(&recipient).await else {
            continue;
        };
        let is_self = recipient == *me;
        let is_friend = friend_set.contains(&recipient);
        let (visible, source, via_name) = match &curr {
            None => (false, "", String::new()),
            Some(s) if is_self => (true, "host", String::new()),
            Some(s) if s.banned.contains(&recipient) => (false, "", String::new()),
            Some(s) if s.active_invite(&recipient).is_some() => (
                true,
                "invite",
                s.active_invite(&recipient).map(|(_, n, _)| n.clone()).unwrap_or_default(),
            ),
            Some(s) => match s.access {
                AccessMode::InviteOnly => (false, "", String::new()),
                AccessMode::Friends => (is_friend, "friend", String::new()),
                AccessMode::FriendsOfFriends => {
                    if is_friend {
                        (true, "friend", String::new())
                    } else if let Some(via) = curr_fof_map.get(&recipient) {
                        (true, "friend_of_friend", via.clone())
                    } else if s.has_reconnect_grace(&recipient) {
                        (true, "friend_of_friend", String::new())
                    } else {
                        (false, "", String::new())
                    }
                }
            },
        };

        let payload = json!({
            "op": "session.state",
            "sessions": [{
                "host": me.to_string(),
                "hostName": host_name,
                "worldName": if visible { curr.as_ref().map(|s| s.world_name.as_str()).unwrap_or("") } else { "" },
                "gameVersion": if visible { curr.as_ref().map(|s| s.game_version.as_str()).unwrap_or("") } else { "" },
                "active": visible,
                "access": if visible { curr.as_ref().map(|s| s.access.as_str()).unwrap_or("") } else { "" },
                "invitePerm": if visible { curr.as_ref().map(|s| s.invite_perm.as_str()).unwrap_or("") } else { "" },
                "source": source,
                "viaName": via_name,
                "locked": if visible { curr.as_ref().map(|s| s.locked).unwrap_or(false) } else { false },
                "playerCount": if visible { curr.as_ref().map(|s| s.player_count).unwrap_or(0) } else { 0 },
                "maxPlayers": if visible { curr.as_ref().map(|s| s.max_players).unwrap_or(0) } else { 0 },
            }]
        })
        .to_string();
        send(&tx, payload).await;
    }
}

fn uuid_field(v: &Value) -> Option<Uuid> {
    v.get("uuid")
        .or_else(|| v.get("host"))
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
}
