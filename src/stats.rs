//! Aggregated, anonymised statistics. Counts only — never a name, uuid or
//! world name — so any screenshot of the dashboard is safe to share.

use crate::db;
use crate::ws::App;
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn snapshot(app: &Arc<App>) -> Value {
    let users_total;
    let friendships_total;
    let events_total;
    let events_24h;
    {
        let conn = app.db.lock().unwrap();
        users_total = db::count(&conn, "SELECT COUNT(*) FROM users");
        friendships_total = db::count(&conn, "SELECT COUNT(*) FROM friendships") / 2;
        events_total = db::count(&conn, "SELECT COUNT(*) FROM events");
        events_24h = db::count_where_ts(&conn, None, db::now() - 86_400);
    }
    let online_now = app.hub.len().await;
    let active_sessions = app.sessions.lock().await.len();
    let relay_rooms = app.relay.rooms.lock().unwrap().len();

    json!({
        "users_total": users_total,
        "friendships_total": friendships_total,
        "online_now": online_now,
        "active_sessions": active_sessions,
        "relay_rooms": relay_rooms,
        "events_total": events_total,
        "events_24h": events_24h,
    })
}
