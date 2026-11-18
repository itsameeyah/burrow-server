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

/// Per-day series for the dashboard graphs. Counts only, never names.
/// Shape: { "events": [[dayTs...],[count...]], "usersNew": [[...],[...]], "usersTotal": [[...],[...] }
pub async fn history(app: &Arc<App>, days: i64) -> Value {
    let since = db::now() - days.max(1) * 86_400;

    let events;
    let users_new;
    let users_total;
    {
        let conn = app.db.lock().unwrap();
        events = db::events_by_day(&conn, since);
        users_new = db::users_by_day(&conn, since);
        users_total = db::count(&conn, "SELECT COUNT(*) FROM users");
    }

    // Users registered before the window starts, for the cumulative walk.
    let users_before =
        users_total - users_new.iter().map(|(_, c)| *c).sum::<i64>();

    // Merge per-kind rows into one total-per-day series.
    let mut event_days: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    for (day, _kind, count) in &events {
        *event_days.entry(*day).or_insert(0) += *count;
    }
    let mut user_days: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    for (day, count) in &users_new {
        user_days.entry(*day).or_insert(0);
        *user_days.get_mut(day).unwrap() += *count;
    }

    // Cumulative users: start from everyone registered before the window.
    let mut running = users_before;
    let mut total_days: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    for (day, count) in &user_days {
        running += *count;
        total_days.insert(*day, running);
    }

    let events_value = series(&event_days);
    let users_new_value = series(&user_days);
    let users_total_value = series(&total_days);

    json!({
        "events": events_value,
        "usersNew": users_new_value,
        "usersTotal": users_total_value,
    })
}

fn series(map: &std::collections::BTreeMap<i64, i64>) -> Value {
    let xs: Vec<i64> = map.keys().copied().collect();
    let ys: Vec<i64> = map.values().copied().collect();
    json!([xs, ys])
}
