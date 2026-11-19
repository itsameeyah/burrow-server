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
    let relay_rooms = app.relay.active_count();

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
    let today = (db::now() / 86_400) * 86_400;
    let since = today - (days.max(1) - 1) * 86_400;

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
    let mut raw_events: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    for (day, _kind, count) in &events {
        *raw_events.entry(*day).or_insert(0) += *count;
    }
    let mut raw_users: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    for (day, count) in &users_new {
        *raw_users.entry(*day).or_insert(0) += *count;
    }

    // Fill quiet days between the earliest recorded day in the window and
    // today so graphs show continuous timelines (with 0s on quiet days and a
    // flat cumulative line even when all users signed up on one day).
    let mut event_days: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    if let Some(&first_day) = raw_events.keys().next() {
        let mut d = first_day;
        while d <= today {
            event_days.insert(d, *raw_events.get(&d).unwrap_or(&0));
            d += 86_400;
        }
    }

    let mut user_days: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    let mut total_days: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
    let first_user_day = raw_users
        .keys()
        .next()
        .copied()
        .or_else(|| (users_total > 0).then_some(since));
    if let Some(first_day) = first_user_day {
        let mut running = users_before;
        let mut d = first_day;
        while d <= today {
            let added = *raw_users.get(&d).unwrap_or(&0);
            user_days.insert(d, added);
            running += added;
            total_days.insert(d, running);
            d += 86_400;
        }
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
