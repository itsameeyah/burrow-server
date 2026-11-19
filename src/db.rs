//! SQLite storage: users, friendship graph, pending requests, event log.

use rusqlite::Connection;
use std::path::Path;
use uuid::Uuid;

pub fn open(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;
         CREATE TABLE IF NOT EXISTS users(
            uuid TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE COLLATE NOCASE,
            created_at INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS friendships(
            a TEXT NOT NULL,
            b TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY(a, b));
        CREATE TABLE IF NOT EXISTS requests(
            sender TEXT NOT NULL,
            receiver TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY(sender, receiver));
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            ts INTEGER NOT NULL,
            data TEXT);
        CREATE INDEX IF NOT EXISTS idx_requests_receiver ON requests(receiver);
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
        ",
    )?;
    Ok(conn)
}

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub fn upsert_user(conn: &Connection, uuid: Uuid, name: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO users(uuid, name, created_at) VALUES(?1, ?2, ?3)
         ON CONFLICT(uuid) DO UPDATE SET name = excluded.name",
        rusqlite::params![uuid.to_string(), name, now()],
    )?;
    Ok(())
}

pub fn user_by_name(conn: &Connection, name: &str) -> Option<(Uuid, String)> {
    let row: Option<(String, String)> = conn
        .query_row(
            "SELECT uuid, name FROM users WHERE name = ?1",
            rusqlite::params![name],
            |r| {
                let uuid_str: String = r.get(0)?;
                let user_name: String = r.get(1)?;
                Ok((uuid_str, user_name))
            },
        )
        .ok();
    row.and_then(|(s, n)| Uuid::parse_str(&s).ok().map(|u| (u, n)))
}

/// Friendships are stored as two directed rows so every lookup is a single
/// indexed select.
pub fn friend_uuids(conn: &Connection, me: &Uuid) -> Vec<Uuid> {
    let Ok(mut stmt) = conn.prepare("SELECT b FROM friendships WHERE a = ?1") else {
        return Vec::new();
    };
    let rows = stmt.query_map(rusqlite::params![me.to_string()], |r| {
        let s: String = r.get(0)?;
        Ok(s)
    });
    match rows {
        Ok(iter) => iter
            .filter_map(|r| r.ok())
            .filter_map(|s| Uuid::parse_str(&s).ok())
            .collect(),
        Err(_) => Vec::new(),
    }
}

pub fn add_friendship(conn: &Connection, a: &Uuid, b: &Uuid) {
    let _ = conn.execute(
        "INSERT OR IGNORE INTO friendships(a, b, created_at) VALUES(?1, ?2, ?3)",
        rusqlite::params![a.to_string(), b.to_string(), now()],
    );
    let _ = conn.execute(
        "INSERT OR IGNORE INTO friendships(a, b, created_at) VALUES(?1, ?2, ?3)",
        rusqlite::params![b.to_string(), a.to_string(), now()],
    );
}

pub fn remove_friendship(conn: &Connection, a: &Uuid, b: &Uuid) {
    let _ = conn.execute(
        "DELETE FROM friendships WHERE a = ?1 AND b = ?2",
        rusqlite::params![a.to_string(), b.to_string()],
    );
    let _ = conn.execute(
        "DELETE FROM friendships WHERE a = ?1 AND b = ?2",
        rusqlite::params![b.to_string(), a.to_string()],
    );
}

pub fn are_friends(conn: &Connection, a: &Uuid, b: &Uuid) -> bool {
    conn.query_row(
        "SELECT 1 FROM friendships WHERE a = ?1 AND b = ?2",
        rusqlite::params![a.to_string(), b.to_string()],
        |_| Ok(()),
    )
    .is_ok()
}

/// Look up a user's stored display name by UUID.
pub fn user_name_by_uuid(conn: &Connection, uuid: &Uuid) -> Option<String> {
    conn.query_row(
        "SELECT name FROM users WHERE uuid = ?1",
        rusqlite::params![uuid.to_string()],
        |r| r.get::<_, String>(0),
    )
    .ok()
}

/// Strictly 1-hop mutual friend between `a` and `b` (i.e. `a` is friends with M
/// and M is friends with `b`). Returns `(M.uuid, M.name)` if one exists.
pub fn mutual_friend(conn: &Connection, a: &Uuid, b: &Uuid) -> Option<(Uuid, String)> {
    let row: Option<(String, String)> = conn
        .query_row(
            "SELECT f1.b, u.name \
             FROM friendships f1 \
             JOIN friendships f2 ON f1.b = f2.a \
             JOIN users u ON u.uuid = f1.b \
             WHERE f1.a = ?1 AND f2.b = ?2 \
             LIMIT 1",
            rusqlite::params![a.to_string(), b.to_string()],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .ok();
    row.and_then(|(s, name)| Uuid::parse_str(&s).ok().map(|u| (u, name)))
}

/// Strictly 2-hop friends-of-friends of `me` (excluding `me` and direct friends).
/// Returns `(fof_uuid, via_mutual_friend_name)`.
pub fn friends_of_friends(conn: &Connection, me: &Uuid) -> Vec<(Uuid, String)> {
    request_rows(
        conn,
        "SELECT f2.b, u.name \
         FROM friendships f1 \
         JOIN friendships f2 ON f1.b = f2.a \
         JOIN users u ON u.uuid = f1.b \
         WHERE f1.a = ?1 \
           AND f2.b != ?1 \
           AND NOT EXISTS ( \
               SELECT 1 FROM friendships direct WHERE direct.a = ?1 AND direct.b = f2.b \
           ) \
         GROUP BY f2.b",
        me,
    )
}

pub fn add_request(conn: &Connection, sender: &Uuid, receiver: &Uuid) -> bool {
    conn.execute(
        "INSERT OR IGNORE INTO requests(sender, receiver, created_at) VALUES(?1, ?2, ?3)",
        rusqlite::params![sender.to_string(), receiver.to_string(), now()],
    )
    .map(|changed| changed == 1)
    .unwrap_or(false)
}

pub fn request_exists(conn: &Connection, sender: &Uuid, receiver: &Uuid) -> bool {
    conn.query_row(
        "SELECT 1 FROM requests WHERE sender = ?1 AND receiver = ?2",
        rusqlite::params![sender.to_string(), receiver.to_string()],
        |_| Ok(()),
    )
    .is_ok()
}

pub fn delete_request(conn: &Connection, sender: &Uuid, receiver: &Uuid) {
    let _ = conn.execute(
        "DELETE FROM requests WHERE sender = ?1 AND receiver = ?2",
        rusqlite::params![sender.to_string(), receiver.to_string()],
    );
}

/// (uuid, name) pairs for all friends of `me` in a single indexed join.
pub fn friends_with_names(conn: &Connection, me: &Uuid) -> Vec<(Uuid, String)> {
    request_rows(
        conn,
        "SELECT f.b, u.name FROM friendships f JOIN users u ON u.uuid = f.b WHERE f.a = ?1",
        me,
    )
}

/// (sender, name) pairs for requests addressed to me.
pub fn incoming_requests(conn: &Connection, me: &Uuid) -> Vec<(Uuid, String)> {
    request_rows(conn, "SELECT r.sender, u.name FROM requests r JOIN users u ON u.uuid = r.sender WHERE r.receiver = ?1", me)
}

/// (receiver, name) pairs for requests I have sent.
pub fn outgoing_requests(conn: &Connection, me: &Uuid) -> Vec<(Uuid, String)> {
    request_rows(conn, "SELECT r.receiver, u.name FROM requests r JOIN users u ON u.uuid = r.receiver WHERE r.sender = ?1", me)
}

fn request_rows(conn: &Connection, sql: &str, me: &Uuid) -> Vec<(Uuid, String)> {
    let Ok(mut stmt) = conn.prepare(sql) else {
        return Vec::new();
    };
    let rows = stmt.query_map(rusqlite::params![me.to_string()], |r| {
        let s: String = r.get(0)?;
        let name: String = r.get(1)?;
        Ok((s, name))
    });
    match rows {
        Ok(iter) => iter
            .filter_map(|r| r.ok())
            .filter_map(|(s, name)| Uuid::parse_str(&s).ok().map(|u| (u, name)))
            .collect(),
        Err(_) => Vec::new(),
    }
}

pub fn log_event(conn: &Connection, kind: &str, data: Option<&str>) {
    let _ = conn.execute(
        "INSERT INTO events(kind, ts, data) VALUES(?1, ?2, ?3)",
        rusqlite::params![kind, now(), data],
    );
}

pub fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get::<_, i64>(0))
        .unwrap_or(0)
}

pub fn count_where_ts(conn: &Connection, kind_filter: Option<&str>, since: i64) -> i64 {
    match kind_filter {
        Some(kind) => conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind = ?1 AND ts >= ?2",
                rusqlite::params![kind, since],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0),
        None => conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE ts >= ?1",
                rusqlite::params![since],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0),
    }
}

/// (midnight-ts, kind, count) for every logged event since `since`.
pub fn events_by_day(conn: &Connection, since: i64) -> Vec<(i64, String, i64)> {
    let Ok(mut stmt) = conn.prepare(
        "SELECT (ts / 86400) * 86400 AS day, kind, COUNT(*) \
         FROM events WHERE ts >= ?1 GROUP BY day, kind ORDER BY day",
    ) else {
        return Vec::new();
    };
    let rows = stmt.query_map(rusqlite::params![since], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))
    });
    match rows {
        Ok(iter) => iter.filter_map(|r| r.ok()).collect(),
        Err(_) => Vec::new(),
    }
}

/// (midnight-ts, new-users-that-day) since `since`.
pub fn users_by_day(conn: &Connection, since: i64) -> Vec<(i64, i64)> {
    let Ok(mut stmt) = conn.prepare(
        "SELECT (created_at / 86400) * 86400 AS day, COUNT(*) \
         FROM users WHERE created_at >= ?1 GROUP BY day ORDER BY day",
    ) else {
        return Vec::new();
    };
    let rows = stmt.query_map(rusqlite::params![since], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
    });
    match rows {
        Ok(iter) => iter.filter_map(|r| r.ok()).collect(),
        Err(_) => Vec::new(),
    }
}
