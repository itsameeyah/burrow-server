//! SQLite storage: users, friendship graph, pending requests, event log.

use rusqlite::Connection;
use std::path::Path;
use uuid::Uuid;

pub fn open(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS users(
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

pub fn user_by_uuid(conn: &Connection, uuid: &Uuid) -> Option<(Uuid, String)> {
    let row: Option<(String, String)> = conn
        .query_row(
            "SELECT uuid, name FROM users WHERE uuid = ?1",
            rusqlite::params![uuid.to_string()],
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
