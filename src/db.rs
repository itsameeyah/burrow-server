//! SQLite storage: users, friendship graph, pending requests, event log.

use rusqlite::Connection;
use std::collections::HashSet;
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
        CREATE TABLE IF NOT EXISTS admin_sessions(
            token TEXT PRIMARY KEY,
            expires_at INTEGER NOT NULL);
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

pub fn upsert_user(conn: &mut Connection, uuid: Uuid, name: &str) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    let uuid_str = uuid.to_string();
    tx.execute(
        "UPDATE users SET name = 'renamed-' || substr(uuid, 1, 8)
         WHERE lower(name) = lower(?1) AND uuid != ?2",
        rusqlite::params![name, &uuid_str],
    )?;
    tx.execute(
        "INSERT INTO users(uuid, name, created_at) VALUES(?1, ?2, ?3)
         ON CONFLICT(uuid) DO UPDATE SET name = excluded.name",
        rusqlite::params![&uuid_str, name, now()],
    )?;
    tx.commit()?;
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
#[allow(dead_code)]
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

/// Returns a direct friend of BOTH `host` and `guest` who is currently in
/// `active_players` (live-presence Friends-of-Friends check: host -> alice -> bob
/// only when alice is currently in the world).
pub fn mutual_friend_in_set(
    conn: &Connection,
    host: &Uuid,
    guest: &Uuid,
    active_players: &HashSet<Uuid>,
) -> Option<(Uuid, String)> {
    if host == guest || active_players.is_empty() {
        return None;
    }
    let Ok(mut stmt) = conn.prepare(
        "SELECT f1.b, u.name \
         FROM friendships f1 \
         JOIN friendships f2 ON f1.b = f2.b \
         JOIN users u ON u.uuid = f1.b \
         WHERE f1.a = ?1 AND f2.a = ?2",
    ) else {
        return None;
    };
    let rows = stmt
        .query_map(
            rusqlite::params![host.to_string(), guest.to_string()],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .ok()?;
    for row in rows.flatten() {
        if let Ok(u) = Uuid::parse_str(&row.0) {
            if active_players.contains(&u) {
                return Some((u, row.1));
            }
        }
    }
    None
}

/// Strictly 2-hop friends-of-friends of `host` (excluding `host` and direct friends)
/// reachable ONLY through a mutual friend who is currently inside `active_players`.
/// Returns `fof_uuid -> active_mutual_friend_name`.
pub fn friends_of_friends_via_players(
    conn: &Connection,
    host: &Uuid,
    active_players: &HashSet<Uuid>,
) -> std::collections::HashMap<Uuid, String> {
    let mut out = std::collections::HashMap::new();
    if active_players.is_empty() {
        return out;
    }
    let direct_friends = friends_with_names(conn, host);
    let direct_set: HashSet<Uuid> = direct_friends.iter().map(|(u, _)| *u).collect();
    for (mutual_uuid, mutual_name) in direct_friends {
        if !active_players.contains(&mutual_uuid) {
            continue;
        }
        for fof_uuid in friend_uuids(conn, &mutual_uuid) {
            if fof_uuid == *host || direct_set.contains(&fof_uuid) {
                continue;
            }
            out.entry(fof_uuid).or_insert_with(|| mutual_name.clone());
        }
    }
    out
}

/// Strictly 2-hop friends-of-friends of `me` (excluding `me` and direct friends).
/// Returns `(fof_uuid, via_mutual_friend_name)`.
#[allow(dead_code)]
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

/// All users reachable from `me` through any chain of friendships (excluding `me`).
#[allow(dead_code)]
pub fn friend_network(conn: &Connection, me: &Uuid) -> HashSet<Uuid> {
    let Ok(mut stmt) = conn.prepare(
        "WITH RECURSIVE net(uuid) AS ( \
             SELECT b FROM friendships WHERE a = ?1 \
             UNION \
             SELECT f.b FROM friendships f JOIN net n ON f.a = n.uuid \
         ) \
         SELECT uuid FROM net WHERE uuid != ?1",
    ) else {
        return HashSet::new();
    };
    let rows = stmt.query_map(rusqlite::params![me.to_string()], |r| r.get::<_, String>(0));
    match rows {
        Ok(iter) => iter
            .filter_map(|r| r.ok())
            .filter_map(|s| Uuid::parse_str(&s).ok())
            .collect(),
        Err(_) => HashSet::new(),
    }
}

/// Returns true iff `b` is reachable from `a` along any chain of friendships.
#[allow(dead_code)]
pub fn in_friend_network(conn: &Connection, a: &Uuid, b: &Uuid) -> bool {
    if a == b {
        return false;
    }
    conn.query_row(
        "WITH RECURSIVE net(uuid) AS ( \
             SELECT b FROM friendships WHERE a = ?1 \
             UNION \
             SELECT f.b FROM friendships f JOIN net n ON f.a = n.uuid \
         ) \
         SELECT 1 FROM net WHERE uuid = ?2 LIMIT 1",
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

pub fn create_admin_session(conn: &Connection, token: &str, expires_at: i64) {
    let _ = conn.execute(
        "DELETE FROM admin_sessions WHERE expires_at <= ?1",
        rusqlite::params![now()],
    );
    let _ = conn.execute(
        "INSERT OR REPLACE INTO admin_sessions(token, expires_at) VALUES(?1, ?2)",
        rusqlite::params![token, expires_at],
    );
}

pub fn is_admin_session_valid(conn: &Connection, token: &str) -> bool {
    let cutoff = now();
    let exp: Option<i64> = conn
        .query_row(
            "SELECT expires_at FROM admin_sessions WHERE token = ?1",
            rusqlite::params![token],
            |r| r.get::<_, i64>(0),
        )
        .ok();
    match exp {
        Some(e) if e > cutoff => true,
        Some(_) => {
            let _ = conn.execute(
                "DELETE FROM admin_sessions WHERE token = ?1",
                rusqlite::params![token],
            );
            false
        }
        None => false,
    }
}

pub fn delete_admin_session(conn: &Connection, token: &str) {
    let _ = conn.execute(
        "DELETE FROM admin_sessions WHERE token = ?1",
        rusqlite::params![token],
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_user_handles_username_rename_case_insensitively() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE users(
                uuid TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                created_at INTEGER NOT NULL);
             CREATE TABLE friendships(
                a TEXT NOT NULL,
                b TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY(a, b));",
        )
        .unwrap();

        let u1 = Uuid::new_v4();
        let u2 = Uuid::new_v4();
        let friend = Uuid::new_v4();

        upsert_user(&mut conn, u1, "Steve").unwrap();
        upsert_user(&mut conn, friend, "Friend").unwrap();
        add_friendship(&conn, &u1, &friend);

        // u2 takes u1's old username with different casing after u1 renamed in MC.
        upsert_user(&mut conn, u2, "steve").unwrap();

        assert_eq!(user_by_name(&conn, "Steve"), Some((u2, "steve".into())));
        let u1_renamed = user_name_by_uuid(&conn, &u1).unwrap();
        assert!(u1_renamed.starts_with("renamed-"));
        // u1's friendship is intact.
        assert!(are_friends(&conn, &u1, &friend));

        // Later u1 signs in with their new Minecraft username.
        upsert_user(&mut conn, u1, "Alex").unwrap();
        assert_eq!(user_by_name(&conn, "Alex"), Some((u1, "Alex".into())));
        assert_eq!(user_by_name(&conn, "steve"), Some((u2, "steve".into())));
    }

    #[test]
    fn friend_network_traverses_chains_and_excludes_strangers() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE friendships(
                a TEXT NOT NULL,
                b TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY(a, b));",
        )
        .unwrap();

        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        let d = Uuid::new_v4();
        let stranger = Uuid::new_v4();

        add_friendship(&conn, &a, &b);
        add_friendship(&conn, &b, &c);
        add_friendship(&conn, &c, &d);
        // Add a cycle back to b to verify termination.
        add_friendship(&conn, &d, &b);

        let net = friend_network(&conn, &a);
        assert!(net.contains(&b));
        assert!(net.contains(&c));
        assert!(net.contains(&d));
        assert!(!net.contains(&a));
        assert!(!net.contains(&stranger));

        assert!(in_friend_network(&conn, &a, &d));
        assert!(!in_friend_network(&conn, &a, &stranger));
    }

    #[test]
    fn live_fof_requires_mutual_friend_in_world() {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE users(uuid TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE COLLATE NOCASE, created_at INTEGER NOT NULL);
             CREATE TABLE friendships(a TEXT NOT NULL, b TEXT NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY(a, b));",
        )
        .unwrap();

        let host = Uuid::new_v4();
        let alice = Uuid::new_v4();
        let bob = Uuid::new_v4();
        let charlie = Uuid::new_v4();
        upsert_user(&mut conn, host, "Host").unwrap();
        upsert_user(&mut conn, alice, "Alice").unwrap();
        upsert_user(&mut conn, bob, "Bob").unwrap();
        upsert_user(&mut conn, charlie, "Charlie").unwrap();

        // host <-> alice <-> bob <-> charlie
        add_friendship(&conn, &host, &alice);
        add_friendship(&conn, &alice, &bob);
        add_friendship(&conn, &bob, &charlie);

        // When only host is in the world, bob cannot join via FoF.
        let mut in_world = HashSet::from([host]);
        assert!(mutual_friend_in_set(&conn, &host, &bob, &in_world).is_none());
        assert!(friends_of_friends_via_players(&conn, &host, &in_world).is_empty());

        // When alice joins, bob CAN see/join via Alice, but charlie STILL cannot!
        in_world.insert(alice);
        assert_eq!(
            mutual_friend_in_set(&conn, &host, &bob, &in_world),
            Some((alice, "Alice".into()))
        );
        let fof = friends_of_friends_via_players(&conn, &host, &in_world);
        assert_eq!(fof.get(&bob).map(String::as_str), Some("Alice"));
        assert!(!fof.contains_key(&charlie));
        assert!(!fof.contains_key(&alice));

        // Even when bob joins, charlie STILL cannot join (strictly host -> alice -> bob, not 3-hop).
        in_world.insert(bob);
        assert!(mutual_friend_in_set(&conn, &host, &charlie, &in_world).is_none());
        let fof2 = friends_of_friends_via_players(&conn, &host, &in_world);
        assert!(!fof2.contains_key(&charlie));

        // When alice leaves, bob is no longer reachable via live FoF.
        in_world.remove(&alice);
        assert!(mutual_friend_in_set(&conn, &host, &bob, &in_world).is_none());
        assert!(friends_of_friends_via_players(&conn, &host, &in_world).is_empty());
    }
}
