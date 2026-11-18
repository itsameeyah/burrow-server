//! Online-client registry. Each connected client owns an mpsc sender that
//! the signaling loop uses to push JSON messages to its websocket.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone)]
pub struct HubClient {
    pub name: String,
    pub tx: mpsc::Sender<String>,
    /// Generation id: a reconnecting client replaces its own hub entry, and
    /// the old connection's cleanup can only remove its own generation.
    pub conn_id: u64,
}

fn next_conn_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

#[derive(Default)]
pub struct Hub {
    inner: tokio::sync::RwLock<HashMap<Uuid, HubClient>>,
}

impl Hub {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, uuid: Uuid, client: HubClient) {
        self.inner.write().await.insert(uuid, client);
    }

    /// Remove our own entry — only succeeds if the registered connection is
    /// still ours (a newer reconnect must not be evicted by stale cleanup).
    pub async fn remove_owned(&self, uuid: &Uuid, conn_id: u64) -> Option<HubClient> {
        let mut guard = self.inner.write().await;
        match guard.get_mut(uuid) {
            Some(client) if client.conn_id == conn_id => guard.remove(uuid),
            _ => None,
        }
    }

    pub async fn tx(&self, uuid: &Uuid) -> Option<mpsc::Sender<String>> {
        self.inner.read().await.get(uuid).map(|c| c.tx.clone())
    }

    pub async fn is_online(&self, uuid: &Uuid) -> bool {
        self.inner.read().await.contains_key(uuid)
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }
}

/// Mint a connection generation id before hello registers the client.
pub fn mint_conn_id() -> u64 {
    next_conn_id()
}
