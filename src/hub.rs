//! Online-client registry. Each connected client owns an mpsc sender that
//! the signaling loop uses to push JSON messages to its websocket.

use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone)]
pub struct HubClient {
    pub name: String,
    pub tx: mpsc::Sender<String>,
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

    pub async fn remove(&self, uuid: &Uuid) -> Option<HubClient> {
        self.inner.write().await.remove(uuid)
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
