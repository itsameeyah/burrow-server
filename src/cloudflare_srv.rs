//! Optional Cloudflare DNS SRV manager for running vanilla Burrow join codes
//! (`<CODE>.burrow.meeyah.dev`) on a non-25565 port without players having to
//! type `:port` at the end.
//!
//! Because DNS wildcards (`*`) are only valid as the leftmost label, a static
//! `_minecraft._tcp.*.burrow.meeyah.dev` SRV record cannot match arbitrary
//! 6-character codes. When configured (`BURROW_CF_API_TOKEN`,
//! `BURROW_CF_ZONE_ID`, `BURROW_CF_SRV_PORT`), Burrow dynamically creates:
//!
//! ```text
//! _minecraft._tcp.<code>.<domain>  SRV  0 5 <port> <code>.<target_domain>
//! ```
//!
//! Pointing the SRV `target` at `<code>.<target_domain>` (which resolves via
//! the static `*.<target_domain>` A record) preserves the join code in
//! Minecraft's Handshake `server_address` field after SRV resolution.
//!
//! All created records are tagged with comment `"burrow-auto-srv"`, deleted
//! immediately when a code is regenerated/disabled or the host stops/disconnects,
//! swept on server startup (in case of a prior crash), and cleaned up on
//! graceful shutdown.

use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

const CF_API_BASE: &str = "https://api.cloudflare.com/client/v4";
const RECORD_COMMENT: &str = "burrow-auto-srv";
const RECORD_TTL: u32 = 60;

#[derive(Default)]
struct State {
    /// Codes that should currently have an active SRV record (uppercase).
    active_codes: HashSet<String>,
    /// Uppercase code -> Cloudflare DNS record ID.
    record_ids: HashMap<String, String>,
}

pub struct CloudflareSrv {
    http: reqwest::Client,
    api_token: String,
    zone_id: String,
    /// Domain under which SRV records live (e.g. `burrow.meeyah.dev`).
    domain: String,
    /// Target domain suffix used in SRV target (`<code>.<target_domain>`).
    target_domain: String,
    /// Public TCP port advertised in the SRV record (e.g. `25570`).
    port: u16,
    state: Mutex<State>,
}

impl CloudflareSrv {
    /// Build from environment variables. Returns `None` unless `BURROW_CF_API_TOKEN`,
    /// `BURROW_CF_ZONE_ID`, and a non-zero `BURROW_CF_SRV_PORT` are all set.
    pub fn from_env(http: reqwest::Client, tcp_domain: Option<&str>) -> Option<Arc<Self>> {
        let api_token = std::env::var("BURROW_CF_API_TOKEN")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        let zone_id = std::env::var("BURROW_CF_ZONE_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        let port = std::env::var("BURROW_CF_SRV_PORT")
            .ok()
            .and_then(|s| s.trim().parse::<u16>().ok())
            .filter(|&p| p > 0)?;

        let domain = std::env::var("BURROW_CF_SRV_DOMAIN")
            .ok()
            .map(|s| normalize_domain(&s))
            .filter(|s| !s.is_empty())
            .or_else(|| tcp_domain.map(normalize_domain).filter(|s| !s.is_empty()))
            .unwrap_or_else(|| "burrow.meeyah.dev".into());

        let target_domain = std::env::var("BURROW_CF_SRV_TARGET_DOMAIN")
            .ok()
            .map(|s| normalize_domain(&s))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| domain.clone());

        tracing::info!(
            "Cloudflare dynamic SRV enabled: _minecraft._tcp.<CODE>.{} -> <CODE>.{}:{}",
            domain,
            target_domain,
            port
        );

        Some(Arc::new(Self {
            http,
            api_token,
            zone_id,
            domain,
            target_domain,
            port,
            state: Mutex::new(State::default()),
        }))
    }

    pub fn srv_record_name(&self, code: &str) -> String {
        format!("_minecraft._tcp.{}.{}", code.to_ascii_lowercase(), self.domain)
    }

    pub fn srv_target(&self, code: &str) -> String {
        format!("{}.{}", code.to_ascii_lowercase(), self.target_domain)
    }

    fn build_create_payload(&self, code: &str) -> Value {
        json!({
            "type": "SRV",
            "name": self.srv_record_name(code),
            "ttl": RECORD_TTL,
            "comment": RECORD_COMMENT,
            "data": {
                "priority": 0,
                "weight": 5,
                "port": self.port,
                "target": self.srv_target(code),
            }
        })
    }

    /// Fire-and-forget transition whenever a session's join code changes.
    pub fn spawn_sync(
        self: &Arc<Self>,
        old_code: Option<String>,
        new_code: Option<String>,
    ) {
        if old_code == new_code {
            return;
        }
        if let Some(old) = old_code {
            let mgr = self.clone();
            tokio::spawn(async move {
                mgr.remove_code(&old).await;
            });
        }
        if let Some(new) = new_code {
            let mgr = self.clone();
            tokio::spawn(async move {
                mgr.ensure_code(&new).await;
            });
        }
    }

    /// Sweep any orphaned `burrow-auto-srv` records left in Cloudflare from a prior crash.
    pub async fn cleanup_orphans(&self) {
        let url = format!(
            "{}/zones/{}/dns_records?type=SRV&comment.exact={}&per_page=100",
            CF_API_BASE, self.zone_id, RECORD_COMMENT
        );
        let resp = match self
            .http
            .get(&url)
            .bearer_auth(&self.api_token)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Cloudflare SRV orphan list failed: {}", e);
                return;
            }
        };
        let body: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("Cloudflare SRV orphan list parse failed: {}", e);
                return;
            }
        };
        let Some(results) = body.get("result").and_then(Value::as_array) else {
            return;
        };
        let suffix = format!(".{}", self.domain);
        let mut removed = 0usize;
        for rec in results {
            let id = rec.get("id").and_then(Value::as_str).unwrap_or("");
            let name = rec
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_ascii_lowercase();
            if !id.is_empty() && name.starts_with("_minecraft._tcp.") && name.ends_with(&suffix) {
                if self.delete_record_by_id(id).await {
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            tracing::info!("Cleaned up {} orphaned Cloudflare SRV record(s)", removed);
        }
    }

    /// Create an SRV record for `code` (if not already tracked).
    pub async fn ensure_code(&self, raw_code: &str) {
        let code = raw_code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return;
        }
        {
            let mut st = self.state.lock().await;
            st.active_codes.insert(code.clone());
            if st.record_ids.contains_key(&code) {
                return;
            }
        }

        let url = format!("{}/zones/{}/dns_records", CF_API_BASE, self.zone_id);
        let payload = self.build_create_payload(&code);
        let resp = match self
            .http
            .post(&url)
            .bearer_auth(&self.api_token)
            .json(&payload)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Cloudflare SRV create failed for {}: {}", code, e);
                return;
            }
        };

        let status = resp.status();
        let body: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("Cloudflare SRV create parse failed for {} ({}): {}", code, status, e);
                return;
            }
        };

        let ok = body.get("success").and_then(Value::as_bool).unwrap_or(false);
        let record_id = body
            .get("result")
            .and_then(|r| r.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string);

        if !ok || record_id.is_none() {
            tracing::warn!(
                "Cloudflare SRV create rejected for {} ({}): {}",
                code,
                status,
                body
            );
            return;
        }
        let id = record_id.unwrap();

        let still_active = {
            let mut st = self.state.lock().await;
            if st.active_codes.contains(&code) {
                st.record_ids.insert(code.clone(), id.clone());
                true
            } else {
                false
            }
        };

        if still_active {
            tracing::info!(
                "Created Cloudflare SRV {} -> {}:{} (id={})",
                self.srv_record_name(&code),
                self.srv_target(&code),
                self.port,
                id
            );
        } else {
            // The code was disabled/regenerated while the POST was in flight; clean it up.
            let _ = self.delete_record_by_id(&id).await;
        }
    }

    /// Delete the SRV record for `code`.
    pub async fn remove_code(&self, raw_code: &str) {
        let code = raw_code.trim().to_ascii_uppercase();
        if code.is_empty() {
            return;
        }
        let existing_id = {
            let mut st = self.state.lock().await;
            st.active_codes.remove(&code);
            st.record_ids.remove(&code)
        };
        if let Some(id) = existing_id {
            if self.delete_record_by_id(&id).await {
                tracing::info!("Deleted Cloudflare SRV for {} (id={})", code, id);
            }
        }
    }

    /// Delete all active SRV records (called during graceful shutdown).
    pub async fn cleanup_all(&self) {
        let ids: Vec<(String, String)> = {
            let mut st = self.state.lock().await;
            st.active_codes.clear();
            st.record_ids.drain().collect()
        };
        for (code, id) in ids {
            if self.delete_record_by_id(&id).await {
                tracing::info!("Shutdown cleaned up Cloudflare SRV for {} (id={})", code, id);
            }
        }
    }

    async fn delete_record_by_id(&self, id: &str) -> bool {
        let url = format!("{}/zones/{}/dns_records/{}", CF_API_BASE, self.zone_id, id);
        match self
            .http
            .delete(&url)
            .bearer_auth(&self.api_token)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => true,
            Ok(r) => {
                let status = r.status();
                let txt = r.text().await.unwrap_or_default();
                tracing::warn!("Cloudflare SRV delete {} failed ({}): {}", id, status, txt);
                false
            }
            Err(e) => {
                tracing::warn!("Cloudflare SRV delete {} error: {}", id, e);
                false
            }
        }
    }
}

pub fn normalize_domain(raw: &str) -> String {
    raw.trim().trim_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_srv_payload_preserving_code_in_target() {
        let mgr = CloudflareSrv {
            http: reqwest::Client::new(),
            api_token: "test".into(),
            zone_id: "zone".into(),
            domain: "burrow.meeyah.dev".into(),
            target_domain: "burrow.meeyah.dev".into(),
            port: 25570,
            state: Mutex::new(State::default()),
        };
        let payload = mgr.build_create_payload("ABC123");
        assert_eq!(payload["type"], "SRV");
        assert_eq!(payload["name"], "_minecraft._tcp.abc123.burrow.meeyah.dev");
        assert_eq!(payload["comment"], "burrow-auto-srv");
        assert_eq!(payload["data"]["port"], 25570);
        assert_eq!(payload["data"]["target"], "abc123.burrow.meeyah.dev");
    }
}
