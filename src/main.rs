mod cloudflare_srv;
mod db;
mod dashboard;
mod hub;
mod relay;
mod stats;
mod tcp_ingress;
mod ws;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let bind = std::env::var("BURROW_BIND").unwrap_or_else(|_| "0.0.0.0:8787".into());
    let tcp_bind = std::env::var("BURROW_TCP_BIND")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let tcp_config = tcp_ingress::TcpIngressConfig::from_env();
    let db_path = std::env::var("BURROW_DB").unwrap_or_else(|_| "burrow.db".into());
    let dev_mode = std::env::var("BURROW_DEV_MODE")
        .map(|v| matches!(v.trim(), "1" | "true"))
        .unwrap_or(false);
    let admin_token = std::env::var("BURROW_ADMIN_TOKEN").ok().filter(|s| !s.is_empty());
    let discord_client_id = std::env::var("BURROW_DISCORD_CLIENT_ID").ok().filter(|s| !s.is_empty());
    let discord_client_secret = std::env::var("BURROW_DISCORD_CLIENT_SECRET").ok().filter(|s| !s.is_empty());
    let discord_redirect_uri = std::env::var("BURROW_DISCORD_REDIRECT_URI").ok().filter(|s| !s.is_empty());
    let discord_allowed_user_id = std::env::var("BURROW_DISCORD_ALLOWED_USER_ID").ok().filter(|s| !s.is_empty());
    let mojang_session_url = std::env::var("BURROW_MOJANG_SESSION_URL")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "https://sessionserver.mojang.com".into());
    let latest_mod_version = std::env::var("BURROW_LATEST_MOD_VERSION")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "0.5.0".into());
    let min_mod_version = std::env::var("BURROW_MIN_MOD_VERSION")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "0.5.0".into());
    let download_url = std::env::var("BURROW_DOWNLOAD_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "https://github.com/itsameeyah/burrow/releases/latest".into());

    let conn = db::open(&PathBuf::from(&db_path)).expect("failed to open database");
    let http = reqwest::Client::new();
    let cf_srv = cloudflare_srv::CloudflareSrv::from_env(http.clone(), tcp_config.domain.as_deref());

    if let Some(ref cf) = cf_srv {
        let cf = cf.clone();
        tokio::spawn(async move {
            cf.cleanup_orphans().await;
        });
    }

    let app = Arc::new(ws::App {
        db: Mutex::new(conn),
        hub: hub::Hub::new(),
        relay: relay::Relay::new(),
        sessions: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        activities: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        dev_mode,
        mojang_session_url,
        latest_mod_version,
        min_mod_version,
        download_url,
        admin_token,
        discord_client_id,
        discord_client_secret,
        discord_redirect_uri,
        discord_allowed_user_id,
        oauth_states: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        http,
        cloudflare_srv: cf_srv.clone(),
    });

    if let Some(tb) = tcp_bind {
        tokio::spawn(tcp_ingress::serve(app.clone(), tb, tcp_config));
    }

    let router = axum::Router::new()
        .route("/ws", axum::routing::get(ws::ws_handler))
        .route("/relay/ws", axum::routing::get(relay::relay_handler))
        .route("/version", axum::routing::get(ws::version_handler))
        .route("/admin", axum::routing::get(dashboard::admin_page))
        .route("/admin/login", axum::routing::get(dashboard::oauth_login))
        .route("/admin/callback", axum::routing::get(dashboard::oauth_callback))
        .route("/admin/logout", axum::routing::get(dashboard::logout))
        .route("/api/metrics", axum::routing::get(dashboard::metrics))
        .route("/api/metrics/history", axum::routing::get(dashboard::metrics_history))
        .with_state(app.clone());

    let listener = tokio::net::TcpListener::bind(&bind).await.expect("bind failed");
    tracing::info!("Burrow server listening on {} (dev_mode={})", bind, dev_mode);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");

    if let Some(cf) = cf_srv {
        let _ = tokio::time::timeout(Duration::from_secs(5), cf.cleanup_all()).await;
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("Shutdown signal received, cleaning up...");
}
