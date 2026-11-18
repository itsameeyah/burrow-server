mod db;
mod dashboard;
mod hub;
mod relay;
mod stats;
mod ws;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let bind = std::env::var("BURROW_BIND").unwrap_or_else(|_| "0.0.0.0:8787".into());
    let db_path = std::env::var("BURROW_DB").unwrap_or_else(|_| "burrow.db".into());
    let dev_mode = std::env::var("BURROW_DEV_MODE").map(|v| v != "0").unwrap_or(false);
    let admin_token = std::env::var("BURROW_ADMIN_TOKEN").ok().filter(|s| !s.is_empty());

    let conn = db::open(&PathBuf::from(&db_path)).expect("failed to open database");

    let app = Arc::new(ws::App {
        db: Mutex::new(conn),
        hub: hub::Hub::new(),
        relay: relay::Relay::new(),
        sessions: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        dev_mode,
        admin_token,
    });

    let router = axum::Router::new()
        .route("/ws", axum::routing::get(ws::ws_handler))
        .route("/relay/ws", axum::routing::get(relay::relay_handler))
        .route("/admin", axum::routing::get(dashboard::admin_page))
        .route("/api/metrics", axum::routing::get(dashboard::metrics))
        .with_state(app.clone());

    let listener = tokio::net::TcpListener::bind(&bind).await.expect("bind failed");
    tracing::info!("Burrow server listening on {} (dev_mode={})", bind, dev_mode);
    axum::serve(listener, router).await.expect("server error");
}
