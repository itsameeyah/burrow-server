//! Placeholder dashboard. Until M3 lands the Discord OAuth gate and the
//! uPlot graphs, both /admin and /api/metrics return 404 unless the server
//! was started with BURROW_ADMIN_TOKEN set — i.e. the dashboard is simply
//! not published, exactly as requested.

use crate::{stats, ws::App};
use axum::extract::State;
use axum::http::{header::AUTHORIZATION, HeaderMap};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};
use std::sync::Arc;

fn authorized(app: &App, headers: &HeaderMap) -> bool {
    let Some(expected) = app.admin_token.as_ref() else {
        return false;
    };
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case(&format!("Bearer {expected}")))
        .unwrap_or(false)
}

pub async fn admin_page(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !authorized(&app, &headers) {
        return StatusCode::NOT_FOUND.into_response();
    }
    Html(
        r#"<!doctype html>
<html><head><meta charset="utf-8"><title>Burrow</title></head>
<body style="font-family: sans-serif; background: #1a1a1e; color: #eee; display: grid; place-items: center; min-height: 100vh; margin: 0">
  <div style="text-align: center">
    <h1>🐇 Burrow</h1>
    <p>Mission control lives here.</p>
    <p style="color: #999">The real dashboard (graphs, live tiles, Discord sign-in) arrives in milestone 3.<br/>Metrics JSON for now: <code>GET /api/metrics</code> with the same bearer token.</p>
  </div>
</body></html>"#,
    )
    .into_response()
}

pub async fn metrics(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !authorized(&app, &headers) {
        return StatusCode::NOT_FOUND.into_response();
    }
    Json(stats::snapshot(&app).await).into_response()
}
