//! The Burrow dashboard: Discord-OAuth gated, meeyah.dev-styled, counts-only.
//!
//! Auth model: a browser either presents a valid `burrow_session` cookie
//! (issued after a Discord sign-in whose user id matches the allowlist) or
//! the static `BURROW_ADMIN_TOKEN` bearer. Without either, /admin is a
//! redirect or a setup page and /api/* are 404 — the dashboard simply does
//! not exist to anyone else.

use crate::{stats, ws::App};
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Json, Redirect, Response};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const DASHBOARD_TEMPLATE: &str = include_str!("assets/dashboard.html");
const SETUP_HTML: &str = include_str!("assets/setup.html");
const UPLOT_JS: &str = include_str!("assets/uplot.min.js");
const UPLOT_CSS: &str = include_str!("assets/uplot.min.css");

const SESSION_TTL: i64 = 86_400; // dashboard sessions last a day
const STATE_TTL: i64 = 600; // oauth round-trip window: 10 minutes

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn rendered_dashboard() -> &'static str {
    static RENDERED: OnceLock<String> = OnceLock::new();
    RENDERED.get_or_init(|| {
        DASHBOARD_TEMPLATE
            .replace("{{UPLOT_CSS}}", &format!("<style>{}</style>", UPLOT_CSS))
            .replace("{{UPLOT_JS}}", UPLOT_JS)
    })
}

fn discord_configured(app: &App) -> bool {
    app.discord_client_id.is_some()
        && app.discord_client_secret.is_some()
        && app.discord_redirect_uri.is_some()
        && app.discord_allowed_user_id.is_some()
}

fn bearer_ok(app: &App, headers: &HeaderMap) -> bool {
    let Some(expected) = app.admin_token.as_ref() else {
        return false;
    };
    // The auth *scheme* is case-insensitive per HTTP, but the token itself
    // must match exactly.
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    match value.split_once(' ') {
        Some((scheme, token)) => scheme.eq_ignore_ascii_case("bearer") && token == expected,
        None => false,
    }
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookie = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in cookie.split(';') {
        let part = part.trim();
        if let Some((k, v)) = part.split_once('=') {
            if k == name {
                return Some(v.to_string());
            }
        }
    }
    None
}

async fn dash_authed(app: &App, headers: &HeaderMap) -> bool {
    if bearer_ok(app, headers) {
        return true;
    }
    let Some(token) = cookie_value(headers, "burrow_session") else {
        return false;
    };
    let mut sessions = app.dash_sessions.lock().await;
    // Cheap opportunistic sweep so abandoned sessions can't accumulate.
    if sessions.len() > 64 {
        let cutoff = now();
        sessions.retain(|_, exp| *exp > cutoff);
    }
    match sessions.get(&token) {
        Some(expiry) if *expiry > now() => true,
        Some(_) => {
            sessions.remove(&token);
            false
        }
        None => false,
    }
}

fn not_found() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

/// GET /admin — the gate itself.
pub async fn admin_page(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if dash_authed(&app, &headers).await {
        return Html(rendered_dashboard()).into_response();
    }
    if !discord_configured(&app) {
        // Setup instructions are fine on a bare LAN dev server, but if an
        // admin token policy exists, strangers get nothing at all.
        if app.admin_token.is_some() && !bearer_ok(&app, &headers) {
            return not_found();
        }
        return Html(SETUP_HTML).into_response();
    }

    // Start the OAuth round-trip: random state, remember it briefly.
    let state = uuid::Uuid::new_v4().to_string();
    {
        let mut states = app.oauth_states.lock().await;
        if states.len() >= 64 {
            let cutoff = now();
            states.retain(|_, exp| *exp > cutoff);
        }
        if states.len() >= 256 {
            return StatusCode::TOO_MANY_REQUESTS.into_response();
        }
        states.insert(state.clone(), now() + STATE_TTL);
    }
    let client_id = app.discord_client_id.clone().unwrap_or_default();
    let redirect = urlencoding_redirect(&app.discord_redirect_uri.clone().unwrap_or_default());
    let authorize = format!(
        "https://discord.com/oauth2/authorize?client_id={}&redirect_uri={}&response_type=code&scope=identify&state={}",
        client_id, redirect, state
    );
    Redirect::to(&authorize).into_response()
}

fn urlencoding_redirect(uri: &str) -> String {
    let mut out = String::with_capacity(uri.len());
    for b in uri.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                use std::fmt::Write;
                let _ = write!(out, "%{:02X}", b);
            }
        }
    }
    out
}

/// GET /admin/callback?code=..&state=.. — finish the OAuth round-trip.
pub async fn oauth_callback(
    State(app): State<Arc<App>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !discord_configured(&app) {
        return not_found();
    }
    let (Some(code), Some(state)) = (params.get("code"), params.get("state")) else {
        return Html("<h3>missing code/state 🐇</h3>").into_response();
    };

    // Consume the CSRF state.
    {
        let mut states = app.oauth_states.lock().await;
        let valid = match states.get(state) {
            Some(expiry) if *expiry > now() => true,
            _ => false,
        };
        states.remove(state);
        if !valid {
            return Html("<h3>login window expired — try again</h3>").into_response();
        }
    }

    // Exchange the code with Discord.
    let client_id = app.discord_client_id.clone().unwrap_or_default();
    let client_secret = app.discord_client_secret.clone().unwrap_or_default();
    let redirect_uri = app.discord_redirect_uri.clone().unwrap_or_default();
    let params_ex = [
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
    ];
    let token_res = app
        .http
        .post("https://discord.com/api/oauth2/token")
        .form(&params_ex)
        .send()
        .await;
    let Ok(token_res) = token_res else {
        return Html("<h3>could not reach Discord</h3>").into_response();
    };
    if !token_res.status().is_success() {
        tracing::warn!("discord token exchange failed: {}", token_res.status());
        return Html("<h3>Discord rejected the login</h3>").into_response();
    }
    let Ok(token_json) = token_res.json::<Value>().await else {
        return Html("<h3>unexpected response from Discord</h3>").into_response();
    };
    let access_token = token_json
        .get("access_token")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if access_token.is_empty() {
        return Html("<h3>Discord did not return an access token</h3>").into_response();
    }

    // Who is signing in?
    let me_res = app
        .http
        .get("https://discord.com/api/users/@me")
        .bearer_auth(&access_token)
        .send()
        .await;
    let Ok(me_res) = me_res else {
        return Html("<h3>could not read your Discord profile</h3>").into_response();
    };
    if !me_res.status().is_success() {
        tracing::warn!("discord @me request failed: {}", me_res.status());
        return Html("<h3>Discord rejected the profile lookup</h3>").into_response();
    }
    let Ok(me) = me_res.json::<Value>().await else {
        return Html("<h3>unexpected profile response</h3>").into_response();
    };
    let user_id = me
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    // THE allowlist: exactly one account may ever pass this gate.
    if user_id.is_empty() || user_id != app.discord_allowed_user_id.clone().unwrap_or_default() {
        tracing::warn!("dashboard login attempt by discord id {}", user_id);
        return Html("<h3>this dashboard isn't for you 🐇</h3>")
            .into_response();
    }

    // Issue a session cookie. SameSite always; Secure when the operator
    // tells us we're behind TLS (the tunnel phase sets this).
    let token = uuid::Uuid::new_v4().to_string();
    app.dash_sessions
        .lock()
        .await
        .insert(token.clone(), now() + SESSION_TTL);
    let secure = std::env::var("BURROW_SECURE_COOKIES")
        .map(|v| matches!(v.trim(), "1" | "true"))
        .unwrap_or(false);
    let mut cookie = format!(
        "burrow_session={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
        token, SESSION_TTL
    );
    if secure {
        cookie.push_str("; Secure");
    }
    let mut headers_out = HeaderMap::new();
    let _ = headers_out.insert(
        header::SET_COOKIE,
        axum::http::HeaderValue::from_str(&cookie).expect("valid cookie header"),
    );
    let _ = headers_out.insert(header::LOCATION, axum::http::HeaderValue::from_static("/admin"));
    (StatusCode::FOUND, headers_out).into_response()
}

/// GET /api/metrics — counts-only live tiles.
pub async fn metrics(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !dash_authed(&app, &headers).await {
        return not_found();
    }
    Json(stats::snapshot(&app).await).into_response()
}

/// GET /api/metrics/history?days=30 — per-day chart series.
pub async fn metrics_history(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !dash_authed(&app, &headers).await {
        return not_found();
    }
    let days = params
        .get("days")
        .and_then(|d| d.parse::<i64>().ok())
        .unwrap_or(30)
        .clamp(1, 365);
    Json(stats::history(&app, days).await).into_response()
}
