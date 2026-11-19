//! The Burrow dashboard: Discord-OAuth gated, meeyah.dev-styled, counts-only.
//!
//! Auth model: a browser either presents a valid `burrow_session` cookie
//! (issued after a Discord sign-in whose user id matches the allowlist) or
//! the static `BURROW_ADMIN_TOKEN` bearer. Without either, /admin is a
//! redirect or a setup page and /api/* are 404 — the dashboard simply does
//! not exist to anyone else.

use crate::{db, stats, ws::App};
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Json, Redirect, Response};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const DASHBOARD_TEMPLATE: &str = include_str!("assets/dashboard.html");
const LOGIN_HTML: &str = include_str!("assets/login.html");
const SETUP_HTML: &str = include_str!("assets/setup.html");
const UPLOT_JS: &str = include_str!("assets/uplot.min.js");
const UPLOT_CSS: &str = include_str!("assets/uplot.min.css");

const SESSION_TTL: i64 = 30 * 86_400; // persistent dashboard sessions last 30 days
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
    let conn = app.db.lock().unwrap();
    db::is_admin_session_valid(&conn, &token)
}

fn not_found() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

fn error_page(status: StatusCode, message: &str) -> Response {
    let body = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
        <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
        <title>burrow</title>\
        <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">\
        <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>\
        <link href=\"https://fonts.googleapis.com/css2?family=Quicksand:wght@500;700&display=swap\" rel=\"stylesheet\">\
        <style>:root{{--bg:#fff0f7;--text:#9e3f77;--header-bg:#fff;--header-text:#c2427f;--radius:12px}}\
        body{{margin:0;min-height:100vh;display:grid;place-items:center;padding:24px;background:var(--bg);color:var(--text);font-family:'Quicksand',Arial,sans-serif}}\
        .card{{background:var(--header-bg);border-radius:var(--radius);padding:24px 28px;max-width:420px;text-align:center;box-shadow:0 8px 24px rgba(158,63,119,.08)}}\
        h1{{margin:0 0 8px;font-size:20px;color:var(--header-text)}}p{{margin:0 0 16px;font-size:14px}}\
        a{{display:inline-block;padding:8px 14px;border-radius:var(--radius);background:#ffd6e7;color:var(--header-text);font-weight:700;font-size:13px;text-decoration:none}}</style>\
        </head><body><div class=\"card\"><h1>burrow</h1><p>{}</p><a href=\"/admin\">try again</a></div></body></html>",
        message
    );
    (status, Html(body)).into_response()
}

/// GET /admin/logout — revoke the persisted session cookie and return to the login screen.
pub async fn logout(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if let Some(token) = cookie_value(&headers, "burrow_session") {
        let conn = app.db.lock().unwrap();
        db::delete_admin_session(&conn, &token);
    }
    let secure = std::env::var("BURROW_SECURE_COOKIES")
        .map(|v| matches!(v.trim(), "1" | "true"))
        .unwrap_or(false);
    let mut cookie = "burrow_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0".to_string();
    if secure {
        cookie.push_str("; Secure");
    }
    let mut headers_out = HeaderMap::new();
    let _ = headers_out.insert(
        header::SET_COOKIE,
        axum::http::HeaderValue::from_str(&cookie).expect("valid cookie header"),
    );
    let _ = headers_out.insert(
        header::LOCATION,
        axum::http::HeaderValue::from_static("/admin"),
    );
    (StatusCode::FOUND, headers_out).into_response()
}

/// GET /admin — dashboard when signed in, otherwise a clean login screen.
pub async fn admin_page(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if dash_authed(&app, &headers).await {
        return Html(rendered_dashboard()).into_response();
    }
    if !discord_configured(&app) {
        if app.admin_token.is_some() && !bearer_ok(&app, &headers) {
            return not_found();
        }
        return Html(SETUP_HTML).into_response();
    }
    Html(LOGIN_HTML).into_response()
}

/// GET /admin/login — begin the Discord OAuth2 sign-in round-trip.
pub async fn oauth_login(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if dash_authed(&app, &headers).await {
        return Redirect::to("/admin").into_response();
    }
    if !discord_configured(&app) {
        return not_found();
    }

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
        return error_page(StatusCode::BAD_REQUEST, "missing login parameters");
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
            return error_page(StatusCode::BAD_REQUEST, "login window expired");
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
        return error_page(StatusCode::BAD_GATEWAY, "could not reach discord");
    };
    if !token_res.status().is_success() {
        tracing::warn!("discord token exchange failed: {}", token_res.status());
        return error_page(StatusCode::BAD_GATEWAY, "discord rejected the login");
    }
    let Ok(token_json) = token_res.json::<Value>().await else {
        return error_page(StatusCode::BAD_GATEWAY, "unexpected response from discord");
    };
    let access_token = token_json
        .get("access_token")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if access_token.is_empty() {
        return error_page(StatusCode::BAD_GATEWAY, "discord did not return an access token");
    }

    // Who is signing in?
    let me_res = app
        .http
        .get("https://discord.com/api/users/@me")
        .bearer_auth(&access_token)
        .send()
        .await;
    let Ok(me_res) = me_res else {
        return error_page(StatusCode::BAD_GATEWAY, "could not read your discord profile");
    };
    if !me_res.status().is_success() {
        tracing::warn!("discord @me request failed: {}", me_res.status());
        return error_page(StatusCode::BAD_GATEWAY, "discord rejected the profile lookup");
    }
    let Ok(me) = me_res.json::<Value>().await else {
        return error_page(StatusCode::BAD_GATEWAY, "unexpected profile response");
    };
    let user_id = me
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    // THE allowlist: exactly one account may ever pass this gate.
    if user_id.is_empty() || user_id != app.discord_allowed_user_id.clone().unwrap_or_default() {
        tracing::warn!("dashboard login attempt by discord id {}", user_id);
        return error_page(StatusCode::FORBIDDEN, "this account is not authorized");
    }

    // Issue a persistent 30-day session stored in SQLite so restarts don't log Mia out.
    let token = uuid::Uuid::new_v4().to_string();
    {
        let conn = app.db.lock().unwrap();
        db::create_admin_session(&conn, &token, now() + SESSION_TTL);
    }
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
