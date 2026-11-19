# Burrow Server

Self-hostable Rust signaling, relay, and admin dashboard server for the [Burrow](https://github.com/itsameeyah/burrow) Minecraft mod.

- **Mojang-authenticated** — verifies every player against Mojang's official `sessionserver` (no separate accounts or passwords).
- **Single binary / Docker container** — stores friends, invites, and stats in a local SQLite database (`/data/burrow.db`).
- **Built-in `/admin` dashboard** — optional Discord OAuth2-protected status & metrics page.

---

## Quick Start (Docker)

```bash
git clone https://github.com/itsameeyah/burrow-server.git
cd burrow-server
docker compose up -d --build
```

That's it — Burrow is now listening on port `8787` (`ws://<your-host>:8787/ws`). No `.env` file is required for a basic relay server.

---

## Exposing Over HTTPS / WSS

For public use, put Burrow behind HTTPS so clients connect over `wss://`. Pick whichever setup fits your server:

### Option A: Cloudflare Tunnel (no port forwarding)

1. Copy `.env.example` to `.env` and set `CLOUDFLARED_TOKEN` (from **Cloudflare Zero Trust → Networks → Tunnels**).
2. In your tunnel's **Public Hostname** settings, route `burrow.yourdomain.com` → `http://burrow:8787`.
3. Start with the `tunnel` profile:
   ```bash
   docker compose --profile tunnel up -d --build
   ```

### Option B: Caddy (automatic TLS)

```caddyfile
burrow.yourdomain.com {
    reverse_proxy 127.0.0.1:8787
}
```

### Option C: Nginx

Make sure WebSocket upgrade headers are forwarded (`proxy_http_version 1.1`, `Upgrade`, `Connection "upgrade"`) and increase `proxy_read_timeout` (e.g. `3600s`) so long-lived game relay connections stay open.

---

## Pointing the Burrow Mod at Your Server

Each player using your self-hosted server just edits `.minecraft/config/burrow.json` (generated automatically on first launch):

```json
{
  "serverUrl": "wss://burrow.yourdomain.com",
  "connectAutomatically": true,
  "incognito": false,
  "discordRichPresence": true
}
```

Restart Minecraft and Burrow will authenticate and connect to your server automatically.

---

## Optional: `/admin` Dashboard (Discord OAuth2)

1. Create an application at the [Discord Developer Portal](https://discord.com/developers/applications).
2. Under **OAuth2 → Redirects**, add `https://burrow.yourdomain.com/admin/callback`.
3. Copy `.env.example` to `.env` and fill in:
   - `BURROW_DISCORD_CLIENT_ID`
   - `BURROW_DISCORD_CLIENT_SECRET`
   - `BURROW_DISCORD_REDIRECT_URI=https://burrow.yourdomain.com/admin/callback`
   - `BURROW_DISCORD_ALLOWED_USER_ID` (your Discord numeric user ID — only this account can sign in)
4. Restart the container (`docker compose up -d`) and visit `https://burrow.yourdomain.com/admin`.

---

## Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `BURROW_PORT` | `8787` | Host port mapped by Docker Compose. |
| `BURROW_DEV_MODE` | `0` | Set to `1` only for offline dev clients (disables Mojang auth). |
| `BURROW_DISCORD_CLIENT_ID` | *(empty)* | Discord OAuth2 client ID for `/admin`. |
| `BURROW_DISCORD_CLIENT_SECRET` | *(empty)* | Discord OAuth2 client secret for `/admin`. |
| `BURROW_DISCORD_REDIRECT_URI` | *(empty)* | Full OAuth2 callback URL (`https://<domain>/admin/callback`). |
| `BURROW_DISCORD_ALLOWED_USER_ID` | *(empty)* | Discord user ID allowed to access `/admin`. |
| `BURROW_SECURE_COOKIES` | `1` | Set to `0` only if accessing `/admin` over plain LAN HTTP. |
| `BURROW_ADMIN_TOKEN` | *(empty)* | Optional static bearer token for `/admin/metrics`. |
| `CLOUDFLARED_TOKEN` | *(empty)* | Cloudflare Tunnel token (when using `--profile tunnel`). |

---

## Running From Source (Without Docker)

Requires Rust 1.80+:

```bash
cargo run --release
```

By default, the server listens on `0.0.0.0:8787` and stores its database in `burrow.db` (configurable via `BURROW_BIND` and `BURROW_DB`).

---

## License

MIT
