#!/usr/bin/env python3
"""Burrow server end-to-end protocol smoke test.

Pretends to be dev/Mojang Minecraft clients and walks the full flow:
hello/auth -> friend request -> accept -> live presence -> session hosting
(with gameVersion) -> join handshake -> relay byte splice -> access modes ->
Mojang sessionserver challenge/verify -> rate limiting -> prod-mode rejection.

Env vars:
  BURROW_TEST_PORT        Target server port (default: 8787).
  BURROW_MOJANG_MOCK_PORT If set, starts a local mock Mojang sessionserver on
                          127.0.0.1:<port> (match BURROW_MOJANG_SESSION_URL)
                          and runs Mojang auth challenge/verify checks.
  BURROW_TEST_DEV_MODE    "1" (default) runs the full dev-mode E2E suite;
                          "0" verifies dev & anonymous hellos are rejected
                          and Mojang auth succeeds against a prod-mode server.

Exit code 0 only if every check passes.
"""

import asyncio
import hashlib
import http.server
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid

import websockets

PORT = int(os.environ.get("BURROW_TEST_PORT", "8787"))
WS = f"ws://127.0.0.1:{PORT}/ws"
RELAY = f"ws://127.0.0.1:{PORT}/relay/ws"
HTTP_BASE = f"http://127.0.0.1:{PORT}"
MOJANG_MOCK_PORT = os.environ.get("BURROW_MOJANG_MOCK_PORT")
TEST_DEV_MODE = os.environ.get("BURROW_TEST_DEV_MODE", "1").strip() in ("1", "true")

RESULTS = []


def check(name, cond):
    RESULTS.append((name, bool(cond)))
    print(("PASS  " if cond else "FAIL  ") + name)


def offline_uuid(name: str) -> str:
    """Java's UUID.nameUUIDFromBytes: MD5-based v3 UUID."""
    digest = bytearray(hashlib.md5(("OfflinePlayer:" + name).encode()).digest())
    digest[6] = (digest[6] & 0x0F) | 0x30
    digest[8] = (digest[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(digest)))


async def recv_until(ws, op, pred=None, timeout=5.0):
    while True:
        try:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout))
        except asyncio.TimeoutError:
            raise AssertionError(f"timed out waiting for op '{op}'")
        if msg.get("op") == op and (pred is None or pred(msg)):
            return msg


class MockMojangServer:
    """Tiny local HTTP server mimicking sessionserver.mojang.com/session/minecraft/hasJoined."""

    def __init__(self, port: int):
        self.port = port
        self.valid_joins: dict[tuple[str, str], tuple[str, str]] = {}
        self.requests: list[tuple[str, str]] = []
        parent = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                if parsed.path != "/session/minecraft/hasJoined":
                    self.send_response(404)
                    self.end_headers()
                    return
                qs = urllib.parse.parse_qs(parsed.query)
                username = (qs.get("username") or [""])[0]
                server_id = (qs.get("serverId") or [""])[0]
                parent.requests.append((username, server_id))
                entry = parent.valid_joins.get((username, server_id))
                if entry is None:
                    self.send_response(204)
                    self.end_headers()
                    return
                raw_id, auth_name = entry
                body = json.dumps({"id": raw_id, "name": auth_name}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()


async def test_mojang_auth(mock: MockMojangServer):
    # 1. Wrong serverId fails and consumes the challenge (single-use).
    async with websockets.connect(WS) as ws:
        await ws.send(json.dumps({"op": "hello", "mode": "mojang", "name": "MojangBunny"}))
        chal = await recv_until(ws, "auth.challenge")
        sid = chal.get("serverId", "")
        check("mojang hello yields 32-char hex serverId challenge",
              len(sid) == 32 and all(c in "0123456789abcdef" for c in sid))

        await ws.send(json.dumps({"op": "auth.verify", "serverId": "0" * 32}))
        bad_err = await recv_until(ws, "error")
        check("mismatched serverId rejected",
              "Mojang verification failed" in bad_err.get("message", ""))

        # Replaying the real serverId after a failed attempt must also fail (single-use).
        mock.valid_joins[("MojangBunny", sid)] = ("069a79f444e94726a5befca90e38aaf5", "MojangBunny")
        await ws.send(json.dumps({"op": "auth.verify", "serverId": sid}))
        replay_err = await recv_until(ws, "error")
        check("consumed challenge cannot be replayed",
              "Mojang verification failed" in replay_err.get("message", ""))

        # Connection must remain unauthenticated.
        await ws.send(json.dumps({"op": "friend.add", "name": "meeyah"}))
        unauth_err = await recv_until(ws, "error")
        check("failed mojang verify leaves connection unauthenticated",
              "say hello first" in unauth_err.get("message", ""))

    # 2. Sessionserver HTTP 204 fails verification.
    async with websockets.connect(WS) as ws:
        await ws.send(json.dumps({"op": "hello", "mode": "mojang", "name": "UnjoinedUser"}))
        chal = await recv_until(ws, "auth.challenge")
        sid = chal["serverId"]
        await ws.send(json.dumps({"op": "auth.verify", "serverId": sid}))
        err204 = await recv_until(ws, "error")
        check("mojang 204 response rejected",
              "Mojang verification failed" in err204.get("message", ""))

    # 3. Valid Mojang sessionserver 200 completes login with authoritative UUID & casing.
    async with websockets.connect(WS) as ws:
        await ws.send(json.dumps({"op": "hello", "mode": "mojang", "name": "mojangbunny"}))
        chal = await recv_until(ws, "auth.challenge")
        sid = chal["serverId"]
        raw_id = "069a79f444e94726a5befca90e38aaf5"
        expected_uuid = "069a79f4-44e9-4726-a5be-fca90e38aaf5"
        mock.valid_joins[("mojangbunny", sid)] = (raw_id, "MojangBunny")

        await ws.send(json.dumps({"op": "auth.verify", "serverId": sid}))
        welcome = await recv_until(ws, "welcome")
        check("mojang verify succeeds with authoritative UUID and name",
              welcome.get("uuid") == expected_uuid and welcome.get("name") == "MojangBunny")
        sync = await recv_until(ws, "friends.sync")
        check("mojang login sends initial friends.sync", sync.get("op") == "friends.sync")


async def run_prod_mode_suite(mock: MockMojangServer | None):
    # Verify dev and anonymous/unknown modes are rejected when BURROW_DEV_MODE=0.
    async with websockets.connect(WS) as ws:
        await ws.send(json.dumps({"op": "hello", "mode": "dev", "name": "meeyah",
                                  "uuid": offline_uuid("meeyah")}))
        dev_err = await recv_until(ws, "error")
        check("prod mode rejects dev hello with authentication required",
              dev_err.get("message") == "authentication required")

        await ws.send(json.dumps({"op": "hello", "mode": "anonymous"}))
        anon_err = await recv_until(ws, "error")
        check("prod mode rejects anonymous hello with authentication required",
              anon_err.get("message") == "authentication required")

        await ws.send(json.dumps({"op": "hello"}))
        missing_err = await recv_until(ws, "error")
        check("prod mode rejects missing mode hello with authentication required",
              missing_err.get("message") == "authentication required")

        await ws.send(json.dumps({"op": "friend.add", "name": "meeyah"}))
        unauth = await recv_until(ws, "error")
        check("rejected hello leaves connection unauthenticated",
              "say hello first" in unauth.get("message", ""))

    if mock is not None:
        await test_mojang_auth(mock)


async def run_dev_mode_suite(mock: MockMojangServer | None):
    async with websockets.connect(WS) as a, websockets.connect(WS) as b:
        # --- anonymous hello rejected even in dev mode (no throwaway Guest-* rows) ---
        async with websockets.connect(WS) as anon:
            await anon.send(json.dumps({"op": "hello", "mode": "anonymous"}))
            anon_err = await recv_until(anon, "error")
            check("anonymous hello rejected with authentication required",
                  anon_err.get("message") == "authentication required")

        # --- hello both clients ---
        await a.send(json.dumps({"op": "hello", "mode": "dev", "name": "meeyah",
                                 "uuid": offline_uuid("meeyah")}))
        welcome_a = await recv_until(a, "welcome")
        check("A welcomed as meeyah", welcome_a.get("name") == "meeyah")
        await recv_until(a, "friends.sync")

        await b.send(json.dumps({"op": "hello", "mode": "dev", "name": "testbunny",
                                 "uuid": offline_uuid("testbunny")}))
        welcome_b = await recv_until(b, "welcome")
        check("B welcomed as testbunny", welcome_b.get("name") == "testbunny")
        await recv_until(b, "friends.sync")

        a_uuid = welcome_a["uuid"]

        # --- friend request A -> B ---
        await a.send(json.dumps({"op": "friend.add", "name": "testbunny"}))
        sync_a = await recv_until(a, "friends.sync")
        check("A sees outgoing request to testbunny",
              any(r.get("name") == "testbunny" for r in sync_a.get("outgoing", [])))
        sync_b = await recv_until(b, "friends.sync")
        check("B got pushed incoming request from meeyah",
              any(r.get("name") == "meeyah" for r in sync_b.get("incoming", [])))

        # --- duplicate request rejected politely ---
        await a.send(json.dumps({"op": "friend.add", "name": "testbunny"}))
        dup = await recv_until(a, "error")
        check("duplicate request gets a friendly error", "already" in dup.get("message", ""))

        # --- B accepts ---
        await b.send(json.dumps({"op": "friend.accept", "uuid": a_uuid}))
        sync_b2 = await recv_until(b, "friends.sync")
        check("B's list now has meeyah online",
              any(f.get("name") == "meeyah" and f.get("presence") == "online"
                  for f in sync_b2.get("friends", [])))
        sync_a2 = await recv_until(a, "friends.sync")
        check("A's list now has testbunny online",
              any(f.get("name") == "testbunny" and f.get("presence") == "online"
                  for f in sync_a2.get("friends", [])))

        # --- A opens a hosted world with gameVersion ---
        await a.send(json.dumps({
            "op": "session.open",
            "worldName": "Bunny Burrow",
            "gameVersion": "1.21.4",
        }))
        sess = await recv_until(b, "session.state")
        s0 = sess["sessions"][0]
        check("B sees A hosting 'Bunny Burrow' with gameVersion 1.21.4",
              s0.get("host") == a_uuid
              and s0.get("worldName") == "Bunny Burrow"
              and s0.get("gameVersion") == "1.21.4"
              and s0.get("active"))

        # Check gameVersion in friends.sync (friends array)
        await b.send(json.dumps({"op": "friends.sync"}))
        sync_ver = await recv_until(b, "friends.sync")
        check("friends.sync carries gameVersion in friends list",
              any(f.get("uuid") == a_uuid and f.get("gameVersion") == "1.21.4"
                  for f in sync_ver.get("friends", [])))

        # --- B asks to join ---
        await b.send(json.dumps({"op": "join.begin", "host": a_uuid}))
        ready = await recv_until(b, "join.ready")
        check("B got join.ready with room + token",
              bool(ready.get("roomId")) and bool(ready.get("token")))
        relay_host = await recv_until(a, "relay.host")
        check("A got relay.host with its own token", bool(relay_host.get("token")))
        check("relay tokens differ per side", relay_host.get("token") != ready.get("token"))
        check("relay.host carries joinSource='friend' and joinerName",
              relay_host.get("joinSource") == "friend" and relay_host.get("joinerName") == "testbunny")

        # --- both connect to the relay and exchange raw bytes ---
        async with websockets.connect(
                f"{RELAY}?room={ready['roomId']}&token={relay_host['token']}") as rh, \
                websockets.connect(
                f"{RELAY}?room={ready['roomId']}&token={ready['token']}") as rj:
            await asyncio.sleep(0.4)  # let the broker pair both sockets
            await rh.send(b"host-says-hi")
            got = await asyncio.wait_for(rj.recv(), 5)
            check("relay forwards host -> joiner bytes", got == b"host-says-hi")
            await rj.send(b"joiner-says-hi")
            got2 = await asyncio.wait_for(rh.recv(), 5)
            check("relay forwards joiner -> host bytes", got2 == b"joiner-says-hi")

        # --- security: strangers and duplicate identities ---
        async with websockets.connect(WS) as stray:
            await stray.send(json.dumps({"op": "hello", "mode": "dev", "name": "stray",
                                         "uuid": offline_uuid("stray")}))
            await recv_until(stray, "welcome")
            await recv_until(stray, "friends.sync")

            # A non-friend asking to join must be rejected.
            await stray.send(json.dumps({"op": "join.begin", "host": a_uuid}))
            err = await recv_until(stray, "error")
            check("non-friend join rejected", "not friends" in err.get("message", ""))

            # So must joining your own world.
            stray_uuid = offline_uuid("stray")
            await stray.send(json.dumps({"op": "join.begin", "host": stray_uuid}))
            own = await recv_until(stray, "error")
            check("self-join rejected", "own world" in own.get("message", ""))

            # A second hello on the same socket must not re-identify.
            await stray.send(json.dumps({"op": "hello", "mode": "dev", "name": "stray",
                                         "uuid": stray_uuid}))
            dup_hello = await recv_until(stray, "error")
            check("duplicate hello rejected", "already identified" in dup_hello.get("message", ""))

        # --- access modes: locked, invite_only + invite, friends_of_friends (strict 2-hop), open, banned ---
        b_uuid = welcome_b["uuid"]
        await a.send(json.dumps({"op": "session.update", "locked": True}))
        await recv_until(b, "session.state")
        await b.send(json.dumps({"op": "join.begin", "host": a_uuid}))
        locked_err = await recv_until(b, "error")
        check("locked world blocks joins", "locked" in locked_err.get("message", ""))

        await a.send(json.dumps({"op": "session.update", "locked": False, "access": "invite_only"}))
        await recv_until(b, "session.state")
        await b.send(json.dumps({"op": "join.begin", "host": a_uuid}))
        inv_err = await recv_until(b, "error")
        check("invite_only blocks uninvited friend", "invite-only" in inv_err.get("message", ""))

        await a.send(json.dumps({"op": "session.invite", "uuid": b_uuid}))
        inv_msg = await recv_until(b, "invite.received")
        check("B received world invite from A", inv_msg.get("host") == a_uuid)
        await b.send(json.dumps({"op": "join.begin", "host": a_uuid}))
        await recv_until(b, "join.ready")
        inv_relay = await recv_until(a, "relay.host")
        check("invited join reports joinSource='invite'", inv_relay.get("joinSource") == "invite")

        # FOF chain: A <-> B <-> hop2 <-> hop3 (hop2 is 2-hop, hop3 is 3-hop)
        async with websockets.connect(WS) as h2, websockets.connect(WS) as h3:
            await h2.send(json.dumps({"op": "hello", "mode": "dev", "name": "hop2",
                                      "uuid": offline_uuid("hop2")}))
            h2_uuid = (await recv_until(h2, "welcome"))["uuid"]
            await recv_until(h2, "friends.sync")
            await h3.send(json.dumps({"op": "hello", "mode": "dev", "name": "hop3",
                                      "uuid": offline_uuid("hop3")}))
            h3_uuid = (await recv_until(h3, "welcome"))["uuid"]
            await recv_until(h3, "friends.sync")

            # Make B <-> hop2 and hop2 <-> hop3 friends
            await b.send(json.dumps({"op": "friend.add", "name": "hop2"}))
            await recv_until(h2, "friends.sync")
            await h2.send(json.dumps({"op": "friend.accept", "uuid": b_uuid}))
            await recv_until(h2, "friends.sync")
            await h2.send(json.dumps({"op": "friend.add", "name": "hop3"}))
            await recv_until(h3, "friends.sync")
            await h3.send(json.dumps({"op": "friend.accept", "uuid": h2_uuid}))
            await recv_until(h3, "friends.sync")

            await a.send(json.dumps({
                "op": "session.update",
                "access": "friends_of_friends",
                "gameVersion": "1.21.5",
            }))
            h2_sess = await recv_until(h2, "session.state")
            check("hop2 sees A's FOF world via testbunny with updated gameVersion 1.21.5",
                  h2_sess["sessions"][0].get("active") is True
                  and h2_sess["sessions"][0].get("viaName") == "testbunny"
                  and h2_sess["sessions"][0].get("gameVersion") == "1.21.5")

            # Check gameVersion in friends.sync (discoverable array)
            await h2.send(json.dumps({"op": "friends.sync"}))
            h2_sync = await recv_until(h2, "friends.sync")
            check("friends.sync carries gameVersion in discoverable list",
                  any(d.get("host") == a_uuid and d.get("gameVersion") == "1.21.5"
                      for d in h2_sync.get("discoverable", [])))

            await h2.send(json.dumps({"op": "join.begin", "host": a_uuid}))
            await recv_until(h2, "join.ready")
            h2_relay = await recv_until(a, "relay.host")
            check("hop2 joins FOF world with viaName='testbunny'",
                  h2_relay.get("joinSource") == "friend_of_friend" and h2_relay.get("viaName") == "testbunny")

            await h3.send(json.dumps({"op": "join.begin", "host": a_uuid}))
            h3_err = await recv_until(h3, "error")
            check("3-hop hop3 rejected from friends_of_friends", "friends of friends" in h3_err.get("message", ""))

            await a.send(json.dumps({"op": "session.update", "access": "open"}))
            await recv_until(h3, "session.state")
            await h3.send(json.dumps({"op": "join.begin", "host": a_uuid}))
            await recv_until(h3, "join.ready")
            h3_relay = await recv_until(a, "relay.host")
            check("hop3 joins Open world with joinSource='open'", h3_relay.get("joinSource") == "open")

            await a.send(json.dumps({"op": "session.update", "banned": [h3_uuid]}))
            await recv_until(h3, "session.state")
            await h3.send(json.dumps({"op": "join.begin", "host": a_uuid}))
            ban_err = await recv_until(h3, "error")
            check("banned player rejected even on Open world", "banned" in ban_err.get("message", ""))

            # Clean back to normal friends access before closing
            await a.send(json.dumps({"op": "session.update", "access": "friends", "banned": []}))
            await recv_until(b, "session.state")

        # --- rich presence (server name + ip) and invite.request ---
        await a.send(json.dumps({
            "op": "presence.update",
            "activity": "server",
            "serverName": "Meeyah SMP",
            "serverIp": "play.meeyah.dev",
            "label": "",
        }))
        pres_srv = await recv_until(
            b, "presence.batch",
            pred=lambda m: any(u.get("uuid") == a_uuid and u.get("activity") == "server"
                               for u in m.get("updates", [])),
        )
        u0 = pres_srv["updates"][0]
        check("B sees A playing on 'Meeyah SMP (play.meeyah.dev)'",
              u0.get("serverName") == "Meeyah SMP" and u0.get("serverIp") == "play.meeyah.dev")

        await b.send(json.dumps({"op": "invite.request", "uuid": a_uuid}))
        req_inv = await recv_until(a, "invite.requested")
        check("A received invite.requested from testbunny",
              req_inv.get("from") == b_uuid and req_inv.get("fromName") == "testbunny")

        await b.send(json.dumps({
            "op": "users.check",
            "uuids": [a_uuid, str(uuid.uuid4())],
            "names": ["meeyah", "nobody_has_burrow"],
        }))
        chk = await recv_until(b, "users.check.result")
        check("users.check identifies registered Burrow players and excludes others",
              a_uuid in chk.get("uuids", [])
              and "meeyah" in chk.get("names", [])
              and "nobody_has_burrow" not in chk.get("names", []))

        # --- MC username rename safety (case-insensitive collision by different UUID) ---
        u_old = str(uuid.uuid4())
        u_new = str(uuid.uuid4())
        async with websockets.connect(WS) as r1:
            await r1.send(json.dumps({"op": "hello", "mode": "dev", "name": "SwapName", "uuid": u_old}))
            await recv_until(r1, "welcome")
        async with websockets.connect(WS) as r2:
            await r2.send(json.dumps({"op": "hello", "mode": "dev", "name": "swapname", "uuid": u_new}))
            w_r2 = await recv_until(r2, "welcome")
            check("username rename takeover succeeds for new UUID",
                  w_r2.get("uuid") == u_new and w_r2.get("name") == "swapname")
            await b.send(json.dumps({"op": "users.check", "uuids": [u_old, u_new], "names": ["SwapName"]}))
            chk_r = await recv_until(b, "users.check.result")
            check("both old and new UUIDs remain registered after rename",
                  u_old in chk_r.get("uuids", []) and u_new in chk_r.get("uuids", []))

        # --- per-connection signaling rate limit ---
        async with websockets.connect(WS) as flood:
            for _ in range(42):
                await flood.send(json.dumps({"op": "friends.sync"}))
            rl_err = await recv_until(flood, "error")
            check("signaling rate limit rejects rapid flood",
                  "rate limit" in rl_err.get("message", ""))

        # --- Mojang sessionserver challenge/verify flow (when mock port configured) ---
        if mock is not None:
            await test_mojang_auth(mock)

        # --- wrong token is rejected ---
        try:
            bad = await websockets.connect(f"{RELAY}?room={ready['roomId']}&token=not-a-token")
            await asyncio.sleep(0.2)
            await bad.send(b"nope")
            await asyncio.sleep(0.3)
            try:
                await asyncio.wait_for(bad.recv(), 2)
                check("bad relay token rejected", False)
            except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError):
                check("bad relay token rejected", True)
        except Exception:
            check("bad relay token rejected", True)

        # --- metrics stays unpublished without a token ---
        try:
            urllib.request.urlopen(f"{HTTP_BASE}/api/metrics")
            check("metrics hidden without token (404)", False)
        except urllib.error.HTTPError as e:
            check("metrics hidden without token (404)", e.code == 404)

        await a.send(json.dumps({"op": "session.close"}))
        sess_closed = await recv_until(
            b, "session.state",
            pred=lambda m: m["sessions"][0].get("host") == a_uuid and m["sessions"][0].get("active") is False,
        )
        sc0 = sess_closed["sessions"][0]
        check("B sees A stopped hosting",
              sc0.get("host") == a_uuid and sc0.get("active") is False)
        print("---- session closed, winding down ----")

        # --- disconnect pushes offline presence to friend ---
        await a.close()
        pres = await recv_until(
            b, "presence.batch",
            pred=lambda m: any(u.get("uuid") == a_uuid and u.get("presence") == "offline"
                               for u in m.get("updates", [])),
        )
        check("B sees A go offline on disconnect",
              any(u.get("uuid") == a_uuid and u.get("presence") == "offline"
                  for u in pres.get("updates", [])))


async def main():
    mock = None
    if MOJANG_MOCK_PORT:
        mock = MockMojangServer(int(MOJANG_MOCK_PORT))
        mock.start()
    try:
        if TEST_DEV_MODE:
            await run_dev_mode_suite(mock)
        else:
            await run_prod_mode_suite(mock)
    finally:
        if mock is not None:
            mock.stop()

    fails = [n for n, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(fails)}/{len(RESULTS)} checks passed")
    if fails:
        print("FAILED:", fails)
        sys.exit(1)
    print("ALL CHECKS PASSED 🐇")


asyncio.run(main())
