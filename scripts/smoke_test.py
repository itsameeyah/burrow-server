#!/usr/bin/env python3
"""Burrow server end-to-end protocol smoke test.

Pretends to be two dev Minecraft clients (meeyah + testbunny) and walks the
whole M1 flow: hello -> friend request -> accept -> live presence ->
session hosting -> join handshake -> relay byte splice in both directions.
Exit code 0 only if every check passes.
"""

import asyncio
import hashlib
import json
import sys
import urllib.error
import urllib.request
import uuid

import websockets

WS = "ws://127.0.0.1:8787/ws"
RELAY = "ws://127.0.0.1:8787/relay/ws"

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


async def main():
    async with websockets.connect(WS) as a, websockets.connect(WS) as b:
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

        # --- presence push when someone disconnects is checked at the end ---

        # --- A opens a hosted world ---
        await a.send(json.dumps({"op": "session.open", "worldName": "Bunny Burrow"}))
        sess = await recv_until(b, "session.state")
        s0 = sess["sessions"][0]
        check("B sees A hosting 'Bunny Burrow'",
              s0.get("host") == a_uuid and s0.get("worldName") == "Bunny Burrow" and s0.get("active"))

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

            await a.send(json.dumps({"op": "session.update", "access": "friends_of_friends"}))
            h2_sess = await recv_until(h2, "session.state")
            check("hop2 sees A's FOF world via testbunny",
                  h2_sess["sessions"][0].get("active") is True and h2_sess["sessions"][0].get("viaName") == "testbunny")

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
            urllib.request.urlopen("http://127.0.0.1:8787/api/metrics")
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

    fails = [n for n, ok in RESULTS if not ok]
    print(f"\n{len(RESULTS) - len(fails)}/{len(RESULTS)} checks passed")
    if fails:
        print("FAILED:", fails)
        sys.exit(1)
    print("ALL CHECKS PASSED 🐇")


asyncio.run(main())
