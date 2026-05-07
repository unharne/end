import os
import json
import time
from collections import defaultdict
from typing import Any, Dict, Optional

from aiohttp import web, WSMsgType, WSCloseCode

routes = web.RouteTableDef()

# All active websocket connections
clients: set[web.WebSocketResponse] = set()

# voice room state (we only have 1 voice room: "main")
voice_clients: dict[str, web.WebSocketResponse] = {}  # clientId -> ws
voice_members: dict[str, dict[str, Any]] = {}  # clientId -> {clientId, name, avatarType, avatarValue}

# Track per-ws metadata (for admin + bans)
client_meta: dict[web.WebSocketResponse, dict[str, Any]] = {}  # ws -> {ip, userAgent, connectedSince, lastSeen}

# bans
banned_ips: set[str] = set()

# simple per-connection rate limiting
last_message_at: dict[web.WebSocketResponse, float] = defaultdict(lambda: 0.0)
message_count_window: dict[web.WebSocketResponse, tuple[float, int]] = defaultdict(lambda: (0.0, 0))

# hard limits
MAX_JSON_BYTES = 8 * 1024  # keep messages small (chat text, control frames, etc.)
MAX_TEXT_LEN = 500
MAX_USER_NAME_LEN = 20
MAX_AVATAR_EMOJI_LEN = 2  # single emoji or 1-2 chars
MAX_AVATAR_DATAURL_BYTES = 256 * 1024  # allow small data URLs only
MAX_FILE_BYTES = 700 * 1024  # for inline file transfer we enforce client side too; server double-checks

# anti-spam: max 20 messages per 10 seconds per ws
SPAM_WINDOW_SEC = 10.0
SPAM_MAX_MSGS = 20

# admin
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "change-me")
ADMIN_TOKEN_HEADER = "X-Admin-Token"


def json_response_safe(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def is_probably_same_origin(request: web.Request) -> bool:
    origin = request.headers.get("Origin")
    if not origin:
        # Non-browser clients may omit Origin; allow (we still have other protections).
        return True
    host = request.host
    if origin.startswith("http://") or origin.startswith("https://"):
        try:
            host_part = origin.split("//", 1)[1].split("/", 1)[0]
            return host_part.split(":", 1)[0] == host.split(":", 1)[0]
        except Exception:
            return False
    return False


def safe_trim_text(s: Any, max_len: int) -> Optional[str]:
    if not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    if len(s) > max_len:
        s = s[:max_len]
    return s


def validate_user_payload(user: Any) -> Optional[dict[str, Any]]:
    if not isinstance(user, dict):
        return None

    name = safe_trim_text(user.get("name"), MAX_USER_NAME_LEN)
    if not name:
        return None

    avatar_type = user.get("avatarType")
    if avatar_type not in ("emoji", "image"):
        return None

    avatar_value = user.get("avatarValue")
    if avatar_type == "emoji":
        if not isinstance(avatar_value, str):
            return None
        avatar_value = avatar_value.strip()
        if len(avatar_value) > MAX_AVATAR_EMOJI_LEN:
            avatar_value = avatar_value[:MAX_AVATAR_EMOJI_LEN]
        return {"name": name, "avatarType": "emoji", "avatarValue": avatar_value}

    if not isinstance(avatar_value, str):
        return None
    if len(avatar_value.encode("utf-8")) > MAX_AVATAR_DATAURL_BYTES:
        return None
    if not avatar_value.startswith("data:image/"):
        return None
    return {"name": name, "avatarType": "image", "avatarValue": avatar_value}


def validate_signal(signal: Any) -> Optional[dict[str, Any]]:
    if not isinstance(signal, dict):
        return None
    st = signal.get("type")
    if st not in ("offer", "answer", "candidate"):
        return None
    if st == "candidate":
        c = signal.get("candidate")
        if not isinstance(c, str) or len(c) > 4000:
            return None
        return signal
    sdp = signal.get("sdp")
    if not isinstance(sdp, str) or len(sdp) > 200000:
        return None
    return signal


def validate_chat_message(data: Any) -> Optional[dict[str, Any]]:
    if not isinstance(data, dict) or data.get("type") != "chat":
        return None

    user = validate_user_payload(data.get("user"))
    if not user:
        return None

    text = safe_trim_text(data.get("text"), MAX_TEXT_LEN)
    if not text:
        return None

    return {"type": "chat", "user": user, "text": text}


def validate_voice_join(data: Any) -> Optional[dict[str, Any]]:
    if not isinstance(data, dict) or data.get("type") != "voice_join":
        return None
    client_id = data.get("clientId")
    if not isinstance(client_id, str) or not client_id or len(client_id) > 64:
        return None
    return {"type": "voice_join", "clientId": client_id}


def validate_voice_leave(data: Any) -> Optional[dict[str, Any]]:
    if not isinstance(data, dict) or data.get("type") != "voice_leave":
        return None
    client_id = data.get("clientId")
    if not isinstance(client_id, str) or not client_id or len(client_id) > 64:
        return None
    return {"type": "voice_leave", "clientId": client_id}


def validate_signal_message(data: Any) -> Optional[dict[str, Any]]:
    if not isinstance(data, dict) or data.get("type") != "signal":
        return None

    sender_id = data.get("senderId")
    target_id = data.get("targetId")
    signal = validate_signal(data.get("signal"))
    if not isinstance(sender_id, str) or not sender_id or len(sender_id) > 64:
        return None
    if not isinstance(target_id, str) or not target_id or len(target_id) > 64:
        return None
    if not signal:
        return None

    return {"type": "signal", "senderId": sender_id, "targetId": target_id, "signal": signal}


def validate_file_transfer(data: Any) -> Optional[dict[str, Any]]:
    if not isinstance(data, dict) or data.get("type") != "file":
        return None
    file_id = data.get("fileId")
    file_name = data.get("fileName")
    mime_type = data.get("mimeType")
    data_b64 = data.get("dataBase64")

    if not isinstance(file_id, str) or not file_id or len(file_id) > 80:
        return None
    if not isinstance(file_name, str) or not file_name:
        return None
    if len(file_name.encode("utf-8")) > 200:
        return None
    if any(ch in file_name for ch in ["\\", "/", "\0"]):
        return None

    if not isinstance(mime_type, str) or not mime_type.startswith(("image/", "application/", "text/")):
        return None

    if not isinstance(data_b64, str):
        return None

    if len(data_b64.encode("utf-8")) > int(MAX_FILE_BYTES * 4 / 3) + 1024:
        return None

    return {
        "type": "file",
        "fileId": file_id,
        "fileName": file_name,
        "mimeType": mime_type,
        "dataBase64": data_b64,
    }


def get_request_ip(request: web.Request) -> str:
    # For most cases remote is enough. If you are behind proxy, set X-Forwarded-For accordingly.
    ip = request.remote
    if not ip:
        return "unknown"
    return ip


def require_admin(request: web.Request) -> Optional[str]:
    token = request.headers.get(ADMIN_TOKEN_HEADER, "")
    if not token:
        return None
    if token != ADMIN_TOKEN:
        return None
    return "admin"


def client_list() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    now = time.time()
    for ws, meta in list(client_meta.items()):
        if ws.closed:
            continue
        out.append(
            {
                "ip": meta.get("ip", "unknown"),
                "userAgent": meta.get("userAgent", ""),
                "connectedSince": meta.get("connectedSince", 0),
                "lastSeenAgoSec": max(0, int(now - meta.get("lastSeen", meta.get("connectedSince", now)))),
            }
        )
    # Sort newest first
    out.sort(key=lambda x: x.get("connectedSince", 0), reverse=True)
    return out


async def broadcast(payload: Dict[str, Any], *, only_open: bool = True) -> None:
    data = json_response_safe(payload)
    # no await in loop; schedule
    for client in list(clients):
        if only_open and (client.closed):
            continue

        async def _send(c: web.WebSocketResponse, d: str) -> None:
            try:
                await c.send_str(d)
            except Exception:
                pass

        # Import locally to avoid breaking type checking
        import asyncio  # noqa: E402
        asyncio.create_task(_send(client, data))


async def kick_by_ip(ip: str) -> int:
    ip_norm = (ip or "").strip()
    if not ip_norm:
        return 0
    kicked = 0
    for ws, meta in list(client_meta.items()):
        if ws.closed:
            continue
        if str(meta.get("ip", "")).strip() == ip_norm:
            try:
                await ws.close(code=WSCloseCode.GOING_AWAY, message=b"admin kick")
                kicked += 1
            except Exception:
                pass
    return kicked


async def clear_chat_everywhere(reason: str) -> None:
    # UI clients must handle this event and clear their chat container.
    await broadcast({"type": "admin_clear_chat", "reason": reason})


async def maybe_rate_limit(ws: web.WebSocketResponse) -> bool:
    now = time.time()
    last = last_message_at[ws]
    if now - last < 0.03:
        return False
    last_message_at[ws] = now

    window_start, cnt = message_count_window[ws]
    if now - window_start > SPAM_WINDOW_SEC:
        message_count_window[ws] = (now, 1)
        return True
    if cnt >= SPAM_MAX_MSGS:
        return False
    message_count_window[ws] = (window_start, cnt + 1)
    return True


@routes.get("/admin/status")
async def admin_status(request: web.Request):
    role = require_admin(request)
    if not role:
        return web.Response(status=401, text=json_response_safe({"ok": False, "error": "unauthorized"}))
    return web.json_response({"ok": True, "role": role, "bannedCount": len(banned_ips)})


@routes.get("/admin/clients")
async def admin_clients(request: web.Request):
    role = require_admin(request)
    if not role:
        return web.Response(status=401, text=json_response_safe({"ok": False, "error": "unauthorized"}))
    return web.json_response({"ok": True, "clients": client_list()})


@routes.get("/admin/bans")
async def admin_bans_get(request: web.Request):
    role = require_admin(request)
    if not role:
        return web.Response(status=401, text=json_response_safe({"ok": False, "error": "unauthorized"}))
    return web.json_response({"ok": True, "bans": sorted(list(banned_ips))})


@routes.post("/admin/clear-chat")
async def admin_clear_chat(request: web.Request):
    role = require_admin(request)
    if not role:
        return web.Response(status=401, text=json_response_safe({"ok": False, "error": "unauthorized"}))

    try:
        body = await request.json()
    except Exception:
        body = {}

    reason = body.get("reason", "admin_clear")
    await clear_chat_everywhere(str(reason)[:200])
    return web.json_response({"ok": True})


@routes.post("/admin/ban")
async def admin_ban_ip(request: web.Request):
    role = require_admin(request)
    if not role:
        return web.Response(status=401, text=json_response_safe({"ok": False, "error": "unauthorized"}))

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    ip = (body.get("ip") or "").strip()
    if not ip:
        return web.json_response({"ok": False, "error": "ip required"}, status=400)

    banned_ips.add(ip)
    # immediately kick those already connected
    kicked = await kick_by_ip(ip)
    return web.json_response({"ok": True, "ip": ip, "kicked": kicked})


@routes.post("/admin/unban")
async def admin_unban_ip(request: web.Request):
    role = require_admin(request)
    if not role:
        return web.Response(status=401, text=json_response_safe({"ok": False, "error": "unauthorized"}))

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    ip = (body.get("ip") or "").strip()
    if not ip:
        return web.json_response({"ok": False, "error": "ip required"}, status=400)

    banned_ips.discard(ip)
    return web.json_response({"ok": True, "ip": ip})


@routes.post("/admin/kick-all")
async def admin_kick_all(request: web.Request):
    role = require_admin(request)
    if not role:
        return web.Response(status=401, text=json_response_safe({"ok": False, "error": "unauthorized"}))

    kicked = 0
    for ws in list(clients):
        if ws.closed:
            continue
        try:
            await ws.close(code=WSCloseCode.GOING_AWAY, message=b"admin kick all")
            kicked += 1
        except Exception:
            pass
    return web.json_response({"ok": True, "kicked": kicked})


@routes.post("/admin/kick-by-ip")
async def admin_kick_by_ip_post(request: web.Request):
    role = require_admin(request)
    if not role:
        return web.Response(status=401, text=json_response_safe({"ok": False, "error": "unauthorized"}))

    try:
        body = await request.json()
    except Exception:
        body = {}

    ip = (body.get("ip") or "").strip()
    if not ip:
        return web.json_response({"ok": False, "error": "ip required"}, status=400)

    kicked = await kick_by_ip(ip)
    return web.json_response({"ok": True, "ip": ip, "kicked": kicked})


@routes.get("/ws")
async def websocket_handler(request: web.Request):
    if not is_probably_same_origin(request):
        return web.Response(status=403, text="Forbidden")

    ip = get_request_ip(request)
    if ip in banned_ips:
        return web.Response(status=403, text="Banned")

    ws = web.WebSocketResponse(max_msg_size=MAX_JSON_BYTES)
    await ws.prepare(request)

    clients.add(ws)
    client_meta[ws] = {
        "ip": ip,
        "userAgent": request.headers.get("User-Agent", ""),
        "connectedSince": time.time(),
        "lastSeen": time.time(),
    }

    print(f"Клиент подключился. Всего: {len(clients)} ip={ip}")

    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue

            client_meta[ws]["lastSeen"] = time.time()

            # Guard parsing size
            if len(msg.data.encode("utf-8")) > MAX_JSON_BYTES:
                continue

            if not await maybe_rate_limit(ws):
                await ws.close(code=WSCloseCode.PROTOCOL_ERROR, message=b"rate limit")
                break

            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            # App ping/pong
            if isinstance(data, dict) and data.get("type") == "ping":
                await ws.send_str(json_response_safe({"type": "pong", "t": data.get("t"), "serverTime": time.time()}))
                continue

            # voice join/leave (track members)
            vj = validate_voice_join(data)
            if vj:
                voice_clients[vj["clientId"]] = ws
                user = validate_user_payload(data.get("user")) if "user" in data else None
                if user:
                    voice_members[vj["clientId"]] = {"clientId": vj["clientId"], **user}
                else:
                    voice_members.setdefault(
                        vj["clientId"],
                        {"clientId": vj["clientId"], "name": "??", "avatarType": "emoji", "avatarValue": "👤"},
                    )
                await ws.send_str(json_response_safe({"type": "voice_state", "members": list(voice_members.values())}))
                continue

            vl = validate_voice_leave(data)
            if vl:
                voice_clients.pop(vl["clientId"], None)
                voice_members.pop(vl["clientId"], None)
                payload = {"type": "voice_state", "members": list(voice_members.values())}
                for member_ws in list(voice_clients.values()):
                    if member_ws.closed:
                        continue
                    await member_ws.send_str(json_response_safe(payload))
                continue

            # voice signaling
            sig = validate_signal_message(data)
            if sig:
                target_ws = voice_clients.get(sig["targetId"])
                if target_ws and not target_ws.closed:
                    await target_ws.send_str(json_response_safe(sig))
                continue

            # chat
            chat = validate_chat_message(data)
            if chat:
                await broadcast(chat)
                continue

            # file transfer
            ft = validate_file_transfer(data)
            if ft:
                await broadcast(ft)
                continue

    finally:
        clients.discard(ws)
        client_meta.pop(ws, None)

        for client_id, client_ws in list(voice_clients.items()):
            if client_ws == ws:
                voice_clients.pop(client_id, None)
                voice_members.pop(client_id, None)

        print(f"Клиент отключился. Всего: {len(clients)} ip={ip}")

        if voice_members:
            payload = {"type": "voice_state", "members": list(voice_members.values())}
            for member_ws in list(voice_clients.values()):
                if member_ws.closed:
                    continue
                try:
                    await member_ws.send_str(json_response_safe(payload))
                except Exception:
                    pass

    return ws


@routes.get("/")
async def index_handler(request: web.Request):
    return web.Response(text="Msgswap server running.")


app = web.Application()
app.add_routes(routes)

# Serve frontend static assets
app.router.add_static("/static/", path="static", show_index=False)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    web.run_app(app, host="0.0.0.0", port=port)
