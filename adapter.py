"""
Sendblue platform adapter for Hermes Agent.

Install this directory as ~/.hermes/plugins/sendblue and Hermes can
receive and send iMessage/SMS/RCS through Sendblue from the gateway loop.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import random
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from urllib import parse

if TYPE_CHECKING:
    import httpx

    HTTPX_AVAILABLE = True
else:
    try:
        import httpx

        HTTPX_AVAILABLE = True
    except ImportError:  # pragma: no cover - current Hermes includes httpx
        httpx = None
        HTTPX_AVAILABLE = False

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_audio_from_bytes,
    cache_document_from_bytes,
    cache_image_from_bytes,
    cache_video_from_bytes,
)

try:
    from gateway.platforms._http_client_limits import platform_httpx_limits
except ImportError:  # pragma: no cover - compatibility with older Hermes cores
    platform_httpx_limits = None

# strip_markdown was consolidated into gateway.platforms.helpers when the
# per-adapter copies in sms.py/bluebubbles.py/feishu.py were de-duplicated.
# Import it when present (current cores) but fall back to a local copy on
# pre-consolidation cores, so a missing symbol can never disable the whole
# plugin at import time (cf. the cache_media_bytes regression). The fallback
# mirrors core's regexes exactly: code content is kept (only fences removed)
# and markdown links collapse to their anchor text.
try:
    from gateway.platforms.helpers import strip_markdown
except Exception:  # pragma: no cover - exercised only on very old cores
    import re as _re

    _MD_FALLBACK_PATTERNS = [
        (_re.compile(r"\*\*(.+?)\*\*", _re.DOTALL), r"\1"),                       # bold
        (_re.compile(r"\*(.+?)\*", _re.DOTALL), r"\1"),                           # italic *
        (_re.compile(r"\b__(?![\s_])(.+?)(?<![\s_])__\b", _re.DOTALL), r"\1"),    # bold _
        (_re.compile(r"\b_(?![\s_])(.+?)(?<![\s_])_\b", _re.DOTALL), r"\1"),      # italic _
        (_re.compile(r"```[a-zA-Z0-9_+-]*\n?"), ""),                              # code fences (keep body)
        (_re.compile(r"`(.+?)`"), r"\1"),                                         # inline code
        (_re.compile(r"^#{1,6}\s+", _re.MULTILINE), ""),                          # ATX headings
        (_re.compile(r"\[([^\]]+)\]\([^\)]+\)"), r"\1"),                          # links -> anchor text
        (_re.compile(r"\n{3,}"), "\n\n"),                                         # collapse blank runs
    ]

    def strip_markdown(text: str) -> str:
        text = str(text or "")
        for pattern, repl in _MD_FALLBACK_PATTERNS:
            text = pattern.sub(repl, text)
        return text.strip()

logger = logging.getLogger("gateway.platforms.sendblue")

DEFAULT_API_BASE = "https://api.sendblue.com"
DEFAULT_USER_AGENT = "sendblue-hermes/1.0 (+https://github.com/njerschow/hermes-sendblue)"
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_LOOKBACK_SECONDS = 60
DEFAULT_LIMIT = 100
MAX_SENDBLUE_BODY_CHARS = 18_000
MAX_WEBHOOK_BODY_BYTES = 1024 * 1024
MIN_WEBHOOK_SECRET_CHARS = 16
SENDBLUE_MAX_UPLOAD_BYTES = 100 * 1024 * 1024  # Sendblue documented per-file limit
SENDBLUE_MEDIA_DOWNLOAD_TIMEOUT = 30.0  # seconds; matches base.py URL-cache helpers
SENDBLUE_MAX_INBOUND_MEDIA_BYTES = 25 * 1024 * 1024  # bound each inbound media download
SENDBLUE_MAX_MEDIA_REDIRECTS = 5
GROUP_ID_MAX_LENGTH = 160
GROUP_ID_ALLOWED_CHARS_PATTERN = re.compile(r"^[A-Za-z0-9+@._:-]+$")
# A one-recipient "group" is really a 1:1 thread: Sendblue returns no group_id
# for it, and any reply arrives without one, so it would route back as a DM and
# strand the id we handed out. Refuse rather than create that dead end.
GROUP_MIN_RECIPIENTS = 2

# Sendblue/iMessage has no stable @identity for the bot. Keep the same default
# wake words as Hermes' BlueBubbles and Photon adapters.
_DEFAULT_MENTION_PATTERNS = [
    r"(?<![\w@])@?hermes\s+agent\b[,:\-]?",
    r"(?<![\w@])@?hermes\b[,:\-]?",
]

# Sendblue inbound messages carry no media-type metadata (only a bare media_url),
# so we download the bytes and let _classify_and_cache() classify them, then map the
# resulting kind to a Hermes MessageType. All audio -> VOICE (not AUDIO) so iMessage
# voice notes are auto-transcribed; the tradeoff is that a plain audio clip is also
# sent to STT, but Sendblue exposes no "is voice note" flag and silently dropping
# real voice notes is the worse failure. This mirrors the BlueBubbles adapter.
_KIND_TO_MESSAGE_TYPE = {
    "image": MessageType.PHOTO,
    "video": MessageType.VIDEO,
    "audio": MessageType.VOICE,
    "document": MessageType.DOCUMENT,
}
_KIND_TO_PLACEHOLDER = {
    "image": "(image)",
    "video": "(video)",
    "audio": "(voice message)",
    "document": "(attachment)",
}

# MIME / extension → kind classification for inbound media. The unified
# cache_media_bytes() dispatcher only landed in core after May 2026 (and even on
# current main its sole caller is telegram.py), so we classify here and route to
# the long-stable per-kind cache_*_from_bytes helpers — the same ones every
# bundled adapter (BlueBubbles, Photon, …) uses. Works on old and new cores.
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".heif", ".bmp", ".tiff"}
_AUDIO_EXTS = {".mp3", ".m4a", ".ogg", ".wav", ".caf", ".aac", ".opus", ".flac", ".amr"}
_VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv", ".avi", ".m4v", ".3gp"}
_IMAGE_EXT_BY_MIME = {
    "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif",
    "image/webp": ".webp", "image/heic": ".jpg", "image/heif": ".jpg",
    "image/tiff": ".jpg", "image/bmp": ".bmp",
}
_AUDIO_EXT_BY_MIME = {
    "audio/mpeg": ".mp3", "audio/mp3": ".mp3", "audio/mp4": ".m4a",
    "audio/aac": ".m4a", "audio/ogg": ".ogg", "audio/wav": ".wav",
    "audio/x-wav": ".wav", "audio/x-caf": ".caf", "audio/amr": ".amr",
}
_VIDEO_EXT_BY_MIME = {
    "video/mp4": ".mp4", "video/quicktime": ".mov",
    "video/webm": ".webm", "video/3gpp": ".3gp",
}


def _classify_and_cache(data: bytes, filename: str, mime: str) -> Optional[tuple]:
    """Cache inbound media bytes locally; return (path, media_type, kind) or None.

    Classifies by MIME prefix then file extension and routes to the per-kind
    cache_*_from_bytes helpers. Returns None when the bytes can't be classified
    (the caller then falls back to the ``[Media: <url>]`` marker). ``kind`` is one
    of "image" / "audio" / "video" / "document".
    """
    mime = (mime or "").lower()
    ext = os.path.splitext(filename or "")[1].lower()
    try:
        if mime.startswith("image/") or ext in _IMAGE_EXTS:
            chosen = ext if ext in _IMAGE_EXTS else _IMAGE_EXT_BY_MIME.get(mime, ".jpg")
            try:
                return cache_image_from_bytes(data, chosen), mime or "image/jpeg", "image"
            except ValueError:
                return None  # claimed to be an image but the bytes aren't one
        if mime.startswith("audio/") or ext in _AUDIO_EXTS:
            chosen = ext if ext in _AUDIO_EXTS else _AUDIO_EXT_BY_MIME.get(mime, ".ogg")
            return cache_audio_from_bytes(data, chosen), mime or "audio/mpeg", "audio"
        if mime.startswith("video/") or ext in _VIDEO_EXTS:
            chosen = ext if ext in _VIDEO_EXTS else _VIDEO_EXT_BY_MIME.get(mime, ".mp4")
            return cache_video_from_bytes(data, chosen), mime or "video/mp4", "video"
        if ext:
            # A named file of some other type → treat as a document.
            return cache_document_from_bytes(data, filename), mime or "application/octet-stream", "document"
        # No MIME and no extension: last-resort image sniff (validates magic bytes).
        try:
            return cache_image_from_bytes(data, ".jpg"), "image/jpeg", "image"
        except ValueError:
            return None
    except Exception:
        return None


def _is_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return ""


def _has_group_chat_id_marker(value: Any) -> bool:
    text = str(value or "").strip()
    return text.startswith("sb_group_") or "_group_id_" in text


def _is_group_chat_id(value: Any) -> bool:
    """Return whether *value* matches Sendblue's public group-id contract."""
    text = str(value or "").strip()
    if not text or len(text) > GROUP_ID_MAX_LENGTH:
        return False
    if not GROUP_ID_ALLOWED_CHARS_PATTERN.fullmatch(text):
        return False
    return _has_group_chat_id_marker(text)


def _normalize_phone(value: str) -> str:
    """Normalize common phone input to E.164-ish form without guessing country."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("+"):
        return "+" + "".join(ch for ch in raw[1:] if ch.isdigit())
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if len(digits) == 10:
        return f"+1{digits}"
    return f"+{digits}" if digits else raw


def _parse_allowlist(raw: Any) -> set:
    """Split an allowlist setting into normalized entries; ``*`` passes through.

    Core matches allowlist entries as exact strings (gateway/authz_mixin.py),
    but ``interactive_setup`` already writes normalized numbers, so normalizing
    both sides here only ever prevents a false rejection of the same number in
    a different format.
    """
    if raw is None:
        return set()
    parts = raw if isinstance(raw, list) else str(raw).split(",")
    entries = set()
    for part in parts:
        text = str(part).strip()
        if not text:
            continue
        entries.add("*" if text == "*" else _normalize_phone(text))
    return entries


def _resolve_message_target(value: Any) -> tuple[str, str]:
    """Resolve a Hermes chat id to (``group``|``dm``, Sendblue target)."""
    raw = str(value or "").strip()
    if not raw:
        return "dm", ""
    if _has_group_chat_id_marker(raw):
        if not _is_group_chat_id(raw):
            raise ValueError("Invalid Sendblue group id")
        return "group", raw
    return "dm", _normalize_phone(raw)


def _compile_mention_patterns(raw: Any) -> List[re.Pattern[str]]:
    """Compile group wake words from config or the environment."""
    if raw is None:
        patterns: List[Any] = list(_DEFAULT_MENTION_PATTERNS)
    elif isinstance(raw, str):
        text = raw.strip()
        try:
            loaded = json.loads(text)
        except Exception:
            loaded = None
        patterns = loaded if isinstance(loaded, list) else [
            part.strip()
            for line in text.splitlines()
            for part in line.split(",")
        ]
    elif isinstance(raw, list):
        patterns = raw
    else:
        patterns = [raw]

    compiled: List[re.Pattern[str]] = []
    for pattern in patterns:
        text = str(pattern).strip()
        if not text:
            continue
        try:
            compiled.append(re.compile(text, re.IGNORECASE))
        except re.error as exc:
            logger.warning("Sendblue: invalid mention pattern %r: %s", text, exc)
    return compiled


def _message_matches_mention_patterns(text: str, patterns: List[re.Pattern[str]]) -> bool:
    return bool(text and patterns and any(pattern.search(text) for pattern in patterns))


def _clean_mention_text(text: str, patterns: List[re.Pattern[str]]) -> str:
    """Strip a leading wake word without deleting ordinary later matches."""
    if not text:
        return text
    stripped = text.lstrip()
    for pattern in patterns:
        match = pattern.match(stripped)
        if match:
            cleaned = stripped[match.end():].lstrip(" ,:-")
            return cleaned or text
    return text


def _redacted_phone(value: str) -> str:
    normalized = _normalize_phone(value)
    if len(normalized) <= 5:
        return "unknown"
    return f"...{normalized[-4:]}"


def _is_valid_webhook_secret(value: str) -> bool:
    secret = str(value or "").strip()
    return bool(secret) and secret.lower() != "change-me" and len(secret) >= MIN_WEBHOOK_SECRET_CHARS


def _split_message(text: str, limit: int = MAX_SENDBLUE_BODY_CHARS) -> List[str]:
    text = str(text or "").strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    chunks: List[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        split_at = remaining.rfind("\n\n", 0, limit)
        if split_at < limit // 3:
            split_at = remaining.rfind("\n", 0, limit)
        if split_at < limit // 3:
            split_at = remaining.rfind(" ", 0, limit)
        if split_at < limit // 3:
            split_at = limit

        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()

    return [chunk for chunk in chunks if chunk]


def _parse_datetime(value: Any) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return datetime.now(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_local_media(value: Any) -> bool:
    text = str(value or "")
    return bool(text) and not text.startswith(("http://", "https://"))


def _sanitize_upload_filename(filename: str) -> str:
    return filename.replace('"', "").replace("\r", "").replace("\n", "")


def _hermes_home() -> Path:
    try:
        from hermes_cli.config import get_hermes_home

        return Path(get_hermes_home())
    except Exception:
        return Path(os.getenv("HERMES_HOME", "~/.hermes")).expanduser()


@dataclass
class SendblueSettings:
    api_key: str
    api_secret: str
    phone_number: str
    api_base: str = DEFAULT_API_BASE
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS
    startup_lookback_seconds: int = DEFAULT_LOOKBACK_SECONDS
    message_limit: int = DEFAULT_LIMIT
    mark_read: bool = False
    webhook_enabled: bool = False
    webhook_host: str = "127.0.0.1"
    webhook_port: int = 3141
    webhook_path: str = "/webhook/sendblue"
    webhook_secret: str = ""
    require_mention: bool = False
    mention_patterns_raw: Any = None
    allowed_users_raw: Any = ""
    allow_all_users: bool = False


def _settings_from_config(config: Any) -> SendblueSettings:
    extra = getattr(config, "extra", {}) or {}

    api_key = (
        _env_first("SENDBLUE_API_KEY", "SENDBLUE_API_API_KEY")
        or extra.get("api_key")
        or extra.get("apiKey")
        or ""
    )
    api_secret = (
        _env_first("SENDBLUE_API_SECRET", "SENDBLUE_API_API_SECRET")
        or extra.get("api_secret")
        or extra.get("apiSecret")
        or ""
    )
    phone_number = (
        _env_first("SENDBLUE_PHONE_NUMBER", "SENDBLUE_NUMBER")
        or extra.get("phone_number")
        or extra.get("phoneNumber")
        or extra.get("sendblue_number")
        or ""
    )

    api_base = os.getenv("SENDBLUE_API_BASE") or extra.get("api_base") or DEFAULT_API_BASE
    poll_interval = os.getenv("SENDBLUE_POLL_INTERVAL_SECONDS") or extra.get("poll_interval_seconds")
    lookback = os.getenv("SENDBLUE_STARTUP_LOOKBACK_SECONDS") or extra.get("startup_lookback_seconds")
    limit = os.getenv("SENDBLUE_MESSAGE_LIMIT") or extra.get("message_limit")
    webhook = extra.get("webhook", {}) if isinstance(extra.get("webhook"), dict) else {}
    # Match BlueBubbles and Photon: behavioral config wins when explicitly
    # present, with environment variables retained as compatibility fallbacks.
    require_mention_raw = extra.get("require_mention")
    if require_mention_raw is None:
        require_mention_raw = os.getenv("SENDBLUE_REQUIRE_MENTION")
    mention_patterns_raw: Any = (
        extra["mention_patterns"]
        if "mention_patterns" in extra
        else os.getenv("SENDBLUE_MENTION_PATTERNS")
    )
    # Deliberately env-only, unlike require_mention above. Core reads the
    # allowlist from the environment alone (gateway/authz_mixin.py resolves
    # allowed_users_env / allow_all_env off os.getenv) and ignores config.yaml.
    # Honoring a config-only allowlist here would let outbound group creation
    # and inbound authorization disagree about who is trusted.
    # Both vars are unioned, not first-wins, because core unions them too.
    allowed_users_raw = ",".join(
        value
        for value in (
            os.getenv("SENDBLUE_ALLOWED_USERS"),
            os.getenv("GATEWAY_ALLOWED_USERS"),
        )
        if value and value.strip()
    )
    # Only the platform flag, not GATEWAY_ALLOW_ALL_USERS: core honors the
    # global one solely when no allowlist is configured at all, which
    # _require_allowed already treats as unrestricted.
    allow_all_users = _is_truthy(os.getenv("SENDBLUE_ALLOW_ALL_USERS"))

    return SendblueSettings(
        api_key=str(api_key).strip(),
        api_secret=str(api_secret).strip(),
        phone_number=_normalize_phone(str(phone_number)),
        api_base=str(api_base).rstrip("/"),
        poll_interval_seconds=float(poll_interval or DEFAULT_POLL_INTERVAL_SECONDS),
        startup_lookback_seconds=int(lookback or DEFAULT_LOOKBACK_SECONDS),
        message_limit=int(limit or DEFAULT_LIMIT),
        mark_read=_is_truthy(os.getenv("SENDBLUE_MARK_READ") or extra.get("mark_read")),
        webhook_enabled=_is_truthy(
            os.getenv("SENDBLUE_WEBHOOK_ENABLED")
            or webhook.get("enabled")
            or extra.get("webhook_enabled")
        ),
        webhook_host=str(
            os.getenv("SENDBLUE_WEBHOOK_HOST")
            or webhook.get("host")
            or extra.get("webhook_host")
            or "127.0.0.1"
        ),
        webhook_port=int(
            os.getenv("SENDBLUE_WEBHOOK_PORT")
            or webhook.get("port")
            or extra.get("webhook_port")
            or 3141
        ),
        webhook_path=str(
            os.getenv("SENDBLUE_WEBHOOK_PATH")
            or webhook.get("path")
            or extra.get("webhook_path")
            or "/webhook/sendblue"
        ),
        webhook_secret=str(
            os.getenv("SENDBLUE_WEBHOOK_SECRET")
            or webhook.get("secret")
            or extra.get("webhook_secret")
            or ""
        ),
        require_mention=_is_truthy(require_mention_raw),
        mention_patterns_raw=mention_patterns_raw,
        allowed_users_raw=allowed_users_raw,
        allow_all_users=allow_all_users,
    )


class SendblueClient:
    def __init__(
        self,
        settings: SendblueSettings,
        *,
        transport: Optional[Any] = None,
    ):
        self.settings = settings
        self._transport = transport
        self._http_client: Optional["httpx.AsyncClient"] = None

    async def open(self) -> None:
        if not HTTPX_AVAILABLE:
            raise RuntimeError("Sendblue requires httpx, which is not installed")
        if self._http_client is not None and not self._http_client.is_closed:
            return

        kwargs: Dict[str, Any] = {
            "timeout": httpx.Timeout(30.0),
            "follow_redirects": False,
        }
        if platform_httpx_limits is not None:
            limits = platform_httpx_limits()
            if limits is not None:
                kwargs["limits"] = limits
        else:
            kwargs["limits"] = httpx.Limits(
                max_keepalive_connections=10,
                keepalive_expiry=2.0,
            )
        if self._transport is not None:
            kwargs["transport"] = self._transport
        self._http_client = httpx.AsyncClient(**kwargs)

    async def aclose(self) -> None:
        client = self._http_client
        self._http_client = None
        if client is not None and not client.is_closed:
            await client.aclose()

    async def __aenter__(self) -> "SendblueClient":
        await self.open()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def _http(self) -> "httpx.AsyncClient":
        await self.open()
        assert self._http_client is not None
        return self._http_client

    def _headers(self, *, content_type: Optional[str] = "application/json") -> Dict[str, str]:
        headers: Dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": DEFAULT_USER_AGENT,
            "sb-api-key-id": self.settings.api_key,
            "sb-api-secret-key": self.settings.api_secret,
        }
        if content_type is not None:
            headers["Content-Type"] = content_type
        return headers

    async def _json_request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        url = f"{self.settings.api_base}{path}"
        client = await self._http()
        headers = self._headers(content_type=None if files is not None else "application/json")
        try:
            response = await client.request(
                method,
                url,
                params=query,
                json=body,
                files=files,
                headers=headers,
                timeout=timeout,
                follow_redirects=False,
            )
            response.raise_for_status()
            return response.json() if response.content else {}
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Sendblue API error {exc.response.status_code}: {exc.response.text}"
            ) from exc
        except httpx.RequestError as exc:
            raise RuntimeError(f"Sendblue API connection error: {exc}") from exc

    async def list_inbound_messages(self, since: datetime) -> List[Dict[str, Any]]:
        cutoff = since - timedelta(seconds=2)
        limit = max(1, min(self.settings.message_limit, 100))
        query: Dict[str, str] = {
            "limit": str(limit),
            "offset": "0",
            "is_outbound": "false",
            "created_at_gte": _iso_utc(cutoff),
            "order_by": "createdAt",
            "order_direction": "asc",
        }
        if self.settings.phone_number:
            query["sendblue_number"] = self.settings.phone_number

        messages: List[Dict[str, Any]] = []
        offset = 0
        while True:
            query["offset"] = str(offset)
            payload = await self._json_request(
                "GET",
                "/api/v2/messages",
                query=query,
            )
            data = payload.get("data") or []
            messages.extend(
                msg
                for msg in data
                if not msg.get("is_outbound")
                and _parse_datetime(msg.get("date_sent") or msg.get("date_created")) >= cutoff
            )

            pagination = payload.get("pagination") or {}
            total = int(pagination.get("total") or 0)
            if len(data) < limit or (total and offset + len(data) >= total):
                break
            offset += len(data)

        messages.sort(key=lambda msg: _parse_datetime(msg.get("date_sent") or msg.get("date_created")))
        return messages

    async def _post_message(
        self,
        target: Dict[str, Any],
        path: str,
        content: str,
        media_url: str,
    ) -> Dict[str, Any]:
        """POST a Sendblue message and return the whole response payload.

        Group creation needs ``group_id`` out of the response, which the
        message-id-only ``_send_one`` contract cannot carry.
        """
        body = dict(target)
        body["from_number"] = self.settings.phone_number
        if content:
            body["content"] = content
        if media_url:
            body["media_url"] = media_url
        if not body.get("content") and not body.get("media_url"):
            raise ValueError("Sendblue message requires content or media_url")

        return await self._json_request(
            "POST",
            path,
            body=body,
        )

    @staticmethod
    def _message_id_from(payload: Dict[str, Any]) -> str:
        return str(payload.get("message_handle") or payload.get("id") or int(time.time() * 1000))

    async def _send_one(
        self,
        target: Dict[str, Any],
        path: str,
        content: str,
        media_url: str,
    ) -> str:
        return self._message_id_from(await self._post_message(target, path, content, media_url))

    async def send_message(
        self,
        to_number: str,
        content: str = "",
        *,
        media_url: str = "",
    ) -> str:
        return await self._send_one(
            {"number": _normalize_phone(to_number)},
            "/api/send-message",
            content,
            media_url,
        )

    async def send_group_message(
        self,
        group_id: str,
        content: str = "",
        *,
        media_url: str = "",
    ) -> str:
        """Send into an existing group.

        Deliberately has no ``numbers`` parameter: creation lives in
        ``create_group`` so that exactly one code path can put a ``numbers``
        array on the wire, and the allowlist has a single chokepoint.
        """
        target = str(group_id or "").strip()
        if not _is_group_chat_id(target):
            raise ValueError("Invalid Sendblue group id")
        return await self._send_one(
            {"group_id": target},
            "/api/send-group-message",
            content,
            media_url,
        )

    def _require_allowed(self, numbers: List[str]) -> None:
        """Reject outbound recipients outside the configured allowlist.

        Skipped when allow-all is set or no allowlist is configured, mirroring
        core: gateway/authz_mixin.py falls through to pairing and global policy
        when a platform has no allowlist of its own. The adapter cannot see the
        pairing store, so a number approved only by pairing is rejected here
        even though inbound traffic from it is accepted -- deliberately
        stricter for an operation that reaches out to a stranger.
        """
        if self.settings.allow_all_users:
            return
        allowed = _parse_allowlist(self.settings.allowed_users_raw)
        if not allowed or "*" in allowed:
            return
        blocked = [number for number in numbers if number not in allowed]
        if blocked:
            raise ValueError(
                "Recipients are not in SENDBLUE_ALLOWED_USERS: "
                + ", ".join(_redacted_phone(number) for number in blocked)
            )

    async def create_group(
        self,
        numbers: List[str],
        content: str = "",
        *,
        media_url: str = "",
    ) -> tuple[str, str]:
        """Create a group by seeding it with a message.

        Returns ``(group_id, message_id)``. Sendblue has no create-only
        endpoint: the group comes into being when the first message is sent to
        a ``numbers`` array, and the new id comes back on that response.
        """
        seen = set()
        recipients: List[str] = []
        for number in numbers or []:
            normalized = _normalize_phone(number)
            # Dropping our own line is not cosmetic: passing it is a plausible
            # caller mistake and it would otherwise count toward the minimum.
            if not normalized or normalized == self.settings.phone_number:
                continue
            if normalized not in seen:
                seen.add(normalized)
                recipients.append(normalized)

        if len(recipients) < GROUP_MIN_RECIPIENTS:
            raise ValueError(
                f"Sendblue group creation requires at least {GROUP_MIN_RECIPIENTS} "
                "distinct recipients; use send() for a one-to-one thread"
            )
        self._require_allowed(recipients)

        payload = await self._post_message(
            {"numbers": recipients},
            "/api/send-group-message",
            content,
            media_url,
        )

        group_id = str(payload.get("group_id") or "").strip()
        if not _is_group_chat_id(group_id):
            # The group-id contract was derived from inbound payloads. If
            # creation ever returns a differently shaped id, this is where it
            # surfaces -- log enough to diagnose without leaking the payload.
            logger.warning(
                "Sendblue group creation returned an unusable group_id (len=%d, keys=%s)",
                len(group_id),
                ",".join(sorted(str(key) for key in payload.keys())),
            )
            raise ValueError("Sendblue group creation returned an unusable group id")
        return group_id, self._message_id_from(payload)

    async def add_recipient_to_group(self, group_id: str, number: str) -> Dict[str, Any]:
        """Add one recipient to an existing group via ``/api/modify-group``."""
        target = str(group_id or "").strip()
        if not _is_group_chat_id(target):
            raise ValueError("Invalid Sendblue group id")
        recipient = _normalize_phone(number)
        if not recipient:
            raise ValueError("Sendblue modify-group requires add_recipient")
        self._require_allowed([recipient])

        return await self._json_request(
            "POST",
            "/api/modify-group",
            body={"group_id": target, "add_recipient": recipient},
        )

    async def send_typing_indicator(self, to_number: str) -> None:
        body = {
            "number": _normalize_phone(to_number),
            "from_number": self.settings.phone_number,
        }
        await self._json_request(
            "POST",
            "/api/send-typing-indicator",
            body=body,
        )

    async def mark_read(self, to_number: str) -> None:
        body = {
            "number": _normalize_phone(to_number),
            "from_number": self.settings.phone_number,
        }
        await self._json_request(
            "POST",
            "/api/mark-read",
            body=body,
        )

    async def upload_file_from_bytes(self, filename: str, data: bytes) -> str:
        if not data:
            raise ValueError(f"File '{filename}' is empty")
        if len(data) > SENDBLUE_MAX_UPLOAD_BYTES:
            raise ValueError(
                f"File '{filename}' is {len(data)} bytes, exceeds Sendblue limit "
                f"of {SENDBLUE_MAX_UPLOAD_BYTES} bytes"
            )

        safe_name = _sanitize_upload_filename(filename)
        payload = await self._json_request(
            "POST",
            "/api/upload-file",
            files={"file": (safe_name, data, "application/octet-stream")},
            timeout=60.0,
        )
        media_url = payload.get("media_url")
        if not media_url:
            raise RuntimeError(f"Sendblue upload returned no media_url: {payload}")
        return str(media_url)

    async def upload_file(self, file_path: str) -> str:
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Local media path not found: {file_path}")
        data = await asyncio.to_thread(path.read_bytes)
        return await self.upload_file_from_bytes(path.name, data)

    @staticmethod
    def _validated_media_url(media_url: str) -> parse.ParseResult:
        # Sendblue CDN media URLs are always HTTPS. Requiring https blocks
        # file://, ftp://, etc. We deliberately skip a full SSRF/DNS check
        # (is_safe_url lives in tools.url_safety, outside gateway.platforms.base):
        # the URL comes from Sendblue's own API response, not user free-text, and a
        # per-poll DNS resolution is avoidable overhead. https-only is the pragmatic
        # guard for a platform plugin.
        parsed = parse.urlparse(media_url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise RuntimeError(f"Refusing non-HTTPS media URL: {media_url}")
        return parsed

    async def download_media(
        self, media_url: str, *, authenticated: bool = False
    ) -> tuple[bytes, str]:
        """Download inbound media bytes; return (data, lowercased content-type)."""
        initial = self._validated_media_url(media_url)

        # The download targets the CDN host, not api.sendblue.com, so by default we
        # do NOT send sb-api-key credentials (avoid leaking them to the CDN). Flip
        # `authenticated` if Sendblue's CDN is ever found to require auth.
        if authenticated:
            headers = self._headers(content_type=None)
        else:
            headers = {"User-Agent": DEFAULT_USER_AGENT, "Accept": "*/*"}

        client = await self._http()
        current_url = media_url
        initial_origin = (initial.scheme.lower(), initial.netloc.lower())
        for redirect_count in range(SENDBLUE_MAX_MEDIA_REDIRECTS + 1):
            self._validated_media_url(current_url)
            try:
                async with client.stream(
                    "GET",
                    current_url,
                    headers=headers,
                    timeout=SENDBLUE_MEDIA_DOWNLOAD_TIMEOUT,
                    follow_redirects=False,
                ) as response:
                    if response.is_redirect:
                        location = response.headers.get("Location")
                        if not location or redirect_count >= SENDBLUE_MAX_MEDIA_REDIRECTS:
                            raise RuntimeError(
                                "Sendblue media download exceeded redirect limit"
                            )
                        next_url = parse.urljoin(str(response.url), location)
                        next_parsed = self._validated_media_url(next_url)
                        if authenticated and (
                            next_parsed.scheme.lower(), next_parsed.netloc.lower()
                        ) != initial_origin:
                            raise RuntimeError(
                                "Refusing to forward Sendblue credentials across media origins"
                            )
                        current_url = next_url
                        continue

                    response.raise_for_status()
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > SENDBLUE_MAX_INBOUND_MEDIA_BYTES:
                            raise RuntimeError(
                                f"Inbound media exceeds {SENDBLUE_MAX_INBOUND_MEDIA_BYTES} bytes"
                            )
                    content_type = (response.headers.get("Content-Type") or "").split(
                        ";", 1
                    )[0]
                    return bytes(data), content_type.strip().lower()
            except httpx.HTTPStatusError as exc:
                raise RuntimeError(
                    f"Sendblue media download error {exc.response.status_code}"
                ) from exc
            except httpx.RequestError as exc:
                raise RuntimeError(
                    f"Sendblue media download connection error: {exc}"
                ) from exc

        raise RuntimeError("Sendblue media download exceeded redirect limit")


class ProcessedMessageStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_messages (
              message_id TEXT PRIMARY KEY,
              processed_at INTEGER NOT NULL
            )
            """
        )
        self._conn.commit()

    def try_mark(self, message_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO processed_messages (message_id, processed_at) VALUES (?, ?)",
                (message_id, int(time.time())),
            )
            self._conn.commit()
            return cur.rowcount > 0

    def cleanup(self, older_than_seconds: int = 7 * 24 * 60 * 60) -> None:
        cutoff = int(time.time()) - older_than_seconds
        with self._lock:
            self._conn.execute("DELETE FROM processed_messages WHERE processed_at < ?", (cutoff,))
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class SendblueWebhookServer:
    def __init__(self, adapter: "SendblueAdapter", settings: SendblueSettings):
        self.adapter = adapter
        self.settings = settings
        self.httpd: Optional[ThreadingHTTPServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        adapter = self.adapter
        settings = self.settings
        loop = asyncio.get_running_loop()

        class Handler(BaseHTTPRequestHandler):
            server_version = "HermesSendblueWebhook/1.0"

            def log_message(self, fmt: str, *args: Any) -> None:
                logger.debug("Sendblue webhook: " + fmt, *args)

            def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
                raw = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:
                if self.path == "/health":
                    self._send_json(200, {"status": "ok"})
                else:
                    self._send_json(404, {"error": "not found"})

            def do_POST(self) -> None:
                path = self.path.split("?", 1)[0]
                if path != settings.webhook_path:
                    self._send_json(404, {"error": "not found"})
                    return

                if settings.webhook_secret:
                    provided = (
                        self.headers.get("sb-signing-secret")
                        or self.headers.get("x-sendblue-secret")
                        or self.headers.get("x-webhook-secret")
                        or self.headers.get("authorization")
                        or ""
                    )
                    if provided.startswith("Bearer "):
                        provided = provided[7:]
                    if not hmac.compare_digest(provided, settings.webhook_secret):
                        self._send_json(401, {"error": "unauthorized"})
                        return

                try:
                    length = int(self.headers.get("content-length", "0"))
                except ValueError:
                    self._send_json(400, {"error": "bad content-length"})
                    return

                if length < 1 or length > MAX_WEBHOOK_BODY_BYTES:
                    self._send_json(413, {"error": "payload too large"})
                    return

                try:
                    payload = json.loads(self.rfile.read(length).decode("utf-8"))
                except Exception:
                    self._send_json(400, {"error": "invalid json"})
                    return

                if not isinstance(payload, dict):
                    self._send_json(400, {"error": "invalid payload"})
                    return

                self._send_json(200, {"received": True})
                asyncio.run_coroutine_threadsafe(adapter._handle_sendblue_message(payload), loop)

        self.httpd = ThreadingHTTPServer((settings.webhook_host, settings.webhook_port), Handler)
        self.thread = threading.Thread(
            target=self.httpd.serve_forever,
            name="sendblue-webhook",
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
            self.thread = None


class SendblueAdapter(BasePlatformAdapter):
    def __init__(self, config: Any, **_: Any):
        super().__init__(config=config, platform=Platform("sendblue"))
        self.settings = _settings_from_config(config)
        self.client = SendblueClient(self.settings)
        self._poll_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._webhook_server: Optional[SendblueWebhookServer] = None
        self._last_poll_time = datetime.now(timezone.utc) - timedelta(
            seconds=max(0, self.settings.startup_lookback_seconds)
        )
        state_path = _hermes_home() / "platforms" / "sendblue" / "processed.sqlite3"
        self._store = ProcessedMessageStore(state_path)
        self._mention_patterns = _compile_mention_patterns(self.settings.mention_patterns_raw)

    @property
    def name(self) -> str:
        return "Sendblue"

    async def connect(self) -> bool:
        missing = []
        if not self.settings.api_key:
            missing.append("SENDBLUE_API_KEY")
        if not self.settings.api_secret:
            missing.append("SENDBLUE_API_SECRET")
        if not self.settings.phone_number:
            missing.append("SENDBLUE_PHONE_NUMBER")
        if missing:
            msg = f"Missing Sendblue configuration: {', '.join(missing)}"
            logger.error(msg)
            self._set_fatal_error("config_missing", msg, retryable=False)
            return False

        if not self._acquire_platform_lock("sendblue", self.settings.phone_number, "Sendblue line"):
            return False

        if self.settings.webhook_enabled and not _is_valid_webhook_secret(
            self.settings.webhook_secret
        ):
            msg = (
                "Webhook mode requires SENDBLUE_WEBHOOK_SECRET with at least "
                f"{MIN_WEBHOOK_SECRET_CHARS} non-placeholder characters"
            )
            logger.error(msg)
            self._set_fatal_error("webhook_secret_missing", msg, retryable=False)
            self._release_platform_lock()
            return False

        try:
            await self.client.open()
            if self.settings.webhook_enabled:
                self._webhook_server = SendblueWebhookServer(self, self.settings)
                self._webhook_server.start()
                logger.info(
                    "Sendblue webhook listening on http://%s:%s%s",
                    self.settings.webhook_host,
                    self.settings.webhook_port,
                    self.settings.webhook_path,
                )
            else:
                self._poll_task = asyncio.create_task(self._poll_loop(), name="sendblue-poll")
                logger.info("Sendblue polling every %.1fs", self.settings.poll_interval_seconds)

            self._cleanup_task = asyncio.create_task(self._cleanup_loop(), name="sendblue-cleanup")
            self._mark_connected()
            return True
        except Exception as exc:
            try:
                await self.client.aclose()
            except Exception:
                logger.debug("Sendblue HTTP client close failed", exc_info=True)
            self._release_platform_lock()
            self._set_fatal_error("connect_failed", str(exc), retryable=True)
            logger.error("Sendblue connect failed: %s", exc, exc_info=True)
            return False

    async def disconnect(self) -> None:
        self._mark_disconnected()

        for task in (self._poll_task, self._cleanup_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self._poll_task = None
        self._cleanup_task = None

        if self._webhook_server:
            await asyncio.to_thread(self._webhook_server.stop)
            self._webhook_server = None

        try:
            await self.client.aclose()
        except Exception:
            logger.debug("Sendblue HTTP client close failed", exc_info=True)

        self._release_platform_lock()
        self._store.close()

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(60 * 60)
            try:
                self._store.cleanup()
            except Exception:
                logger.debug("Sendblue cleanup failed", exc_info=True)

    async def _poll_loop(self) -> None:
        while True:
            started_at = datetime.now(timezone.utc)
            try:
                messages = await self.client.list_inbound_messages(self._last_poll_time)
                for message in messages:
                    await self._handle_sendblue_message(message)
                self._last_poll_time = started_at
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Sendblue poll failed: %s", exc)

            await asyncio.sleep(max(1.0, self.settings.poll_interval_seconds))

    async def _handle_sendblue_message(self, message: Dict[str, Any]) -> None:
        if message.get("is_outbound"):
            return

        message_id = str(message.get("message_handle") or message.get("id") or "").strip()
        from_number = _normalize_phone(str(message.get("from_number") or message.get("number") or ""))
        if not message_id or not from_number:
            logger.warning(
                "Sendblue inbound message missing message_handle or from_number; keys=%s",
                sorted(str(key) for key in message.keys()),
            )
            return

        recipient_numbers = {
            _normalize_phone(str(message.get(key) or ""))
            for key in ("to_number", "sendblue_number")
            if message.get(key)
        }
        if self.settings.phone_number and self.settings.phone_number not in recipient_numbers:
            logger.warning(
                "Dropping Sendblue message %s for unexpected recipient fields=%s",
                message_id[-8:],
                ",".join(sorted(_redacted_phone(num) for num in recipient_numbers)) or "missing",
            )
            return

        if not self._store.try_mark(message_id):
            return

        content = str(message.get("content") or "").strip()
        media_url = str(message.get("media_url") or "").strip()

        text = content
        media_urls: List[str] = []
        media_types: List[str] = []
        message_type = MessageType.TEXT

        if media_url:
            # Download + cache the attachment so the gateway can attach images to
            # the model's vision input and route voice notes to STT. On any failure
            # (download error, oversize, or an unclassifiable type) fall back to the
            # plain [Media: <url>] marker so the message is never lost.
            try:
                data, content_type = await self.client.download_media(media_url)
                filename = parse.unquote(os.path.basename(parse.urlparse(media_url).path))
                cached = _classify_and_cache(data, filename, content_type)
                if cached is None:
                    raise RuntimeError(
                        f"unclassifiable media (content-type={content_type or 'unknown'})"
                    )
                cached_path, cached_mime, cached_kind = cached
                media_urls.append(cached_path)
                media_types.append(cached_mime)
                message_type = _KIND_TO_MESSAGE_TYPE.get(cached_kind, MessageType.DOCUMENT)
                if not content:
                    text = _KIND_TO_PLACEHOLDER.get(cached_kind, "(attachment)")
            except Exception as exc:
                logger.warning(
                    "Sendblue media handling failed for %s; falling back to link: %s",
                    message_id[-8:],
                    exc,
                )
                media_urls = []
                media_types = []
                message_type = MessageType.TEXT
                text = (
                    f"{content}\n\n[Media: {media_url}]" if content else f"[Media: {media_url}]"
                )

        if not text:
            return

        group_id = str(message.get("group_id") or "").strip()
        declared_group = str(message.get("message_type") or "").strip().lower() == "group"
        if declared_group and not group_id:
            logger.warning(
                "Dropping Sendblue group message %s with no group_id",
                message_id[-8:],
            )
            return
        if group_id and not _is_group_chat_id(group_id):
            logger.warning(
                "Dropping Sendblue message %s with invalid group_id",
                message_id[-8:],
            )
            return

        is_group = bool(group_id)
        if is_group and self.settings.require_mention:
            if not _message_matches_mention_patterns(text, self._mention_patterns):
                logger.debug(
                    "Sendblue: ignoring group message "
                    "(require_mention=true, no mention pattern matched)"
                )
                return
            text = _clean_mention_text(text, self._mention_patterns)

        # Sendblue's typing/read endpoints accept a phone number, not a group id.
        if self.settings.mark_read and not is_group:
            try:
                await self.client.mark_read(from_number)
            except Exception:
                logger.debug("Sendblue mark-read failed", exc_info=True)

        if is_group:
            display_name = str(message.get("group_display_name") or "").strip()
            source = self.build_source(
                chat_id=group_id,
                chat_name=display_name or f"Sendblue group {group_id[-6:]}",
                chat_type="group",
                user_id=from_number,
                user_name=f"Sendblue contact {_redacted_phone(from_number)}",
                message_id=message_id,
            )
        else:
            source = self.build_source(
                chat_id=from_number,
                chat_name=f"Sendblue contact {_redacted_phone(from_number)}",
                chat_type="dm",
                user_id=from_number,
                user_name=f"Sendblue contact {_redacted_phone(from_number)}",
                message_id=message_id,
            )
        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            media_urls=media_urls,
            media_types=media_types,
            raw_message={
                "message_handle": message_id,
                "service": message.get("service"),
                "message_type": message.get("message_type"),
                "group_id": group_id or None,
                "has_media": bool(media_urls),
            },
            message_id=message_id,
            timestamp=_parse_datetime(message.get("date_sent") or message.get("date_created")),
        )
        await self.handle_message(event)

    def format_message(self, content: str) -> str:
        """Strip markdown — iMessage/SMS/RCS render it as literal characters.

        Mirrors the SMS, BlueBubbles, and Photon adapters: Sendblue is a
        plain-text transport, so **bold**, headings, [links](url), and code
        fences would otherwise reach the recipient as raw syntax.
        """
        return strip_markdown(content or "")

    def _resolve_target(self, chat_id: str) -> tuple[str, str]:
        return _resolve_message_target(chat_id)

    async def _send_chunk(
        self,
        kind: str,
        target: str,
        content: str = "",
        *,
        media_url: str = "",
    ) -> str:
        if kind == "group":
            return await self.client.send_group_message(target, content, media_url=media_url)
        return await self.client.send_message(target, content, media_url=media_url)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del reply_to, metadata
        message_ids: List[str] = []
        try:
            kind, target = self._resolve_target(chat_id)
            if not target:
                return SendResult(success=False, error="Missing destination phone number or group id")
            for chunk in _split_message(self.format_message(content)):
                message_ids.append(await self._send_chunk(kind, target, chunk))
            if not message_ids:
                return SendResult(success=False, error="Empty message")
            return SendResult(success=True, message_id=",".join(message_ids))
        except ValueError as exc:
            return SendResult(success=False, error=str(exc), retryable=False)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def _send_with_retry(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Any = None,
        max_retries: int = 2,
        base_delay: float = 2.0,
    ) -> SendResult:
        """Retry transient sends without the base Markdown-fallback banner.

        Sendblue's :meth:`send` already strips Markdown, so a permanent send
        failure cannot be repaired by prepending the base adapter's
        ``(Response formatting failed, plain text:)`` notice and sending the
        same response again.  Keep the base exponential-backoff behavior for
        connection failures, but return permanent and delivery-ambiguous
        failures unchanged.
        """
        result = await self.send(
            chat_id=chat_id,
            content=content,
            reply_to=reply_to,
            metadata=metadata,
        )
        if result.success:
            return result

        error_str = result.error or ""
        pattern_retryable = self._is_retryable_error(error_str)

        # Sendblue currently marks broad request exceptions retryable.  A
        # read/write timeout is different: the API may have accepted the
        # non-idempotent send, so retrying could deliver a duplicate.  A
        # recognized connection failure (including ConnectTimeout) is safe.
        if self._is_timeout_error(error_str) and not pattern_retryable:
            return result

        if not (result.retryable or pattern_retryable):
            logger.warning("[sendblue] Send failed permanently: %s", error_str)
            return result

        for attempt in range(1, max_retries + 1):
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
            logger.warning(
                "[sendblue] Send failed (attempt %d/%d, retrying in %.1fs): %s",
                attempt,
                max_retries,
                delay,
                error_str,
            )
            await asyncio.sleep(delay)
            result = await self.send(
                chat_id=chat_id,
                content=content,
                reply_to=reply_to,
                metadata=metadata,
            )
            if result.success:
                logger.info("[sendblue] Send succeeded on retry %d", attempt)
                return result

            error_str = result.error or ""
            pattern_retryable = self._is_retryable_error(error_str)
            if self._is_timeout_error(error_str) and not pattern_retryable:
                return result
            if not (result.retryable or pattern_retryable):
                logger.warning("[sendblue] Send failed permanently: %s", error_str)
                return result

        logger.error(
            "[sendblue] Failed to deliver response after %d retries: %s",
            max_retries,
            error_str,
        )
        return result

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        del metadata
        try:
            kind, target = self._resolve_target(chat_id)
            if kind == "group" or not target:
                return
            await self.client.send_typing_indicator(target)
        except Exception:
            logger.debug("Sendblue typing indicator failed", exc_info=True)

    async def _attach_local_or_remote(
        self,
        chat_id: str,
        media: str,
        caption: Optional[str] = None,
    ) -> SendResult:
        """Upload a local file (or pass through a URL) and send it as a Sendblue attachment."""
        if not media:
            return SendResult(success=False, error="Missing media path or URL")

        try:
            kind, target = self._resolve_target(chat_id)
        except ValueError as exc:
            return SendResult(success=False, error=str(exc), retryable=False)
        if not target:
            return SendResult(success=False, error="Missing destination phone number or group id")

        media_url = media
        if _is_local_media(media):
            try:
                media_url = await self.client.upload_file(media)
                logger.info("Sendblue uploaded local file %s -> %s", media, media_url)
            except (FileNotFoundError, ValueError) as exc:
                return SendResult(success=False, error=str(exc), retryable=False)
            except Exception as exc:
                return SendResult(success=False, error=str(exc), retryable=True)

        try:
            message_id = await self._send_chunk(
                kind, target, self.format_message(caption or ""), media_url=media_url
            )
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del reply_to, metadata
        return await self._attach_local_or_remote(chat_id, image_url, caption)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del reply_to, metadata, kwargs
        return await self._attach_local_or_remote(chat_id, image_path, caption)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del file_name, reply_to, metadata, kwargs
        return await self._attach_local_or_remote(chat_id, file_path, caption)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del reply_to, metadata, kwargs
        return await self._attach_local_or_remote(chat_id, video_path, caption)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> SendResult:
        del reply_to, metadata, kwargs
        return await self._attach_local_or_remote(chat_id, audio_path, caption)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        kind, target = self._resolve_target(chat_id)
        if kind == "group":
            return {"name": f"Sendblue group {target[-6:]}", "type": "group", "id": target}
        return {"name": target, "type": "dm", "id": target}

    # ------------------------------------------------------------------
    # Group creation & management
    #
    # Not part of the BasePlatformAdapter contract -- plain public methods, as
    # with MatrixAdapter.create_room() / invite_user().
    # ------------------------------------------------------------------

    async def create_group(
        self,
        numbers: List[str],
        content: str = "",
        *,
        media_url: str = "",
    ) -> SendResult:
        """Create a group seeded with *content*.

        On success ``raw_response`` carries ``{"group_id", "chat_id"}``. Both
        hold the same value; ``chat_id`` is the name the caller wants, because
        it can be passed straight back into ``send()``.
        """
        try:
            # Only the first chunk can ride the creation POST -- it is the one
            # that carries `numbers`. The rest go to the id it returns.
            chunks = _split_message(self.format_message(content)) or [""]
            group_id, first_id = await self.client.create_group(
                numbers, chunks[0], media_url=media_url
            )
            message_ids = [first_id]
            for chunk in chunks[1:]:
                message_ids.append(await self._send_chunk("group", group_id, chunk))
            return SendResult(
                success=True,
                message_id=",".join(message_ids),
                raw_response={"group_id": group_id, "chat_id": group_id},
            )
        except ValueError as exc:
            return SendResult(success=False, error=str(exc), retryable=False)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def add_recipient_to_group(self, chat_id: str, number: str) -> SendResult:
        """Add one recipient to the group addressed by *chat_id*."""
        try:
            kind, target = self._resolve_target(chat_id)
            if kind != "group":
                return SendResult(
                    success=False,
                    error="add_recipient_to_group requires a Sendblue group id",
                    retryable=False,
                )
            payload = await self.client.add_recipient_to_group(target, number)
            # Sendblue returns a bare status; there is no roster read-back, so
            # do not claim more than that the call was accepted.
            return SendResult(success=True, raw_response=payload)
        except ValueError as exc:
            return SendResult(success=False, error=str(exc), retryable=False)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)


def check_requirements() -> bool:
    """Return whether Hermes' HTTP client dependency is available."""
    return HTTPX_AVAILABLE


def validate_config(config: Any) -> bool:
    settings = _settings_from_config(config)
    return bool(settings.api_key and settings.api_secret and settings.phone_number)


def is_connected(config: Any) -> bool:
    return validate_config(config)


def _env_enablement() -> Optional[Dict[str, Any]]:
    settings = _settings_from_config(type("Config", (), {"extra": {}})())
    if not (settings.api_key and settings.api_secret and settings.phone_number):
        return None

    seed: Dict[str, Any] = {
        "api_key": settings.api_key,
        "api_secret": settings.api_secret,
        "phone_number": settings.phone_number,
        "api_base": settings.api_base,
        "poll_interval_seconds": settings.poll_interval_seconds,
    }

    home = _env_first("SENDBLUE_HOME_CHANNEL", "SENDBLUE_HOME_PHONE")
    if home:
        try:
            _, home_target = _resolve_message_target(home)
        except ValueError:
            logger.warning("Ignoring invalid SENDBLUE_HOME_CHANNEL group id")
        else:
            seed["home_channel"] = {"chat_id": home_target, "name": home_target}

    if settings.webhook_enabled:
        seed["webhook"] = {
            "enabled": True,
            "host": settings.webhook_host,
            "port": settings.webhook_port,
            "path": settings.webhook_path,
            "secret": settings.webhook_secret,
        }

    return seed


async def _standalone_send(
    pconfig: Any,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    del thread_id, force_document
    settings = _settings_from_config(pconfig)
    if not (settings.api_key and settings.api_secret and settings.phone_number):
        return {"error": "Sendblue standalone send: missing SENDBLUE_API_KEY, SENDBLUE_API_SECRET, or SENDBLUE_PHONE_NUMBER"}

    client = SendblueClient(settings)
    try:
        await client.open()
        kind, target = _resolve_message_target(
            chat_id or _env_first("SENDBLUE_HOME_CHANNEL", "SENDBLUE_HOME_PHONE")
        )
        if not target:
            return {"error": "Sendblue standalone send: missing chat_id or SENDBLUE_HOME_CHANNEL"}

        async def send_target(content: str = "", *, media_url: str = "") -> str:
            if kind == "group":
                return await client.send_group_message(target, content, media_url=media_url)
            return await client.send_message(target, content, media_url=media_url)

        ids: List[str] = []
        # Strip markdown like the interactive send() path — Sendblue is plain text.
        for chunk in _split_message(strip_markdown(message or "")):
            ids.append(await send_target(chunk))

        for media in media_files or []:
            media_str = str(media)
            if _is_local_media(media_str):
                try:
                    uploaded_url = await client.upload_file(media_str)
                except Exception as exc:
                    return {
                        "error": (
                            f"Sendblue standalone send failed to upload {media_str}: {exc}"
                        )
                    }
                ids.append(await send_target("", media_url=uploaded_url))
            else:
                ids.append(await send_target("", media_url=media_str))

        return {"success": True, "message_id": ",".join(ids) if ids else ""}
    except Exception as exc:
        return {"error": f"Sendblue standalone send failed: {exc}"}
    finally:
        try:
            await client.aclose()
        except Exception:
            logger.debug("Sendblue standalone HTTP client close failed", exc_info=True)


def interactive_setup() -> None:
    from hermes_cli.setup import (
        get_env_value,
        print_header,
        print_info,
        print_success,
        print_warning,
        prompt,
        prompt_yes_no,
        save_env_value,
    )

    print_header("Sendblue")
    existing = get_env_value("SENDBLUE_PHONE_NUMBER")
    if existing:
        print_info(f"Sendblue is already configured for {existing}")
        if not prompt_yes_no("Reconfigure Sendblue?", False):
            return

    print_info("Sendblue lets Hermes talk over iMessage, SMS, and RCS.")
    api_key = prompt("Sendblue API key", default=get_env_value("SENDBLUE_API_KEY") or "", password=True)
    api_secret = prompt("Sendblue API secret", password=True)
    phone_number = prompt("Your Sendblue phone number", default=existing or "")

    if not (api_key and api_secret and phone_number):
        print_warning("API key, API secret, and phone number are required.")
        return

    save_env_value("SENDBLUE_API_KEY", api_key.strip())
    save_env_value("SENDBLUE_API_SECRET", api_secret.strip())
    save_env_value("SENDBLUE_PHONE_NUMBER", _normalize_phone(phone_number))

    allowed = prompt(
        "Allowed phone numbers (comma-separated, leave empty to use pairing)",
        default=get_env_value("SENDBLUE_ALLOWED_USERS") or "",
    )
    save_env_value("SENDBLUE_ALLOWED_USERS", ",".join(_normalize_phone(p) for p in allowed.split(",") if p.strip()))

    home = prompt(
        "Default delivery phone or group id for cron/notifications (optional)",
        default=get_env_value("SENDBLUE_HOME_CHANNEL") or "",
    )
    if home:
        try:
            _, home_target = _resolve_message_target(home)
        except ValueError as exc:
            print_warning(f"{exc}; SENDBLUE_HOME_CHANNEL was not changed.")
        else:
            save_env_value("SENDBLUE_HOME_CHANNEL", home_target)

    if prompt_yes_no("Enable webhook mode? (requires a public HTTPS tunnel/proxy)", False):
        secret = prompt(f"Webhook secret (minimum {MIN_WEBHOOK_SECRET_CHARS} chars)", password=True)
        if not _is_valid_webhook_secret(secret):
            print_warning(
                f"Webhook secret must be at least {MIN_WEBHOOK_SECRET_CHARS} chars and cannot be 'change-me'. "
                "Webhook mode was not enabled."
            )
            save_env_value("SENDBLUE_WEBHOOK_ENABLED", "false")
            print_success("Sendblue configuration saved to ~/.hermes/.env")
            print_info("Restart the gateway for changes to take effect: hermes gateway restart")
            return

        save_env_value("SENDBLUE_WEBHOOK_ENABLED", "true")
        port = prompt("Local webhook port", default=get_env_value("SENDBLUE_WEBHOOK_PORT") or "3141")
        path = prompt("Webhook path", default=get_env_value("SENDBLUE_WEBHOOK_PATH") or "/webhook/sendblue")
        save_env_value("SENDBLUE_WEBHOOK_PORT", port or "3141")
        save_env_value("SENDBLUE_WEBHOOK_PATH", path or "/webhook/sendblue")
        save_env_value("SENDBLUE_WEBHOOK_SECRET", secret)
    else:
        save_env_value("SENDBLUE_WEBHOOK_ENABLED", "false")

    print_success("Sendblue configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway for changes to take effect: hermes gateway restart")


def register(ctx: Any) -> None:
    ctx.register_platform(
        name="sendblue",
        label="Sendblue",
        adapter_factory=lambda cfg: SendblueAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["SENDBLUE_API_KEY", "SENDBLUE_API_SECRET", "SENDBLUE_PHONE_NUMBER"],
        install_hint="Install this plugin under ~/.hermes/plugins/sendblue and set Sendblue credentials",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="SENDBLUE_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="SENDBLUE_ALLOWED_USERS",
        allow_all_env="SENDBLUE_ALLOW_ALL_USERS",
        max_message_length=MAX_SENDBLUE_BODY_CHARS,
        pii_safe=True,
        emoji="SB",
        allow_update_command=True,
        platform_hint=(
            "You are chatting through Sendblue over iMessage/SMS/RCS. "
            "Phone screens are small, so keep replies concise unless the user asks for detail. "
            "Sendblue delivers images, videos, audio, and documents as native iMessage/SMS attachments — "
            "local file paths in MEDIA:<path> tags are uploaded automatically."
        ),
    )
