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
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error, parse, request

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger("gateway.platforms.sendblue")

DEFAULT_API_BASE = "https://api.sendblue.com"
DEFAULT_USER_AGENT = "sendblue-hermes/1.0 (+https://github.com/njerschow/hermes-sendblue)"
DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_LOOKBACK_SECONDS = 60
DEFAULT_LIMIT = 100
MAX_SENDBLUE_BODY_CHARS = 18_000
MAX_WEBHOOK_BODY_BYTES = 1024 * 1024
MIN_WEBHOOK_SECRET_CHARS = 16


def _is_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _env_first(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()
    return ""


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
    )


class SendblueClient:
    def __init__(self, settings: SendblueSettings):
        self.settings = settings

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": DEFAULT_USER_AGENT,
            "sb-api-key-id": self.settings.api_key,
            "sb-api-secret-key": self.settings.api_secret,
        }

    def _json_request_sync(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.settings.api_base}{path}"
        if query:
            url = f"{url}?{parse.urlencode(query)}"

        payload = json.dumps(body).encode("utf-8") if body is not None else None
        req = request.Request(url, data=payload, headers=self._headers(), method=method)
        try:
            with request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Sendblue API error {exc.code}: {raw}") from exc
        except error.URLError as exc:
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
            payload = await asyncio.to_thread(
                self._json_request_sync,
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

    async def send_message(
        self,
        to_number: str,
        content: str = "",
        *,
        media_url: str = "",
    ) -> str:
        body: Dict[str, Any] = {
            "number": _normalize_phone(to_number),
            "from_number": self.settings.phone_number,
        }
        if content:
            body["content"] = content
        if media_url:
            body["media_url"] = media_url
        if not body.get("content") and not body.get("media_url"):
            raise ValueError("Sendblue message requires content or media_url")

        payload = await asyncio.to_thread(
            self._json_request_sync,
            "POST",
            "/api/send-message",
            body=body,
        )
        return str(payload.get("message_handle") or payload.get("id") or int(time.time() * 1000))

    async def send_typing_indicator(self, to_number: str) -> None:
        body = {
            "number": _normalize_phone(to_number),
            "from_number": self.settings.phone_number,
        }
        await asyncio.to_thread(
            self._json_request_sync,
            "POST",
            "/api/send-typing-indicator",
            body=body,
        )

    async def mark_read(self, to_number: str) -> None:
        body = {
            "number": _normalize_phone(to_number),
            "from_number": self.settings.phone_number,
        }
        await asyncio.to_thread(
            self._json_request_sync,
            "POST",
            "/api/mark-read",
            body=body,
        )


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

        try:
            if self.settings.webhook_enabled:
                if not _is_valid_webhook_secret(self.settings.webhook_secret):
                    msg = (
                        "Webhook mode requires SENDBLUE_WEBHOOK_SECRET with at least "
                        f"{MIN_WEBHOOK_SECRET_CHARS} non-placeholder characters"
                    )
                    logger.error(msg)
                    self._set_fatal_error("webhook_secret_missing", msg, retryable=False)
                    self._release_platform_lock()
                    return False

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
        if media_url:
            media_line = f"[Media: {media_url}]"
            text = f"{content}\n\n{media_line}" if content else media_line
        if not text:
            return

        if self.settings.mark_read:
            try:
                await self.client.mark_read(from_number)
            except Exception:
                logger.debug("Sendblue mark-read failed", exc_info=True)

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
            message_type=MessageType.TEXT,
            source=source,
            raw_message={
                "message_handle": message_id,
                "service": message.get("service"),
                "message_type": message.get("message_type"),
                "has_media": bool(media_url),
            },
            message_id=message_id,
            timestamp=_parse_datetime(message.get("date_sent") or message.get("date_created")),
        )
        await self.handle_message(event)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del reply_to, metadata
        target = _normalize_phone(chat_id)
        if not target:
            return SendResult(success=False, error="Missing destination phone number")

        message_ids: List[str] = []
        try:
            for chunk in _split_message(content):
                message_ids.append(await self.client.send_message(target, chunk))
            if not message_ids:
                return SendResult(success=False, error="Empty message")
            return SendResult(success=True, message_id=",".join(message_ids))
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        del metadata
        try:
            await self.client.send_typing_indicator(chat_id)
        except Exception:
            logger.debug("Sendblue typing indicator failed", exc_info=True)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        del reply_to, metadata
        if not image_url.startswith(("http://", "https://")):
            return await self.send(chat_id=chat_id, content=f"{caption or ''}\n{image_url}".strip())
        try:
            message_id = await self.client.send_message(
                _normalize_phone(chat_id),
                caption or "",
                media_url=image_url,
            )
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        phone = _normalize_phone(chat_id)
        return {"name": phone, "type": "dm"}


def check_requirements() -> bool:
    """No optional dependencies are required; Hermes supplies the gateway API."""
    return True


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
        home_phone = _normalize_phone(home)
        seed["home_channel"] = {"chat_id": home_phone, "name": home_phone}

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
    target = _normalize_phone(chat_id or _env_first("SENDBLUE_HOME_CHANNEL", "SENDBLUE_HOME_PHONE"))
    if not target:
        return {"error": "Sendblue standalone send: missing chat_id or SENDBLUE_HOME_CHANNEL"}

    try:
        ids: List[str] = []
        for chunk in _split_message(message):
            ids.append(await client.send_message(target, chunk))

        for media in media_files or []:
            if str(media).startswith(("http://", "https://")):
                ids.append(await client.send_message(target, "", media_url=str(media)))
            else:
                ids.append(await client.send_message(target, f"Generated file: {media}"))

        return {"success": True, "message_id": ",".join(ids) if ids else ""}
    except Exception as exc:
        return {"error": f"Sendblue standalone send failed: {exc}"}


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
        "Default delivery phone for cron/notifications (optional)",
        default=get_env_value("SENDBLUE_HOME_CHANNEL") or "",
    )
    if home:
        save_env_value("SENDBLUE_HOME_CHANNEL", _normalize_phone(home))

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
            "Sendblue can deliver image URLs as media attachments; local generated files may need a public URL."
        ),
    )
