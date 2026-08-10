"""Unit tests for the local file upload helpers."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx

ROOT = Path(__file__).resolve().parents[1]


def _load_adapter():
    """Load adapter.py as a standalone module without hermes_cli/gateway installed."""
    # adapter.py imports from `gateway.*`. Prefer the real hermes-agent core when
    # it's importable (e.g. running inside a hermes-agent checkout); otherwise stub
    # gateway.* so this test still runs anywhere.
    try:
        import gateway.platforms.base  # noqa: F401

        _real_core = True
    except Exception:
        _real_core = False

    if not _real_core and "gateway" not in sys.modules:
        gateway_mod = type(sys)("gateway")
        config_mod = type(sys)("gateway.config")
        config_mod.Platform = lambda name: name  # type: ignore[attr-defined]
        platforms_mod = type(sys)("gateway.platforms")
        base_mod = type(sys)("gateway.platforms.base")

        class _StubAdapter:
            def __init__(self, *args, **kwargs):
                pass

            def _acquire_platform_lock(self, *args, **kwargs):
                return True

            def _release_platform_lock(self, *args, **kwargs):
                pass

            def _set_fatal_error(self, *args, **kwargs):
                pass

            def _mark_connected(self):
                pass

            def _mark_disconnected(self):
                pass

            def build_source(self, **kwargs):
                return kwargs

            async def handle_message(self, event):
                pass

        base_mod.BasePlatformAdapter = _StubAdapter  # type: ignore[attr-defined]
        base_mod.MessageEvent = SimpleNamespace  # type: ignore[attr-defined]

        class _MessageType:
            TEXT = "text"
            PHOTO = "photo"
            VIDEO = "video"
            AUDIO = "audio"
            VOICE = "voice"
            DOCUMENT = "document"

        base_mod.MessageType = _MessageType  # type: ignore[attr-defined]

        class _SendResult(SimpleNamespace):
            pass

        base_mod.SendResult = _SendResult  # type: ignore[attr-defined]
        base_mod.cache_image_from_bytes = lambda *a, **k: "/tmp/fake-image"  # type: ignore[attr-defined]
        base_mod.cache_audio_from_bytes = lambda *a, **k: "/tmp/fake-audio"  # type: ignore[attr-defined]
        base_mod.cache_video_from_bytes = lambda *a, **k: "/tmp/fake-video"  # type: ignore[attr-defined]
        base_mod.cache_document_from_bytes = lambda *a, **k: "/tmp/fake-doc"  # type: ignore[attr-defined]

        sys.modules["gateway"] = gateway_mod
        sys.modules["gateway.config"] = config_mod
        sys.modules["gateway.platforms"] = platforms_mod
        sys.modules["gateway.platforms.base"] = base_mod

    spec = importlib.util.spec_from_file_location("hermes_sendblue_adapter", ROOT / "adapter.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["hermes_sendblue_adapter"] = module
    spec.loader.exec_module(module)
    return module


adapter = _load_adapter()


class IsLocalMediaTests(unittest.TestCase):
    def test_http_url_is_not_local(self) -> None:
        self.assertFalse(adapter._is_local_media("http://example.com/x.png"))

    def test_https_url_is_not_local(self) -> None:
        self.assertFalse(adapter._is_local_media("https://cdn.example.com/x.png"))

    def test_absolute_path_is_local(self) -> None:
        self.assertTrue(adapter._is_local_media("/tmp/foo.png"))

    def test_relative_path_is_local(self) -> None:
        self.assertTrue(adapter._is_local_media("./bar.pdf"))

    def test_empty_is_not_local(self) -> None:
        self.assertFalse(adapter._is_local_media(""))

    def test_none_is_not_local(self) -> None:
        self.assertFalse(adapter._is_local_media(None))


class UploadGuardTests(unittest.TestCase):
    @staticmethod
    def _settings():
        return adapter.SendblueSettings(
            api_key="k",
            api_secret="s",
            phone_number="+15550000000",
        )

    def _client(self, handler=None) -> "adapter.SendblueClient":
        transport = httpx.MockTransport(handler) if handler is not None else None
        return adapter.SendblueClient(self._settings(), transport=transport)

    def test_upload_posts_httpx_multipart_contract(self) -> None:
        captured: dict = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["method"] = request.method
            captured["url"] = str(request.url)
            captured["headers"] = request.headers
            captured["body"] = await request.aread()
            captured["timeout"] = request.extensions["timeout"]
            return httpx.Response(
                200, json={"status": "OK", "media_url": "https://cdn.sendblue.test/abc"}
            )

        async def run() -> str:
            async with self._client(handler) as client:
                return await client.upload_file_from_bytes(
                    'weird"\nname.png', b"\x00\x01RAW\xff"
                )

        url = asyncio.run(run())
        self.assertEqual(url, "https://cdn.sendblue.test/abc")
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["url"], "https://api.sendblue.com/api/upload-file")
        self.assertIn("multipart/form-data; boundary=", captured["headers"]["content-type"])
        self.assertEqual(captured["headers"]["sb-api-key-id"], "k")
        self.assertEqual(captured["headers"]["sb-api-secret-key"], "s")
        self.assertIn(b'name="file"', captured["body"])
        self.assertIn(b'filename="weirdname.png"', captured["body"])
        self.assertIn(b"Content-Type: application/octet-stream", captured["body"])
        self.assertIn(b"\x00\x01RAW\xff", captured["body"])
        self.assertEqual(captured["timeout"]["write"], 60.0)

    def test_upload_raises_when_response_missing_media_url(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"status": "OK"})

        async def run() -> None:
            async with self._client(handler) as client:
                await client.upload_file_from_bytes("x.png", b"data")

        with self.assertRaisesRegex(RuntimeError, "no media_url"):
            asyncio.run(run())

    def test_upload_http_error_preserves_status_and_body(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(413, text="too large upstream")

        async def run() -> None:
            async with self._client(handler) as client:
                await client.upload_file_from_bytes("x.png", b"data")

        with self.assertRaisesRegex(RuntimeError, "API error 413: too large upstream"):
            asyncio.run(run())

    def test_upload_connection_error_is_wrapped(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("network down", request=request)

        async def run() -> None:
            async with self._client(handler) as client:
                await client.upload_file_from_bytes("x.png", b"data")

        with self.assertRaisesRegex(RuntimeError, "API connection error: network down"):
            asyncio.run(run())

    def test_client_context_closes_httpx_client(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={})

        async def run():
            client = self._client(handler)
            async with client:
                opened = client._http_client
                self.assertIsNotNone(opened)
            return client, opened

        client, opened = asyncio.run(run())
        self.assertIsNone(client._http_client)
        self.assertTrue(opened.is_closed)

    def test_empty_bytes_rejected(self) -> None:
        client = self._client()
        with self.assertRaises(ValueError):
            asyncio.run(client.upload_file_from_bytes("empty.png", b""))

    def test_oversize_bytes_rejected(self) -> None:
        client = self._client()
        oversize = b"\x00" * (adapter.SENDBLUE_MAX_UPLOAD_BYTES + 1)
        with self.assertRaises(ValueError):
            asyncio.run(client.upload_file_from_bytes("big.bin", oversize))

    def test_missing_file_raises_filenotfound(self) -> None:
        client = self._client()
        with self.assertRaises(FileNotFoundError):
            asyncio.run(client.upload_file("/nonexistent/path/that/does/not/exist.png"))


class HttpClientTests(unittest.TestCase):
    @staticmethod
    def _settings():
        settings = adapter.SendblueSettings(
            api_key="k",
            api_secret="s",
            phone_number="+15550000000",
        )
        return settings

    def test_json_request_uses_json_and_auth_headers(self) -> None:
        captured: dict = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            captured["body"] = await request.aread()
            return httpx.Response(200, json={"message_handle": "m-1"})

        async def run() -> str:
            client = adapter.SendblueClient(
                self._settings(), transport=httpx.MockTransport(handler)
            )
            async with client:
                return await client.send_message("+15551112222", "hello")

        self.assertEqual(asyncio.run(run()), "m-1")
        request = captured["request"]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.url.path, "/api/send-message")
        self.assertEqual(request.headers["sb-api-key-id"], "k")
        self.assertEqual(request.headers["content-type"], "application/json")
        self.assertEqual(
            json.loads(captured["body"]),
            {
                "number": "+15551112222",
                "from_number": "+15550000000",
                "content": "hello",
            },
        )

    def test_api_redirect_is_not_followed(self) -> None:
        calls = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))
            return httpx.Response(302, headers={"Location": "https://other.test/secret"})

        async def run() -> None:
            client = adapter.SendblueClient(
                self._settings(), transport=httpx.MockTransport(handler)
            )
            async with client:
                await client.send_message("+15551112222", "hello")

        with self.assertRaisesRegex(RuntimeError, "API error 302"):
            asyncio.run(run())
        self.assertEqual(calls, ["https://api.sendblue.com/api/send-message"])

    def test_polling_filters_are_encoded_as_query_parameters(self) -> None:
        captured = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["request"] = request
            return httpx.Response(200, json={"data": [], "pagination": {"total": 0}})

        async def run():
            client = adapter.SendblueClient(
                self._settings(), transport=httpx.MockTransport(handler)
            )
            async with client:
                return await client.list_inbound_messages(
                    datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
                )

        self.assertEqual(asyncio.run(run()), [])
        request = captured["request"]
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.url.path, "/api/v2/messages")
        self.assertEqual(request.url.params["is_outbound"], "false")
        self.assertEqual(request.url.params["sendblue_number"], "+15550000000")
        self.assertEqual(request.url.params["offset"], "0")
        self.assertEqual(request.url.params["created_at_gte"], "2026-08-10T11:59:58Z")

    def test_request_cancellation_propagates(self) -> None:
        async def run() -> None:
            request_started = asyncio.Event()
            never_finishes = asyncio.Event()

            async def handler(request: httpx.Request) -> httpx.Response:
                request_started.set()
                await never_finishes.wait()
                return httpx.Response(200, json={})

            client = adapter.SendblueClient(
                self._settings(), transport=httpx.MockTransport(handler)
            )
            async with client:
                task = asyncio.create_task(
                    client.send_message("+15551112222", "hello")
                )
                await request_started.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

        asyncio.run(run())

    def test_check_requirements_tracks_httpx_availability(self) -> None:
        with patch.object(adapter, "HTTPX_AVAILABLE", False):
            self.assertFalse(adapter.check_requirements())


class _ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk


class MediaDownloadTests(unittest.TestCase):
    @staticmethod
    def _settings():
        return adapter.SendblueSettings(
            api_key="k",
            api_secret="s",
            phone_number="+15550000000",
        )

    def _client(self, handler) -> "adapter.SendblueClient":
        return adapter.SendblueClient(
            self._settings(), transport=httpx.MockTransport(handler)
        )

    def test_streams_media_and_normalizes_content_type_without_api_secrets(self) -> None:
        captured = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured["headers"] = request.headers
            return httpx.Response(
                200,
                headers={"Content-Type": "image/png; charset=binary"},
                stream=_ChunkStream([b"abc", b"def"]),
            )

        async def run():
            async with self._client(handler) as client:
                return await client.download_media("https://cdn.sendblue.test/image.png")

        data, content_type = asyncio.run(run())
        self.assertEqual(data, b"abcdef")
        self.assertEqual(content_type, "image/png")
        self.assertNotIn("sb-api-key-id", captured["headers"])
        self.assertNotIn("sb-api-secret-key", captured["headers"])

    def test_accepts_exact_limit_and_rejects_first_byte_over_limit(self) -> None:
        async def exact_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, stream=_ChunkStream([b"abc", b"def"]))

        async def oversized_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, stream=_ChunkStream([b"abc", b"def", b"g"]))

        async def download(handler):
            async with self._client(handler) as client:
                return await client.download_media("https://cdn.sendblue.test/file")

        with patch.object(adapter, "SENDBLUE_MAX_INBOUND_MEDIA_BYTES", 6):
            data, _ = asyncio.run(download(exact_handler))
            self.assertEqual(data, b"abcdef")
            with self.assertRaisesRegex(RuntimeError, "Inbound media exceeds 6 bytes"):
                asyncio.run(download(oversized_handler))

    def test_rejects_non_https_before_opening_client(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("transport must not be reached")

        client = self._client(handler)
        with self.assertRaisesRegex(RuntimeError, "Refusing non-HTTPS"):
            asyncio.run(client.download_media("http://cdn.sendblue.test/file"))
        self.assertIsNone(client._http_client)

    def test_follows_public_https_redirect_without_credentials(self) -> None:
        requests = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.host == "cdn.sendblue.test":
                return httpx.Response(
                    302, headers={"Location": "https://assets.sendblue.test/final"}
                )
            return httpx.Response(200, content=b"media")

        async def run():
            async with self._client(handler) as client:
                return await client.download_media("https://cdn.sendblue.test/start")

        data, _ = asyncio.run(run())
        self.assertEqual(data, b"media")
        self.assertEqual(
            [request.url.host for request in requests],
            ["cdn.sendblue.test", "assets.sendblue.test"],
        )
        for request in requests:
            self.assertNotIn("sb-api-key-id", request.headers)

    def test_rejects_authenticated_cross_origin_redirect(self) -> None:
        calls = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return httpx.Response(
                302, headers={"Location": "https://other.test/final"}
            )

        async def run():
            async with self._client(handler) as client:
                await client.download_media(
                    "https://cdn.sendblue.test/start", authenticated=True
                )

        with self.assertRaisesRegex(RuntimeError, "credentials across media origins"):
            asyncio.run(run())
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].headers["sb-api-key-id"], "k")

    def test_wraps_media_http_and_connection_errors(self) -> None:
        async def status_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text="missing")

        async def connection_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("stalled", request=request)

        async def download(handler):
            async with self._client(handler) as client:
                return await client.download_media("https://cdn.sendblue.test/file")

        with self.assertRaisesRegex(RuntimeError, "media download error 404"):
            asyncio.run(download(status_handler))
        with self.assertRaisesRegex(RuntimeError, "connection error: stalled"):
            asyncio.run(download(connection_handler))


class FormatMessageTests(unittest.TestCase):
    """Outbound markdown stripping for the plain-text iMessage/SMS/RCS transport.

    Mirrors the SMS/BlueBubbles/Photon convention (``format_message`` ->
    ``strip_markdown``). Runs against whichever ``strip_markdown`` the adapter
    loaded — the real core helper when present, else the local fallback, which
    is regex-for-regex identical.
    """

    @staticmethod
    def _fmt(text: str) -> str:
        return adapter.SendblueAdapter.format_message(SimpleNamespace(), text)

    def test_bold_and_italic_unwrapped(self) -> None:
        self.assertEqual(self._fmt("**bold** and *italic*"), "bold and italic")

    def test_headings_stripped(self) -> None:
        self.assertEqual(self._fmt("# Title\nbody"), "Title\nbody")

    def test_inline_code_unwrapped(self) -> None:
        self.assertEqual(self._fmt("run `make test` now"), "run make test now")

    def test_code_fence_removed_but_body_kept(self) -> None:
        out = self._fmt("```python\nprint(1)\n```")
        self.assertIn("print(1)", out)
        self.assertNotIn("```", out)

    def test_markdown_link_collapses_to_anchor_text(self) -> None:
        # Documented lossy case shared with SMS/BlueBubbles/Photon: the URL drops.
        self.assertEqual(self._fmt("see [the docs](https://x.com/a)"), "see the docs")

    def test_raw_url_is_preserved(self) -> None:
        self.assertEqual(self._fmt("see https://x.com/a"), "see https://x.com/a")

    def test_plain_text_unchanged(self) -> None:
        self.assertEqual(self._fmt("just a normal message"), "just a normal message")

    def test_empty_is_safe(self) -> None:
        self.assertEqual(self._fmt(""), "")


if __name__ == "__main__":
    unittest.main()
