"""Unit tests for the local file upload helpers."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def _load_adapter():
    """Load adapter.py as a standalone module without hermes_cli/gateway installed."""
    # adapter.py imports from `gateway.*`; stub those out so this test runs anywhere.
    if "gateway" not in sys.modules:
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

        base_mod.MessageType = _MessageType  # type: ignore[attr-defined]

        class _SendResult(SimpleNamespace):
            pass

        base_mod.SendResult = _SendResult  # type: ignore[attr-defined]

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


class BuildMultipartTests(unittest.TestCase):
    def test_includes_boundary_header_and_footer(self) -> None:
        body = adapter._build_multipart("hello.png", b"\x89PNGdata", "BOUND123")
        self.assertIn(b"--BOUND123\r\n", body)
        self.assertTrue(body.endswith(b"\r\n--BOUND123--\r\n"))

    def test_includes_form_field_name_and_filename(self) -> None:
        body = adapter._build_multipart("doc.pdf", b"PDFBYTES", "BX")
        self.assertIn(b'name="file"', body)
        self.assertIn(b'filename="doc.pdf"', body)

    def test_uses_octet_stream_content_type(self) -> None:
        body = adapter._build_multipart("x", b"y", "BX")
        self.assertIn(b"Content-Type: application/octet-stream", body)

    def test_payload_bytes_are_embedded(self) -> None:
        payload = b"\x00\x01\x02RAW\xff"
        body = adapter._build_multipart("any.bin", payload, "BX")
        self.assertIn(payload, body)

    def test_filename_strips_quotes_and_newlines(self) -> None:
        body = adapter._build_multipart('weird"\nname.png', b"x", "BX")
        # Sanitized name should appear in the header.
        self.assertIn(b'filename="weirdname.png"', body)


class UploadGuardTests(unittest.TestCase):
    def _client(self) -> "adapter.SendblueClient":
        settings = adapter.SendblueSettings(
            api_key="k",
            api_secret="s",
            phone_number="+15550000000",
        )
        return adapter.SendblueClient(settings)

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

    def test_upload_posts_to_expected_endpoint(self) -> None:
        client = self._client()
        captured: dict = {}

        def fake_post(path, headers, body, *, timeout=60.0):
            captured["path"] = path
            captured["headers"] = headers
            captured["body"] = body
            return {"status": "OK", "media_url": "https://cdn.sendblue.test/abc"}

        with patch.object(client, "_raw_post_sync", side_effect=fake_post):
            url = asyncio.run(client.upload_file_from_bytes("hi.png", b"PNGBYTES"))

        self.assertEqual(url, "https://cdn.sendblue.test/abc")
        self.assertEqual(captured["path"], "/api/upload-file")
        self.assertIn("multipart/form-data; boundary=", captured["headers"]["Content-Type"])
        self.assertIn("sb-api-key-id", captured["headers"])
        self.assertIn(b'name="file"', captured["body"])
        self.assertIn(b"PNGBYTES", captured["body"])

    def test_upload_raises_when_response_missing_media_url(self) -> None:
        client = self._client()

        def fake_post(path, headers, body, *, timeout=60.0):
            return {"status": "OK"}

        with patch.object(client, "_raw_post_sync", side_effect=fake_post):
            with self.assertRaises(RuntimeError):
                asyncio.run(client.upload_file_from_bytes("x.png", b"data"))


if __name__ == "__main__":
    unittest.main()
