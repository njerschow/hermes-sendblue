"""Tests for Sendblue's plain-text-aware send retry policy."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

# Reuse the adapter loader that supports both an installed Hermes core and the
# lightweight gateway stubs used by this standalone plugin test suite.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_upload  # noqa: E402

adapter = test_upload.adapter


def _result(*, success: bool, error: str = "", retryable: bool = False):
    return adapter.SendResult(
        success=success,
        message_id="ok" if success else None,
        error=error or None,
        retryable=retryable,
    )


class _RetryHarness:
    """Duck-typed adapter that records calls and supplies queued results."""

    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.calls.append(
            {
                "chat_id": chat_id,
                "content": content,
                "reply_to": reply_to,
                "metadata": metadata,
            }
        )
        return self.results.pop(0)

    @staticmethod
    def _is_retryable_error(error):
        lowered = str(error or "").lower()
        return any(
            marker in lowered
            for marker in (
                "connecterror",
                "connectionerror",
                "connectionreset",
                "connectionrefused",
                "connecttimeout",
                "network",
                "broken pipe",
            )
        )

    @staticmethod
    def _is_timeout_error(error):
        lowered = str(error or "").lower()
        return (
            "timed out" in lowered
            or "readtimeout" in lowered
            or "writetimeout" in lowered
        )

    async def run(self, **kwargs):
        return await adapter.SendblueAdapter._send_with_retry(self, **kwargs)


class SendWithRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_sends_once_and_forwards_context(self):
        harness = _RetryHarness([_result(success=True)])
        metadata = {"source": "test"}

        result = await harness.run(
            chat_id="+15551112222",
            content="**hello**",
            reply_to="message-1",
            metadata=metadata,
        )

        self.assertTrue(result.success)
        self.assertEqual(len(harness.calls), 1)
        self.assertEqual(harness.calls[0]["content"], "**hello**")
        self.assertEqual(harness.calls[0]["reply_to"], "message-1")
        self.assertIs(harness.calls[0]["metadata"], metadata)

    async def test_transient_failure_retries_with_exponential_backoff(self):
        harness = _RetryHarness(
            [
                _result(success=False, error="ConnectError", retryable=True),
                _result(success=False, error="ConnectError", retryable=True),
                _result(success=True),
            ]
        )

        with (
            patch.object(adapter.random, "uniform", return_value=0.25),
            patch.object(adapter.asyncio, "sleep", new_callable=AsyncMock) as sleep,
        ):
            result = await harness.run(
                chat_id="+15551112222",
                content="hello",
                max_retries=2,
                base_delay=2.0,
            )

        self.assertTrue(result.success)
        self.assertEqual(len(harness.calls), 3)
        self.assertEqual([call.args[0] for call in sleep.await_args_list], [2.25, 4.25])

    async def test_ambiguous_timeout_is_not_retried_even_when_flagged_retryable(self):
        harness = _RetryHarness(
            [_result(success=False, error="ReadTimeout: request timed out", retryable=True)]
        )

        with patch.object(adapter.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            result = await harness.run(chat_id="+15551112222", content="hello")

        self.assertFalse(result.success)
        self.assertEqual(len(harness.calls), 1)
        sleep.assert_not_awaited()

    async def test_permanent_failure_returns_without_plain_text_fallback(self):
        harness = _RetryHarness(
            [_result(success=False, error="Invalid destination", retryable=False)]
        )

        result = await harness.run(chat_id="bad-target", content="**hello**")

        self.assertFalse(result.success)
        self.assertEqual(len(harness.calls), 1)
        self.assertNotIn("Response formatting failed", harness.calls[0]["content"])

    async def test_retry_exhaustion_does_not_send_banner_or_notice(self):
        failure = _result(success=False, error="ConnectionError", retryable=True)
        harness = _RetryHarness([failure, failure, failure])

        with (
            patch.object(adapter.random, "uniform", return_value=0),
            patch.object(adapter.asyncio, "sleep", new_callable=AsyncMock),
        ):
            result = await harness.run(
                chat_id="+15551112222",
                content="hello",
                max_retries=2,
                base_delay=0,
            )

        self.assertFalse(result.success)
        self.assertEqual(len(harness.calls), 3)
        self.assertTrue(
            all("Response formatting failed" not in call["content"] for call in harness.calls)
        )


if __name__ == "__main__":
    unittest.main()
