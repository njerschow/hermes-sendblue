"""Unit tests for Sendblue group detection, gating, API payloads, and routing.

Existing groups are addressed by their stable ``group_id``; Sendblue expands
that id to the persisted participant roster. These drive inbound and outbound
adapter methods with a duck-typed ``self`` and reuse ``test_upload``'s adapter
loader, so they run under both the stub gateway and the real installed core.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

# Ensure sibling test modules are importable regardless of how unittest/pytest
# is invoked (``-m unittest tests.test_groups`` does not add tests/ to sys.path).
sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_upload  # loads the adapter (real core when importable, else a stub)  # noqa: E402

adapter = test_upload.adapter

OUR = "+15550000000"
SENDER = "+15551112222"
GROUP = "sb_group_abc123"


def _bind(fake, *names):
    """Attach real SendblueAdapter methods to a duck-typed ``fake`` self."""
    for name in names:
        setattr(fake, name, getattr(adapter.SendblueAdapter, name).__get__(fake))


class _Base(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = adapter.ProcessedMessageStore(Path(tmp.name) / "s.sqlite3")
        self.addCleanup(self.store.close)

    def _settings(self, **over):
        # require_mention=True here so the inbound tests exercise the wake-word
        # path; production defaults it to False (auto-respond).
        base = dict(
            api_key="k",
            api_secret="s",
            phone_number=OUR,
            require_mention=True,
            mention_patterns_raw=None,
        )
        base.update(over)
        return adapter.SendblueSettings(**base)


class InboundGroupTests(_Base):
    def _inbound(self, msg, settings=None, client=None):
        settings = settings or self._settings()
        events: list = []

        async def handle_message(event):
            events.append(event)

        fake = SimpleNamespace(
            settings=settings,
            _store=self.store,
            client=client or SimpleNamespace(),
            _mention_patterns=adapter._compile_mention_patterns(settings.mention_patterns_raw),
            build_source=lambda **kw: SimpleNamespace(**kw),
            handle_message=handle_message,
        )
        asyncio.run(adapter.SendblueAdapter._handle_sendblue_message(fake, msg))
        return events

    def _group_msg(self, **over):
        # Mirrors a real Sendblue group payload: empty participants, our number in
        # to_number/sendblue_number, group_id with the sb_group_ prefix.
        msg = {
            "message_handle": "g-1",
            "from_number": SENDER,
            "to_number": OUR,
            "sendblue_number": OUR,
            "group_id": GROUP,
            "participants": [],
            "group_display_name": None,
            "message_type": "group",
            "is_outbound": False,
            "content": "hermes what's up",
            "date_sent": "2026-06-15T00:00:00Z",
        }
        msg.update(over)
        return msg

    def _dm_msg(self, **over):
        msg = {
            "message_handle": "d-1",
            "from_number": SENDER,
            "to_number": OUR,
            "sendblue_number": OUR,
            "is_outbound": False,
            "content": "hello",
            "date_sent": "2026-06-15T00:00:00Z",
        }
        msg.update(over)
        return msg

    def test_group_detected_and_routed(self):
        ev = self._inbound(self._group_msg())
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0].source.chat_type, "group")
        self.assertEqual(ev[0].source.chat_id, GROUP)  # session keyed on group_id
        self.assertEqual(ev[0].source.user_id, SENDER)  # sender, distinct from group
        self.assertEqual(ev[0].text, "what's up")  # wake-word stripped

    def test_dm_unchanged(self):
        ev = self._inbound(self._dm_msg())
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0].source.chat_type, "dm")
        self.assertEqual(ev[0].source.chat_id, SENDER)

    def test_dropped_when_wrong_recipient(self):
        ev = self._inbound(self._group_msg(to_number="+19998887777", sendblue_number="+19998887777"))
        self.assertEqual(ev, [])

    def test_require_mention_false_replies_to_everything(self):
        ev = self._inbound(
            self._group_msg(content="lol ok"),
            settings=self._settings(require_mention=False),
        )
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0].text, "lol ok")

    def test_dropped_without_wakeword(self):
        ev = self._inbound(self._group_msg(content="just chatting amongst ourselves"))
        self.assertEqual(ev, [])

    def test_declared_group_without_group_id_is_not_misrouted_as_dm(self):
        ev = self._inbound(self._group_msg(group_id=""))
        self.assertEqual(ev, [])

    def test_dm_never_gated(self):
        ev = self._inbound(self._dm_msg(content="no wake-word here"))
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0].text, "no wake-word here")

    def test_group_read_receipt_is_suppressed(self):
        calls = []

        async def mark_read(number):
            calls.append(number)

        ev = self._inbound(
            self._group_msg(content="ordinary chatter"),
            settings=self._settings(require_mention=False, mark_read=True),
            client=SimpleNamespace(mark_read=mark_read),
        )
        self.assertEqual(len(ev), 1)
        self.assertEqual(calls, [])


class OutboundGroupTests(_Base):
    def _adapter_and_calls(self, settings=None):
        settings = settings or self._settings()
        calls = SimpleNamespace(dm=[], group=[])

        async def send_message(to_number, content="", *, media_url=""):
            calls.dm.append((to_number, content, media_url))
            return "m-dm"

        async def send_group_message(group_id, content="", *, media_url=""):
            calls.group.append((group_id, content, media_url))
            return "m-grp"

        async def upload_file(path):
            return "https://cdn.sendblue.test/uploaded"

        fake = SimpleNamespace(
            settings=settings,
            _store=self.store,
            client=SimpleNamespace(
                send_message=send_message,
                send_group_message=send_group_message,
                upload_file=upload_file,
            ),
        )
        _bind(fake, "format_message", "_resolve_target", "_send_chunk", "send", "_attach_local_or_remote")
        return fake, calls

    def test_send_to_group_preserves_group_id(self):
        fake, calls = self._adapter_and_calls()
        res = asyncio.run(adapter.SendblueAdapter.send(fake, GROUP, "**hi** there"))
        self.assertTrue(res.success)
        self.assertEqual(len(calls.group), 1)
        self.assertEqual(calls.dm, [])
        group_id, content, _media = calls.group[0]
        self.assertEqual(group_id, GROUP)
        self.assertEqual(content, "hi there")  # markdown stripped

    def test_send_to_phone_uses_dm_api(self):
        fake, calls = self._adapter_and_calls()
        res = asyncio.run(adapter.SendblueAdapter.send(fake, SENDER, "hello"))
        self.assertTrue(res.success)
        self.assertEqual(len(calls.dm), 1)
        self.assertEqual(calls.group, [])

    def test_group_media_reply_preserves_group_id(self):
        fake, calls = self._adapter_and_calls()
        res = asyncio.run(
            adapter.SendblueAdapter._attach_local_or_remote(
                fake, GROUP, "https://cdn.sendblue.test/photo.jpg", "**look**"
            )
        )
        self.assertTrue(res.success)
        self.assertEqual(len(calls.group), 1)
        group_id, content, media_url = calls.group[0]
        self.assertEqual(group_id, GROUP)
        self.assertEqual(content, "look")
        self.assertEqual(media_url, "https://cdn.sendblue.test/photo.jpg")

    def test_typing_is_suppressed_for_groups(self):
        calls = []

        async def send_typing_indicator(target):
            calls.append(target)

        fake = SimpleNamespace(client=SimpleNamespace(send_typing_indicator=send_typing_indicator))
        _bind(fake, "_resolve_target")
        asyncio.run(adapter.SendblueAdapter.send_typing(fake, GROUP))
        self.assertEqual(calls, [])

    def test_group_chat_info_is_group_typed(self):
        fake = SimpleNamespace()
        _bind(fake, "_resolve_target")
        info = asyncio.run(adapter.SendblueAdapter.get_chat_info(fake, GROUP))
        self.assertEqual(info["type"], "group")
        self.assertEqual(info["id"], GROUP)


class ClientGroupTests(_Base):
    def test_group_send_posts_exact_api_contract(self):
        settings = self._settings()
        client = adapter.SendblueClient(settings)
        calls = []

        def request(method, path, *, query=None, body=None):
            calls.append((method, path, query, body))
            return {"message_handle": "m-group"}

        client._json_request_sync = request
        result = asyncio.run(client.send_group_message(GROUP, "hello"))

        self.assertEqual(result, "m-group")
        self.assertEqual(
            calls,
            [("POST", "/api/send-group-message", None, {
                "group_id": GROUP,
                "from_number": OUR,
                "content": "hello",
            })],
        )

    def test_group_id_validator_matches_server_contract(self):
        self.assertTrue(adapter._is_group_chat_id(GROUP))
        self.assertTrue(adapter._is_group_chat_id("legacy_group_id_123"))
        self.assertFalse(adapter._is_group_chat_id("sb_group_bad value"))
        self.assertFalse(adapter._is_group_chat_id("not-a-group"))

    def test_unset_mention_setting_uses_hermes_defaults(self):
        patterns = adapter._compile_mention_patterns(None)
        self.assertEqual(len(patterns), 2)
        self.assertTrue(adapter._message_matches_mention_patterns("Hermes, help", patterns))

    def test_explicit_empty_mention_setting_disables_default_patterns(self):
        self.assertEqual(adapter._compile_mention_patterns(""), [])

    def test_group_settings_are_read_from_declared_env_vars(self):
        env = {
            "SENDBLUE_API_KEY": "k",
            "SENDBLUE_API_SECRET": "s",
            "SENDBLUE_PHONE_NUMBER": OUR,
            "SENDBLUE_REQUIRE_MENTION": "true",
            "SENDBLUE_MENTION_PATTERNS": "sendblue bot",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = adapter._settings_from_config(SimpleNamespace(extra={}))

        self.assertTrue(settings.require_mention)
        self.assertEqual(settings.mention_patterns_raw, "sendblue bot")

    def test_group_config_takes_precedence_over_env_vars(self):
        config = SimpleNamespace(extra={
            "require_mention": False,
            "mention_patterns": [r"(?i)hey sendblue"],
        })
        env = {
            "SENDBLUE_API_KEY": "k",
            "SENDBLUE_API_SECRET": "s",
            "SENDBLUE_PHONE_NUMBER": OUR,
            "SENDBLUE_REQUIRE_MENTION": "true",
            "SENDBLUE_MENTION_PATTERNS": "hermes",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = adapter._settings_from_config(config)

        self.assertFalse(settings.require_mention)
        self.assertEqual(settings.mention_patterns_raw, [r"(?i)hey sendblue"])

    def test_standalone_home_channel_preserves_group_id(self):
        calls = []

        class FakeClient:
            def __init__(self, settings):
                self.settings = settings

            async def send_group_message(self, group_id, content="", *, media_url=""):
                calls.append((group_id, content, media_url))
                return "cron-group"

            async def send_message(self, number, content="", *, media_url=""):
                raise AssertionError(f"group target was routed as DM: {number}")

        config = SimpleNamespace(extra={
            "api_key": "k",
            "api_secret": "s",
            "phone_number": OUR,
        })
        with patch.object(adapter, "SendblueClient", FakeClient), patch.dict(
            os.environ, {"SENDBLUE_HOME_CHANNEL": GROUP}, clear=False
        ):
            result = asyncio.run(adapter._standalone_send(config, "", "cron hello"))

        self.assertTrue(result["success"])
        self.assertEqual(calls, [(GROUP, "cron hello", "")])


if __name__ == "__main__":
    unittest.main()
