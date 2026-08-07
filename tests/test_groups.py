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

    def _recording_client(self, response, **settings_over):
        """A real SendblueClient whose HTTP layer records instead of sending."""
        client = adapter.SendblueClient(self._settings(**settings_over))
        calls = []

        def request(method, path, *, query=None, body=None):
            calls.append((method, path, query, body))
            return response

        client._json_request_sync = request
        return client, calls


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


class CreateGroupClientTests(_Base):
    """Wire contract for /api/send-group-message with a numbers array."""

    CREATED = {"message_handle": "m-1", "group_id": GROUP}

    def test_create_group_posts_numbers_payload(self):
        client, calls = self._recording_client(self.CREATED)
        group_id, message_id = asyncio.run(
            client.create_group([SENDER, "+15553334444"], "hi all")
        )

        self.assertEqual((group_id, message_id), (GROUP, "m-1"))
        self.assertEqual(
            calls,
            [("POST", "/api/send-group-message", None, {
                "numbers": [SENDER, "+15553334444"],
                "from_number": OUR,
                "content": "hi all",
            })],
        )

    def test_create_group_normalizes_and_dedupes_numbers(self):
        client, calls = self._recording_client(self.CREATED)
        asyncio.run(client.create_group(["555-111-2222", "+15551112222", "5553334444"], "hi"))
        self.assertEqual(calls[0][3]["numbers"], [SENDER, "+15553334444"])

    def test_create_group_drops_our_own_number(self):
        # Passing our own line is a plausible caller mistake; it must not count
        # toward the recipient minimum.
        client, calls = self._recording_client(self.CREATED)
        with self.assertRaises(ValueError):
            asyncio.run(client.create_group([OUR, SENDER], "hi"))
        self.assertEqual(calls, [])

    def test_create_group_requires_two_recipients(self):
        for numbers in ([], [SENDER]):
            client, calls = self._recording_client(self.CREATED)
            with self.assertRaises(ValueError):
                asyncio.run(client.create_group(numbers, "hi"))
            self.assertEqual(calls, [], "must reject before touching the wire")

    def test_create_group_requires_content_or_media(self):
        client, calls = self._recording_client(self.CREATED)
        with self.assertRaises(ValueError):
            asyncio.run(client.create_group([SENDER, "+15553334444"], ""))
        self.assertEqual(calls, [])

    def test_create_group_media_only_needs_no_content(self):
        client, calls = self._recording_client(self.CREATED)
        asyncio.run(
            client.create_group([SENDER, "+15553334444"], "", media_url="https://cdn/x.jpg")
        )
        self.assertEqual(calls[0][3]["media_url"], "https://cdn/x.jpg")
        self.assertNotIn("content", calls[0][3])

    def test_create_group_rejects_untrusted_server_group_id(self):
        client, _ = self._recording_client({"message_handle": "m-1", "group_id": "not-a-group"})
        with self.assertRaises(ValueError):
            asyncio.run(client.create_group([SENDER, "+15553334444"], "hi"))

    def test_create_group_rejects_missing_group_id(self):
        client, _ = self._recording_client({"message_handle": "m-1"})
        with self.assertRaises(ValueError):
            asyncio.run(client.create_group([SENDER, "+15553334444"], "hi"))

    def test_add_recipient_posts_modify_group_payload(self):
        client, calls = self._recording_client({"status": "OK"})
        result = asyncio.run(client.add_recipient_to_group(GROUP, "555-333-4444"))

        self.assertEqual(result, {"status": "OK"})
        self.assertEqual(
            calls,
            [("POST", "/api/modify-group", None, {
                "group_id": GROUP,
                "add_recipient": "+15553334444",
            })],
        )

    def test_add_recipient_rejects_non_group_id(self):
        for bad in (SENDER, "sb_group_bad value", ""):
            client, calls = self._recording_client({"status": "OK"})
            with self.assertRaises(ValueError):
                asyncio.run(client.add_recipient_to_group(bad, SENDER))
            self.assertEqual(calls, [])

    def test_add_recipient_requires_a_number(self):
        client, calls = self._recording_client({"status": "OK"})
        with self.assertRaises(ValueError):
            asyncio.run(client.add_recipient_to_group(GROUP, ""))
        self.assertEqual(calls, [])

    def test_dm_send_still_returns_only_a_message_id(self):
        # Regression guard on the _send_one -> _post_message refactor: the
        # str-returning contract both send_message and send_group_message rely
        # on must survive create_group needing the whole payload.
        client, calls = self._recording_client({"message_handle": "m-dm", "group_id": GROUP})
        self.assertEqual(asyncio.run(client.send_message(SENDER, "hi")), "m-dm")
        self.assertEqual(calls[0][1], "/api/send-message")


class AllowlistTests(_Base):
    """Outbound recipients are gated by SENDBLUE_ALLOWED_USERS."""

    CREATED = {"message_handle": "m-1", "group_id": GROUP}
    PAIR = [SENDER, "+15553334444"]

    def test_create_group_rejects_number_outside_allowlist(self):
        client, calls = self._recording_client(self.CREATED, allowed_users_raw=SENDER)
        with self.assertRaises(ValueError):
            asyncio.run(client.create_group(self.PAIR, "hi"))
        self.assertEqual(calls, [], "must reject before touching the wire")

    def test_allowlist_admits_fully_listed_recipients(self):
        client, calls = self._recording_client(
            self.CREATED, allowed_users_raw=f"{SENDER},+15553334444"
        )
        asyncio.run(client.create_group(self.PAIR, "hi"))
        self.assertEqual(len(calls), 1)

    def test_allowlist_matches_across_phone_formats(self):
        client, calls = self._recording_client(
            self.CREATED, allowed_users_raw="555-111-2222, (555) 333-4444"
        )
        asyncio.run(client.create_group(self.PAIR, "hi"))
        self.assertEqual(len(calls), 1)

    def test_allowlist_skipped_when_unset(self):
        client, calls = self._recording_client(self.CREATED, allowed_users_raw="")
        asyncio.run(client.create_group(self.PAIR, "hi"))
        self.assertEqual(len(calls), 1)

    def test_allowlist_skipped_when_allow_all(self):
        client, calls = self._recording_client(
            self.CREATED, allowed_users_raw=SENDER, allow_all_users=True
        )
        asyncio.run(client.create_group(self.PAIR, "hi"))
        self.assertEqual(len(calls), 1)

    def test_allowlist_star_allows_everyone(self):
        client, calls = self._recording_client(self.CREATED, allowed_users_raw="*")
        asyncio.run(client.create_group(self.PAIR, "hi"))
        self.assertEqual(len(calls), 1)

    def test_add_recipient_enforces_allowlist(self):
        client, calls = self._recording_client({"status": "OK"}, allowed_users_raw=SENDER)
        with self.assertRaises(ValueError):
            asyncio.run(client.add_recipient_to_group(GROUP, "+15553334444"))
        self.assertEqual(calls, [])

    def test_allowlist_error_redacts_phone_numbers(self):
        # The message lands verbatim in SendResult.error; plugin.yaml declares
        # pii_safe: true.
        client, _ = self._recording_client(self.CREATED, allowed_users_raw=SENDER)
        with self.assertRaises(ValueError) as caught:
            asyncio.run(client.create_group(self.PAIR, "hi"))
        message = str(caught.exception)
        self.assertNotIn("5553334444", message)
        self.assertIn("...4444", message)

    def test_allowlist_is_read_from_declared_env_vars(self):
        env = {
            "SENDBLUE_API_KEY": "k",
            "SENDBLUE_API_SECRET": "s",
            "SENDBLUE_PHONE_NUMBER": OUR,
            "SENDBLUE_ALLOWED_USERS": SENDER,
            "GATEWAY_ALLOWED_USERS": "+15553334444",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = adapter._settings_from_config(SimpleNamespace(extra={}))

        # Unioned, not first-wins -- core unions the two allowlists too.
        self.assertEqual(
            adapter._parse_allowlist(settings.allowed_users_raw),
            {SENDER, "+15553334444"},
        )
        self.assertFalse(settings.allow_all_users)

    def test_config_yaml_cannot_widen_the_allowlist(self):
        # Core reads the allowlist from the environment only, so honoring a
        # config-only allowlist here would let outbound and inbound disagree.
        config = SimpleNamespace(extra={"allowed_users": "*", "allow_all_users": True})
        env = {
            "SENDBLUE_API_KEY": "k",
            "SENDBLUE_API_SECRET": "s",
            "SENDBLUE_PHONE_NUMBER": OUR,
            "SENDBLUE_ALLOWED_USERS": SENDER,
        }
        with patch.dict(os.environ, env, clear=True):
            settings = adapter._settings_from_config(config)

        self.assertEqual(adapter._parse_allowlist(settings.allowed_users_raw), {SENDER})
        self.assertFalse(settings.allow_all_users)


class GroupManagementTests(_Base):
    """Adapter-level create_group / add_recipient_to_group."""

    def _adapter(self, *, create=None, add=None):
        calls = SimpleNamespace(create=[], add=[], group=[])

        async def create_group(numbers, content="", *, media_url=""):
            calls.create.append((list(numbers), content, media_url))
            if isinstance(create, Exception):
                raise create
            return create or (GROUP, "m-1")

        async def send_group_message(group_id, content="", *, media_url=""):
            calls.group.append((group_id, content, media_url))
            return f"m-{len(calls.group) + 1}"

        async def send_message(to_number, content="", *, media_url=""):
            raise AssertionError(f"group flow routed as DM: {to_number}")

        async def add_recipient_to_group(group_id, number):
            calls.add.append((group_id, number))
            if isinstance(add, Exception):
                raise add
            return add or {"status": "OK"}

        fake = SimpleNamespace(
            settings=self._settings(),
            _store=self.store,
            client=SimpleNamespace(
                create_group=create_group,
                send_group_message=send_group_message,
                send_message=send_message,
                add_recipient_to_group=add_recipient_to_group,
            ),
        )
        _bind(
            fake,
            "format_message",
            "_resolve_target",
            "_send_chunk",
            "send",
            "create_group",
            "add_recipient_to_group",
        )
        return fake, calls

    def test_created_group_id_is_immediately_usable_as_chat_id(self):
        fake, calls = self._adapter()
        res = asyncio.run(adapter.SendblueAdapter.create_group(fake, [SENDER, "+15553334444"], "hi"))

        self.assertTrue(res.success)
        self.assertEqual(res.raw_response["group_id"], GROUP)
        self.assertEqual(res.raw_response["chat_id"], GROUP)

        # The whole point: feed it straight back into send().
        follow_up = asyncio.run(adapter.SendblueAdapter.send(fake, res.raw_response["chat_id"], "again"))
        self.assertTrue(follow_up.success)
        self.assertEqual(calls.group, [(GROUP, "again", "")])

    def test_create_group_strips_markdown_from_seed(self):
        fake, calls = self._adapter()
        asyncio.run(adapter.SendblueAdapter.create_group(fake, [SENDER, "+15553334444"], "**hi** all"))
        self.assertEqual(calls.create[0][1], "hi all")

    def test_create_group_sends_overflow_chunks_to_the_new_group(self):
        # Only the first chunk can ride the creation POST -- it carries numbers.
        fake, calls = self._adapter()
        long_text = "x" * (adapter.MAX_SENDBLUE_BODY_CHARS + 50)
        res = asyncio.run(adapter.SendblueAdapter.create_group(fake, [SENDER, "+15553334444"], long_text))

        self.assertTrue(res.success)
        self.assertEqual(len(calls.create), 1)
        self.assertEqual(len(calls.group), 1, "remainder goes to the returned group id")
        self.assertEqual(calls.group[0][0], GROUP)
        self.assertEqual(res.message_id, "m-1,m-2")

    def test_create_group_media_only_seed_sends_empty_content(self):
        fake, calls = self._adapter()
        res = asyncio.run(
            adapter.SendblueAdapter.create_group(fake, [SENDER, "+15553334444"], "", media_url="https://cdn/x.jpg")
        )
        self.assertTrue(res.success)
        self.assertEqual(calls.create, [([SENDER, "+15553334444"], "", "https://cdn/x.jpg")])

    def test_create_group_maps_value_error_to_non_retryable(self):
        fake, _ = self._adapter(create=ValueError("Invalid Sendblue group id"))
        res = asyncio.run(adapter.SendblueAdapter.create_group(fake, [SENDER, "+15553334444"], "hi"))
        self.assertFalse(res.success)
        self.assertFalse(res.retryable)
        self.assertIn("Invalid Sendblue group id", res.error)

    def test_create_group_maps_transport_error_to_retryable(self):
        fake, _ = self._adapter(create=RuntimeError("Sendblue API error 503"))
        res = asyncio.run(adapter.SendblueAdapter.create_group(fake, [SENDER, "+15553334444"], "hi"))
        self.assertFalse(res.success)
        self.assertTrue(res.retryable)

    def test_add_recipient_passes_group_id_through(self):
        fake, calls = self._adapter()
        res = asyncio.run(adapter.SendblueAdapter.add_recipient_to_group(fake, GROUP, SENDER))
        self.assertTrue(res.success)
        self.assertEqual(calls.add, [(GROUP, SENDER)])

    def test_add_recipient_rejects_a_phone_chat_id(self):
        fake, calls = self._adapter()
        res = asyncio.run(adapter.SendblueAdapter.add_recipient_to_group(fake, SENDER, "+15553334444"))
        self.assertFalse(res.success)
        self.assertFalse(res.retryable)
        self.assertEqual(calls.add, [], "a DM target must never reach modify-group")


if __name__ == "__main__":
    unittest.main()
