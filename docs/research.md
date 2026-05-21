# Research Notes

Research date: 2026-05-10. Revised: 2026-05-21.

## Hermes Agent Findings

- Hermes Agent is the Nous Research self-improving agent with a CLI and a long-running messaging gateway. The official README describes CLI, gateway, skills, memory, cron scheduling, and OpenClaw migration support.
- Hermes' messaging gateway is the right integration point for Sendblue. It routes platform adapter events to a per-chat Hermes session and then to `AIAgent`.
- Third-party Hermes plugins install at `~/.hermes/plugins/<name>/` with a `plugin.yaml` manifest and an `adapter.py` entry point exposing `register(ctx)`. The `plugins/platforms/<name>/` path under the hermes-agent repo itself is reserved for **bundled** adapters that ship with Hermes core (see `gateway/config.py:_scan_bundled_plugin_platforms` and `gateway/platforms/ADDING_A_PLATFORM.md`); the plugin loader does not scan `~/.hermes/plugins/platforms/`.
- Platform adapters extend `BasePlatformAdapter` and implement `connect()`, `disconnect()`, `send()`, and `get_chat_info()`. Inbound messages are normalized into `MessageEvent` objects and handed to `handle_message()`.
- Plugin registration supports useful Sendblue-specific hooks: `allowed_users_env`, `allow_all_env`, `env_enablement_fn`, `cron_deliver_env_var`, `standalone_sender_fn`, `max_message_length`, `pii_safe`, and `platform_hint`.
- Hermes supports dynamic plugin platform names through `Platform("sendblue")` after the plugin registers itself.
- Hermes has built-in DM pairing. A plugin only needs to provide `source.user_id` consistently; the gateway can authorize via platform allowlists, global allowlists, or pairing approvals.

Primary Hermes sources:

- https://github.com/NousResearch/hermes-agent
- https://hermes-agent.nousresearch.com/docs/
- https://hermes-agent.nousresearch.com/docs/user-guide/messaging
- https://hermes-agent.nousresearch.com/docs/developer-guide/adding-platform-adapters
- https://hermes-agent.nousresearch.com/docs/developer-guide/gateway-internals
- Live source checked from `gateway/platforms/base.py`, `gateway/platform_registry.py`, `gateway/config.py`, `gateway/run.py`, and bundled platform plugin examples.

## Sendblue API Findings

- Sendblue lists messages at `GET /api/v2/messages`; the current docs describe `limit`, `offset`, `order_by`, `order_direction`, `is_outbound`, `from_number`, `to_number`, `number`, `sendblue_number`, `created_at_gte/lte`, and `sent_at_gte/lte` filters. This adapter filters inbound messages with the documented `is_outbound=false`, `sendblue_number=<Sendblue line>`, and `created_at_gte=<poll cutoff>` parameters, then paginates with `offset`.
- Sendblue sends single-recipient messages with `POST /api/send-message`. Required fields are `number` and `from_number`; either `content` or `media_url` must be present.
- Sendblue can receive inbound messages through dashboard-configured receive webhooks. Inbound payloads include `message_handle`, `content`, `from_number`, `to_number`, `number`, `sendblue_number`, `is_outbound`, `media_url`, `service`, and timestamps.
- Sendblue asks webhook receivers to return an HTTP response to avoid repeated callbacks.
- Sendblue supports typing indicators at `POST /api/send-typing-indicator`.
- Sendblue message text is capped below 18,996 characters, so this plugin chunks text at 18,000 characters.

Primary Sendblue sources:

- https://docs.sendblue.com/api-v2/messages/
- https://docs.sendblue.com/getting-started/sending-messages/
- https://docs.sendblue.com/getting-started/receiving-messages/
- https://docs.sendblue.com/getting-started/webhooks/
- https://docs.sendblue.com/api-v2/typing-indicators/

## Design Translation From openclaw-sendblue

The OpenClaw repo registers a Sendblue channel, polls or receives webhooks, de-dupes messages, checks allowed senders, dispatches inbound text to the agent runtime, and sends replies back through Sendblue.

The Hermes version maps those same responsibilities into Hermes primitives:

- OpenClaw channel plugin -> Hermes platform plugin
- OpenClaw runtime dispatcher -> `BasePlatformAdapter.handle_message()`
- OpenClaw allowlist -> Hermes gateway allowlists and pairing
- OpenClaw service lifecycle -> Hermes adapter `connect()` / `disconnect()`
- OpenClaw outbound adapter -> Hermes `send()` / `send_image()`
- OpenClaw polling/webhook options -> Sendblue poll loop or local webhook server
- OpenClaw SQLite de-dupe -> Hermes-profile-scoped SQLite de-dupe
- OpenClaw config -> `~/.hermes/.env` and optional `gateway.platforms.sendblue.extra`
