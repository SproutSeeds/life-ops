# CMAIL data map

Use the live CMAIL health endpoint as the authoritative source for the database path:

```sh
curl -fsS http://127.0.0.1:4311/api/health
```

The installed service currently defaults to these local surfaces:

| Surface | Canonical location | Purpose |
| --- | --- | --- |
| CMAIL web/API | `http://127.0.0.1:4311` | Local mailbox UI and JSON API |
| Private CMAIL route | `https://cmail.tail649edd.ts.net` | Tailscale Serve route to the loopback service |
| Live runtime database | `~/.lifeops/data/cmail_runtime.db` | Mail, drafts, delivery state, contacts, and sync state used by the running UI |
| Runtime metadata | `~/.lifeops/data/cmail_runtime_state.json` | Runtime preparation/seal metadata |
| Attachment vault | `~/.lifeops/data/attachments/` | Encrypted saved mail attachments and extracted evidence |
| Agent selection | `~/.lifeops/state/cmail-agent.json` | Pointer to the CMAIL item selected when Agent was clicked |
| Service logs | `~/Library/Logs/life-ops/` | CMAIL service and watchdog output |
| Source checkout | `/Volumes/Code_2TB/code/life-ops` | Code, commands, tests, and this context map |

Environment overrides such as `LIFE_OPS_CMAIL_RUNTIME_DB`, `LIFE_OPS_HOME`, and `LIFE_OPS_CMAIL_PUBLIC_URL` take precedence. Confirm `/api/health` and the installed LaunchAgent before assuming a path has remained unchanged.

## Primary database records

- `communications`: inbound messages, outbound messages, and CMAIL drafts. `id` is the value referenced by `communicationId` or `draftId`.
- `communication_attachments`: attachment metadata, encrypted vault paths, extraction status, and extracted text.
- `mail_contacts`: contact-level correspondence state.
- `mail_delivery_queue`: Resend delivery attempts and status.
- `sync_state`: provider and runtime cursors/health metadata.
- `system_alerts`: active operational issues shown by CMAIL.
- Spam sender rules: `sync_state["mail_ui:hidden_contacts"]`; new exact-address
  keys use `sender:<email>`. See [CMAIL Spam](../cmail-spam.md) for routing,
  restoration and the API. Original messages and work dispositions are preserved.
- `mail_labels`, `mail_label_rules`, `mail_label_conversations`: CMAIL label
  definitions, rules, and conversation choices keyed by Message-ID. Labels are an
  organizing view only. See [CMAIL Labels](../cmail-labels.md).
- `communications.envelope_to`: the delivered-to recipient captured at ingest,
  recorded for mail received from September 27, 2026.

Use `life_ops.store.open_db(...)` for application-aware access. Use `life_ops.mail_ui._communication_detail(connection, id)` or the local API when complete message/thread context is required; these paths keep identity inference and attachment summaries consistent with the UI.

## Useful local API reads

```text
GET /api/overview?identity=all
GET /api/overview?identity=all&label=<label id or slug>
GET /api/labels?identity=all
GET /api/communications/<communicationId>
GET /api/drafts?identity=all
GET /api/health
```

`GET /api/communications/<id>` includes the body, thread metadata, reply scaffolds, and attachment summaries. Keep full mail content in the live local data layer; do not add it to the pointer state or context documentation.

## Source modules

- `src/life_ops/mail_ui.py`: CMAIL UI, API, selection validation, drafts, replies, and mail presentation.
- `src/life_ops/mail_labels.py`: label rules, header-linked conversation matching, manual choices, and suggestions.
- `src/life_ops/cmail_agent.py`: persistent session binding, state handoff, and Terminal launch.
- `src/life_ops/clawdad_client.py`: authenticated local ClawDad client and native-server discovery.
- `src/life_ops/cmail_runtime.py`: live runtime database preparation, sync, sealing, and backup behavior.
- `src/life_ops/store.py`: schema and canonical database access helpers.
- `src/life_ops/mail_ingest.py`: message ingestion and normalization.
- `src/life_ops/mail_vault.py`: encrypted attachment vault access.
- `src/life_ops/resend_integration.py`: outbound delivery and Resend receive/sync paths.
- `src/life_ops/cloudflare_email.py`: inbound Cloudflare email queue sync.
- `src/life_ops/mail_identities.py`: mailbox identity routing and signatures.
