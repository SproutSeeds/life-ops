# C-mail data map

| Surface | Location | Role |
| --- | --- | --- |
| Active mail website | https://cmail.frg.earth | Inbox, Sent, threads, contacts, Drafts and sending |
| Agent entrypoints | `cmail`, `lifeops cmail` | Same cloud records and revisions as the website |
| Cloud service | `life-ops-email-ingest` / existing encrypted `MAIL_QUEUE` Durable Object | Canonical encrypted mail, draft and delivery records |
| Agent transport | Signed `/api/mail/agent` on the existing worker | Timestamped HMAC; fixed action allowlist; explicit send confirmation |
| Machine cutover | `~/.lifeops/config/cmail-cloud.json` | Nonsecret activation, old-to-cloud draft IDs, projection cursor baseline |
| Legacy runtime | `~/.lifeops/data/cmail_runtime.db` | Life Ops automation projection and historical evidence |
| Frozen legacy archive | `~/.lifeops/archive/cmail-local-2026-10-20/` | Read-only database snapshot, original runtime/guidance, SHA-256 manifest |
| Attachment vault | `~/.lifeops/data/attachments/` | Existing encrypted local historical attachments |
| Old selected-item state | `~/.lifeops/state/cmail-agent.json` | Historical pointer only; reconcile to cloud before action |
| Old loopback/tailnet route | `127.0.0.1:4311`, `cmail.tail649edd.ts.net` | Redirect; APIs reject actions; health identifies projection role |

Cloud IDs use `mail_` plus a sequence or `draft_` plus a UUID. Old numeric
communication IDs belong to the projection, not the cloud. Never reuse a numeric
mail ID as a cloud ID. Migrated numeric draft IDs have an explicit mapping.

The signed agent API shares `MailIndex`, `SentIndex`, `ContactIndex`, `DraftStore`
and the durable send outbox with browser requests. No second agent draft table
exists. API credentials come from the established Keychain resolver and are not
stored in the cutover configuration. The worker destination is pinned in code.

The local projection uses an ascending cloud ingestion cursor and reads at most
ten new messages per pass. Existing Message-IDs are skipped, preserving local
follow-up dispositions. Each successful item commits with its cursor; failures
retry without skipping mail. It never initiates delivery. Secondary Gmail data,
Life Ops task classifications, old receipts and existing attachments remain in
the local data layer. The full older local archive has not been reimported as a
new cloud mailbox.

Source: `src/life_ops/cmail_cloud.py`, `packages/lifeops-cli/src/cmail-cloud.js`,
and `packages/cmail-cloud/src/`. Original local runtime/UI/provider functions
have cutover guards. Current operations are documented in [README.md](README.md).
