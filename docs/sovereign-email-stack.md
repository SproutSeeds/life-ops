# Sovereign Email Stack

This is the low-cost, non-Google, domain-first email architecture for `life-ops`.

## The stack

1. `frg.earth` stays under your control.
2. Cloudflare handles DNS and inbound Email Routing.
3. A Cloudflare Email Worker receives inbound mail and stores it durably inside a Cloudflare queue.
4. Resend or SES handles outbound sending.
5. `life-ops` pulls from that queue into local SQLite, becoming the system of record for classification, notifications, and send/receive lifecycle management.
6. The canonical local SQLite database is encrypted at rest on disk.
7. Raw mail artifacts and attachment blobs are encrypted at rest in the local mail vault.
8. Outbound mail is journaled locally before provider delivery and retried from a local queue if a provider send fails.

## Source of record

- Local SQLite is the primary operational truth for the agent.
- The on-disk canonical DB is stored as encrypted logical storage, not as a long-lived plaintext `life_ops.db` file.
- Cloudflare keeps a redundant cloud copy of inbound mail inside the durable queue.
- Sync acknowledges messages only after they are stored locally.
- Acknowledged messages are removed from the pending queue but still remain stored in the worker-side object for redundancy and replay.
- Raw `.eml` payloads, attachment blobs, and outbound manifests are encrypted before they are written into the local vault.
- Encrypted local backups can be created with `backup-create` and restored with `backup-restore`.
- Outbound provider sends are queued first, then marked `sent`, `retrying`, or `failed` in local storage.
- Flow issues are surfaced locally through `mail-alerts`.

## Why this path

- no Google Workspace dependency
- no self-hosted SMTP pain
- inbound can be near-free
- outbound can stay cheap
- provider layer stays replaceable
- cloud redundancy without depending on a public tunnel back to your laptop

## Cloudflare inbound

Official docs:

- https://developers.cloudflare.com/email-routing/
- https://developers.cloudflare.com/email-routing/get-started/
- https://developers.cloudflare.com/email-routing/email-workers/enable-email-workers/

Local setup:

```bash
zsh ./bin/life-ops cloudflare-mail-init-config
zsh ./bin/life-ops cloudflare-mail-write-worker
zsh ./bin/life-ops cloudflare-mail-status
zsh ./bin/life-ops mail-ingest-generate-secret
zsh ./bin/life-ops cloudflare-mail-queue-status
zsh ./bin/life-ops cloudflare-mail-sync
zsh ./bin/cloudflare-mail-sync-service install
zsh ./bin/cloudflare-mail-sync-service status
```

The generated worker template expects:

- optional `FORWARD_TO`
- secret `LIFE_OPS_MAIL_INGEST_SECRET`
- a Durable Object binding named `MAIL_QUEUE`

The worker exposes signed queue endpoints:

- `POST /api/mail/queue/pull`
- `POST /api/mail/queue/ack`
- `POST /api/mail/queue/status`

Requests are authenticated with timestamped HMAC headers:

- `X-Life-Ops-Timestamp`
- `X-Life-Ops-Signature`

The worker rejects missing, invalid, or stale signatures by default.
The local sync command ingests pulled mail into SQLite under source `cloudflare_email` and only acknowledges queue items after successful local persistence.

## Automatic local sync

On macOS, the repo now ships a user-level `launchd` manager:

```bash
zsh ./bin/cloudflare-mail-sync-service install
zsh ./bin/cloudflare-mail-sync-service status
zsh ./bin/cloudflare-mail-sync-service tail
```

This service uses native `launchd` scheduling (`RunAtLoad` + `StartInterval`) to run a single sync every `30` seconds by default. The daemon's own stdout/stderr logs live in `~/Library/Logs/life-ops/`, while synced mail is stored in the repo-local SQLite database.
The same loop also processes due outbound Resend queue items, so inbound drain and outbound retry share one background service.

## Resend outbound

Official docs:

- https://resend.com/docs/api-reference/emails
- https://resend.com/docs/api-reference/domains
- https://resend.com/pricing

Local setup:

```bash
zsh ./bin/life-ops resend-init-config
zsh ./bin/life-ops keys-set --name RESEND_API_KEY --value "your-resend-api-key"
zsh ./bin/life-ops resend-status
zsh ./bin/life-ops resend-signature-show
zsh ./bin/life-ops resend-signature-set --text $'Cody Mitchell\nFractal Research Group\ncody@frg.earth'
zsh ./bin/life-ops resend-domains
zsh ./bin/life-ops resend-queue-status
zsh ./bin/life-ops mail-alerts
```

Create a sending domain:

```bash
zsh ./bin/life-ops resend-domain-create --name frg.earth
```

Send a test email:

```bash
zsh ./bin/life-ops resend-send-email \
  --to you@example.com \
  --subject "life-ops sovereign stack online" \
  --text "hello from resend"
```

Queue a message locally without attempting immediate provider delivery:

```bash
zsh ./bin/life-ops resend-send-email \
  --to you@example.com \
  --subject "queue first" \
  --text "send me after journaling" \
  --queue-only
```

Process due queued outbound mail manually:

```bash
zsh ./bin/life-ops resend-queue-process
```

Send outbound mail with a normal attachment:

```bash
zsh ./bin/life-ops resend-send-email \
  --to you@example.com \
  --subject "attachment test" \
  --text "see attached" \
  --attach ./notes.pdf
```

Send outbound HTML with an inline image:

```bash
zsh ./bin/life-ops resend-send-email \
  --to you@example.com \
  --subject "inline image test" \
  --html '<p>diagram below</p><img src="cid:hero-image">' \
  --inline ./hero.png::hero-image
```

Skip the saved default signature for one-off sends:

```bash
zsh ./bin/life-ops resend-send-email \
  --to you@example.com \
  --subject "quick note" \
  --text "hello from resend" \
  --no-signature
```

Send a threaded note with explicit `cc` and reply-chain metadata:

```bash
zsh ./bin/life-ops resend-send-email \
  --to you@example.com \
  --cc collaborator@example.com \
  --subject "follow-up" \
  --text "looping you in on this thread" \
  --in-reply-to '<root@example.com>' \
  --reference '<root@example.com>' \
  --reference '<mid@example.com>'
```

## What is live now

- Cloudflare mail config template
- generated Cloudflare Email Worker template
- durable queue storage in the worker
- signed pull/ack queue endpoints
- local sync command for pulling queue items into SQLite
- local raw `.eml` preservation for sovereign inbound mail
- encrypted local raw `.eml` preservation for sovereign inbound mail
- encrypted local vaulting of Cloudflare inbound attachments and inline parts
- type-aware attachment summaries for archives, code, media, and executable-like binaries
- Resend config template
- Resend status/domain/send commands
- Resend queue status/process commands
- Resend outbound attachments and inline CID image support
- inbound and outbound envelope metadata persisted in SQLite
- local thread tracking via `message-id`, `in-reply-to`, `references`, and `thread_key`
- outbound Resend journaling into local communications plus encrypted attachment vault artifacts
- local outbound retry/backoff queue plus alert surfacing
- operational header allowlist stored in SQLite by default; full forensic headers remain recoverable from the encrypted raw message when needed

## Next phases

1. Unified provider abstraction across Gmail, Fastmail, and the sovereign stack
2. Reply automation and follow-up policies on top of the new thread metadata
3. Encrypted cloud archive promotion beyond the current queue retention layer
