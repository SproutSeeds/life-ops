# life-ops

`life-ops` is a local-first personal operating system for scheduling, communications, events, and routines.

Maintained by SproutSeeds. Research stewardship: Fractal Research Group ([frg.earth](https://frg.earth)).

The first MVP is intentionally narrow: it gives your agent a reliable place to look when you ask "what's on the agenda?" and returns a live week view built from:

- calendar events
- communication follow-ups
- organizations tied to those items
- daily and weekly routines

## What works now

- SQLite-backed storage for organizations, events, communications, and routines
- A CLI for adding records and rendering a 7-day agenda
- Google Calendar sync into the local event store
- Gmail sync with full-message body extraction, attachment metadata capture, and thread-level grouping
- Communication classification into `open`, `reference`, or `ignore` with broad life buckets including `tax`, `medical`, `insurance`, `benefits`, `identity`, `creative`, `career`, `community`, `finance`, `shopping`, `developer`, `entertainment`, `home`, `logistics`, `pets`, and `travel`
- Profile-context extraction for high-value life records like identity docs, insurance records, tax mail, medical records, benefits notices, and immigration history
- Local attachment vault plus PDF/image/text extraction for profile-grade Gmail evidence
- Daily and weekly routine support so your operating rhythm stays visible
- A demo seed command so you can see the full flow immediately
- Text and JSON output for both human use and agent consumption

The repo now also includes two JavaScript entry points, but they are not equally primary:

- `lifeops` is the main public npm package and the install path most people should start with
- `@lifeops/core` is the secondary SDK for developers who want to build adjacent tools on top of Life Ops primitives

If you are evaluating or adopting Life Ops, start with the CLI.
If you are embedding Life Ops ideas into another codebase, reach for the SDK.

## Repo layout

- `bin/life-ops`: zero-install entrypoint for the CLI
- `src/life_ops`: app code
- `docs/daily-weekly-flow.md`: recommended operating rhythm
- `docs/agent-usage.md`: how an agent should query the system
- `docs/google-setup.md`: how to connect Gmail and Google Calendar
- `docs/academic-outreach/workflow.md`: ORP-gated workflow for recipient-aware academic outreach drafts
- `docs/tracing.md`: how behavior traces are captured and exported
- `docs/profile-context.md`: how important human-profile records are extracted from stored mail
- `docs/x-integration.md`: setup and command surface for X account integration
- `docs/x-content.md`: how local X article packages and image briefs work
- `docs/keys.md`: how global key storage and activation works
- `docs/fastmail-setup.md`: how to set up Fastmail as the non-Google mailbox backend
- `docs/sovereign-email-stack.md`: how to run the low-cost Cloudflare + Resend mail stack on your own domain
- `packages/life-ops-core`: installable npm package for connectors, agenda composition, and structured project-share email drafting
- `packages/lifeops-cli`: installable npm CLI package exposing the global `lifeops` command
- `data/life_ops.db.enc.json`: encrypted canonical local database storage created on first run
- `data/attachments/`: local vault for downloaded Gmail attachments used in profile extraction

## Quick start

```bash
cd /Volumes/Code_2TB/code/life-ops
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
zsh ./bin/life-ops init
zsh ./bin/life-ops seed-demo
zsh ./bin/life-ops agenda --days 7
```

## NPM install path

Most users should start here:

```bash
npm install -g lifeops
lifeops init
```

The SDK is optional and only for builders:

```bash
npm install @lifeops/core
```

## Academic outreach workflow

For research-adjacent outreach where recipient distinctions matter, use the ORP-gated workflow instead of freehanding drafts:

```bash
python3 scripts/validate_academic_outreach.py \
  --manifest examples/academic-outreach/manifest.json

orp --config orp.academic-outreach.yml \
  gate run --profile academic_outreach_default --json

orp --config orp.academic-outreach.yml \
  packet emit --profile academic_outreach_default --json
```

This validates:

- current recipient title/source metadata
- required project links and install command
- plain-text draft hygiene
- distinct recipient angles so multiple drafts do not collapse into one template

## Core commands

```bash
zsh ./bin/life-ops init
zsh ./bin/life-ops agenda --days 7
zsh ./bin/life-ops agenda --days 7 --format json
zsh ./bin/life-ops add-org --name "Primary Work"
zsh ./bin/life-ops add-event --title "Founder sync" --start 2026-03-25T10:00 --end 2026-03-25T11:00 --organization "Primary Work"
zsh ./bin/life-ops add-comm --subject "Reply to investor email" --channel email --follow-up-at 2026-03-26T13:00 --organization "Primary Work"
zsh ./bin/life-ops add-item --list personal --title "Buy new dish sponge"
zsh ./bin/life-ops add-item --list professional --title "Reply to partner note"
zsh ./bin/life-ops list-items --status open
zsh ./bin/life-ops done-item --id 1
zsh ./bin/life-ops add-routine --name "Morning planning" --cadence daily --start-time 08:30 --duration 30
zsh ./bin/life-ops add-routine --name "Weekly review" --cadence weekly --day sunday --start-time 18:00 --duration 60
zsh ./bin/life-ops done-comm --id 1
zsh ./bin/life-ops google-auth
zsh ./bin/life-ops google-list-calendars
zsh ./bin/life-ops sync-google
zsh ./bin/life-ops backfill-gmail --max-results 1000
zsh ./bin/life-ops sync-gmail-category-pass --max-results 100 --reset-cursors
zsh ./bin/life-ops sync-gmail-corpus --backfill-query '-in:chats -category:promotions -category:social -category:forums' --backfill-max-results 250 --backfill-max-runs 3
zsh ./bin/life-ops sync-gmail-corpus --backfill-query=-in:chats --backfill-max-results 250 --backfill-max-runs 0 --reset-backfill-cursor
zsh ./bin/life-ops comms --status open
zsh ./bin/life-ops comms --status reference --source gmail
zsh ./bin/life-ops comms --status reference --source gmail --category career
zsh ./bin/life-ops comms-summary --source gmail
zsh ./bin/life-ops gmail-heartbeat
zsh ./bin/life-ops reclassify-gmail
zsh ./bin/life-ops extract-profile-context
zsh ./bin/life-ops ingest-profile-attachments --limit 25
zsh ./bin/life-ops ingest-profile-attachments --scope sensitive --limit 100
zsh ./bin/life-ops backfill-profile-attachments --max-results 100 --max-runs 0
zsh ./bin/life-ops profile-attachment-heartbeat
zsh ./bin/life-ops attachment-summary
zsh ./bin/life-ops attachments --limit 10
zsh ./bin/life-ops profile-context-summary
zsh ./bin/life-ops profile-context --item-type identity_document
zsh ./bin/life-ops profile-context --item-type immigration_record
zsh ./bin/life-ops profile-context --subject-key wife_sisy
zsh ./bin/life-ops profile-review-next
zsh ./bin/life-ops profile-approve --id 123 --notes "confirmed from PDF"
zsh ./bin/life-ops profile-records
zsh ./bin/life-ops profile-alerts
zsh ./bin/life-ops profile-review-set --id 123 --status approved --notes "confirmed from PDF"
zsh ./bin/life-ops keys-set --name OPENAI_API_KEY --from-env
zsh ./bin/life-ops keys-list
eval "$(zsh ./bin/life-ops-env)"
zsh ./bin/life-ops cloudflare-mail-init-config
zsh ./bin/life-ops cloudflare-mail-status
zsh ./bin/life-ops cloudflare-mail-queue-status
zsh ./bin/life-ops cloudflare-mail-write-worker
zsh ./bin/life-ops mail-ingest-generate-secret
zsh ./bin/life-ops cloudflare-mail-sync
zsh ./bin/cloudflare-mail-sync-service install
zsh ./bin/cloudflare-mail-sync-service status
zsh ./bin/life-ops mail-ui
zsh ./bin/life-ops cmail-drafts
zsh ./bin/life-ops cmail-draft-save --subject "Draft for Terence Tao" --body-file ./tao.txt
zsh ./bin/life-ops cmail-draft-save --subject "Draft for Thomas Bloom" --body-file ./bloom.txt
zsh ./bin/life-ops resend-init-config
zsh ./bin/life-ops resend-status
zsh ./bin/life-ops resend-signature-show
zsh ./bin/life-ops resend-signature-set --text $'Cody Mitchell\nFractal Research Group\ncody@frg.earth'
zsh ./bin/life-ops resend-domains
zsh ./bin/life-ops resend-domain-create --name frg.earth
zsh ./bin/life-ops resend-send-email --to you@example.com --subject "life-ops sovereign stack online" --text "hello from resend"
zsh ./bin/life-ops resend-send-email --to you@example.com --subject "queue first" --text "send me after journaling" --queue-only
zsh ./bin/life-ops resend-send-email --to you@example.com --cc collaborator@example.com --subject "reply test" --text "looping you in" --in-reply-to '<root@example.com>' --reference '<root@example.com>'
zsh ./bin/life-ops resend-send-email --to you@example.com --subject "attachment test" --text "see attached" --attach ./notes.pdf
zsh ./bin/life-ops resend-send-email --to you@example.com --subject "inline image test" --html '<p>diagram below</p><img src="cid:hero-image">' --inline ./hero.png::hero-image
zsh ./bin/life-ops resend-queue-status
zsh ./bin/life-ops resend-queue-process
zsh ./bin/life-ops mail-alerts
zsh ./bin/life-ops cloudflare-mail-inject-test --subject "synthetic attachment test" --body "raw mime + local vault proof" --attach ./notes.pdf
zsh ./bin/life-ops fastmail-init-config
zsh ./bin/life-ops fastmail-status
zsh ./bin/life-ops fastmail-session
zsh ./bin/life-ops fastmail-mailboxes
zsh ./bin/life-ops emma-status
zsh ./bin/life-ops emma-me
zsh ./bin/life-ops emma-agents
zsh ./bin/life-ops emma-chat --agent soulbind --message "Talk to me about what you think I've been holding lately."
zsh ./bin/life-ops x-init-config
zsh ./bin/life-ops x-status
zsh ./bin/life-ops x-auth
zsh ./bin/life-ops x-me
zsh ./bin/life-ops x-posts --limit 5
zsh ./bin/life-ops x-user --username XDevelopers
zsh ./bin/life-ops x-home --limit 10
zsh ./bin/life-ops x-post --text "hello from life-ops"
zsh ./bin/life-ops x-package-create --title "Define Your Canonical Dossier" --point "Capture the truth layer first" --point "Separate active queue from long-term archive"
zsh ./bin/life-ops x-content
zsh ./bin/life-ops x-content-show --id 1
zsh ./bin/life-ops x-media --content-id 1
zsh ./bin/life-ops x-generate-image --asset-id 1
zsh ./bin/life-ops x-generate-image --asset-id 1 --provider xai --model grok-imagine-image --resolution 2k
zsh ./bin/life-ops trace-summary
zsh ./bin/life-ops export-traces --trace-type gmail_sync --output data/exports/gmail_sync.jsonl
zsh ./bin/gmail-exhaust status
zsh ./bin/profile-attachment-exhaust status
```

The wrapper is kept in `bin/`, but on this drive it needs to be invoked through `zsh` rather than executed directly.

## Sovereign mail fidelity

The Cloudflare + Resend path now preserves more than plain message text:

- inbound `cloudflare_email` messages keep the raw `.eml` locally
- inbound attachments are saved into the local attachment vault
- inline image parts keep `content-id` metadata
- extracted text sidecars are created when the file type is readable
- archives now get entry-level summaries
- code files now get preview-style code summaries
- audio, video, and executable-like binaries now get fingerprint/descriptor summaries
- outbound Resend mail supports normal attachments and inline CID images
- inbound and outbound mail now keep first-class envelope/thread metadata including `to`, `cc`, `bcc`, `reply-to`, `message-id`, `in-reply-to`, `references`, and a stable local `thread_key`
- outbound Resend sends are journaled into the same local communications store your inbound mail uses
- outbound Resend sends now enter a local delivery queue first, retry with backoff on provider failures, and surface flow issues through `mail-alerts`

## NPM modules

The public npm story is intentionally simple:

- `lifeops` is the primary package
- `@lifeops/core` is the optional builder package

Use `lifeops` if you want to:

- scaffold starter files with `lifeops init`
- render agendas from JSON feeds with `lifeops agenda`
- create share packets and follow-up queues with `lifeops share`

Use `@lifeops/core` only if you want to:

- normalize signals from your own inbox, calendar, CRM, task, or codebase feeds
- compose a shared agenda inside another application
- draft structured project-share emails and follow-up items from your own code
- plug those drafts into your own sender layer

Local workspace usage:

```bash
cd /Volumes/Code_2TB/code/life-ops
npm install
npm test --workspace @lifeops/core
npm test --workspace lifeops
```

Install from a local checkout:

```bash
npm install ./packages/life-ops-core
npm install -g ./packages/lifeops-cli
```

Install targets:

```bash
npm install -g lifeops
npm install @lifeops/core
```

## Global keys

`life-ops` now has a small global key registry outside the repo. Once a key is stored there, the CLI auto-loads it at startup, so commands like `x-generate-image` can work without re-exporting the key each time.

Use `keys-set`, `keys-list`, `keys-export`, and `bin/life-ops-env` for this layer.

That same key layer now covers Emma too, so `EMMA_API_KEY` can be stored once and reused by `emma-me`, `emma-agents`, and `emma-chat`.

It also covers Fastmail now, so `FASTMAIL_API_TOKEN` can be stored once and reused by `fastmail-status`, `fastmail-session`, and `fastmail-mailboxes`.

It also covers the sovereign mail stack, so `RESEND_API_KEY` and `LIFE_OPS_MAIL_INGEST_SECRET` can be stored once and reused by the Cloudflare/Resend commands.

On macOS, the safe default is Keychain-backed storage. Plaintext file-backed secrets now require an explicit `--allow-insecure-file-backend` opt-in.

## Sovereign email setup

If you want domain-first email without paying Google or self-hosting an SMTP headache, use the sovereign stack in `docs/sovereign-email-stack.md`.

That path is:

- Cloudflare DNS + Email Routing for inbound
- a Cloudflare Email Worker with a durable queue and signed control endpoints
- Resend for outbound sending
- `life-ops` syncing mail into local SQLite as the system of record
- encrypted canonical local SQLite storage at rest
- encrypted local mail vault artifacts for raw inbound mail, attachments, and outbound manifests

In this setup, the local SQLite database is the primary operational source of truth for the agent, while the Cloudflare durable queue keeps a redundant cloud copy of raw inbound messages for resilience and replay.
`life_ops.db` now lives as an encrypted logical store on disk, with `backup-create` producing encrypted snapshots from that canonical store.
On macOS, `zsh ./bin/cloudflare-mail-sync-service install` installs a native `launchd` job that runs a sync every `30` seconds by default. Its daemon logs live in `~/Library/Logs/life-ops/`.
The sync path is single-flight, so overlapping runs now skip cleanly instead of stacking up behind the encrypted DB lock.
That same sync loop also processes due outbound Resend queue items, so one background service covers both inbound drain and outbound retry.

Start with:

```bash
zsh ./bin/life-ops cloudflare-mail-init-config
zsh ./bin/life-ops cloudflare-mail-write-worker
zsh ./bin/life-ops cloudflare-mail-status
zsh ./bin/life-ops mail-ingest-generate-secret
zsh ./bin/life-ops cloudflare-mail-queue-status
zsh ./bin/life-ops cloudflare-mail-sync
zsh ./bin/cloudflare-mail-sync-service install
zsh ./bin/cloudflare-mail-sync-service status
zsh ./bin/life-ops resend-init-config
zsh ./bin/life-ops resend-status
zsh ./bin/life-ops resend-queue-status
zsh ./bin/life-ops mail-alerts
```

## Fastmail setup

If you want custom-domain email without Google Workspace baggage, use Fastmail.

Start with `docs/fastmail-setup.md`, then:

```bash
zsh ./bin/life-ops fastmail-init-config
zsh ./bin/life-ops keys-set --name FASTMAIL_API_TOKEN --value "your-fastmail-api-token"
zsh ./bin/life-ops fastmail-status
zsh ./bin/life-ops fastmail-mailboxes
```

## Google setup

1. Create Desktop app OAuth credentials in Google Cloud and enable both Gmail API and Google Calendar API.
2. Save the JSON file to `config/google_credentials.json`.
3. Run `zsh ./bin/life-ops google-auth`.
4. Run `zsh ./bin/life-ops google-list-calendars` if you want IDs beyond `primary`.
5. Run `zsh ./bin/life-ops sync-google`.

After sync, your normal agenda command will include the imported events and open email follow-ups. Reference mail is still stored locally and can be inspected with `comms` and `comms-summary`, including category filters.

For archive/history ingestion, use `backfill-gmail`. It pages older Gmail history into the same local store, keeps a resume cursor in SQLite sync state, and avoids overwriting newer thread snapshots with older ones.

For taxonomy cleanup without another Gmail API crawl, use `reclassify-gmail`. By default it preserves the current `open` versus `reference` queue while pushing newer category rules across the stored Gmail corpus.

For a focused taxonomy sweep across important life buckets before a bigger archive crawl, use `sync-gmail-category-pass`. It runs a set of category-driven history queries, stores anything relevant locally, and then reclassifies the full local Gmail corpus.

For a fuller end-to-end Gmail pass, use `sync-gmail-corpus`. It runs the recent inbox sync, a configurable number of archive backfill loops, and then a local reclassification sweep in one command so the taxonomy baseline gets pushed down across more of the stored inbox.

If you want the archive crawl to keep going until Gmail has no older matching mail left, pass `--backfill-max-runs 0`. That switches the corpus command into exhaustive mode instead of stopping at a fixed loop count.

Use `gmail-heartbeat` when you want a quick live checkpoint on the backlog runner. It shows the active mailbox, current corpus size, top categories, active trace ids, and the current archive cursor date.

After the Gmail corpus is stored locally, run `extract-profile-context` to build a separate candidate layer of profile records from the historical mail stream. It is intentionally more conservative than general Gmail classification and leans on subject lines, snippets, attachment names, and trusted domains so it can surface things like ID cards, policy docs, USCIS notices, tax records, and medical history without turning every newsletter mention into a profile fact.

If you want the profile layer to use actual document content instead of just message metadata, run `ingest-profile-attachments` first. The default `--scope profile` mode downloads attachments tied to current profile-context candidates. The broader `--scope sensitive` mode sweeps Gmail rows already classified into sensitive life buckets like identity, tax, insurance, immigration, medical, and benefits so you can keep expanding the document vault without waiting for a candidate to already exist.

The ingest pass now skips common decorative email fragments like logos, bars, corners, and tiny inline image assets so the vault stays focused on real records instead of newsletter chrome.

For a resumable full-corpus attachment walk, use `backfill-profile-attachments`. It pages through the locally stored Gmail corpus with a saved cursor so the sensitive document sweep can keep chewing through older mail without restarting from the top every time. Use `profile-attachment-heartbeat` for a live checkpoint on that backfill.

Use `profile-context-summary` for a quick count by item type and subject, then drill in with `profile-context --item-type ...` or `profile-context --subject-key ...`.

When you are ready to turn candidates into trusted profile memory, use `profile-review-next`, `profile-approve`, `profile-reject`, and `profile-merge`. Approved items are promoted into canonical profile records with linked source candidates and linked extracted attachments. Those canonical records can be inspected with `profile-records`, `profile-record-show`, and `profile-record-summary`.

Use `profile-alerts` to surface operational records that should stay especially visible, including recent immigration movement, benefits/tax/admin packets, and canonical records that still do not have a linked extracted attachment.

Use `attachments` and `attachment-summary` to inspect the local evidence store, and `profile-review-set` when you want to approve or reject a candidate profile item after review.

For tmux-based backlog management, use `bin/gmail-exhaust`:

```bash
zsh ./bin/gmail-exhaust start
zsh ./bin/gmail-exhaust restart
zsh ./bin/gmail-exhaust status
zsh ./bin/gmail-exhaust tail
zsh ./bin/gmail-exhaust attach
zsh ./bin/gmail-exhaust stop
```

For tmux-based sensitive attachment backfill management, use `bin/profile-attachment-exhaust`:

```bash
zsh ./bin/profile-attachment-exhaust start
zsh ./bin/profile-attachment-exhaust restart
zsh ./bin/profile-attachment-exhaust status
zsh ./bin/profile-attachment-exhaust tail
zsh ./bin/profile-attachment-exhaust attach
zsh ./bin/profile-attachment-exhaust stop
```

## X setup

1. Create an X app and enable OAuth 2.0 user auth with the Authorization Code Flow + PKCE.
2. Set the callback URL to `http://127.0.0.1:8787/x/callback`.
3. Use at least these scopes: `tweet.read`, `users.read`, `tweet.write`, `offline.access`.
4. Save the local app config in `config/x_client.json`.
5. Run `zsh ./bin/life-ops x-auth`.
6. Verify with `zsh ./bin/life-ops x-me` and `zsh ./bin/life-ops x-posts --limit 5`.

Once connected, `life-ops` can read your account identity, inspect your recent posts, look up other public accounts, read your home timeline with user context, and publish/delete posts through the same local CLI.

## Emma setup

Store your Emma developer API key under `EMMA_API_KEY`, then use:

```bash
zsh ./bin/life-ops emma-status
zsh ./bin/life-ops emma-me
zsh ./bin/life-ops emma-agents
zsh ./bin/life-ops emma-chat --agent soulbind --message "Talk to me about what you think I've been holding lately."
```

By default the Emma client targets `https://emma-sable.vercel.app`, but every Emma command also accepts `--base-url` if you want to point it at a local or alternate deployment.

## X content studio

Use `x-package-create` when you want a local-first X content package: one article draft, a supporting thread, and a set of tied image briefs. The package is stored in SQLite, the media prompts are stored in SQLite, and optional rendered images are saved in `data/x_media/`.

If you want real rendered images instead of just prompts, use `x-generate-image`. The repo now supports both OpenAI and xAI providers. OpenAI defaults to `gpt-image-1.5`; xAI can use `grok-imagine-image`. The default `--provider auto` path prefers xAI when `XAI_API_KEY` is available and automatically falls back to OpenAI if xAI is blocked or unavailable.

## Behavior tracing

This repo now records structured traces for:

- Gmail triage decisions
- Gmail classification decisions, body previews, and attachment metadata
- Gmail archive backfill decisions and resume progress
- Google Calendar sync behavior
- agenda items surfaced to the user

Those traces are stored locally in the same SQLite database and can be exported later as JSONL for model-training pipelines or offline analysis.

## Product direction

This repo is the foundation, not the finished system. The next logical layers are:

1. contact and organization memory
2. automatic follow-up suggestions
3. a proper agent-facing API or MCP surface
4. routines that adapt based on workload and commitments
5. smarter email triage and relationship cadence

For now, the important thing is that we have a clean home for the system and a usable agenda engine instead of just notes about one.
