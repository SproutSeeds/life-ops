# C-mail cloud receiving, replies, Sent and indexed reading

Production: <https://cmail.frg.earth>, Cloudflare Worker
`life-ops-email-ingest`. This package restores Bind My Soul receiving to the
website's existing encrypted Durable Object archive.

The message reader also supports authenticated plaintext replies. Click Reply,
write above the visible signature, then click Send. From, To and conversation
headers come from the original message and verified mailbox configuration.

## Source provenance

The October 13 cloud rebuild's original source directory was absent from both
this checkout and its GitHub main. `src/production-base.mjs` is the authenticated
production download of version `23d95fd6-1325-4385-a276-e3576127f70c`, recovered
October 17, 2026 America/Chicago. Its original SHA-256 was
`65da80cbc79ee9de38140ac160f4047917c4f1653699288038d3344d8f425cc1`.

The recovered bundle retains the deployed website, authentication, MIME parser,
cryptography, attachments, and Cloudflare mail receiver. Adapters expose its
existing primitives. Readable behavior lives in `src/index.mjs`, `src/resend.mjs`,
`src/reply*.mjs`, and `src/mail-index.mjs`. `src/web-app.mjs` is the recovered
browser application with indexed pagination and direct conversation loading.
This package is the reproducible source for the deployed repairs.

## Receiving and storage

- The existing mailbox configuration includes `support@bindmysoul.com`.
- A Cloudflare cron (`*/2 * * * *`) checks Resend Receiving using the existing
  credential stored as the encrypted `RESEND_RECEIVING_API_KEY` binding.
- Pagination is bounded and resumable. Provider calls are spaced by at least
  650 ms, raw downloads receive no API credential, and redirects are rejected.
- Stable Resend IDs and raw SHA-256 hashes prevent duplicate messages. A
  transaction allocates IDs and commits encrypted chunks, metadata, and indexes.
  Existing read/archive choices survive retries.
- New support messages are immediately available in the cloud archive. They do
  not enter the legacy Cloudflare-to-local queue; the existing local Resend sync
  remains responsible for local mail and follow-up state.
- Historical imports retain original inbound MIME and attachments. Sent history
  is reconstructed from preserved delivery requests, marked with
  `X-Cmail-History-Provenance`, and placed in Archive. Deleted/superseded records
  are excluded. The local database is opened read-only throughout.
- Archive ordering uses original dates, keeping backfilled mail in its proper
  chronological position. Existing archive IDs and encrypted records remain valid.

The signed admin endpoints use the existing timestamped HMAC scheme:

| Route | Purpose |
| --- | --- |
| `/api/mail/resend/status` | Last success, last scheduled success, counters and sanitized failure |
| `/api/mail/resend/sync` | Run receiving; `verify_existing: true` exercises current provider downloads without duplicating history |
| `/api/mail/archive/import` | Import verified support history without local re-ingestion |
| `/api/mail/archive/verify` | Verify raw and attachment hashes without returning message bodies |

All are POST-only. Website content retains Cloudflare Access protection. A failed
scheduled sync is surfaced as a failed Cloudflare invocation and in the signed
status response. Provider bodies, credentials, and download URLs stay out of
diagnostics. Operators should inspect `last_scheduled_success_at` as well as
`status`; the UI's existing “Cloud inbox” badge denotes archive availability.

## Replies

- The inline composer displays the existing sender signature for editing. Drafts
  survive Cancel, Escape, Back and reload within the same browser tab, using
  session storage. Sign out clears those tab drafts. Sending requires an explicit
  click; opening a message, preparing a reply and checking status never send.
- Access protects the assets and API. Sending additionally requires a matching
  Origin, a custom action header and JSON. The server derives From/To/subject
  from the archived MIME; client-supplied recipients or credentials are ignored.
- Bind My Soul uses its existing sending key in `CMAIL_SUPPORT_SEND_KEY`. The
  other five inboxes use their existing account's `CMAIL_RESEND_API_KEY`.
  `CMAIL_SEND_IDENTITIES` carries the allowed From addresses and signatures;
  private signature content is populated from local configuration at deployment.
- Before sending, the exact request is encrypted into the existing archive
  storage. Its UUID and Resend idempotency key remain stable across uncertain
  retries, including concurrent submissions. Ambiguous requests older than
  23 hours stop for review before the provider's 24-hour key expiration.
- Provider acceptance is saved before creating the encrypted sent copy.
  An archive failure can retry without another provider send. Confirmed copies
  appear in the conversation and Archive, marked read. API acceptance is not
  recipient-delivery confirmation. No delivery webhook was introduced.
- Replies currently support plaintext, one reply recipient group derived from
  Reply-To/From, and the existing conversation headers. Attachments, Reply all,
  new-message composition, and cross-device draft sync are separate features.
- Cloud replies remain in the cloud archive and do not modify local follow-up
  classifications or enter the legacy inbound-delivery queue.

Public endpoints: GET/POST `/cmail/api/messages/:id/reply` prepares/sends;
GET `/cmail/api/replies/:request_id` reads a receipt. The internal Durable Object
routes are not exposed through the signed administrative API.

## Sent conversations

The mail-view selector includes **Sent** when `CMAIL_SENT_ENABLED=1`. Sent shows
one row per conversation containing a confirmed outbound message, including
standalone sends. Opening it displays the sent and received messages together.
Later incoming replies update the same row; multiple sends in a thread do not
create duplicate rows. Archive/Inbox state does not determine Sent membership.
The identity selector filters by the mailbox that sent a message in that thread.
Rows show the latest sent recipients and are ordered by latest activity.

`src/sent-index.mjs` maintains connected groups and their sorted Sent entries
in the original message transaction. Exact Message-ID/References/In-Reply-To
links and the stored archive-parent pointer join messages; matching subjects
alone do not. Smaller groups merge into larger ones, preserving aliases for
earlier lookups and moving only the smaller membership set. Sender domains or
From headers never establish a send; the trusted stored outbound direction does.
Normal Sent pagination uses one sorted range and batched encrypted summaries;
it does not decrypt raw MIME or scan the archive. Full-text Sent search examines
up to ten related groups per request, with projected bodies fetched in batches
of 100, and can match older sent or received text in the conversation.

The original encrypted MIME and existing read/archive state are preserved. Sent
uses the same message reader, Reply, downloads and message-state controls as
the other views. A reply without an original Message-ID can still join its
stored archive parent. Historical messages without usable thread evidence
remain standalone rather than being merged by subject.

The signed `/api/mail/sent/status` and `/api/mail/sent/backfill` POST routes
inspect/build the additive index. Deploy the writer first, run backfill, then
activate Sent after readiness matches the original archive count. The deployer
checks readiness and requires indexed reads. New arrivals remain indexed
during the build. An older-code rollback can be followed by idempotent backfill.

`import_history.py --sent-all` inventories confirmed local sent receipts for the
six connected mailboxes, including older records without a populated identity
field. The preserved delivery request supplies the verified sender. Deleted,
superseded and unsent records are excluded. Original local artifacts remain
untouched; reconstructed MIME retains text/HTML, dates, thread headers and
attachment bytes, with an explicit provenance header. Historical sends are
archived/read without entering the delivery queue. The admin import route
accepts outgoing history only for configured mailboxes; inbound history imports
remain restricted to Bind My Soul. Verification compares MIME and attachment
hashes, dates, IDs, mailbox, direction and flags. None of these operations sends
mail through a provider.

## Indexed reading

`CMAIL_FAST_READS=1` enables the indexed API and browser application. New arrivals
commit encrypted summaries and compact text projections in the same transaction
as the original encrypted MIME. Original records, attachment bytes, IDs and
read/archive flags remain authoritative. Mailbox identities and thread-header
tokens are hashed in lookup keys; summary/body content remains encrypted.

- Inbox pages use a mailbox/view/date prefix, stable date-and-ID cursors and
  exact counts. They read only the requested page plus one lookahead entry,
  without decrypting unrelated mail or attachment bytes. Recent mail appears
  first; a selected deep link loads alongside the inbox.
- GET `/cmail/api/messages/:id/conversation` follows indexed Message-ID,
  In-Reply-To and References relationships, including siblings with a missing
  parent. It returns only that conversation's metadata, deduplicated and newest
  first. The client no longer downloads the whole archive's thread index.
- History initially shows up to 20 other message headers. Expanding a message
  loads its text; Show older messages extends the list. Copy thread explicitly
  loads the related bodies in batches of three and copies oldest first.
- Read flags are fetched fresh; archive/unarchive atomically moves the sorted
  entries and counters. Server caches contain immutable projections only and
  are bounded by both entries and estimated bytes (4 MiB per cache). Overlapping
  cold requests preserve correct cache accounting and independent results.
- Explicit full-text searches inspect at most 100 projected messages per API
  request in the selected mailbox/view/date range. The client continues until
  the requested results page plus lookahead is filled, or the range ends.
  Rare/no-match searches can still traverse the selected range; normal inbox
  and conversation loading never perform this search. Stop/Continue remain
  available. Search bodies omit MIME attachments.

Storage reads/writes are batched in groups of at most 128 keys. Diagnostic
`get_calls`, `list_calls` and `keys_read` describe logical storage API work,
not physical disk accesses or billable database operations. Cached runtime
clocks can report zero elapsed milliseconds; use browser timings for UX.

Signed POST administrative routes:

| Route | Purpose |
| --- | --- |
| `/api/mail/performance/status` | Compare indexed count with original archive count |
| `/api/mail/performance/backfill` | Resume an additive, idempotent bounded index build |
| `/api/mail/performance/probe` | Compare cold indexed and legacy read work without returning mail content |

Enable the feature in two stages: deploy index-capable writers while preserving
the existing read flag, run the bounded backfill, then enable fast reads only
when counts match. This keeps new arrivals indexed during the build. The
deployer refuses initial activation while the index is incomplete. On a code
rollback, keep original storage and the additive index; an older writer can
create unindexed arrivals, so resume backfill before reactivation. Backfill
revisits higher IDs missed by a prior saved cursor without duplicating counts.

## Verification and operations

From the Life Ops repository root:

```sh
npm run check --workspace @sproutseeds/cmail-cloud
npm test --workspace @sproutseeds/cmail-cloud
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/import_history.py
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/deploy.py
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/verify_deployment.py --require-scheduled
```

The first two operations test code; the Python commands above inspect existing
mail/configuration and save private receipts. To reproduce the isolated runtime
test, install `miniflare@4.20260730.0` in an external temporary directory and run
`node packages/cmail-cloud/scripts/runtime-smoke.mjs /absolute/path/to/miniflare/dist/src/index.js`.
It checks an upgrade of an existing SQLite Durable Object, native outbound
fetch with a provider fixture, a 7 MB encrypted MIME round trip, authentication,
ordering, deduplication, and preservation of the local delivery queue.
`scripts/reply-runtime-smoke.mjs` takes the same Miniflare module path and checks
the real runtime's outbound path against a network-isolated fake provider,
including concurrent double-submit, durable receipts and sent conversation
headers. Add `--fast` to exercise indexed reads and `--serve` for the synthetic
browser fixture on port 9346. Its
authentication bypass lives only in `test/reply-worker.mjs`; deployment uploads
only `src/*.mjs`.

Explicit production operations:

```sh
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/deploy.py --deploy --expected-version CURRENT_VERSION
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/import_history.py --apply
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/import_history.py --replay
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/deploy.py --schedule --expected-version CURRENT_VERSION
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/deploy.py --deploy --enable-replies --expected-version CURRENT_VERSION --receipt-dir data/qa/cmail-reply-2026-10-17
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/verify_deployment.py --require-replies --require-scheduled --receipt-dir data/qa/cmail-reply-2026-10-17
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/index_archive.py --apply
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/deploy.py --deploy --enable-fast-reads --expected-version CURRENT_VERSION --receipt-dir data/qa/cmail-performance-2026-10-18
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/index_archive.py --probe
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/verify_deployment.py --require-replies --require-scheduled --require-index --receipt-dir data/qa/cmail-performance-2026-10-18
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/index_archive.py --sent --apply --receipt-dir data/qa/cmail-sent-2026-10-18
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/import_history.py --sent-all --receipt-dir data/qa/cmail-sent-2026-10-18/history
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/import_history.py --sent-all --apply --receipt-dir data/qa/cmail-sent-2026-10-18/history
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/deploy.py --deploy --enable-sent --expected-version CURRENT_VERSION --receipt-dir data/qa/cmail-sent-2026-10-18
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/verify_deployment.py --require-replies --require-scheduled --require-index --require-sent --receipt-dir data/qa/cmail-sent-2026-10-18
```

The deployer checks the current version and exact existing namespace before
writing. It inherits all existing bindings, adds the required mailbox/credential,
and verifies preserved bindings afterward. It does not create a namespace, run
a Durable Object migration, change DNS/MX, or alter Access. Private receipts and
source snapshots belong in `data/qa/cmail-bindmysoul-2026-10-17/`; customer content
must not be committed. `--replay` must report zero new records.

For a code rollback, retain the same Durable Object namespace and encrypted
archive. Restore the previous Worker version using Cloudflare deployments and
the exact `production-before-*.json` receipt. Restore the previous cron list
when rolling back to a version without a scheduled handler. Imported mail can
remain encrypted in the archive; rollback never requires deleting records,
recreating storage, changing the encryption key, or undoing local mail state.

Provider references: [Resend receiving list](https://resend.com/docs/api-reference/emails/list-received-emails),
[received email retrieval](https://resend.com/docs/api-reference/emails/retrieve-received-email),
[sending API](https://resend.com/docs/api-reference/emails/send-email),
[send idempotency](https://resend.com/docs/dashboard/emails/idempotency-keys),
[Cloudflare multipart deployment metadata](https://developers.cloudflare.com/workers/configuration/multipart-upload-metadata/).
