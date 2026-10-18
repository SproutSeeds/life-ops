# C-mail cloud receiving repair

Production: <https://cmail.frg.earth>, Cloudflare Worker
`life-ops-email-ingest`. This package restores Bind My Soul receiving to the
website's existing encrypted Durable Object archive.

## Source provenance

The October 13 cloud rebuild's original source directory was absent from both
this checkout and its GitHub main. `src/production-base.mjs` is the authenticated
production download of version `23d95fd6-1325-4385-a276-e3576127f70c`, recovered
October 17, 2026 America/Chicago. Its original SHA-256 was
`65da80cbc79ee9de38140ac160f4047917c4f1653699288038d3344d8f425cc1`.

The recovered bundle retains the deployed website, authentication, MIME parser,
cryptography, attachments, threading, and Cloudflare mail receiver. Two small
adapters expose its existing primitives and allow archive listing through an
original-date index. Readable new behavior lives in `src/index.mjs` and
`src/resend.mjs`. Recovering the original unbundled UI remains desirable; this
package is the reproducible source for the deployed repair.

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

Explicit production operations:

```sh
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/deploy.py --deploy --expected-version CURRENT_VERSION
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/import_history.py --apply
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/import_history.py --replay
PYTHONPATH=src ./.venv/bin/python packages/cmail-cloud/scripts/deploy.py --schedule --expected-version CURRENT_VERSION
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
[Cloudflare multipart deployment metadata](https://developers.cloudflare.com/workers/configuration/multipart-upload-metadata/).
