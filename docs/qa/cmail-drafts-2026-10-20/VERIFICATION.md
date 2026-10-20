# C-mail saved Drafts release

Implemented and verified October 20, 2026 America/Chicago.
Production: <https://cmail.frg.earth/?view=drafts>.
Evidence revision: `cmail-drafts-2026-10-20-v1`.

## User behavior

- Drafts is an option beside Inbox and Sent. Its New draft button opens the
  composer, with the selected mailbox as the sending identity.
- Save draft keeps multiple independent emails without sending. Unfinished
  recipients, empty subjects and blank messages can be saved.
- Save & close, Escape and browser Back save changes and return to the previous
  screen. Saved emails reopen from Drafts, including in another signed-in tab.
  Reply uses the same composer and Drafts mailbox.
- Unsaved typing uses tab recovery with no network calls. Explicit save/close
  persists to the cloud; closing the browser before saving is not a cloud-save
  guarantee. Sign out clears local recovery while retaining saved cloud drafts.
- Concurrent edits cannot silently overwrite a newer revision. Save a copy
  preserves the local version separately. Save failures retain local content and
  offer retry or Close, keep local copy.
- Discard removes the selected draft. Explicit Send validates and submits the
  saved content; confirmed sends leave Drafts and appear in Sent. Pending
  confirmation retains the draft and prevents editing its submitted payload.

## Persistence, efficiency and safety

The additive draft store uses the existing encrypted Durable Object. Draft
metadata and small encrypted previews are separate from encrypted bodies, with
bounded chunks for large messages. Original mail, flags, queues and attachments
are untouched by save/list/discard. Sender choices remain server allowlisted.
Reply routing and thread headers come from the archived original.

An indexed range returns newest edits first with exact counts and batched
previews. Native Durable Object multi-get returns keys in sorted order, so the
implementation explicitly follows the original range ordering when assembling
the response. A cold production page containing one verification draft used
**two point-read calls, one range listing and three keys**, with zero raw MIME
loads. The larger unit fixture also verifies bounded pagination and no body
reads. These are logical storage operations, not physical or billable I/O.
The runtime clock reported 0 ms; this report makes no latency claim from it.

Normal pages contain at most 50 drafts. Search checks at most 100 summaries
per request and provides explicit Next navigation. The mailbox selector filters
by sending identity. Drafts view skips inbox/conversation loading. Another tab
refreshes the list when reopened or when Refresh is clicked.

Atomic revision checks, immutable save IDs and tombstones prevent duplicate
saves, stale overwrites and resurrection after discard/send. Sending reuses the
existing durable outbox and provider idempotency key. Acceptance is recorded
before archive completion; archive recovery never resubmits accepted mail.
GET/list/save/close/discard do not send. No real provider send was used in QA.

Access protects all draft assets and APIs. Mutations require same-origin JSON
and the draft action header. The signed administrative API exposes draft
status and a content-free performance probe, with no draft mutation routes.

## Verification

- Package syntax checks and Python deployment-script compilation passed.
- Package tests: **47 passed, zero failures**. Draft regressions cover encryption,
  multiple drafts, incomplete fields, save replay, revision conflicts, chunk
  storage/cleanup, tombstones, range ordering, bounded preview-only pagination,
  identity filtering, validation, authenticated HTTP and same-origin checks.
- Send tests cover concurrent submissions, ambiguous retries, provider
  rejection, archive failure, crash recovery, complete reply threading and
  removal from Drafts only after confirmation. Existing receiving, contacts,
  indexed reading, Compose, Reply and Sent regressions remain passing.
- Native workerd with SQLite Durable Objects passed Drafts, Compose and Reply
  runtime smoke tests. Their isolated fake provider received one message per
  logical send, including concurrent duplicate requests.
- Browser fixture verified New draft, explicit save, Save & close, fresh-tab
  reopening, two-tab conflict detection and Save a copy preserving both bodies.
  Reply drafts saved via Escape and browser Back, then reopened and sent through
  the fake provider. Discard preserved the other draft; mailbox filtering showed
  the correct empty state. Newest-first ordering was verified after correcting
  the native multi-get ordering assumption.
- Desktop list and composer screenshots were inspected. Physical mobile and
  separate-device acceptance were not performed.
- Live production browser verified Drafts, New draft and saving an unfinished
  recipient/body. A fresh tab reopened the exact saved content. The temporary
  verification draft was discarded, returning the count from 1 to 0. Production
  Send was never clicked. Existing correspondence and read/archive flags were
  not changed by these browser checks.
- The production download matched all **14 source modules**. Existing namespace,
  bindings, Access and the two-minute receiving cron were preserved. All checked
  unauthenticated mail/draft routes redirected through Access; unsigned admin
  requests returned 401. Scheduled receiving was healthy.
- Production archive/index count was **2,773**; Sent contained **310 messages /
  251 conversations**, contacts **726**, and the local delivery queue was empty.
  Archive and Sent counts remained unchanged after the draft-only live check.

## Deployment and scope

Previous production version: `829bbd99-0964-47be-8fd3-d9c78aaacb74`.
Drafts production version: `e4835f66-68c3-4323-bdd9-9c70518a7de0`.
Feature activation: `CMAIL_DRAFTS_ENABLED=1`.

Deployment used the existing Cloudflare route with an expected-version check;
no namespace, credential, domain, Access or migration changes were needed.
Rollback can restore the previous Worker while retaining encrypted draft keys
for later reactivation. Unconfirmed legacy tab sends retain their old receipt
flow, while unsent tab drafts migrate only after an acknowledged cloud save.

Private deployment receipts, source hashes, status/probe results, screenshots
and runtime output are in ignored `data/qa/cmail-drafts-2026-10-20/`. Customer
content, contact history and secret/signature configuration stay out of commits.
The scoped source and this record are committed locally. Production was deployed
directly; unrelated local commits were not pushed to the repository remote.

The 163 pre-existing dirty paths remain classified and preserved. Existing
`build/` scratch stays with its prior owner. The private closeout inventory lists
the remaining exact buckets and next actions. No unclassified paths remain.

## Resume decision

**No change.** This increment completes the existing private CMAIL workflow
already grouped under Founder Operations in the authoritative
`master-resume-system/resume_evidence.py` entry for CMAIL and LifeOps Actions.
That entry covers outbound mail, encrypted persistence, threading and retries.
This saved-draft feature does not justify another public accomplishment or
republication of the resume. Its verified engineering evidence remains in this
project record. No canonical resume edit, document render or public resume
deployment is claimed.
