# C-mail Compose and recipient history release

Implemented and verified October 19, 2026 America/Chicago. Production is
<https://cmail.frg.earth>; Worker `life-ops-email-ingest`.

## User behavior

- Compose is always available in the top bar. The selected inbox determines
  the default sender; All inboxes uses the primary configured identity.
- Names and address fragments match existing received and sent history.
  Multiple results narrow as the user types; a unique result becomes a single
  completion hint accepted with Tab, Enter or a click. Selected and complete
  addresses clear suggestions. New addresses can be entered directly.
- Up to ten recipients, editable subject/body/signature, explicit Send, and
  a direct View in Sent link after confirmation. New mail starts its own thread.
- Cancel, Escape, browser Back and reload preserve the tab's draft. Escape
  dismisses suggestions first. Closing restores focus and retains the reader
  underneath. Discard clears the draft; Sign out clears drafts and contacts.

## Implementation and limits

The existing encrypted durable outbox now serves both replies and new messages.
Server-side sender allowlisting, per-account credentials, recipient/header
validation, exact-payload retry binding, stable provider idempotency keys and
archive-only recovery apply to both. The browser receives identity signatures
but no credentials. Access, origin checks and explicit-action headers remain
required. Sending tests used only synthetic addresses and an isolated fake
provider. Live checks did not submit a message or change existing mail state.

The additive encrypted contact index updates atomically with mail intake.
Backfill processed all **2,768** archived messages into **726** distinct contacts.
Sent remained **309 messages / 250 conversations**, with zero queued local
deliveries. Original message content and read/archive flags remain authoritative.
Incoming From/Reply-To and trusted outbound To/Cc supply contacts; configured
own addresses are excluded. Large header fanout is capped at 500 contacts per
message, preserving ingestion. Contacts are matches from history, not verified
identities or inferred recipients.

Autocomplete loads three bounded catalog pages on first opening and performs
zero network/storage calls per keystroke. A five-minute page-memory cache is
invalidated after a send. Interrupted pagination detects index revisions and
restarts. The first cold production page returned 250 contacts using three
logical point-read batches and one range listing, with zero raw MIME loads.
These are application storage calls, not physical or billed I/O counts.
Runtime cached clocks returned 0 ms, so this receipt makes no latency claim
from those clocks. No contact catalog is fetched for ordinary inbox loading.

This release supports plaintext and one new-message draft per browser tab.
Attachments, Cc/Bcc composition and cross-device draft sync remain outside scope.
Provider acceptance and saving a Sent copy do not establish recipient delivery.

## Verification

- `npm run check --workspace @sproutseeds/cmail-cloud` passed.
- `npm test --workspace @sproutseeds/cmail-cloud`: **38 passed**. Coverage includes
  recipient/header validation, account routing, standalone Sent threads,
  changed-recipient retry rejection, shared reply/compose UUID collision,
  archive recovery without resending, encrypted contact deduplication, bounded
  pagination, resumable backfill, revision changes, ranking and auth gates.
- Native workerd with SQLite Durable Objects passed Compose and existing Reply
  runtime smoke tests. Concurrent duplicate submissions called the fake mail
  provider once; receipt replays did not resend.
- Browser fixture passed multiple matches, unique completion, arrow navigation,
  click/Tab/Enter selection, exact-address suppression, new addresses, multiple
  recipients, sender signature changes, signature-only Send disabling, draft
  reload, visible Cancel, Escape, browser Back, focus restoration, explicit
  fake-provider send and the corrected View in Sent link.
- Fixture request counters stayed at one catalog load while typing and
  reopening within the same page; reload triggered one fresh catalog load.
  Enter in recipient/subject fields did not submit a send.
- Desktop composer layout was inspected from a screenshot. Physical mobile
  acceptance was not performed.
- Live browser confirmed Compose, all six sender choices, support-inbox default,
  five history matches for a broad fragment, one narrowed hint, Tab completion
  and clearing suggestions. The unsent verification draft was discarded.
- Production source download matched all 11 local modules. Namespace, existing
  bindings, cron, encrypted archive and Access protection were preserved.
  Unauthenticated website, mail, Compose and contacts requests redirect through
  Access; unsigned admin requests return 401. Mail sync was healthy.

## Deployment and evidence

Original version: `f3289366-a728-4221-9cca-dd65c997cd59`.
Index writer: `2fea42b2-e37f-4a8b-8daa-e7d2140e8f43`.
Compose activation: `829bbd99-0964-47be-8fd3-d9c78aaacb74`.

The new signed contact endpoint initially returned 404 during deployment
propagation, then became available without a code/configuration change.
Backfill started only after readiness was observable. Its idempotent replay
and activation gate preserve the original archive and existing inbox behavior.
Rollback retains the same namespace and additive contact data. After an older
writer rollback, rerun contact backfill before re-enabling Compose.

Private receipts, full contact-free build counts, hashes, runtime results and
screenshots are in ignored `data/qa/cmail-compose-2026-10-19/`. Contact names,
email contents, signatures and deployment settings are excluded from commits.
Only this cloud package and this QA record belong to the change. The 162
pre-existing dirty paths remain classified and preserved; `build/` is existing
scratch owned by its prior lane. Their next action remains with that lane.
The deployed source is committed locally as a bounded change. A broad push of
unrelated local commits to the repository remote is outside this release.

## Resume decision

**No change.** This is an incremental completion of the existing private CMAIL
workflow already represented under Founder Operations in
`master-resume-system/resume_evidence.py` (CMAIL and LifeOps Actions). That entry
already covers outbound mail, encrypted persistence, threading and retries.
The release evidence stays here; customer correspondence is private. No new
public accomplishment, canonical resume edit or public resume publication is
justified by this bounded feature addition. No document render or public resume
deployment is claimed.
