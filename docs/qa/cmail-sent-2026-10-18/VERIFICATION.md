# C-mail Sent conversations and restored sending history

Event: `cmail-cloud-sent-2026-10-18-r1`.

Production: <https://cmail.frg.earth>, Worker `life-ops-email-ingest`, version
`f3289366-a728-4221-9cca-dd65c997cd59`. Sent is enabled in the existing mail-view
selector. The deployment preserves the original Durable Object namespace,
credentials, encryption, Access configuration and two-minute receiving cron.

## Delivered behavior

Sent shows one row per conversation containing at least one stored outbound
message. Standalone sends and mixed incoming/outgoing threads both appear.
Multiple sends in the same thread share a row, later incoming replies update
its latest activity, and opening it shows the complete available conversation.
Archiving or moving a message to Inbox does not remove Sent membership. The
identity selector filters by sending mailbox, and rows show sent recipients.

The Sent group index is updated in the same transaction as the original mail
and encrypted read projections. Exact thread headers and stored archive-parent
pointers connect messages; sender headers and matching subjects alone do not
establish membership. Smaller connected groups merge into larger groups with
stable aliases. Normal Sent loading performs a sorted range lookup and batched
summary/flag reads. It never scans the archive or parses unrelated MIME.

The existing reader, Reply, Copy thread, attachments and message-state controls
remain available. The visible back label follows the selected view; Escape and
the back control return to the list, restore focus, and preserve Sent. Opening
a row also supports browser Back. Full-text search includes older sent and
incoming content in the matching conversation; it processes up to ten related
groups per request and projected bodies in batches of 100.

## Historical coverage and preservation

The website initially had 44 sent messages, including the recently sent cloud
reply. A read-only inventory found 308 active, confirmed sent records locally.
Forty-three already existed in the cloud. The remaining **265 messages** were
restored, producing **309 sent messages in 250 conversations**:

| Sending mailbox | Stored sent messages | Sent conversations |
| --- | ---: | ---: |
| cody@frg.earth | 260 | 230 |
| support@bindmysoul.com | 48 | 19 |
| info@frg.earth | 1 | 1 |

Older local records sometimes lacked the identity field; their preserved
delivery request supplied the sending mailbox. Deleted, superseded and unsent
rows were excluded. One historical send lacks a Message-ID and is retained
without inventing one. The original sender, recipients, text/HTML, dates,
available thread headers and attachment bytes are reconstructed into MIME
with an explicit provenance header. All cloud copies remain encrypted.

Every one of the 308 historical messages was verified against the cloud for raw
SHA-256, byte length, Message-ID, date, mailbox, direction and expected flags.
All **81 attachment payload hashes** matched. The original local-row snapshot
hash remained unchanged. The import stored 265 and recognized 43 duplicates;
it did not resend mail or enter the inbound delivery queue. Existing cloud IDs
and duplicate records' state were preserved. Historical sent copies are read
and archived while remaining visible in Sent.

One independent new incoming message arrived during the work. Both read indexes
were ready at **2,741 / 2,741 messages** in final verification, with zero pending
deliveries. The receiving cron completed healthy after activation at
`2026-10-18T05:42:13.254Z`.

## Verification and performance

- All **30 Node tests** pass. Six new Sent tests cover standalone/mixed threads,
  late component merges, missing-parent/header cases, multiple identities,
  stable pagination, bounded storage reads, full-thread body search, date
  filters, Archive independence, fresh read flags, resumable backfill,
  concurrent arrivals, older-code rollback, authentication and import scope.
- Actual workerd/SQLite suites passed authenticated routes, native fake-provider
  replies, double-submit idempotency, encrypted receipts and saved Sent copies.
  The Sent runtime verifies standalone and mixed conversations and zero raw
  MIME reads for its list. The preservation runtime passed a **22 MiB MIME
  round trip**, exceeding the largest restored message's 22,191,328 bytes.
- The isolated browser verified both conversation types, lazy incoming-body
  expansion, identity filtering, empty state, Escape, visible Back, browser Back,
  focus restoration and retaining both Sent rows after Archive. Provider sends
  were synthetic and network-isolated. No real email was sent by this change.
- Authenticated production verifies the Sent option, 19 support conversations,
  230 primary conversations, all-mailbox pages `1–50 of 250` and `51–100 of 250`,
  and the user's two-message support conversation opened from its actual Sent
  row. Searching an older incoming message's body returns that mixed thread.
- Production download hashes match all nine local source modules. Website,
  Sent and conversation APIs retain Access protection; unsigned administration
  is rejected. Module syntax and `git diff --check` pass.

Three authenticated Chrome full reloads of the actual support Sent row:

| Visible state | Observed range |
| --- | ---: |
| All 19 support Sent rows | 0.151–0.189 s |
| Selected sent message | 0.151–0.175 s |
| Two-message conversation metadata | 0.220–0.246 s |

These are observed browser timings, not a general latency or load guarantee.
Cold application-cache storage probes on the live archive:

| Operation | get calls | list calls | Keys requested/listed | Original MIME loads |
| --- | ---: | ---: | ---: | ---: |
| Complete support Sent list, 19 threads | 3 | 1 | 84 | 0 |
| All-mailbox Sent page, 50 threads | 3 | 1 | 209 | 0 |
| Selected two-message conversation | 5 | 1 | 12 | 0 |

These are logical storage API measurements, not physical or billed I/O.
Full-text search is an explicit scan of related projected content; ordinary
Sent/inbox/conversation loads use indexes. Private receipts, source hashes,
history manifests, browser timings and screenshots are under
`data/qa/cmail-sent-2026-10-18/`. Customer and employer correspondence is excluded
from committed evidence.

## Worktree and evidence decision

Only the C-mail cloud package and this record belong to this change. Unrelated
Life Ops paths remain classified under their existing owners; the private
worktree closeout lists their exact buckets and next actions. Production was
published directly through Cloudflare. Unrelated local main commits were not
pushed to the public remote.

Resume decision: **no-change / completion of an existing private operating
workflow**. The represented Founder Operations capability already covers CMAIL,
encrypted mail storage, Resend delivery and threading. This restores the rebuilt
website's Sent workflow and preserves its history. Private correspondence and
incident-specific measurements do not warrant another public accomplishment;
no canonical resume edit or public resume deployment is needed for this event.
