# Bind My Soul cloud inbox repair

Event: `cmail-bindmysoul-production-repair-2026-10-17-r1`.

Production: <https://cmail.frg.earth>. User authorized the complete repair and
production implementation on October 17, 2026 America/Chicago.

Status: **Deployed and verified, including automatic receiving.**

## Result

The website's configured inbox selector now includes `support@bindmysoul.com`.
Its original missing source, Resend Receiving, is connected directly to the
existing encrypted cloud archive. A two-minute Cloudflare schedule is enabled.
The active version is `ca0ae8f3-c44e-4f02-83c0-4dc81b844a91`.

The historical import restored **45 received messages** and **43 active sent
replies**, with **11 image attachments**. Inbox shows 45; Archive shows 43; All
mail paginates through all 88. All inboxes retains its original recent messages
and now has 2,429 inbox messages. The full archive contains 2,472 records.

All 45 received messages preserve their original raw MIME bytes. The 43 sent
messages were reconstructed from saved successful delivery requests, retaining
their text/HTML, original Message-ID, reply/reference headers and sent date.
Their raw source includes an explicit reconstruction provenance header. One
superseded/deleted send and two deleted drafts were excluded.

The import retained 42 read and three unread inbound messages. Sent history is
read and archived. All support communication rows in the local database stayed
unchanged, verified by before/after SHA-256
`5fb97e526d1d9e5744b3af842c6fa7205393ece4c6331ffcfa17eb588cc4b4e1`.
The pre-existing local sync and follow-up classifications remain authoritative
for local operations.

## Implementation

`packages/cmail-cloud/` is the recovered and reviewable deployment source.
The deployed October 13 bundle was preserved exactly except for two adapters:
export existing storage/authentication primitives and read archive lists through
an original-date metadata index. The new modules add resumable receiving,
stable source-ID/raw-hash deduplication, transactional encrypted writes, signed
history import and integrity verification, and source-specific diagnostics.

The existing Durable Object namespace, encryption key, ingestion key, Access
settings and other bindings were preserved and compared after deployment. No
new storage namespace, Durable Object migration, DNS/MX change, or message send
was required. Historical and Resend cloud imports do not re-enter the local
Cloudflare delivery queue; it remains at zero pending messages.

The first live receiving check exposed workerd's unsupported `redirect: error`
option. This was reproduced in the actual runtime, fixed with `manual` plus
explicit non-2xx rejection, and added to the runtime test. The final deployed
version successfully retrieved all three currently provider-visible messages,
recognizing them as duplicates of restored history.

## Verified evidence

- **11 passing regression tests:** encrypted storage, state preservation,
  chronological pagination, concurrency, attachments, source deduplication,
  pagination/catch-up, cursor stalls, transaction failure, authentication, and
  scheduled-error propagation.
- **Actual workerd/SQLite test:** upgrade existing archive data, 7 MB encrypted
  MIME round trip, provider fixture through native fetch, signed admin requests,
  website access, and unchanged pending-delivery behavior.
- **88/88 production messages:** raw SHA-256, byte count, Message-ID, date,
  direction, read/archive state and all 11 attachment hashes verified.
- **88/88 replay:** zero new records; all duplicates, with preserved state and
  unchanged local communication rows.
- **Live provider path:** three real receiving downloads, three duplicates,
  zero errors; `2026-10-18T04:13:04.948Z`.
- **Authenticated website:** ordinary dropdown option, 45-message Inbox,
  43-message Archive, All mail pages 1–50 and 51–88, original date ordering,
  message body, attachment control, and reconstructed conversation display.
  The selected attachment message was already read before inspection.
- **Source verification:** authenticated download of all three deployed modules
  matches local SHA-256s. The website and Worker mail API redirect unauthenticated
  callers to Access; unsigned admin requests return HTTP 401.
- **Automatic run:** schedule configured at `2026-10-18T04:13:50.891086Z`;
  the first observed platform-triggered run completed healthy at
  `2026-10-18T04:20:41.165Z`, with three existing messages recognized, zero new
  duplicates, and no error. No manual request used the scheduled trigger label.

Private receipts, inventory, source snapshot and screenshot are in
`data/qa/cmail-bindmysoul-2026-10-17/`. Customer content and original email files
are excluded from Git. `packages/cmail-cloud/README.md` documents reproduction,
diagnostics and rollback. The prior read-only diagnosis remains in
`INVESTIGATION.md` as historical evidence.

Acceptance limits: no new SMTP message was sent as part of verification. The
real provider messages exercised receiving/download/deduplication; fixture tests
exercised first-time scheduled insertion. Sent history is reconstructed rather
than represented as an original outbound SMTP capture. The original unbundled
website source remains unavailable; the exact deployed bundle is preserved.

## Worktree and evidence decision

This repair owns `packages/cmail-cloud/**`, the two C-mail workspace entries in
`package-lock.json`, and `docs/qa/cmail-bindmysoul-2026-10-17/**`. Existing Rhythms
lockfile changes and every unrelated dirty path remain outside this change.
The complete classified dirty-path inventory is saved privately in
`worktree-closeout.json`, including an exact path inventory and next action for
each unrelated bucket; zero paths are unclassified and expansion is allowed.
Pre-existing `build/` output remains with its original lane for review.

Production was deployed directly through the existing Cloudflare API. The
repair's source is retained in a scoped local commit. Public `origin/main` is
behind three pre-existing local commits (`75e2896`, `bf86d13`, `dc311db`); those
unrelated commits were not published as a side effect of this repair. The next
source-publication action is to review that existing branch and the recovered
deployment package together under the repository's release policy.

Resume decision: **no-change / private repair evidence**. The current canonical
Founder Operations accomplishment already covers CMAIL ingestion, encrypted
storage, retries, threading, health checks and product support mail
(`master-resume-system/resume_evidence.py`, Founder Operations). This restores
that represented operating capability after the website rebuild. Private inbox
counts and customer records do not justify a separate public accomplishment.
Canonical resume artifacts and the public resume were not edited or redeployed.
