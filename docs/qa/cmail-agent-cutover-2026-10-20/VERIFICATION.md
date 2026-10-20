# C-mail agent and legacy-system cutover

Evidence revision: `cmail-agent-cutover-2026-10-20-v1`.
Production: https://cmail.frg.earth.
Final worker version: `89c5863d-5683-4067-93db-22a85c248698`.
Previous Drafts release: `e4835f66-68c3-4323-bdd9-9c70518a7de0`.

## Result and cause

The website stored drafts in the encrypted cloud Durable Object while installed
agent commands and global instructions still saved into local SQLite. That split
made a valid agent-created draft invisible in the new website. The installed
`cmail` and `lifeops cmail` commands now share the website's actual mail, drafts,
contacts, thread indexes and durable send receipts through an authenticated API.

The cutover was explicitly authorized by Cody, including migration of the saved
project correspondence. It did not authorize delivering any email. No real
provider send occurred during implementation, migration or verification.

## Canonical routing and legacy retirement

- The new signed API has a fixed action allowlist, timestamped HMAC verification,
  a deployment enable flag, private/no-store responses and an explicit send
  confirmation requirement. It reuses the same revision checks and durable
  outbox as the browser. Credentials stay in the existing Keychain path; the
  destination is pinned in code and has no local/provider fallback.
- Installed Node entrypoints, exported CMAIL command dispatch and Python legacy
  entrypoints use the cutover configuration. Legacy direct draft writes and
  direct Resend sends are guarded. Unknown/unsupported operations fail clearly.
- The old localhost and Tailscale UI now return 308 to the cloud website; old
  mail APIs return 410. The local server uses a lightweight redirect/health
  handler without opening the retired mailbox or warming its caches. The old
  sending thread is disabled; its pending queue was empty at cutover.
- Global Codex, Claude, home, volume, code-root and project instructions were
  updated, with original copies retained privately. Nine instruction entrypoints
  and four guide entrypoints received current routing. Current CMAIL guides were
  rewritten and their old versions explicitly archived. No model memory files
  were edited.
- A consistent SQLite backup passed `quick_check` and has a SHA-256 manifest.
  The snapshot is read-only inside a private archive directory. Existing mail,
  Gmail records, attachment vaults, send receipts and task dispositions were
  preserved. The live local database remains a Life Ops automation projection;
  it is not the mail/draft authority. Existing sealing and backup jobs remain.

## Draft migration and compatibility

Seven active drafts were migrated using deterministic IDs and stable save IDs,
then reopened from the cloud and compared with the originals. Exact bodies,
subjects, senders, To/CC/BCC and reply headers were retained. The migration found
no active draft attachments. An earlier deleted draft was preserved in history
and was not resurrected.

Migrated cloud drafts remain editable and unsent. Original relative recency was
restored using revision-checked, content-identical saves; original timestamps
remain in the migration receipts and frozen snapshot. The most recent requested
draft is first in the list. Numeric legacy draft IDs resolve to their verified
cloud IDs. The seven old live rows became historical reference records with
cloud links; their bodies stayed unchanged and no active local drafts remain.
The snapshot retains the original draft state.

CC/BCC and original reply headers survive agent/browser edits and exact send
retries. BCC addresses are envelope recipients at the provider and are omitted
from its recipient-visible headers; the owner's private Sent copy retains them.
Tabs opened before these fields existed cannot silently strip them: omitted
fields retain stored values, while explicit empty fields clear them. CLI edits
preserve fields that were not specified, including the sending identity. Stale
revisions fail rather than overwrite concurrent edits.

## Efficiency and Life Ops projection

Interactive mail and draft reads still use existing bounded indexes. The agent
API introduces no browser polling or scans. The existing local sync worker now
uses an ascending ingestion cursor over cloud records, rather than scanning
inboxes or polling Resend separately. It reads at most ten new messages in one
pass and stops a raw batch after reaching 4 MiB; a large message can exceed that
threshold individually. Ordinary mailbox views never use this raw export.

The projection skips existing Message-IDs and preserves task classifications.
A successful new record commits with its cursor, so failure cannot skip a mail
item. Outbound cloud records retain outbound direction, sent state and thread
headers. An empty pass uses one bounded remote listing and loads no raw mail.
The projection covers new arrivals from the recorded cutover cursor; the whole
older local/Gmail archive was not reimported as another cloud mailbox.

## Verification

- **53 JavaScript tests passed**: existing mail/index/draft regressions plus
  signed agent authentication, stale/tampered rejection, feature gating, shared
  browser/agent drafts, explicit-send requirement, idempotency, CC/BCC/thread
  preservation, old-tab compatibility and bounded resumable raw projection.
- **28 Python tests passed** across the new cloud client and existing ingest
  suite. Coverage includes legacy write/send guards, redirect retirement,
  avoiding old mailbox/cache initialization, stable save IDs, field preservation,
  no fallback, explicit send confirmation, projection deduplication/recovery,
  and outbound projection status/headers.
- The **20 existing Node CLI tests passed** before machine activation, retaining
  upstream behavior for machines without a cutover configuration. Actual
  installed commands were then verified after activation from `/tmp`.
- Native workerd with SQLite Durable Objects passed shared agent/browser draft
  visibility, an old browser save preserving headers, concurrent send retry
  deduplication and ascending projection cursors. Its only provider was an
  isolated fake Resend endpoint with external networking disabled.
- Browser fixture verified CC/BCC visibility, website edits read back through
  the agent API with all headers intact, Save & close and browser Back. The
  composer screenshot was visually inspected. No physical-device claim is made.
- Production browser opened the requested migrated draft, showed the expected
  sender/recipient/subject and original wording, and showed seven saved drafts.
  Production Send was never clicked. The final direct draft link was refreshed.
- Two temporary production CLI verification drafts were saved, reread and
  discarded. The second proved edit field preservation, identical-save retry
  deduplication and stale-edit rejection. The final cloud draft count is seven.
- Live old localhost/Tailscale URLs return 308; old draft/read/send APIs return
  410. Installed and repository legacy send commands and direct Python Resend
  calls were rejected before provider work. Local pending delivery count is zero.
- All **15 production source modules** match the verified local source hashes.
  Existing namespace, bindings, Access policy and two-minute receiving schedule
  were preserved. Unauthenticated website requests redirect through Access and
  unsigned agent requests return 401. Scheduled receiving remains healthy.

Private receipts, hashes, original-content comparisons, screenshots, installed
module manifests, guide backups and local runtime evidence are in ignored
`data/qa/cmail-agent-cutover-2026-10-20/` and the private legacy archive. Customer
correspondence, addresses, signatures and credentials are excluded from this
public verification record and the source commit.

## Scope, remaining limits and resume decision

Cloud sending currently exposes plain text, To/CC/BCC and reply headers.
Attachment/HTML-only sending, delayed/batch sending and local-label operations
fail as unsupported; they do not invoke the old system. Secondary Gmail remains
separate for explicitly requested Gmail work. The cutover governs installed
entrypoints and instructions; it is not an OS sandbox against arbitrary code
with the account's filesystem and credential access.

Only this bounded cloud/client/retirement change and its documentation are
committed. Preexisting dirty work remains classified and preserved. Owned hunks
in previously dirty files are staged separately. Machine-specific instruction
changes and the preexisting untracked storage map remain local with exact
backup/receipt records. A broad push of unrelated local commits is outside this
production deployment.

**Resume: no change.** This is an incremental repair of the private CMAIL
workflow already represented by the existing Founder Operations accomplishment
in `master-resume-system/resume_evidence.py`, which covers outbound delivery,
threading, encrypted persistence, retries and health. This record supplies the
specific engineering evidence; the private correspondence and machine routing
change do not justify another public accomplishment. No canonical resume edit,
document render or public resume deployment is claimed.
