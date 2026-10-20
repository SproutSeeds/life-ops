# C-mail Drafts split view

Evidence revision: `cmail-draft-pane-2026-10-20-v1`.
Production: https://cmail.frg.earth/?view=drafts.
Worker version: `b7ab2d15-f2d3-4021-b90c-5f567c500b3e`.
Previous version: `89c5863d-5683-4067-93db-22a85c248698`.

## Result

Opening a saved draft now shows an editable reading pane beside the draft list.
The desktop sidebar uses the Inbox widths and selection treatment. New draft
also opens in this pane while viewing Drafts. Narrow screens show the selected
editor with a visible Back control. Inbox Compose and Reply retain their existing
shared composer.

The same revision-checked cloud save, conflict recovery, recipient matching and
send receipt logic serve both presentations. Switching drafts, changing mailbox
or view, clicking the Inbox brand link, Escape and browser Back preserve pending
edits before leaving. A failed save keeps the current editor and restores the
route. An actual concurrent edit is rejected and offers Save a copy.

Direct draft links wait for the split workspace to mount. Selection is reflected
in the URL and sidebar, and returning restores focus to the draft row. List
refreshes preserve the active editor and restore row focus after replacement.
No database schema, mail transport, credentials or production access changed.

## Verification

- All 53 cloud package tests passed, including draft persistence, encryption,
  bounded list reads, concurrent revisions, send idempotency, thread/CC/BCC
  preservation, authentication and agent/browser compatibility.
- Package syntax checks and `git diff --check` passed.
- Native workerd with SQLite Durable Objects passed the existing Drafts runtime
  smoke. Its fake provider is isolated with external networking disabled.
- Browser fixture verified direct links, selecting and switching saved drafts,
  new drafts, explicit Save, Back, Escape, focus restoration, Inbox navigation,
  save retry after a simulated outage, failure during view changes and browser
  Back, a two-tab conflict, Save a copy, and confirmed fake Send leaving Drafts.
- Desktop screenshots were visually inspected. A 390-by-844 same-origin fixture
  iframe verified the actual narrow-layout CSS, editor and visible Back control.
  This is browser viewport verification, not physical-device acceptance.
- Clicking the already-selected draft and typing added zero API calls. One
  explicit save added one save request and one bounded list refresh, with no
  extra draft-body read, contact request or send. The existing list test confirms
  a 50-item page uses one list call, two get calls and no raw-mail loads.
- Production verification downloaded and matched all 15 source modules, checked
  the existing namespace and schedule, confirmed all feature readiness flags,
  and verified unauthenticated routes remain protected.
- A temporary production draft was saved through the canonical CLI, reopened
  exactly, edited in the deployed pane and saved by its Back control. The CLI
  then verified revision 2 and the exact edited body before discarding that
  temporary draft. The live pane showed a 390px list, selected row, inline editor
  and zero open modal dialogs. Production Send was never clicked or invoked.

The real draft used for the initial read-only comparison changed from editing
to sent during this work, independently of these verification actions. Its
existing sent receipt was preserved. It was neither rewritten nor recreated.

Private screenshots, request counters, draft receipts and deployment receipts
are retained under `data/qa/cmail-draft-pane-2026-10-20/`. Correspondence and
credentials are excluded from this committed evidence.

## Scope and completion

Only three production UI modules, the isolated browser fixture and this QA note
belong to this change. The original 163 dirty workspace paths remain preserved
in their existing hygiene categories; no unclassified paths were introduced.
The production fixture iframe and failure controls exist only in the test
worker, which the deployment script never uploads.

Resume intake disposition: **no change**. This is a routine presentation and
navigation refinement of the existing CMAIL accomplishment in the master
resume's Founder Operations entry. It does not justify a separate public
accomplishment or publication, and private correspondence stays out of resume
evidence. No canonical resume or public resume files were changed.
