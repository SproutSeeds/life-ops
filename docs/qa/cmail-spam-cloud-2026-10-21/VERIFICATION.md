# C-mail cloud Spam release

Evidence revision: `cmail-spam-cloud-2026-10-21-v1`.
Production: https://cmail.frg.earth/?view=spam.
Worker version: `e10204a4-e988-4575-8710-8029f09a446c`.
Previous version: `38ca7f25-8f04-4432-bcea-ec15017ad313`.

## Delivered behavior

The reader and bulk Actions offer Mark as spam. Selected messages move out of
Inbox/Archive into an indexed Spam folder. An encrypted exact-From-address rule
catches future inbound messages across C-mail identities. Case is normalized;
display names, Reply-To, domains, subdomains and plus-address variations do not
broaden the rule. Own addresses and outbound messages are protected.

Not spam restores selected messages to Inbox and removes their sender rules.
Other previously caught mail stays recoverable in Spam. A sender later added as
an owned identity cannot trap its earlier quarantined messages. Ordinary Archive
keeps Spam quarantined; unarchive directs callers to Not spam. All mail labels
Spam and retains the full archive, including Sent and conversation history.

The canonical CLI supports `spam`, `not-spam`, `spam-senders`, and list/search
with `--view spam`. The cloud guide replaces ambiguous historical local Spam
instructions. Historical local rules were not imported or activated.

## Verification

- All 80 cloud package tests passed, including 12 Spam tests for current/future
  routing, cross-inbox exact matching, safe restoration, read/archive state,
  preserved MIME and attachments, idempotency, encrypted restart persistence,
  owned/outbound protection, atomic failures, concurrency, bounded pagination
  and search, legacy indexed records, browser/agent parity and authentication.
- All 10 canonical cloud CLI tests passed. Installed `cmail spam --help` and
  `cmail not-spam --help` resolve the updated checkout without reinstalling or
  invoking the retired mail system. Package syntax checks, Python syntax and
  `git diff --check` passed.
- Native workerd with SQLite Durable Objects verified future classification,
  provider replay deduplication, restore/unblock and unchanged original MIME.
  External connections were disabled. No provider calls or sends occurred.
- The native Spam list probe used one range-list call and three batched point
  reads, with zero raw-message loads. Twenty-item pagination is also covered by
  the bounded unit probe. Inbox/Spam use one read-state value per row; All mail
  and Sent include an additional Spam boolean in their bounded state batches.
  An eligible incoming message adds one exact sender-rule lookup. There is no
  recurring scan, new provider service or full archive migration.
- Browser fixtures verified single-message Spam, a simulated HTTP failure and
  retry, future mail going to Spam, future mail returning to Inbox after Not
  spam, multi-message Spam and restoration, search, exact counts, attachments,
  hidden Spam action on own outbound mail, visible Back, Escape with focus
  restoration, browser Back and keyboard activation.
- Counters around a failed action and successful retry showed exactly two
  action POSTs and no additional list/body/conversation requests. A normal
  successful move requires one action request; the existing pagination logic
  can request the next bounded page when more rows are needed.
- Desktop and 390-by-844 browser layouts were visually inspected. Spam search,
  guidance, labels, Not spam and Back fit the existing responsive reader. This
  is browser viewport evidence, not physical-device acceptance.
- The guarded production deployment preserved all existing bindings, mailbox
  identities, signatures, namespace and schedule. Verification matched all 18
  source modules, checked feature readiness and receiving-schedule health, and
  confirmed access protection for Spam listing and message actions.
- Production `cmail messages --view spam --limit 1` and `cmail spam-senders
  --limit 1` succeeded with zero messages/rules. No real correspondence was
  classified, moved, drafted or sent during release validation.

## Live browser acceptance

The production browser session required sign-in. Automatic approval review first
blocked the Access redirect; its hostname was then verified against the deployed
`CMAIL_ACCESS_TEAM_DOMAIN`, and the normal route successfully reached C-mail's
login page. Review subsequently rejected the Cloudflare identity-provider button
because it could expose broader dashboard account resources beyond C-mail.

No alternate browser, credential injection or authentication bypass was used.
The remaining live visual check requires Cody to sign in to the open C-mail tab.
Production source/API checks and isolated browser acceptance are complete; an
authenticated production UI inspection is not claimed.

## Operations and scope

Rule updates, Spam flags, archive flags, indexed folder placement and counts
commit together. Bulk selections are validated before any mutation. Raw MIME,
attachments and read state are preserved. Once rules exist, rollback or repair
must retain Spam-aware ingestion and folder/action handlers; a pre-Spam build
does not understand the new folder state. Prefer a forward repair.

This feature changes cloud mail classification only. Explicit Life Ops job, bill
and manual obligations keep their existing state. No monitoring or attention
policy was changed. No paid resource, mail provider credential, authentication
policy, DNS record or external correspondence was changed.

Private request counters, fixture screenshots, production receipts and hygiene
inventories are in `data/qa/cmail-spam-cloud-2026-10-21/`. Test routes, synthetic
delivery/failure controls and authentication bypasses exist only under `test/`
and never enter production uploads.

The original 163 unrelated dirty paths remain classified and preserved. Existing
`build/` scratch stays with its owner for review; the private hygiene inventory
records that bucket and the source/documentation groups. Only this feature's
audited files are committed. There are zero unclassified paths. Unrelated commits
ahead of origin are preserved; production uses the established Worker deploy.

Resume intake disposition: **no change**. This restores sender-based Spam parity
within the existing C-mail product work and does not justify a separate public
career accomplishment. The release evidence stays in this project; no canonical
or public resume files were changed.
