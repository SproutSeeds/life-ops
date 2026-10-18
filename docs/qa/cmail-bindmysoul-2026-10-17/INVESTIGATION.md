# Bind My Soul inbox missing from cloud C-mail

Event: `cmail-bindmysoul-cloud-inventory-2026-10-17-r1`

Checked: October 17, 2026 America/Chicago (October 18 UTC).

Status at the initial read-only checkpoint: **Verified diagnosis; repair pending.**
The subsequently authorized production repair is recorded in [REPAIR.md](REPAIR.md).
This diagnostic pass inspected the live
website, deployment settings and source, provider inventory, DNS, and the local
runtime. It made no production configuration, routing, mail-state, or data-import
changes.

## Findings

1. The live website at `https://cmail.frg.earth` omits
   `support@bindmysoul.com` from its configured inbox list. Cloudflare's current
   `CMAIL_MAILBOXES` setting is exactly:

   ```text
   cody@frg.earth,cody@codacli.com,info@frg.earth,clawdad@frg.earth,support@deckfound.com
   ```

2. The website and the older local C-mail UI have different data sources. The
   cloud website reads the encrypted Cloudflare Durable Object mail archive;
   the local app reads `~/.lifeops/data/cmail_runtime.db`, which includes Resend.
   The live cloud archive reports 2,384 stored and acknowledged messages and
   zero pending queue messages. The website's Inbox also reports 2,384 messages.

3. The deployed worker has the Cloudflare `email` receiver and the existing
   signed queue injection endpoint. It has no Resend webhook, Resend polling
   handler, or Resend credential binding. Bind My Soul mail still arrives via
   Resend; its current MX is `9 inbound-smtp.us-east-1.amazonaws.com.`

4. A complete website scan with the mailbox set to `support@bindmysoul.com`
   and view set to **All mail** finished with **No messages in this inbox**.
   The filter was supplied through the supported URL parameter in a temporary
   audit tab. That parameter adds an option to that tab only; it does not repair
   the deployed inbox list. Searching all inboxes for the address returned only
   an August 12 verification reply delivered to `cody@frg.earth`.

5. The local runtime retains **45 inbound Resend messages**, **44 outbound
   Resend messages**, and **2 drafts** with `mail_identity='bind_my_soul'`.
   Resend's receiving API currently returned three messages, `has_more=false`,
   dated October 4, October 6, and October 13. All three provider IDs matched
   stored local records, IDs 77711, 77775, and 77998 respectively. The current
   provider inventory is not evidence of its full historical retention.

## Deployment and source evidence

- Host mapping: `cmail.frg.earth` -> `life-ops-email-ingest`, production.
- Deployment: `7c3cadbd-4577-482e-94d0-eb390f780092`.
- Worker version: `23d95fd6-1325-4385-a276-e3576127f70c`, 100% traffic.
- Deployed at: `2026-10-13T14:46:10.827366Z`.
- Authenticated download of the deployed module is preserved privately at
  `data/qa/cmail-bindmysoul-2026-10-17/deployed-index.mjs` (420,565 bytes).
- Module SHA-256:
  `65da80cbc79ee9de38140ac160f4047917c4f1653699288038d3344d8f425cc1`.
- Source references in that module: `src/legacy-worker.mjs`, `src/archive.mjs`,
  `src/index.mjs`, and
  `/Users/codymitchell/Documents/code/life-ops/cloud/cmail/web/app.js`.
- `cloud/cmail` is absent from this checkout. The Documents code path resolves
  to this same external-drive checkout. GitHub's current main is also older
  than the cloud rebuild. The original rebuild source location remains unknown;
  a deployed-module snapshot is available for recovery.
- Snapshot lines 6078-6149 implement archive filtering; lines 6246-6256 select
  the legacy Cloudflare receiver; lines 6313-6316 derive inbox options from
  `CMAIL_MAILBOXES`; subsequent routes read the Durable Object archive.

Credentials were resolved privately from existing local storage. No keys,
authentication cookies, message bodies, or customer addresses are included in
this report. No provider messages were marked read, archived, sent, or deleted.

## Concrete repair scope

1. Recover the rebuild's source checkout, or reconstruct a reviewed source
   package from the exact deployed module before changing application code.
2. Add `support@bindmysoul.com` to the configured inbox list.
3. Connect Resend-received mail to the existing encrypted cloud archive with
   authenticated ingestion and stable deduplication. Preserve the current mail
   routing and the legacy local sync. New messages must remain visible without
   depending on a manual historical import.
4. Import the 45 existing inbound support messages after inventorying available
   raw MIME and attachments. Preserve original message IDs, original dates,
   envelope recipients, and attachments. Record any reconstruction limits.
   Sent history and drafts need an explicit preservation decision before import.
5. Verify the normal dropdown, support-only view, All inboxes, attachments,
   pagination, duplicate prevention, and subsequent real inbound delivery.
   A dropdown-only change does not complete this repair.

## Verification and worktree closeout

**PASS:** live dropdown omission; production setting and deployment mapping;
deployed source inspection; complete all-mail support filter; current Resend
inventory/local-record reconciliation; current DNS; older service health.

No application code changed; no test suite run was needed for this read-only
diagnosis. This report and the private deployed-source snapshot are the only
new artifacts in this task. The pre-existing dirty worktree remains preserved.
The next action for this evidence bucket is source recovery and the bounded
repair above. Temporary browser filters were restored in the user's original
tab; the separate audit tab is disposable.

Resume decision: **no-change / internal evidence only**. This is a private
mailbox diagnosis with no implemented repair or new public accomplishment.
No canonical resume or public resume publication is warranted.
