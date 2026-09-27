# CMAIL Spam production deployment — September 23, 2026

Cody requested a sender-based Spam/out-of-sight folder, implemented completely
and deployed to production. It is live at https://cmail.tail649edd.ts.net/?view=spam.
Usage and source ownership are documented in [CMAIL Spam](../../cmail-spam.md).

## Behavior

- The existing Hidden surface is now Spam, with an explicit message action,
  Manage senders dialog, exact-address rules covering all CMAIL identities, and
  restoration in both the folder and rule list.
- Existing and future correspondence is filtered dynamically. Provider replay
  and process/page reload do not remove the rule. Original bodies, attachments,
  mail statuses and work dispositions remain intact.
- Spam stays out of mail review, mail-derived communication-loop reminders and
  Day Sheet mail lists. Billing candidate selection and job reconciliation also
  honor Spam. Explicit existing jobs, bills and manual obligations retain their
  canonical state; moving a sender to Spam is not work completion.
- Legacy Hidden rules retain their previous inbox scope until deliberately
  replaced or restored. Gmail provider labels are unaffected.
- Rule mutations use serialized database transactions and the existing local or
  verified private-tailnet request boundary. Back/Escape close the dialog and
  restore focus. Controls remain disabled while an update is pending.

## Deployment and preservation

The production runtime was sealed and encrypted backup
`life-ops-db-20260923T233159Z` was created before installation. `DEPLOYMENT.json`
records original and deployed hashes. `PRODUCTION.patch` is the audited patch
against the previously installed modules. Seven modules were updated in the
existing ignored backend bundle. Billing/job modules received only the Spam
changes so unrelated checkout changes were not included in the CMAIL installation.

The existing virtual environment was rebuilt with:

```sh
/Users/codymitchell/.lifeops/venvs/cmail/bin/python -m pip install \
  --no-index --no-deps --no-build-isolation --force-reinstall \
  ./packages/lifeops-cli/backend
```

The existing CMAIL and Life UI launchd services were restarted. Life UI, attention
and the scheduled review commands use the checkout directly. All seven installed
module hashes matched the manifest; an installed-code smoke test verified Spam
routing, restore, and job/bill guards. No provider labels, credentials, schemas,
new services or automated outbound messaging were introduced.

## Verification

- 188 distinct Python tests passed across Spam, CMAIL, communication review,
  calendar, attention, store synchronization, job follow-up and critical bills.
- 20 CLI tests passed. JavaScript syntax and `git diff --check` passed.
- Synthetic browser QA verified moving an existing message, arrival of a later
  message from that sender, page reload, opening preserved content, restoring both
  messages, Back, Escape and focus restoration. The dialog was inspected at
  desktop size and 390-pixel phone width. This was browser emulation, not a
  physical-phone test. The fixture server was stopped afterward.
- Through the private production URL, a reserved `example.invalid` rule was
  created and removed. The original sender-rule state digest was restored, with
  zero real senders changed and no real messages reclassified.
- Final local/private health checks passed. The production Inbox and Spam reads
  succeeded in approximately half a second each. The private-route doctor passed.
- The final live browser showed the Spam tab, Manage senders, the empty-folder
  instruction and the updated sync timestamp. Existing tabs need a reload to
  receive the new client code.

`VERIFICATION.json` contains sanitized live evidence. `WORKSPACE_BUCKETS.json`
records the exact repair scope and all preserved unrelated work. The checkout
remains intentionally dirty and classified, with zero unclassified dirty paths.
The pre-existing `build/` scratch remains for its owner's packaging checkpoint.
No broad staging, source-repository push or unrelated cleanup was performed;
production delivery was to the managed private CMAIL runtime.
