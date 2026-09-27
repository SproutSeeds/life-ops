# CMAIL Labels production deployment — September 27, 2026

Cody asked for keyword labels that gather correspondence into its own space,
chosen from a dropdown, with keywords or email addresses added at any time. The
ClawDad handoff (`clawdad/marketing-site/docs/cmail-label-handoff.md`) supplied
the first label's matching contract. Labels are live at
https://cmail.tail649edd.ts.net with **ClawDad** as the first label. Usage and
source ownership are documented in [CMAIL Labels](../../cmail-labels.md).

## Behavior

- A label dropdown sits beside the inbox picker. Choosing a label filters the
  list on the server across all correspondence, beyond the 80-message overview
  cap that limits the search box.
- Rules are whole-word keywords (with `*` prefixes and phrases), exact addresses
  in any address field including the delivered-to envelope, and correspondent
  domains. A match labels the whole conversation through Message-ID,
  In-Reply-To and References headers.
- Manage labels creates, renames and deletes labels, edits rules and reviews
  suggestions. **labels** on an open message adds or removes that conversation;
  removal holds even when a rule matches. **Save as label** turns a search into
  a label.
- Mail status, inbox identity, reminders, Day Sheet lists and Spam behavior are
  unchanged. Spam still takes precedence.
- Ingest now records `communications.envelope_to` for new mail, which covers an
  alias that appears only in the envelope (for example Bcc).
- The overview reuses its in-scope message list for `mailbox_version`, removing
  a second full correspondence build per overview request.
- Phone list rows keep their full height; the list scrolls instead of
  overlapping row text.

## ClawDad

The label has one rule, `clawdad@frg.earth`. At deployment it held 17 site
notifications from one contact, each labeled `From clawdad@frg.earth`, visible
in both All inboxes and Cody / FRG. One conversation that mentions ClawDad in
its subject without using the alias waits in Suggestions for review. No mail had
yet arrived addressed to the alias, so the first real reply to it is the next
acceptance check.

## Deployment and preservation

The production runtime was sealed and encrypted backup
`life-ops-db-20260927T080328Z` was created before installation, using the
installed CMAIL environment. Checkout-based scheduled jobs had already created
the empty label tables and the `envelope_to` column in the runtime database
after the checkout schema change; the additions are additive and the previous
service ignored them. `PREDEPLOY_STATE.json` records that state.

`PRODUCTION.patch` is the audited patch against the previously installed
modules; `DEPLOYMENT.json` records before and after hashes and the rollback
path. Four modules were updated in the existing ignored backend bundle.
`store.py` and `mail_ingest.py` received only the label changes, so unrelated
checkout changes stay out of the CMAIL installation. The environment was
rebuilt with:

```sh
/Users/codymitchell/.lifeops/venvs/cmail/bin/python -m pip install \
  --no-index --no-deps --no-build-isolation --force-reinstall \
  ./packages/lifeops-cli/backend
```

Every installed module matched the bundle. Only the CMAIL launchd job was
restarted (`launchctl kickstart -k gui/501/com.frg.lifeops.cmail`); Life UI and
the schedulers run checkout source directly. The ClawDad label was created
through the local API and the runtime was sealed again.

## Verification

- `tests.test_mail_labels`: 14 tests passed, and three deliberate regressions
  (header linking, whole-word matching, manual choices) were each caught.
- The checkout suite ran 655 tests with 654 passing. The one failure is a
  scheduled-run count that another in-progress change raised; it fails
  identically with the label changes reverted.
- Against the patched bundle, the label, CMAIL UI, runtime, store and ingest
  suites passed except one ingest test that fails identically on the previous
  live code (its fix exists only in the checkout). The Spam suite imports
  `attention.py`, which the CMAIL bundle does not include.
- JavaScript syntax passed `node --check`.
- Headless browser QA ran against a private copy of the runtime database with
  provider sync, sends and Terminal launch disabled: dropdown, label view,
  detail chips and reasons, conversation dialog and Escape, Manage labels,
  suggestions, keyword label creation, invalid-rule feedback and Save as label.
  At 390 pixels there was no horizontal overflow and no clipped row. This was
  browser emulation, not a physical-phone test.
- Live checks: local and private-route health passed; the default overview kept
  380 messages across 337 contacts; the page served the label controls; the
  service log showed no tracebacks after the restart. `VERIFICATION.json` holds
  sanitized evidence.

## Observations

- The previous service log holds 3,306 intermittent `unable to open database
  file` errors from its read path, predating this deployment.
- Live ingest still lacks the checkout's malformed Message-ID fix; a future
  CMAIL deployment can include it.
- The checkout remains intentionally dirty and classified by ORP hygiene with
  zero unclassified paths. The pre-existing `build/` scratch remains for its
  owner. No staging, commits or pushes were made.
