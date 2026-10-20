# CMAIL Spam

CMAIL routing update, October 20, 2026: the active mailbox and Drafts are at
https://cmail.frg.earth and the installed `cmail` command. Follow
[the current CMAIL agent guide](cmail-agent/README.md). Local mail UI/API, local draft
and direct-provider sending examples in older sections are historical. Local
communication records remain Life Ops follow-up projections.

Open a message and choose **Move sender to Spam**, review the address, then choose
**Move sender to Spam** in the dialog. You can also open **Spam → Manage senders**
and enter an address directly. Rules match an exact email address, ignoring case,
across CMAIL inbox identities. They cover existing and future correspondence,
including the saved conversation history with that address.

**Restore sender**, **Restore** in the Spam list, or **Restore** in Manage senders
removes the rule and returns the correspondence to the main view. Spam remains
searchable and readable. Back and Escape close the dialog and restore focus;
Escape from an unselected Spam folder returns to Correspondence.

Rules are local CMAIL preferences. They do not unsubscribe, reject delivery,
delete messages, alter attachments, mark obligations complete, or modify Gmail
provider labels. Existing deliberate job, bill and manual obligations retain their
canonical state. Spam is excluded from CMAIL mail-review candidates, mail-derived
communication-loop reminders, and Day Sheet mail lists. A conversation involving
an unblocked person's evidence remains visible. Removing a rule restores the
original mail-follow-up state.

Automated billing candidate selection and job-review reconciliation also honor
Spam. Spam-only evidence cannot create, reopen, rewrite or resolve a job item;
existing explicit job and bill obligations remain intact.

## Storage and compatibility

`life_ops.mail_spam` owns sender-rule normalization and matching. It reuses
`sync_state["mail_ui:hidden_contacts"]`, which is already included in CMAIL runtime
sealing and encrypted backups. New keys are `sender:<lowercase-email>`. Existing
Hidden keys retain their original inbox scope until explicitly replaced or
restored; the visible Hidden tab is now named Spam. No message schema or provider
migration is needed. The `hidden` mailbox API and stored view remain compatible;
`mailbox=spam` and `view=spam` are accepted aliases.

- `GET /api/spam/senders`: list sender rules.
- `POST /api/spam/senders` with `{"sender":"sender@example.com"}`: add a rule.
- `POST /api/spam/senders/remove` with `{"id":"sender:sender@example.com"}`:
  remove a rule.

Mutations require the existing local/same-origin or verified private-tailnet
request boundary. Rule edits serialize their read/modify/write transaction.
Restoration preserves bodies, statuses, dates, unread state and attachments.
Rule edits invalidate the mailbox snapshots and caches. Other devices pick up
changes during their regular mailbox refresh.

## Verification

`tests.test_mail_spam` covers current/future messages, provider replay, exact
matching, Resend inbound and inbox identities, legacy rules, concurrent edits,
restart persistence, HTTP operations and origin rejection, and reversible
suppression in mail review, the Day Sheet and Home. The September 23 deployment
record and synthetic browser fixture are in `docs/qa/cmail-spam-2026-09-23/`.
