# C-mail Spam

C-mail cloud at https://cmail.frg.earth is the active Spam system. The website
and installed `cmail` command share the same messages, folder and sender rules.

## Use Spam

Open an incoming message and choose **Mark as spam**, or check several messages
and choose **Actions → Mark as spam**. The selected messages leave Inbox or
Archive and appear in **Spam** in the folder selector. Future incoming mail from
those exact From addresses goes directly to Spam across all C-mail inboxes.
Rules ignore address case. They do not match an entire domain, display name,
Reply-To address, subdomain or plus-address variation. Owned sending addresses
and outgoing messages cannot be marked as spam.

Choose **Spam** to search, read or download caught mail. **Not spam**, in the
reader or bulk Actions, restores selected messages to Inbox and removes their
sender rules. Other messages previously caught remain in Spam until restored.
Older messages from a newly blocked sender remain where they were unless you
select them too. All mail retains every message and labels spam; Sent and thread
history remain intact. Normal Archive preserves Spam, and Move to inbox asks
for Not spam when the selection contains Spam.

There is no automatic deletion or expiry. These controls do not send mail,
unsubscribe, reject delivery, change Gmail, or complete Life Ops obligations.

## Agent commands

Read [the canonical agent guide](cmail-agent/README.md). Classifying specific
mail requires Cody's authorization, just like archiving it.

```sh
cmail spam MAIL_ID [MAIL_ID ...]
cmail messages --view spam
cmail search --view spam --query 'sender or subject'
cmail spam-senders
cmail not-spam MAIL_ID [MAIL_ID ...]
```

List/search commands support bounded pages and cursors. `spam-senders` lists
active rules and their original message IDs; it also supports `--cursor` and
`--limit`. Reopen changed message IDs to verify the returned `spam` and `archived`
flags. Reads alone do not classify, restore or mark messages read.

## Implementation

`packages/cmail-cloud/src/spam.mjs` owns exact-sender rules. Addresses are hashed
in storage keys and encrypted in values using the existing archive key. Spam
flags, sender-rule changes, folder indexes and counts commit atomically. Rules
are checked within the incoming-message transaction, and provider replays retain
the original message's current state. Restarting the Worker preserves the rules.

Spam uses the existing newest-first mailbox index and bounded search. Incoming
mail adds one point lookup for an eligible sender; it does not scan old mail.
Inbox and Spam listing retain the existing number of database requests. All mail
and Sent read one additional boolean per message in the existing batched state
read. The release needs no full archive migration or new provider service.

Original MIME, bodies, attachments, dates and read flags are preserved by Spam
and Not spam. Selected legacy records lacking direction metadata may load their
original envelope once during action validation; ordinary folder reads never
load MIME. The entire selection is validated before changing a rule or message.

## Historical local system

The pre-October-20 local CMAIL Spam implementation and its preferences belong to
the retired local system. Its `/api/spam/senders` endpoints and local database
commands are not active cloud routes. This release does not import or activate
those historical preferences. Use the cloud folder and commands above.

## Verification

See [the release plan](qa/cmail-spam-cloud-2026-10-21/PLAN.md) and
[verification record](qa/cmail-spam-cloud-2026-10-21/VERIFICATION.md). Isolated
fixtures cover current/future classification, exact matching, duplicates,
concurrent actions, encrypted persistence, atomic failures, restoration,
authentication, bounded folder reads and browser navigation.
