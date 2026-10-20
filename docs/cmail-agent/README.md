# C-mail agent guide

C-mail at **https://cmail.frg.earth** is the active mailbox for all configured
C-mail identities. The installed `cmail` and `lifeops cmail` commands use the same
cloud messages, threads, contacts, Sent and Drafts as the website. This applies
across projects and working directories after the October 20, 2026 cutover.

Read-only review is the default. Saving, discarding, archiving, marking read and
sending require the authority in Cody's request. Saving never sends. Sending
requires authorization for the exact message; a broad software change or a
request to edit a draft does not authorize delivery.

## Read and find mail

```sh
cmail status
cmail identities
cmail messages --mailbox cody@frg.earth --view inbox --limit 50
cmail search --mailbox cody@frg.earth --view all --query 'subject or sender'
cmail messages --view sent
cmail message mail_0000000000000001
cmail thread mail_0000000000000001
cmail contacts
cmail drafts --mailbox cody@frg.earth
cmail draft draft_UUID
```

Use IDs returned by the commands; the IDs above illustrate their shape. Lists
return bounded pages and cursors. Follow `next_cursor` when the result is
incomplete. Search evaluates bounded candidate pages, so a single empty page
with a cursor is not proof that no match exists. Message and thread reads do not
mark mail read. `cmail message ID --raw --output /private/path/email.eml` and
`--attachment INDEX --output /private/path/file` download preserved content;
existing output files are not overwritten.

## Save a shared draft

Generate one UUID for the logical save and keep it for retries. For example,
run `uuidgen` once, then use its lowercase value as `--save-id`. The ID of a new
draft is `draft_` followed by that UUID.

```sh
cmail draft-save --save-id UUID --sender cody@frg.earth \
  --to recipient@example.com --subject 'Subject' --body-file /private/path/body.txt
cmail draft draft_UUID
```

The result must be `state: editing`; reopen it and verify the sender, recipient,
subject and body. It appears immediately in the website's Drafts box. The
command appends the configured signature once; use `--no-signature` when the
provided body must be preserved exactly. CC and BCC are supported with `--cc`
and `--bcc`. A draft may be incomplete and is validated again before sending.

To edit, first read the current draft and use its returned revision with a new
stable save UUID. `--id draft_UUID --revision N` prevents overwriting a newer
browser/agent edit. `--json-file /private/path/save.json` accepts the exact
`{id, revision, save_id, draft}` envelope for faithful updates. Keep every
existing field, including recipients and thread headers. On a save timeout,
retry the identical envelope and save ID. Never generate another ID just to
work around an uncertain response.

For a reply, inspect `cmail reply-context MAIL_ID`, then save using
`--reply-to-message MAIL_ID`; the server derives the correct identity, recipient
and headers. If a draft already exists for the thread, edit that draft using its
revision. Original reply routing remains locked. Migrated replies retain their
original headers and any CC recipients.

## Send only when authorized

Read the exact saved revision, verify the intended recipient and message, then:

```sh
cmail draft-send draft_UUID --revision N --confirm-send
```

Only `state: sent` confirms completion. Uncertain sends retain the same cloud
outbox receipt and provider idempotency key. Check the same draft and retry its
confirmation; never switch providers, send a replacement, or regenerate the
message. Confirmed mail leaves Drafts and appears in Sent, including its linked
inbound conversation. Nothing in this guide grants send authority.

`cmail draft-discard ID --revision N` discards an authorized draft.
`cmail read`, `unread`, `archive`, and `unarchive` accept cloud message IDs.
Cloud sending supports plain text, To/CC/BCC, reply headers and configured logo signatures.
For branded identities, the server also saves an HTML alternative when the configured
signature remains at the end of the text. Editing or omitting that signature preserves
plain-text sending. The rendered alternative is frozen with the saved draft revision.
Attachment sending, HTML-only sending, scheduled/batch delivery and legacy local
label administration are not exposed by this client. Report missing capability
and retain the draft; do not fall back to the retired system.

## Identity and correspondence

Default to `cody@frg.earth` for ordinary Cody mail. Select another configured
identity only when the request or existing thread calls for it. Write in Cody
Mitchell's first-person voice and preserve his signature. Do not introduce or
sign as an agent unless Cody explicitly requests that attribution. Editing a
sent message does not authorize a replacement send.

Secondary Gmail remains separate for explicitly requested Gmail work. Ordinary
Cody correspondence and agent drafts belong in C-mail, not Gmail drafts or a
standalone Markdown file.

## Retired local system

The old localhost/Tailscale mailbox redirects to the cloud. Old mail APIs return
410, local draft writes and direct Resend sends are blocked, and the old send
worker is disabled. Local `communications` remains a Life Ops follow-up and
historical projection; it is not the live mailbox or draft authority. New cloud
mail projects incrementally into Life Ops without changing existing task
classification. The existing backup/sealing jobs remain active.

Seven active local drafts were migrated with exact body and routing readback.
Their old rows are now historical reference records with cloud links; the frozen
archive preserves their original draft state and content.
Numeric migrated draft IDs can be read with `cmail draft LEGACY_ID`; this resolves
the verified cloud ID. Other numeric selected-item pointers are historical and
must be reconciled to cloud message IDs before action.

The frozen original database and runtime/guidance copies are under
`~/.lifeops/archive/cmail-local-2026-10-20/`, with a hash manifest. Use that archive
only for explicitly scoped historical recovery. Do not restore it as a second
active mail system. [Old agent documentation](archive/local-2026-10-20/README.md)
is labeled historical. See [DATA_MAP.md](DATA_MAP.md) and [RUNBOOK.md](RUNBOOK.md).
