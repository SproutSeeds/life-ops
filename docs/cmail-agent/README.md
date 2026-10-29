# C-mail agent guide

C-mail at **https://cmail.frg.earth** is the active mailbox for all configured
C-mail identities. The installed `cmail` and `lifeops cmail` commands use the same
cloud messages, threads, contacts, Sent and Drafts as the website. This applies
across projects and working directories after the October 20, 2026 cutover.

Read-only review is the default. Saving, discarding, archiving, marking read and
sending require the authority in Cody's request. Saving never sends. Sending
requires authorization for the exact message; a broad software change or a
request to edit a draft does not authorize delivery.

Website and CLI capabilities must ship together. Follow the mandatory
[feature parity rule and release checks](../cmail-feature-parity.md) for every
C-mail implementation change.

## Read and find mail

```sh
cmail status
cmail identities
cmail messages --mailbox cody@frg.earth --view inbox --limit 50
cmail search --mailbox cody@frg.earth --view all --query 'subject or sender'
cmail messages --view sent
cmail messages --view spam
cmail spam-senders
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

## Draft attachments

Attach files directly from the command line when composing, replying or editing:

```sh
cmail draft-save --save-id UUID --to recipient@example.com --subject 'Files' \
  --body-file /private/path/body.txt --attach /private/path/image.png \
  --attach /private/path/document.pdf
cmail draft-save --id draft_UUID --revision N --save-id NEW_UUID \
  --attach /private/path/archive.zip
cmail draft draft_UUID
cmail draft draft_UUID --attachment 0 --output /private/path/download.png
cmail draft-save --id draft_UUID --revision N --save-id NEW_UUID \
  --remove-attachment ATTACHMENT_UUID
```

Use the returned cloud IDs and latest revision. `--attach` is repeatable and
preserves existing attachments, recipients, body and thread headers. The same
flags work with `--reply-to-message`. Files appear immediately in the website's
saved draft. Reopen it and verify each filename, byte count and SHA-256 as well
as the message before requesting send authorization.

Downloads accept a zero-based index or attachment UUID, verify bytes against
the saved hash, and refuse to overwrite an existing output file. Repeat
`--remove-attachment UUID` for multiple removals; `--clear-attachments` removes
all saved attachments before adding any new files. `draft-copy ID --revision N
--save-id UUID` preserves a separate copy with the same attachments.

Both interfaces accept provider-supported types, including images, PDF, Office
documents, ZIP, audio, video and custom formats, up to **20 MiB combined and 50
files**. `cmail status` reports the shared policy, including blocked extensions.
Empty files and blocked types receive explicit errors. File bytes remain in the
same encrypted cloud store; `draft-send` sends the exact saved references.

Keep the same save UUID, file contents, paths and flag order on a retry. Upload
IDs are stable per save; changed bytes under that ID are rejected. A failed
save leaves the previous draft intact. Reopen after a conflict and use its
current revision with a new save UUID for a deliberate new edit. Saving and
uploading never send mail. Every command also works as `lifeops cmail ...`.

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
`cmail spam ID [ID ...]` moves authorized messages to Spam and filters future
inbound mail from those exact From addresses across inboxes. `cmail not-spam ID
[ID ...]` restores selected messages to Inbox and removes their sender rules.
Other caught messages remain in Spam until restored. Search with `--view spam`
and list active rules with `cmail spam-senders`. Reopen changed IDs to verify
`spam` and `archived` flags. Owned addresses and outbound mail cannot be blocked.
See [Spam behavior](../cmail-spam.md); local historical spam commands stay retired.
Cloud sending supports plain text, To/CC/BCC, reply headers and configured logo signatures.
For branded identities, the server also saves an HTML alternative when the configured
signature remains at the end of the text. Editing or omitting that signature preserves
plain-text sending. The rendered alternative is frozen with the saved draft revision.
Website and CLI Compose/reply drafts support the same attachments, including
download, removal, copying and persistence across clients. When editing
through JSON, preserve the attachment references; omitting the field retains
existing attachments, while an explicit empty array removes them. HTML-only
sending, scheduled/batch delivery and legacy local label administration are not
exposed by either cloud interface. Report missing capability
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
