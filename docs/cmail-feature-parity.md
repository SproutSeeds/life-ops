# C-mail website and command-line parity

Cody's October 29, 2026 rule: every new or changed C-mail mail capability ships
on the website and the command line together. The two interfaces share cloud
data, limits, validation, optimistic revisions, signatures, attachment bytes,
send authorization and durable receipts. A feature is complete only when both
interfaces pass verification. This applies to fixes as well as new features.

Interface mechanics may differ: the website has file selection, dropping and
pasting; the CLI accepts file paths. Both must produce the same saved draft and
message. Browser layout, clipboard widgets and navigation do not require literal
terminal widgets. Operational diagnostics and the read-only Life Ops projection
remain agent administration rather than separate user mail features.

## Capability matrix

| Mail operation | Website | Installed CLI (`cmail` or `lifeops cmail`) |
| --- | --- | --- |
| Identities and signatures | From selector and signature preview | `identities`; `draft-save --sender` |
| Inbox, Sent, Archive, Spam and all mail | Mailbox and folder selectors | `messages --mailbox --view` |
| Search, date range and pagination | Search, Dates and page controls | `search --query --after --before --cursor --limit` |
| Message, original and delivered attachments | Reader, Original and download links | `message`; `--raw`; `--attachment INDEX --output PATH` |
| Conversation and copyable contents | History, Copy message/thread | `thread` for ordered IDs, `message` for each body and headers |
| Contacts | Recipient autocomplete and unique recipients | `contacts --cursor --revision`; `draft-save --to` |
| Compose, reply, CC/BCC and saved drafts | Compose, Reply, Drafts | `draft-save`, `--reply-to-message`, `--cc`, `--bcc`, `drafts`, `draft` |
| Add one or multiple attachments | Attach files, drop, paste | `draft-save --attach PATH` (repeatable) |
| Read draft attachments | Filename download link | `draft ID --attachment INDEX_OR_UUID --output PATH` |
| Remove draft attachments | Remove | `draft-save --remove-attachment UUID`; `--clear-attachments` |
| Preserve a separate draft copy | Save a copy after conflict | `draft-copy ID --revision N --save-id UUID`, or an exact new-ID JSON envelope |
| Save and close or discard | Save draft, Save & close, Discard | `draft-save`; `draft-discard ID --revision N` |
| Send exact saved contents and retry confirmation | Send / Retry confirmation | `draft-send ID --revision N --confirm-send` |
| Read/unread, archive/restore and sender-based Spam | Reader and bulk Actions | `read`, `unread`, `archive`, `unarchive`, `spam`, `not-spam`; `spam-senders` |

## Release gate

1. Update both adapters, this matrix and the agent guide with every changed
   capability. Reuse the shared server operation; avoid separate stores or
   duplicate policy definitions. `cmail status` exposes the live attachment
   limits and blocked extensions used by the website.
2. Add behavior tests that write through one interface and read/edit through
   the other. Cover retries, conflicts and errors, preservation of unrelated
   fields, and the distinction between saving and sending.
3. Run `PYTHONPATH=src .venv/bin/python packages/cmail-cloud/scripts/check-parity.py`
   from the repository root. It runs the cloud syntax/unit tests and actual CLI
   parser tests. `deploy.py --deploy` runs this gate automatically and refuses
   deployment on failure. Keep the deployment's expected-version check.
4. For attachment, transport or send changes, run the native runtime test:
   `node packages/cmail-cloud/scripts/parity-runtime-smoke.mjs MINIFLARE_MODULE --large`.
   It invokes the Python CLI against a signed local Worker, checks the full
   20 MiB boundary across both interfaces, and verifies fake-provider/Sent bytes.
5. Verify deployed source hashes and `--require-attachments` readiness. Exercise
   the installed `cmail` and `lifeops cmail` commands, reopen the same synthetic
   unsent draft on the authenticated website, and verify edits in both
   directions. Discard only the test drafts. Record real sends as zero.

Tests cannot automatically infer the intended semantics of an arbitrary future
feature. Updating the matrix and proving both interfaces is a required release
review responsibility, in addition to the enforced automated checks.
