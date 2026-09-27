# CMAIL Labels

Labels gather correspondence into its own view. Choose a label from the dropdown
beside the inbox picker to see only that label's correspondence; **All labels**
returns to the full mailbox. A label combines with the inbox picker, the
all/new/opened filter and search, and it applies in Correspondence and Spam.

## Making labels

- **Manage labels…** in the dropdown creates, renames and deletes labels and
  edits their rules. Deleting asks for a second click.
- **Save as label** appears beside all/new/opened while you search. It opens a
  new label with the search as its first rule. The list search covers recently
  loaded mail; a label covers all correspondence.
- **labels** on an open message adds or removes that conversation, creates a
  label on the spot, or adds every message with the sender as a rule.

## Rules

| Rule | Matches |
| --- | --- |
| `grant` | The whole word in the subject, sender name or full body, ignoring case |
| `grant*` | Words that begin with `grant` |
| `"machine learning"` | The phrase, across any spacing or line break |
| `person@example.com` | That exact address in From, To, Cc, Bcc, Reply-To or the delivered-to envelope recipient |
| `@example.com` | Correspondents at the domain or its subdomains; CMAIL's own inbox addresses are skipped |

One matching message labels its whole conversation. Conversations are linked
through Message-ID, In-Reply-To and References headers, so replies carry the
label, including Cody's replies from another address. Subject lines are used
only for suggestions.

## Choices by hand and suggestions

Checking a label on a conversation adds it. Unchecking removes it and keeps it
out even when a rule matches. The latest choice in a conversation wins, and
later replies follow it because choices are stored against the Message-ID.

Suggestions list conversations whose subject mentions the label's name that no
rule matched. **Add** or **Dismiss** each one; a dismissal is remembered.

Hovering a label chip shows why it applies: the matching address or keyword,
`Same conversation (reply headers)`, or `Added by you`.

## Scope

Labels organize the mailbox view. Mail status, inbox identity, follow-up state,
Day Sheet lists, reminders and Spam rules keep their own behavior, and Spam
still wins: a Spam sender's labeled mail appears under Spam. A label is an
organizing signal. Sender authenticity, beta admission, authorization and
entitlement come from their own records.

## ClawDad

The ClawDad label has one rule, `clawdad@frg.earth`, per the ClawDad handoff
(`clawdad/marketing-site/docs/cmail-label-handoff.md`). It covers site
notifications from the alias, tester replies addressed or copied to it or
delivered to it, and every header-linked message after them. Mail to the alias
stays in the FRG inbox. Replies to the September 27 welcome, which used
Reply-To `cody@frg.earth`, show up under Suggestions for review.

## Storage and API

`life_ops.mail_labels` owns rule parsing, matching and conversation linking.
`mail_ui` applies the selected label on the server before the 80-message
overview cap, so a label view reaches all correspondence. The tables
`mail_labels`, `mail_label_rules` and `mail_label_conversations` live in the
CMAIL runtime database; runtime sealing and external backups snapshot the whole
database, so labels are sealed and backed up with it.
`communications.envelope_to` records the delivered-to recipient for mail
ingested from September 27, 2026.

- `GET /api/overview?source=correspondence&label=<id or slug>`: label view. The
  payload includes `labels` with counts for the current inbox and `label`.
- `GET /api/labels?identity=<inbox>&mailbox=<correspondence|hidden>`: labels,
  rules and counts.
- `GET /api/labels/suggestions?label=<id or slug>`: suggested conversations.
- `POST /api/labels` with `{"name":"ClawDad","rules":["clawdad@frg.earth"]}`.
- `POST /api/labels/update` with `{"label":1,"name":"New name"}`.
- `POST /api/labels/delete` with `{"label":1}`.
- `POST /api/labels/rules` with `{"label":1,"rule":"@example.com"}`.
- `POST /api/labels/rules/remove` with `{"label":1,"rule_id":2}`.
- `POST /api/labels/conversation` with
  `{"label":1,"communication_id":123,"action":"add"}` or `"remove"`.

Mutations use the same local/same-origin or verified private-tailnet request
boundary as Spam rules and invalidate the mailbox snapshot and caches. Every
label edit changes `mailbox_version.ui_state_digest`, so other open CMAIL tabs
refresh on their next poll.

## Verification

`tests.test_mail_labels` covers rule parsing; whole-word, prefix and phrase
keywords; every address field and domain rules; the ClawDad handoff acceptance
checks (alias in To and Cc, alias only in the envelope, Cody's reply, the
tester's next reply to Cody, an unrelated same-subject conversation, FRG and
All visibility, Spam precedence); manual choices; suggestions and dismissal;
label views beyond the message cap; refresh digests; persistence; read-only
snapshots from before the migration; and the HTTP lifecycle with origin
rejection. The deployment record is in `docs/qa/cmail-labels-2026-09-27/`.
