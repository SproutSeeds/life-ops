# C-mail incoming-message readability

Evidence revision: `cmail-readable-mail-2026-10-20-v1`.
Production: https://cmail.frg.earth.
Worker version: `38ca7f25-8f04-4432-bcea-ec15017ad313`.
Previous version: `10d2914d-2260-4aca-8ecf-b280fa3430d7`.

## Result

Incoming messages now use compact paragraphs with controlled spacing. Repeated
blank lines, invisible padding and leading layout indentation are removed from
the reading view. Lists, hard line breaks, quoted replies, fenced code, recognized
unfenced code and aligned text tables retain their useful structure. Long URLs
have compact labels with the full destination retained in both href and title.

The Original text toggle restores the exact text and spacing of the selected
message and already-loaded conversation bodies. It preserves expanded thread
items and resets to readable mode when opening another message. Copy message,
Copy thread, raw downloads and stored mail remain unchanged. Empty attachment
containers no longer introduce an extra gap below the sender header.

Formatting uses text nodes and locally created paragraphs, quotes, links and
preformatted blocks. Email HTML is never executed, and no remote image is loaded.
This remains a plain-text reader; content omitted by a sender's plain-text
alternative is not reconstructed from HTML. Exact original text remains
available for layouts outside the formatter's recognition rules.

## Verification

- All 68 package tests passed, including 10 formatter tests for padding, Unicode,
  lists, quotes, code, aligned tables, links, exact original mode, unsafe markup,
  bounded DOM work and embedded application syntax. Package syntax checks and
  `git diff --check` passed.
- A native workerd fixture with SQLite Durable Objects verified the original API
  text, indexed conversation metadata and served formatting assets. Its six
  messages included three synthetic cases and three private local samples.
  External networking was disabled and no provider calls occurred.
- Five already-read cloud samples covered an order confirmation, newsletter,
  sales message, tracking-link-heavy notification and research reply. Every
  sample retained its words and numbers in the original order. Local parser
  medians were below 0.06 ms per sample, excluding browser paint and networking.
- The order confirmation contained 236 source lines, including 199 blank lines
  and a run of 38 blank lines. Production rendered it as 29 compact blocks.
  Body height was approximately 1,181 px versus 5,284 px in Original text mode,
  about 78% less vertical space using the same line-height setting.
- Browser inspection covered the real sample, exact-original comparison,
  technical content, expanded conversation bodies and literal malicious HTML.
  No active script/image/iframe/style nodes or tracker/frame requests appeared.
- Request counters were identical before and after toggling the selected body
  twice, and again after toggling with an expanded conversation. Formatting
  adds zero requests, database reads or provider calls. It reuses the existing
  app script and stylesheet without another asset request. DOM creation is
  bounded to 1,000 blocks plus a text tail and 400 links per body; remaining
  content stays visible as text.
- Desktop and 390-by-844 browser layouts were visually inspected. The narrow
  layout kept all reader controls visible and wrapped the content. Enter toggled
  Original text; the visible Back button and Escape returned to the Inbox.
  This is browser-layout evidence, not physical-device acceptance.
- Production verification matched all 17 uploaded source modules, confirmed
  existing bindings, namespace and schedule, and checked readiness for Inbox,
  Sent, contacts, Drafts and agent access. Unauthenticated routes remained
  protected, and the receiving schedule had a recent healthy completion.
- A live already-read message was inspected in the deployed reader. Original
  text matched the canonical cloud CLI body exactly. CLI reads before and after
  confirmed unchanged text hash, read/archive flags, headers and attachments.
  No message was sent, drafted, archived or otherwise acted on for verification.

Private screenshots, captured messages, request counters and deployment receipts
are under `data/qa/cmail-readable-mail-2026-10-20/`. No correspondence or credentials
are included in this committed note. Test-only authentication and fixture routes
are outside `src/` and are excluded from deployment.

## Scope and completion

This change covers the formatter, browser integration, existing stylesheet
response, package syntax check, isolated tests/fixture, package README and this
verification record. It requires no index rebuild, migration, provider change
or stored-message rewrite. Existing mailbox identities and signatures remain
preserved by the guarded production deployment.

The original 163 unrelated dirty paths remain in their existing hygiene
categories. The existing `build/` scratch bucket belongs to that prior work and
is preserved for its owner's review. The exact path inventory is retained in the
private hygiene receipts. These changes are committed separately; unrelated
source, documentation and generated artifacts are not staged. No unclassified
paths were introduced. Existing unrelated commits ahead of origin are preserved;
production was published directly through the established Worker deployment.

Resume intake disposition: **no change**. This is a routine readability refinement
of the existing CMAIL accomplishment, without a distinct new public milestone.
Private correspondence is excluded from resume evidence. No canonical or public
resume files were changed.
