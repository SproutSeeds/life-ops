# C-mail conversation and inbox performance repair

Event: `cmail-cloud-performance-2026-10-18-r1`.

Production: <https://cmail.frg.earth>, Worker `life-ops-email-ingest`, version
`14404822-95f1-4fc6-97ca-db6e3d8d1e95`. Cody authorized a complete performance
audit, repair and production deployment after observing a conversation scan of
the entire archive. The indexed build is deployed and enabled.

## Findings and implementation

The old inbox decrypted unrelated archived MIME while finding matching mailbox
rows. Opening a deep link waited for that inbox scan. Conversation discovery
then paginated the complete archive's header index before rendering history.
Even a two-message conversation scanned 2,475 stored messages.

The repair adds encrypted summary/text projections, sorted mailbox/view/date
indexes, exact counters and hashed thread-header relationships. New arrivals
commit these alongside the original encrypted message. Existing messages were
indexed in bounded resumable batches before activating the new read path.
All 2,475 originals are indexed; the original namespace, encryption, attachments,
IDs, flags, delivery queue and receiving schedule are preserved.

The browser opens a selected message alongside the recent-first inbox request.
A single conversation request follows only related header edges. It displays
newest-first metadata and loads older text only on expansion; Copy thread
explicitly fetches related bodies. Normal inbox and conversation reading never
scan unrelated MIME. Date filters and cursor pagination stay within sorted
index ranges. Explicit full-text search can inspect the selected mailbox range
in 100-message batches until a results page fills or the range ends.

Caches retain immutable projections and have entry/byte limits; flags remain
fresh. Concurrent fills of the same message are covered by a regression test.
Backfill survives interruption, new arrivals, restarts and older-code rollback
without replacing original mail or double-counting indexed records.

## Measured user experience

Authenticated Chrome, the same selected production deep link and mailbox,
outer wall clock from navigation/reload until rendered DOM readiness:

| Visible state | Before | First activated build, three runs | Final build, three full reloads |
| --- | ---: | ---: | ---: |
| Inbox | 11.388 s | 0.252–0.522 s | 0.197–0.264 s |
| Selected message | 11.472 s | 0.252–0.522 s | 0.197–0.234 s |
| Complete conversation metadata | 22.142 s | 0.252–0.804 s | 0.283–0.371 s |

These are observed browser runs, not a latency guarantee or a load test. The
production conversation has two messages. The baseline used the original read
path before index activation. An intermediate fragment-only navigation was
discarded; `browser-final.json` explicitly records full reload measurements.
An explicit support-mailbox body search returned its one matching result in
0.305 seconds in the live browser.

Cold application-level storage probes on the same 2,475-message archive:

| Operation | get calls | list calls | Keys requested/listed | Original MIME loads | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Legacy inbox, first scan chunk only | 181 | 1 | 222 | 39 | 0 matching rows; more scan required |
| Indexed support inbox, complete | 3 | 1 | 138 | 0 | All 45 inbox rows |
| Legacy thread index, first chunk only | 45 | 1 | 86 | 40 | 40 archive rows; more scan required |
| Indexed conversation, complete | 4 | 2 | 9 | 0 | Both related messages |
| Legacy selected message | 4 | 0 | 4 | 1 | Selected message |
| Indexed selected message | 2 | 0 | 3 | 0 | Selected message |

Legacy chunk counts are deliberately not presented as the old full-operation
cost. The old 40-row thread scan needed 62 API pages for this archive. Calls
and keys are logical storage API measurements, not physical or billed I/O.
Probes instantiate fresh application caches; Durable Object storage may already
be warm. Runtime clocks reported zero for these probes, so only browser wall
clock measurements support latency claims.

## Verification

- All 24 Node tests pass, including six performance/index tests. They cover
  mixed-mailbox pagination, date/cursor boundaries, body search, exact counters,
  read/archive mutations, missing-parent threading, Message-ID deduplication,
  backfill preservation and overlapping cold cache requests. Existing receive,
  reply, idempotency, encryption, attachment and authorization tests pass.
- Actual workerd with SQLite Durable Objects passes the indexed reply runtime
  suite: native fake-provider sending, concurrent double-submit, durable
  receipt replay, encrypted sent copy, thread headers and HTML integration.
  The receiving runtime also passed its upgrade and 7 MB MIME checks.
- An isolated browser fixture verifies lazy body expansion, Copy thread and
  explicit fake-provider reply followed by the updated newest-first history.
  Its network is blocked outside synthetic fixtures. No real email was sent
  by this performance repair.
- Live authenticated browser checks confirm 45 support inbox messages, 44
  archived support messages, newest-first all-inbox pagination, body search,
  a direct two-message conversation and the retained Reply control. The user's
  own recently sent reply remains visible in the conversation and Archive.
- Production download hashes match all eight deployed local source modules.
  Index readiness is 2,475/2,475; the pending delivery queue is zero. Website
  and conversation API retain Access redirects; unsigned administration is
  rejected. All existing namespace/bindings and the two-minute receiving cron
  are preserved. Exact scheduled-run times are recorded in the final private
  production verification receipt.
- Module syntax checks and `git diff --check` pass. Production and synthetic
  browser screenshots are private; no customer message content is committed.

Private evidence: `data/qa/cmail-performance-2026-10-18/`, including deployment
receipts, original source/settings snapshots, backfill state, cold probes,
browser timings, final source/authentication/receiving checks and worktree
classification. The package README documents deployment, backfill and rollback.

## Scope, preservation and evidence decision

This change owns only the cloud-mail package and this verification record.
Unrelated Life Ops dirty paths retain their existing buckets and next actions,
recorded in private `worktree-closeout.json`. Production publication is direct
through Cloudflare; no unrelated local commits are pushed to the public remote.

Resume decision: **no-change / bounded maintenance of an existing private
operating capability**. This resolves the rebuilt reader's performance defect
in CMAIL. Existing Founder Operations evidence already represents the encrypted
mail/Resend/threading system. The incident's customer correspondence stays
private; these narrowly observed timings do not establish a new general-scale
product milestone. No separate canonical resume or public resume change is
warranted. Unrelated pending resume work retains its owners and receipts.
