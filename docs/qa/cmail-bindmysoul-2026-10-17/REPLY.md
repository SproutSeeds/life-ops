# C-mail website reply repair

Event: `cmail-cloud-reply-repair-2026-10-17-r1`.

The rebuilt website had no Reply control or outbound endpoint. On October 17,
2026 America/Chicago, Cody asked how to reply after authorizing the complete
production repair. The live reader now has Reply, an inline editor with From/To
and the existing editable signature, Send, Cancel and a saved-copy confirmation.

Production version: `9b8e6f97-ed1f-42ce-b4a1-040661a08e53` on
<https://cmail.frg.earth>. The deployment preserves the existing Durable Object
namespace, archive, receiving schedule, encryption and Access configuration.
Both provider accounts' existing sending credentials are bound as secrets; the
mailbox determines the account. The four sending domains were checked against
their respective live Resend account inventories before deployment.

## Verification

- 18 passing tests cover receiving preservation plus reply identities,
  Reply-To, original thread headers, exact body/signature preservation,
  encrypted outbox and sent archive, provider-account routing, interrupted
  requests, receipt replay, expiry, changed-body rejection, archive recovery,
  empty reply rejection, authentication and same-origin enforcement.
- Both actual workerd/SQLite suites passed. Concurrent duplicate reply requests
  caused exactly one fake-provider send; receipt replay caused none. Existing
  receiving, upgrade and 7 MB MIME tests remain green.
- Browser fixture verified a disabled signature-only Send, pointer cursors,
  draft retention across Cancel/Escape/Inbox/browser Back, restored focus,
  exact draft text, one explicit fake send, sent status, and the saved reply
  appearing in the conversation. Only synthetic addresses and credentials were
  used; the fixture blocks every unmocked external network destination.
- Authenticated production browser verified Reply on the user's selected Bind
  My Soul message, correct From/To/signature, disabled empty Send and the visible
  composer. It was left open for Cody. No real message was sent by this repair.
- The authenticated production source download matches all six local module
  SHA-256s. Both new sending bindings are secret bindings, all six identities
  are configured, and unauthenticated mail access still redirects to Access.
  The existing pending inbound queue remains empty. A post-deployment
  scheduled receiving run completed healthy at `2026-10-18T04:50:13.904Z`.

Private receipts and screenshots are in `data/qa/cmail-reply-2026-10-17/`.
Provider acceptance, retry safety and archive creation were tested with a fake
provider; live recipient delivery awaits Cody's explicit Send. Plaintext reply
is the delivered scope. Attachment sending, Reply all, new-message composition,
cross-device drafts and local follow-up reconciliation are separate features.

## Preservation and evidence decision

Owned paths are the reply modules, their integration/deployment checks, tests,
package README/scripts and this record. Other Life Ops dirty buckets remain
with their existing owners; the private `worktree-closeout.json` records exact
paths and next actions. Source publication remains separate from direct
Cloudflare deployment because main includes pre-existing unrelated commits.

Resume decision: **no-change / routine capability restoration**. This restores
the already represented CMAIL outbound support workflow after the website
rebuild. The existing Founder Operations evidence already covers Resend,
outbound queues, threading, encryption and retries. Customer mail and private
signatures are excluded from public evidence. No separate resume accomplishment,
canonical document edit or public resume deployment is warranted. Existing
unrelated resume verification-pending requests retain their original owners and
receipts; this task does not claim to resolve them.
