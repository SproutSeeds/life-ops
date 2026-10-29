# Ritmo owner recovery mail — October 29, 2026

Claim: **Verified** production connection and access boundaries; recipient delivery and owner password choice are pending the user's first request.

Ritmo's requested Forgot password feature uses the existing C-mail cloud delivery and encrypted draft/outbox/Sent receipts. `POST /api/mail/ritmo-recovery` accepts only a Supabase recovery-token hash, signed with a separate timestamped HMAC capability. Sender is fixed to `hello@ritmo.coffee`, recipient to `cody@ritmo.coffee`, subject to `Reset your Ritmo password`, and destination to `https://ritmo.coffee/admin/reset`. It cannot read mail, change recipients, submit arbitrary bodies, or access the agent API. Only a user-initiated reset triggers this fixed transactional template. Ordinary correspondence keeps its existing draft/review/send approval requirements.

The token is in a URL fragment so loading an email link does not send it to the website's access logs. The site consumes it only when the owner submits a new password. The provider governs expiry and single use. C-mail deduplicates identical requests through a stable draft/save identifier and its existing provider receipt; a fixed eight-per-hour ceiling supplements the site's network/global limits. Tokens and template content stay in C-mail's existing encrypted store and never appear in diagnostics. The new secret is held in Keychain and encrypted Cloudflare/Vercel settings.

Verification hooks and record:

- **PASS** `env -u NODE_TEST_CONTEXT npm test` in `packages/cmail-cloud`: 92 tests. Added forged/stale/oversized transport, scope restriction, fixed template/recipient, exact retry deduplication, flooding and provider-failure checks. The real delivery provider is mocked in these tests.
- **PASS** `npm run check`; release preflight also ran 13 CLI parity tests.
- **PASS** deployed worker `19d5a0b8-0cd4-4b1e-96b7-1a97774d4290`; all existing bindings and schedule preserved. Previous version `d11e5a6e-b6c2-40e0-a93c-b1b9d774f735`. Source `166edf0` preserves the already-live attachment release `3128b75`.
- **PASS** `packages/cmail-cloud/scripts/verify_deployment.py --require-agent --require-attachments`: exact live module hashes, namespace, Access protection and existing agent capability checked. Receipts are under ignored `data/qa/ritmo-recovery-2026-10-29/` in the release worktree.
- **PASS** Ritmo's `node site/scripts/check-auth-mail.mjs`: unsigned recovery 401; signed invalid body 400; the dedicated credential attempting the administrative mail API receives 401. Zero emails sent by these probes.
- **INCONCLUSIVE** actual recipient delivery and password change: user-controlled first request. No agent-triggered reset email or owner password change was performed.

Isolation: the main Life Ops checkout had unrelated classified changes, including an in-progress attachment release. This release used one temporary worktree and rebased onto that release after its live-source verification. Only the four auth-mail source/test/deployer paths and this record belong to this change. No unrelated paths are attributed or staged. Resume disposition: evidence-only refinement of Ritmo's existing prelaunch admin login; no separate public accomplishment.

Private-backup recovery: pre-push scanning found a historical `generic-api-key` match at `75e2896:src/life_ops/vault_crypto.py:21`. Inspection confirmed it is the static 15-character encryption-purpose label beside other purpose labels; the real master key is resolved separately through the credential store. `.gitleaksignore` records only that exact commit/path/rule/line fingerprint. No scanner is disabled and no actual credential is exempted. The complete outgoing commit range is rescanned before push. This scoped scanner metadata is part of the release's durable backup work.
