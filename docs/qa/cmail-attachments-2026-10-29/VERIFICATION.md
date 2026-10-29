# C-mail compose and draft attachments

Production: https://cmail.frg.earth

Deployed October 29, 2026 as Cloudflare Worker version
`e9b25ef2-b739-4162-8d81-a35561a494d5`, replacing
`e10204a4-e988-4575-8710-8029f09a446c`. The existing namespace, bindings,
identities and two-minute receiving schedule were preserved. All deployed source
hashes match this checkout's `packages/cmail-cloud/src/` modules.

## Result

The cloud composer previously had no file attachment implementation. Compose and
reply drafts now support selecting multiple files, dropping files, pasting
images, downloading and removing files, and persisting uploads across draft
reopens. Attachments use encrypted immutable chunks and small draft references.
The existing revision checks and durable send receipts cover those references.
Sent MIME preserves the exact file bytes, filenames, reply headers and branded
text/HTML alternatives. Older clients preserve attachments when they omit the
new field.

The limit is 50 files and 20 MiB combined. There is no extension allowlist;
the current provider's blocked extensions produce an explicit error. This total
fits the existing 30 MiB MIME archive limit and the provider's 40 MB encoded
message limit. Removed or abandoned uploads receive a seven-day grace period;
the existing scheduled receiving invocation cleans at most one expired,
unreferenced file per run. Draft and outbox references protect active files.

## Verification

- `env -u NODE_TEST_CONTEXT npm test --prefix packages/cmail-cloud`: 86 passed.
  The environment variable must be removed in this tool environment to prevent
  Node from incorrectly treating the runner as a recursive test child.
- `npm run check --prefix packages/cmail-cloud`: passed for all source modules.
- `PYTHONPATH=src .venv/bin/python -m unittest tests.test_cmail_cloud`: 10 passed.
- The network-isolated Miniflare runtime test passed at the full 20 MiB boundary.
  SHA-256 `99254018a4506cae413a471f8b9d968a1ab1771565f3247b6e1c3f927e9a572f`
  matched upload, draft download, simulated provider request and Sent attachment.
  A second file exceeding the combined limit was rejected without changing the
  saved draft. Outbound networking was disabled; provider replies were fixtures.
- Browser checks covered multiple files, TXT/PDF/ZIP/unknown extensions,
  Unicode filenames, removal, reload, image paste, oversized and blocked-type
  errors, inline and modal composers, simulated sending, Escape/back navigation,
  pointer cursors and a 390px CSS viewport without horizontal overflow.
- Production browser upload, automatic draft save, reload and download passed.
  Cloud CLI readback confirmed the exact three filenames, sizes and SHA-256s.
  The downloaded TXT file matched the fixture byte for byte.
- The production synthetic draft had no recipients and Send remained disabled.
  It was discarded through the canonical CLI and read back as discarded. No
  real email was sent, and existing user drafts were not changed.
- Production source verification, Access redirects, signed-agent protection,
  indexed Inbox/Sent/contacts/drafts, Spam and post-deployment scheduled receiving
  passed. Both attachment upload and download redirected unauthenticated calls.
- Scoped `git diff --check` passed. Unrelated pre-existing work remains classified
  in `WORKTREE-INVENTORY.json`; no unrelated paths are included in this commit.

## Evidence and limits

Private receipts, test logs, synthetic fixtures and the production screenshot
are under `data/qa/cmail-attachments-2026-10-29/` (ignored by Git). The runtime
test is reproducible with:

```sh
env -u NODE_TEST_CONTEXT node packages/cmail-cloud/scripts/attachments-runtime-smoke.mjs \
  /private/tmp/cmail-attachment-runtime/node_modules/miniflare/dist/src/index.js --large
```

The temporary Miniflare dependency installation is retained at that path for
reproduction; it is separate from repository dependencies. The test server was
stopped after QA. Real provider delivery and a physical-phone test were not run.
Browser drag-and-drop shares the tested file-input handler but was not separately
driven through an operating-system drag gesture. The public resume remains
unchanged; see `RESUME_ASSESSMENT.json` for the routine-maintenance disposition.
