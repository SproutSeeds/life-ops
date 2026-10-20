# C-mail cloud runbook

Start with `cmail status` and `cmail --help`. Run these from any working directory.
`lifeops cmail status` must report the same `cmail-cloud` system and identities.
The website is https://cmail.frg.earth. The cloud credential stays in Keychain;
never paste it into chat, print it, or place it in a request artifact.

If a draft appears missing, check the sender filter with `cmail drafts`, then
read its cloud ID with `cmail draft ID`. An agent's prose/file is not a saved
email. A legacy numeric draft ID is resolved only through the migration mapping
in `~/.lifeops/config/cmail-cloud.json`. Verify exact content and `state: editing`
after any authorized save. The link shape is
`https://cmail.frg.earth/?view=drafts&draft=draft_UUID`.

On conflict, reread the current cloud revision and reconcile the actual changes.
Do not overwrite a browser edit by inventing a revision. On a send timeout, check
the same draft/receipt; never fall back to direct Resend or another mailbox.
Unsupported commands fail explicitly rather than create hidden local drafts.

Local `GET /api/health` remains available only for the service supervisor and
reports `system: cmail-cloud`, `legacy_ui_retired: true`. All other old mail APIs
return 410. The localhost/Tailscale website redirects to the new website. The
legacy send queue was empty at cutover and its processing worker is disabled.
Do not run old install/repair instructions to reopen it.

The existing local service now projects new cloud messages into Life Ops and
keeps its sealing/backup jobs. Inspect `cmail_cloud_projection` and runtime worker
health when troubleshooting that projection. Keep task/follow-up dispositions
separate from cloud mailbox read/archive state. Projection errors must be fixed
without reopening the old mail UI or sending queue.

Verification commands for this implementation:

```sh
npm test --prefix packages/cmail-cloud
npm run check --prefix packages/cmail-cloud
PYTHONPATH=src ./.venv/bin/python -m unittest tests.test_cmail_cloud
```

Tests for the retired upstream behavior must use an isolated temporary HOME or
mock `cmail_cloud.active` false; this machine's real cutover intentionally blocks
old local writes. Never disable the live cutover file merely to run legacy tests.

Deploy through `packages/cmail-cloud/scripts/deploy.py` with the observed expected
version and existing namespace. Verify all production source hashes, Access,
agent HMAC protection and scheduled receiving. Draft migration uses
`scripts/migrate_local_drafts.py` within that package, deterministic IDs and a
private receipt; it never sends or modifies original rows. A verified prior
receipt makes reruns read-only for that draft, preserving subsequent user edits.

The October 20 cutover evidence is in
`docs/qa/cmail-agent-cutover-2026-10-20/VERIFICATION.md`; private receipts and
screenshots are under the corresponding ignored `data/qa/` directory.
