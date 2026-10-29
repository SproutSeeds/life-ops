# C-mail CLI attachment and feature parity verification

Completed October 29, 2026. Production Worker version
`d11e5a6e-b6c2-40e0-a93c-b1b9d774f735` replaced
`e9b25ef2-b739-4162-8d81-a35561a494d5` at 19:48:34 UTC.
The website is https://cmail.frg.earth. Existing bindings, encryption namespace,
identities and the two-minute receiving schedule were preserved.

## Result

The signed agent API now uploads and downloads draft attachments through the
same immutable encrypted store used by the website. The installed `cmail` and
`lifeops cmail` commands support repeated `draft-save --attach PATH`, individual
removal, clearing, verified downloads and draft copies. Existing message fields
and attachments are preserved. Upload IDs are stable across save retries;
changed bytes under the same ID are rejected. The shared limits remain 50 files
and 20 MiB combined, with the same provider-blocked extensions.

The capability audit also added the website's inclusive date filters and
contact pagination revision to the CLI. The standing rule and capability matrix
are in `docs/cmail-feature-parity.md`, linked from the agent guide and package
instructions. Production deployment now runs cloud and CLI checks automatically
before accessing deployment credentials or mutating production.

## Verification

- Mandatory release gate: **88 cloud tests and 13 Python CLI tests passed**,
  plus cloud syntax checks. The gate removes inherited `NODE_TEST_CONTEXT` so
  child Node tests cannot silently skip their run.
- Native workerd/SQLite test invoked the actual Python CLI over signed HTTP
  with a **20,971,520-byte attachment**. Browser and CLI downloads, fake-provider
  request and Sent MIME all matched SHA-256
  `99254018a4506cae413a471f8b9d968a1ab1771565f3247b6e1c3f927e9a572f`.
- The isolated test covered upload/save replay, copies, one fake send across
  send retries, website-to-CLI edits, stale revision rejection, removal, reply
  attachments and cleanup. External networking was disabled. **Zero real sends.**
- Installed production `cmail` created one synthetic unsent draft with a PNG,
  Unicode-named PDF and custom binary file. Installed `lifeops cmail` reopened
  it and downloaded all three with matching hashes.
- Authenticated production website displayed those exact three attachments.
  A fourth file uploaded through the website was read and downloaded through
  `lifeops cmail`, matching its source bytes and hash.
- CLI removal of the PDF preserved the website-added file and message body.
  Repeating the exact save returned the same revision. Reloading the website
  showed the three remaining files.
- The synthetic draft was discarded through `lifeops cmail`, reopened through
  `cmail` as `discarded`, and absent from the website search. The verification
  tab was closed. Existing correspondence was unchanged.
- Deployed source hashes matched local modules. Website/attachment routes
  remained Access protected; unsigned agent calls remained rejected. The
  successful scheduled tick at **19:50:13 UTC** followed the new deployment.
- `git diff --check` passed. ORP hygiene reported zero unclassified paths.

Private machine-local receipts, fixture downloads, deployment inspection,
source hashes and a synthetic-only screenshot are in
`data/qa/cmail-cli-parity-2026-10-29/` (ignored). The previously installed external
Miniflare fixture remains under `/private/tmp/cmail-attachment-runtime/` for
repeatable runtime testing. Its per-run generated file directory was removed.

Real provider delivery was not exercised. The fake sender verified exact
payloads, Sent MIME and idempotency without delivering mail. This correction
changes the CLI and agent adapter; the previously deployed website UI remains
the shared visual surface.

## Worktree boundary

The 163 pre-existing dirty entries remain classified and preserved. Exact groups
and their owning-workstream next actions are recorded in
`../cmail-attachments-2026-10-29/WORKTREE-INVENTORY.json`. The pre-existing untracked
root `AGENTS.md` received only the requested parity-rule paragraph and remains
local; the same rule is committed in both package instruction files and the
canonical parity guide. This commit does not absorb that unrelated instruction
file or the pre-existing build output. Production used the established direct
Cloudflare deployment route; unrelated local Git history was not pushed.
