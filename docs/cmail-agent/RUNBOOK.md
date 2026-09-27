# CMAIL Agent runbook

## Expected launch

1. CMAIL posts the current `view`, `identity`, and selected row ID to `POST /api/agent-terminal`.
2. The server looks up the row in the live database and derives the trusted thread, message, contact, and identity pointers.
3. `src/life_ops/cmail_agent.py` resolves the dedicated `CMAIL Agent` ClawDad session, writes the pointer state with mode `0600`, and asks ClawDad to open the session in Terminal.
4. ClawDad launches Codex with the Life Ops checkout as its working directory. Existing native sessions use the shared Codex app-server resume path.

The browser cannot provide a shell command, project path, session ID, message body, or ClawDad token. The launch route accepts the loopback CMAIL origin and Cody's authenticated Tailscale identity on the canonical CMAIL host.

## Health checks

```sh
curl -fsS http://127.0.0.1:4311/api/health
zsh ./packages/lifeops-cli/bin/cmail-service status
clawdad sessions-doctor /Volumes/Code_2TB/code/life-ops --json
```

Inspect pointer state without treating it as message content:

```sh
stat -f '%Sp %N' ~/.lifeops/state/cmail-agent.json
python3 -m json.tool ~/.lifeops/state/cmail-agent.json
```

The file should be readable only by the local user (`-rw-------`).

## Button errors

- **CMAIL Agent is already opening**: wait for the current launch to finish, then click Retry.
- **Local ClawDad service is unavailable**: open the native ClawDad app, verify its health, and retry.
- **Session has no saved transcript / invalid project binding**: the launcher normally self-heals by creating a fresh named session. Run the session doctor if it repeats.
- **Selected CMAIL item is no longer available**: refresh CMAIL, select a current message or draft, and retry.
- **Tailnet identity required**: open the canonical CMAIL URL through Tailscale Serve while signed in as an allowed user.

## Verification after a code change

```sh
/Users/codymitchell/.lifeops/venvs/cmail/bin/python -m unittest \
  tests.test_clawdad_client \
  tests.test_cmail_agent

/Users/codymitchell/.lifeops/venvs/cmail/bin/python -m unittest \
  tests.test_mail_ui.MailUiTests.test_cmail_agent_button_is_keyboard_accessible_and_posts_selection \
  tests.test_mail_ui.MailUiTests.test_cmail_agent_selection_uses_server_side_pointer_fields_only \
  tests.test_mail_ui.MailUiTests.test_cmail_agent_launch_authorization_accepts_local_and_verified_tailnet \
  tests.test_mail_ui.MailUiTests.test_cmail_agent_endpoint_launches_with_validated_message_pointer

npm run prepack --prefix packages/lifeops-cli
zsh ./packages/lifeops-cli/bin/cmail-service restart
```

Calling `POST /api/agent-terminal` during a live smoke test opens a real Terminal window. Use a current CMAIL row ID and expect that visible side effect.
