# Fastmail Setup

Fastmail is now the recommended non-Google mailbox path for `life-ops` when you want custom-domain email plus agent-friendly automation.

## Why Fastmail

- custom domains without Google Workspace overhead
- JMAP API for modern mail automation
- IMAP/SMTP fallback if needed later
- API tokens for self-use now, OAuth later for a distributed/public integration

Official references:

- https://www.fastmail.com/dev/
- https://www.fastmail.help/hc/en-us/articles/360058753394-Custom-domains-with-Fastmail
- https://www.fastmail.com/pricing/

## Suggested path for `frg.earth`

1. Create a Fastmail account.
2. Add `frg.earth` as a custom domain in Fastmail.
3. Follow Fastmail's DNS wizard for verification and mail routing.
4. Create `cody@frg.earth`.
5. Generate a Fastmail API token in `Settings -> Privacy & Security -> Manage API tokens`.
6. Register that token in `life-ops`.

## Local `life-ops` setup

Write a local Fastmail config template:

```bash
zsh ./bin/life-ops fastmail-init-config
```

That creates:

- `config/fastmail.json`

Recommended config shape:

```json
{
  "account_email": "cody@frg.earth",
  "session_url": "https://api.fastmail.com/jmap/session",
  "api_token": "",
  "api_token_env": "FASTMAIL_API_TOKEN"
}
```

Register the API token once:

```bash
zsh ./bin/life-ops keys-set --name FASTMAIL_API_TOKEN --value "your-fastmail-api-token"
```

Then validate the connection:

```bash
zsh ./bin/life-ops fastmail-status
zsh ./bin/life-ops fastmail-session
zsh ./bin/life-ops fastmail-mailboxes
```

## What is live now

- local Fastmail config template
- JMAP session validation
- mailbox listing through JMAP

## Next phases

1. Mailbox and thread sync into SQLite
2. Draft/send/reply flows through JMAP submission
3. Notification and lifecycle automation on top of Fastmail instead of Gmail
