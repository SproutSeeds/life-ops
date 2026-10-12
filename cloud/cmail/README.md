# C-mail cloud inbox

This adds a private, read-only inbox to the existing Cloudflare email-ingest Worker. It reads encrypted messages already held in the `primary` MailQueue Durable Object, including messages acknowledged by the desktop sync process. It does not acknowledge, delete, requeue, forward, or send mail.

The browser can search senders/subjects, filter by recipient mailbox and received date, page through older messages, read decoded message bodies, and download attachments or the original `.eml`. Its manifest supports installation as a web app on macOS and iPhone. Mail bodies are not cached for offline use. Only mail delivered to this Cloudflare Worker is available; Gmail/Fastmail account synchronization and cloud sending are separate follow-up work.

## Existing service compatibility

`src/legacy-worker.mjs` is generated from `life_ops.cloudflare_email._worker_source` at Life Ops commit `6ff0e1c`. It retains the inbound email handler, encryption format, record keys, `primary` object name, and signed `/api/mail/queue/*` API. The active deployment inspected before this change was `3b8232d5`; its only observed protocol-helper difference is an older constant-time signature comparison implementation.

The cloud `MailQueue` subclasses this implementation. No namespace replacement, destructive migration, or master-key rotation is required. Keep the existing `MAIL_QUEUE` binding, `LIFE_OPS_MASTER_KEY`, `LIFE_OPS_MAIL_INGEST_SECRET`, and optional `FORWARD_TO` intact. Restoring the prior Worker version rolls back the reader without converting stored data.

Deploy this Worker from `cloud/cmail`. The Python CLI's older Worker-generation command does not include the cloud reader and would replace it if redeployed over the same service.

## Build and validation

```sh
cd cloud/cmail
npm ci
npm run build
npm test
npx wrangler deploy --dry-run
```

Node 20 or newer is required. `node scripts/preview.mjs` starts an isolated sample inbox on `127.0.0.1:8788/cmail/` for visual QA. The preview's sample data and authentication stub are not included in the production bundle.

## Private access

Configure a Cloudflare Access **self-hosted application** protecting the existing Worker's hostname under `/cmail` and every descendant path. Do not protect the entire Worker: desktop clients still need to reach the HMAC-protected `/api/mail/queue/*` routes without interactive sign-in.

Restrict its Allow policy to the owner's exact sign-in email. Use an independently accessible mailbox for sign-in so access does not depend on reading a C-mail email first. Do not use Everyone, wildcard domains, or a Bypass policy. Protect production and any preview hostname used to read real mail.

Configure these Worker environment values:

| Setting | Value |
| --- | --- |
| `CMAIL_ACCESS_TEAM_DOMAIN` | The exact `<team>.cloudflareaccess.com` domain, without a URL scheme. |
| `CMAIL_ACCESS_AUD` | The Access application's audience tag. |
| `CMAIL_OWNER_EMAIL` | The exact email allowed by the Access policy. |

The Worker independently verifies the Access JWT signature, issuer, audience, expiry, required identity claims, and owner email. Missing configuration or an invalid token fails closed, including through alternate Worker URLs. All inbox responses use `Cache-Control: private, no-store`; message HTML is converted to text, and remote images/scripts are never rendered.

Cloudflare may require activating Zero Trust Free before Access can be configured. Its checkout can require accepting terms and authorizing usage-based charges despite a $0 base price. Complete that account-level approval before proceeding; deployment alone must never remove authentication to work around it.

## Deploy and verify

1. Record the active Worker version and confirm the existing namespace binding. Use the same Cloudflare account and existing Worker name; do not create a replacement namespace.
2. Finish the Access application and owner-only policy. Confirm the Access audience and team domain.
3. Set the three Access values while preserving existing environment and secrets. Never commit authentication tokens or archive keys.
4. Deploy with the checked-in configuration, or upload the built `dist/index.js` through the existing Worker's editor without changing its bindings.
5. Verify the public `/healthz` endpoint still reports the encrypted queue healthy.
6. Verify anonymous `/cmail/` and `/cmail/api/messages` requests cannot return mailbox data. Sign in as the owner, open a known older message and its attachment, and verify a different identity is denied.
7. Confirm the old signed queue endpoint remains usable. Cloud reads must not change pending or acknowledged counts.
8. Save the `/cmail/` URL to the Mac Dock or iPhone Home Screen after the live verification succeeds.

Search intentionally scans bounded pages of archive records so a large inbox does not create one unbounded decryption request. “Search older messages” continues a search even when the current page contains no matches. Dates use the cloud receipt date in UTC. A corrupt message is reported without hiding other readable messages.

## Follow-up migration

This first release restores access to retained inbound domain mail independently of the home Mac. A full migration still needs an independently stored database/vault backup, outgoing-provider credentials, cloud sending/drafts with idempotency, and explicit account-by-account Gmail/Fastmail connection work. Do not start the old desktop send worker on a recovered database until queued sends have been reviewed.
