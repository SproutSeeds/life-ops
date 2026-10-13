# C-mail cloud inbox

The public entry point is [cmail.frg.earth](https://cmail.frg.earth/). It requires owner sign-in and opens the inbox at the domain root. The existing Worker's `/cmail/` address and bookmarked message links continue to work.

This adds a private, read-only inbox to the existing Cloudflare email-ingest Worker. It reads encrypted messages already held in the `primary` MailQueue Durable Object, including messages acknowledged by the desktop sync process. It does not acknowledge, delete, requeue, forward, or send mail.

The browser can search senders, subjects, recipients, and decoded message bodies, filter by recipient mailbox and received date, page through older messages, read decoded message bodies, and download attachments or the original `.eml`. Its manifest supports installation as a web app on macOS and iPhone. Mail bodies are not cached for offline use. Only mail delivered to this Cloudflare Worker is available; Gmail/Fastmail account synchronization and cloud sending are separate follow-up work.

Opening a message adds its archive ID to the URL fragment. This gives each message a bookmarkable link that still requires owner sign-in and survives a page reload.

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

Add `cmail.frg.earth` as a second public hostname in that same Access application, with the path empty so the whole custom hostname is protected. This keeps the same owner policy and audience. Configure this protection before adding the Worker's custom domain. The custom domain is recorded in `wrangler.toml`; Cloudflare provisions its DNS record and TLS certificate. Keep desktop sync clients on their existing Worker URL.

Restrict its Allow policy to the owner's exact sign-in email. Use an independently accessible mailbox for sign-in so access does not depend on reading a C-mail email first. Do not use Everyone, wildcard domains, or a Bypass policy. Protect production and any preview hostname used to read real mail.

Configure these Worker environment values:

| Setting | Value |
| --- | --- |
| `CMAIL_ACCESS_TEAM_DOMAIN` | The exact `<team>.cloudflareaccess.com` domain, without a URL scheme. |
| `CMAIL_ACCESS_AUD` | The Access application's audience tag. |
| `CMAIL_OWNER_EMAIL` | The exact email allowed by the Access policy. |

The Worker independently verifies the Access JWT signature, issuer, audience, expiry, required identity claims, and owner email. Missing configuration or an invalid token fails closed, including through alternate Worker URLs. All inbox responses use `Cache-Control: private, no-store`; message HTML is converted to text, and remote images/scripts are never rendered.

The header includes a visible Sign out link on desktop and mobile. It uses Cloudflare's `/cdn-cgi/access/logout` endpoint and clears rendered mail before leaving. Cloudflare revokes the user's Access session across applications, clears the current hostname's authorization cookie immediately, and stops accepting previously issued tokens within 20–30 seconds. Returning through browser history reloads the protected page rather than restoring a stale inbox. An existing Access session can sign the owner in automatically; that does not make the inbox public. See [Cloudflare session management](https://developers.cloudflare.com/cloudflare-one/access-controls/access-settings/session-management/).

Cloudflare may require activating Zero Trust Free before Access can be configured. Its checkout can require accepting terms and authorizing usage-based charges despite a $0 base price. Complete that account-level approval before proceeding; deployment alone must never remove authentication to work around it.

### Login appearance

The hosted sign-in page uses C-mail branding, configured in **Zero Trust → Reusable components → Custom pages → Access login page**. This is an organization-wide appearance setting; C-mail was the only Access application when it was applied.

| Setting | Value |
| --- | --- |
| Organization name | `C-mail` |
| Logo URL | `https://raw.githubusercontent.com/SproutSeeds/life-ops/e7e3a3b7c7a192c3762e18e084e03cd3a88c04b0/cloud/cmail/web/icon-192.png` |
| Header | Your mail, wherever you are. |
| Message | A private space for your connected mail.<br>Sign in with your authorized account to continue. |
| Background | `#0d1411` |

The logo is a public, commit-pinned copy of the existing app icon so it loads before authentication. These settings do not change the owner policy, identity providers, session duration, or Worker authentication. The hosted page retains the Cloudflare Access heading and its provider button; the available customization controls do not offer a fully unbranded form. See [Cloudflare login page customization](https://developers.cloudflare.com/cloudflare-one/reusable-components/custom-pages/access-login-page/).

## Deploy and verify

1. Record the active Worker version and confirm the existing namespace binding. Use the same Cloudflare account and existing Worker name; do not create a replacement namespace.
2. Finish the Access application and owner-only policy. Confirm the Access audience and team domain.
3. Set the three Access values while preserving existing environment and secrets. Never commit authentication tokens or archive keys.
4. Deploy with the checked-in configuration, or upload the built `dist/index.js` through the existing Worker's editor without changing its bindings.
5. Verify the public `/healthz` endpoint still reports the encrypted queue healthy.
6. Verify anonymous `/cmail/` and `/cmail/api/messages` requests cannot return mailbox data. Sign in as the owner, open a known older message and its attachment, and verify a different identity is denied.
7. Confirm the old signed queue endpoint remains usable. Cloud reads must not change pending or acknowledged counts.
8. Verify owner sign-in and anonymous denial at `https://cmail.frg.earth/` and its `/cmail/api/messages` path, then save the custom domain to the Mac Dock or iPhone Home Screen.

Search scans bounded pages of archive records, automatically continuing through older pages while displaying progress and matches as they arrive. Stop pauses the scan; Continue search resumes it. A new search cancels the previous request. Only a completed scan may report no matches; interrupted or corrupt reads are identified as incomplete. Matching includes decoded plain text and HTML message bodies, without indexing binary attachments or creating a plaintext search index. Searches can take time on a large encrypted archive. Dates use the cloud receipt date in UTC.

## Follow-up migration

This first release restores access to retained inbound domain mail independently of the home Mac. A full migration still needs an independently stored database/vault backup, outgoing-provider credentials, cloud sending/drafts with idempotency, and explicit account-by-account Gmail/Fastmail connection work. Do not start the old desktop send worker on a recovered database until queued sends have been reviewed.
