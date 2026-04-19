# lifeops

`lifeops` is the main public npm package for Life Ops.

Maintained by Fractal Research Group (`frg.earth`).

It is designed for teams and codebases that want a lightweight, installable command line for:

- scaffolding Life Ops starter files
- composing a week agenda from structured JSON feeds
- drafting structured project-share emails and follow-up queues
- controlling the local managed CMAIL inbox service

This package is the main install path for Life Ops.
The sibling [`@lifeops/core`](../life-ops-core/README.md) package remains available as a secondary SDK layer for builders who want the reusable primitives directly.

If you are trying Life Ops for the first time, start here.

## Self-hosted CMAIL

`CMAIL` is a self-hosted mail surface for Life Ops.

That means:

- you bring your own domain
- you bring your own Cloudflare setup
- you bring your own Resend setup
- you bring your own local secrets and API keys

Life Ops does not ship any FRG credentials, tokens, domains, or provider billing.
Each user/operator sets up and pays for their own mail infrastructure.

`lifeops cmail install` bootstraps the bundled Python backend into a local user-owned environment and installs the managed mailbox service for that user.

## Install

```bash
npm install -g lifeops
```

For local development from this repo:

```bash
npm install
npm install -g ./packages/lifeops-cli
```

## Commands

### `lifeops init`

Create starter files in the current directory:

```bash
lifeops init
```

Or scaffold another folder:

```bash
lifeops init ./my-lifeops
```

This writes:

- `lifeops.items.json`
- `lifeops.project.json`
- `lifeops.recipients.json`

### `lifeops agenda`

Render a week agenda from an item feed:

```bash
lifeops agenda --input ./lifeops.items.json --days 7 --timezone America/Chicago
lifeops agenda --input ./lifeops.items.json --format json
```

The input can be either:

- a JSON array of Life Ops items
- an object with an `items` array

### `lifeops share`

Create structured share drafts and follow-up items:

```bash
lifeops share \
  --project ./lifeops.project.json \
  --recipients ./lifeops.recipients.json \
  --sender-name Cody
```

To write packet artifacts to disk:

```bash
lifeops share \
  --project ./lifeops.project.json \
  --recipients ./lifeops.recipients.json \
  --output-dir ./outreach-packet
```

This writes:

- `packet.json`
- one `.txt` draft per recipient
- one `.html` draft per recipient

### `lifeops cmail`

Control the local managed CMAIL service:

```bash
lifeops cmail install
lifeops cmail status
lifeops cmail restart
lifeops cmail open
lifeops cmail tailscale
```

The package also installs a dedicated `cmail` shortcut:

```bash
cmail status
cmail restart
cmail open
cmail tailscale
```

For iPhone or other private mobile access, install Tailscale on the computer running CMAIL and on the phone, sign into the same tailnet, then run:

```bash
cmail tailscale-status
cmail secure-doctor
```

This deployment's canonical private phone URL is:

```text
https://cmail.tail649edd.ts.net
```

CMAIL itself still stays bound to `127.0.0.1:4311`. The shared Tailscale live-app-host routes `cmail.tail649edd.ts.net:443` to that local listener, so mobile users should save the no-port canonical URL to the iPhone Home Screen and keep the Tailscale app connected. Do not expose CMAIL with public Tailscale Funnel.

CMAIL is independent from Clawdad. Updating or publishing Clawdad does not update CMAIL; NPM/package users need a separate LifeOps/CMAIL release.

Drafts can also be created from anywhere on the machine, including attachments:

```bash
cmail new-draft --to alex@example.com --subject "A note" --body-file ./note.txt --attach ./paper.pdf --format json
cmail draft-save --id 74222 --attach ./figure.png --format json
```

This is intended for a self-hosted local mailbox running on your machine.
The install flow bootstraps the bundled backend into your local Life Ops home before starting the service.
