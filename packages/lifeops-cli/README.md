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
```

The package also installs a dedicated `cmail` shortcut:

```bash
cmail status
cmail restart
cmail open
```

This is intended for a self-hosted local mailbox running on your machine.
The install flow bootstraps the bundled backend into your local Life Ops home before starting the service.
