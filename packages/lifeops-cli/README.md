# lifeops

`lifeops` is the global npm CLI for the public JavaScript surface of Life Ops.

It is designed for teams and codebases that want a lightweight, installable command line for:

- scaffolding Life Ops starter files
- composing a week agenda from structured JSON feeds
- drafting structured project-share emails and follow-up queues

This package sits on top of [`@lifeops/core`](../life-ops-core/README.md), which remains the reusable SDK layer.

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
