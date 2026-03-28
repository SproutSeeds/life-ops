# @lifeops/core

`@lifeops/core` is the installable JavaScript SDK for Life Ops.

If you want the global command-line surface instead of the embeddable SDK, use the sibling `lifeops` package in `packages/lifeops-cli`.

It gives other codebases a small, connector-based surface for:

- pulling important signals from their own data feeds
- normalizing those signals into a shared Life Ops item model
- composing a week agenda for agent workflows
- drafting structured project-share emails and follow-ups
- handing email drafts to any transport layer they already trust

## Install

```bash
npm install @lifeops/core
```

For local development from this repo:

```bash
npm install ./packages/life-ops-core
```

## Quick example

```js
import {
  LifeOpsClient,
  createStaticConnector,
  buildProjectSharePacket,
  renderAgendaText,
} from "@lifeops/core";

const client = new LifeOpsClient({
  connectors: [
    createStaticConnector({
      name: "calendar",
      items: [
        {
          id: "demo-sync",
          kind: "event",
          title: "Founder sync",
          startsAt: "2026-03-26T15:00:00.000Z",
          endsAt: "2026-03-26T16:00:00.000Z",
          priority: "high",
          organization: "Life Ops",
        },
      ],
    }),
  ],
});

const agenda = await client.agenda({
  now: "2026-03-26T00:00:00.000Z",
  days: 7,
  timezone: "America/Chicago",
});

console.log(renderAgendaText(agenda));

const sharePacket = buildProjectSharePacket({
  project: {
    name: "Life Ops",
    summary: "A local-first operating system for important human data and action loops.",
    whyNow: "The project is ready to share with a few high-context collaborators.",
    highlights: [
      "Normalizes inbox, calendar, and records into one action layer.",
      "Lets an agent draft and follow through on operational outreach.",
    ],
    proofPoints: [
      "Full Gmail archive and attachment corpus ingestion.",
      "Canonical profile memory and alert surfaces.",
    ],
    links: [
      { label: "Repo", url: "https://github.com/example/life-ops" },
    ],
  },
  recipients: [
    {
      name: "Alicia",
      email: "alicia@example.com",
      whyRecipient: "You care about agent-native personal software and sharp product framing.",
      ask: "If it resonates, I'd love a short reaction and one intro that feels obvious to you.",
    },
  ],
  senderName: "Cody",
});

console.log(sharePacket.drafts[0].text);
```

## API surface

### Connectors

- `defineConnector`
- `createStaticConnector`
- `collectItems`
- `LifeOpsClient`

### Agenda

- `normalizeItem`
- `normalizeItems`
- `composeAgenda`
- `renderAgendaText`

### Email and project sharing

- `draftStructuredEmail`
- `renderEmailText`
- `renderEmailHtml`
- `sendEmailDraft`
- `draftProjectShareEmail`
- `buildProjectSharePacket`
- `createProjectShareFollowUpItem`

## Email sending

`@lifeops/core` does not force a mail provider.

Instead, `sendEmailDraft` expects a sender object with a `send(payload)` method, which lets you plug in `resend`, `nodemailer`, `postmark`, or your own queue.

```js
await sendEmailDraft({
  draft,
  sender: {
    async send(payload) {
      return resend.emails.send({
        from: "you@example.com",
        to: payload.to.map((recipient) => recipient.email),
        subject: payload.subject,
        text: payload.text,
        html: payload.html,
      });
    },
  },
});
```
