import test from "node:test";
import assert from "node:assert/strict";

import {
  LifeOpsClient,
  collectItems,
  composeAgenda,
  createStaticConnector,
  defineConnector,
  renderAgendaText,
} from "../src/index.js";

test("collectItems merges connector output into normalized items", async () => {
  const calendar = createStaticConnector({
    name: "calendar",
    items: [
      {
        id: "founder-sync",
        kind: "event",
        title: "Founder sync",
        startsAt: "2026-03-26T15:00:00.000Z",
        endsAt: "2026-03-26T16:00:00.000Z",
        organization: "Life Ops",
        priority: "high",
        source: { connector: "calendar", id: "founder-sync" },
      },
    ],
  });

  const followUps = defineConnector({
    name: "outreach",
    async pull() {
      return {
        items: [
          {
            id: "email-demo",
            kind: "communication",
            title: "Send project share email",
            followUpAt: "2026-03-26T18:00:00.000Z",
            priority: "urgent",
            organization: "Warm leads",
            source: { connector: "outreach", id: "email-demo" },
          },
          {
            id: "weekly-review",
            kind: "routine",
            title: "Weekly review",
            dueAt: "2026-03-27T01:00:00.000Z",
            source: { connector: "outreach", id: "weekly-review" },
          },
        ],
      };
    },
  });

  const collection = await collectItems({
    connectors: [calendar, followUps],
  });

  assert.equal(collection.items.length, 3);
  assert.equal(collection.reports.length, 2);
  assert.equal(collection.reports[0].ok, true);
});

test("LifeOpsClient agendas group a week view and render text", async () => {
  const client = new LifeOpsClient({
    connectors: [
      createStaticConnector({
        name: "calendar",
        items: [
          {
            id: "ship",
            kind: "event",
            title: "Ship npm package",
            startsAt: "2026-03-26T15:00:00.000Z",
            endsAt: "2026-03-26T15:30:00.000Z",
            organization: "Life Ops",
            source: { connector: "calendar", id: "ship" },
          },
          {
            id: "docs",
            kind: "task",
            title: "Write docs for connectors",
            dueAt: "2026-03-27T15:00:00.000Z",
            source: { connector: "calendar", id: "docs" },
          },
        ],
      }),
    ],
  });

  const agenda = await client.agenda({
    now: "2026-03-26T00:00:00.000Z",
    days: 3,
    timeZone: "UTC",
  });

  assert.equal(agenda.days.length, 3);
  assert.equal(agenda.days[0].items.length, 1);
  assert.equal(agenda.days[1].items.length, 1);

  const text = renderAgendaText(agenda);
  assert.match(text, /Ship npm package/);
  assert.match(text, /Write docs for connectors/);
});

test("composeAgenda keeps untimed open items in a floating section", () => {
  const agenda = composeAgenda({
    now: "2026-03-26T00:00:00.000Z",
    days: 2,
    timeZone: "UTC",
    items: [
      {
        id: "follow-up",
        kind: "communication",
        title: "Figure out share list",
        priority: "high",
        source: { connector: "manual", id: "follow-up" },
      },
    ],
  });

  assert.equal(agenda.floatingItems.length, 1);
  assert.equal(agenda.floatingItems[0].title, "Figure out share list");
});
