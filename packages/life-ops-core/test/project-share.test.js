import test from "node:test";
import assert from "node:assert/strict";

import {
  buildProjectSharePacket,
  draftProjectShareEmail,
  renderEmailHtml,
  renderEmailText,
  sendEmailDraft,
} from "../src/index.js";

const project = {
  name: "Life Ops",
  summary: "A local-first operating system for important human data and action loops.",
  whyNow: "The project has enough real surface area to get useful outside feedback.",
  highlights: [
    "Connector-based agenda composition for agents.",
    "Profile memory built from inbox, attachments, and review loops.",
  ],
  proofPoints: [
    "Structured Gmail and calendar ingestion already work in production for one user.",
    "Canonical profile records and X/Emma integrations are live.",
  ],
  codebases: [
    "life-ops Python CLI",
    "life-ops-core npm SDK",
  ],
  asks: ["Would you be open to a 15 minute walkthrough next week?"],
  links: [{ label: "Repo", url: "https://github.com/example/life-ops" }],
};

test("draftProjectShareEmail produces a structured outbound email", () => {
  const draft = draftProjectShareEmail({
    project,
    recipient: {
      name: "Alicia",
      email: "alicia@example.com",
      whyRecipient: "You care about agent-native software and sharp user loops.",
    },
    senderName: "Cody",
  });

  assert.equal(draft.subject, "Sharing Life Ops with you");
  assert.match(renderEmailText(draft), /Why I'm sending this your way/);
  assert.match(renderEmailText(draft), /Codebases in motion/);
  assert.match(renderEmailText(draft), /Would you be open to a 15 minute walkthrough next week\?/);
  assert.match(renderEmailHtml(draft), /Life Ops/);
});

test("buildProjectSharePacket creates both drafts and follow-up items", () => {
  const packet = buildProjectSharePacket({
    project,
    recipients: [
      {
        name: "Alicia",
        email: "alicia@example.com",
        whyRecipient: "You care about operational software.",
        ask: "If it resonates, I would love one direct reaction.",
        organization: "Operator Friends",
      },
    ],
    senderName: "Cody",
    baseTime: "2026-03-26T00:00:00.000Z",
  });

  assert.equal(packet.drafts.length, 1);
  assert.equal(packet.followUps.length, 1);
  assert.equal(packet.followUps[0].kind, "communication");
  assert.equal(packet.followUps[0].organization, "Operator Friends");
  assert.match(packet.followUps[0].title, /Follow up with Alicia/);
});

test("sendEmailDraft delegates to the provided sender transport", async () => {
  const draft = draftProjectShareEmail({
    project,
    recipient: {
      email: "ops@example.com",
      ask: "If useful, I'd love a pointed take.",
    },
  });

  let payload = null;
  const result = await sendEmailDraft({
    draft,
    sender: {
      async send(input) {
        payload = input;
        return { queued: true, id: "message-1" };
      },
    },
    context: { provider: "test" },
  });

  assert.deepEqual(result, { queued: true, id: "message-1" });
  assert.equal(payload.subject, draft.subject);
  assert.equal(payload.context.provider, "test");
});
