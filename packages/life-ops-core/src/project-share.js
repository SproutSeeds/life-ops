import { draftStructuredEmail } from "./email.js";
import { normalizeItem } from "./model.js";

function normalizeStringArray(values = []) {
  if (!Array.isArray(values)) {
    return [];
  }
  return values.map((value) => String(value).trim()).filter(Boolean);
}

function normalizeLinks(links = []) {
  if (!Array.isArray(links)) {
    return [];
  }
  return links
    .map((link) => {
      if (!link || typeof link !== "object") {
        return null;
      }
      const label = String(link.label ?? "").trim();
      const url = String(link.url ?? "").trim();
      if (!label || !url) {
        return null;
      }
      return { label, url };
    })
    .filter(Boolean);
}

function normalizeProject(project) {
  if (!project || typeof project !== "object") {
    throw new Error("Expected a project definition object.");
  }
  const name = String(project.name ?? "").trim();
  const summary = String(project.summary ?? "").trim();
  if (!name || !summary) {
    throw new Error("Projects require both a name and summary.");
  }
  return {
    name,
    summary,
    whyNow: String(project.whyNow ?? "").trim(),
    highlights: normalizeStringArray(project.highlights),
    proofPoints: normalizeStringArray(project.proofPoints),
    asks: normalizeStringArray(project.asks),
    links: normalizeLinks(project.links),
    codebases: normalizeStringArray(project.codebases),
  };
}

function normalizeRecipient(recipient) {
  if (!recipient || typeof recipient !== "object") {
    throw new Error("Expected a recipient definition object.");
  }
  const email = String(recipient.email ?? "").trim();
  if (!email) {
    throw new Error("Project share recipients require an email.");
  }
  return {
    email,
    name: recipient.name == null ? null : String(recipient.name).trim() || null,
    whyRecipient: String(recipient.whyRecipient ?? "").trim(),
    ask: String(recipient.ask ?? "").trim(),
    organization: recipient.organization == null ? null : String(recipient.organization).trim() || null,
    subjectHook: String(recipient.subjectHook ?? "").trim(),
    followUpDays:
      recipient.followUpDays == null ? null : Math.max(1, Number.parseInt(String(recipient.followUpDays), 10)),
  };
}

function createSubject(project, recipient) {
  if (recipient.subjectHook) {
    return `${project.name} | ${recipient.subjectHook}`;
  }
  return `Sharing ${project.name} with you`;
}

function createPreviewText(project) {
  return `${project.name}: ${project.summary}`;
}

function createIntro(recipient) {
  if (recipient.name) {
    return `Hey ${recipient.name},`;
  }
  return "Hey there,";
}

export function draftProjectShareEmail({
  project,
  recipient,
  senderName = "A collaborator",
  closingNote = "",
} = {}) {
  const normalizedProject = normalizeProject(project);
  const normalizedRecipient = normalizeRecipient(recipient);
  const ask = normalizedRecipient.ask || normalizedProject.asks[0] || "If it resonates, I'd love your reaction.";

  const sections = [
    normalizedRecipient.whyRecipient
      ? {
          heading: "Why I'm sending this your way",
          body: normalizedRecipient.whyRecipient,
        }
      : null,
    {
      heading: "What it is",
      body: `${normalizedProject.name}: ${normalizedProject.summary}`,
      bullets: normalizedProject.highlights,
    },
    normalizedProject.whyNow
      ? {
          heading: "Why now",
          body: normalizedProject.whyNow,
        }
      : null,
    normalizedProject.proofPoints.length > 0
      ? {
          heading: "What makes it real already",
          bullets: normalizedProject.proofPoints,
        }
      : null,
    normalizedProject.codebases.length > 0
      ? {
          heading: "Codebases in motion",
          bullets: normalizedProject.codebases,
        }
      : null,
    normalizedProject.links.length > 0
      ? {
          heading: "Where to look",
          bullets: normalizedProject.links.map((link) => `${link.label}: ${link.url}`),
        }
      : null,
    {
      heading: "Specific ask",
      body: ask,
    },
  ].filter(Boolean);

  const closingParts = ["Thanks,", senderName];
  if (closingNote) {
    closingParts.push("", closingNote);
  }

  return {
    project: normalizedProject,
    recipient: normalizedRecipient,
    ...draftStructuredEmail({
      to: [{ email: normalizedRecipient.email, name: normalizedRecipient.name }],
      subject: createSubject(normalizedProject, normalizedRecipient),
      previewText: createPreviewText(normalizedProject),
      intro: createIntro(normalizedRecipient),
      sections,
      cta: "Happy to send more context, a walkthrough, or a tighter build note if useful.",
      closing: closingParts.join("\n"),
      metadata: {
        kind: "project-share",
        projectName: normalizedProject.name,
      },
    }),
  };
}

function addDays(isoString, days) {
  const date = new Date(isoString);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString();
}

export function createProjectShareFollowUpItem({
  draft,
  followUpAt,
  baseTime = new Date().toISOString(),
} = {}) {
  if (!draft || typeof draft !== "object") {
    throw new Error("Expected a project-share draft.");
  }
  const recipientName = draft.recipient?.name || draft.recipient?.email || "contact";
  const scheduledFollowUpAt =
    followUpAt ??
    addDays(baseTime, draft.recipient?.followUpDays ?? 5);

  return normalizeItem({
    id: `follow-up:${draft.project.name}:${draft.recipient.email}`,
    kind: "communication",
    title: `Follow up with ${recipientName} about ${draft.project.name}`,
    summary: draft.subject,
    followUpAt: scheduledFollowUpAt,
    priority: "high",
    organization: draft.recipient?.organization,
    tags: ["project-share", "email-follow-up"],
    source: {
      connector: "life-ops-project-share",
      id: draft.recipient.email,
    },
    metadata: {
      recipientEmail: draft.recipient.email,
      projectName: draft.project.name,
    },
  });
}

export function buildProjectSharePacket({
  project,
  recipients = [],
  senderName = "A collaborator",
  baseTime = new Date().toISOString(),
} = {}) {
  if (!Array.isArray(recipients) || recipients.length === 0) {
    throw new Error("Project share packets require at least one recipient.");
  }

  const drafts = recipients.map((recipient) =>
    draftProjectShareEmail({
      project,
      recipient,
      senderName,
    }),
  );

  const followUps = drafts.map((draft) =>
    createProjectShareFollowUpItem({
      draft,
      baseTime,
    }),
  );

  return {
    project: normalizeProject(project),
    drafts,
    followUps,
  };
}
