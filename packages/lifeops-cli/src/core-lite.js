const ITEM_KINDS = new Set([
  "event",
  "communication",
  "routine",
  "task",
  "alert",
  "document",
]);

const ITEM_STATUSES = new Set([
  "open",
  "in_progress",
  "reference",
  "done",
  "ignored",
]);

const PRIORITY_LEVELS = new Set(["urgent", "high", "normal", "low"]);

function normalizeDateValue(value, fieldName) {
  if (value == null || value === "") {
    return null;
  }
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Expected ${fieldName} to be a valid date-like value.`);
  }
  return date.toISOString();
}

function normalizeStringArray(values) {
  if (!Array.isArray(values)) {
    return [];
  }
  return values
    .map((value) => String(value).trim())
    .filter(Boolean);
}

function normalizeLinks(links) {
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

function normalizeSource(source) {
  if (!source || typeof source !== "object") {
    return { connector: "manual", id: null, account: null };
  }
  return {
    connector: String(source.connector ?? "manual").trim() || "manual",
    id: source.id == null ? null : String(source.id),
    account: source.account == null ? null : String(source.account),
  };
}

function inferStatus(kind, status) {
  if (status) {
    return status;
  }
  if (kind === "document") {
    return "reference";
  }
  return "open";
}

function normalizeItem(item) {
  if (!item || typeof item !== "object") {
    throw new Error("Expected a Life Ops item object.");
  }

  const kind = String(item.kind ?? "").trim();
  if (!ITEM_KINDS.has(kind)) {
    throw new Error(`Unsupported item kind: ${kind || "missing"}.`);
  }

  const title = String(item.title ?? "").trim();
  if (!title) {
    throw new Error("Life Ops items require a title.");
  }

  const status = inferStatus(kind, item.status ? String(item.status).trim() : "");
  if (!ITEM_STATUSES.has(status)) {
    throw new Error(`Unsupported item status: ${status}.`);
  }

  const priority = item.priority ? String(item.priority).trim() : "normal";
  if (!PRIORITY_LEVELS.has(priority)) {
    throw new Error(`Unsupported priority level: ${priority}.`);
  }

  return {
    id: item.id == null ? `item:${kind}:${title.toLowerCase().replace(/\s+/g, "-")}` : String(item.id),
    kind,
    title,
    summary: String(item.summary ?? "").trim(),
    status,
    priority,
    startsAt: normalizeDateValue(item.startsAt, "startsAt"),
    endsAt: normalizeDateValue(item.endsAt, "endsAt"),
    dueAt: normalizeDateValue(item.dueAt, "dueAt"),
    followUpAt: normalizeDateValue(item.followUpAt, "followUpAt"),
    organization: item.organization == null ? null : String(item.organization).trim() || null,
    tags: normalizeStringArray(item.tags),
    links: normalizeLinks(item.links),
    source: normalizeSource(item.source),
    metadata: item.metadata && typeof item.metadata === "object" ? { ...item.metadata } : {},
  };
}

function normalizeItems(items = []) {
  if (!Array.isArray(items)) {
    throw new Error("Expected an array of Life Ops items.");
  }
  return items.map((item) => normalizeItem(item));
}

function getItemTimestamp(item) {
  return item.startsAt ?? item.followUpAt ?? item.dueAt ?? item.endsAt ?? null;
}

function compareItems(left, right) {
  const leftTime = getItemTimestamp(left);
  const rightTime = getItemTimestamp(right);

  if (leftTime && rightTime) {
    const difference = new Date(leftTime).getTime() - new Date(rightTime).getTime();
    if (difference !== 0) {
      return difference;
    }
  }

  if (leftTime && !rightTime) {
    return -1;
  }
  if (!leftTime && rightTime) {
    return 1;
  }

  const priorityRank = { urgent: 0, high: 1, normal: 2, low: 3 };
  const priorityDifference = priorityRank[left.priority] - priorityRank[right.priority];
  if (priorityDifference !== 0) {
    return priorityDifference;
  }

  return left.title.localeCompare(right.title);
}

function createDayBucket(baseDate, timeZone) {
  const keyFormatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const labelFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    weekday: "short",
    month: "short",
    day: "numeric",
  });

  return {
    date: keyFormatter.format(baseDate),
    label: labelFormatter.format(baseDate),
    items: [],
  };
}

function formatItemTime(item, timeZone) {
  const timestamp = getItemTimestamp(item);
  if (!timestamp) {
    return null;
  }
  return new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(timestamp));
}

function createAgendaEntry(item, timeZone) {
  return {
    ...item,
    primaryTime: getItemTimestamp(item),
    timeLabel: formatItemTime(item, timeZone),
  };
}

export function composeAgenda({
  items = [],
  now = new Date(),
  days = 7,
  timeZone,
  timezone,
  includeStatuses = ["open", "in_progress"],
  includeUntimed = true,
} = {}) {
  const normalizedNow = now instanceof Date ? now : new Date(now);
  if (Number.isNaN(normalizedNow.getTime())) {
    throw new Error("Expected now to be a valid date-like value.");
  }
  if (!Number.isInteger(days) || days <= 0) {
    throw new Error("Expected days to be a positive integer.");
  }
  const resolvedTimeZone = String(timeZone ?? timezone ?? "UTC");

  const startMs = normalizedNow.getTime();
  const endMs = startMs + days * 24 * 60 * 60 * 1000;
  const normalizedItems = normalizeItems(items).filter((item) => includeStatuses.includes(item.status));

  const dayBuckets = [];
  const dayMap = new Map();
  for (let offset = 0; offset < days; offset += 1) {
    const day = new Date(startMs + offset * 24 * 60 * 60 * 1000);
    const bucket = createDayBucket(day, resolvedTimeZone);
    dayBuckets.push(bucket);
    dayMap.set(bucket.date, bucket);
  }

  const floatingItems = [];
  const countsByKind = {};

  for (const item of normalizedItems) {
    countsByKind[item.kind] = (countsByKind[item.kind] ?? 0) + 1;
    const timestamp = getItemTimestamp(item);
    if (!timestamp) {
      if (includeUntimed) {
        floatingItems.push(createAgendaEntry(item, resolvedTimeZone));
      }
      continue;
    }

    const millis = new Date(timestamp).getTime();
    if (millis < startMs || millis >= endMs) {
      continue;
    }

    const dateKey = new Intl.DateTimeFormat("en-CA", {
      timeZone: resolvedTimeZone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(new Date(timestamp));

    const bucket = dayMap.get(dateKey);
    if (!bucket) {
      continue;
    }
    bucket.items.push(createAgendaEntry(item, resolvedTimeZone));
  }

  for (const bucket of dayBuckets) {
    bucket.items.sort(compareItems);
  }
  floatingItems.sort(compareItems);

  return {
    generatedAt: new Date().toISOString(),
    windowStart: new Date(startMs).toISOString(),
    windowEnd: new Date(endMs).toISOString(),
    timeZone: resolvedTimeZone,
    stats: {
      totalItems: normalizedItems.length,
      scheduledItems: dayBuckets.reduce((count, bucket) => count + bucket.items.length, 0),
      floatingItems: floatingItems.length,
      countsByKind,
    },
    days: dayBuckets,
    floatingItems,
  };
}

export function renderAgendaText(agenda) {
  const lines = [
    `Agenda ${agenda.windowStart} -> ${agenda.windowEnd} (${agenda.timeZone})`,
  ];

  for (const day of agenda.days) {
    lines.push("");
    lines.push(day.label);
    if (day.items.length === 0) {
      lines.push("- No scheduled items");
      continue;
    }
    for (const item of day.items) {
      const prefix = item.timeLabel ? `${item.timeLabel} ` : "";
      const context = [item.kind, item.organization, item.priority].filter(Boolean).join(", ");
      lines.push(`- ${prefix}${item.title}${context ? ` [${context}]` : ""}`);
    }
  }

  if (agenda.floatingItems.length > 0) {
    lines.push("");
    lines.push("Floating");
    for (const item of agenda.floatingItems) {
      const context = [item.kind, item.organization, item.priority].filter(Boolean).join(", ");
      lines.push(`- ${item.title}${context ? ` [${context}]` : ""}`);
    }
  }

  return lines.join("\n");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeRecipient(recipient) {
  if (typeof recipient === "string") {
    const email = recipient.trim();
    if (!email) {
      throw new Error("Email recipient strings cannot be empty.");
    }
    return { email, name: null };
  }
  if (!recipient || typeof recipient !== "object") {
    throw new Error("Expected an email recipient object or email string.");
  }
  const email = String(recipient.email ?? "").trim();
  if (!email) {
    throw new Error("Email recipients require an email address.");
  }
  const name = recipient.name == null ? null : String(recipient.name).trim() || null;
  return { email, name };
}

function normalizeRecipients(recipients = []) {
  if (!Array.isArray(recipients)) {
    throw new Error("Expected recipients to be an array.");
  }
  return recipients.map((recipient) => normalizeRecipient(recipient));
}

function normalizeSections(sections = []) {
  if (!Array.isArray(sections)) {
    throw new Error("Expected sections to be an array.");
  }
  return sections
    .map((section) => {
      if (!section || typeof section !== "object") {
        return null;
      }
      const heading = String(section.heading ?? "").trim();
      const body = String(section.body ?? "").trim();
      const bullets = Array.isArray(section.bullets)
        ? section.bullets.map((bullet) => String(bullet).trim()).filter(Boolean)
        : [];
      if (!heading && !body && bullets.length === 0) {
        return null;
      }
      return { heading, body, bullets };
    })
    .filter(Boolean);
}

export function renderEmailText(draft) {
  const lines = [];

  if (draft.intro) {
    lines.push(draft.intro);
    lines.push("");
  }

  for (const section of draft.sections) {
    if (section.heading) {
      lines.push(section.heading);
    }
    if (section.body) {
      lines.push(section.body);
    }
    for (const bullet of section.bullets) {
      lines.push(`- ${bullet}`);
    }
    lines.push("");
  }

  if (draft.cta) {
    lines.push(draft.cta);
    lines.push("");
  }

  if (draft.closing) {
    lines.push(draft.closing);
  }

  return lines.join("\n").trim();
}

function renderEmailHtml(draft) {
  const sectionMarkup = draft.sections
    .map((section) => {
      const parts = [];
      if (section.heading) {
        parts.push(`<h2>${escapeHtml(section.heading)}</h2>`);
      }
      if (section.body) {
        parts.push(`<p>${escapeHtml(section.body)}</p>`);
      }
      if (section.bullets.length > 0) {
        parts.push(
          `<ul>${section.bullets.map((bullet) => `<li>${escapeHtml(bullet)}</li>`).join("")}</ul>`,
        );
      }
      return parts.join("");
    })
    .join("");

  return [
    "<!doctype html>",
    "<html>",
    "<body>",
    draft.intro ? `<p>${escapeHtml(draft.intro)}</p>` : "",
    sectionMarkup,
    draft.cta ? `<p>${escapeHtml(draft.cta)}</p>` : "",
    draft.closing ? `<p>${escapeHtml(draft.closing).replaceAll("\n", "<br />")}</p>` : "",
    "</body>",
    "</html>",
  ].join("");
}

function draftStructuredEmail({
  to = [],
  cc = [],
  bcc = [],
  subject,
  previewText = "",
  intro = "",
  sections = [],
  cta = "",
  closing = "Thanks,",
  metadata = {},
} = {}) {
  const normalizedSubject = String(subject ?? "").trim();
  if (!normalizedSubject) {
    throw new Error("Structured emails require a subject.");
  }

  const draft = {
    to: normalizeRecipients(to),
    cc: normalizeRecipients(cc),
    bcc: normalizeRecipients(bcc),
    subject: normalizedSubject,
    previewText: String(previewText).trim(),
    intro: String(intro).trim(),
    sections: normalizeSections(sections),
    cta: String(cta).trim(),
    closing: String(closing).trim(),
    metadata: metadata && typeof metadata === "object" ? { ...metadata } : {},
  };

  return {
    ...draft,
    text: renderEmailText(draft),
    html: renderEmailHtml(draft),
  };
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

function normalizeProjectRecipient(recipient) {
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

function draftProjectShareEmail({
  project,
  recipient,
  senderName = "A collaborator",
  closingNote = "",
} = {}) {
  const normalizedProject = normalizeProject(project);
  const normalizedRecipient = normalizeProjectRecipient(recipient);
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

function createProjectShareFollowUpItem({
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
