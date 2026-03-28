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

export function normalizeItem(item) {
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

export function normalizeItems(items = []) {
  if (!Array.isArray(items)) {
    throw new Error("Expected an array of Life Ops items.");
  }
  return items.map((item) => normalizeItem(item));
}

export function getItemTimestamp(item) {
  return item.startsAt ?? item.followUpAt ?? item.dueAt ?? item.endsAt ?? null;
}

export function compareItems(left, right) {
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

export const itemKinds = Object.freeze([...ITEM_KINDS]);
export const itemStatuses = Object.freeze([...ITEM_STATUSES]);
export const priorityLevels = Object.freeze([...PRIORITY_LEVELS]);
