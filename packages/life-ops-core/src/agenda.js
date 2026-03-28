import { compareItems, getItemTimestamp, normalizeItems } from "./model.js";

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
