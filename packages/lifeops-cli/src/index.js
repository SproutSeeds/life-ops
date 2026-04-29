import { mkdir, readFile, readdir, rename, stat, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

import {
  buildProjectSharePacket,
  composeAgenda,
  renderAgendaText,
  renderEmailText,
} from "./core-lite.js";

const packageDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const templateDir = path.join(packageDir, "templates");
const packageJsonPath = path.join(packageDir, "package.json");

function parseArgv(argv = []) {
  const positionals = [];
  const options = {};

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("-")) {
      positionals.push(token);
      continue;
    }

    if (token === "--") {
      positionals.push(...argv.slice(index + 1));
      break;
    }

    if (token === "-h") {
      options.help = true;
      continue;
    }

    if (token === "-v") {
      options.version = true;
      continue;
    }

    if (token.startsWith("--")) {
      const body = token.slice(2);
      const equalsIndex = body.indexOf("=");
      if (equalsIndex >= 0) {
        const key = body.slice(0, equalsIndex);
        options[key] = body.slice(equalsIndex + 1);
        continue;
      }

      const next = argv[index + 1];
      if (next != null && !next.startsWith("-")) {
        options[body] = next;
        index += 1;
      } else {
        options[body] = true;
      }
      continue;
    }

    throw new Error(`Unsupported argument: ${token}`);
  }

  return { positionals, options };
}

function getHelpText() {
  return [
    "Life Ops CLI",
    "",
    "Usage:",
    "  lifeops init [dir] [--force]",
    "  lifeops item add --title \"Write blog post\" [--list professional] [--notes ...] [--priority normal] [--due 2026-04-30] [--format text|json]",
    "  lifeops item list [--list professional] [--status open|done|all] [--format text|json]",
    "  lifeops item done <id> [--format text|json]",
    "  lifeops routine add --name \"Weekly blog post\" --cadence weekly --day Wednesday --time 09:00 [--duration 60] [--notes ...] [--task-title ...] [--format text|json]",
    "  lifeops routine update <id> [--task-title ...] [--task-notes ...] [--task-priority normal] [--task-list professional] [--clear-task]",
    "  lifeops routine list [--format text|json]",
    "  lifeops agenda [--input ./lifeops.items.json] [--days 7] [--timezone America/Chicago] [--format text|json]",
    "  lifeops share --project ./lifeops.project.json --recipients ./lifeops.recipients.json [--sender-name Cody] [--base-time 2026-03-26T00:00:00.000Z] [--format text|json] [--output-dir ./out]",
    "  lifeops cmail <status|start|stop|restart|install|tail|plist|url|open|audit|tailscale|tailscale-status|secure-doctor>",
    "  lifeops version",
    "",
    "Notes:",
    "  `agenda` reads the persistent local Life Ops store when --input is omitted.",
    "  `agenda --input` accepts either a JSON array of items or an object with an `items` array.",
    "  `item` and `routine` are local-first; data lives under LIFE_OPS_HOME or ~/.lifeops.",
    "  Routines with `--task-title` materialize durable task instances and do not duplicate completed weeks.",
    "  `share` creates structured email drafts and follow-up communication items.",
    "  `cmail` is the self-hosted mail surface for Life Ops.",
    "  You bring your own domain, provider accounts, and API keys.",
  ].join("\n");
}

function getCmailHelpText() {
  return [
    "CMAIL",
    "",
    "Usage:",
    "  cmail status",
    "  cmail start",
    "  cmail stop",
    "  cmail restart",
    "  cmail install",
    "  cmail tail",
    "  cmail plist",
    "  cmail url",
    "  cmail open",
    "  cmail audit [--repair] [--strict] [--format text|json]",
    "  cmail tailscale",
    "  cmail tailscale-status",
    "  cmail secure-doctor",
    "  cmail auth-code [--rotate]",
    "  cmail new-draft [--to alex@example.com] [--subject ...] [--body ... | --body-file ./note.txt] [--attach ./file.pdf] [--format text|json]",
    "  cmail drafts [--format text|json]",
    "  cmail draft-save [--id 0] [--to alex@example.com] [--subject ...] [--body ... | --body-file ./note.txt] [--attach ./file.pdf] [--format text|json]",
    "  cmail draft-send --id 74222 [--send-at 2026-04-16T15:00:00Z | --delay-minutes 15] [--format text|json]",
    "  cmail batch-send --ids 74222,74223 [--max-per-hour 5] [--min-gap-minutes 12] [--daily-cap 20] [--dry-run] [--format text|json]",
    "",
    "Notes:",
    "  CMAIL is the self-hosted mail surface for Life Ops.",
    "  It runs as a managed local service on http://127.0.0.1:4311.",
    "  The official private phone URL for this deployment is https://cmail.tail649edd.ts.net.",
    "  Mobile users must be connected to the tailnet in Tailscale before opening CMAIL.",
    "  CMAIL releases are separate from Clawdad releases; publishing Clawdad does not update CMAIL.",
    "  A scheduled watchdog audit runs morning, afternoon, and evening as a fail-safe.",
    "  The extra CMAIL unlock code is opt-in with LIFE_OPS_CMAIL_AUTH_REQUIRED=1.",
    "  `cmail install` bootstraps the bundled Python backend into a local user-owned environment.",
    "  You bring your own domain, Cloudflare/Resend accounts, and API keys.",
    "  Life Ops does not ship any FRG credentials or pay provider costs on your behalf.",
  ].join("\n");
}

function getMissingCmailBackendText() {
  return [
    "CMAIL backend not found.",
    "",
    "This install is missing the bundled self-hosted CMAIL backend payload.",
    "",
    "CMAIL is a bring-your-own-infrastructure feature:",
    "  - your own domain",
    "  - your own Cloudflare setup",
    "  - your own Resend setup",
    "  - your own local secrets and service install",
    "",
    "Reinstall `lifeops` or use a full local Life Ops checkout if this package is incomplete.",
  ].join("\n");
}

function getMissingCmailInstallText() {
  return [
    "CMAIL backend environment not installed.",
    "",
    "Run `cmail install` first to bootstrap the local Python backend and managed service.",
  ].join("\n");
}

async function readJson(jsonPath) {
  try {
    const raw = await readFile(jsonPath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error(`Could not parse JSON at ${jsonPath}: ${error.message}`);
    }
    throw error;
  }
}

function resolveItems(payload) {
  if (Array.isArray(payload)) {
    return payload;
  }
  if (payload && typeof payload === "object" && Array.isArray(payload.items)) {
    return payload.items;
  }
  throw new Error("Expected agenda input JSON to be an array or an object with an `items` array.");
}

function resolveRecipients(payload) {
  if (!Array.isArray(payload) || payload.length === 0) {
    throw new Error("Expected recipients JSON to be a non-empty array.");
  }
  return payload;
}

function asPositiveInteger(value, fallback) {
  if (value == null) {
    return fallback;
  }
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Expected a positive integer, received: ${value}`);
  }
  return parsed;
}

function getLifeOpsHome() {
  return process.env.LIFE_OPS_HOME ?? path.join(os.homedir(), ".lifeops");
}

function resolveStorePath({ options = {}, cwd = process.cwd() } = {}) {
  const explicitPath = options.store ?? options["store-path"] ?? process.env.LIFE_OPS_STORE_PATH;
  if (explicitPath) {
    return path.resolve(cwd, String(explicitPath));
  }
  return path.join(getLifeOpsHome(), "lifeops.store.json");
}

function createEmptyStore() {
  return {
    version: 1,
    nextItemId: 1,
    nextRoutineId: 1,
    items: [],
    routines: [],
  };
}

function normalizeStore(store) {
  const normalized = {
    ...createEmptyStore(),
    ...(store && typeof store === "object" ? store : {}),
  };
  normalized.items = Array.isArray(normalized.items) ? normalized.items : [];
  normalized.routines = Array.isArray(normalized.routines) ? normalized.routines : [];
  normalized.items = normalized.items.map((item) => ({
    ...item,
    metadata: item?.metadata && typeof item.metadata === "object" ? item.metadata : {},
  }));

  const maxItemId = normalized.items.reduce((max, item) => {
    const id = Number.parseInt(String(item.id ?? 0), 10);
    return Number.isInteger(id) ? Math.max(max, id) : max;
  }, 0);
  const maxRoutineId = normalized.routines.reduce((max, routine) => {
    const id = Number.parseInt(String(routine.id ?? 0), 10);
    return Number.isInteger(id) ? Math.max(max, id) : max;
  }, 0);
  normalized.nextItemId = Math.max(
    Number.parseInt(String(normalized.nextItemId ?? 1), 10) || 1,
    maxItemId + 1,
  );
  normalized.nextRoutineId = Math.max(
    Number.parseInt(String(normalized.nextRoutineId ?? 1), 10) || 1,
    maxRoutineId + 1,
  );
  return normalized;
}

async function readPersistentStore(storePath) {
  try {
    return normalizeStore(JSON.parse(await readFile(storePath, "utf8")));
  } catch (error) {
    if (error && error.code === "ENOENT") {
      return createEmptyStore();
    }
    if (error instanceof SyntaxError) {
      throw new Error(`Could not parse Life Ops store at ${storePath}: ${error.message}`);
    }
    throw error;
  }
}

async function writePersistentStore(storePath, store) {
  await mkdir(path.dirname(storePath), { recursive: true });
  const tempPath = `${storePath}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(tempPath, `${JSON.stringify(normalizeStore(store), null, 2)}\n`, "utf8");
  await rename(tempPath, storePath);
}

function getRequiredText(options, keys, label) {
  for (const key of keys) {
    const value = options[key];
    if (value != null && String(value).trim()) {
      return String(value).trim();
    }
  }
  throw new Error(`${label} is required.`);
}

function normalizePriority(value) {
  const priority = String(value ?? "normal").trim();
  if (!["urgent", "high", "normal", "low"].includes(priority)) {
    throw new Error(`Unsupported priority: ${priority}.`);
  }
  return priority;
}

function normalizeItemStatus(value) {
  const status = String(value ?? "open").trim();
  if (!["open", "in_progress", "reference", "done", "ignored", "all"].includes(status)) {
    throw new Error(`Unsupported status: ${status}.`);
  }
  return status;
}

function normalizeCadence(value) {
  const cadence = String(value ?? "").trim().toLowerCase();
  if (!["daily", "weekly"].includes(cadence)) {
    throw new Error("routine add requires --cadence daily|weekly.");
  }
  return cadence;
}

function normalizeTimeValue(value) {
  const normalized = String(value ?? "").trim();
  if (!/^\d{1,2}:\d{2}$/.test(normalized)) {
    throw new Error("Expected time in HH:MM format.");
  }
  const [hour, minute] = normalized.split(":").map((part) => Number.parseInt(part, 10));
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    throw new Error("Expected time in HH:MM format.");
  }
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

const WEEKDAYS = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

function normalizeWeekday(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  const match = WEEKDAYS.find((weekday) => weekday.toLowerCase() === normalized);
  if (!match) {
    throw new Error("Expected --day to be a weekday name.");
  }
  return match;
}

function normalizeDueAt(value) {
  if (value == null || value === "") {
    return null;
  }
  const raw = String(value).trim();
  const date = /^\d{4}-\d{2}-\d{2}$/.test(raw) ? new Date(`${raw}T23:59:00`) : new Date(raw);
  if (Number.isNaN(date.getTime())) {
    throw new Error("Expected --due to be a valid date or datetime.");
  }
  return date.toISOString();
}

function formatStoreItem(item) {
  const prefix = `#${item.id} [${item.status}] ${item.listName}`;
  const occurrence = item.metadata?.occurrenceKey ? ` (${item.metadata.occurrenceKey})` : "";
  return `${prefix} ${item.title}${occurrence}`;
}

function formatRoutine(routine) {
  const day = routine.cadence === "weekly" ? `${routine.day} ` : "";
  const task = routine.taskTitle ? ` -> task: ${routine.taskTitle}` : "";
  return `#${routine.id} [${routine.status}] ${routine.cadence} ${day}${routine.startTime} ${routine.durationMinutes}m ${routine.name}${task}`;
}

const STORE_STATUS_RANK = {
  open: 0,
  in_progress: 1,
  reference: 2,
  done: 3,
  ignored: 4,
};

const STORE_PRIORITY_RANK = {
  urgent: 0,
  high: 1,
  normal: 2,
  low: 3,
};

function compareStoreItems(left, right) {
  const statusDifference =
    (STORE_STATUS_RANK[left.status] ?? 99) - (STORE_STATUS_RANK[right.status] ?? 99);
  if (statusDifference !== 0) {
    return statusDifference;
  }

  const priorityDifference =
    (STORE_PRIORITY_RANK[left.priority] ?? 99) - (STORE_PRIORITY_RANK[right.priority] ?? 99);
  if (priorityDifference !== 0) {
    return priorityDifference;
  }

  if (left.dueAt && right.dueAt) {
    const dueDifference = new Date(left.dueAt).getTime() - new Date(right.dueAt).getTime();
    if (dueDifference !== 0) {
      return dueDifference;
    }
  }
  if (left.dueAt && !right.dueAt) {
    return -1;
  }
  if (!left.dueAt && right.dueAt) {
    return 1;
  }

  const titleDifference = String(left.title).localeCompare(String(right.title));
  if (titleDifference !== 0) {
    return titleDifference;
  }

  return Number(left.id ?? 0) - Number(right.id ?? 0);
}

function getDateKey(date, timeZone) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function getWeekdayName(date, timeZone) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone,
    weekday: "long",
  }).format(date);
}

function getTimeZoneOffsetMs(timeZone, date) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const asUtc = Date.UTC(
    Number(values.year),
    Number(values.month) - 1,
    Number(values.day),
    Number(values.hour),
    Number(values.minute),
    Number(values.second),
  );
  return asUtc - date.getTime();
}

function zonedDateTimeToUtcIso(dateKey, time, timeZone) {
  const [year, month, day] = dateKey.split("-").map((part) => Number.parseInt(part, 10));
  const [hour, minute] = time.split(":").map((part) => Number.parseInt(part, 10));
  const localAsUtc = Date.UTC(year, month - 1, day, hour, minute, 0, 0);
  let utcMs = localAsUtc - getTimeZoneOffsetMs(timeZone, new Date(localAsUtc));
  utcMs = localAsUtc - getTimeZoneOffsetMs(timeZone, new Date(utcMs));
  return new Date(utcMs).toISOString();
}

function addMinutes(isoTimestamp, minutes) {
  return new Date(new Date(isoTimestamp).getTime() + minutes * 60 * 1000).toISOString();
}

function convertStoreItemsToAgendaItems(store) {
  return store.items.map((item) => ({
    id: `lifeops:item:${item.id}`,
    kind: "task",
    title: item.metadata?.occurrenceKey ? `${item.title} (${item.metadata.occurrenceKey})` : item.title,
    summary: item.notes ?? "",
    status: item.status,
    priority: item.priority ?? "normal",
    dueAt: item.dueAt ?? null,
    tags: [item.listName].filter(Boolean),
    source: {
      connector: "lifeops-local-store",
      id: String(item.id),
      account: item.listName ?? null,
    },
    metadata: {
      ...(item.metadata ?? {}),
      listName: item.listName,
      createdAt: item.createdAt,
      updatedAt: item.updatedAt,
      completedAt: item.completedAt ?? null,
    },
  }));
}

function getRoutineOccurrences({ store, now, days, timeZone }) {
  const normalizedNow = now instanceof Date ? now : new Date(now);
  const occurrences = [];
  const dayMs = 24 * 60 * 60 * 1000;

  for (let offset = 0; offset < days; offset += 1) {
    const date = new Date(normalizedNow.getTime() + offset * dayMs);
    const dateKey = getDateKey(date, timeZone);
    const weekday = getWeekdayName(date, timeZone);

    for (const routine of store.routines) {
      if (routine.status !== "active") {
        continue;
      }
      if (routine.cadence === "weekly" && routine.day !== weekday) {
        continue;
      }
      const startsAt = zonedDateTimeToUtcIso(dateKey, routine.startTime, timeZone);
      occurrences.push({
        routine,
        dateKey,
        weekday,
        startsAt,
        endsAt: addMinutes(startsAt, routine.durationMinutes),
      });
    }
  }

  return occurrences;
}

function expandRoutinesToAgendaItems({ store, now, days, timeZone }) {
  return getRoutineOccurrences({ store, now, days, timeZone }).map(
    ({ routine, dateKey, startsAt, endsAt }) => ({
      id: `lifeops:routine:${routine.id}:${dateKey}`,
      kind: "routine",
      title: routine.name,
      summary: routine.notes ?? "",
      status: "open",
      priority: "normal",
      startsAt,
      endsAt,
      source: {
        connector: "lifeops-local-store",
        id: String(routine.id),
        account: "routine",
      },
      metadata: {
        routineId: routine.id,
        cadence: routine.cadence,
        day: routine.day ?? null,
        durationMinutes: routine.durationMinutes,
      },
    }),
  );
}

function routineMaterializesTasks(routine) {
  return Boolean(String(routine.taskTitle ?? "").trim());
}

function getRoutineTaskNotes(routine, dateKey) {
  const baseNotes = String(routine.taskNotes ?? routine.notes ?? "").trim();
  const occurrenceLine = `Routine occurrence: ${routine.name} on ${dateKey}.`;
  return baseNotes ? `${baseNotes}\n\n${occurrenceLine}` : occurrenceLine;
}

function findMaterializedTask(store, routine, dateKey) {
  return store.items.find(
    (item) =>
      String(item.metadata?.routineId ?? "") === String(routine.id) &&
      item.metadata?.occurrenceKey === dateKey,
  );
}

function findAdoptableTask(store, routine) {
  return store.items.find(
    (item) =>
      item.status !== "done" &&
      item.status !== "ignored" &&
      !item.metadata?.routineId &&
      item.title === routine.taskTitle,
  );
}

function syncOpenMaterializedTask(item, routine) {
  if (item.status === "done" || item.status === "ignored") {
    return false;
  }

  let changed = false;
  if (routine.taskTitle != null) {
    const title = String(routine.taskTitle).trim();
    if (title && item.title !== title) {
      item.title = title;
      changed = true;
    }
  }
  if (routine.taskPriority != null) {
    const priority = normalizePriority(routine.taskPriority);
    if (item.priority !== priority) {
      item.priority = priority;
      changed = true;
    }
  }
  if (routine.taskList != null) {
    const listName = String(routine.taskList).trim() || "professional";
    if (item.listName !== listName) {
      item.listName = listName;
      changed = true;
    }
  }
  if (!item.notes && (routine.taskNotes || routine.notes)) {
    item.notes = String(routine.taskNotes ?? routine.notes ?? "").trim();
    changed = true;
  }

  if (changed) {
    item.updatedAt = new Date().toISOString();
  }
  return changed;
}

function materializeRoutineTasks({ store, now, days, timeZone }) {
  const nowIso = new Date().toISOString();
  const created = [];
  const adopted = [];
  const updated = [];

  for (const { routine, dateKey, startsAt, endsAt } of getRoutineOccurrences({
    store,
    now,
    days,
    timeZone,
  })) {
    if (!routineMaterializesTasks(routine)) {
      continue;
    }
    const existingTask = findMaterializedTask(store, routine, dateKey);
    if (existingTask) {
      if (syncOpenMaterializedTask(existingTask, routine)) {
        updated.push(existingTask);
      }
      continue;
    }

    const metadata = {
      routineId: routine.id,
      routineName: routine.name,
      occurrenceKey: dateKey,
      occurrenceStartsAt: startsAt,
      occurrenceEndsAt: endsAt,
      materializedAt: nowIso,
    };
    const adoptableTask = findAdoptableTask(store, routine);
    if (adoptableTask) {
      adoptableTask.metadata = {
        ...(adoptableTask.metadata ?? {}),
        ...metadata,
      };
      adoptableTask.notes = adoptableTask.notes || getRoutineTaskNotes(routine, dateKey);
      adoptableTask.priority = normalizePriority(routine.taskPriority ?? adoptableTask.priority ?? "normal");
      adoptableTask.listName = String(routine.taskList ?? adoptableTask.listName ?? "professional").trim() || "professional";
      adoptableTask.updatedAt = nowIso;
      adopted.push(adoptableTask);
      continue;
    }

    const item = {
      id: store.nextItemId,
      listName: String(routine.taskList ?? "professional").trim() || "professional",
      title: String(routine.taskTitle).trim(),
      notes: getRoutineTaskNotes(routine, dateKey),
      status: "open",
      priority: normalizePriority(routine.taskPriority ?? "normal"),
      dueAt: null,
      createdAt: nowIso,
      updatedAt: nowIso,
      completedAt: null,
      metadata,
    };
    store.nextItemId += 1;
    store.items.push(item);
    created.push(item);
  }

  return { created, adopted, updated };
}

function resolveAgendaNow(options) {
  if (options.start) {
    return new Date(`${String(options.start)}T00:00:00`);
  }
  return options.now ?? new Date();
}

async function loadPersistentAgendaItems({ options, cwd, days, timeZone, now }) {
  const storePath = resolveStorePath({ options, cwd });
  const store = await readPersistentStore(storePath);
  const { created, adopted, updated } = materializeRoutineTasks({
    store,
    now,
    days,
    timeZone,
  });
  if (created.length > 0 || adopted.length > 0 || updated.length > 0) {
    await writePersistentStore(storePath, store);
  }
  return [
    ...convertStoreItemsToAgendaItems(store),
    ...expandRoutinesToAgendaItems({
      store,
      now,
      days,
      timeZone,
    }),
  ];
}

function formatSharePacketText(packet) {
  const lines = [
    `Project: ${packet.project.name}`,
    `Summary: ${packet.project.summary}`,
    "",
    "Drafts",
  ];

  for (let index = 0; index < packet.drafts.length; index += 1) {
    const draft = packet.drafts[index];
    const recipient = draft.recipient.name
      ? `${draft.recipient.name} <${draft.recipient.email}>`
      : draft.recipient.email;
    lines.push("");
    lines.push(`=== Draft ${index + 1}: ${recipient} ===`);
    lines.push(renderEmailText(draft));
  }

  lines.push("");
  lines.push("Follow Ups");
  for (const followUp of packet.followUps) {
    const when = followUp.followUpAt ?? followUp.dueAt ?? "unscheduled";
    lines.push(`- ${followUp.title} (${when})`);
  }

  return lines.join("\n");
}

function sanitizeFilePart(value, fallback) {
  const normalized = String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized || fallback;
}

async function writeShareOutputs(outputDir, packet) {
  await mkdir(outputDir, { recursive: true });
  const packetPath = path.join(outputDir, "packet.json");
  await writeFile(packetPath, `${JSON.stringify(packet, null, 2)}\n`, "utf8");

  for (let index = 0; index < packet.drafts.length; index += 1) {
    const draft = packet.drafts[index];
    const stem = `${String(index + 1).padStart(2, "0")}-${sanitizeFilePart(
      draft.recipient.name ?? draft.recipient.email,
      `recipient-${index + 1}`,
    )}`;
    await writeFile(path.join(outputDir, `${stem}.txt`), `${renderEmailText(draft)}\n`, "utf8");
    await writeFile(path.join(outputDir, `${stem}.html`), `${draft.html}\n`, "utf8");
  }

  return {
    outputDir,
    packetPath,
    files: await readdir(outputDir),
  };
}

async function runInitCommand({ targetDir, force = false, io }) {
  await mkdir(targetDir, { recursive: true });

  const templateNames = await readdir(templateDir);
  const created = [];
  for (const templateName of templateNames) {
    const sourcePath = path.join(templateDir, templateName);
    const destinationPath = path.join(targetDir, templateName);
    const destinationExists = await stat(destinationPath).then(() => true).catch(() => false);
    if (destinationExists && !force) {
      throw new Error(`Refusing to overwrite ${destinationPath}. Re-run with --force if you want to replace it.`);
    }
    const contents = await readFile(sourcePath, "utf8");
    await writeFile(destinationPath, contents, "utf8");
    created.push(destinationPath);
  }

  io.stdout.write(["Created Life Ops starter files:", ...created.map((item) => `- ${item}`)].join("\n") + "\n");
  return 0;
}

async function runAgendaCommand({ options, io, cwd }) {
  const days = asPositiveInteger(options.days, 7);
  const timeZone = options.timezone ?? options.timeZone ?? "UTC";
  const now = resolveAgendaNow(options);
  let items;

  if (options.input) {
    const inputPath = path.resolve(cwd, String(options.input));
    const payload = await readJson(inputPath);
    items = resolveItems(payload);
  } else {
    items = await loadPersistentAgendaItems({
      options,
      cwd,
      days,
      timeZone,
      now,
    });
  }

  const agenda = composeAgenda({
    items,
    now,
    days,
    timeZone,
  });

  if ((options.format ?? "text") === "json") {
    io.stdout.write(`${JSON.stringify(agenda, null, 2)}\n`);
    return 0;
  }

  io.stdout.write(`${renderAgendaText(agenda)}\n`);
  return 0;
}

async function runItemCommand({ subcommand, options, positionals, io, cwd }) {
  const storePath = resolveStorePath({ options, cwd });
  const store = await readPersistentStore(storePath);
  const format = options.format ?? "text";

  if (subcommand === "add") {
    const now = new Date().toISOString();
    const item = {
      id: store.nextItemId,
      listName: String(options.list ?? "professional").trim() || "professional",
      title: getRequiredText(options, ["title"], "item add --title"),
      notes: String(options.notes ?? "").trim(),
      status: "open",
      priority: normalizePriority(options.priority),
      dueAt: normalizeDueAt(options.due),
      createdAt: now,
      updatedAt: now,
      completedAt: null,
    };
    store.nextItemId += 1;
    store.items.push(item);
    await writePersistentStore(storePath, store);

    if (format === "json") {
      io.stdout.write(`${JSON.stringify({ item, storePath }, null, 2)}\n`);
    } else {
      io.stdout.write(`Added ${item.listName} item #${item.id}: ${item.title}\n`);
    }
    return 0;
  }

  if (subcommand === "list") {
    const status = normalizeItemStatus(options.status ?? "open");
    const listName = options.list == null ? null : String(options.list).trim();
    const items = store.items.filter((item) => {
      if (listName && item.listName !== listName) {
        return false;
      }
      if (status !== "all" && item.status !== status) {
        return false;
      }
      return true;
    }).sort(compareStoreItems);

    if (format === "json") {
      io.stdout.write(`${JSON.stringify(items, null, 2)}\n`);
    } else if (items.length === 0) {
      io.stdout.write("No matching Life Ops items.\n");
    } else {
      io.stdout.write(`${items.map(formatStoreItem).join("\n")}\n`);
    }
    return 0;
  }

  if (subcommand === "done") {
    const rawId = String(options.id ?? positionals[2] ?? "").replace(/^#/, "").trim();
    if (!rawId) {
      throw new Error("item done requires an item id.");
    }
    const item = store.items.find((candidate) => String(candidate.id) === rawId);
    if (!item) {
      throw new Error(`No Life Ops item found for id ${rawId}.`);
    }
    const now = new Date().toISOString();
    item.status = "done";
    item.updatedAt = now;
    item.completedAt = now;
    await writePersistentStore(storePath, store);

    if (format === "json") {
      io.stdout.write(`${JSON.stringify({ item, storePath }, null, 2)}\n`);
    } else {
      io.stdout.write(`Marked item #${item.id} done: ${item.title}\n`);
    }
    return 0;
  }

  throw new Error(`Unknown item command: ${subcommand}`);
}

async function runRoutineCommand({ subcommand, options, positionals = [], io, cwd }) {
  const storePath = resolveStorePath({ options, cwd });
  const store = await readPersistentStore(storePath);
  const format = options.format ?? "text";

  if (subcommand === "add") {
    const cadence = normalizeCadence(options.cadence);
    const startTime = normalizeTimeValue(options.time ?? options["start-time"] ?? options.startTime);
    const durationMinutes = asPositiveInteger(options.duration ?? options["duration-minutes"], 60);
    const taskTitle = options["task-title"] ?? options.taskTitle;
    const now = new Date().toISOString();
    const routine = {
      id: store.nextRoutineId,
      name: getRequiredText(options, ["name"], "routine add --name"),
      cadence,
      day: cadence === "weekly" ? normalizeWeekday(options.day) : null,
      startTime,
      durationMinutes,
      notes: String(options.notes ?? "").trim(),
      status: "active",
      createdAt: now,
      updatedAt: now,
    };
    if (taskTitle != null && String(taskTitle).trim()) {
      routine.taskTitle = String(taskTitle).trim();
      routine.taskNotes = String(options["task-notes"] ?? options.taskNotes ?? options.notes ?? "").trim();
      routine.taskPriority = normalizePriority(options["task-priority"] ?? options.taskPriority);
      routine.taskList = String(options["task-list"] ?? options.taskList ?? "professional").trim() || "professional";
    }
    store.nextRoutineId += 1;
    store.routines.push(routine);
    await writePersistentStore(storePath, store);

    if (format === "json") {
      io.stdout.write(`${JSON.stringify({ routine, storePath }, null, 2)}\n`);
    } else {
      io.stdout.write(`Added routine #${routine.id}: ${routine.name}\n`);
    }
    return 0;
  }

  if (subcommand === "update") {
    const id = String(options.id ?? options.routine ?? options.routineId ?? positionals[2] ?? "")
      .replace(/^#/, "")
      .trim();
    if (!id) {
      throw new Error("routine update requires a routine id.");
    }
    const routine = store.routines.find((candidate) => String(candidate.id) === id);
    if (!routine) {
      throw new Error(`No Life Ops routine found for id ${id}.`);
    }

    if (options.name != null) {
      routine.name = String(options.name).trim() || routine.name;
    }
    if (options.cadence != null) {
      routine.cadence = normalizeCadence(options.cadence);
    }
    if (options.day != null) {
      routine.day = routine.cadence === "weekly" ? normalizeWeekday(options.day) : null;
    }
    if (options.time != null || options["start-time"] != null || options.startTime != null) {
      routine.startTime = normalizeTimeValue(options.time ?? options["start-time"] ?? options.startTime);
    }
    if (options.duration != null || options["duration-minutes"] != null) {
      routine.durationMinutes = asPositiveInteger(options.duration ?? options["duration-minutes"], routine.durationMinutes);
    }
    if (options.notes != null) {
      routine.notes = String(options.notes).trim();
    }

    if (options["clear-task"]) {
      delete routine.taskTitle;
      delete routine.taskNotes;
      delete routine.taskPriority;
      delete routine.taskList;
    } else if (
      options["task-title"] != null ||
      options.taskTitle != null ||
      options["task-notes"] != null ||
      options.taskNotes != null ||
      options["task-priority"] != null ||
      options.taskPriority != null ||
      options["task-list"] != null ||
      options.taskList != null
    ) {
      const taskTitle = options["task-title"] ?? options.taskTitle ?? routine.taskTitle;
      if (taskTitle == null || !String(taskTitle).trim()) {
        throw new Error("routine update task materialization requires --task-title.");
      }
      routine.taskTitle = String(taskTitle).trim();
      routine.taskNotes = String(options["task-notes"] ?? options.taskNotes ?? routine.taskNotes ?? routine.notes ?? "").trim();
      routine.taskPriority = normalizePriority(options["task-priority"] ?? options.taskPriority ?? routine.taskPriority);
      routine.taskList = String(options["task-list"] ?? options.taskList ?? routine.taskList ?? "professional").trim() || "professional";
    }

    routine.updatedAt = new Date().toISOString();
    await writePersistentStore(storePath, store);

    if (format === "json") {
      io.stdout.write(`${JSON.stringify({ routine, storePath }, null, 2)}\n`);
    } else {
      io.stdout.write(`Updated routine #${routine.id}: ${routine.name}\n`);
    }
    return 0;
  }

  if (subcommand === "list") {
    const status = String(options.status ?? "active").trim();
    const routines = store.routines.filter((routine) => status === "all" || routine.status === status);

    if (format === "json") {
      io.stdout.write(`${JSON.stringify(routines, null, 2)}\n`);
    } else if (routines.length === 0) {
      io.stdout.write("No matching Life Ops routines.\n");
    } else {
      io.stdout.write(`${routines.map(formatRoutine).join("\n")}\n`);
    }
    return 0;
  }

  throw new Error(`Unknown routine command: ${subcommand}`);
}

async function runShareCommand({ options, io, cwd }) {
  if (!options.project) {
    throw new Error("share requires --project <path>.");
  }
  if (!options.recipients) {
    throw new Error("share requires --recipients <path>.");
  }

  const project = await readJson(path.resolve(cwd, String(options.project)));
  const recipients = resolveRecipients(await readJson(path.resolve(cwd, String(options.recipients))));
  const packet = buildProjectSharePacket({
    project,
    recipients,
    senderName: String(options["sender-name"] ?? options.senderName ?? "A collaborator"),
    baseTime: options["base-time"] ?? options.baseTime,
  });

  let writeSummary = null;
  if (options["output-dir"] || options.outputDir) {
    writeSummary = await writeShareOutputs(
      path.resolve(cwd, String(options["output-dir"] ?? options.outputDir)),
      packet,
    );
  }

  if ((options.format ?? "text") === "json") {
    io.stdout.write(`${JSON.stringify({ packet, writeSummary }, null, 2)}\n`);
    return 0;
  }

  io.stdout.write(`${formatSharePacketText(packet)}\n`);
  if (writeSummary) {
    io.stdout.write(
      `\nWrote ${writeSummary.files.length} files to ${writeSummary.outputDir}\n`,
    );
  }
  return 0;
}

async function defaultProcessRunner({ command, args, cwd, io, env }) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env: env ?? process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout.on("data", (chunk) => {
      io.stdout.write(String(chunk));
    });
    child.stderr.on("data", (chunk) => {
      io.stderr.write(String(chunk));
    });
    child.on("error", reject);
    child.on("close", (code, signal) => {
      if (signal) {
        resolve(1);
        return;
      }
      resolve(code ?? 0);
    });
  });
}

function resolveCmailPaths() {
  const stateRoot = process.env.LIFE_OPS_HOME ?? path.join(os.homedir(), ".lifeops");
  const repoRoot = path.resolve(packageDir, "..", "..");
  const packagedBackendDir = path.join(packageDir, "backend");
  const packagedBackendPython = path.join(stateRoot, "venvs", "cmail", "bin", "python");
  const repoBackendPython = path.join(repoRoot, ".venv", "bin", "python");
  return {
    stateRoot,
    serviceScript: path.join(packageDir, "bin", "cmail-service"),
    packagedBackendDir,
    packagedBackendPython,
    repoRoot,
    repoBackendPython,
  };
}

async function runCmailCommand({
  argv = [],
  invokedFromLifeops = false,
  io,
  runner = defaultProcessRunner,
}) {
  const cmailArgv = invokedFromLifeops ? argv.slice(1) : argv;
  const subcommand = cmailArgv[0] ?? "help";
  const serviceScript = path.join(packageDir, "bin", "cmail-service");
  const mailboxUrl = "http://127.0.0.1:4311";
  const {
    stateRoot,
    packagedBackendDir,
    packagedBackendPython,
    repoRoot,
    repoBackendPython,
  } = resolveCmailPaths();

  if (subcommand === "help" || subcommand === "--help" || subcommand === "-h") {
    io.stdout.write(`${getCmailHelpText()}\n`);
    return 0;
  }

  if (subcommand === "url") {
    io.stdout.write(`${mailboxUrl}\n`);
    return 0;
  }

  if (subcommand === "open") {
    return runner({
      command: "open",
      args: [mailboxUrl],
      cwd: packageDir,
      io,
    });
  }

  try {
    const details = await stat(serviceScript);
    if (!details.isFile()) {
      throw new Error("not a file");
    }
  } catch {
    throw new Error(getMissingCmailBackendText());
  }

  if (
    [
      "status",
      "start",
      "stop",
      "restart",
      "install",
      "tail",
      "plist",
      "audit",
      "tailscale",
      "tailscale-status",
      "secure-doctor",
      "auth-code",
    ].includes(subcommand)
  ) {
    return runner({
      command: "zsh",
      args: ["./bin/cmail-service", subcommand, ...cmailArgv.slice(1)],
      cwd: packageDir,
      io,
    });
  }

  const backendCommands = {
    "new-draft": "cmail-draft-save",
    drafts: "cmail-drafts",
    "draft-save": "cmail-draft-save",
    "draft-send": "cmail-draft-send",
    "batch-send": "cmail-batch-send",
  };
  const backendCommand = backendCommands[subcommand];
  if (!backendCommand) {
    throw new Error(`Unknown CMAIL command: ${subcommand}`);
  }

  let backendPython = packagedBackendPython;
  let backendRoot = packagedBackendDir;
  let backendEnv = {
    ...process.env,
    LIFE_OPS_HOME: stateRoot,
    LIFE_OPS_PACKAGE_ROOT: packagedBackendDir,
  };
  try {
    const details = await stat(packagedBackendPython);
    if (!details.isFile()) {
      throw new Error("not a file");
    }
  } catch {
    try {
      const repoPythonDetails = await stat(repoBackendPython);
      const repoSrcDetails = await stat(path.join(repoRoot, "src", "life_ops"));
      if (!repoPythonDetails.isFile() || !repoSrcDetails.isDirectory()) {
        throw new Error("repo fallback unavailable");
      }
      backendPython = repoBackendPython;
      backendRoot = repoRoot;
      backendEnv = {
        ...process.env,
        LIFE_OPS_PACKAGE_ROOT: repoRoot,
        PYTHONPATH: path.join(repoRoot, "src"),
      };
    } catch {
      throw new Error(getMissingCmailInstallText());
    }
  }

  return runner({
    command: backendPython,
    args: ["-m", "life_ops", backendCommand, ...cmailArgv.slice(1)],
    cwd: packageDir,
    io,
    env: {
      ...backendEnv,
      LIFE_OPS_PACKAGE_ROOT: backendRoot,
    },
  });
}

export async function runCli(
  argv = [],
  io = {
    stdout: process.stdout,
    stderr: process.stderr,
  },
  runtime = {},
) {
  try {
    const { positionals, options } = parseArgv(argv);
    const command = positionals[0] ?? (options.version ? "version" : "help");
    const cwd = process.cwd();

    if (command === "cmail" && options.help) {
      return runCmailCommand({
        argv: ["cmail", "help"],
        invokedFromLifeops: true,
        io,
        runner: runtime.runner,
      });
    }

    if (options.help || command === "help") {
      io.stdout.write(`${getHelpText()}\n`);
      return 0;
    }

    if (command === "version") {
      const packageMeta = await readJson(packageJsonPath);
      io.stdout.write(`lifeops ${packageMeta?.version ?? "unknown"}\n`);
      return 0;
    }

    if (command === "init") {
      const targetDir = path.resolve(cwd, positionals[1] ?? ".");
      return runInitCommand({
        targetDir,
        force: Boolean(options.force),
        io,
      });
    }

    if (command === "item") {
      return runItemCommand({
        subcommand: positionals[1] ?? "list",
        options,
        positionals,
        io,
        cwd,
      });
    }

    if (command === "add-item") {
      return runItemCommand({
        subcommand: "add",
        options,
        positionals: ["item", "add", ...positionals.slice(1)],
        io,
        cwd,
      });
    }

    if (command === "list-items") {
      return runItemCommand({
        subcommand: "list",
        options,
        positionals: ["item", "list", ...positionals.slice(1)],
        io,
        cwd,
      });
    }

    if (command === "done-item") {
      return runItemCommand({
        subcommand: "done",
        options,
        positionals: ["item", "done", ...positionals.slice(1)],
        io,
        cwd,
      });
    }

    if (command === "routine") {
      return runRoutineCommand({
        subcommand: positionals[1] ?? "list",
        options,
        positionals,
        io,
        cwd,
      });
    }

    if (command === "add-routine") {
      return runRoutineCommand({
        subcommand: "add",
        options,
        positionals: ["routine", "add", ...positionals.slice(1)],
        io,
        cwd,
      });
    }

    if (command === "list-routines") {
      return runRoutineCommand({
        subcommand: "list",
        options,
        positionals: ["routine", "list", ...positionals.slice(1)],
        io,
        cwd,
      });
    }

    if (command === "agenda") {
      return runAgendaCommand({ options, io, cwd });
    }

    if (command === "share") {
      return runShareCommand({ options, io, cwd });
    }

    if (command === "cmail") {
      return runCmailCommand({
        argv,
        invokedFromLifeops: true,
        io,
        runner: runtime.runner,
      });
    }

    throw new Error(`Unknown command: ${command}`);
  } catch (error) {
    io.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }
}

export async function runCmailCli(
  argv = [],
  io = {
    stdout: process.stdout,
    stderr: process.stderr,
  },
  runtime = {},
) {
  return runCmailCommand({
    argv,
    io,
    runner: runtime.runner,
  });
}
