import { mkdir, readFile, readdir, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

import {
  buildProjectSharePacket,
  composeAgenda,
  renderAgendaText,
  renderEmailText,
} from "@lifeops/core";

const packageDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const templateDir = path.join(packageDir, "templates");

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
    "  lifeops agenda --input ./lifeops.items.json [--days 7] [--timezone America/Chicago] [--format text|json]",
    "  lifeops share --project ./lifeops.project.json --recipients ./lifeops.recipients.json [--sender-name Cody] [--base-time 2026-03-26T00:00:00.000Z] [--format text|json] [--output-dir ./out]",
    "  lifeops version",
    "",
    "Notes:",
    "  `agenda` accepts either a JSON array of items or an object with an `items` array.",
    "  `share` creates structured email drafts and follow-up communication items.",
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
  if (!options.input) {
    throw new Error("agenda requires --input <path>.");
  }
  const inputPath = path.resolve(cwd, String(options.input));
  const payload = await readJson(inputPath);
  const agenda = composeAgenda({
    items: resolveItems(payload),
    now: options.now,
    days: asPositiveInteger(options.days, 7),
    timeZone: options.timezone ?? options.timeZone ?? "UTC",
  });

  if ((options.format ?? "text") === "json") {
    io.stdout.write(`${JSON.stringify(agenda, null, 2)}\n`);
    return 0;
  }

  io.stdout.write(`${renderAgendaText(agenda)}\n`);
  return 0;
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

export async function runCli(
  argv = [],
  io = {
    stdout: process.stdout,
    stderr: process.stderr,
  },
) {
  try {
    const { positionals, options } = parseArgv(argv);
    const command = positionals[0] ?? (options.version ? "version" : "help");
    const cwd = process.cwd();

    if (options.help || command === "help") {
      io.stdout.write(`${getHelpText()}\n`);
      return 0;
    }

    if (command === "version") {
      io.stdout.write("lifeops 0.1.0\n");
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

    if (command === "agenda") {
      return runAgendaCommand({ options, io, cwd });
    }

    if (command === "share") {
      return runShareCommand({ options, io, cwd });
    }

    throw new Error(`Unknown command: ${command}`);
  } catch (error) {
    io.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  }
}
