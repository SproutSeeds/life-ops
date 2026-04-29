import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";

import { runCli } from "../src/index.js";

function createIo() {
  let stdout = "";
  let stderr = "";
  return {
    io: {
      stdout: {
        write(chunk) {
          stdout += String(chunk);
        },
      },
      stderr: {
        write(chunk) {
          stderr += String(chunk);
        },
      },
    },
    getStdout() {
      return stdout;
    },
    getStderr() {
      return stderr;
    },
  };
}

async function createFakeCmailHome() {
  const home = await mkdtemp(path.join(os.tmpdir(), "lifeops-cmail-home-"));
  const binDir = path.join(home, "venvs", "cmail", "bin");
  await mkdir(binDir, { recursive: true });
  await writeFile(path.join(binDir, "python"), "", "utf8");
  return home;
}

test("lifeops init scaffolds starter files", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "lifeops-cli-init-"));
  const { io, getStdout, getStderr } = createIo();

  const originalCwd = process.cwd();
  process.chdir(tempDir);
  try {
    const exitCode = await runCli(["init"], io);
    assert.equal(exitCode, 0);
    assert.equal(getStderr(), "");
    assert.match(getStdout(), /Created Life Ops starter files/);

    const items = JSON.parse(await readFile(path.join(tempDir, "lifeops.items.json"), "utf8"));
    assert.equal(Array.isArray(items), true);
  } finally {
    process.chdir(originalCwd);
  }
});

test("lifeops agenda renders an agenda from JSON input", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "lifeops-cli-agenda-"));
  const { io, getStdout, getStderr } = createIo();

  const originalCwd = process.cwd();
  process.chdir(tempDir);
  try {
    await runCli(["init"], createIo().io);
    const exitCode = await runCli(
      [
        "agenda",
        "--input",
        "./lifeops.items.json",
        "--now",
        "2026-03-26T00:00:00.000Z",
        "--days",
        "7",
        "--timezone",
        "UTC",
      ],
      io,
    );

    assert.equal(exitCode, 0);
    assert.equal(getStderr(), "");
    assert.match(getStdout(), /Founder sync/);
    assert.match(getStdout(), /Follow up on Life Ops pilot outreach/);
  } finally {
    process.chdir(originalCwd);
  }
});

test("lifeops item commands persist local tasks until explicitly completed", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "lifeops-cli-items-"));
  const storePath = path.join(tempDir, "lifeops.store.json");

  const add = createIo();
  const addExitCode = await runCli(
    [
      "item",
      "add",
      "--store",
      storePath,
      "--list",
      "professional",
      "--title",
      "Write and publish weekly FRG blog post",
      "--notes",
      "Do not mark done until verified live.",
      "--format",
      "json",
    ],
    add.io,
  );
  assert.equal(addExitCode, 0);
  assert.equal(add.getStderr(), "");
  const added = JSON.parse(add.getStdout());
  assert.equal(added.item.id, 1);
  assert.equal(added.item.status, "open");

  const listOpen = createIo();
  const listOpenExitCode = await runCli(
    ["item", "list", "--store", storePath, "--list", "professional", "--format", "json"],
    listOpen.io,
  );
  assert.equal(listOpenExitCode, 0);
  assert.equal(listOpen.getStderr(), "");
  const openItems = JSON.parse(listOpen.getStdout());
  assert.equal(openItems.length, 1);
  assert.equal(openItems[0].title, "Write and publish weekly FRG blog post");

  const done = createIo();
  const doneExitCode = await runCli(["item", "done", "1", "--store", storePath, "--format", "json"], done.io);
  assert.equal(doneExitCode, 0);
  assert.equal(done.getStderr(), "");
  const completed = JSON.parse(done.getStdout());
  assert.equal(completed.item.status, "done");
  assert.equal(typeof completed.item.completedAt, "string");

  const listAfterDone = createIo();
  const listAfterDoneExitCode = await runCli(
    ["item", "list", "--store", storePath, "--status", "open", "--format", "json"],
    listAfterDone.io,
  );
  assert.equal(listAfterDoneExitCode, 0);
  assert.equal(JSON.parse(listAfterDone.getStdout()).length, 0);
});

test("lifeops routine commands feed persistent agenda output", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "lifeops-cli-routines-"));
  const storePath = path.join(tempDir, "lifeops.store.json");

  const itemAdd = createIo();
  const itemExitCode = await runCli(
    [
      "item",
      "add",
      "--store",
      storePath,
      "--title",
      "Write and publish weekly FRG blog post",
      "--notes",
      "Persistent publishing checkpoint.",
    ],
    itemAdd.io,
  );
  assert.equal(itemExitCode, 0);
  assert.equal(itemAdd.getStderr(), "");

  const routineAdd = createIo();
  const routineExitCode = await runCli(
    [
      "routine",
      "add",
      "--store",
      storePath,
      "--name",
      "Weekly FRG blog post",
      "--cadence",
      "weekly",
      "--day",
      "Wednesday",
      "--time",
      "09:00",
      "--duration",
      "60",
    ],
    routineAdd.io,
  );
  assert.equal(routineExitCode, 0);
  assert.equal(routineAdd.getStderr(), "");

  const agenda = createIo();
  const agendaExitCode = await runCli(
    [
      "agenda",
      "--store",
      storePath,
      "--start",
      "2026-04-29",
      "--days",
      "8",
      "--timezone",
      "UTC",
      "--format",
      "json",
    ],
    agenda.io,
  );
  assert.equal(agendaExitCode, 0);
  assert.equal(agenda.getStderr(), "");

  const payload = JSON.parse(agenda.getStdout());
  assert.equal(payload.floatingItems.some((item) => item.title === "Write and publish weekly FRG blog post"), true);
  const scheduledRoutineTitles = payload.days.flatMap((day) => day.items.map((item) => item.title));
  assert.equal(scheduledRoutineTitles.filter((title) => title === "Weekly FRG blog post").length, 2);
});

test("lifeops routines materialize weekly task instances without duplicates", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "lifeops-cli-materialize-"));
  const storePath = path.join(tempDir, "lifeops.store.json");

  const routineAdd = createIo();
  const routineExitCode = await runCli(
    [
      "routine",
      "add",
      "--store",
      storePath,
      "--name",
      "Weekly FRG blog post",
      "--cadence",
      "weekly",
      "--day",
      "Wednesday",
      "--time",
      "09:00",
      "--duration",
      "60",
      "--task-title",
      "Write and publish weekly FRG blog post",
      "--task-priority",
      "high",
    ],
    routineAdd.io,
  );
  assert.equal(routineExitCode, 0);
  assert.equal(routineAdd.getStderr(), "");

  for (let index = 0; index < 2; index += 1) {
    const agenda = createIo();
    const agendaExitCode = await runCli(
      [
        "agenda",
        "--store",
        storePath,
        "--start",
        "2026-04-29",
        "--days",
        "8",
        "--timezone",
        "UTC",
        "--format",
        "json",
      ],
      agenda.io,
    );
    assert.equal(agendaExitCode, 0);
    assert.equal(agenda.getStderr(), "");
  }

  const listAll = createIo();
  const listAllExitCode = await runCli(["item", "list", "--store", storePath, "--status", "all", "--format", "json"], listAll.io);
  assert.equal(listAllExitCode, 0);
  assert.equal(listAll.getStderr(), "");
  const items = JSON.parse(listAll.getStdout());
  assert.equal(items.length, 2);
  assert.deepEqual(items.map((item) => item.metadata.occurrenceKey), ["2026-04-29", "2026-05-06"]);
  assert.equal(items.every((item) => item.priority === "high"), true);

  const done = createIo();
  const doneExitCode = await runCli(["item", "done", "1", "--store", storePath], done.io);
  assert.equal(doneExitCode, 0);
  assert.equal(done.getStderr(), "");

  const agendaAfterDone = createIo();
  const agendaAfterDoneExitCode = await runCli(
    [
      "agenda",
      "--store",
      storePath,
      "--start",
      "2026-04-29",
      "--days",
      "8",
      "--timezone",
      "UTC",
      "--format",
      "json",
    ],
    agendaAfterDone.io,
  );
  assert.equal(agendaAfterDoneExitCode, 0);
  assert.equal(agendaAfterDone.getStderr(), "");

  const finalList = createIo();
  const finalListExitCode = await runCli(["item", "list", "--store", storePath, "--status", "all", "--format", "json"], finalList.io);
  assert.equal(finalListExitCode, 0);
  const finalItems = JSON.parse(finalList.getStdout());
  assert.equal(finalItems.length, 2);
  assert.equal(finalItems.filter((item) => item.status === "done").length, 1);
  assert.equal(finalItems.filter((item) => item.status === "open").length, 1);
});

test("lifeops agenda adopts an existing open task when connecting a routine template", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "lifeops-cli-adopt-task-"));
  const storePath = path.join(tempDir, "lifeops.store.json");

  const itemAdd = createIo();
  const itemExitCode = await runCli(
    [
      "item",
      "add",
      "--store",
      storePath,
      "--title",
      "Write and publish weekly FRG blog post",
      "--notes",
      "Already on the list.",
    ],
    itemAdd.io,
  );
  assert.equal(itemExitCode, 0);
  assert.equal(itemAdd.getStderr(), "");

  const routineAdd = createIo();
  const routineExitCode = await runCli(
    [
      "routine",
      "add",
      "--store",
      storePath,
      "--name",
      "Weekly FRG blog post",
      "--cadence",
      "weekly",
      "--day",
      "Wednesday",
      "--time",
      "09:00",
      "--task-title",
      "Write and publish weekly FRG blog post",
      "--task-priority",
      "high",
    ],
    routineAdd.io,
  );
  assert.equal(routineExitCode, 0);
  assert.equal(routineAdd.getStderr(), "");

  const agenda = createIo();
  const agendaExitCode = await runCli(
    [
      "agenda",
      "--store",
      storePath,
      "--start",
      "2026-04-29",
      "--days",
      "1",
      "--timezone",
      "UTC",
      "--format",
      "json",
    ],
    agenda.io,
  );
  assert.equal(agendaExitCode, 0);
  assert.equal(agenda.getStderr(), "");

  const listAll = createIo();
  const listAllExitCode = await runCli(["item", "list", "--store", storePath, "--status", "all", "--format", "json"], listAll.io);
  assert.equal(listAllExitCode, 0);
  const items = JSON.parse(listAll.getStdout());
  assert.equal(items.length, 1);
  assert.equal(items[0].metadata.routineId, 1);
  assert.equal(items[0].metadata.occurrenceKey, "2026-04-29");
  assert.equal(items[0].priority, "high");
});

test("lifeops share emits JSON packet output and writes packet files", async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "lifeops-cli-share-"));
  const { io, getStdout, getStderr } = createIo();

  const originalCwd = process.cwd();
  process.chdir(tempDir);
  try {
    await runCli(["init"], createIo().io);
    const exitCode = await runCli(
      [
        "share",
        "--project",
        "./lifeops.project.json",
        "--recipients",
        "./lifeops.recipients.json",
        "--sender-name",
        "Cody",
        "--format",
        "json",
        "--output-dir",
        "./outreach",
      ],
      io,
    );

    assert.equal(exitCode, 0);
    assert.equal(getStderr(), "");

    const payload = JSON.parse(getStdout());
    assert.equal(payload.packet.drafts.length, 1);
    assert.equal(payload.packet.followUps.length, 1);
    assert.equal(payload.writeSummary.files.includes("packet.json"), true);
  } finally {
    process.chdir(originalCwd);
  }
});

test("lifeops cmail delegates to the managed cmail service wrapper", async () => {
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const exitCode = await runCli(
    ["cmail", "status"],
    io,
    {
      runner: async (payload) => {
        calls.push(payload);
        io.stdout.write("cmail ok\n");
        return 0;
      },
    },
  );

  assert.equal(exitCode, 0);
  assert.equal(getStderr(), "");
  assert.match(getStdout(), /cmail ok/);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].command, "zsh");
  assert.deepEqual(calls[0].args, ["./bin/cmail-service", "status"]);
});

test("lifeops cmail audit delegates to the managed cmail service wrapper", async () => {
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const exitCode = await runCli(
    ["cmail", "audit"],
    io,
    {
      runner: async (payload) => {
        calls.push(payload);
        io.stdout.write("audit ok\n");
        return 0;
      },
    },
  );

  assert.equal(exitCode, 0);
  assert.equal(getStderr(), "");
  assert.match(getStdout(), /audit ok/);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].command, "zsh");
  assert.deepEqual(calls[0].args, ["./bin/cmail-service", "audit"]);
});

test("lifeops cmail tailscale delegates to the managed cmail service wrapper", async () => {
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const exitCode = await runCli(
    ["cmail", "tailscale", "--https-port", "8443"],
    io,
    {
      runner: async (payload) => {
        calls.push(payload);
        io.stdout.write("tailscale ok\n");
        return 0;
      },
    },
  );

  assert.equal(exitCode, 0);
  assert.equal(getStderr(), "");
  assert.match(getStdout(), /tailscale ok/);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].command, "zsh");
  assert.deepEqual(calls[0].args, ["./bin/cmail-service", "tailscale", "--https-port", "8443"]);
});

test("lifeops cmail secure-doctor delegates to the managed cmail service wrapper", async () => {
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const exitCode = await runCli(
    ["cmail", "secure-doctor", "--https-port", "4311"],
    io,
    {
      runner: async (payload) => {
        calls.push(payload);
        io.stdout.write("secure doctor ok\n");
        return 0;
      },
    },
  );

  assert.equal(exitCode, 0);
  assert.equal(getStderr(), "");
  assert.match(getStdout(), /secure doctor ok/);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].command, "zsh");
  assert.deepEqual(calls[0].args, ["./bin/cmail-service", "secure-doctor", "--https-port", "4311"]);
});

test("lifeops cmail auth-code delegates to the managed cmail service wrapper", async () => {
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const exitCode = await runCli(
    ["cmail", "auth-code", "--rotate"],
    io,
    {
      runner: async (payload) => {
        calls.push(payload);
        io.stdout.write("auth code ok\n");
        return 0;
      },
    },
  );

  assert.equal(exitCode, 0);
  assert.equal(getStderr(), "");
  assert.match(getStdout(), /auth code ok/);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].command, "zsh");
  assert.deepEqual(calls[0].args, ["./bin/cmail-service", "auth-code", "--rotate"]);
});

test("lifeops cmail drafts delegates to the bundled backend python CLI", async () => {
  const fakeHome = await createFakeCmailHome();
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const originalHome = process.env.LIFE_OPS_HOME;
  process.env.LIFE_OPS_HOME = fakeHome;
  try {
    const exitCode = await runCli(
      ["cmail", "drafts", "--format", "json"],
      io,
      {
        runner: async (payload) => {
          calls.push(payload);
          io.stdout.write('{"drafts":[]}\n');
          return 0;
        },
      },
    );

    assert.equal(exitCode, 0);
    assert.equal(getStderr(), "");
    assert.match(getStdout(), /"drafts":\[\]/);
    assert.equal(calls.length, 1);
    assert.match(calls[0].command, /\/venvs\/cmail\/bin\/python$/);
    assert.deepEqual(calls[0].args, ["-m", "life_ops", "cmail-drafts", "--format", "json"]);
    assert.equal(calls[0].env.LIFE_OPS_HOME, fakeHome);
  } finally {
    if (originalHome === undefined) {
      delete process.env.LIFE_OPS_HOME;
    } else {
      process.env.LIFE_OPS_HOME = originalHome;
    }
  }
});

test("lifeops cmail draft-save preserves raw draft flags for backend CLI", async () => {
  const fakeHome = await createFakeCmailHome();
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const originalHome = process.env.LIFE_OPS_HOME;
  process.env.LIFE_OPS_HOME = fakeHome;
  try {
    const exitCode = await runCli(
      [
        "cmail",
        "draft-save",
        "--to",
        "alexwg@alexwg.org",
        "--subject",
        "Hello",
        "--body",
        "Hi Alex",
        "--attach",
        "./paper.pdf",
      ],
      io,
      {
        runner: async (payload) => {
          calls.push(payload);
          io.stdout.write("saved\n");
          return 0;
        },
      },
    );

    assert.equal(exitCode, 0);
    assert.equal(getStderr(), "");
    assert.match(getStdout(), /saved/);
    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].args, [
      "-m",
      "life_ops",
      "cmail-draft-save",
      "--to",
      "alexwg@alexwg.org",
      "--subject",
      "Hello",
      "--body",
      "Hi Alex",
      "--attach",
      "./paper.pdf",
    ]);
  } finally {
    if (originalHome === undefined) {
      delete process.env.LIFE_OPS_HOME;
    } else {
      process.env.LIFE_OPS_HOME = originalHome;
    }
  }
});

test("lifeops cmail new-draft is a friendly alias for draft-save", async () => {
  const fakeHome = await createFakeCmailHome();
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const originalHome = process.env.LIFE_OPS_HOME;
  process.env.LIFE_OPS_HOME = fakeHome;
  try {
    const exitCode = await runCli(
      ["cmail", "new-draft", "--to", "terry@example.com", "--subject", "Hello"],
      io,
      {
        runner: async (payload) => {
          calls.push(payload);
          io.stdout.write("saved\n");
          return 0;
        },
      },
    );

    assert.equal(exitCode, 0);
    assert.equal(getStderr(), "");
    assert.match(getStdout(), /saved/);
    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].args, [
      "-m",
      "life_ops",
      "cmail-draft-save",
      "--to",
      "terry@example.com",
      "--subject",
      "Hello",
    ]);
  } finally {
    if (originalHome === undefined) {
      delete process.env.LIFE_OPS_HOME;
    } else {
      process.env.LIFE_OPS_HOME = originalHome;
    }
  }
});

test("lifeops cmail batch-send delegates to throttled backend command", async () => {
  const fakeHome = await createFakeCmailHome();
  const { io, getStdout, getStderr } = createIo();
  const calls = [];
  const originalHome = process.env.LIFE_OPS_HOME;
  process.env.LIFE_OPS_HOME = fakeHome;
  try {
    const exitCode = await runCli(
      [
        "cmail",
        "batch-send",
        "--ids",
        "74222,74223",
        "--max-per-hour",
        "5",
        "--min-gap-minutes",
        "12",
        "--dry-run",
        "--format",
        "json",
      ],
      io,
      {
        runner: async (payload) => {
          calls.push(payload);
          io.stdout.write('{"dry_run":true}\n');
          return 0;
        },
      },
    );

    assert.equal(exitCode, 0);
    assert.equal(getStderr(), "");
    assert.match(getStdout(), /"dry_run":true/);
    assert.equal(calls.length, 1);
    assert.deepEqual(calls[0].args, [
      "-m",
      "life_ops",
      "cmail-batch-send",
      "--ids",
      "74222,74223",
      "--max-per-hour",
      "5",
      "--min-gap-minutes",
      "12",
      "--dry-run",
      "--format",
      "json",
    ]);
  } finally {
    if (originalHome === undefined) {
      delete process.env.LIFE_OPS_HOME;
    } else {
      process.env.LIFE_OPS_HOME = originalHome;
    }
  }
});

test("lifeops cmail url prints the mailbox URL without spawning a process", async () => {
  const { io, getStdout, getStderr } = createIo();
  const exitCode = await runCli(["cmail", "url"], io);

  assert.equal(exitCode, 0);
  assert.equal(getStderr(), "");
  assert.equal(getStdout().trim(), "http://127.0.0.1:4311");
});

test("lifeops version reads the installed package version", async () => {
  const { io, getStdout, getStderr } = createIo();
  const packageMeta = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
  const exitCode = await runCli(["version"], io);

  assert.equal(exitCode, 0);
  assert.equal(getStderr(), "");
  assert.equal(getStdout().trim(), `lifeops ${packageMeta.version}`);
});

test("lifeops cmail help is available from the shortcut entrypoint", async () => {
  const { io, getStdout, getStderr } = createIo();
  const exitCode = await runCli(["cmail", "--help"], io);

  assert.equal(exitCode, 0);
  assert.equal(getStderr(), "");
  assert.match(getStdout(), /self-hosted mail surface/i);
  assert.match(getStdout(), /Cloudflare\/Resend accounts/i);
  assert.match(getStdout(), /cmail new-draft/);
  assert.match(getStdout(), /cmail drafts/);
  assert.match(getStdout(), /cmail draft-save/);
  assert.match(getStdout(), /cmail batch-send/);
  assert.match(getStdout(), /cmail audit/);
  assert.match(getStdout(), /cmail tailscale/);
  assert.match(getStdout(), /cmail secure-doctor/);
  assert.match(getStdout(), /cmail auth-code/);
  assert.match(getStdout(), /--attach \.\/file\.pdf/);
});
