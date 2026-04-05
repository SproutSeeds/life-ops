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
      ["cmail", "draft-save", "--to", "alexwg@alexwg.org", "--subject", "Hello", "--body", "Hi Alex"],
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
});
