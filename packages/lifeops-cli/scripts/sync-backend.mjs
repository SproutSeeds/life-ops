import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const packageDir = path.resolve(scriptDir, "..");
const repoRoot = path.resolve(packageDir, "..", "..");
const backendDir = path.join(packageDir, "backend");

async function copyDir(source, target) {
  await cp(source, target, {
    recursive: true,
    force: true,
    filter(entry) {
      const base = path.basename(entry);
      if (base === "__pycache__") {
        return false;
      }
      if (base.endsWith(".egg-info")) {
        return false;
      }
      if (base.endsWith(".pyc")) {
        return false;
      }
      return true;
    },
  });
}

async function main() {
  await rm(backendDir, { recursive: true, force: true });
  await mkdir(backendDir, { recursive: true });

  await copyDir(path.join(repoRoot, "src"), path.join(backendDir, "src"));
  await copyDir(path.join(repoRoot, "static"), path.join(backendDir, "static"));
  await copyDir(
    path.join(repoRoot, "config", "cloudflare_email_worker"),
    path.join(backendDir, "config", "cloudflare_email_worker"),
  );

  const pyproject = await readFile(path.join(repoRoot, "pyproject.toml"), "utf8");
  await writeFile(path.join(backendDir, "pyproject.toml"), pyproject, "utf8");

  const license = await readFile(path.join(repoRoot, "LICENSE"), "utf8");
  await writeFile(path.join(backendDir, "LICENSE"), license, "utf8");
}

await main();
