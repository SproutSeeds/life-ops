import { readFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// One machine-wide cutover. Missing configuration retains upstream behavior;
// an invalid configuration or an unreachable cloud must never reopen old mail.
export async function runCloudCmail(argv) {
  let config;
  try { config = JSON.parse(await readFile(path.join(os.homedir(), '.lifeops/config/cmail-cloud.json'), 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  if (config.active !== true || config.system !== 'cmail-cloud') throw new Error('Invalid CMAIL cutover configuration; refusing local fallback');
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
  const python = path.join(root, '.venv/bin/python');
  if (!argv.length || argv[0] === 'help') argv = ['--help'];
  return new Promise((resolve, reject) => {
    const child = spawn(python, ['-m', 'life_ops.cmail_cloud', ...argv], {
      cwd: root, stdio: 'inherit', env: { ...process.env, PYTHONPATH: path.join(root, 'src') },
    });
    child.on('error', reject);
    child.on('exit', (code, signal) => resolve(signal ? 1 : code ?? 1));
  });
}
