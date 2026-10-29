"""Mandatory local C-mail website/CLI release checks; no live mail operations."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]


def main():
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    # An inherited node test-runner marker otherwise silently skips child tests.
    env.pop("NODE_TEST_CONTEXT", None)
    commands = [
        ["npm", "run", "check", "--prefix", "packages/cmail-cloud"],
        ["npm", "test", "--prefix", "packages/cmail-cloud"],
        [sys.executable, "-m", "unittest", "tests.test_cmail_cloud"],
    ]
    for command in commands:
        subprocess.run(command, cwd=ROOT, env=env, check=True)
    print("C-mail website/CLI release checks passed", flush=True)


if __name__ == "__main__":
    main()
