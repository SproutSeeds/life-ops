#!/bin/zsh
set -euo pipefail

SCRIPT_PATH="${0:A}"
SCRIPT_DIR="${SCRIPT_PATH:h}"
ROOT="${LIFEOPS_REPO_ROOT:-${SCRIPT_DIR:h}}"
USER_HOME="${HOME:-}"
if [[ -z "$USER_HOME" ]]; then
  echo "HOME is required so LifeOps can locate its state directory." >&2
  exit 1
fi

DEFAULT_LIFEOPS_REAL_BIN="${USER_HOME}/.lifeops/venvs/cmail/bin/life-ops"
if [[ ! -x "$DEFAULT_LIFEOPS_REAL_BIN" ]]; then
  DEFAULT_LIFEOPS_REAL_BIN="${ROOT}/bin/life-ops"
fi
LIFEOPS_REAL_BIN="${LIFEOPS_REAL_BIN:-$DEFAULT_LIFEOPS_REAL_BIN}"
LIFEOPS_STATE_ROOT="${LIFEOPS_STATE_ROOT:-${LIFE_OPS_HOME:-${USER_HOME}/.lifeops}}"
LIFEOPS_DB="${LIFEOPS_DAY_SHEET_DB:-${LIFEOPS_DB:-${LIFEOPS_STATE_ROOT}/data/cmail_runtime.db}}"

export LIFE_OPS_HOME="$LIFEOPS_STATE_ROOT"
exec "$LIFEOPS_REAL_BIN" --db "$LIFEOPS_DB" "$@"
