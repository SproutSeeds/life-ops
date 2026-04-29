#!/bin/zsh
set -uo pipefail

SCRIPT_PATH="${0:A}"
SCRIPT_DIR="${SCRIPT_PATH:h}"
ROOT="${LIFEOPS_REPO_ROOT:-${SCRIPT_DIR:h}}"
COLLAB_ROOT="${LIFEOPS_COLLAB_ROOT:-${ROOT:h}/collaboration}"
USER_HOME="${HOME:-}"
if [[ -z "$USER_HOME" ]]; then
  echo "HOME is required so LifeOps can locate its state directory." >&2
  exit 1
fi
DEFAULT_LIFEOPS_BIN="${USER_HOME}/.lifeops/venvs/cmail/bin/life-ops"
if [[ ! -x "$DEFAULT_LIFEOPS_BIN" ]]; then
  DEFAULT_LIFEOPS_BIN="${ROOT}/bin/life-ops"
fi
LIFEOPS_BIN="${LIFEOPS_BIN:-$DEFAULT_LIFEOPS_BIN}"
LIFEOPS_STATE_ROOT="${LIFEOPS_STATE_ROOT:-${LIFE_OPS_HOME:-${USER_HOME}/.lifeops}}"
LIFEOPS_DB="${LIFEOPS_DAY_SHEET_DB:-${LIFEOPS_STATE_ROOT}/data/cmail_runtime.db}"
GITHUB_SWEEP="${GITHUB_SWEEP:-${COLLAB_ROOT}/_coordination/scripts/github_morning_action_plan.sh}"
DEFAULT_GITHUB_LIFEOPS_BIN="${USER_HOME}/.lifeops/bin/lifeops_day_sheet_db.sh"
if [[ ! -x "$DEFAULT_GITHUB_LIFEOPS_BIN" ]]; then
  DEFAULT_GITHUB_LIFEOPS_BIN="${ROOT}/scripts/lifeops_day_sheet_db.sh"
fi
GITHUB_LIFEOPS_BIN="${GITHUB_LIFEOPS_BIN:-$DEFAULT_GITHUB_LIFEOPS_BIN}"
OUTPUT_DIR="${LIFEOPS_DAY_SHEET_PRINT_OUTPUT_DIR:-${LIFEOPS_NOON_PRINT_OUTPUT_DIR:-${USER_HOME}/.codex/memories/lifeops-day-sheet-print}}"
ORP_OUTPUT_DIR="${LIFEOPS_DAY_SHEET_ORP_OUTPUT_DIR:-${LIFEOPS_NOON_ORP_OUTPUT_DIR:-${OUTPUT_DIR}/orp}}"
GITHUB_OUTPUT_DIR="${LIFEOPS_DAY_SHEET_GITHUB_OUTPUT_DIR:-${LIFEOPS_NOON_GITHUB_OUTPUT_DIR:-${OUTPUT_DIR}/github}}"
GITHUB_SCAN_OUTPUT_DIR="${LIFEOPS_DAY_SHEET_GITHUB_SCAN_OUTPUT_DIR:-${LIFEOPS_NOON_GITHUB_SCAN_OUTPUT_DIR:-${OUTPUT_DIR}/github-scan}}"
PRINTER="${LIFEOPS_DAY_SHEET_PRINTER:-${LIFEOPS_NOON_PRINTER:-}}"
SIDES="${LIFEOPS_DAY_SHEET_PRINT_SIDES:-${LIFEOPS_NOON_PRINT_SIDES:-one-sided}}"
PRINT_VERIFY_TIMEOUT="${LIFEOPS_DAY_SHEET_PRINT_VERIFY_TIMEOUT:-90}"
STRICT_REFRESH="${LIFEOPS_DAY_SHEET_STRICT_REFRESH:-${LIFEOPS_NOON_STRICT_REFRESH:-0}}"
FOCUS_WORKSPACE="${LIFEOPS_DAY_SHEET_ORP_WORKSPACE:-${LIFEOPS_NOON_ORP_WORKSPACE:-focused-items}}"
MAX_PROJECTS="${LIFEOPS_DAY_SHEET_ORP_MAX_PROJECTS:-${LIFEOPS_NOON_ORP_MAX_PROJECTS:-36}}"
CALENDAR_LIMIT="${LIFEOPS_DAY_SHEET_ORP_CALENDAR_LIMIT:-${LIFEOPS_NOON_ORP_CALENDAR_LIMIT:-36}}"
ROADMAP_DAYS="${LIFEOPS_DAY_SHEET_ROADMAP_DAYS:-91}"
ROADMAP_ITEM_LIMIT="${LIFEOPS_DAY_SHEET_ROADMAP_ITEM_LIMIT:-1000}"
OPEN_LIST_ITEM_LIMIT="${LIFEOPS_DAY_SHEET_OPEN_LIST_ITEM_LIMIT:-12}"
PDF_ENGINE="${LIFEOPS_DAY_SHEET_PDF_ENGINE:-}"

DRY_RUN=0
SKIP_REFRESH=0
SKIP_SAVE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      ;;
    --skip-refresh)
      SKIP_REFRESH=1
      ;;
    --skip-save)
      SKIP_SAVE=1
      ;;
    --output-dir)
      shift
      OUTPUT_DIR="$1"
      ORP_OUTPUT_DIR="${OUTPUT_DIR}/orp"
      GITHUB_OUTPUT_DIR="${OUTPUT_DIR}/github"
      GITHUB_SCAN_OUTPUT_DIR="${OUTPUT_DIR}/github-scan"
      ;;
    --printer)
      shift
      PRINTER="$1"
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH}"
export LIFE_OPS_HOME="$LIFEOPS_STATE_ROOT"
export LIFEOPS_DAY_SHEET_ORP_WORKSPACE="$FOCUS_WORKSPACE"
mkdir -p "$OUTPUT_DIR" "$ORP_OUTPUT_DIR" "$GITHUB_OUTPUT_DIR" "$GITHUB_SCAN_OUTPUT_DIR"

DAY="$(date '+%Y-%m-%d')"
STAMP="$(date '+%Y%m%d-%H%M%S')"
RUN_LOG="${OUTPUT_DIR}/day-sheet-print-${STAMP}.log"
LATEST_LOG="${OUTPUT_DIR}/latest.log"
HTML_PATH="${OUTPUT_DIR}/lifeops-day-sheet-${DAY}-${STAMP}.html"
TEXT_PATH="${OUTPUT_DIR}/lifeops-day-sheet-${DAY}-${STAMP}.txt"
TEX_PATH="${OUTPUT_DIR}/lifeops-day-sheet-${DAY}-${STAMP}.tex"
PDF_PATH="${OUTPUT_DIR}/lifeops-day-sheet-${DAY}-${STAMP}.pdf"
LATEST_HTML="${OUTPUT_DIR}/latest.html"
LATEST_TEXT="${OUTPUT_DIR}/latest.txt"
LATEST_TEX="${OUTPUT_DIR}/latest.tex"
LATEST_PDF="${OUTPUT_DIR}/latest.pdf"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" | tee -a "$RUN_LOG"
}

run_refresh_step() {
  local name="$1"
  shift
  log "start ${name}"
  "$@" >> "$RUN_LOG" 2>&1
  local step_status=$?
  if [[ "$step_status" -eq 0 ]]; then
    log "ok ${name}"
    return 0
  fi
  log "warn ${name} failed status=${step_status}"
  if [[ "$STRICT_REFRESH" == "1" ]]; then
    return "$step_status"
  fi
  return 0
}

target_printer() {
  if [[ -n "$PRINTER" ]]; then
    printf '%s\n' "$PRINTER"
    return 0
  fi
  lpstat -d 2>/dev/null | sed 's/^system default destination: //'
}

cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
log "scheduled LifeOps day-sheet print pipeline starting day=${DAY} dry_run=${DRY_RUN} db=${LIFEOPS_DB}"

if [[ "$SKIP_REFRESH" -eq 0 ]]; then
  if [[ -x "$GITHUB_SWEEP" ]]; then
    run_refresh_step "github_action_plan" \
      env \
        GITHUB_MORNING_OUTPUT_DIR="$GITHUB_OUTPUT_DIR" \
        GITHUB_QUEUE_SCAN_OUTPUT_DIR="$GITHUB_SCAN_OUTPUT_DIR" \
        LIFEOPS_BIN="$GITHUB_LIFEOPS_BIN" \
        LIFEOPS_REAL_BIN="$LIFEOPS_BIN" \
        LIFEOPS_DB="$LIFEOPS_DB" \
        LIFEOPS_DAY_SHEET_DB="$LIFEOPS_DB" \
        LIFE_OPS_HOME="$LIFE_OPS_HOME" \
        "$GITHUB_SWEEP"
  else
    log "skip github_action_plan missing executable path=${GITHUB_SWEEP}"
  fi

  run_refresh_step "orp_project_sweep" \
    "$LIFEOPS_BIN" --db "$LIFEOPS_DB" orp-sweep \
      --date "$DAY" \
      --update-calendar \
      --max-projects "$MAX_PROJECTS" \
      --calendar-limit "$CALENDAR_LIMIT" \
      --output-dir "$ORP_OUTPUT_DIR"
else
  log "skip refresh steps"
fi

if [[ "$SKIP_SAVE" -eq 0 ]]; then
  run_refresh_step "calendar_snapshot" \
    "$LIFEOPS_BIN" --db "$LIFEOPS_DB" calendar-save-day \
      --date "$DAY" \
      --title "Scheduled print snapshot" \
      --summary "Scheduled print snapshot after LifeOps refresh."
else
  log "skip calendar snapshot"
fi

if [[ -z "$PDF_ENGINE" ]]; then
  PDF_ENGINE="$(command -v pdflatex || command -v xelatex || true)"
fi
if [[ -z "$PDF_ENGINE" ]]; then
  log "error no LaTeX PDF engine found; install pdflatex or xelatex"
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit 1
fi

log "render compact day sheet pdf=${PDF_PATH} html=${HTML_PATH} text=${TEXT_PATH}"
"$LIFEOPS_BIN" --db "$LIFEOPS_DB" day-sheet --date "$DAY" --max-open-list-items "$OPEN_LIST_ITEM_LIMIT" --roadmap-days "$ROADMAP_DAYS" --roadmap-item-limit "$ROADMAP_ITEM_LIMIT" --no-frg-first-page --format html --output "$HTML_PATH" >> "$RUN_LOG" 2>&1
render_html_status=$?
"$LIFEOPS_BIN" --db "$LIFEOPS_DB" day-sheet --date "$DAY" --max-open-list-items "$OPEN_LIST_ITEM_LIMIT" --roadmap-days "$ROADMAP_DAYS" --roadmap-item-limit "$ROADMAP_ITEM_LIMIT" --no-frg-first-page --output "$TEXT_PATH" >> "$RUN_LOG" 2>&1
render_text_status=$?
"$LIFEOPS_BIN" --db "$LIFEOPS_DB" day-sheet --date "$DAY" --max-open-list-items "$OPEN_LIST_ITEM_LIMIT" --roadmap-days "$ROADMAP_DAYS" --roadmap-item-limit "$ROADMAP_ITEM_LIMIT" --no-frg-first-page --format latex --output "$TEX_PATH" >> "$RUN_LOG" 2>&1
render_tex_status=$?
if [[ "$render_html_status" -ne 0 || "$render_text_status" -ne 0 || "$render_tex_status" -ne 0 ]]; then
  log "error render failed html_status=${render_html_status} text_status=${render_text_status} tex_status=${render_tex_status}"
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit 1
fi
"$PDF_ENGINE" -interaction=nonstopmode -halt-on-error -output-directory "$OUTPUT_DIR" "$TEX_PATH" >> "$RUN_LOG" 2>&1
pdf_status=$?
if [[ "$pdf_status" -ne 0 || ! -s "$PDF_PATH" ]]; then
  log "error pdf build failed status=${pdf_status} engine=${PDF_ENGINE}"
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit 1
fi
cp "$HTML_PATH" "$LATEST_HTML"
cp "$TEXT_PATH" "$LATEST_TEXT"
cp "$TEX_PATH" "$LATEST_TEX"
cp "$PDF_PATH" "$LATEST_PDF"

TARGET_PRINTER="$(target_printer)"
if [[ -z "$TARGET_PRINTER" ]]; then
  log "error no printer configured and no system default printer found"
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit 1
fi

log "printer preflight target=${TARGET_PRINTER}"
lpstat -p "$TARGET_PRINTER" >> "$RUN_LOG" 2>&1
printer_status=$?
lpstat -a "$TARGET_PRINTER" >> "$RUN_LOG" 2>&1
accept_status=$?
if [[ "$printer_status" -ne 0 || "$accept_status" -ne 0 ]]; then
  log "error printer preflight failed printer_status=${printer_status} accept_status=${accept_status}"
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit 1
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  log "dry run: would print ${PDF_PATH}"
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit 0
fi

lp_args=(
  -d "$TARGET_PRINTER"
  -t "LifeOps Day Sheet ${DAY}"
  -o "sides=${SIDES}"
  -o "PageSize=Letter"
  -o "media=Letter"
  -o "fit-to-page"
  -o "print-scaling=fit"
)

log "print day sheet source=${PDF_PATH} printer=${TARGET_PRINTER} sides=${SIDES} verify_timeout=${PRINT_VERIFY_TIMEOUT}s"
lp_output="$(lp "${lp_args[@]}" "$PDF_PATH" 2>&1)"
print_status=$?
printf '%s\n' "$lp_output" >> "$RUN_LOG"
if [[ "$print_status" -ne 0 ]]; then
  log "error print failed status=${print_status}"
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit "$print_status"
fi
JOB_ID="$(printf '%s\n' "$lp_output" | sed -n 's/^request id is \([^ ]*\).*/\1/p' | head -1)"
if [[ -z "$JOB_ID" ]]; then
  log "error print accepted but no cups job id was returned"
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit 1
fi
log "print job submitted job_id=${JOB_ID}"

deadline=$((SECONDS + PRINT_VERIFY_TIMEOUT))
while [[ "$SECONDS" -lt "$deadline" ]]; do
  if ! lpstat -W not-completed -o "$JOB_ID" >> "$RUN_LOG" 2>&1; then
    break
  fi
  sleep 2
done

if lpstat -W not-completed -o "$JOB_ID" >> "$RUN_LOG" 2>&1; then
  log "error print job still not completed after ${PRINT_VERIFY_TIMEOUT}s job_id=${JOB_ID}"
  lpstat -l -p "$TARGET_PRINTER" >> "$RUN_LOG" 2>&1 || true
  cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
  exit 1
fi

log "print job left active queue job_id=${JOB_ID}"
lpstat -W completed -l -o "$JOB_ID" >> "$RUN_LOG" 2>&1 || true
lpstat -l -p "$TARGET_PRINTER" >> "$RUN_LOG" 2>&1 || true

log "scheduled LifeOps day-sheet print pipeline complete"
cp "$RUN_LOG" "$LATEST_LOG" 2>/dev/null || true
