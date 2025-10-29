#!/usr/bin/env bash
# Run weekly update of player_stock_summary.csv and commit if changed.
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

# Activate venv if present
if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

PY_SCRIPT="scripts/run_full_season_chunked.py"
OUT_FILE="data/player_stock_summary.csv"

if [ ! -f "$PY_SCRIPT" ]; then
  echo "Error: $PY_SCRIPT not found" >&2
  exit 2
fi

# Run the analyzer
echo "Running $PY_SCRIPT -> $OUT_FILE"
python3 "$PY_SCRIPT"

# If running in a git repo, commit the updated CSV when it changed
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git add "$OUT_FILE" || true
  if git diff --cached --quiet -- "$OUT_FILE"; then
    echo "No changes to $OUT_FILE"
    # nothing to commit
    exit 0
  fi
  git commit -m "chore(stocks): weekly update player_stock_summary.csv" || true
  echo "Committed updated $OUT_FILE"
else
  echo "Not in a git repo; skipping commit"
fi
