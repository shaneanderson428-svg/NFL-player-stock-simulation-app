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

# Attempt API-Sports fetch and merge prior to compute
WEEK="${WEEK:-1}"
if [ "$WEEK" = "1" ]; then
  echo "WEEK not set; defaulting to WEEK=1 for API-Sports/merge steps"
fi

echo "Fetching API-Sports data for week=$WEEK (if APISPORTS_KEY present)"
APIS_PATH="external/apisports/player_stats_week_${WEEK}.csv"
TANK_PATH="external/tank01/player_stats_week_${WEEK}.csv"
NFLFAST_PATH="external/nflfastR/player_stats_2025.csv"

if [ -f "$NFLFAST_PATH" ]; then
  echo "nflfastR master present at $NFLFAST_PATH; it will be preferred when merging"
fi

if [ -f "$APIS_PATH" ]; then
  echo "API-Sports CSV already present at $APIS_PATH; skipping fetch"
else
  if [ -f "$TANK_PATH" ]; then
    echo "API-Sports CSV missing; falling back to existing Tank01 CSV at $TANK_PATH"
  else
    echo "API-Sports and Tank01 CSVs missing; attempting to fetch API-Sports and Tank01 data"
    python3 external/apisports/fetch_apisports_week.py --week "$WEEK" || echo "API-Sports fetch failed or skipped (continuing)"
    python3 scripts/fetch_tank01_week.py --week "$WEEK" || echo "Tank01 fetch failed (continuing)"
    # Fetch API-Sports advanced weekly stats (separate endpoint) and then merge
    .venv/bin/python scripts/fetch_apisports_adv_week.py --week "$WEEK" || echo "API-Sports advanced fetch failed or skipped (continuing)"
    .venv/bin/python scripts/merge_tank01_apisports.py --week "$WEEK" || echo "Tank01+API-Sports merge failed (continuing)"
  fi
fi

echo "Merging API-Sports with nflfastR for week=$WEEK"
python3 external/merge/merge_week.py --week "$WEEK" || echo "Merge step failed or skipped (continuing)"

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
