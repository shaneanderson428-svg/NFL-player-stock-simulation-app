#!/usr/bin/env bash
# Run the weekly update pipeline (manual run)
# Usage: ./scripts/run_weekly_update.sh <SEASON> <WEEK>

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <SEASON> <WEEK>" >&2
  echo "Example: $0 2025 7" >&2
  exit 2
fi

SEASON=$1
WEEK=$2

echo "Starting weekly update: season=$SEASON week=$WEEK"

PY=python3

# Ensure RAPIDAPI_KEY is present in environment for any scripts that require it.
if [ -z "${RAPIDAPI_KEY:-}" ]; then
  echo "Error: RAPIDAPI_KEY is not set in the environment. Export it (or source your .env.local into the environment) before running this script." >&2
  echo "Example: export RAPIDAPI_KEY=your_key_here" >&2
  exit 3
fi

# Ensure Python can import the local `scripts` package when running as modules
export PYTHONPATH="$ROOT_DIR"

echo "1/4: Fetching weekly boxscores and player stats"
# Run fetch as a module so package imports work reliably
$PY -m scripts.fetch_weekly_all_positions --season "$SEASON" --week "$WEEK"

# After fetch, ensure the tank01 CSV for the requested week is non-empty. If it's empty
# (sometimes providers produce a zero-byte placeholder), try to find the latest non-empty week
# and continue with that week instead.
TANK_FILE="$ROOT_DIR/external/tank01/player_stats_week_${WEEK}.csv"
if [ -f "$TANK_FILE" ]; then
  size=$(wc -c <"$TANK_FILE" | tr -d '[:space:]') || size=0
  if [ "$size" -lt 20 ]; then
    echo "Warning: $TANK_FILE exists but is very small ($size bytes). Searching for latest non-empty week file..."
    latest_found=""
    for f in "$ROOT_DIR"/external/tank01/player_stats_week_*.csv; do
      # iterate and find the highest week with reasonable size
      [ -e "$f" ] || continue
      s=$(wc -c <"$f" | tr -d '[:space:]') || s=0
      if [ "$s" -gt 100 ]; then
        # extract week number
        fname=$(basename "$f")
        wk=$(echo "$fname" | sed -E 's/player_stats_week_([0-9]+)\.csv/\1/')
        if [ -z "$latest_found" ] || [ "$wk" -gt "$latest_found" ]; then
          latest_found=$wk
        fi
      fi
    done
    if [ -n "$latest_found" ]; then
      echo "Using latest non-empty week: $latest_found (instead of requested $WEEK)"
      WEEK=$latest_found
      TANK_FILE="$ROOT_DIR/external/tank01/player_stats_week_${WEEK}.csv"
    else
      echo "Error: no non-empty tank01 weekly CSV found. Aborting." >&2
      exit 4
    fi
  fi
else
  echo "Warning: expected tank file $TANK_FILE not found. Proceeding and letting downstream scripts error if missing."
fi

# If a RapidAPI live CSV exists but is clearly invalid (very small or header-only),
# quarantine it so downstream `compute_player_stock` prefers the Tank01 CSV instead.
RAPID_CSV="$ROOT_DIR/external/rapid/player_stats_live.csv"
if [ -f "$RAPID_CSV" ]; then
  # count non-empty data lines (exclude header)
  nonempty=$(awk 'NR>1 && /[^,]/ {count++} END {print count+0}' "$RAPID_CSV") || nonempty=0
  size=$(wc -c <"$RAPID_CSV" | tr -d '[:space:]') || size=0
  # check if any data row contains a non-empty player name in the first column
  has_player=$(awk -F',' 'NR>1{ if ($1 ~ /[^[:space:]]/) {print 1; exit} } END{print 0}' "$RAPID_CSV") || has_player=0
  if [ "$size" -lt 50 ] || [ "$nonempty" -eq 0 ] || [ "$has_player" -eq 0 ]; then
    echo "Quarantining potentially-empty or invalid RapidAPI CSV ($RAPID_CSV): size=${size}, data-rows=${nonempty}, has_player=${has_player}"
    mv "$RAPID_CSV" "${RAPID_CSV}.bak" || echo "Failed to move $RAPID_CSV"
  fi
fi

# Also quarantine the RapidAPI JSON if it contains no players
RAPID_JSON="$ROOT_DIR/external/rapid/player_stats_live.json"
if [ -f "$RAPID_JSON" ]; then
  ply_count=0
  ply_count=$(python3 - <<PY
import json,sys
try:
  j=json.load(open('$RAPID_JSON'))
  print(len(j.get('players',[])))
except Exception:
  print(0)
PY
)
  if [ "$ply_count" -eq 0 ]; then
    echo "Quarantining empty RapidAPI JSON ($RAPID_JSON): players=${ply_count}"
    mv "$RAPID_JSON" "${RAPID_JSON}.bak" || echo "Failed to move $RAPID_JSON"
  fi
fi

echo "2/4: Computing advanced metrics (week=$WEEK)"
# Run compute_advanced_metrics as a module and pass the explicit week
$PY -m scripts.compute_advanced_metrics --week "$WEEK"

echo "3/4: Computing player stock (site-ready outputs)"
# compute_player_stock.py produces the site-ready CSVs used by the website. Run as a module to ensure imports resolve.
if [ -f scripts/compute_player_stock.py ]; then
  $PY -m scripts.compute_player_stock
else
  # fallback to update_weekly_prices if compute_player_stock isn't present
  if [ -f scripts/update_weekly_prices.py ]; then
    echo "compute_player_stock.py not found; running update_weekly_prices.py as fallback"
    $PY -m scripts.update_weekly_prices
  else
    echo "Warning: neither scripts/compute_player_stock.py nor scripts/update_weekly_prices.py found. Skipping stock computation." >&2
  fi
fi

# Update persistent weekly prices/history (append-only)
echo "Updating persistent weekly prices/history (week=$WEEK)"
$PY -m scripts.update_weekly_prices --season "$SEASON" --week "$WEEK"

echo "4/4: Post-run summary and cleanup"

echo "Weekly update complete. Check generated outputs under external/ and data/ as applicable."

# Produce a small JSON summary of outputs (non-exhaustive)
SUMMARY_FILE="data/weekly_update_summary_${SEASON}_w${WEEK}.json"
mkdir -p data
python3 - <<PY >> "$SUMMARY_FILE"
import json, os, time
season = ${SEASON}
week = ${WEEK}
files = []
candidates = [
  f"external/tank01/player_stats_week_{week}.csv",
  f"external/tank01/player_stats_week_{week}.json",
  f"external/advanced/advanced_metrics_week_{week}.csv",
  "data/player_stock_summary.csv",
  "data/player_stock_history.csv",
  "external/history/player_prices.json",
]
for p in candidates:
    if os.path.exists(p):
        st = os.stat(p)
        files.append({"path": p, "size": st.st_size, "mtime": int(st.st_mtime)})
summary = {"season": season, "week": week, "timestamp": int(time.time()), "files": files}
print(json.dumps(summary, indent=2))
PY

echo "Wrote summary to $SUMMARY_FILE"

# Also write a stable "latest" summary for quick checks
LATEST_SUMMARY="data/weekly_update_summary_latest.json"
cp "$SUMMARY_FILE" "$LATEST_SUMMARY" || echo "Warning: failed to copy summary to $LATEST_SUMMARY"
echo "Wrote latest summary to $LATEST_SUMMARY"
