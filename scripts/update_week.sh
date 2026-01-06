#!/usr/bin/env bash
set -euo pipefail

# Simple orchestrator to run the weekly market update in order:
# 1) compute_weekly_prices.py
# 2) append_price_history.py
# 3) generate_player_meta.py
# Usage: ./scripts/update_week.sh --season 2025 --week 2

SEASON=""
WEEK=""

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --season)
      SEASON="$2"
      shift 2
      ;;
    --week)
      WEEK="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 --season <SEASON> --week <WEEK>"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 --season <SEASON> --week <WEEK>"
      exit 1
      ;;
  esac
done

if [[ -z "$SEASON" || -z "$WEEK" ]]; then
  echo "ERROR: --season and --week are required"
  echo "Usage: $0 --season <SEASON> --week <WEEK>"
  exit 2
fi

echo "Running weekly market update: season=$SEASON week=$WEEK"

# Determine expected canonical weekly CSV (what compute_weekly_prices.py looks for)
CANONICAL_CSV="${PWD}/data/weekly/player_stats_${SEASON}_week_${WEEK}.csv"
# Also consider the newer fetcher output location (data/weekly/<season>/week_<week>.csv)
FETCHED_CSV="${PWD}/data/weekly/${SEASON}/week_${WEEK}.csv"

if [[ -f "$CANONICAL_CSV" ]]; then
  echo "Found weekly stats at $CANONICAL_CSV"
else
  echo "Weekly stats not found at $CANONICAL_CSV"
  echo "Attempting to fetch weekly stats for season=$SEASON week=$WEEK..."
  # Run fetcher but don't let its non-zero exit crash this orchestrator (we'll re-check files)
  if python3 scripts/fetch_weekly_all_positions.py --season "$SEASON" --week "$WEEK"; then
    echo "Fetcher finished (exit 0)."
  else
    echo "Fetcher exited with non-zero status; continuing to re-check for files." >&2
  fi

  # If fetcher wrote the season-scoped week CSV, copy it to the canonical path expected by compute
  if [[ -f "$FETCHED_CSV" ]]; then
    echo "Found fetched CSV at $FETCHED_CSV — copying to canonical path $CANONICAL_CSV"
    mkdir -p "$(dirname "$CANONICAL_CSV")"
    cp "$FETCHED_CSV" "$CANONICAL_CSV"
  fi

  # Final check
  if [[ ! -f "$CANONICAL_CSV" ]]; then
    echo "No weekly stats CSV available for season=$SEASON week=$WEEK after fetch attempt. Skipping this week." >&2
    exit 0
  fi
fi

echo "1/3: Computing weekly prices..."
python3 scripts/compute_weekly_prices.py --season "$SEASON" --week "$WEEK"

echo "2/3: Appending price history..."
python3 scripts/append_price_history.py --season "$SEASON" --week "$WEEK"

echo "3/3: Generating player metadata..."
python3 scripts/generate_player_meta.py

echo "Weekly update complete."
