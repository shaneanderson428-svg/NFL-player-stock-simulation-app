#!/usr/bin/env bash
set -euo pipefail

# scripts/run_all.sh
# Regenerate summary, history, and cleaned profiles using the project's .venv Python.
# Usage: ./scripts/run_all.sh
# Notes:
#  - This script uses .venv/bin/python explicitly so it doesn't depend on a system "python" in PATH.
#  - It prints simple progress messages and exits on the first failure.

echo "1/2: Running compute_player_stock.py (summary + history)..."
.venv/bin/python scripts/compute_player_stock.py --input data/player_game_stats.csv --output data/player_stock_summary.csv

echo "2/2: Running clean_player_profiles.py (profiles)..."
.venv/bin/python scripts/clean_player_profiles.py

echo "\nAll done. Current timestamps and sizes for generated files:"
for f in data/player_stock_summary.csv data/player_stock_history.csv data/player_profiles_cleaned.csv; do
  if [ -e "$f" ]; then
    # macOS-friendly stat format: prints modified time, filename and size in bytes
    stat -f "%Sm %N %z bytes" -t "%Y-%m-%d %H:%M:%S" "$f"
  else
    echo "MISSING: $f"
  fi
done

# Optional: run fast JS typecheck if npx is available
if command -v npx >/dev/null 2>&1; then
  echo "\nOptional: running npx tsc --noEmit (typecheck)"
  npx tsc --noEmit || echo "Typecheck failed (optional step)"
fi

echo "Run finished."
