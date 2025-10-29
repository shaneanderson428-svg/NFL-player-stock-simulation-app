#!/usr/bin/env bash
set -euo pipefail

# Run the compute script inside a project-local virtualenv to avoid
# "externally-managed-environment" pip errors on macOS.

VENV_DIR=".venv"
PYTHON="${VENV_DIR}/bin/python3"

INPUT="${1:-data/player_game_stats.csv}"
OUTPUT="${2:-data/player_stock_summary.csv}"

echo "Using input: $INPUT"
echo "Writing output: $OUTPUT"

if [ ! -x "$PYTHON" ]; then
  echo "Creating virtualenv at $VENV_DIR..."
  python3 -m venv "$VENV_DIR"
fi

echo "Upgrading pip and installing runtime deps in venv..."
"$PYTHON" -m pip install --upgrade pip setuptools wheel >/dev/null
"$PYTHON" -m pip install pandas numpy >/dev/null

echo "Running compute script..."
# Use the canonical compute script implementation
"$PYTHON" scripts/compute_player_stock.py --input "$INPUT" --output "$OUTPUT"

echo "Compute finished. Summary written to $OUTPUT"
