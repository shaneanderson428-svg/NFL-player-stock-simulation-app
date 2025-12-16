#!/usr/bin/env bash
set -euo pipefail

PY=${PY:-python3}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Prefer using a merged Tank01+API-Sports CSV when available
WEEK="${WEEK:-1}"


echo "Fetching nflfastR stats..."
# Try to fetch for the target season but don't exit the script if fetch fails —
# we'll fall back to the newest available player_stats CSV under external/nflfastR/
if ! $PY scripts/fetch_nfl_stats.py --season 2025; then
	echo "Warning: fetch_nfl_stats.py failed or returned no new file; will attempt to use existing external/nflfastR files if present."
fi

fi

# Determine input CSV: prefer merged Tank01+API-Sports file for the requested week, then derived 2025 file if present, otherwise prefer exact 2025 player_stats CSV,
# otherwise pick the latest player_stats_*.csv found.
INPUT_PATH=""
MERGED_PATH="external/combined/week_${WEEK}_merged.csv"
if [ -f "$MERGED_PATH" ]; then
	INPUT_PATH="$MERGED_PATH"
	echo "Using merged Tank01+API-Sports input: $INPUT_PATH"
else
if [ -f external/rapidapi/player_stats_enriched_2025.csv ]; then
	INPUT_PATH="external/rapidapi/player_stats_enriched_2025.csv"
	echo "Using RapidAPI enriched CSV: $INPUT_PATH"
elif [ -f external/rapid/player_stats_live.csv ]; then
	INPUT_PATH="external/rapid/player_stats_live.csv"
	echo "Using RapidAPI live CSV: $INPUT_PATH"
elif [ -f external/rapid/player_stats_live.json ]; then
	# prefer CSV but accept JSON if present; compute script will convert JSON if needed
	INPUT_PATH="external/rapid/player_stats_live.json"
	echo "Using RapidAPI live JSON: $INPUT_PATH"
elif [ -f external/nflfastR/player_stats_2025_derived.csv ]; then
	INPUT_PATH="external/nflfastR/player_stats_2025_derived.csv"
	echo "Using derived live 2025 player stats: $INPUT_PATH"
elif [ -f external/nflfastR/player_stats_2025.csv ]; then
	INPUT_PATH="external/nflfastR/player_stats_2025.csv"
else
	# find latest by version/season in filename
	LATEST=$(ls -1 external/nflfastR/player_stats_*.csv 2>/dev/null | sort -V | tail -n 1 || true)
	if [ -n "$LATEST" ]; then
		INPUT_PATH="$LATEST"
		echo "Using fallback input CSV: $INPUT_PATH"
	else
			# Try to fall back to existing local game-level CSV if available
			if [ -f data/player_game_stats.csv ]; then
				INPUT_PATH="data/player_game_stats.csv"
				echo "Falling back to local data/player_game_stats.csv as input"
			else
				echo "No player_stats CSV found under external/nflfastR/ and no local data/player_game_stats.csv available. Cannot compute player stock."
				exit 0
			fi
	fi
fi

echo "Computing player stock using input: $INPUT_PATH"
$PY scripts/compute_player_stock.py --input "$INPUT_PATH" --output data/player_stock_summary.csv

echo "Done."