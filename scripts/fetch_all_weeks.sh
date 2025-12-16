#!/bin/bash
set -e

# Ensure the script runs from project root
cd "$(dirname "$0")/.."

echo "Fetching past Tank01 weekly stats (Weeks 1–13)..."

for WEEK in {1..13}
do
    echo "---------------------------------------"
    echo "Fetching week $WEEK..."
    .venv/bin/python scripts/fetch_tank01_week.py --week $WEEK
    echo "Finished week $WEEK"
done

echo ""
echo "---------------------------------------"
echo "Rebuilding full WR history from all available weeks..."
.venv/bin/python scripts/generate_wr_history.py

echo ""
echo "All past weeks fetched and history rebuilt!"
echo "You can now run:  npm run dev  and check http://localhost:3000/wr"
