#!/usr/bin/env bash
set -euo pipefail

# Run the R fetch inside Docker if Rscript is not available locally.
# Usage: ./scripts/run_fetch_pbp.sh [2018 2019 ...]

args=("$@")
if command -v Rscript >/dev/null 2>&1; then
  echo "Rscript found locally — running directly"
  Rscript scripts/fetch_pbp.R "${args[@]}"
  exit 0
fi

echo "Rscript not found — running via Docker container"
docker build -f Dockerfile.r-fetch -t myapp-r-fetch:latest .

# Mount the repo so output goes to host data/pbp
docker run --rm -v "$(pwd)/data/pbp:/workspace/data/pbp" myapp-r-fetch:latest Rscript scripts/fetch_pbp.R "${args[@]}"
