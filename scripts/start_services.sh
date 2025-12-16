#!/usr/bin/env bash
set -euo pipefail
# Start the Next.js dev server in a resilient way. Prefer pm2 if available,
# otherwise fall back to nohup (detached) with logs written to .dev-server.log

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."
cd "$ROOT_DIR"

if command -v pm2 >/dev/null 2>&1; then
  echo "Starting Next dev with pm2..."
  pm2 start npm --name my-app-dev -- run dev --output .dev-server.log --error .dev-server.log
  pm2 save
  echo "pm2 started. Use 'pm2 logs my-app-dev' to view logs."
else
  echo "pm2 not found; starting Next dev with nohup..."
  nohup npm run dev > .dev-server.log 2>&1 &
  echo $! > .dev-server.pid
  echo "Started (pid=$(cat .dev-server.pid)). Logs: .dev-server.log"
fi

echo "Start script finished."
