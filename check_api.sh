#!/usr/bin/env bash
set -euo pipefail

# check_api.sh - check Next.js /api/nfl/stocks endpoint on 3000/3001 and pretty-print JSON
# Usage: ./check_api.sh

HOST="127.0.0.1"
PORTS=(3000 3001)
PATH_URL="/api/nfl/stocks"

TMPRESP=""
for PORT in "${PORTS[@]}"; do
  URL="http://${HOST}:${PORT}${PATH_URL}"
  # try a quick curl; --fail makes curl return non-zero on HTTP >=400
  if curl -sS --fail --max-time 3 "$URL" -o /tmp/check_api_response 2>/dev/null; then
    echo "Found Next.js dev server on port ${PORT} — fetching ${PATH_URL} ..."
    python3 -m json.tool /tmp/check_api_response || cat /tmp/check_api_response
    rm -f /tmp/check_api_response
    exit 0
  fi
done

echo "⚠️ Next.js dev server not running. Start it with npm run dev."
exit 1
