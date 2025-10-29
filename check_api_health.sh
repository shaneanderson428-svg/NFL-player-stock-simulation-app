#!/usr/bin/env bash
# Simple health check for /api/nfl/stocks on local Next dev server
set -euo pipefail

HOSTS=("127.0.0.1" "localhost")
PORTS=(3000 3001)
TARGET_PATH="/api/nfl/stocks"

found=""
url=""

for h in "${HOSTS[@]}"; do
  for p in "${PORTS[@]}"; do
    if curl -sS --connect-timeout 1 "http://${h}:${p}${TARGET_PATH}" >/dev/null 2>&1; then
      found="${h}:${p}"
      url="http://${h}:${p}${TARGET_PATH}"
      break 2
    fi
  done
done

if [ -z "$found" ]; then
  echo "⚠️  Dev server not running. Start with npm run dev"
  exit 1
fi

# Perform request and capture status
http_status=$(curl -sS -o /tmp/check_api_health_resp.json -w "%{http_code}" "$url") || true

if [ "$http_status" = "200" ]; then
  echo "✅ API is healthy ($url)"
  # pretty-print JSON
  if command -v python3 >/dev/null 2>&1; then
    python3 -m json.tool /tmp/check_api_health_resp.json || cat /tmp/check_api_health_resp.json
  else
    cat /tmp/check_api_health_resp.json
  fi
  exit 0
elif [ "$http_status" = "500" ]; then
  echo "❌ API returned 500 – check route.ts logs"
  echo "Response body:"
  cat /tmp/check_api_health_resp.json || true
  exit 1
else
  echo "❌ Unexpected HTTP status: $http_status"
  echo "Response body:"
  cat /tmp/check_api_health_resp.json || true
  exit 1
fi
