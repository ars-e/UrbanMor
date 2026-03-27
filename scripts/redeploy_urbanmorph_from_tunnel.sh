#!/usr/bin/env bash
set -euo pipefail

NGROK_URL="${NGROK_URL:-https://postparturient-damon-drowsily.ngrok-free.dev}"
SITE_DIR="/Users/ars-e/projects/inklet-lab-site"

if ! curl -fsS -m 8 "$NGROK_URL/health" >/dev/null; then
  echo "Stable ngrok URL is not healthy: $NGROK_URL" >&2
  echo "Start it with:" >&2
  echo "  ngrok http 8000 --url=$NGROK_URL" >&2
  exit 1
fi

if [[ "${1:-}" == "--print" ]]; then
  echo "$NGROK_URL"
  exit 0
fi

echo "Using stable ngrok URL: $NGROK_URL"
cd "$SITE_DIR"
VITE_API_BASE_URL="$NGROK_URL" npm run sync:umv1
npx vercel --prod --yes

echo "Done. /urbanmorph and /umv1 now point to: $NGROK_URL"
