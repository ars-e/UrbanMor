#!/usr/bin/env bash
set -euo pipefail

TUNNEL_LOG="/Users/ars-e/projects/UrbanMor/output/qa/cloudflared.agent.err.log"
SITE_DIR="/Users/ars-e/projects/inklet-lab-site"

if [[ ! -f "$TUNNEL_LOG" ]]; then
  echo "Tunnel log not found: $TUNNEL_LOG" >&2
  exit 1
fi

TUNNEL_URL="$(rg -o 'https://[a-z0-9-]+\.trycloudflare\.com' -N "$TUNNEL_LOG" | tail -n 1)"
if [[ -z "$TUNNEL_URL" ]]; then
  echo "Could not detect tunnel URL from log: $TUNNEL_LOG" >&2
  exit 1
fi

if ! curl -fsS "$TUNNEL_URL/health" >/dev/null; then
  echo "Tunnel URL is not healthy: $TUNNEL_URL" >&2
  exit 1
fi

if [[ "${1:-}" == "--print" ]]; then
  echo "$TUNNEL_URL"
  exit 0
fi

echo "Using tunnel URL: $TUNNEL_URL"
cd "$SITE_DIR"
VITE_API_BASE_URL="$TUNNEL_URL" npm run sync:urbanmorph
vercel --prod --yes

echo "Done. /urbanmorph now points to: $TUNNEL_URL"
