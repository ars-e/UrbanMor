#!/usr/bin/env bash
set -euo pipefail

TUNNEL_LOG="/Users/ars-e/projects/UrbanMor/output/qa/cloudflared.agent.err.log"
SITE_DIR="/Users/ars-e/projects/inklet-lab-site"

if [[ ! -f "$TUNNEL_LOG" ]]; then
  echo "Tunnel log not found: $TUNNEL_LOG" >&2
  exit 1
fi

TUNNEL_CANDIDATES=()
while IFS= read -r candidate; do
  TUNNEL_CANDIDATES+=("$candidate")
done < <(rg -o 'https://[a-z0-9-]+\.trycloudflare\.com' -N "$TUNNEL_LOG" | awk '!seen[$0]++')
if [[ ${#TUNNEL_CANDIDATES[@]} -eq 0 ]]; then
  echo "Could not detect tunnel URL from log: $TUNNEL_LOG" >&2
  exit 1
fi

TUNNEL_URL=""
for (( idx=${#TUNNEL_CANDIDATES[@]}-1; idx>=0; idx-- )); do
  candidate="${TUNNEL_CANDIDATES[$idx]}"
  if curl -fsS -m 8 "$candidate/health" >/dev/null; then
    TUNNEL_URL="$candidate"
    break
  fi
done

if [[ -z "$TUNNEL_URL" ]]; then
  echo "No healthy tunnel URL found in $TUNNEL_LOG" >&2
  printf 'Checked URLs (newest first):\n' >&2
  for (( idx=${#TUNNEL_CANDIDATES[@]}-1; idx>=0; idx-- )); do
    printf '  %s\n' "${TUNNEL_CANDIDATES[$idx]}" >&2
  done
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
