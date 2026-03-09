#!/usr/bin/env bash
# UrbanMor — go live
#
# Restarts the tunnel (with a fresh URL) and redeploys the Vercel frontend to point at it.
# Run this any time you want the site live, including after a long break.
#
# Usage:
#   bash scripts/go_live.sh
#
# What it does:
#   1. Checks the backend is healthy
#   2. Restarts cloudflared (clears old log, gets a new tunnel URL)
#   3. Waits up to 60 s for a working tunnel
#   4. Redeploys the Vercel frontend pointing at the new tunnel URL

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TUNNEL_LOG="$PROJECT_DIR/output/qa/cloudflared.agent.err.log"
SITE_DIR="/Users/ars-e/projects/inklet-lab-site"
BACKEND_URL="http://127.0.0.1:8000"

log()  { echo "▶ $*"; }
ok()   { echo "✓ $*"; }
fail() { echo "✗ $*" >&2; exit 1; }

# ── 1. Backend health check ───────────────────────────────────────────────────
log "Checking backend..."
if ! curl -fsS -m 5 "$BACKEND_URL/health" >/dev/null; then
  echo
  echo "  Backend is not running. Start it with:"
  echo "    launchctl kickstart -k gui/\$(id -u)/com.urbanmor.api"
  echo "  Then wait ~5 s and re-run this script."
  exit 1
fi
ok "Backend healthy"

# ── 2. Restart cloudflared (fresh tunnel URL) ─────────────────────────────────
log "Restarting cloudflared tunnel..."
# Truncate the log so only the new session's URLs are in it
> "$TUNNEL_LOG"
launchctl kickstart -k gui/$(id -u)/com.urbanmor.cloudflared
ok "Cloudflared restarted"

# ── 3. Wait for a live tunnel URL ─────────────────────────────────────────────
log "Waiting for tunnel to come up (up to 60 s)..."
TUNNEL_URL=""
for i in $(seq 1 12); do
  # Pick the newest URL from the log
  candidate=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' "$TUNNEL_LOG" 2>/dev/null | tail -1 || true)
  if [[ -n "$candidate" ]] && curl -fsS -m 8 "$candidate/health" >/dev/null 2>&1; then
    TUNNEL_URL="$candidate"
    break
  fi
  echo "  attempt $i/12 — waiting 5 s..."
  sleep 5
done

if [[ -z "$TUNNEL_URL" ]]; then
  fail "Tunnel did not come up after 60 s. Check: tail -f $TUNNEL_LOG"
fi
ok "Tunnel live: $TUNNEL_URL"

# ── 4. Redeploy frontend to Vercel ────────────────────────────────────────────
log "Deploying frontend → $TUNNEL_URL ..."
cd "$SITE_DIR"
VITE_API_BASE_URL="$TUNNEL_URL" npm run sync:urbanmorph
vercel --prod --yes

echo
echo "══════════════════════════════════════════════"
echo " Live at: https://www.inkletlab.com/urbanmorph/"
echo " API via: $TUNNEL_URL"
echo "══════════════════════════════════════════════"
