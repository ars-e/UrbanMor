#!/usr/bin/env bash
# UrbanMor — go live
#
# Ensures the stable ngrok LaunchAgent is serving the local backend, then
# redeploys the Vercel frontend to point at it.
# Run this any time you want the site live, including after a long break.
#
# Usage:
#   bash scripts/go_live.sh
#
# What it does:
#   1. Checks the backend is healthy
#   2. Restarts the ngrok LaunchAgent on the stable public domain
#   3. Waits up to 60 s for a working tunnel
#   4. Redeploys the Vercel frontend pointing at the stable ngrok URL

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NGROK_STDOUT_LOG="$PROJECT_DIR/output/qa/ngrok.agent.out.log"
NGROK_STDERR_LOG="$PROJECT_DIR/output/qa/ngrok.agent.err.log"
SITE_DIR="/Users/ars-e/projects/inklet-lab-site"
BACKEND_URL="http://127.0.0.1:8000"
NGROK_URL="${NGROK_URL:-https://postparturient-damon-drowsily.ngrok-free.dev}"
NGROK_LABEL="com.urbanmor.ngrok"
LAUNCHD_TARGET="gui/$(id -u)/$NGROK_LABEL"

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

# ── 2. Restart the ngrok LaunchAgent on the stable domain ─────────────────────
if ! launchctl print "$LAUNCHD_TARGET" >/dev/null 2>&1; then
  fail "ngrok LaunchAgent is not installed. Expected: ~/Library/LaunchAgents/com.urbanmor.ngrok.plist"
fi
log "Restarting ngrok LaunchAgent..."
launchctl kickstart -k "$LAUNCHD_TARGET"
ok "ngrok LaunchAgent restarted"

# ── 3. Wait for the stable ngrok URL to come up ───────────────────────────────
log "Waiting for ngrok to come up on $NGROK_URL (up to 60 s)..."
for i in $(seq 1 12); do
  if curl -fsS -m 8 "$NGROK_URL/health" >/dev/null 2>&1; then
    break
  fi
  echo "  attempt $i/12 — waiting 5 s..."
  sleep 5
done

if ! curl -fsS -m 8 "$NGROK_URL/health" >/dev/null 2>&1; then
  fail "ngrok did not come up after 60 s. Check: tail -f $NGROK_STDOUT_LOG and tail -f $NGROK_STDERR_LOG"
fi
ok "ngrok live: $NGROK_URL"

# ── 4. Redeploy frontend to Vercel ────────────────────────────────────────────
log "Deploying frontend → $NGROK_URL ..."
cd "$SITE_DIR"
VITE_API_BASE_URL="$NGROK_URL" npm run sync:umv1
npx vercel --prod --yes

echo
echo "══════════════════════════════════════════════"
echo " Live at: https://www.inkletlab.com/urbanmorph/"
echo " Live at: https://www.inkletlab.com/umv1/"
echo " API via: $NGROK_URL"
echo "══════════════════════════════════════════════"
