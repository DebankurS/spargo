#!/usr/bin/env bash
# Local end-to-end demo: emulator (real Docker containers) + GitOps scaling with spargo.
# Usage: uv run ./examples/demo.sh   (after `uv sync --extra dev`)
set -euo pipefail
cd "$(dirname "$0")/.."
WORK=$(mktemp -d); trap 'kill $EMU 2>/dev/null; wait $EMU 2>/dev/null; rm -rf "$WORK"' EXIT

spcs-emulator --port 8085 --token demo \
  --image-map "/APPS/PUBLIC/IMAGES/whoami:latest=traefik/whoami:latest" >"$WORK/emu.log" 2>&1 &
EMU=$!
until curl -sf localhost:8085/_emu/health >/dev/null; do sleep 0.3; done
export SPARGO_ACCOUNT_URL=http://127.0.0.1:8085 SPARGO_TOKEN=demo SPARGO_CACHE="$WORK/cache"

# a "git repo" holding the desired state
cp -r examples/whoami "$WORK/repo"; cd "$WORK/repo"
git init -q -b main && git add -A && git -c user.email=d@d -c user.name=d commit -qm "3 replicas"
APP="$WORK/repo/application.yaml"

echo "### diff (nothing deployed yet)"; spargo diff "$APP" || true
echo "### sync";                        spargo sync "$APP"
EP=$(curl -s -H "Authorization: Bearer demo" -H 'Content-Type: application/json' localhost:8085/api/v2/statements \
  -d '{"statement":"SHOW ENDPOINTS IN SERVICE APPS.PUBLIC.WHOAMI"}' | python -c 'import json,sys;print(json.load(sys.stdin)["data"][0][4])')
hosts() { for _ in $(seq 40); do curl -s "$EP" | grep Hostname; done | sort -u | wc -l; }
echo "### distinct replicas answering: $(hosts)"
spargo status "$APP"

echo "### scale to 5 replicas via git commit"
sed -i 's/replicas: 3 /replicas: 5 /' services/whoami.yaml
git -c user.email=d@d -c user.name=d commit -qam "scale to 5"
spargo sync "$APP"
echo "### distinct replicas answering: $(hosts)"
spargo status "$APP"

echo "### simulate someone running ALTER SERVICE by hand (drift)"
curl -s -H "Authorization: Bearer demo" -H 'Content-Type: application/json' localhost:8085/api/v2/statements \
  -d '{"statement":"ALTER SERVICE APPS.PUBLIC.WHOAMI SET MIN_INSTANCES=1 MAX_INSTANCES=1"}' >/dev/null
spargo diff "$APP" || true
echo "### self-heal pass (controller --once)"; spargo watch "$APP" --once
spargo status "$APP"
