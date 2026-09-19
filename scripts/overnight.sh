#!/usr/bin/env bash
# Unattended full backfill: re-runs app.backfill until nothing is left to do,
# sleeping between rounds so YouTube throttling (which aborts a round) can cool off.
set -u
cd "$(dirname "$0")/.."
PY=.venv/bin/python
MAX_ROUNDS=${MAX_ROUNDS:-8}
for round in $(seq 1 "$MAX_ROUNDS"); do
  echo "=== round $round $(date -u +%FT%TZ)"
  $PY -m app.backfill --max-hours "${MAX_HOURS:-9}"
  left=$($PY -m app.backfill --stats | $PY -c "import json,sys; s=json.load(sys.stdin); print(s['pending']+s['error'])")
  echo "=== round $round finished; pending+error=$left"
  [ "$left" -eq 0 ] && break
  sleep "${ROUND_SLEEP:-1800}"
done
$PY -m app.backfill --stats
