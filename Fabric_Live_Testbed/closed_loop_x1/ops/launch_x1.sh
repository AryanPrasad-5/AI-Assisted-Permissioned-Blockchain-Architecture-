#!/usr/bin/env bash
# X1 launcher. One authorized run, exactly once, no retry. Run via closed_loop_v2/ops/withnv.sh (inside WSL).
#   Usage: launch_x1.sh <run id>      cb01 .. cb09
# Derived from closed_loop_v2/ops/launch_v2.sh. Gates, in order:
#   1. owner-signed AUTH_X1.txt accepted by check_auth_x1.py (runs, channels, stage hash, targets 10 (inert), max_broadcasts 0)
#   2. the previous run has a CONTINUE decision (the first run has no predecessor)
#   3. preflight (containers, memory >= 1,500 MB, disk), 6/6 containers running with 0 restarts, Caliper idle, runner import
#   4. enough authorization time left for one run (>= 25 min)
#   5. the source files equal the freeze the stage manifest pins (source_hashes_frozen_x1.txt)
# After the run: container logs, independent verification (verify_x1.py), continuation decision (closed_loop_v2/ops/continue_check_v2.py).
set -u
R=${1:?run id cb01..cb09}
case "$R" in cb0[1-9]) ;; *) echo "refusing: $R is not an X1 run"; exit 2;; esac
OUT=${X1_OUT:-/home/mailf18/e1/x1-block-20261126}
export X1_OUT="$OUT"
S=${X1_RECORDS:-/mnt/d/PLAN_BLOCK18/x1_block_size_headroom_2026-11-25}
REPO=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-
OPS=$REPO/Fabric_Live_Testbed/closed_loop_x1/ops
V2OPS=$REPO/Fabric_Live_Testbed/closed_loop_v2/ops
AUTH=$S/AUTH_X1.txt
STAGE=$S/stage_x1
SELF=$OPS/launch_x1.sh
LOG="$OUT/${R}_runner_console.log"
mkdir -p "$OUT" "$OUT/decisions_x1"
for p in "$LOG" "$OUT/$R" "/home/mailf18/e1/caliper-ws-benchmark/runs/$R" "$OUT/${R}_fabric_logs" "$OUT/${R}_verification" "$OUT/decisions_x1/$R.json"; do
  [ -e "$p" ] && { echo "refusing: $p exists ($R already launched or path reused)"; exit 2; }
done
N=$((10#${R:2})); PREV=$(printf '%s%02d' "${R:0:2}" $((N-1)))
G="$OUT/${R}_gate.log"
{
echo "launcher_x1 sha256 $(sha256sum "$SELF" | cut -d' ' -f1)  gate UTC $(date -u +%FT%T.%3NZ)"
python3 -B "$OPS/check_auth_x1.py" "$AUTH" "$STAGE" || { echo "GATE FAIL: authorization"; exit 4; }
if [ "$N" -gt 1 ]; then
python3 -B - "$OUT/decisions_x1/$PREV.json" "$PREV" <<'PYEOF' || { echo "GATE FAIL: previous run $PREV not CONTINUE"; exit 4; }
import json, sys, hashlib
from pathlib import Path
p = Path(sys.argv[1])
if not p.exists():
    print("previous decision file absent:", p); sys.exit(1)
d = json.loads(p.read_text())
print("previous", sys.argv[2], "decision", d.get("decision"), "| verdict", d.get("recorded_runner_verdict"), "| flags", d.get("recorded_flags"),
      "| sha256", hashlib.sha256(p.read_bytes()).hexdigest())
sys.exit(0 if d.get("run_id") == sys.argv[2] and d.get("decision") == "CONTINUE" else 1)
PYEOF
else echo "previous-run gate: first run of the phase, no predecessor"; fi
bash Fabric_Live_Testbed/closed_loop/preflight_check.sh > "$OUT/${R}_preflight.log" 2>&1
st=$(docker inspect orderer.example.com orderer2.example.com orderer3.example.com peer0.org1.example.com peer0.org2.example.com caliper-benchmark-001 \
  --format '{{.Name}} running={{.State.Running}} restarts={{.RestartCount}}')
echo "$st" | tee -a "$OUT/${R}_preflight.log"
if [ "$(echo "$st" | grep -c 'running=true restarts=0')" != 6 ]; then echo "GATE FAIL: containers"; exit 4; fi
bash "$REPO/Fabric_Live_Testbed/closed_loop/series_ops_v2/caliper_idle_check.sh" caliper-benchmark-001 || { echo "GATE FAIL: caliper not idle"; exit 4; }
grep -q 'runner import OK' "$OUT/${R}_preflight.log" || { echo "GATE FAIL: runner import"; exit 4; }
python3 -B -c "
import datetime as d,sys
e=d.datetime.strptime(open('$AUTH').read().split('expires_utc=')[1][:20],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=d.timezone.utc)
left=(e-d.datetime.now(d.timezone.utc)).total_seconds(); print('AUTH seconds left', int(left)); sys.exit(0 if left>=1500 else 1)" \
  || { echo "GATE FAIL: authorization expires in < 25 min"; exit 4; }
bash "$OPS/freeze_sources_x1.sh" > "$OUT/${R}_source_hashes.txt" || { echo "GATE FAIL: freeze script"; exit 4; }
if ! diff -q "$OUT/${R}_source_hashes.txt" "$S/source_hashes_frozen_x1.txt" >/dev/null; then echo "GATE FAIL: source changed"; diff "$OUT/${R}_source_hashes.txt" "$S/source_hashes_frozen_x1.txt"; exit 4; fi
python3 -B -c "
import json,hashlib,sys
m=json.load(open('$STAGE/STAGE_MANIFEST.json'))
h=hashlib.sha256(open('$S/source_hashes_frozen_x1.txt','rb').read()).hexdigest()
print('stage pins source freeze', m.get('source_freeze_sha256'), 'file', h); sys.exit(0 if m.get('source_freeze_sha256')==h else 1)" \
  || { echo "GATE FAIL: stage does not pin this source freeze"; exit 4; }
echo "ALL GATES PASS"
} 2>&1 | tee "$G"
[ "${PIPESTATUS[0]}" = 0 ] || exit 4
grep -q '^ALL GATES PASS$' "$G" || exit 4
# --- run, exactly once ---
T0=$(date -u +%FT%TZ)
echo "launch UTC $(date -u +%FT%T.%3NZ)" > "$LOG"
python3 -B -m Fabric_Live_Testbed.closed_loop_v2.runner_v2 --stage "$STAGE" --run-id "$R" \
  --authorization "$AUTH" --out-root "$OUT" --execute >> "$LOG" 2>&1
rc=$?
echo "runner exit code $rc  end UTC $(date -u +%FT%T.%3NZ)" >> "$LOG"
sleep 3
T1=$(date -u +%FT%TZ)
CONT="orderer.example.com orderer2.example.com orderer3.example.com peer0.org1.example.com peer0.org2.example.com caliper-benchmark-001"
L="$OUT/${R}_fabric_logs"; mkdir -p "$L"
for n in $CONT; do docker logs --timestamps --since "$T0" --until "$T1" "$n" > "$L/$n.log" 2>&1; done
docker inspect $CONT --format '{{.Name}} running={{.State.Running}} restarts={{.RestartCount}} started={{.State.StartedAt}}' > "$L/containers_after.txt"
tail -3 "$LOG"
if [ -f "$OUT/$R/run_result.json" ]; then
  python3 -B "$OPS/verify_x1.py" "$R" "$STAGE" 2>&1 | tee "$OUT/${R}_verify_stdout.txt"
  python3 -B "$V2OPS/continue_check_v2.py" "$R" --out "$OUT" --decisions "$OUT/decisions_x1" --stage "$STAGE" --auth "$AUTH" 2>&1 | tee "$OUT/${R}_continue_stdout.txt"
  dec=${PIPESTATUS[0]}
else
  echo "no run_result.json -> STOP"; dec=5
fi
echo "LAUNCH_DONE $R rc=$rc continue_exit=$dec"
[ "$rc" = 0 ] && [ "$dec" = 0 ] && exit 0
exit 5
