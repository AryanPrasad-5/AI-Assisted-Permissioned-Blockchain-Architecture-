#!/usr/bin/env bash
# M2 launcher (protocol M2-v1). One authorized run, exactly once, no retry. Run via withnv.sh (inside WSL).
#   Usage: launch_m2.sh <m2|act> <run id>      act -> ca01 (ACTUATION_CHECK) ; m2 -> cc01..cc12
# Derived from closed_loop/series_ops_v2/launch_run_v3.sh. Gates, in order:
#   1. owner-signed AUTH_<PHASE>.txt accepted by check_auth_m2.py (runs, channels, stage hash, broadcast limits)
#   2. the previous run of the phase has a CONTINUE decision (the first run of a phase has no predecessor)
#   3. preflight (containers, memory >= 1,500 MB, disk), 6/6 containers running with 0 restarts, Caliper idle, runner import
#   4. enough authorization time left for one run (>= 25 min)
#   5. the source files equal the freeze the stage manifest pins (source_hashes_frozen_m2.txt)
# After the run: container logs, independent verification (verify_m2.py), continuation decision (continue_check_v2.py, unchanged).
set -u
PH=${1:?phase m2|act}
R=${2:?run id}
case "$PH:$R" in act:ca01|m2:cc0[1-9]|m2:cc1[0-2]) ;; *) echo "refusing: $R is not a $PH run"; exit 2;; esac
OUT=${M2_OUT:-/home/mailf18/e1/m2-comparison}
export M2_OUT="$OUT"
S=/mnt/d/PLAN_BLOCK18/three_controller_comparison_2026-11-25
OPS=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/Fabric_Live_Testbed/closed_loop_m2/ops
OPS_V2=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/Fabric_Live_Testbed/closed_loop_v2/ops
AUTH=$S/AUTH_$(echo "$PH" | tr a-z A-Z).txt
STAGE=$S/stage_$PH
SELF=$OPS/launch_m2.sh
LOG="$OUT/${R}_runner_console.log"
mkdir -p "$OUT" "$OUT/decisions_m2"
for p in "$LOG" "$OUT/$R" "/home/mailf18/e1/caliper-ws-benchmark/runs/$R" "$OUT/${R}_fabric_logs" "$OUT/${R}_verification" "$OUT/decisions_m2/$R.json"; do
  [ -e "$p" ] && { echo "refusing: $p exists ($R already launched or path reused)"; exit 2; }
done
N=$((10#${R:2})); PREV=$(printf '%s%02d' "${R:0:2}" $((N-1)))
G="$OUT/${R}_gate.log"
{
echo "launcher_m2 sha256 $(sha256sum "$SELF" | cut -d' ' -f1)  gate UTC $(date -u +%FT%T.%3NZ)"
python3 -B "$OPS/check_auth_m2.py" "$PH" "$AUTH" "$STAGE" || { echo "GATE FAIL: authorization"; exit 4; }
if [ "$N" -gt 1 ]; then
python3 -B - "$OUT/decisions_m2/$PREV.json" "$PREV" <<'PYEOF' || { echo "GATE FAIL: previous run $PREV not CONTINUE"; exit 4; }
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
elif [ "$PH" = m2 ]; then
python3 -B - "$OUT/decisions_m2/ca01.json" <<'PYEOF' || { echo "GATE FAIL: the ACTUATION_CHECK run ca01 has no CONTINUE decision (the comparison needs the 2/5 switch path proven first)"; exit 4; }
import json, sys, hashlib
from pathlib import Path
p = Path(sys.argv[1])
if not p.exists():
    print("ca01 decision file absent:", p); sys.exit(1)
d = json.loads(p.read_text())
print("ca01 decision", d.get("decision"), "| verdict", d.get("recorded_runner_verdict"), "| sha256", hashlib.sha256(p.read_bytes()).hexdigest())
sys.exit(0 if d.get("run_id") == "ca01" and d.get("decision") == "CONTINUE" else 1)
PYEOF
else echo "previous-run gate: first run of the phase, no predecessor"; fi
bash Fabric_Live_Testbed/closed_loop/preflight_check.sh > "$OUT/${R}_preflight.log" 2>&1
st=$(docker inspect orderer.example.com orderer2.example.com orderer3.example.com peer0.org1.example.com peer0.org2.example.com caliper-benchmark-001 \
  --format '{{.Name}} running={{.State.Running}} restarts={{.RestartCount}}')
echo "$st" | tee -a "$OUT/${R}_preflight.log"
if [ "$(echo "$st" | grep -c 'running=true restarts=0')" != 6 ]; then echo "GATE FAIL: containers"; exit 4; fi
bash /mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/Fabric_Live_Testbed/closed_loop/series_ops_v2/caliper_idle_check.sh caliper-benchmark-001 || { echo "GATE FAIL: caliper not idle"; exit 4; }
grep -q 'runner import OK' "$OUT/${R}_preflight.log" || { echo "GATE FAIL: runner import"; exit 4; }
python3 -B -c "
import datetime as d,sys
e=d.datetime.strptime(open('$AUTH').read().split('expires_utc=')[1][:20],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=d.timezone.utc)
left=(e-d.datetime.now(d.timezone.utc)).total_seconds(); print('AUTH seconds left', int(left)); sys.exit(0 if left>=1500 else 1)" \
  || { echo "GATE FAIL: authorization expires in < 25 min"; exit 4; }
bash "$OPS/freeze_sources_m2.sh" > "$OUT/${R}_source_hashes.txt" || { echo "GATE FAIL: freeze script"; exit 4; }
if ! diff -q "$OUT/${R}_source_hashes.txt" "$S/source_hashes_frozen_m2.txt" >/dev/null; then echo "GATE FAIL: source changed"; diff "$OUT/${R}_source_hashes.txt" "$S/source_hashes_frozen_m2.txt"; exit 4; fi
python3 -B -c "
import json,hashlib,sys
m=json.load(open('$STAGE/STAGE_MANIFEST.json'))
h=hashlib.sha256(open('$S/source_hashes_frozen_m2.txt','rb').read()).hexdigest()
print('stage pins source freeze', m.get('source_freeze_sha256'), 'file', h); sys.exit(0 if m.get('source_freeze_sha256')==h else 1)" \
  || { echo "GATE FAIL: stage does not pin this source freeze"; exit 4; }
echo "ALL GATES PASS"
} 2>&1 | tee "$G"
[ "${PIPESTATUS[0]}" = 0 ] || exit 4
grep -q '^ALL GATES PASS$' "$G" || exit 4
# --- run, exactly once ---
T0=$(date -u +%FT%TZ)
echo "launch UTC $(date -u +%FT%T.%3NZ)" > "$LOG"
python3 -B -m Fabric_Live_Testbed.closed_loop_m2.runner_m2 --stage "$STAGE" --run-id "$R" \
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
  python3 -B "$OPS/verify_m2.py" "$R" "$STAGE" 2>&1 | tee "$OUT/${R}_verify_stdout.txt"
  python3 -B "$OPS_V2/continue_check_v2.py" "$R" --out "$OUT" --decisions "$OUT/decisions_m2" --stage "$STAGE" --auth "$AUTH" 2>&1 | tee "$OUT/${R}_continue_stdout.txt"
  dec=${PIPESTATUS[0]}
else
  echo "no run_result.json -> STOP"; dec=5
fi
echo "LAUNCH_DONE $R rc=$rc continue_exit=$dec"
[ "$rc" = 0 ] && [ "$dec" = 0 ] && exit 0
exit 5
