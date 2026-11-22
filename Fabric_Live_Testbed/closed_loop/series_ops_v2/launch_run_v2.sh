#!/usr/bin/env bash
# Launcher v2 (S5-A1, post-cl01). One authorized series run, exactly once, no retry. Run via withnv.sh.
# Usage: launch_run_v2.sh clNN   (cl02..cl12 only; cl01 is never re-run)
# Differences from the as-run cl01 launcher (sha256 35624086...):
#   1. Caliper-idle gate = caliper_idle_check.sh (the cl01 test used `docker top -o args`, which errors, so it could not fail)
#   2. Gate: owner-written AUTH_S5_AMENDMENT.txt accepted by check_s5_amendment.py (binds this file's sha256)
#   3. Gate: the previous run's S5-A1 decision file exists and says CONTINUE
#   4. Gate: AUTH_SERIES.txt must have >= 25 min left (a run takes ~14 min; broadcasts need an unexpired file)
#   5. After verify_run.py: s5a1_continuation_check.py writes this run's decision (exit 5 = STOP)
# Unchanged: preflight, container gate, channel-absent gate, project auth parser + require_run, frozen source hashes, the
# runner command line, container-log preservation, verify_run.py (byte-identical copy of the cl01 version).
set -u
R=$1
case "$R" in cl0[2-9]|cl1[0-2]) ;; *) echo "refusing: $R is not cl02..cl12"; exit 2;; esac
OUT=/home/mailf18/e1/closed-loop-20261123
S=/mnt/d/PLAN_BLOCK18/closed_loop_experiment_2026-11-22
OPS=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/Fabric_Live_Testbed/closed_loop/series_ops_v2
SELF=$OPS/launch_run_v2.sh
LOG="$OUT/${R}_runner_console.log"
for p in "$LOG" "$OUT/$R" "/home/mailf18/e1/caliper-ws-benchmark/runs/$R" "$OUT/${R}_fabric_logs" "$OUT/${R}_verification" "$OUT/s5a1_decisions/$R.json"; do
  [ -e "$p" ] && { echo "refusing: $p exists ($R already launched or path reused)"; exit 2; }
done
N=$((10#${R#cl})); PREV=$(printf 'cl%02d' $((N-1)))
G="$OUT/${R}_gate_v2.log"
{
echo "launcher_v2 sha256 $(sha256sum "$SELF" | cut -d' ' -f1)  gate UTC $(date -u +%FT%T.%3NZ)"
# --- S5-A1 gates ---
python3 -B "$OPS/check_s5_amendment.py" "$S/AUTH_S5_AMENDMENT.txt" "$R" "$SELF" || { echo "GATE FAIL: S5-A1 approval"; exit 4; }
python3 -B - "$OUT/s5a1_decisions/$PREV.json" "$PREV" <<'EOF' || { echo "GATE FAIL: previous run $PREV not CONTINUE"; exit 4; }
import json, sys, hashlib
from pathlib import Path
p = Path(sys.argv[1])
if not p.exists():
    print("previous decision file absent:", p); sys.exit(1)
d = json.loads(p.read_text())
print("previous", sys.argv[2], "decision", d.get("decision"), "| recorded verdict", d.get("recorded_analysis_verdict"),
      "| flags", d.get("recorded_flags"), "| sha256", hashlib.sha256(p.read_bytes()).hexdigest())
sys.exit(0 if d.get("run_id") == sys.argv[2] and d.get("decision") == "CONTINUE" and d.get("policy") == "S5-A1" else 1)
EOF
# --- pre-run gate (as cl01) ---
bash Fabric_Live_Testbed/closed_loop/preflight_check.sh > "$OUT/${R}_preflight.log" 2>&1
st=$(docker inspect orderer.example.com orderer2.example.com orderer3.example.com peer0.org1.example.com peer0.org2.example.com caliper-benchmark-001 \
  --format '{{.Name}} running={{.State.Running}} restarts={{.RestartCount}}')
echo "$st" | tee -a "$OUT/${R}_preflight.log"
if [ "$(echo "$st" | grep -c 'running=true restarts=0')" != 6 ]; then echo "GATE FAIL: containers"; exit 4; fi
bash "$OPS/caliper_idle_check.sh" caliper-benchmark-001 || { echo "GATE FAIL: caliper not idle"; exit 4; }
if sed -n '/existing e1-cl/,/runner import/p' "$OUT/${R}_preflight.log" | grep -qx "e1-$R"; then echo "GATE FAIL: channel e1-$R exists"; exit 4; fi
grep -q 'runner import OK' "$OUT/${R}_preflight.log" || { echo "GATE FAIL: runner import"; exit 4; }
python3 -B "$OPS/check_auth_series.py" "$S/AUTH_SERIES.txt" "$S/stage_series/SHA256SUMS.txt" > "$OUT/${R}_auth_check.log" 2>&1 || { echo "GATE FAIL: auth"; cat "$OUT/${R}_auth_check.log"; exit 4; }
cat "$OUT/${R}_auth_check.log"
python3 -B -c "
import datetime as d,sys
e=d.datetime.strptime(open('$S/AUTH_SERIES.txt').read().split('expires_utc=')[1][:20],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=d.timezone.utc)
left=(e-d.datetime.now(d.timezone.utc)).total_seconds(); print('AUTH_SERIES seconds left', int(left)); sys.exit(0 if left>=1500 else 1)" \
  || { echo "GATE FAIL: AUTH_SERIES.txt expires in < 25 min"; exit 4; }
sha256sum scripts/e1/e1_config_actuator.py scripts/e1/e1_independent_config_verify.py scripts/e1/e1_channel_run.py \
  scripts/e1/e1_block_fetch.py scripts/e1/run_fabric_config_verifier.sh tools/fabric_config_verifier/main.go \
  Fabric_Live_Testbed/closed_loop/*.py src/fabric_perf/controllers/channel_selection.py src/fabric_perf/controllers/strategies.py \
  src/fabric_perf/controllers/base.py > "$OUT/${R}_source_hashes.txt"
if ! diff -q "$OUT/${R}_source_hashes.txt" "$OPS/source_hashes_frozen.txt" >/dev/null; then echo "GATE FAIL: source changed"; diff "$OUT/${R}_source_hashes.txt" "$OPS/source_hashes_frozen.txt"; exit 4; fi
echo "ALL GATES PASS"
} 2>&1 | tee "$G"
[ "${PIPESTATUS[0]}" = 0 ] || exit 4
grep -q '^ALL GATES PASS$' "$G" || exit 4
# --- run, exactly once ---
T0=$(date -u +%FT%TZ)
echo "launch UTC $(date -u +%FT%T.%3NZ)" > "$LOG"
python3 -B -m Fabric_Live_Testbed.closed_loop.runner --stage "$S/stage_series" --run-id "$R" \
  --authorization "$S/AUTH_SERIES.txt" --out-root "$OUT" --execute >> "$LOG" 2>&1
rc=$?
echo "runner exit code $rc  end UTC $(date -u +%FT%T.%3NZ)" >> "$LOG"
sleep 3
T1=$(date -u +%FT%TZ)
# --- preserve container logs for the run window (read-only) ---
CONT="orderer.example.com orderer2.example.com orderer3.example.com peer0.org1.example.com peer0.org2.example.com caliper-benchmark-001"
L="$OUT/${R}_fabric_logs"; mkdir -p "$L"
for n in $CONT; do docker logs --timestamps --since "$T0" --until "$T1" "$n" > "$L/$n.log" 2>&1; done
docker inspect $CONT --format '{{.Name}} running={{.State.Running}} restarts={{.RestartCount}} started={{.State.StartedAt}}' > "$L/containers_after.txt"
tail -3 "$LOG"
# --- independent verification, then the S5-A1 continuation decision ---
if [ -f "$OUT/$R/run_result.json" ]; then
  python3 -B "$OPS/verify_run.py" "$R" 2>&1 | tee "$OUT/${R}_verify_stdout.txt"
  python3 -B "$OPS/s5a1_continuation_check.py" "$R" 2>&1 | tee "$OUT/${R}_s5a1_stdout.txt"
  dec=${PIPESTATUS[0]}
else
  echo "no run_result.json -> STOP"; dec=5
fi
echo "LAUNCH_DONE $R rc=$rc s5a1_exit=$dec"
[ "$rc" = 0 ] && [ "$dec" = 0 ] && exit 0
exit 5
