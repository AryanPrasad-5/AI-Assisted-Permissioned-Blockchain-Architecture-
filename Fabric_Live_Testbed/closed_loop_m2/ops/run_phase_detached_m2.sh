#!/usr/bin/env bash
# Detached phase driver (M2): runs one phase's runs in staged order, one launch each, never a retry. Start INSIDE WSL (a detached process:
# a background wsl.exe child can be killed by the host, as happened to cl04):
#   setsid nohup bash .../closed_loop_v2/ops/withnv.sh bash .../closed_loop_m2/ops/run_phase_detached_m2.sh m2 > $OUT/m2_driver.log 2>&1 < /dev/null &
# Stops at the first non-zero launcher exit. After each run it copies the evidence to D: (verified). The file $OUT/STOP_PHASE ends the
# phase cleanly before the next launch; a run in progress is never interrupted. M2_LAST=cc08 ends the phase after that run (2 replicates).
PH=${1:?phase m2|act}
OPS=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/Fabric_Live_Testbed/closed_loop_m2/ops
OUT=${M2_OUT:-/home/mailf18/e1/m2-comparison}
export M2_OUT="$OUT"
case "$PH" in act) RUNS="ca01";; m2) RUNS="cc01 cc02 cc03 cc04 cc05 cc06 cc07 cc08 cc09 cc10 cc11 cc12";; *) echo "phase?"; exit 2;; esac
echo "driver start UTC $(date -u +%FT%T.%3NZ) pid $$ phase $PH"
for R in $RUNS; do
  if [ -e "$OUT/STOP_PHASE" ]; then echo "STOP_PHASE present: not launching $R"; exit 6; fi
  echo "=== launch $R UTC $(date -u +%FT%T.%3NZ)"
  bash "$OPS/launch_m2.sh" "$PH" "$R"; rc=$?
  echo "=== $R launcher exit $rc UTC $(date -u +%FT%T.%3NZ)"
  if [ -d "$OUT/$R" ]; then bash "$OPS/copy_evidence_m2.sh" "$R" || echo "evidence copy for $R reported a problem"; fi
  if [ "$rc" != 0 ]; then echo "PHASE STOPPED at $R (exit $rc); later runs not launched"; exit "$rc"; fi
  if [ -n "${M2_LAST:-}" ] && [ "$R" = "$M2_LAST" ]; then echo "M2_LAST=$M2_LAST reached: phase ends here by owner choice"; break; fi
done
echo "PHASE COMPLETE UTC $(date -u +%FT%T.%3NZ)"
