#!/usr/bin/env bash
# Detached phase driver (v2): runs one phase's runs in staged order, one launch each, never a retry. Start INSIDE WSL:
#   setsid nohup bash .../ops/withnv.sh bash .../ops/run_phase_detached.sh x0 > $OUT/x0_driver.log 2>&1 < /dev/null &
# Stops at the first non-zero launcher exit. After each run it copies the evidence to D: (verified). The file $OUT/STOP_PHASE ends the
# phase cleanly before the next launch; a run in progress is never interrupted.
PH=${1:?phase x0|m}
OPS=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/Fabric_Live_Testbed/closed_loop_v2/ops
OUT=${LPC_OUT:-/home/mailf18/e1/lpc-20261126}
case "$PH" in x0) RUNS="cx01 cx02 cx03 cx04";; m) RUNS="cm01 cm02 cm03 cm04 cm05 cm06 cm07 cm08 cm09 cm10 cm11 cm12";; *) echo "phase?"; exit 2;; esac
# optional: LPC_LAST=cm08 stops the phase after that run (n = 2 replicates = first 8 staged runs ONLY if the stage was made with 2 replicates)
echo "driver start UTC $(date -u +%FT%T.%3NZ) pid $$ phase $PH"
for R in $RUNS; do
  if [ -e "$OUT/STOP_PHASE" ]; then echo "STOP_PHASE present: not launching $R"; exit 6; fi
  echo "=== launch $R UTC $(date -u +%FT%T.%3NZ)"
  bash "$OPS/launch_v2.sh" "$PH" "$R"; rc=$?
  echo "=== $R launcher exit $rc UTC $(date -u +%FT%T.%3NZ)"
  if [ -d "$OUT/$R" ]; then bash "$OPS/copy_evidence_v2.sh" "$R" || echo "evidence copy for $R reported a problem"; fi
  if [ "$rc" != 0 ]; then echo "PHASE STOPPED at $R (exit $rc); later runs not launched"; exit "$rc"; fi
  if [ -n "${LPC_LAST:-}" ] && [ "$R" = "$LPC_LAST" ]; then echo "LPC_LAST=$LPC_LAST reached: phase ends here by owner choice"; break; fi
done
echo "PHASE COMPLETE UTC $(date -u +%FT%T.%3NZ)"
