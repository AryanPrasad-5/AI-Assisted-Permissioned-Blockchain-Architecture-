#!/usr/bin/env bash
# Detached X1 phase driver: runs cb01..cb09 in staged order, one launch each, never a retry. Start INSIDE WSL:
#   setsid nohup bash .../closed_loop_v2/ops/withnv.sh bash .../closed_loop_x1/ops/run_phase_detached_x1.sh > $OUT/x1_driver.log 2>&1 < /dev/null &
# Stops at the first non-zero launcher exit. After each run it copies the evidence to D: (verified). The file $OUT/STOP_PHASE ends the
# phase cleanly before the next launch; a run in progress is never interrupted.
# X1_LAST=cb06 ends the phase after that run (the first 6 staged runs hold exactly 2 replicates of every configuration).
OPS=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/Fabric_Live_Testbed/closed_loop_x1/ops
OUT=${X1_OUT:-/home/mailf18/e1/x1-block-20261126}
RUNS="cb01 cb02 cb03 cb04 cb05 cb06 cb07 cb08 cb09"
echo "driver start UTC $(date -u +%FT%T.%3NZ) pid $$ phase X1"
for R in $RUNS; do
  if [ -e "$OUT/STOP_PHASE" ]; then echo "STOP_PHASE present: not launching $R"; exit 6; fi
  echo "=== launch $R UTC $(date -u +%FT%T.%3NZ)"
  bash "$OPS/launch_x1.sh" "$R"; rc=$?
  echo "=== $R launcher exit $rc UTC $(date -u +%FT%T.%3NZ)"
  if [ -d "$OUT/$R" ]; then bash "$OPS/copy_evidence_x1.sh" "$R" || echo "evidence copy for $R reported a problem"; fi
  if [ "$rc" != 0 ]; then echo "PHASE STOPPED at $R (exit $rc); later runs not launched"; exit "$rc"; fi
  if [ -n "${X1_LAST:-}" ] && [ "$R" = "$X1_LAST" ]; then echo "X1_LAST=$X1_LAST reached: phase ends here by owner choice"; break; fi
done
echo "PHASE COMPLETE UTC $(date -u +%FT%T.%3NZ)"
