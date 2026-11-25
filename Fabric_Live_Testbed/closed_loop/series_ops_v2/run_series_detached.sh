#!/usr/bin/env bash
# Detached series driver (S5-A2): runs cl05..cl12 in the seeded order, one launch each, never a retry.
# Start INSIDE WSL so a host-side kill of wsl.exe cannot orphan Caliper (the cl04 failure):
#   setsid nohup bash .../withnv.sh bash .../run_series_detached.sh > $OUT/series_a2_driver.log 2>&1 < /dev/null &
# Stops at the first run whose launcher exits non-zero (gate failure, S5 STOP, runner failure). Never waives or retries.
# After each run it makes the verified evidence copy on D:. The STOP file /home/mailf18/e1/closed-loop-20261123/STOP_SERIES
# (create it with `touch`) ends the series cleanly before the next launch; a run in progress is never interrupted.
OPS=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/Fabric_Live_Testbed/closed_loop/series_ops_v2
OUT=/home/mailf18/e1/closed-loop-20261123
echo "driver start UTC $(date -u +%FT%T.%3NZ) pid $$"
for R in cl05 cl06 cl07 cl08 cl09 cl10 cl11 cl12; do
  if [ -e "$OUT/STOP_SERIES" ]; then echo "STOP_SERIES present: not launching $R"; exit 6; fi
  echo "=== launch $R UTC $(date -u +%FT%T.%3NZ)"
  bash "$OPS/launch_run_v3.sh" "$R"; rc=$?
  echo "=== $R launcher exit $rc UTC $(date -u +%FT%T.%3NZ)"
  if [ -d "$OUT/$R" ]; then bash "$OPS/copy_run_evidence.sh" "$R" || echo "evidence copy for $R reported a problem"; fi
  if [ "$rc" != 0 ]; then echo "SERIES STOPPED at $R (exit $rc); later runs not launched"; exit "$rc"; fi
done
echo "SERIES COMPLETE UTC $(date -u +%FT%T.%3NZ)"
