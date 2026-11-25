#!/usr/bin/env bash
# Prints the SHA-256 list of every source file an X1 run depends on (stdout only; writes nothing). Run from anywhere.
# The stage manifest pins the sha256 of this list; launch_x1.sh refuses if the list differs at launch time.
# Tests are not listed. Every runtime module of the unchanged v1 package, the unchanged v2 package (runner, stage helpers, continuation
# check) and the X1 package is listed, plus the actuator tools and the repo controllers the runner imports.
R=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-
cd "$R" || exit 1
{
  ls scripts/e1/e1_config_actuator.py scripts/e1/e1_independent_config_verify.py scripts/e1/e1_channel_run.py scripts/e1/e1_block_fetch.py \
     scripts/e1/run_fabric_config_verifier.sh tools/fabric_config_verifier/main.go
  ls Fabric_Live_Testbed/closed_loop/*.py Fabric_Live_Testbed/closed_loop_v2/*.py Fabric_Live_Testbed/closed_loop_v2/ops/*.py \
     Fabric_Live_Testbed/closed_loop_v2/ops/*.sh Fabric_Live_Testbed/closed_loop/series_ops_v2/caliper_idle_check.sh
  ls Fabric_Live_Testbed/closed_loop_x1/*.py Fabric_Live_Testbed/closed_loop_x1/ops/*.py Fabric_Live_Testbed/closed_loop_x1/ops/*.sh
  ls src/fabric_perf/controllers/channel_selection.py src/fabric_perf/controllers/strategies.py src/fabric_perf/controllers/base.py
} | LC_ALL=C sort | xargs sha256sum
