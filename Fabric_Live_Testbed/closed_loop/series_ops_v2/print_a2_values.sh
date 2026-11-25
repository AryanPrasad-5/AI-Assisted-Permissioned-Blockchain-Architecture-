#!/usr/bin/env bash
# Read-only helper for the owner: prints the six hash lines for AUTH_S5_A2.txt from the CURRENT files, plus the AUTH_SERIES window.
# It writes nothing. Run after AUTH_SERIES.txt has been renewed. Usage: bash print_a2_values.sh
S=/mnt/d/PLAN_BLOCK18/closed_loop_experiment_2026-11-22
OPS=$(cd "$(dirname "$0")" && pwd)
echo "# AUTH_SERIES window:"; grep -E '^(approved_utc|expires_utc)=' "$S/AUTH_SERIES.txt"; echo "# now UTC: $(date -u +%FT%TZ)"
echo "amendment_record_sha256=$(sha256sum "$S/S5_AMENDMENT_A2_2026-11-25.md" | cut -d' ' -f1)"
echo "a1_approval_sha256=$(sha256sum "$S/AUTH_S5_AMENDMENT.txt" | cut -d' ' -f1)"
echo "auth_series_sha256=$(sha256sum "$S/AUTH_SERIES.txt" | cut -d' ' -f1)"
echo "stage_sha256=$(sha256sum "$S/stage_series/SHA256SUMS.txt" | cut -d' ' -f1)"
echo "launcher_sha256=$(sha256sum "$OPS/launch_run_v3.sh" | cut -d' ' -f1)"
echo "continuation_check_sha256=$(sha256sum "$OPS/s5a1_continuation_check.py" | cut -d' ' -f1)"
