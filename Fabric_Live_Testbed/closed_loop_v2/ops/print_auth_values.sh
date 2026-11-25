#!/usr/bin/env bash
# Read-only helper for the OWNER: prints the key=value lines for AUTH_<PHASE>.txt from the staged phase. Writes nothing.
#   Usage: bash print_auth_values.sh <x0|m>
# You still write approved_by, approved_utc and expires_utc yourself (expires: at least 4 h after approved for x0, 6 h for m).
PH=${1:?phase x0|m}
S=/mnt/d/PLAN_BLOCK18/live_predictive_control_2026-11-25
STAGE=$S/stage_$PH
python3 -B - "$PH" "$STAGE" <<'PYEOF'
import hashlib, json, sys
from pathlib import Path
ph, stage = sys.argv[1], Path(sys.argv[2])
man = json.loads((stage / "STAGE_MANIFEST.json").read_text())
runs = [r["run_id"] for r in man["runs"]]
print("stage_sha256=" + hashlib.sha256((stage / "SHA256SUMS.txt").read_bytes()).hexdigest())
print("runs=" + ",".join(runs))
print("broadcast_channels=" + ",".join(r["channel"] for r in man["runs"]))
print("broadcast_targets=10,40")
print("max_broadcasts_per_run=" + ("0" if ph == "x0" else "10"))
print("native_verification=required")
print("# stage manifest pins: protocol", str(man.get("protocol_sha256"))[:16], "| source freeze", str(man.get("source_freeze_sha256"))[:16], "| kind", man.get("kind"))
PYEOF
