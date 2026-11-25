#!/usr/bin/env bash
# Read-only helper for the OWNER: prints the key=value lines for AUTH_X1.txt from the staged X1 phase. Writes nothing.
#   Usage: bash print_auth_values_x1.sh
# You still write approved_by, approved_utc and expires_utc yourself (expires: at least 5 h after approved; the phase needs about 3 h).
S=${X1_RECORDS:-/mnt/d/PLAN_BLOCK18/x1_block_size_headroom_2026-11-25}
STAGE=$S/stage_x1
python3 -B - "$STAGE" <<'PYEOF'
import hashlib, json, sys
from pathlib import Path
stage = Path(sys.argv[1])
man = json.loads((stage / "STAGE_MANIFEST.json").read_text())
runs = [r["run_id"] for r in man["runs"]]
print("stage_sha256=" + hashlib.sha256((stage / "SHA256SUMS.txt").read_bytes()).hexdigest())
print("runs=" + ",".join(runs))
print("broadcast_channels=" + ",".join(r["channel"] for r in man["runs"]))
print("broadcast_targets=10")
print("max_broadcasts_per_run=0")
print("native_verification=required")
print("# stage manifest pins: protocol", str(man.get("protocol_sha256"))[:16], "| source freeze", str(man.get("source_freeze_sha256"))[:16], "| kind", man.get("kind"))
PYEOF
