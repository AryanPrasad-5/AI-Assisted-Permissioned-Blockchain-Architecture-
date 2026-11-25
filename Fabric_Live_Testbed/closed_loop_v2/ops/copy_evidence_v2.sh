#!/usr/bin/env bash
# Verified second copy of one v2 run's evidence on D: (new directory; refuses to overwrite). Usage: copy_evidence_v2.sh cxNN|cmNN
R=$1
OUT=${LPC_OUT:-/home/mailf18/e1/lpc-20261126}
DST=/mnt/d/PLAN_BLOCK18/live_predictive_control_2026-11-25/evidence/$R
[ -e "$DST" ] && { echo "exists: $DST"; exit 2; }
mkdir -p "$DST/caliper_run_workspace"
cp -r "$OUT/$R" "$DST/"
for d in "${R}_fabric_logs" "${R}_verification"; do [ -d "$OUT/$d" ] && cp -r "$OUT/$d" "$DST/"; done
[ -d "/home/mailf18/e1/caliper-ws-benchmark/runs/$R" ] && cp -r "/home/mailf18/e1/caliper-ws-benchmark/runs/$R" "$DST/caliper_run_workspace/"
cp "$OUT"/${R}_*.log "$OUT"/${R}_*.txt "$DST/" 2>/dev/null
[ -f "$OUT/decisions_v2/$R.json" ] && { mkdir -p "$DST/decisions_v2"; cp "$OUT/decisions_v2/$R.json" "$DST/decisions_v2/"; }
python3 - "$OUT" "$DST" "$R" <<'PYEOF'
import hashlib, sys
from pathlib import Path
out, dst, R = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
ws = f"/home/mailf18/e1/caliper-ws-benchmark/runs/{R}"
ok = bad = 0
lst = out / f"{R}_EVIDENCE_SHA256SUMS.txt"
if not lst.exists():
    print(R, "no evidence list (the run stopped before verification)"); sys.exit(0)
for line in lst.read_text().splitlines():
    digest, path = line.split("  ", 1)
    t = dst / "caliper_run_workspace" / R / Path(path).relative_to(ws) if path.startswith(ws) else dst / Path(path).relative_to(out)
    if t.exists() and hashlib.sha256(t.read_bytes()).hexdigest() == digest:
        ok += 1
    else:
        bad += 1; print("MISMATCH", t)
print(R, "copy verified:", ok, "match,", bad, "mismatch; list sha256", hashlib.sha256(lst.read_bytes()).hexdigest())
PYEOF
du -sh "$DST"
