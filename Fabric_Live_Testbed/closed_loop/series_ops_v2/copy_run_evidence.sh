#!/usr/bin/env bash
# Verified second copy of one run's evidence on D: (new directory; refuses to overwrite). Usage: copy_run_evidence.sh clNN
R=$1
OUT=/home/mailf18/e1/closed-loop-20261123
DST=/mnt/d/PLAN_BLOCK18/closed_loop_experiment_2026-11-22/series_evidence_2026-11-22/$R
[ -e "$DST" ] && { echo "exists: $DST"; exit 2; }
mkdir -p "$DST/caliper_run_workspace"
cp -r "$OUT/$R" "$OUT/${R}_fabric_logs" "$OUT/${R}_verification" "$DST/"
cp -r "/home/mailf18/e1/caliper-ws-benchmark/runs/$R" "$DST/caliper_run_workspace/"
cp "$OUT"/${R}_*.log "$OUT"/${R}_*.txt "$DST/"
python3 - "$OUT" "$DST" "$R" <<'EOF'
import hashlib, sys
from pathlib import Path
out, dst, R = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
ws = f"/home/mailf18/e1/caliper-ws-benchmark/runs/{R}"
ok = bad = 0
lst = out / f"{R}_EVIDENCE_SHA256SUMS.txt"
for line in lst.read_text().splitlines():
    digest, path = line.split("  ", 1)
    t = dst / "caliper_run_workspace" / R / Path(path).relative_to(ws) if path.startswith(ws) else dst / Path(path).relative_to(out)
    if t.exists() and hashlib.sha256(t.read_bytes()).hexdigest() == digest:
        ok += 1
    else:
        bad += 1; print("MISMATCH", t)
print(R, "copy verified:", ok, "match,", bad, "mismatch; list sha256", hashlib.sha256(lst.read_bytes()).hexdigest())
EOF
du -sh "$DST"
