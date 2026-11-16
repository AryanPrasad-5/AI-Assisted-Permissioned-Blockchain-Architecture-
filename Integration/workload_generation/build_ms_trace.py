"""P1: build millisecond Flashbots arrival traces and reconcile them with the stored audit.

Command (read-only on the archives; writes data/processed/flashbots_ms/ only if every check passes):
    .venv/Scripts/python.exe -I scripts/replay/build_ms_trace.py

Exit codes: 0 all checks passed and derived data written; 2 integrity failure (source hash, size, rows, order, day);
3 reconciliation failure (nothing written).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.replay import trace  # noqa: E402
from fabric_perf.replay.common import IntegrityError  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--manifest", type=Path, default=REPO / "data/manifests/flashbots_mempool_manifest.yaml")
    parser.add_argument("--audit", type=Path, default=REPO / "experiments/results/flashbots_archive_audit.json")
    parser.add_argument("--quality", type=Path, default=REPO / "data/processed/flashbots/data_quality_audit.json")
    parser.add_argument("--series-dir", type=Path, default=REPO / "data/processed/flashbots")
    parser.add_argument("--out-dir", type=Path, default=REPO / "data/processed/flashbots_ms")
    parser.add_argument("--dates", nargs="+", default=["2026-09-21", "2026-10-12"])
    args = parser.parse_args()

    manifest = trace.load_manifest(args.manifest)
    audit = json.loads(args.audit.read_text(encoding="utf-8"))
    quality = json.loads(args.quality.read_text(encoding="utf-8"))
    traces, reports = [], []
    try:
        for date in args.dates:
            entry = manifest[date]
            print(f"[{date}] verifying SHA-256 and reading {entry['source_path']}", flush=True)
            built = trace.build_day_trace(entry["source_path"], entry)
            report = trace.reconcile_day(built, audit, quality, args.series_dir)
            print(report.render(), flush=True)
            traces.append(built)
            reports.append(report)
    except IntegrityError as exc:
        print(f"INTEGRITY FAILURE: {exc}\nSTOP: nothing written.")
        return 2
    if not all(r.ok for r in reports):
        print("RECONCILIATION FAILED: nothing written.")
        return 3
    summary = trace.write_traces(traces, reports, args.out_dir, [trace.__file__, __file__])
    print(f"ALL CHECKS PASSED. Wrote {args.out_dir} (manifest sha256 {summary['manifest_sha256']}).")
    for date, info in summary["days"].items():
        print(f"  {date}: {info['array']} sha256={info['array_sha256']} rows={info['rows']} exact_tie_gaps={info['exact_tie_gaps']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
