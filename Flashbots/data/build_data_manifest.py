"""Compile Flashbots/data/DATA_MANIFEST.json: where every dataset lives, its checksum, schema and provenance.

The raw archives are NOT copied into this folder (they stay at their registered path; copying 180 MB of ZIPs
would only duplicate them). This manifest records what exists and how each derived file was produced.

    .venv/Scripts/python.exe -B Flashbots/data/build_data_manifest.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fbcommon as fc  # noqa: E402


def rel(p) -> str:
    return str(Path(p).resolve().relative_to(fc.REPO)).replace("\\", "/")


def main() -> None:
    audit = json.loads((fc.FB / "data_audit" / "audit_report.json").read_text(encoding="utf-8"))
    extract = json.loads((fc.DERIVED_DIR / "extract_record.json").read_text(encoding="utf-8"))
    verification = json.loads((fc.TS_DIR / "series_verification.json").read_text(encoding="utf-8"))
    manifest = fc.load_manifest()
    raw = []
    for day in audit["days"]:
        entry = manifest[day["date_utc"]]
        raw.append({"date_utc": day["date_utc"], "path": entry["source_path"], "sha256": day["archive_sha256_verified"],
                    "sha256_matches_registered_manifest": day["archive_sha256_verified"] == entry["sha256"].upper(),
                    "compressed_bytes": day["compressed_bytes"], "csv_entry": day["csv_entry"], "rows": day["rows"],
                    "utc_first": day["timestamps"]["first_utc"], "utc_last": day["timestamps"]["last_utc"]})
    payload = {
        "generated_utc": fc.utc_now(),
        "dataset": "Flashbots Mempool Dumpster - daily public-mempool transaction archives (Ethereum mainnet)",
        "source": "https://github.com/flashbots/mempool-dumpster (archives: https://mempool-dumpster.flashbots.net/)",
        "license": "CC-0 public domain (stated in the mempool-dumpster README; data licence, not the code licence)",
        "acquisition": "Downloaded manually by the project owner; registered in data/manifests/flashbots_mempool_manifest.yaml with SHA-256.",
        "what_one_row_is": "One unique transaction hash first received by the Flashbots collectors on that UTC day and not already on-chain at reception.",
        "registered_manifest": rel(fc.MANIFEST),
        "raw_archives_not_copied": raw,
        "combined_rows": audit["combined"]["rows"],
        "schema": {"columns": audit["days"][0]["columns"], "identical_across_dates": audit["combined"]["schemas_identical"]},
        "derived": {
            "transaction_extract": [{**f, "note": "row-complete numeric extract, identity fields dropped"} for f in extract["files"]],
            "target_series_1s": verification["target_series_files"],
            "stored_series_verified": [{k: c[k] for k in ("stored_file", "stored_sha256", "identical")} for c in verification["stored_series_checks"]],
        },
        "other_existing_flashbots_artifacts_outside_this_folder": {
            "data/processed/flashbots/": "six UTC arrival-count series (1/5/10 s) + data_quality_audit.json (verified identical here)",
            "data/processed/flashbots_ms/": "millisecond arrival traces (.npy) and Caliper replay-rate schedules for windows D_33000, V_76200, F_82800 (Integration/workload_generation/)",
            "experiments/results/flashbots_archive_audit.json": "first archive audit (reconciled field by field in data_audit/audit_report.json)",
            "experiments/results/flashbots_baseline_metrics.csv": "legacy first baseline benchmark (HISTORICAL)",
            "experiments/results/flashbots_horizon_benchmark_v2/": "frozen P2 benchmark + law-implied VOI (reproduced exactly in results/p2_reproduction/)",
        },
    }
    fc.write_json(fc.FB / "data" / "DATA_MANIFEST.json", payload)
    print("wrote", rel(fc.FB / "data" / "DATA_MANIFEST.json"))


if __name__ == "__main__":
    main()
