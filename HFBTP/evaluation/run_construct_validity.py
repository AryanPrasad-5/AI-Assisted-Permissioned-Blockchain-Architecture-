"""Regenerate every construct-validity number. Read-only for inputs.

Usage: python scripts/construct_validity/run_construct_validity.py [--out PATH] [--skip-frozen]
Writes one JSON file. Default: docs/research/results/construct_validity_results.json
"""
from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.dont_write_bytecode = True

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import sklearn  # noqa: E402

import cv_common as cc  # noqa: E402
import cv_e1_predictions as e1  # noqa: E402
import cv_identities as ident  # noqa: E402
import cv_mechanism as mech  # noqa: E402
import cv_mmbpd_observed as mm  # noqa: E402
import cv_protocol_stats as ps  # noqa: E402


def _records(frame: pd.DataFrame, limit: int) -> list:
    return json.loads(frame.head(limit).to_json(orient="records"))


def build(skip_frozen: bool) -> dict:
    h = cc.load_hfbtp()
    offset = mech.commit_offset(h)
    manifests = cc.load_manifests()
    results = cc.load_phase1c_results()
    fill = mech.fill_regime_scan(h, offset)
    blocks = mech.all_block_scan(h, offset)
    timer = mech.timer_cap_scan(h, offset)
    pick = timer.iloc[0]
    cap = int(pick["cap"])
    scan = ident.divisor_scan(h, cc.SET, cc.BLOCK, cc.TPUT)
    out = {
        "meta": {"commit_offset": offset, "python": platform.python_version(), "pandas": pd.__version__,
                 "numpy": np.__version__, "sklearn": sklearn.__version__,
                 "git_head": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True,
                                            cwd=cc.REPO_ROOT).stdout.strip(),
                 "hfbtp_md5": cc.HFBTP_MD5, "mmbpd_md5": cc.MMBPD_MD5},
        "hfbtp_identity": {
            "n_bound_violation": {str(n): ident.n_bound_violation_rate(h[cc.TPUT], h[cc.ACTUAL], h[cc.MAX_LAT], n)
                                  for n in (500, 1000, 1050, 1100, 1200, 2000)},
            "divisor_scan_top3": scan[:3],
            "divisor_rank_of_1000": next(i for i, r in enumerate(scan) if r["n"] == 1000),
            "timer_reading": ident.timer_reading(h, cc.SET),
            "divisor_contrast_by_level": {str(int(k)): float(v) for k, v in ident.divisor_contrast(h, cc.SET).items()},
        },
        "hfbtp_structure": {"level_165": ident.level_165_structure(h), "block_10": ident.block_10_stalls(h),
                            "heavy": ident.heavy_decomposition(h), "large_block_flatness": ident.large_block_flatness(h)},
        "identification": {"fill_regime_top5": _records(fill, 5), "all_block_rule_top3": _records(blocks, 3),
                           "timer_cap_top5": _records(timer, 5),
                           "timer_rule_no_cap": _records(timer[timer["cap"] == 0], 1)},
        "partitions": {
            "phase1c": mech.phase1c_reference(results),
            "interpolation": ident.interpolation_scores(h, manifests, cc.partition_keys),
            "legacy_float_as_reported": mech.partition_scores(mech.legacy_prediction_table(h, offset), manifests),
            "exact_no_cap": mech.partition_scores(mech.prediction_table(h, offset), manifests),
            "exact_cap_timer_survives": mech.partition_scores(
                mech.prediction_table(h, offset, byte_cap=cap, timer_survives_overflow=bool(pick["timer_survives"])),
                manifests),
            "selected_cap": cap,
        },
        "mmbpd_observed": mm.mmbpd_summary(cc.load_mmbpd()),
        "orderer_gap": mm.orderer_gap(h, cc.load_mmbpd()),
        "protocol": ps.summary(),
        "e1_predictions": e1.design_table(h, offset, cap),
    }
    if not skip_frozen:
        import cv_frozen_models as frozen
        out["frozen_models"] = frozen.heavy_flatness(h)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=cc.REPO_ROOT / "docs/research/results/construct_validity_results.json")
    parser.add_argument("--skip-frozen", action="store_true")
    args = parser.parse_args()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(build(args.skip_frozen), indent=1, sort_keys=True, default=float) + "\n", encoding="utf-8")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
