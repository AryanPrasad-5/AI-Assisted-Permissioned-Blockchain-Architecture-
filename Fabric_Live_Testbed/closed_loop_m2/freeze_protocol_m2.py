"""Write the machine-readable twin of protocol M2-v1 (generated from the code that runs) and print its SHA-256. Offline; refuses to overwrite.

  python -m Fabric_Live_Testbed.closed_loop_m2.freeze_protocol_m2 --out <PROTOCOL_M2_v1.json> --x1-decision <x1_decision.json>

The file holds: the protocol constants, the actuator range policy (with the pinned SHA-256 of the frozen actuator), and the design derived from
X1 (surface points, reactive thresholds, window, k) with the SHA-256 of the X1 decision file and of the arrivals file it used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from . import ensure_paths
from . import protocol_m2 as P

ensure_paths()


def build_doc(design: dict | None, provenance: dict | None) -> dict:
    import e1_actuator_range_m2 as policy
    doc = {"protocol": P.protocol_doc(), "actuator_range_policy": policy.policy_doc()}
    if design is not None:
        doc["derived_design"] = {"r_max_tps": design["r_max_tps"], "x1_status": design["x1_status"], "k": design["k"],
                                 "window_start_s": design["window"]["start_s"],
                                 "window_segment_rates_tps": design["window"]["segment_rates_tps"],
                                 "scaled_segment_rates_tps": design["window_selection"]["detail"]["scaled_segment_rates_tps"],
                                 "window_candidates_tried": design["window_selection"]["tried"], "reactive": design["reactive"],
                                 "surface_points": design["surface_points"], "provenance": provenance}
    return doc


def write_doc(out: Path, doc: dict) -> str:
    if out.exists():
        raise FileExistsError(f"refusing to overwrite {out}")
    out.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with out.open("xb") as fh:
        fh.write(raw)
    return hashlib.sha256(raw).hexdigest()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--x1-decision", type=Path)
    a = ap.parse_args(argv)
    design = prov = None
    if a.x1_decision:
        from .stage_m2 import load_design
        design, prov = load_design(a.x1_decision)
    print(write_doc(a.out, build_doc(design, prov)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
