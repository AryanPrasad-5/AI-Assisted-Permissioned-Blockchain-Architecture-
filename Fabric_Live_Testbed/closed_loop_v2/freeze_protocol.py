"""Write the frozen protocol file PROTOCOL_LPC_v1.json (Windows; numpy via window_selection). Machine-readable: every constant, rule text and
the full deterministic window ranking, generated from the SAME code the experiment runs, plus the SHA-256 of the source freeze list.
The file is written once into a NEW directory; the X0 stage pins its SHA-256, and the owner's authorization pins the stage.

  python -m Fabric_Live_Testbed.closed_loop_v2.freeze_protocol --source-freeze-file <source_hashes_frozen_v2.txt> --out <new dir>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from . import PACKAGE_VERSION
from . import protocol as P
from . import stage_v2 as ST
from .predictor import DELIVERED_DEADBAND
from .window_selection import load_ranking

RULES = {
    "primary_metric": "p95 latency (ms) per (B, rate) rung, mean over replicates; mean latency and delivered fraction are secondary",
    "crossover_rule": ("r_c = the lowest client-valid rung at which B=40 beats B=10 by more than sesoi(r) in the replicate mean AND in "
                       "every replicate, and the next client-valid rung also does (CONFIRM_RUNGS=2); B=10 must win by more than the "
                       "SESOI at some lower rung; a configuration that exceeds the failed-transaction limit loses that rung"),
    "sesoi": "sesoi(r) = max(0.10 * min(p95_B10, p95_B40), 25 ms); 10% of the better configuration's p95 is the smallest difference a "
             "latency-sensitive operator would act on, and the floor is >= 3.5 x the measured p95 reproducibility coefficient "
             "(RC 6.97 ms, P1a, 10 df); the earlier 92.6 ms was the same 10% rule at 10 tx/s",
    "client_vs_fabric_saturation": ("a round is CLIENT_LIMITED when the load driver itself was late (dispatch lag p99 > 100 ms, or any "
                                    "overflow callback) or INCOMPLETE; such rungs are excluded and cap r_max (contiguous from below). "
                                    "Fabric saturation shows with the driver ON TIME as latency growth, pending transactions and "
                                    "failures, and counts as a result"),
    "x0_abort_and_failure": ("no launch if any gate fails; a run stops on a runner step failure, a container problem, memory below the "
                             "floor or the Caliper hard timeout; nothing is retried; one replacement run (same B and rung order, new "
                             "run id, new owner authorization) is allowed per aborted run; failed transactions and driver lag are data"),
    "no_crossover": "if the outcome is not CROSSOVER (none within the client-valid range, not established, or no feasible window) the "
                    "adaptive comparison is NOT run and the negative result is reported",
    "k_formula": "k = min(r_c / sqrt(min m * max m), 0.9 * r_max / max m, 25); feasible iff k >= 2 and >= 2 segments lie below r_c(1-0.10) and >= 2 above r_c(1+0.10)",
    "window_rule": "deterministic rank by R_robust over eligible 600-s windows of 2026-10-12 (excluding the F_82800 window); first feasible of the top 5",
    "reactive_rule": "high = mean latency of B=10 at r_c (from X0); low = 0.8 * high; 2 consecutive samples; no cooldown; decided from X0 only",
    "predictor_rule": "local surface from client-valid X0 rungs; log-rate linear interpolation; delivered fraction normalized per B with a 0.95 deadband; refuses outside the measured range",
    "m_inclusion": "a run is comparable iff no measured segment is CLIENT_LIMITED or INCOMPLETE; all runs are reported; primary statistics use comparable runs",
    "m_primary_comparison": "predictive_live vs the better static arm and vs reactive on run-level p95 and mean latency; descriptive effect sizes with intervals; no significance claim with n <= 3",
    "continuation": "integrity-only STOP rule (ops/continue_check_v2.py); performance outcomes never stop a phase",
    "scaling_loss": "piecewise-constant per-minute rates with evenly spaced arrivals: minute-to-minute shape preserved; all within-minute structure (12-s cycle, 1-s bursts, over-dispersion) NOT preserved",
}


def build(source_freeze_text: str) -> dict:
    ranked, src = load_ranking()
    return {
        "protocol": P.PROTOCOL_ID, "tool": PACKAGE_VERSION,
        "constants": {k: getattr(P, k) for k in ("MENU_B", "INITIAL_B", "BATCH_TIMEOUT", "FORECAST", "PREDICTIVE", "REACTIVE_RULE",
                                                 "X0_RUNG_RATES_TPS", "X0_PREROLL_RATE_TPS", "X0_REPLICATES_PER_B", "X0_ORDER_SEED",
                                                 "X0_HARD_TIMEOUT_S", "CLIENT_LAG_P99_MS", "FAILED_FRACTION_LIMIT", "SESOI_REL",
                                                 "SESOI_FLOOR_MS", "CONFIRM_RUNGS", "WINDOW", "K_MIN", "K_MAX", "CAP_FRAC",
                                                 "STRADDLE_DELTA", "MIN_SEGMENTS_EACH_SIDE", "FALLBACK_TOP_N", "M_ARMS",
                                                 "M_REPLICATES_TARGET", "M_REPLICATES_MIN", "M_ORDER_SEED")} | {"DELIVERED_DEADBAND": DELIVERED_DEADBAND},
        "rules": RULES,
        "x0_run_specs": ST.x0_run_specs(),
        "m_run_order_3_replicates": ST.m_run_specs(),
        "window_ranking_source": src,
        "window_ranking": ranked,
        "source_freeze_sha256": hashlib.sha256(source_freeze_text.encode("utf-8")).hexdigest(),
        "source_freeze_files": len([ln for ln in source_freeze_text.splitlines() if ln.strip()]),
        "frozen_before": "any X0 measurement; no controller output or Fabric measurement of this protocol existed when this file was written",
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-freeze-file", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    doc = build(a.source_freeze_file.read_bytes().decode("utf-8"))
    a.out.mkdir(parents=True, exist_ok=False)
    body = (json.dumps(doc, indent=1, sort_keys=True) + "\n").encode("utf-8")
    (a.out / "PROTOCOL_LPC_v1.json").write_bytes(body)
    print("PROTOCOL_LPC_v1.json sha256", hashlib.sha256(body).hexdigest(), "| source freeze sha256", doc["source_freeze_sha256"],
          "| eligible windows", doc["window_ranking_source"]["n_eligible_windows"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
