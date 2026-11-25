"""Write the frozen protocol file PROTOCOL_X1_v1.json (stdlib). Machine-readable: every constant, rule text, the run order, the numeric
predictions written BEFORE any X1 measurement, and the SHA-256 of the source-freeze list. Generated from the SAME code the experiment runs.
The X1 stage pins this file's SHA-256, and the owner's authorization pins the stage.

  python -m Fabric_Live_Testbed.closed_loop_x1.freeze_protocol_x1 --source-freeze-file <source_hashes_frozen_x1.txt> --out <new dir>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from . import PACKAGE_VERSION
from . import protocol_x1 as X
from . import stage_x1 as ST

# Predictions are arithmetic on STORED X0 / P1a numbers (B=10 and B=40 measured at 10-160 tx/s). They are HYPOTHESES for B=2 and B=5.
X0_MEASURED_P95_MS = {   # stored X0 values (D:\PLAN_BLOCK18\insight_x0_crossover_negative_result_2026-11-25.md and x0_decision rung_table), mean of 2 replicates where reported
    10: {10: 924.0, 20: 474.0, 40: 248.0, 60: 175.0, 80: 137.0, 100: 117.0, 130: 96.0, 160: 82.0},
    40: {10: 1949.0, 20: 1898.0, 40: 982.0, 60: 666.0, 80: 508.0, 100: 414.0, 130: 335.0, 160: 280.0}}   # single-run values from cx01 / cx03


def _model(b, r):
    return 900.0 * b / r + 24.0


def latency_model() -> dict:
    dev = {str(b): {str(r): round(v / _model(b, r) - 1.0, 3) for r, v in d.items()} for b, d in X0_MEASURED_P95_MS.items()}
    return {"formula": "p95_ms = 900 * B / rate_tps + 24",
            "mechanism": "p95 wait ~ 0.9 x block fill time B/rate plus a ~24 ms ordering/commit floor; the 2 s BatchTimeout caps the wait when B/rate > 2 s",
            "x0_measured_p95_ms": {str(b): {str(r): v for r, v in d.items()} for b, d in X0_MEASURED_P95_MS.items()},
            "relative_deviation_measured_over_model": dev,
            "note": "B=10 fits within 3% at all 8 rates; B=40 sits 4-12% above the model wherever the timer does not bind (at 10 tx/s B/rate = 4 s > 2 s, model not applicable)"}


def predictions() -> dict:
    f = lambda b, r: round(900.0 * b / r + 24.0, 1)
    return {
        "label": "HYPOTHESES written before any X1 measurement; derived from stored X0/P1a values only; no HFBTP transfer is used",
        "H1_latency_model": {**latency_model(), "predicted_p95_ms": {str(b): {str(r): f(b, r) for r in X.RUNG_RATES_TPS} for b in X.CONFIGS},
                             "falsified_if": "measured p95 of an UNSATURATED B=2 or B=5 cell deviates from the formula by more than 30% at three or more rungs"},
        "H2_saturation": {"statement": "B=2 saturates (delivered ratio < 0.95 or failures > 0.5% with the driver on time) once rate/B exceeds the "
                                       "~16-17 blocks/s that B=10 sustained cleanly in X0 (160 tx/s = 16 blocks/s), i.e. above ~32-34 tx/s: "
                                       "first saturated rung expected at 35-40 tx/s; B=5 (12 blocks/s at 60 tx/s) and B=10 stay unsaturated to 60 tx/s",
                          "falsified_if": "B=2 is unsaturated at every client-valid rung (then B=2 dominates the whole range) or B=5 saturates at <= 60 tx/s"},
        "H3_decision": {"statement": "if H1 and H2 hold: B=2 is decisive at 5-30 tx/s, B=5 at 35-60 tx/s, B=10 is never decisively best; ordering reversal "
                                     "B2->B5 at ~32 tx/s; menu {2,5}. Natural Flashbots minutes are 5-33 tx/s (median 11), i.e. almost entirely in the "
                                     "B=2 regime, so at natural scale one static B=2 is expected to be near-optimal and a headroom claim needs a window "
                                     "scaled by k >= ~2 to straddle ~32 tx/s",
                         "falsified_if": "a decisive B=10 regime appears, or the transition lies outside 20-50 tx/s"},
    }


RULES = {
    "question": "Does a smaller block size (B=2, B=5) give a rate-dependent ordering against B=10 AND enough decision headroom, under achievable "
                "(client-valid) load, for an adaptive controller to be able to matter? One estimand: oracle p95 headroom over the tuned static configuration.",
    "status": "EXPLORATORY follow-up to X0 (LPC-v1). X0, cl01-cl12 and their authorizations, results and analysis are not modified or re-analysed.",
    "primary_metrics": "latency: p95 per (B, rate) cell, mean over non-client-limited replicates (mean latency secondary); throughput: delivered ratio "
                       "(success / round wall) / (scheduled / 60 s), committed tx/s reported; saturation = delivered ratio < 0.95 or failed > 0.5% with the driver on time",
    "client_vs_fabric": "CLIENT_LIMITED (dispatch-lag p99 > 100 ms or any overflow callback) and INCOMPLETE rounds are excluded; FABRIC_SATURATED is a result. "
                        "A cell needs >= 2 valid replicates of 3. A rung is VALID only if all three configurations have valid cells; when only some "
                        "configurations were client-limited the rung is CO_LOCATION_AMBIGUOUS (one shared machine cannot separate client from Fabric load) "
                        "and is excluded. r_max = highest rung such that it and every lower rung are VALID (contiguous).",
    "sesoi": "max(10% of the better p95, 25 ms) per rung (as X0). Pairwise winner needs gap > SESOI AND every valid replicate of the worse cell above every valid "
             "replicate of the better one. A saturated cell loses to an unsaturated one.",
    "regimes_and_reversal": "a configuration is decisive at a rung when it beats both others; it forms a regime when decisive at 2 consecutive VALID rungs; "
                            "ordering reversal = at least two configurations with regimes. Transition rate r_x = geometric mean of the last rung of the "
                            "first regime and the first rung of the second.",
    "menu": "menu = tuned static configuration of the selected window UNION configurations oracle-best on >= 20% of its segments; must have >= 2 members. "
            "Every menu member is also a Static arm in any later live comparison (no strawman baseline).",
    "workload_for_gate": "the window is chosen by a structural rule that never sees a headroom value: first of the top-5 LPC-v1 ranked windows (hash-pinned) "
                         "with k = min(r_x / sqrt(min m * max m), 0.9 * r_max / max m, 25) >= 1 and >= 2 scaled segments below r_x(0.9) and >= 2 above r_x(1.1). "
                         "One shot: if the gate fails on that window the experiment stops; no other window is tried. Whole-day natural minutes and all 14 "
                         "eligible windows at k=1 are reported descriptively only.",
    "headroom": "evaluated on the X1 surface (log-rate interpolation of measured cells, no extrapolation): static = best fixed configuration (fewest saturated "
                "segments, then lowest mean p95); oracle = perfect-foresight best unsaturated configuration per segment, no switching cost; persistence = "
                "choose by the previous minute's rate (DIAGNOSTIC ONLY, reported, never a gate: near a saturation cliff a no-skill rule mis-steps on a rising "
                "segment, and stopping on that would end the experiment exactly where forecasting could matter). GO needs ALL of: full coverage; oracle gain "
                ">= 10% and >= 25 ms; the gain comes from >= 2 segments each better than the SESOI; oracle never on a saturated cell; menu >= 2; and the same "
                "conditions in each leave-one-replicate-out re-evaluation.",
    "revision_r1": "2026-11-25, BEFORE any X1 measurement: a persistence-headroom gate (>= 5%) was removed after a model-based dry run on the pre-registered "
                   "predictions (not on data) showed it fails on the predicted world because the first segment jumps past B=2's saturation cliff; it was "
                   "replaced by the >= 2 segments with a SESOI-sized gain condition. The earlier stage/freeze/protocol files were superseded, never used.",
    "outcomes": {"GO_LIVE_DESIGN": "design (not run) a separately authorized live Static vs Reactive vs Predictive comparison",
                 "STOP_NO_HEADROOM": "stop experimenting; finalize the paper with the completed evidence",
                 "STOP_NOT_ESTABLISHED": "no client-valid lowest rung: no conclusion; stop"},
    "no_tuning": "no controller threshold, forecast parameter, k or window is tuned on any run of a later live comparison; thresholds/surfaces for it come from X1 only",
    "stopping": "no launch if any gate fails; a run stops on runner failure, container problem, memory below the floor or the 1,500-s hard timeout; nothing is "
                "retried; one replacement run (same B and rung order, new run id, new owner authorization) per aborted run; the continuation rule is integrity-only "
                "(closed_loop_v2/ops/continue_check_v2.py); performance outcomes never stop the phase. The first 6 staged runs hold 2 replicates of every "
                "configuration (minimum acceptable analysis set).",
    "power_note": "n=3 per configuration: an exact two-sided rank test cannot go below p=0.10, so no significance claim is made; decisions use the SESOI and "
                  "the non-overlap rule, with effect sizes reported",
    "explanations": {"co_location": "smaller B means more blocks per second and more peer/orderer CPU on the machine that also runs Caliper; separated by the "
                                    "dispatch-lag record and the CO_LOCATION_AMBIGUOUS rule, not by assumption",
                     "batch_timeout": "at 5 tx/s B=10 fills in 2 s = BatchTimeout, so the timer, not B, bounds its wait; the rung is reported, not corrected",
                     "drift": "run order is block-randomized by replicate with a seeded rung order; time-of-day drift is reported via run order",
                     "client_capacity": "X0 showed one stalled round at 80 tx/s (cx02); X1's top rung is 60 tx/s"},
    "limits": "one laptop, 3 Raft orderers, 2 peers, Caliper co-located, BatchTimeout 2 s fixed, 60-s rungs, synthetic 128-byte CreateAsset, 1 stored Flashbots day "
              "for the workload shape (rate-trace scaling loses all within-minute structure), n=3, no claim about natural Ethereum load, HFBTP or other hardware",
}


def build(source_freeze_text: str) -> dict:
    return {
        "protocol": X.PROTOCOL_ID, "tool": PACKAGE_VERSION,
        "constants": {k: getattr(X, k) for k in (
            "CONFIGS", "BASELINE_B", "BATCH_TIMEOUT", "RUNG_RATES_TPS", "PREROLL_RATE_TPS", "REPLICATES_PER_B", "MIN_VALID_REPS", "ORDER_SEED",
            "HARD_TIMEOUT_S", "RUN_PREFIX", "CLIENT_LAG_P99_MS", "FAILED_FRACTION_LIMIT", "DELIVERED_RATIO_MIN", "SESOI_REL", "SESOI_FLOOR_MS",
            "CONFIRM_RUNGS", "HEADROOM_ORACLE_MIN", "MIN_ABS_GAIN_MS", "MIN_GAIN_SEGMENTS", "MIN_REGIME_SHARE", "MIN_MENU_SIZE", "K_MIN", "K_MAX",
            "CAP_FRAC", "STRADDLE_DELTA", "MIN_SEGMENTS_EACH_SIDE", "FALLBACK_TOP_N", "LPC_PROTOCOL_SHA256")},
        "rules": RULES, "predictions": predictions(), "run_specs": ST.run_specs(),
        "source_freeze_sha256": hashlib.sha256(source_freeze_text.encode("utf-8")).hexdigest(),
        "source_freeze_files": len([ln for ln in source_freeze_text.splitlines() if ln.strip()]),
        "frozen_before": "any X1 measurement; no Fabric measurement of this protocol existed when this file was written",
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-freeze-file", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    doc = build(a.source_freeze_file.read_bytes().decode("utf-8"))
    a.out.mkdir(parents=True, exist_ok=False)
    body = (json.dumps(doc, indent=1, sort_keys=True) + "\n").encode("utf-8")
    (a.out / "PROTOCOL_X1_v1.json").write_bytes(body)
    print("PROTOCOL_X1_v1.json sha256", hashlib.sha256(body).hexdigest(), "| source freeze sha256", doc["source_freeze_sha256"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
