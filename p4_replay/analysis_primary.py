"""Primary confirmatory analysis of the reduced replay (Phase 2C): run-level pooled per-transaction p95 latency, B=10 against B=40, same window, three runs per arm.
OFFLINE. Written and tested BEFORE any measured run, on synthetic data only. It has not been run on any real run.

Estimand. For one run: the 95th percentile of (final_ms - create_ms) over ALL transactions of that run's MEASURED round (one pooled sample per run, cohort by INTENDED submission
time: the drain tail belongs to the run it was scheduled in). The unit of analysis is the RUN (statistical-validation skill: never the transaction). Primary contrast:
B=40 minus B=10 on that quantity.

Conventions, stated and not assumed
  * run p95 = NEAREST-RANK: the ceil(0.95 n)-th smallest latency (no interpolation). Source of the convention: the P1b analysis summary key `latency_ms_p95_nearest_rank`, and the
    Phase 2 `series.run_p95_ms`. Latency of a transaction that did not succeed is not defined and is excluded (a run with any failure is never usable anyway).
  * M3 (the law-side number) is a DIFFERENT quantity: a volume-weighted 95th percentile of one-second law-implied interval p95 values. Its quantile convention was stated in no protocol text until
    preregistration amendment 02b and is set out in amendment 02c, which the owner accepted with limitations (see M3_CONVENTION): weighted lower inverse-CDF, no interpolation. See `weighted_quantile_lower`.
  * dispersion/uncertainty at n = 3 is DESCRIPTIVE ONLY: full ranges, all 3 x 3 cross differences, the Hodges-Lehmann shift, the exact permutation p value and the smallest p the design
    can ever reach, and, for an arm of exactly three runs, the closed-form chi-square interval for its standard deviation (df = 2, normality assumed, labelled as such).
    No alpha is applied and no run of this script ever says "significant", "confirmed" or "established".

Which runs enter the contrast: those classified HEALTHY_REPLAY (approved thresholds; `usable_as_fabric_result`). Others are listed, with the reason, and excluded from the contrast.
If an arm has fewer than three usable runs the contrast is labelled PARTIAL or NOT_COMPUTABLE, never silently reduced.

The law comparison (M3) is returned under its own key and rendered in its own section. It is a hypothesis to be falsified, not a result.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from . import PACKAGE_VERSION
from . import prereg as pr

SCHEMA = "p4_replay.analysis_inputs/1"
HEALTHY = "HEALTHY_REPLAY"
ARMS = (10, 40)
M3_NAME = "M3_p95ofintervals_1s"
_M3_IX = list(pr.MAPPINGS).index(M3_NAME)
M3_STORED_MS = {10: pr.LAW_TABLE_P4_DRAFT[("A", 10)][_M3_IX], 40: pr.LAW_TABLE_P4_DRAFT[("A", 40)][_M3_IX]}      # 1776 and 1940, as stored (not recomputed)
M3_GAP_MS = M3_STORED_MS[40] - M3_STORED_MS[10]                                                                     # 164
RUN_P95_CONVENTION = "nearest-rank: the ceil(0.95 n)-th smallest of final_ms - create_ms over the successful transactions of the measured round"

M3_CONVENTION = {
    "definition_text": "volume-weighted 95th percentile of one-second law-implied interval p95 values (P4 draft section 3: 'interval-quantile proxy'; prereg.MAPPINGS['M3_p95ofintervals_1s'])",
    "status": "OWNER_ACCEPTED_WITH_LIMITATIONS_2026-11-15",
    "owner_decision": "ACCEPTED WITH LIMITATIONS on 2026-11-15 for the preregistered one-second schedule analysis (M3_p95ofintervals_1s) only. Records: "
                      "02c_PREREGISTRATION_AMENDMENT_M3_CONVENTION_REV2_2026-11-15.md and 06b_N2_OWNER_DECISION_RECORD_REV2_2026-11-15.md in D:/PLAN_BLOCK18/flashbots_replay_phase2c_2026-11-14/. "
                      "This is not the series-approval key m3_convention_review, which this code does not set; setting it is a separate owner act.",
    "accepted_limitations": "L1-L10 as worded in section 2 of amendment 02c. They describe the state at the date of that amendment (an offline reproduction only; no live Fabric replay, "
                            "no delivered-throughput measurement under the Flashbots schedules and no predictive-controller measurement) "
                            "and are not restated here, so that this field cannot drift from the amendment.",
    "found_in": "scripts/replay/demo_offline.py lines 31-35, function weighted_quantile (the P4 draft says its table comes from that script)",
    "algorithm": "drop non-finite values and zero weights; sort ascending; cumulative weights c; return the first value with c >= q x sum(weights) (numpy searchsorted, side='left')",
    "interpolation": "NONE: the result is always one of the input values (a lower inverse-CDF step function)",
    "weights": "rate x interval_s, i.e. transactions in the interval",
    "reproduced": True,
    "reproduction_status": "REPRODUCED_2026-11-15_ONE_PRESERVED_RUN_CURRENT_ENVIRONMENT",
    "reproduction_note": "REPRODUCED, 2026-11-15, one preserved run, under the current environment: an offline run of scripts/replay/demo_offline.py on window F_82800, arm A, one-second counts "
                         "(report file demo_offline_report.json, SHA-256 beginning ca6fc5e6; the full value is in the evidence folder). B=10 1776 ms, B=40 1940 ms, difference 164 ms: "
                         "law-implied interval statistics, not measured pooled run p95. This shows reproducibility under the current environment, not historical provenance of the stored values. "
                         "This analysis still uses the stored values as stored and does not recompute them. The command line and interpreter are not recorded in the outputs; "
                         "interpreter versions and the exit code are owner-supplied, not independently verified.",
    "reproduction_evidence": {"report_file": "demo_offline_report.json", "report_sha256_prefix": "ca6fc5e6",
                              "evidence_dir": "D:/PLAN_BLOCK18/flashbots_replay_phase2c_2026-11-14/07_n2_m3_reproduction_evidence_2026-11-15/",
                              "checksums_file": "SHA256SUMS.txt (full SHA-256 of the report, stdout.txt and stderr.txt)"},
    "historical_provenance": "INCOMPLETE while the generating code (scripts/replay/demo_offline.py, src/fabric_perf/forecasting/law_voi.py and the fabric_perf.replay modules it imports) "
                             "is untracked in Git and absent from the pinned source manifest (limitation L5). Change this field only through a new dated record.",
    "validation_against_measured_run_p95": "NOT_VALIDATED: this decision does not validate M3 against a measured pooled Fabric run-level p95; the two are different quantities.",
    "limitations": [
        "The convention was stated in no protocol text until preregistration amendment 02b (2026-11-15), whose current revision is amendment 02c; the owner accepted it with limitations (see accepted_limitations). That acceptance is not a validation of M3 as a predictor of measured run p95.",
        "One-second intervals are shorter than the 2 s batching timeout, so a one-second law p95 is not a latency distribution of anything that was observed within that second.",
        "A quantile of per-interval p95 values is not the p95 of the pooled latency distribution (a different estimand): M3 is a hypothesis, not a validated prediction of the pooled run p95.",
        "With B=40 the law saturates at its 2 s timer ceiling, so many intervals share the value 1940 ms and the step quantile returns exactly that ceiling.",
    ],
}


class AnalysisInputError(ValueError):
    """An input is missing or unreadable. A run whose evidence is missing is listed and excluded from the contrast, with the reason; the analysis never guesses a value."""


class IntegrityError(AnalysisInputError):
    """An input does not match its recorded SHA-256. This is not missing data: the whole analysis refuses (fail closed), because silently excluding a tampered file would hide it."""


# ----------------------------------------------------------------------------------------------- quantiles
def nearest_rank(values, q: float) -> float:
    """The ceil(q n)-th smallest value (1-based), q in (0, 1]. No interpolation."""
    xs = sorted(float(v) for v in values)
    if not xs:
        raise ValueError("no values")
    if not (0 < q <= 1):
        raise ValueError("q must be in (0, 1]")
    return xs[max(0, math.ceil(q * len(xs) - 1e-12) - 1)]


def weighted_quantile_lower(values, weights, q: float) -> float:
    """Transcription of scripts/replay/demo_offline.py weighted_quantile (lines 31-35). Kept to document and test the M3 convention; it is NOT used to produce any number here."""
    v_all, w_all = np.asarray(values, dtype=float), np.asarray(weights, dtype=float)
    ok = np.isfinite(v_all) & (w_all > 0)
    order = np.argsort(v_all[ok])
    v, w = v_all[ok][order], w_all[ok][order]
    return float(v[np.searchsorted(np.cumsum(w), q * w.sum())])


# ----------------------------------------------------------------------------------------------- per-run extraction
@dataclass(frozen=True)
class RunInput:
    run_id: str
    B: int
    replicate: int
    position: int | None
    cls: str
    recorder_csv: Path | None
    recorder_sha256: str | None = None


def read_latencies(path: Path, expected_sha256: str | None = None) -> dict:
    """Latencies (ms) of the successful, verified transactions in a measured-round recorder csv, with the counts. Raises AnalysisInputError on a missing file, a bad hash or a bad row."""
    p = Path(path)
    if not p.is_file():
        raise AnalysisInputError(f"{p}: recorder file not found")
    data = p.read_bytes()
    if expected_sha256 and hashlib.sha256(data).hexdigest() != expected_sha256.lower():
        raise IntegrityError(f"{p}: SHA-256 differs from the recorded {expected_sha256[:16]}...")
    rows = list(csv.DictReader(data.decode("utf-8").splitlines()))
    need = {"tx_id", "create_ms", "final_ms", "status", "verified"}
    if not rows or not need <= set(rows[0]):
        raise AnalysisInputError(f"{p}: header lacks {sorted(need - set(rows[0] if rows else []))}")
    lat, failed = [], 0
    for i, r in enumerate(rows, start=2):
        if r["status"] != "success" or str(r["verified"]).lower() != "true":
            failed += 1
            continue
        try:
            lat.append(float(r["final_ms"]) - float(r["create_ms"]))
        except ValueError as exc:
            raise AnalysisInputError(f"{p}: row {i} has a non-numeric time") from exc
    if any(x < 0 for x in lat):
        raise AnalysisInputError(f"{p}: negative latency present (final before create)")
    return {"n_rows": len(rows), "n_success": len(lat), "n_failed": failed, "latencies_ms": lat}


def per_run_table(runs: list[RunInput]) -> list[dict]:
    out = []
    for r in sorted(runs, key=lambda x: (x.position if x.position is not None else 10**6, x.run_id)):
        row = {"run_id": r.run_id, "B": r.B, "replicate": r.replicate, "order_position": r.position, "class": r.cls, "p95_ms": None, "n_transactions": None,
               "n_failed": None, "p50_ms": None, "max_ms": None, "usable_for_contrast": False, "excluded_because": None}
        try:
            if r.recorder_csv is None:
                raise AnalysisInputError("no recorder file recorded for this run")
            lat = read_latencies(r.recorder_csv, r.recorder_sha256)
        except IntegrityError:
            raise
        except AnalysisInputError as exc:
            row["excluded_because"] = f"EVIDENCE_UNAVAILABLE: {exc}"
            out.append(row)
            continue
        row.update(n_transactions=lat["n_success"], n_failed=lat["n_failed"])
        if lat["n_success"]:
            xs = lat["latencies_ms"]
            row.update(p95_ms=nearest_rank(xs, 0.95), p50_ms=nearest_rank(xs, 0.50), max_ms=max(xs))
        if r.cls != HEALTHY:
            row["excluded_because"] = f"class {r.cls} (only {HEALTHY} under approved thresholds is a Fabric result)"
        elif lat["n_failed"] or not lat["n_success"]:
            row["excluded_because"] = f"{lat['n_failed']} failed transactions" if lat["n_failed"] else "no successful transaction"
        else:
            row["usable_for_contrast"] = True
        out.append(row)
    return out


# ----------------------------------------------------------------------------------------------- descriptive statistics (closed form)
def exact_permutation_p_two_sided(a: list[float], b: list[float]) -> float:
    """Exact two-sided permutation p value for the difference of means of two small samples (all C(n+m, n) assignments)."""
    pooled, n = list(a) + list(b), len(a)
    obs = abs(statistics.fmean(b) - statistics.fmean(a))
    hits = total = 0
    for idx in itertools.combinations(range(len(pooled)), n):
        left = [pooled[i] for i in idx]
        right = [pooled[i] for i in range(len(pooled)) if i not in idx]
        total += 1
        hits += abs(statistics.fmean(right) - statistics.fmean(left)) >= obs - 1e-9
    return hits / total


def minimum_attainable_p_two_sided(n: int, m: int) -> float:
    """Smallest two-sided exact permutation p the design can give for samples of n and m: 2 / C(n+m, n), capped at 1."""
    return min(1.0, 2.0 / math.comb(n + m, n))


def minimum_signed_rank_p_two_sided(k: int) -> float:
    """Smallest two-sided exact Wilcoxon signed-rank p with k pairs: 2 / 2^k, capped at 1 (k = 3: 0.25)."""
    return min(1.0, 2.0 / 2 ** k)


def sd_interval_df2(sd: float) -> dict:
    """95 % chi-square interval for a standard deviation from n = 3 observations (df = 2), assuming normality. Closed form: chi2 quantiles at df 2 are -2 ln(1 - p)."""
    lo_q, hi_q = -2.0 * math.log(1 - 0.025), -2.0 * math.log(1 - 0.975)          # chi2_{0.025,2}, chi2_{0.975,2}
    return {"assumes": "normal errors, n = 3 (df = 2)", "lower_ms": sd * math.sqrt(2.0 / hi_q), "upper_ms": sd * math.sqrt(2.0 / lo_q),
            "multipliers": [math.sqrt(2.0 / hi_q), math.sqrt(2.0 / lo_q)]}


def _arm_summary(values: list[float]) -> dict:
    if not values:
        return {"n_usable": 0}
    out = {"n_usable": len(values), "values_ms": [float(v) for v in values], "median_ms": float(statistics.median(values)), "min_ms": float(min(values)), "max_ms": float(max(values)),
           "range_ms": float(max(values) - min(values))}
    if len(values) >= 2:
        out["sd_ms"] = float(statistics.stdev(values))
        if len(values) == 3:
            out["sd_interval_95"] = sd_interval_df2(out["sd_ms"])
    return out


# ----------------------------------------------------------------------------------------------- the analysis
def analyze(runs: list[RunInput]) -> dict:
    table = per_run_table(runs)
    usable = {B: [r for r in table if r["B"] == B and r["usable_for_contrast"]] for B in ARMS}
    arms = {str(B): _arm_summary([r["p95_ms"] for r in usable[B]]) for B in ARMS}
    n10, n40 = len(usable[10]), len(usable[40])
    status = "NOT_COMPUTABLE" if not (n10 and n40) else "COMPUTED_n3_per_arm" if (n10 == n40 == 3) else f"PARTIAL_n{n10}_vs_n{n40}"
    measured: dict = {"estimand": "run-level pooled per-transaction p95 latency, B=40 minus B=10, same window (F_82800), schedule A", "run_p95_convention": RUN_P95_CONVENTION,
                      "runs": table, "arms": arms, "contrast_status": status}
    cross, matched = [], []
    if n10 and n40:
        for a in usable[10]:
            for b in usable[40]:
                cross.append({"B10_run": a["run_id"], "B40_run": b["run_id"], "diff_ms": b["p95_ms"] - a["p95_ms"]})
        for a in usable[10]:
            for b in usable[40]:
                if a["replicate"] == b["replicate"]:
                    matched.append({"replicate": a["replicate"], "B10_run": a["run_id"], "B40_run": b["run_id"], "diff_ms": b["p95_ms"] - a["p95_ms"]})
        diffs = [c["diff_ms"] for c in cross]
        v10, v40 = [r["p95_ms"] for r in usable[10]], [r["p95_ms"] for r in usable[40]]
        measured["contrast"] = {
            "difference_of_medians_ms": float(statistics.median(v40) - statistics.median(v10)),
            "hodges_lehmann_shift_ms": float(statistics.median(diffs)), "cross_difference_range_ms": [float(min(diffs)), float(max(diffs))], "n_cross_pairs": len(cross),
            "separation": ("B40_ALL_ABOVE_B10_ALL" if min(v40) > max(v10) else "B40_ALL_BELOW_B10_ALL" if max(v40) < min(v10) else "OVERLAP"),
            "gap_over_largest_within_arm_range": (float(statistics.median(v40) - statistics.median(v10)) / max(max(v10) - min(v10), max(v40) - min(v40)))
            if max(max(v10) - min(v10), max(v40) - min(v40)) > 0 else None,
            "cross_pairs": cross, "matched_replicate_pairs": matched,
            "matched_pairing_note": "replicate numbers are labels within an arm, not a physical pairing (runs are separated in time and seeded in order); the matched list is nominal, "
                                    "the 3 x 3 cross list is order-free",
            "an1_any_B40_run_below_any_B10_run": any(d < 0 for d in diffs), "an1_matched_pairs_with_B40_lower": [m["replicate"] for m in matched if m["diff_ms"] < 0],
            "an1_meaning": "AN1: a B=40 run that beats B=10 falsifies the law's direction in this regime; reported, never a stop condition"}
        measured["descriptive_uncertainty"] = {
            "kind": "DESCRIPTIVE_ONLY", "inference": "NOT_CLAIMED",
            "exact_permutation_p_two_sided_diff_of_means": exact_permutation_p_two_sided(v10, v40),
            "minimum_attainable_p_two_sided": minimum_attainable_p_two_sided(len(v10), len(v40)),
            "minimum_signed_rank_p_two_sided_for_matched_pairs": minimum_signed_rank_p_two_sided(len(matched)) if matched else None,
            "reading": "at 3 runs per arm the smallest p value the design can produce is 0.10 (two-sided exact permutation), which cannot reach a conventional alpha, "
                       "so no result of this experiment can be called significant. The p value above is reported only as the exact small-sample figure.",
            "bootstrap": "not computed: the project rule is a bootstrap interval only from n >= 8 per group; at n = 3 the full ranges above are the interval"}
    law = {"mapping": M3_NAME, "status": "HYPOTHESIS_NOT_A_VALIDATED_PREDICTION", "stored_ms": {"B10": M3_STORED_MS[10], "B40": M3_STORED_MS[40]}, "gap_ms": M3_GAP_MS,
           "source": "values STORED in the P4 draft section 3 (prereg.LAW_TABLE_P4_DRAFT), used as stored; not recomputed", "convention": M3_CONVENTION,
           "estimand_mismatch": "M3 is a quantile of per-interval law p95 values; the measured p95 is pooled over transactions. The two are different quantities, so a difference between "
                                "them is not by itself a failure of the law."}
    if cross:
        law["measured_minus_law"] = {
            "arm_median_p95_ms": {"B10": {"measured": arms["10"]["median_ms"], "law": M3_STORED_MS[10], "measured_minus_law_ms": arms["10"]["median_ms"] - M3_STORED_MS[10]},
                                  "B40": {"measured": arms["40"]["median_ms"], "law": M3_STORED_MS[40], "measured_minus_law_ms": arms["40"]["median_ms"] - M3_STORED_MS[40]}},
            "each_cross_pair": [{**c, "minus_law_gap_ms": c["diff_ms"] - M3_GAP_MS, "same_direction_as_law": c["diff_ms"] > 0} for c in cross],
            "each_matched_pair": [{**m, "minus_law_gap_ms": m["diff_ms"] - M3_GAP_MS, "same_direction_as_law": m["diff_ms"] > 0} for m in matched]}
        n_dir = sum(1 for c in cross if c["diff_ms"] > 0)
        law["lp1_directional"] = {
            "rule": "LP1 is DIRECTIONAL ONLY: the owner withdrew the 0.6 s magnitude threshold (decision D4); each pair's difference is reported against the 164 ms M3 gap",
            "cross_pairs_with_B40_above_B10": n_dir, "of": len(cross), "matched_pairs_with_B40_above_B10": sum(1 for m in matched if m["diff_ms"] > 0), "of_matched": len(matched),
            "directional_agreement_all_pairs": n_dir == len(cross),
            "not_statistical_confirmation": "directional agreement of 3 x 3 runs is a descriptive observation; it is not a confirmation, and its smallest attainable p is 0.10"}
        lo2, hi2 = pr.LP2_BAND_MS
        lo3, hi3 = pr.LP3_BAND_MS
        law["bands_descriptive"] = {"LP2_B40_in_band_ms": list(pr.LP2_BAND_MS), "B40_runs_inside": [r["run_id"] for r in usable[40] if lo2 <= r["p95_ms"] <= hi2],
                                    "LP3_B10_in_band_ms": list(pr.LP3_BAND_MS), "B10_runs_inside": [r["run_id"] for r in usable[10] if lo3 <= r["p95_ms"] <= hi3],
                                    "note": "descriptive; LP3's band holds all four mappings and cannot discriminate between them; LP4 is withdrawn (A-only design)"}
    limitations = [
        "n = 3 runs per arm: descriptive only. No significance, confirmation or effect-size claim; the smallest attainable exact two-sided p is 0.10.",
        "Run order is seeded and one window is used: the result is for this window (F_82800, one day) and this machine; it says nothing about other days, rates, hardware or HFBTP.",
        "Runs are separated in time on one machine: drift (thermal, Docker, WSL, background load) is confounded with order within a pair; the seeded order limits, not removes, this.",
        "F_82800 lies in the F pool that was already scored offline; the replay predictions are not blind to the F rate distribution (carried disclosure C2, C3).",
        "A HEALTHY_REPLAY class rests on engineering-safeguard thresholds with no empirical basis until the owner approves them with one.",
        "The pooled p95 includes the drain tail of each round and depends on the nearest-rank convention; a different quantile convention can move it by one rank.",
        "Backlog and resource exhaustion are not watched live (no watchdog): a run that saturates is seen only afterwards, through its drain and in-flight figures.",
        "The M3 comparison tests a hypothesis about the law, with an estimand that differs from the measurement; see law_comparison.estimand_mismatch."]
    return {"tool": f"p4_replay.analysis_primary {PACKAGE_VERSION}", "status_label": "DESCRIPTIVE_n3_NO_INFERENCE", "measured_outcomes": measured,
            "law_comparison_hypothesis_not_measured": law, "limitations": limitations}


# ----------------------------------------------------------------------------------------------- manifest in, report out
def load_manifest(path: Path) -> list[RunInput]:
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if doc.get("schema") != SCHEMA:
        raise AnalysisInputError(f"{path}: schema {doc.get('schema')!r} is not {SCHEMA!r}")
    runs = []
    for r in doc["runs"]:
        runs.append(RunInput(r["run_id"], int(r["B"]), int(r["replicate"]), r.get("position"), str(r["class"]), Path(r["recorder_csv"]) if r.get("recorder_csv") else None, r.get("recorder_sha256")))
    return runs


def render_markdown(report: dict) -> str:
    m, law = report["measured_outcomes"], report["law_comparison_hypothesis_not_measured"]
    f = lambda v: "n/a" if v is None else f"{v:,.1f}"                       # noqa: E731
    lines = ["# Primary analysis: run-level pooled p95 latency, B=40 vs B=10", "", f"Status: **{report['status_label']}**. Descriptive only; no inference is claimed.", "",
             "## A. Measured outcomes", "", f"Estimand: {m['estimand']}. Run p95 convention: {m['run_p95_convention']}.", "",
             "| run | B | rep | order | class | p95 (ms) | tx | used |", "|---|---|---|---|---|---|---|---|"]
    for r in m["runs"]:
        lines.append(f"| {r['run_id']} | {r['B']} | {r['replicate']} | {r['order_position']} | {r['class']} | {f(r['p95_ms'])} | {r['n_transactions']} | "
                     f"{'yes' if r['usable_for_contrast'] else 'no: ' + str(r['excluded_because'])} |")
    lines += ["", f"Contrast status: **{m['contrast_status']}**.", ""]
    for B in ("10", "40"):
        a = m["arms"][B]
        lines.append(f"- B={B}: n={a['n_usable']}" + (f", median {f(a['median_ms'])} ms, range [{f(a['min_ms'])}, {f(a['max_ms'])}] ms" if a["n_usable"] else ""))
    if "contrast" in m:
        c, u = m["contrast"], m["descriptive_uncertainty"]
        lines += ["", f"- Difference of medians (B40 - B10): {f(c['difference_of_medians_ms'])} ms; Hodges-Lehmann shift {f(c['hodges_lehmann_shift_ms'])} ms; "
                      f"all {c['n_cross_pairs']} cross differences in [{f(c['cross_difference_range_ms'][0])}, {f(c['cross_difference_range_ms'][1])}] ms; separation: {c['separation']}.",
                  f"- AN1 (a B=40 run below a B=10 run): {c['an1_any_B40_run_below_any_B10_run']}.",
                  f"- Descriptive uncertainty: exact permutation p (two-sided, diff of means) {u['exact_permutation_p_two_sided_diff_of_means']:.3f}; "
                  f"smallest attainable {u['minimum_attainable_p_two_sided']:.3f}. {u['reading']}"]
    lines += ["", "## B. Law reference M3 (a hypothesis, NOT a measurement)", "",
              f"M3 (stored): B=10 {M3_STORED_MS[10]} ms, B=40 {M3_STORED_MS[40]} ms, gap {M3_GAP_MS} ms. Convention status: **{law['convention']['status']}** "
              f"(weighted lower inverse-CDF step, no interpolation; read from `{law['convention']['found_in']}`). {law['estimand_mismatch']}", ""]
    if "lp1_directional" in law:
        d = law["lp1_directional"]
        lines += [f"- LP1, directional only: B40 above B10 in {d['cross_pairs_with_B40_above_B10']} of {d['of']} cross pairs. {d['not_statistical_confirmation']}", ""]
        lines += ["| B10 run | B40 run | measured diff (ms) | minus M3 gap (ms) | same direction |", "|---|---|---|---|---|"]
        lines += [f"| {p['B10_run']} | {p['B40_run']} | {f(p['diff_ms'])} | {f(p['minus_law_gap_ms'])} | {p['same_direction_as_law']} |" for p in law["measured_minus_law"]["each_cross_pair"]]
    lines += ["", "## C. Limits", ""] + [f"- {x}" for x in report["limitations"]] + [""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="p4_replay.analysis_primary", description="Primary analysis of a measured launch (offline; reads recorder csv files, writes two new files).")
    ap.add_argument("--manifest", type=Path, required=True, help="analysis_inputs.json written by the launch")
    ap.add_argument("--out-json", type=Path, required=True)
    ap.add_argument("--out-md", type=Path, required=True)
    args = ap.parse_args(argv)
    try:
        report = analyze(load_manifest(args.manifest))
        for path, text in ((args.out_json, json.dumps(report, indent=2)), (args.out_md, render_markdown(report))):
            with open(path, "x", encoding="utf-8", newline="\n") as fh:
                fh.write(text)
    except (AnalysisInputError, OSError, KeyError, ValueError) as exc:
        print(f"REFUSED: {exc!r}")
        return 2
    print(f"{report['measured_outcomes']['contrast_status']}: wrote {args.out_json} and {args.out_md}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
