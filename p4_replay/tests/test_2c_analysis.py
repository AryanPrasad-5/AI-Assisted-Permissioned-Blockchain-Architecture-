"""Phase 2C, Package B: the primary analysis script, validated on SYNTHETIC data before any measured run. Quantile conventions, per-run extraction, the paired comparison, the n = 3
limits, the M3 law section kept apart from measured outcomes, LP1 as direction only, and refusal on tampered evidence. Nothing here touches a real run."""

import hashlib
import json
import math
import re
from fractions import Fraction
from pathlib import Path

import numpy as np
import pytest

from p4_replay import REPO_ROOT, analysis_primary as an, prereg as pr


def write_run(tmp: Path, name: str, latencies, *, failed=0) -> tuple[Path, str]:
    rows = ["tx_id,create_ms,final_ms,status,verified"]
    for i, lat in enumerate(latencies):
        rows.append(f"{name}t{i},{1_000_000 + i * 10},{1_000_000 + i * 10 + lat},success,true")
    for j in range(failed):
        rows.append(f"{name}f{j},1,2,failed,false")
    p = tmp / f"{name}.csv"
    p.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return p, hashlib.sha256(p.read_bytes()).hexdigest()


def runs_with(tmp, spec, cls="HEALTHY_REPLAY"):
    """spec: {run_id: (B, replicate, position, p95-ish latency level)}; the 100 latencies make the nearest-rank p95 exactly `level`."""
    out = []
    for rid, (B, rep, pos, level) in spec.items():
        lat = [level - 100 - (k % 7) for k in range(95)] + [level] * 5                            # 100 values; the 95th smallest is below `level`, so make it exact:
        lat = sorted(lat)
        lat[94] = level                                                                            # nearest rank of 100 values at 0.95 is the 95th smallest
        lat[95:] = [level + 50] * 5
        p, h = write_run(tmp, rid, lat)
        c = cls[rid] if isinstance(cls, dict) else cls
        out.append(an.RunInput(rid, B, rep, pos, c, p, h))
    return out


SPEC = {"rp01": (10, 1, 1, 1790.0), "rp02": (40, 1, 2, 1950.0), "rp03": (40, 2, 3, 1960.0), "rp04": (10, 2, 4, 1800.0), "rp05": (10, 3, 5, 1810.0), "rp06": (40, 3, 6, 1940.0)}


# ----------------------------------------------------------------------------------------------- quantiles
@pytest.mark.parametrize("n", list(range(1, 130)) + [199, 200, 201, 1000, 6229, 6887])
def test_nearest_rank_equals_the_exact_rational_definition(n):
    xs = list(np.random.default_rng(n).permutation(n).astype(float))
    exact = sorted(xs)[math.ceil(Fraction(95, 100) * n) - 1]
    assert an.nearest_rank(xs, 0.95) == exact


def test_nearest_rank_basics_and_refusals():
    assert an.nearest_rank([5.0], 0.95) == 5.0 and an.nearest_rank([1, 2, 3, 4], 1.0) == 4 and an.nearest_rank([1, 2, 3, 4], 0.25) == 1
    for bad in (0, -0.1, 1.5):
        with pytest.raises(ValueError):
            an.nearest_rank([1, 2], bad)
    with pytest.raises(ValueError):
        an.nearest_rank([], 0.95)


def test_the_m3_quantile_function_is_a_verbatim_transcription_of_the_source_and_is_a_lower_step():
    src = (REPO_ROOT / "scripts" / "replay" / "demo_offline.py").read_text(encoding="utf-8")
    for line in ("ok = np.isfinite(values) & (weights > 0)", "order = np.argsort(values[ok])", "v, w = values[ok][order], weights[ok][order]",
                 "return float(v[np.searchsorted(np.cumsum(w), q * w.sum())])"):
        assert line in src, f"the convention source no longer contains: {line}"
    assert an.M3_CONVENTION["interpolation"].startswith("NONE")
    assert an.weighted_quantile_lower([1, 2, 3, 4], [1, 1, 1, 1], 0.5) == 2.0              # the lower of the two middle values, no interpolation to 2.5
    assert an.weighted_quantile_lower([1, 2, 3, 4], [1, 1, 1, 1], 0.95) == 4.0
    assert an.weighted_quantile_lower([1, 2, 3, 4], [10, 1, 1, 1], 0.5) == 1.0               # weights matter
    assert an.weighted_quantile_lower([4, 1, 3, 2], [1, 1, 1, 1], 0.5) == 2.0                # order of input is irrelevant
    assert an.weighted_quantile_lower([1, np.nan, 2], [1, 5, 1], 0.5) == 1.0                 # non-finite values are dropped
    assert an.weighted_quantile_lower([1, 2, 3], [1, 0, 1], 0.5) == 1.0                      # zero weights are dropped


def test_the_m3_function_returns_an_input_value_and_equals_nearest_rank_with_equal_weights():
    rng = np.random.default_rng(3)
    for n in (7, 20, 99, 100, 256):
        v = rng.normal(size=n)
        w = rng.uniform(0.5, 2.0, size=n)
        assert an.weighted_quantile_lower(v, w, 0.95) in set(v)
        assert an.weighted_quantile_lower(v, np.ones(n), 0.95) == an.nearest_rank(v, 0.95)


def test_the_m3_numbers_are_the_stored_ones_and_the_convention_metadata_keeps_acceptance_reproduction_provenance_and_validation_apart():
    assert (an.M3_STORED_MS[10], an.M3_STORED_MS[40], an.M3_GAP_MS) == (1776, 1940, 164)
    assert pr.LAW_TABLE_P4_DRAFT[("A", 10)][2] == 1776 and pr.LAW_TABLE_P4_DRAFT[("A", 40)][2] == 1940
    c = an.M3_CONVENTION
    # (a) owner acceptance with limitations: it is not OWNER_REVIEWED and not the approval key
    assert c["status"] == "OWNER_ACCEPTED_WITH_LIMITATIONS_2026-11-15"
    assert "m3_convention_review" not in c and "OWNER_REVIEWED" not in json.dumps(c)
    assert "02c_PREREGISTRATION_AMENDMENT_M3_CONVENTION_REV2_2026-11-15.md" in c["owner_decision"] and "06b_N2_OWNER_DECISION_RECORD_REV2_2026-11-15.md" in c["owner_decision"]
    assert "M3_p95ofintervals_1s" in c["owner_decision"] and "separate owner act" in c["owner_decision"]
    assert c["accepted_limitations"].startswith("L1-L10 as worded in section 2 of amendment 02c") and "state at the date of that amendment" in c["accepted_limitations"]
    # (b) one preserved reproduction under the current environment; the stale 'not run' wording is gone
    assert c["reproduced"] is True and c["reproduction_status"] == "REPRODUCED_2026-11-15_ONE_PRESERVED_RUN_CURRENT_ENVIRONMENT"
    note = c["reproduction_note"]
    assert note.startswith("REPRODUCED, 2026-11-15, one preserved run") and "REPRODUCTION NOT RUN" not in note
    assert "1776 ms" in note and "1940 ms" in note and "164 ms" in note and "not measured pooled run p95" in note
    assert "not historical provenance" in note and "owner-supplied" in note and "not recorded in the outputs" in note
    ev = c["reproduction_evidence"]
    assert ev["report_file"] == "demo_offline_report.json" and ev["report_sha256_prefix"] == "ca6fc5e6" and ev["report_sha256_prefix"] in note
    assert ev["checksums_file"].startswith("SHA256SUMS.txt")
    # (c) historical provenance stays incomplete
    assert c["historical_provenance"].startswith("INCOMPLETE") and "untracked in Git" in c["historical_provenance"] and "L5" in c["historical_provenance"]
    # (d) no validation against a measured pooled run-level p95
    assert c["validation_against_measured_run_p95"].startswith("NOT_VALIDATED") and "does not validate M3" in c["validation_against_measured_run_p95"]
    # limitations: [0] corrected, the other three kept, no stale wording
    assert len(c["limitations"]) == 4
    assert c["limitations"][0].startswith("The convention was stated in no protocol text until preregistration amendment 02b") and "not a validation" in c["limitations"][0]
    assert any("2 s batching timeout" in x for x in c["limitations"]) and any("different estimand" in x for x in c["limitations"])
    assert any("2 s timer ceiling" in x and "1940 ms" in x for x in c["limitations"])
    assert not any("must review" in x or "not stated in any protocol" in x for x in c["limitations"])
    # the pins test bans 64-hex literals in Phase 2C modules: the metadata carries only an 8-hex prefix
    assert not re.search(r"(?<![0-9a-fA-F])[0-9a-fA-F]{64}(?![0-9a-fA-F])", json.dumps(c))


# ----------------------------------------------------------------------------------------------- per-run extraction
def test_per_run_p95_counts_failures_and_drops_them(tmp_path):
    p, h = write_run(tmp_path, "r", [100.0] * 20 + [900.0], failed=3)
    d = an.read_latencies(p, h)
    assert d["n_success"] == 21 and d["n_failed"] == 3 and an.nearest_rank(d["latencies_ms"], 0.95) == 100.0


def test_extraction_refuses_a_bad_file(tmp_path):
    with pytest.raises(an.AnalysisInputError):
        an.read_latencies(tmp_path / "nope.csv")
    bad = tmp_path / "bad.csv"
    bad.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(an.AnalysisInputError):
        an.read_latencies(bad)
    neg = tmp_path / "neg.csv"
    neg.write_text("tx_id,create_ms,final_ms,status,verified\nx,10,5,success,true\n", encoding="utf-8")
    with pytest.raises(an.AnalysisInputError, match="negative"):
        an.read_latencies(neg)
    p, h = write_run(tmp_path, "ok", [1.0, 2.0])
    p.write_text(p.read_text() + "tampered,1,2,success,true\n", encoding="utf-8")
    with pytest.raises(an.IntegrityError):
        an.read_latencies(p, h)


# ----------------------------------------------------------------------------------------------- the paired comparison
def test_three_by_three_all_healthy_gives_every_descriptive_figure(tmp_path):
    rep = an.analyze(runs_with(tmp_path, SPEC))
    m = rep["measured_outcomes"]
    assert m["contrast_status"] == "COMPUTED_n3_per_arm" and rep["status_label"] == "DESCRIPTIVE_n3_NO_INFERENCE"
    assert {r["run_id"]: r["p95_ms"] for r in m["runs"]} == {k: v[3] for k, v in SPEC.items()}
    a10, a40 = m["arms"]["10"], m["arms"]["40"]
    assert a10["values_ms"] == [1790.0, 1800.0, 1810.0] and a10["median_ms"] == 1800.0 and a10["range_ms"] == 20.0
    assert a40["values_ms"] == [1950.0, 1960.0, 1940.0] and a40["median_ms"] == 1950.0 and (a40["min_ms"], a40["max_ms"]) == (1940.0, 1960.0)
    c = m["contrast"]
    assert c["difference_of_medians_ms"] == 150.0 and c["n_cross_pairs"] == 9 and c["cross_difference_range_ms"] == [130.0, 170.0] and c["hodges_lehmann_shift_ms"] == 150.0
    assert c["separation"] == "B40_ALL_ABOVE_B10_ALL" and c["an1_any_B40_run_below_any_B10_run"] is False and c["an1_matched_pairs_with_B40_lower"] == []
    assert sorted((p["replicate"], p["diff_ms"]) for p in c["matched_replicate_pairs"]) == [(1, 160.0), (2, 160.0), (3, 130.0)]
    assert "nominal" in c["matched_pairing_note"] and "order-free" in c["matched_pairing_note"]


def test_the_n3_limits_are_stated_as_closed_form_facts(tmp_path):
    rep = an.analyze(runs_with(tmp_path, SPEC))
    u = rep["measured_outcomes"]["descriptive_uncertainty"]
    assert u["kind"] == "DESCRIPTIVE_ONLY" and u["inference"] == "NOT_CLAIMED"
    assert u["minimum_attainable_p_two_sided"] == pytest.approx(0.1) and u["exact_permutation_p_two_sided_diff_of_means"] == pytest.approx(0.1)      # complete separation: the floor
    assert u["minimum_signed_rank_p_two_sided_for_matched_pairs"] == pytest.approx(0.25)
    assert "cannot reach a conventional alpha" in u["reading"] and "no result of this experiment can be called significant" in u["reading"] and "n >= 8" in u["bootstrap"]
    assert an.minimum_attainable_p_two_sided(3, 3) == pytest.approx(0.1) and an.minimum_attainable_p_two_sided(1, 1) == 1.0 and an.minimum_signed_rank_p_two_sided(1) == 1.0
    lo, hi = an.sd_interval_df2(1.0)["multipliers"]
    assert lo == pytest.approx(0.52, abs=0.005) and hi == pytest.approx(6.28, abs=0.01), "the project's stored chi-square interval for an SD at n = 3"
    assert rep["measured_outcomes"]["arms"]["10"]["sd_interval_95"]["assumes"].startswith("normal errors")


def test_the_exact_permutation_p_value_is_exact():
    assert an.exact_permutation_p_two_sided([1, 2, 3], [4, 5, 6]) == pytest.approx(0.1)
    assert an.exact_permutation_p_two_sided([1, 2, 3], [1, 2, 3]) == pytest.approx(1.0)
    assert an.exact_permutation_p_two_sided([1, 4, 5], [2, 3, 6]) == pytest.approx(1.0)       # a perfectly balanced split is never extreme
    assert an.exact_permutation_p_two_sided([1, 2], [3]) == pytest.approx(2 / 3)


def test_overlap_and_a_b40_run_beating_b10_are_reported_as_an1(tmp_path):
    spec = dict(SPEC, rp06=(40, 3, 6, 1795.0))                                                 # one B=40 run below two B=10 runs
    m = an.analyze(runs_with(tmp_path, spec))["measured_outcomes"]
    c = m["contrast"]
    assert c["separation"] == "OVERLAP" and c["an1_any_B40_run_below_any_B10_run"] is True and c["an1_matched_pairs_with_B40_lower"] == [3]
    assert c["cross_difference_range_ms"] == [-15.0, 170.0] and min(c["cross_difference_range_ms"]) < 0 and "falsifies the law's direction" in c["an1_meaning"] and "never a stop condition" in c["an1_meaning"]


# ----------------------------------------------------------------------------------------------- which runs enter
def test_a_limited_run_is_listed_and_excluded_never_silently_dropped_or_reduced(tmp_path):
    cls = {k: "HEALTHY_REPLAY" for k in SPEC} | {"rp03": "FIDELITY_LIMITED_REPLAY"}
    m = an.analyze(runs_with(tmp_path, SPEC, cls))["measured_outcomes"]
    row = next(r for r in m["runs"] if r["run_id"] == "rp03")
    assert row["usable_for_contrast"] is False and "FIDELITY_LIMITED_REPLAY" in row["excluded_because"] and row["p95_ms"] == 1960.0
    assert m["arms"]["40"]["n_usable"] == 2 and m["contrast_status"] == "PARTIAL_n3_vs_n2" and m["contrast"]["n_cross_pairs"] == 6
    assert "sd_interval_95" not in m["arms"]["40"]


def test_a_provisional_class_is_not_a_fabric_result_and_is_excluded(tmp_path):
    m = an.analyze(runs_with(tmp_path, SPEC, "HEALTHY_REPLAY_PROVISIONAL"))["measured_outcomes"]
    assert m["contrast_status"] == "NOT_COMPUTABLE" and "contrast" not in m and all("HEALTHY_REPLAY_PROVISIONAL" in r["excluded_because"] for r in m["runs"])


def test_missing_evidence_excludes_with_a_reason_and_a_missing_arm_is_not_computable(tmp_path):
    runs = runs_with(tmp_path, SPEC)
    runs = [an.RunInput(r.run_id, r.B, r.replicate, r.position, r.cls, None if r.B == 40 else r.recorder_csv, r.recorder_sha256) for r in runs]
    rep = an.analyze(runs)
    m = rep["measured_outcomes"]
    assert m["contrast_status"] == "NOT_COMPUTABLE" and "contrast" not in m and all("EVIDENCE_UNAVAILABLE" in r["excluded_because"] for r in m["runs"] if r["B"] == 40)
    assert "measured_minus_law" not in rep["law_comparison_hypothesis_not_measured"]


def test_a_tampered_recorder_file_refuses_the_whole_analysis(tmp_path):
    runs = runs_with(tmp_path, SPEC)
    runs[0].recorder_csv.write_text(runs[0].recorder_csv.read_text() + "x,1,2,success,true\n", encoding="utf-8")
    with pytest.raises(an.IntegrityError):
        an.analyze(runs)


# ----------------------------------------------------------------------------------------------- the law is separate, M3 is a hypothesis, LP1 is direction only
def test_the_law_comparison_is_a_separate_hypothesis_section_and_measured_outcomes_hold_no_law_number(tmp_path):
    rep = an.analyze(runs_with(tmp_path, SPEC))
    assert set(rep) >= {"measured_outcomes", "law_comparison_hypothesis_not_measured", "limitations"}
    measured_text = json.dumps(rep["measured_outcomes"])
    assert "1776" not in measured_text and not {"stored_ms", "gap_ms", "mapping", "convention"} & set(rep["measured_outcomes"])
    assert "measured_minus_law" not in rep["measured_outcomes"] and "lp1_directional" not in rep["measured_outcomes"]
    law = rep["law_comparison_hypothesis_not_measured"]
    assert law["status"] == "HYPOTHESIS_NOT_A_VALIDATED_PREDICTION" and law["stored_ms"] == {"B10": 1776, "B40": 1940} and law["gap_ms"] == 164 and law["mapping"] == "M3_p95ofintervals_1s"
    assert "different quantities" in law["estimand_mismatch"] and "not recomputed" in law["source"]


def test_lp1_is_directional_only_and_each_pair_is_reported_against_the_164_ms_gap(tmp_path):
    law = an.analyze(runs_with(tmp_path, SPEC))["law_comparison_hypothesis_not_measured"]
    d = law["lp1_directional"]
    assert "DIRECTIONAL ONLY" in d["rule"] and "0.6 s magnitude threshold" in d["rule"] and "withdrew" in d["rule"]
    assert d["cross_pairs_with_B40_above_B10"] == 9 and d["of"] == 9 and d["directional_agreement_all_pairs"] is True and "not a confirmation" in d["not_statistical_confirmation"]
    pairs = law["measured_minus_law"]["each_cross_pair"]
    assert len(pairs) == 9 and all(p["minus_law_gap_ms"] == p["diff_ms"] - 164 and p["same_direction_as_law"] is True for p in pairs)
    assert law["measured_minus_law"]["arm_median_p95_ms"]["B10"] == {"measured": 1800.0, "law": 1776, "measured_minus_law_ms": 24.0}
    assert not any(k in json.dumps(law) for k in ("lp1_min_gap_ms", "600 ms threshold applied"))
    assert "LP4 is withdrawn" in law["bands_descriptive"]["note"]


def test_the_report_never_claims_significance_or_confirmation(tmp_path):
    rep = an.analyze(runs_with(tmp_path, SPEC))
    md = an.render_markdown(rep)
    text = (json.dumps(rep) + md).lower()
    for phrase in ("is significant", "are significant", "statistically significant", "confirms the", "confirmed the", "p < 0.05", "significant at"):
        assert phrase not in text, phrase
    assert "descriptive only; no inference is claimed" in md.lower() and "a hypothesis, not a measurement" in md.lower() and "## c. limits" in md.lower()
    assert any("n = 3 runs per arm" in x for x in rep["limitations"]) and any("F_82800" in x and "not blind" in x for x in rep["limitations"])
    assert any("watchdog" in x for x in rep["limitations"]) and any("estimand" in x for x in rep["limitations"])


# ----------------------------------------------------------------------------------------------- the manifest and the command line
def write_manifest(tmp: Path, runs) -> Path:
    doc = {"schema": an.SCHEMA, "launch_id": "p4r-meas-01", "window": "F_82800",
           "runs": [{"run_id": r.run_id, "B": r.B, "replicate": r.replicate, "position": r.position, "class": r.cls, "recorder_csv": str(r.recorder_csv), "recorder_sha256": r.recorder_sha256} for r in runs]}
    p = tmp / "analysis_inputs.json"
    p.write_text(json.dumps(doc), encoding="utf-8")
    return p


def test_the_command_line_writes_two_new_files_and_never_overwrites(tmp_path):
    man = write_manifest(tmp_path, runs_with(tmp_path, SPEC))
    out_json, out_md = tmp_path / "r.json", tmp_path / "r.md"
    assert an.main(["--manifest", str(man), "--out-json", str(out_json), "--out-md", str(out_md)]) == 0
    assert json.loads(out_json.read_text())["measured_outcomes"]["contrast_status"] == "COMPUTED_n3_per_arm" and "Primary analysis" in out_md.read_text()
    before = out_json.read_bytes()
    assert an.main(["--manifest", str(man), "--out-json", str(out_json), "--out-md", str(tmp_path / "r2.md")]) == 2
    assert out_json.read_bytes() == before


def test_the_command_line_refuses_a_wrong_schema_a_missing_manifest_and_tampered_evidence(tmp_path):
    runs = runs_with(tmp_path, SPEC)
    man = write_manifest(tmp_path, runs)
    doc = json.loads(man.read_text())
    doc["schema"] = "other/1"
    man.write_text(json.dumps(doc), encoding="utf-8")
    assert an.main(["--manifest", str(man), "--out-json", str(tmp_path / "a.json"), "--out-md", str(tmp_path / "a.md")]) == 2
    assert an.main(["--manifest", str(tmp_path / "none.json"), "--out-json", str(tmp_path / "b.json"), "--out-md", str(tmp_path / "b.md")]) == 2
    man2 = write_manifest(tmp_path, runs)
    runs[0].recorder_csv.write_text("tx_id,create_ms,final_ms,status,verified\nz,1,2,success,true\n", encoding="utf-8")
    assert an.main(["--manifest", str(man2), "--out-json", str(tmp_path / "c.json"), "--out-md", str(tmp_path / "c.md")]) == 2
    assert not (tmp_path / "c.json").exists()


def test_the_analysis_module_imports_no_model_simulator_or_construct_validity_code():
    src = (REPO_ROOT / "p4_replay" / "analysis_primary.py").read_text(encoding="utf-8")
    for forbidden in ("cv_block_cutter", "cv_mechanism", "cv_frozen_models", "cv_e1_predictions", "cv_protocol_stats", "joblib", "sklearn", "run_voi", "law_voi", "demo_offline import"):
        assert not re.search(rf"^\s*(from|import)\s+.*{forbidden}", src, re.M), forbidden
