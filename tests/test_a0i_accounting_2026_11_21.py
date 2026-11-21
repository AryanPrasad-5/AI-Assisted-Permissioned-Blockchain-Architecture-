"""A0i abstention accounting (checklist items 34-42). Pure counting over decision rows; no model, no forecast, no performance claim.

Rows come from real offline Stage 3 fixture runs (synthetic captured day) and from hand-built rows for the edge cases.
"""

from __future__ import annotations

import copy
import json
import random

import pytest

from tests import a0i_support as s
from tests.a0i_support import acc, ch, wc

F = s.h.f
A = acc


def row(index, controller, *, status="accepted", outcome="hold", reason="fixed_configuration", frame_reason=None, ood=None, stage=None, error=None,
        stamp=None, schema="stage3_shadow_decision/2"):
    evidence = None
    if controller == "predictive":
        evidence = {"ood_status": ood or "not_applicable_fixture_model", "failure": None if error is None and stage is None else {"stage": stage, "error": error}}
    return {"schema": schema, "controller": controller, "outcome": outcome, "reason": reason, "abstention_scope": None,
            "frame": {"index": index, "status": status, "reason_code": frame_reason, "observation_timestamp_utc": stamp or f"2026-09-21T12:00:{index:02d}+00:00"},
            "predictive_evidence": evidence}


def frame_rows(index, *, status="accepted", predictive=None, reactive=None, static=None, frame_reason=None):
    """All three controllers for one frame. A shared invalid or initialization frame makes every controller abstain."""
    if status != "accepted":
        return [row(index, c, status=status, outcome="abstain", reason="shared_" + status, frame_reason=frame_reason or "frame_" + status) for c in A.CONTROLLERS]
    return [row(index, "static", **(static or {})), row(index, "reactive", **(reactive or {})), row(index, "predictive", **(predictive or dict(outcome="propose", reason="x")))]


HISTORY = dict(outcome="abstain", reason="workload_prefix_incomplete_fail_safe", stage="forecast", error="UTC day: 0 missing and 30 unavailable seconds")
GRID = dict(outcome="abstain", reason="prediction_failure_fail_safe", ood="outside_training_grid", stage="performance_model",
            error="arrival_rate_outside_training_grid: 7.5 tx/s is outside the HFBTP training range")
OTHER = dict(outcome="abstain", reason="prediction_failure_fail_safe", stage="performance_model", error="performance_output_invalid: non-finite")


def attested_run(tmp_path, *, steps=None, counts=None, name="run"):
    _, _, path = s.covered_journal(tmp_path, until=43_204 + (0 if steps is None else 4 * len(steps)), name=f"{name}.jsonl")
    return s.h.run_captured(tmp_path, s.captured(s.coverage_for(path), counts), steps=steps, name=name)


# ------------------------------------------------------------------------------------------------------------ 34 denominator
def test_34_the_denominator_is_the_scheduled_frame_list_and_every_controller_adds_up_to_it(tmp_path):
    history = s.captured(wc.VerifiedCoverage.no_evidence(s.SOURCE_ID, "none"), {100: 0})
    manifest, rows, out = s.h.run_captured(tmp_path, history)
    result = A.account_run_directory(out, segment="synthetic-day")
    assert result["denominator"]["scheduled_frames"] == 4 and result["consistent"] is True and result["run_status"] == "COMPLETED"
    assert result["frames"] == {"scheduled": 4, "valid_shared": 3, "shared_invalid": 0, "initialization": 1, "without_any_row": 0}
    pred, static = result["controllers"]["predictive"], result["controllers"]["static"]
    assert pred["total_scheduled_decision_opportunities"] == static["total_scheduled_decision_opportunities"] == 4
    assert pred["initialization_frames"] == 1 and pred["controller_specific_abstentions"] == {"total": 3, "predictive_history_abstention": 3,
                                                                                               "forecast_out_of_grid": 0, "controller_specific_failure": 0}
    assert pred["valid_recommendations"]["total"] == 0 and pred["decision_coverage_rate"] == {"numerator": 0, "denominator": 4, "rate": 0.0}
    assert static["valid_recommendations"] == {"total": 3, "propose": 0, "hold": 3} and static["decision_coverage_rate"]["rate"] == 0.75
    assert static["coverage_rate_given_valid_shared_frame"] == {"numerator": 3, "denominator": 3, "rate": 1.0}
    for controller in result["controllers"].values():
        assert sum(controller["category_counts"].values()) == 4
    assert pred["timestamp_ranges"]["predictive_history_abstention"]["frames"] == 3
    assert pred["timestamp_ranges"]["initialization"]["first_utc"] == pred["timestamp_ranges"]["initialization"]["last_utc"]
    assert "not realised blockchain performance" in result["interpretation"]


def test_34b_the_denominator_comes_from_the_caller_not_from_the_rows_found():
    rows = [r for i in range(3) for r in frame_rows(i)]
    short = A.account(rows, scheduled_frames=5, segment="seg")                 # two frames were scheduled but never produced a row
    assert short["frames"]["without_any_row"] == 2 and short["frames"]["scheduled"] == 5
    for name in A.CONTROLLERS:
        assert short["controllers"][name]["missing_decisions"] == 2 and short["controllers"][name]["total_scheduled_decision_opportunities"] == 5
    explicit = A.account(rows, scheduled_frames=[0, 2], segment="seg")        # a chosen subset of frames: row 1 is then unscheduled, and reported
    assert explicit["denominator"]["frame_indices"] == [0, 2] and explicit["consistent"] is False and "unscheduled frame 1" in explicit["problems"][0]


# ----------------------------------------------------------------------------------------------- 35 shared abstentions
def test_35_shared_invalid_and_initialization_frames_are_counted_once_per_controller():
    rows = (frame_rows(0, status="initialization", frame_reason="stage3_initialization_ledger_baseline") + frame_rows(1) +
            frame_rows(2, status="rejected", frame_reason="stale_telemetry_fail_safe") + frame_rows(3, status="rejected", frame_reason="telemetry_missing_fail_safe") +
            frame_rows(4))
    result = A.account(rows, scheduled_frames=5, segment="seg")
    assert result["frames"] == {"scheduled": 5, "valid_shared": 2, "shared_invalid": 2, "initialization": 1, "without_any_row": 0}
    for name in A.CONTROLLERS:
        c = result["controllers"][name]
        assert c["shared_invalid_frames"] == 2 and c["initialization_frames"] == 1 and c["controller_specific_abstentions"]["total"] == 0
        assert c["reason_breakdown"]["shared_invalid_frame"] == {"stale_telemetry_fail_safe": 1, "telemetry_missing_fail_safe": 1}
        assert c["valid_shared_frames"] == 2
    assert result["controllers"]["static"]["decision_coverage_rate"]["rate"] == pytest.approx(2 / 5)


def test_35b_controllers_that_disagree_about_a_shared_frame_are_reported_not_resolved():
    rows = frame_rows(0)
    rows[0] = row(0, "static", status="rejected", outcome="abstain", frame_reason="x")
    result = A.account(rows, scheduled_frames=1, segment="seg")
    assert result["consistent"] is False and "disagree" in result["problems"][0]


# ------------------------------------------------------------------------------------------------ 36 predictive-only, 37 grid
def test_36_a_predictive_only_abstention_leaves_the_other_controllers_valid():
    rows = frame_rows(0) + frame_rows(1, predictive=HISTORY) + frame_rows(2, predictive=HISTORY) + frame_rows(3)
    result = A.account(rows, scheduled_frames=4, segment="seg")
    p, st, re_ = (result["controllers"][n] for n in ("predictive", "static", "reactive"))
    assert p["controller_specific_abstentions"]["predictive_history_abstention"] == 2 and p["valid_recommendations"]["total"] == 2
    assert st["controller_specific_abstentions"]["total"] == 0 and re_["valid_recommendations"]["total"] == 4 and st["shared_invalid_frames"] == 0
    assert p["reason_breakdown"]["predictive_history_abstention"] == {"workload_prefix_incomplete_fail_safe": 2}
    assert p["valid_shared_frames"] == st["valid_shared_frames"] == 4                                   # the frame was valid for everyone


def test_37_a_forecast_outside_the_grid_is_its_own_category_and_a_real_run_agrees(tmp_path):
    steps = F.steady(3)
    steps[3] = {**steps[3], "forecast": 7.5}
    _, rows, out = attested_run(tmp_path, steps=steps)
    result = A.account_run_directory(out)
    p = result["controllers"]["predictive"]
    assert p["controller_specific_abstentions"]["forecast_out_of_grid"] == 1 and p["valid_recommendations"]["total"] == 2
    assert p["category_counts"]["predictive_history_abstention"] == 0
    synthetic = A.account(frame_rows(0, predictive=GRID) + frame_rows(1, predictive=dict(OTHER)), scheduled_frames=2, segment="seg")
    sp_ = synthetic["controllers"]["predictive"]
    assert sp_["controller_specific_abstentions"] == {"total": 2, "predictive_history_abstention": 0, "forecast_out_of_grid": 1, "controller_specific_failure": 1}


def test_37b_a_history_failure_inside_the_forecast_stage_is_a_predictive_history_abstention():
    stale = dict(outcome="abstain", reason="prediction_failure_fail_safe", stage="forecast", error="forecast_history_stale: last bin is 7.000s old, limit 5.0s")
    insufficient = dict(outcome="abstain", reason="prediction_failure_fail_safe", stage="forecast", error="forecast_history_insufficient: 300 bins supplied")
    unrelated = dict(outcome="abstain", reason="prediction_failure_fail_safe", stage="forecast", error="forecast_result_invalid: not finite")
    result = A.account(frame_rows(0, predictive=stale) + frame_rows(1, predictive=insufficient) + frame_rows(2, predictive=unrelated), scheduled_frames=3, segment="seg")
    assert result["controllers"]["predictive"]["controller_specific_abstentions"] == {"total": 3, "predictive_history_abstention": 2,
                                                                                      "forecast_out_of_grid": 0, "controller_specific_failure": 1}


# ------------------------------------------------------------------------------------------------------- 38 initialization
def test_38_initialization_frames_are_not_controller_abstentions_and_are_not_valid_recommendations():
    rows = frame_rows(0, status="initialization", frame_reason="stage3_initialization_ledger_baseline") + frame_rows(1)
    result = A.account(rows, scheduled_frames=2, segment="seg")
    for name in A.CONTROLLERS:
        c = result["controllers"][name]
        assert c["initialization_frames"] == 1 and c["controller_specific_abstentions"]["total"] == 0 and c["missing_decisions"] == 0
        assert c["category_counts"]["initialization"] == 1 and c["valid_shared_frames"] == 1
        assert c["reason_breakdown"]["initialization"] == {"stage3_initialization_ledger_baseline": 1}
    assert result["controllers"]["static"]["decision_coverage_rate"] == {"numerator": 1, "denominator": 2, "rate": 0.5}
    assert result["controllers"]["static"]["coverage_rate_given_valid_shared_frame"]["rate"] == 1.0


# ---------------------------------------------------------------------------------------------------------- 39 multiple reasons
def test_39_several_reasons_in_one_category_stay_separate_and_add_up():
    rows = (frame_rows(0, predictive=HISTORY) + frame_rows(1, predictive=OTHER) + frame_rows(2, predictive=dict(OTHER, reason="another_fail_safe")) +
            frame_rows(3, predictive=GRID) + frame_rows(4, predictive=dict(outcome="hold", reason="no_better_candidate")) + frame_rows(5))
    result = A.account(rows, scheduled_frames=6, segment="seg")
    p = result["controllers"]["predictive"]
    assert p["reason_breakdown"]["controller_specific_failure"] == {"another_fail_safe": 1, "prediction_failure_fail_safe": 1}
    assert p["reason_breakdown"]["valid_recommendation"] == {"hold": 1, "propose": 1}
    assert p["valid_recommendations"] == {"total": 2, "propose": 1, "hold": 1}
    assert sum(sum(v.values()) for v in p["reason_breakdown"].values()) == 6 == sum(p["category_counts"].values())


# ------------------------------------------------------------------------------------------------- 40 missing controller record
def test_40_a_scheduled_decision_with_no_row_is_counted_as_missing_not_dropped():
    rows = frame_rows(0) + frame_rows(1) + frame_rows(2)
    rows = [r for r in rows if not (r["frame"]["index"] == 1 and r["controller"] == "reactive")]
    result = A.account(rows, scheduled_frames=3, segment="seg")
    re_ = result["controllers"]["reactive"]
    assert re_["missing_decisions"] == 1 and re_["valid_recommendations"]["total"] == 2 and re_["decision_coverage_rate"]["denominator"] == 3
    assert re_["reason_breakdown"]["missing_decision"] == {"no_decision_record": 1} and result["frames"]["valid_shared"] == 3
    assert result["controllers"]["static"]["missing_decisions"] == 0 and result["consistent"] is True


def test_40b_an_interrupted_run_counts_its_unprocessed_frames_as_missing(tmp_path):
    _, _, out = s.h.run_captured(tmp_path, ch.CapturedHistory(s.h.binned(), zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL))
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert A.account_run_directory(out)["problems"] == []
    summary["frames_in_fixture"], summary["status"] = 7, "INTERRUPTED"
    copy_dir = tmp_path / "interrupted"
    copy_dir.mkdir()
    (copy_dir / "run_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (copy_dir / "shadow_decisions.jsonl").write_text((out / "shadow_decisions.jsonl").read_text(encoding="utf-8"), encoding="utf-8")
    result = A.account_run_directory(copy_dir)
    assert result["consistent"] is False and "INTERRUPTED" in result["problems"][0]
    assert result["controllers"]["static"]["missing_decisions"] == 3 and result["frames"]["without_any_row"] == 3


# ------------------------------------------------------------------------------------------------ 41 stable coverage-rate calculation
def test_41_rates_are_exact_fractions_none_when_undefined_and_independent_of_row_order():
    rows = [r for i in range(7) for r in frame_rows(i, predictive=HISTORY if i % 3 == 0 else dict(outcome="propose", reason="x"))]
    first = A.account(rows, scheduled_frames=7, segment="seg")
    shuffled = copy.deepcopy(rows)
    random.Random(7).shuffle(shuffled)
    second = A.account(shuffled, scheduled_frames=7, segment="seg")
    assert first == second
    assert first["controllers"]["predictive"]["decision_coverage_rate"] == {"numerator": 4, "denominator": 7, "rate": 4 / 7}
    empty = A.account([], scheduled_frames=0, segment="seg")
    for name in A.CONTROLLERS:
        assert empty["controllers"][name]["decision_coverage_rate"] == {"numerator": 0, "denominator": 0, "rate": None}
    pooled = A.combine_segments([first, A.account(frame_rows(0) + frame_rows(1), scheduled_frames=2, segment="other")])
    assert pooled["pooled_descriptive"]["predictive"]["scheduled"] == 9 and pooled["pooled_descriptive"]["predictive"]["decision_coverage_rate"]["numerator"] == 6
    assert [x["segment"] for x in pooled["per_segment"]] == ["seg", "other"] and "never only the pool" in pooled["note"]


# ----------------------------------------------------------------------------------------------------- 42 no silent frame exclusion
def test_42_nothing_is_silently_dropped_unknown_duplicate_and_unscheduled_rows_are_reported():
    rows = frame_rows(0) + frame_rows(1)
    rows.append(row(1, "static"))                                  # a duplicate (frame, controller) row
    rows.append(row(9, "reactive"))                                # a row for a frame that was never scheduled
    rows.append(row(0, "oracle"))                                  # a controller nobody asked about
    result = A.account(rows, scheduled_frames=2, segment="seg")
    assert result["consistent"] is False and len(result["problems"]) == 3
    assert any("duplicate row for frame 1 controller static" in p for p in result["problems"])
    assert any("unscheduled frame 9" in p for p in result["problems"]) and any("unknown controller 'oracle'" in p for p in result["problems"])
    assert result["controllers"]["static"]["valid_recommendations"]["total"] == 2                 # the duplicate did not inflate the count


def test_42b_declared_exclusions_are_reported_in_full_and_never_change_the_primary_rate():
    rows = frame_rows(0) + frame_rows(1, predictive=HISTORY) + frame_rows(2)
    plain = A.account(rows, scheduled_frames=3, segment="seg")
    excluded = A.account(rows, scheduled_frames=3, segment="seg", exclusions=[{"frame": 1, "reason": "operator declared: collector maintenance"}])
    assert excluded["exclusions"] == [{"frame": 1, "reason": "operator declared: collector maintenance"}]
    assert excluded["controllers"]["predictive"]["decision_coverage_rate"] == plain["controllers"]["predictive"]["decision_coverage_rate"] == {
        "numerator": 2, "denominator": 3, "rate": 2 / 3}
    assert excluded["controllers"]["predictive"]["coverage_rate_excluding_declared_exclusions"] == {"numerator": 2, "denominator": 2, "rate": 1.0}
    assert plain["controllers"]["predictive"]["coverage_rate_excluding_declared_exclusions"] is None
    assert excluded["controllers"]["predictive"]["category_counts"] == plain["controllers"]["predictive"]["category_counts"]       # the hard frame is still counted
    for bad in ([{"frame": 5, "reason": "x"}], [{"frame": 1, "reason": ""}], [{"frame": 1, "reason": "a"}, {"frame": 1, "reason": "b"}]):
        with pytest.raises(A.AccountingError):
            A.account(rows, scheduled_frames=3, segment="seg", exclusions=bad)


def test_42c_malformed_input_is_refused_instead_of_repaired():
    with pytest.raises(A.AccountingError, match="segment label"):
        A.account([], scheduled_frames=1, segment=" ")
    with pytest.raises(A.AccountingError, match="lists a frame twice"):
        A.account([], scheduled_frames=[0, 0], segment="seg")
    with pytest.raises(A.AccountingError, match="schema"):
        A.account([row(0, "static", schema="something/9")], scheduled_frames=1, segment="seg")
    with pytest.raises(A.AccountingError, match="unknown outcome"):
        A.account([row(0, "static", outcome="maybe")], scheduled_frames=1, segment="seg")
    with pytest.raises(A.AccountingError, match="unknown status"):
        A.account([row(0, "static", status="weird")], scheduled_frames=1, segment="seg")


def test_42d_for_any_mix_of_rows_every_controller_adds_up_to_the_denominator():
    rng = random.Random(20261121)
    pools = [dict(outcome="propose", reason="x"), dict(outcome="hold", reason="y"), HISTORY, GRID, OTHER]
    for _ in range(60):
        frames = rng.randint(1, 12)
        rows = []
        for i in range(frames):
            status = rng.choice(["accepted", "accepted", "accepted", "rejected", "initialization"])
            rows += frame_rows(i, status=status, predictive=rng.choice(pools), static=rng.choice(pools[:2]), reactive=rng.choice(pools[:2]))
        rows = [r for r in rows if rng.random() > 0.1]                                     # some decisions go missing
        result = A.account(rows, scheduled_frames=frames, segment="rand")
        for c in result["controllers"].values():
            assert sum(c["category_counts"].values()) == frames == c["total_scheduled_decision_opportunities"]
            assert c["decision_coverage_rate"]["denominator"] == frames
        f = result["frames"]
        assert f["valid_shared"] + f["shared_invalid"] + f["initialization"] + f["without_any_row"] == frames


# ------------------------------------------------------------------------------------------------ the historical proxy, labelled
def test_the_stored_below_grid_proportion_is_a_labelled_proxy_not_a_measured_abstention_rate():
    path = s.REPO / "Integration/system_integration/a0h_2026-11-21/results/forecast_boundary_analysis.json"
    assert s.sha(path) == "0cd131fd5cf75c39664839186c6bc12ec52c25e14612c09f66a8c2fa696f763a"
    proxy = A.historical_below_grid_proxy(path)
    assert (proxy["eligible_forecasts"], proxy["below_grid"], proxy["inside_grid"], proxy["above_grid"]) == (2150, 528, 1622, 0)
    assert proxy["below_grid_fraction"] == pytest.approx(0.2455813953, abs=1e-9) and proxy["is_measured_live_abstention_rate"] is False
    assert [(x["utc_day"], x["eligible"], x["below_grid"]) for x in proxy["segments"]] == [("2026-09-21", 720, 59), ("2026-10-12", 1430, 469)]
    assert proxy["kind"] == "historical_stored_forecast_below_grid_proxy" and any("model selection" in c for c in proxy["caveats"])


def test_the_accounting_has_no_weights_preferences_or_ranking_in_its_inputs_or_outputs():
    import inspect
    assert list(inspect.signature(A.account).parameters) == ["rows", "scheduled_frames", "segment", "exclusions", "controllers"]
    result = A.account(frame_rows(0) + frame_rows(1, predictive=HISTORY), scheduled_frames=2, segment="seg")
    assert set(result) == {"schema", "segment", "denominator", "exclusions", "exclusions_note", "frames", "controllers", "problems", "consistent", "interpretation"}
    assert set(result["controllers"]["predictive"]) == {
        "total_scheduled_decision_opportunities", "valid_shared_frames", "shared_invalid_frames", "initialization_frames", "controller_specific_abstentions",
        "valid_recommendations", "missing_decisions", "decision_coverage_rate", "coverage_rate_given_valid_shared_frame",
        "coverage_rate_excluding_declared_exclusions", "category_counts", "reason_breakdown", "timestamp_ranges"}
    assert not any(word in json.dumps(result).lower() for word in ("winner", "rank", "score", "preferred", "outperform"))
