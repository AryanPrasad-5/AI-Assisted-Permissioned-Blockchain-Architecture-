"""A0t workstream D: the capacity-bracket design. The design contains no measurement of its own; the only measured numbers are the stored P1b pairs, which are cited and checked against the
stored A0s headroom results. No Fabric, no model, no simulation (the signed-rank bound is closed-form arithmetic)."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tests.a0s_support import REPO

from fabric_perf.controllers import capacity_bracket as cb      # noqa: E402
from fabric_perf.controllers import channel_selection as cs      # noqa: E402
from fabric_perf.controllers import config_inspection as ci      # noqa: E402

HEADROOM = REPO / "docs" / "research" / "e1" / "a0s_headroom_verification_2026-11-22" / "headroom_results.json"


@pytest.fixture(scope="module")
def pairs():
    return json.loads(HEADROOM.read_text(encoding="utf-8"))["measured_fabric_evidence"]["pairs"]


@pytest.fixture()
def design(pairs):
    return cb.build_design(measured_pairs=pairs)


# ---- 29: supported configuration-cell references
def test_29_the_two_cells_are_the_supported_p1b_cells_and_valid_expected_cells(design):
    base, alt = design["configurations"]["static_baseline"], design["configurations"]["alternative"]
    assert base["cell_id"] == "b10-t2000ms-p10485760-a103809024-o3" and alt["cell_id"] == "b40-t2000ms-p10485760-a103809024-o3"
    for cfg in (base, alt):
        cell = {k: cfg[k] for k in ci.EXPECTED_FIELDS}
        assert ci.cell_identifier(ci.expected_cell(cell)) == cfg["cell_id"]
    assert design["configurations"]["other_configurations"] == []
    source = (REPO / "src" / "fabric_perf" / "controllers" / "predictive_pipeline.py").read_text(encoding="utf-8")          # the controller menu is these two block sizes (static source check)
    assert "ApprovedMaxMessageCountSpace(frozenset({10, 40})" in source and "for size in (10, 40)" in source
    assert {10, 40} == {base["max_message_count"], alt["max_message_count"]}
    assert "B = 20" in design["configurations"]["other_configurations_reason"]


def test_29_the_held_constant_fields_equal_the_stored_p1b_settings(design):
    held = design["held_constant"]
    assert (held["batch_timeout_ms"], held["preferred_max_bytes"], held["absolute_max_bytes"], held["orderers"], held["workers"]) == (2000, 10485760, 103809024, 3, 1)
    for cfg in design["configurations"]["static_baseline"], design["configurations"]["alternative"]:
        assert (cfg["batch_timeout_ms"], cfg["preferred_max_bytes"], cfg["absolute_max_bytes"], cfg["orderers"]) == (2000, 10485760, 103809024, 3)


# ---- 30: baseline
def test_30_the_static_baseline_is_b10_because_it_is_the_measured_winner_at_every_tested_rate(design, pairs):
    assert design["configurations"]["static_baseline"]["max_message_count"] == 10 and design["configurations"]["alternative"]["max_message_count"] == 40
    assert all(p["p95_B10_ms"] < p["p95_B40_ms"] for p in pairs)
    cited = design["evidence"]["fabric_evidence"]["pairs_cited"]
    assert [(c["rate_tps"], c["p95_B10_ms"], c["p95_B40_ms"]) for c in cited] == [(p["rate"], p["p95_B10_ms"], p["p95_B40_ms"]) for p in pairs]


def test_30_a_design_with_a_weakened_or_swapped_baseline_is_invalid(design):
    swapped = copy.deepcopy(design)
    swapped["configurations"]["static_baseline"], swapped["configurations"]["alternative"] = design["configurations"]["alternative"], design["configurations"]["static_baseline"]
    assert "baseline" in {p["code"] for p in cb.validate_design(swapped)}
    extra = copy.deepcopy(design)
    extra["configurations"]["other_configurations"] = [{"max_message_count": 20}]
    assert "unsupported_configuration" in {p["code"] for p in cb.validate_design(extra)}
    changed = copy.deepcopy(design)
    changed["configurations"]["alternative"]["batch_timeout_ms"] = 1000
    assert "held_constant" in {p["code"] for p in cb.validate_design(changed)}


# ---- 31: workload-domain labelling
def test_31_model_support_and_fabric_evidence_are_kept_apart(design, pairs):
    ev = design["evidence"]
    assert ev["model_support"]["hfbtp_offered_rate_domain_tps"] == [10, 200] and "not evidence" in ev["model_support"]["use"]
    assert ev["fabric_evidence"]["rates_tested_tps"] == [p["rate"] for p in pairs] and ev["fabric_evidence"]["highest_measured_rate_tps"] == 40
    assert ev["fabric_evidence"]["capacity_located"] is False and ev["fabric_evidence"]["noise_floor"] == "NOT MEASURED"
    assert "HYPOTHESIS" in ev["mechanistic_hypothesis"] and ev["crossover_support_in_existing_data"].startswith("NONE")
    bad = copy.deepcopy(design)
    bad["evidence"]["model_support"]["use"] = "extrapolate to the testbed"
    assert "model_as_evidence" in {p["code"] for p in cb.validate_design(bad)}
    bad = copy.deepcopy(design)
    bad["evidence"]["fabric_evidence"]["pairs_cited"].append({"rate_tps": 60, "p95_B10_ms": 1, "p95_B40_ms": 2})
    assert "evidence_above_measured_range" in {p["code"] for p in cb.validate_design(bad)}
    del bad["evidence"]["model_support"]
    assert "evidence_labels" in {p["code"] for p in cb.validate_design(bad)}


# ---- 32: no invented measured results
def test_32_the_design_contains_no_measurement_of_its_own(design, pairs):
    assert design["measured_results"] is None and design["approved"] is False and design["status"] == "PROPOSED_NOT_APPROVED"
    cited = {(c["rate_tps"], c["p95_B10_ms"], c["p95_B40_ms"]) for c in design["evidence"]["fabric_evidence"]["pairs_cited"]}
    assert cited == {(p["rate"], p["p95_B10_ms"], p["p95_B40_ms"]) for p in pairs}
    text = json.dumps({k: v for k, v in design.items() if k != "evidence"})
    for word in ("throughput_mean", "p95_measured", "capacity_tps", "saturation_tps", "noise_floor_ms"):
        assert word not in text
    assert design["replicates"]["power_calculation"].startswith("NONE")
    bad = copy.deepcopy(design)
    bad["measured_results"] = {"p95": 1}
    assert "measured_results_present" in {p["code"] for p in cb.validate_design(bad)}
    bad = copy.deepcopy(design)
    bad["replicates"]["power_calculation"] = "n = 12 gives 80% power"
    assert "invented_power" in {p["code"] for p in cb.validate_design(bad)}


# ---- 33: no unapproved upper TPS
def test_33_no_maximum_rate_or_threshold_is_set_by_the_design(design):
    assert design["offered_load"]["max_offered_rate_tps"] is None and "owner sets the ceiling" in design["offered_load"]["max_rate_note"]
    assert design["offered_load"]["rung_status"].startswith("PROPOSED_ONLY") and "none is claimed safe" in design["offered_load"]["rung_status"]
    assert design["analysis"]["practical_significance"]["latency_ms"] is None and design["analysis"]["practical_significance"]["throughput_tps"] is None
    for key in ("warmup_s", "measurement_window_s", "cooldown_s"):
        assert design["timing"][key] is None
    assert design["schedule"]["seed"] is None and design["schedule"]["channel_family"] is None and design["replicates"]["maximum_per_cell"] is None
    bad = copy.deepcopy(design)
    bad["offered_load"]["max_offered_rate_tps"] = 100
    assert "invented_ceiling" in {p["code"] for p in cb.validate_design(bad)}
    bad = copy.deepcopy(design)
    bad["analysis"]["practical_significance"]["latency_ms"] = 62.5
    assert "invented_threshold" in {p["code"] for p in cb.validate_design(bad)}
    assert min(design["offered_load"]["proposed_escalation_rungs_tps"]) > design["evidence"]["fabric_evidence"]["highest_measured_rate_tps"]


def test_33_rungs_must_ascend_above_the_last_measured_rate(design):
    for rungs in ([], [40, 50], [60, 50], [30, 50]):
        bad = copy.deepcopy(design)
        bad["offered_load"]["proposed_escalation_rungs_tps"] = rungs
        assert "rungs" in {p["code"] for p in cb.validate_design(bad)}
    bad = copy.deepcopy(design)
    bad["offered_load"]["anchor_rate_tps"] = 50
    assert "anchor" in {p["code"] for p in cb.validate_design(bad)}


# ---- 34: replicate plan completeness
def test_34_the_replicate_plan_is_complete_and_the_rank_test_bound_is_closed_form(design):
    r = design["replicates"]
    assert r["initial_per_cell"] == 3 and r["maximum_per_cell"] is None and "never based on the sign" in r["extension_rule"]
    assert cb.minimum_exact_signed_rank_p(3) == 0.25 and cb.minimum_exact_signed_rank_p(5) == 0.0625 and cb.minimum_exact_signed_rank_p(6) == 0.03125
    assert cb.smallest_n_for_rank_p_below(0.05) == 6 and r["n_for_rank_p_below_0.05"] == 6
    for bad in (0, -1, True, 2.5):
        with pytest.raises(cb.DesignInvalid):
            cb.minimum_exact_signed_rank_p(bad)
    thin = copy.deepcopy(design)
    thin["replicates"]["initial_per_cell"] = 2
    assert "replicates" in {p["code"] for p in cb.validate_design(thin)}


def test_34_the_schedule_is_complete_balanced_randomised_and_reproducible(design):
    sched = cb.build_schedule(design, seed=20261122, rungs_tps=[40, 50, 60], replicates=3, channel_family="cbtest")           # ILLUSTRATIVE seed
    runs = sched["runs"]
    assert sched["status"] == "ILLUSTRATIVE_NOT_AUTHORIZED" and sched["run_count"] == len(runs) == (3 * 3 + 1) * 2
    assert [r["sequence"] for r in runs] == list(range(1, len(runs) + 1))
    for rate in (40, 50, 60):
        for rep in (1, 2, 3):
            block = [r for r in runs if r["rate_tps"] == rate and r["replicate"] == rep and r["kind"] != "drift_recheck"]
            assert sorted(r["block"] for r in block) == [10, 40] and sorted(r["position_in_block"] for r in block) == [1, 2]
    drift = [r for r in runs if r["kind"] == "drift_recheck"]
    assert len(drift) == 2 and {r["rate_tps"] for r in drift} == {40} and runs[-2:] == drift
    rates = [r["rate_tps"] for r in runs if r["kind"] != "drift_recheck"]
    assert rates == sorted(rates), "ascending across rungs: sequential escalation"
    assert sched == cb.build_schedule(design, seed=20261122, rungs_tps=[40, 50, 60], replicates=3, channel_family="cbtest")
    orders = {tuple(r["block"] for r in runs if r["rate_tps"] == 50 and r["replicate"] == k) for k in (1, 2, 3)}
    other = cb.build_schedule(design, seed=7, rungs_tps=[40, 50, 60], replicates=3, channel_family="cbtest")
    assert other != sched or len(orders) > 1
    assert len({r["channel"] for r in runs}) == len(runs)
    for r in runs:
        assert cs.select_experiment_channel(r["channel"]) == r["channel"] and r["cell_id"] in (design["configurations"]["static_baseline"]["cell_id"], design["configurations"]["alternative"]["cell_id"])


def test_34_the_order_within_blocks_varies_with_the_seed_across_many_seeds(design):
    seen = set()
    for seed in range(40):
        sched = cb.build_schedule(design, seed=seed, rungs_tps=[40, 50], replicates=3, channel_family="cbtest")
        seen.add(tuple(r["block"] for r in sched["runs"] if r["rate_tps"] == 50))
    assert len(seen) > 1


@pytest.mark.parametrize("kwargs", [dict(seed=None), dict(seed=True), dict(seed="1"), dict(replicates=2), dict(replicates=True), dict(rungs_tps=[50, 60]), dict(rungs_tps=[40, 60, 50]),
                                    dict(rungs_tps=[40, 40]), dict(rungs_tps=[]), dict(channel_family=""), dict(channel_family="Bad_Name"), dict(channel_family="ql01"),
                                    dict(channel_family="cp01"), dict(channel_family="rp01")])
def test_34_the_schedule_builder_has_no_defaults_and_refuses_bad_input(design, kwargs):
    args = dict(seed=1, rungs_tps=[40, 50], replicates=3, channel_family="cbtest")
    args.update(kwargs)
    with pytest.raises((cb.DesignInvalid, TypeError)):
        cb.build_schedule(design, **args)


def test_34_the_schedule_builder_requires_every_argument(design):
    with pytest.raises(TypeError):
        cb.build_schedule(design)


# ---- 35: stop conditions
def test_35_every_stop_rule_is_present_with_text_and_the_ones_that_need_owner_numbers_are_listed(design):
    ids = [r["id"] for r in design["stop_rules"]]
    assert ids == [f"S{i:02d}" for i in range(1, 11)] and all(r["text"] and r["name"] for r in design["stop_rules"])
    names = {r["name"] for r in design["stop_rules"]}
    assert {"run_class_not_healthy", "failure_rate_above_bound", "delivery_fidelity_below_bound", "resource_floor", "latency_ceiling", "configuration_or_clock_gate", "ceiling_reached",
            "interrupt_or_evidence_failure", "no_automatic_continuation", "owner_review_interval"} == names
    for needed in ("failure_rate_bound", "delivery_fidelity_floor", "latency_ceiling_ms", "resource_floor_wsl_mem_available_mb", "max_offered_rate_tps"):
        assert needed in design["required_owner_parameters"]
    assert set(cb.REQUIRED_OWNER_PARAMETERS) <= set(design["required_owner_parameters"])
    bad = copy.deepcopy(design)
    bad["stop_rules"] = bad["stop_rules"][:-1]
    assert "stop_rules" in {p["code"] for p in cb.validate_design(bad)}
    bad = copy.deepcopy(design)
    bad["required_owner_parameters"].remove("max_offered_rate_tps")
    assert "owner_parameter_missing" in {p["code"] for p in cb.validate_design(bad)}


def test_35_the_stage_gate_covers_all_four_outcomes_without_building_actuation(design):
    gate = design["stage_gate"]
    assert set(gate) == {"B10_consistently_best", "reproducible_crossover", "uncertainty_too_large", "rates_not_sustained"}
    assert "build nothing" in gate["uncertainty_too_large"] and "without extrapolation" in gate["rates_not_sustained"]
    assert "actuation" in gate["reproducible_crossover"] and "widen the menu" in gate["B10_consistently_best"]


# ---- 36: model support versus Fabric evidence, and the analysis plan
def test_36_the_analysis_plan_names_the_required_statistics_without_inventing_numbers(design):
    a = design["analysis"]
    for key in ("per_configuration_uncertainty", "between_run_variability", "paired_or_blocked", "effect_size", "practical_significance", "multiple_comparisons", "workload_delivery_fidelity",
                "failed_transactions", "saturated_runs", "decision_relevant_crossover"):
        assert a[key]
    assert "Holm" in a["multiple_comparisons"] and "successful transactions only" in a["failed_transactions"] and "capacity boundary" in a["saturated_runs"]
    assert "n >= 6" in a["effect_size"]
    primary = [m["name"] for m in design["metrics"]["primary"]]
    assert primary == ["client_p95_latency_ms", "ledger_committed_throughput_tps"]
    assert "never Caliper's summary table" in design["metrics"]["primary"][0]["source"] and "client completion is not ledger commitment" in design["metrics"]["primary"][1]["source"]
    assert design["schedule"]["reset_between_runs"].startswith("a fresh dedicated channel per run") and "no actuator" in design["schedule"]["reset_between_runs"]
    assert "ONE run" in design["schedule"]["series_authorization_note"]


def test_a_valid_design_validates_and_serialises_deterministically(design, pairs):
    assert cb.validate_design(design) == []
    assert cb.design_json(design) == cb.design_json(cb.build_design(measured_pairs=pairs))
    assert json.loads(cb.design_json(design)) == json.loads(json.dumps(design))
    assert design["claims_not_made"] and any("safe maximum" in c for c in design["claims_not_made"])
