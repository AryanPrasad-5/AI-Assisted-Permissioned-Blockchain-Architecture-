"""Task A0c (2026-11-21): the opt-in Stage 3 frame-quality contract and the bounded consecutive-abstention stop.

Closes, for an opted-in run only, the limitations L1 (client metrics not required), L2 (unknown state age) and L3 (no bound on
consecutive abstentions) that the A0b review left open. The defaults are NOT changed; the A0b characterization tests, which
describe the default path, still pass unmodified. Offline and in-process: a fake performance model and forecaster, no Fabric,
no file outside ``tmp_path``.
"""

import inspect
import json
from dataclasses import replace

import pytest

from fabric_perf.controllers import (
    FabricConfig,
    IntegratedControllerRunner,
    PredictiveController,
    ReactiveController,
    StaticController,
    TelemetryFrame,
)
from fabric_perf.controllers.base import ControllerState, PerformancePrediction
from fabric_perf.controllers.harness import ShadowActuator
from fabric_perf.controllers.live_integration import IntegrationSafetyError
from fabric_perf.controllers.stage3_frame_quality import STAGE3_CLIENT_FIELDS, Stage3FrameQualityContract

CONFIG = FabricConfig(max_message_count=45)
THREE = {"static", "reactive", "predictive"}
CONTINUOUS = {"recorder": "r.live.csv", "submits": "s.live.csv", "ledger": "l.csv"}


class FakeModel:
    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


class FakeForecaster:
    def predict(self, history, horizon):
        return 77.0                                                     # distinct from every observed arrival rate below


def controllers(consecutive=1):
    return {"static": StaticController(CONFIG),
            "reactive": ReactiveController(CONFIG, congested_config=replace(CONFIG, max_message_count=40), latency_high_ms=50,
                                           latency_low_ms=10, consecutive_samples=consecutive, cooldown_samples=0),
            "predictive": PredictiveController(FakeModel(), CONFIG, arrival_forecaster=FakeForecaster(),
                                               candidate_configs=[CONFIG, replace(CONFIG, max_message_count=40)])}


def frame(index, *, omit=None, age=0.5, freshness=None, critical=(), source="continuous", observed=None, **overrides):
    """A good Stage 3 frame by default: every client field present, a continuous client source, a known age."""
    stamp = observed or f"2026-11-21T12:00:{index:02d}Z"
    values = dict(observed_latency_ms=100.0, observed_latency_p95_ms=140.0, observed_throughput_tps=10.0,
                  observed_arrival_rate_tps=10.0, queue_depth=0.0)
    values.update(overrides)
    if omit:
        values[omit] = None
    state = ControllerState(timestamp_utc=stamp, current_config=CONFIG, telemetry_age_s=age,
                            arrival_history_tps=(10.0,), arrival_history_timestamps_utc=(stamp,), **values)
    provenance = {"continuous_caliper_source": CONTINUOUS} if source == "continuous" else (
        {"continuous_caliper_source": None, "caliper_summary": "round_summary.json"} if source == "summary" else {})
    return TelemetryFrame("r", index, stamp, 1000 + index, state, source_timestamps_utc={"caliper": stamp},
                          freshness_seconds={"caliper": 0.1} if freshness is None else freshness,
                          provenance=provenance, critical_missing=critical)


def contract(limit=3, **kwargs):
    return Stage3FrameQualityContract(max_consecutive_abstentions=limit, **kwargs)


def runner(limit=3, consecutive=1, **kwargs):
    kwargs.setdefault("invalid_frame_policy", "abstain")
    kwargs.setdefault("frame_quality", contract(limit))
    return IntegratedControllerRunner(controllers(consecutive), CONFIG, run_id="r", **kwargs)


def run(frames, limit=3, consecutive=1, **run_kwargs):
    return runner(limit, consecutive).run(frames, **run_kwargs)


def at(result, index):
    return [r for r in result["records"] if r["integration"]["decision_index"] == index]


def outcomes(result, index):
    return {r["decision"]["controller"]: r["decision"]["outcome"] for r in at(result, index)}


# ============================================================ the contract: fail closed without a threshold
def test_a_contract_cannot_be_constructed_without_a_threshold():
    with pytest.raises(TypeError):
        Stage3FrameQualityContract()
    parameter = inspect.signature(Stage3FrameQualityContract).parameters["max_consecutive_abstentions"]
    assert parameter.default is inspect.Parameter.empty                  # there is no default threshold to fall back on


@pytest.mark.parametrize("bad", [None, 0, -1, True, False, 2.5, "3", float("inf"), [3]])
def test_a_threshold_must_be_an_operator_supplied_positive_integer(bad):
    with pytest.raises(ValueError, match="max_consecutive_abstentions"):
        Stage3FrameQualityContract(max_consecutive_abstentions=bad)


@pytest.mark.parametrize("mapping, message", [
    ({}, "none was provided"),
    ({"max_consecutive_abstentions": None}, "none was provided"),
    ({"max_consecutive_abstentions": 0}, "positive integer"),
    ({"max_consecutive_abstentions": 3, "tolerance": 1}, "unknown Stage 3"),
    ("3", "must be a mapping"),
])
def test_configuration_from_a_mapping_is_fail_closed(mapping, message):
    with pytest.raises(ValueError, match=message):
        Stage3FrameQualityContract.from_mapping(mapping)


def test_a_valid_mapping_builds_the_same_contract_and_coerces_the_field_list():
    built = Stage3FrameQualityContract.from_mapping({"max_consecutive_abstentions": 4, "required_client_fields": ["queue_depth"]})
    assert built.max_consecutive_abstentions == 4 and built.required_client_fields == ("queue_depth",)


@pytest.mark.parametrize("fields, message", [((), "non-empty"), (("not_a_field",), "no such ControllerState field"),
                                             (("queue_depth", "queue_depth"), "duplicate"), ((3,), "non-empty")])
def test_required_fields_must_be_real_controller_state_fields(fields, message):
    with pytest.raises(ValueError, match=message):
        contract(required_client_fields=fields)


def test_the_default_required_fields_are_the_measured_client_fields_and_the_basis_is_stated():
    assert STAGE3_CLIENT_FIELDS == ("observed_arrival_rate_tps", "observed_throughput_tps", "observed_latency_ms",
                                    "observed_latency_p95_ms", "queue_depth")
    described = contract(7).as_dict()
    assert described["max_consecutive_abstentions"] == 7 and described["require_continuous_client_source"] is True
    assert "no scientific threshold is claimed" in described["max_consecutive_abstentions_basis"]


@pytest.mark.parametrize("flag", ["require_continuous_client_source", "require_known_state_age"])
def test_the_flags_must_be_booleans(flag):
    with pytest.raises(ValueError, match=flag):
        contract(**{flag: "yes"})


# ============================================================ the runner: opt-in, abstain-only, shadow-only
def test_the_contract_requires_the_abstain_policy_and_the_default_policy_is_refused():
    with pytest.raises(ValueError, match="requires invalid_frame_policy='abstain'"):
        IntegratedControllerRunner(controllers(), CONFIG, run_id="r", frame_quality=contract())          # default is "stop"
    with pytest.raises(ValueError, match="requires invalid_frame_policy='abstain'"):
        IntegratedControllerRunner(controllers(), CONFIG, run_id="r", frame_quality=contract(), invalid_frame_policy="stop")
    assert runner().invalid_frame_policy == "abstain"


def test_the_contract_is_shadow_only_and_must_be_a_contract():
    with pytest.raises(IntegrationSafetyError, match="shadow runs only"):
        IntegratedControllerRunner(controllers(), CONFIG, run_id="r", shadow=False, enable_live=True,
                                   invalid_frame_policy="abstain", frame_quality=contract())
    with pytest.raises(ValueError, match="Stage3FrameQualityContract"):
        IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain",
                                   frame_quality={"max_consecutive_abstentions": 3})


def test_without_a_contract_nothing_changes():
    plain = IntegratedControllerRunner(controllers(), CONFIG, run_id="r")
    assert plain.frame_quality is None and plain.invalid_frame_policy == "stop"
    assert inspect.signature(IntegratedControllerRunner.__init__).parameters["frame_quality"].default is None
    result = plain.run([frame(0)])
    assert result["frame_quality_contract"] is None and result["consecutive_abstention_limit_reached"] is False
    assert result["longest_consecutive_abstention_run"] == 0 and result["stop_reasons"] == []


# ============================================================ one disposition for every controller
def test_missing_client_metrics_make_all_three_controllers_abstain_where_the_default_lets_them_diverge():
    bad = frame(1, omit="observed_latency_ms", queue_depth=None)
    default = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain").run([bad])
    assert {r["decision"]["controller"]: r["decision"]["outcome"] for r in default["records"]} == {
        "static": "hold", "reactive": "abstain", "predictive": "hold"}                     # limitation L1 on the default path
    staged = run([bad])
    assert outcomes(staged, 1) == {name: "abstain" for name in THREE}                      # closed under the contract
    assert staged["abstained_frame_count"] == 1


def test_every_controller_gets_the_same_reason_the_same_frame_identity_and_its_own_identity():
    result = run([frame(0), frame(1, omit="observed_latency_ms"), frame(2)])
    at_bad = at(result, 1)
    assert {r["integration"]["strategy"] for r in at_bad} == THREE and len(at_bad) == 3
    reasons = {(r["decision"]["reason"], r["decision"]["metadata"]["telemetry_rejection"]["reason_code"],
                r["decision"]["metadata"]["telemetry_rejection"]["abstention_reason"]) for r in at_bad}
    assert reasons == {("telemetry_missing_fail_safe", "required_field_missing", "telemetry_missing_fail_safe")}
    assert {r["decision"]["metadata"]["telemetry_rejection"]["details"]["field"] for r in at_bad} == {"observed_latency_ms"}
    assert len({r["integration"]["input_digest"] for r in at_bad}) == 1                   # one frame identity for all three
    assert {r["integration"]["decision_index"] for r in at_bad} == {1}
    assert {r["integration"]["observation_rejected"]["reason_code"] for r in at_bad} == {"required_field_missing"}
    evidence = [v for v in result["validation_records"] if v["decision_index"] == 1][0]
    assert evidence["status"] == "rejected" and evidence["handled"] == "abstained" and evidence["reason_code"] == "required_field_missing"


@pytest.mark.parametrize("name", STAGE3_CLIENT_FIELDS)
def test_each_required_client_field_is_enforced_for_every_controller(name):
    result = run([frame(0), frame(1, omit=name)])
    assert outcomes(result, 1) == {c: "abstain" for c in THREE}
    assert {r["decision"]["metadata"]["telemetry_rejection"]["details"]["field"] for r in at(result, 1)} == {name}
    assert set(outcomes(result, 0).values()) != {"abstain"}                                 # the complete frame was not affected


def test_a_summary_only_client_source_is_not_contemporaneous_and_is_rejected_for_all():
    summary_frame = frame(1, source="summary")
    default = IntegratedControllerRunner(controllers(), CONFIG, run_id="r").run([summary_frame])
    assert set(r["decision"]["outcome"] for r in default["records"]) != {"abstain"}        # valid on the default path
    staged = run([frame(0), summary_frame])
    assert outcomes(staged, 1) == {c: "abstain" for c in THREE}
    rejection = at(staged, 1)[0]["decision"]["metadata"]["telemetry_rejection"]
    assert rejection["reason_code"] == "client_source_not_continuous" and rejection["details"] == {"caliper_summary_configured": True}


def test_no_client_source_at_all_is_rejected_for_all():
    staged = run([frame(0, source="none")])
    assert outcomes(staged, 0) == {c: "abstain" for c in THREE}
    assert at(staged, 0)[0]["decision"]["metadata"]["telemetry_rejection"]["details"] == {"caliper_summary_configured": False}


def test_an_unknown_state_age_is_rejected_for_all_where_the_default_lets_the_controllers_diverge():
    unknown = frame(1, age=None)
    default = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain").run([unknown])
    assert {r["decision"]["controller"]: r["decision"]["outcome"] for r in default["records"]} == {
        "static": "hold", "reactive": "abstain", "predictive": "abstain"}                  # limitation L2 on the default path
    staged = run([unknown])
    assert outcomes(staged, 1) == {c: "abstain" for c in THREE}
    assert at(staged, 1)[0]["decision"]["reason"] == "telemetry_freshness_unknown_fail_safe"
    assert at(staged, 1)[0]["decision"]["metadata"]["telemetry_rejection"]["reason_code"] == "state_age_unknown"


@pytest.mark.parametrize("bad, code", [
    (frame(1, freshness={"caliper": 9.0}), "stale_observation"),
    (frame(1, age=9.0), "stale_observation"),
    (frame(1, freshness={"caliper": -10.0}), "future_dated_observation"),
    (frame(1, freshness={"caliper": None}), "observation_timestamp_missing"),
    (frame(1, critical=("continuous_source_read_error:recorder:schema_error",)), "critical_telemetry_missing"),
], ids=["stale source", "stale state", "future source", "age unknown", "client read error"])
def test_stale_future_and_unreadable_client_telemetry_is_rejected_for_every_controller(bad, code):
    result = run([frame(0), bad])
    assert outcomes(result, 1) == {c: "abstain" for c in THREE}
    assert {r["decision"]["metadata"]["telemetry_rejection"]["reason_code"] for r in at(result, 1)} == {code}


def test_the_stricter_contract_does_not_reject_a_complete_continuous_frame():
    result = run([frame(0), frame(1)])
    assert result["abstained_frame_count"] == 0 and result["stop_reasons"] == [] and result["record_count"] == 6
    assert set(outcomes(result, 1).values()) != {"abstain"}


def test_mixed_validity_keeps_the_good_frames_identical_and_breaks_the_reactive_streak():
    clean = run([frame(0), frame(1)], consecutive=2)
    assert outcomes(clean, 1)["reactive"] == "propose"                                      # two high-latency frames in a row
    mixed = run([frame(0), frame(1, omit="queue_depth"), frame(2)], consecutive=2)
    assert outcomes(mixed, 1) == {c: "abstain" for c in THREE}
    assert outcomes(mixed, 2)["reactive"] != "propose"                                      # the abstention reset the streak
    assert mixed["identical_input_digests"] is True
    assert {len({r["integration"]["input_digest"] for r in at(mixed, i)}) for i in range(3)} == {1}


# ============================================================ bounded consecutive abstentions
def test_the_run_stops_at_the_configured_number_of_consecutive_abstentions():
    frames = [frame(0), frame(1, omit="queue_depth"), frame(2, omit="queue_depth"), frame(3, omit="queue_depth"), frame(4), frame(5)]
    result = run(frames, limit=3)
    assert result["stop_reasons"] == ["consecutive_abstention_limit_reached"]
    assert result["consecutive_abstention_limit_reached"] is True
    assert result["longest_consecutive_abstention_run"] == 3 and result["abstained_frame_count"] == 3
    assert result["record_count"] == 4 * 3                                                  # frames 0-3 only, all three controllers
    assert at(result, 4) == [] and at(result, 5) == []                                      # nothing is decided after the stop
    assert outcomes(result, 3) == {c: "abstain" for c in THREE}                             # the frame that hit the limit is recorded
    stop = result["validation_records"][-1]
    assert stop["status"] == "stopped" and stop["reasons"] == ["consecutive_abstention_limit_reached"]
    assert stop["consecutive_abstained_frames"] == 3 and stop["max_consecutive_abstentions"] == 3
    assert stop["last_rejection"]["reason_code"] == "required_field_missing" and len(stop["input_digest"]) == 64


def test_abstentions_that_are_not_consecutive_never_reach_the_limit():
    pattern = [frame(i, omit="queue_depth") if i % 3 != 2 else frame(i) for i in range(9)]   # bad, bad, good, ...
    result = run(pattern, limit=3)
    assert result["stop_reasons"] == [] and result["consecutive_abstention_limit_reached"] is False
    assert result["abstained_frame_count"] == 6 and result["longest_consecutive_abstention_run"] == 2
    assert result["record_count"] == 27


def test_a_limit_of_one_stops_at_the_first_unusable_frame_after_recording_it_for_every_controller():
    result = run([frame(0), frame(1, omit="queue_depth"), frame(2)], limit=1)
    assert result["stop_reasons"] == ["consecutive_abstention_limit_reached"]
    assert outcomes(result, 1) == {c: "abstain" for c in THREE} and at(result, 2) == []


def test_a_limit_larger_than_the_stream_never_triggers():
    result = run([frame(i, omit="queue_depth") for i in range(5)], limit=6)
    assert result["stop_reasons"] == [] and result["abstained_frame_count"] == 5 and result["longest_consecutive_abstention_run"] == 5


def test_the_limit_is_reached_exactly_not_exceeded():
    result = run([frame(i, omit="queue_depth") for i in range(5)], limit=2)
    assert result["abstained_frame_count"] == 2 and result["record_count"] == 6


def test_a_frame_that_does_not_advance_counts_as_an_abstention_towards_the_limit():
    result = run([frame(0), frame(1, observed="2026-11-21T12:00:00Z"), frame(2, observed="2026-11-21T12:00:00Z")], limit=2)
    codes = [v.get("reason_code") for v in result["validation_records"] if v["status"] == "rejected"]
    assert codes == ["frames_not_advancing", "frames_not_advancing"]
    assert result["stop_reasons"] == ["consecutive_abstention_limit_reached"]


def test_the_stop_is_written_to_the_streamed_evidence_files(tmp_path):
    frames = [frame(0), frame(1, omit="queue_depth"), frame(2, omit="queue_depth")]
    out = tmp_path / "out"
    result = runner(limit=2).run(frames, output_dir=out, retain_records=False)
    assert result["stop_reasons"] == ["consecutive_abstention_limit_reached"] and result["records"] == []
    manifest = json.loads((out / "integration_manifest.json").read_text(encoding="utf-8"))
    assert manifest["consecutive_abstention_limit_reached"] is True and manifest["longest_consecutive_abstention_run"] == 2
    assert manifest["frame_quality_contract"]["max_consecutive_abstentions"] == 2
    assert manifest["frame_quality_contract"]["required_client_fields"] == list(STAGE3_CLIENT_FIELDS)
    evidence = json.loads((out / "validation_evidence.json").read_text(encoding="utf-8"))
    assert evidence["stop_reasons"] == ["consecutive_abstention_limit_reached"]
    assert evidence["validation_records"][-1]["status"] == "stopped"
    lines = (out / "controller_decisions.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3 * 3 and {json.loads(l)["integration"]["strategy"] for l in lines} == THREE


# ============================================================ shadow safety: recommendation is not application
def test_a_rejected_frame_causes_no_actuator_call_and_no_reported_change(monkeypatch):
    offered = []
    original = ShadowActuator.apply

    def spy(self, config):
        offered.append(config)
        return original(self, config)
    monkeypatch.setattr(ShadowActuator, "apply", spy)
    run([frame(0)], consecutive=1)
    baseline = len(offered)
    assert baseline >= 1                                                                    # the good frame does propose: not vacuous
    offered.clear()
    result = run([frame(0), frame(1, omit="queue_depth"), frame(2, source="summary")], limit=5, consecutive=1)
    assert len(offered) == baseline                                                         # the two unusable frames offered nothing
    for index in (1, 2):
        for record in at(result, index):
            assert record["state"] is None and record["application"] is None
            assert record["configuration_state"]["proposal_applied"] is False
            assert record["configuration_state"]["after"]["max_message_count"] == 45
            assert record["decision"]["changed"] is False
    assert result["mode"] == "shadow" and result["live_submission_enabled"] is False


def test_a_recommendation_is_distinct_from_the_observed_and_the_applied_configuration():
    result = run([frame(0), frame(1)], consecutive=1)
    record = next(r for r in at(result, 1) if r["integration"]["strategy"] == "reactive")
    assert record["decision"]["outcome"] == "propose" and record["decision"]["config"]["max_message_count"] == 40   # recommended
    assert record["state"]["current_config"]["max_message_count"] == 45                                        # observed
    assert record["configuration_state"]["after"]["max_message_count"] == 45                                   # nothing applied
    assert record["configuration_state"]["proposal_applied"] is False
    assert record["application"]["error"] == "shadow_mode_no_submission" and record["application"]["accepted"] is False
    assert record["application"]["requested_config"]["max_message_count"] == 40
    assert record["application"]["verified_config"] is None


def test_the_forecast_workload_is_distinct_from_the_observed_workload():
    result = run([frame(0), frame(1)])
    record = next(r for r in at(result, 1) if r["integration"]["strategy"] == "predictive")
    assert record["state"]["observed_arrival_rate_tps"] == 10.0                                                # measured
    assert record["decision"]["predicted_arrival_rate_tps"] == 77.0                                            # forecast
    assert record["decision_trace"]["decision"]["predicted_arrival_rate_tps"] == 77.0


def test_every_arm_in_a_stage_3_run_uses_a_shadow_actuator():
    stage3 = runner()
    assert stage3.shadow is True and all(isinstance(a, ShadowActuator) for a in stage3.actuators.values())
    assert set(stage3.actuators) == THREE
