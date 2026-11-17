"""Live-shadow fix 2 (2026-11-17): explicit freshness policy, no stale or refreshed observations.

The limit is five seconds and an age exactly equal to it is fresh. Every non-fresh observation is
classified FRESH / STALE / MISSING / FUTURE_DATED / INCONSISTENT, keeps its original timestamp, and
reaches the controllers as an abstention with a stated reason. Nothing here touches Fabric.
"""

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from fabric_perf.controllers import (
    FabricConfig,
    FreshnessPolicy,
    FreshnessStatus,
    IntegratedControllerRunner,
    PredictiveController,
    ProductionTelemetryProvider,
    PrometheusTelemetryAdapter,
    ReactiveController,
    StaticController,
    TelemetryDataError,
    TelemetryFrame,
    TelemetryQualityError,
    TelemetryTimestampError,
    caliper_snapshot_from_summary,
)
from fabric_perf.controllers.base import ControllerState, PerformancePrediction
from fabric_perf.controllers.live_integration import IntegrationSafetyError
from fabric_perf.controllers.telemetry import TelemetryTimestampConflict
from fabric_perf.controllers.telemetry_freshness import (
    ClockConsistencyMonitor,
    assess_sources,
    classify_observation,
)

T0 = datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc)
CHANNEL = "ch"
PROM = f'consensus_etcdraft_is_leader{{channel="{CHANNEL}"}} 1\n'
CONFIG = FabricConfig(max_message_count=45)


def iso(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds")


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


# ---------------------------------------------------------------- policy object
def test_default_limit_is_the_unchanged_five_seconds_everywhere():
    assert FreshnessPolicy().max_age_s == 5.0
    adapter = PrometheusTelemetryAdapter({"o": "http://o/metrics"}, fetch=lambda *_: PROM)
    assert ProductionTelemetryProvider(adapter, CHANNEL).freshness_policy.max_age_s == 5.0
    runner = IntegratedControllerRunner({"static": StaticController(CONFIG)}, CONFIG, run_id="r")
    assert runner.freshness_policy.max_age_s == 5.0
    assert ReactiveController(CONFIG, latency_high_ms=100).max_telemetry_age_s == 5.0
    assert PredictiveController(None, CONFIG, arrival_forecaster=object(),
                                candidate_configs=[CONFIG]).max_telemetry_age_s == 5.0


@pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), True, "5", None, 30.0001, 3600])
def test_policy_refuses_invalid_or_silently_widened_limits(bad):
    with pytest.raises(ValueError):
        FreshnessPolicy(max_age_s=bad)


def test_policy_is_configurable_only_through_validation():
    policy = FreshnessPolicy.from_mapping({"max_age_s": 2, "max_future_skew_s": 0.25})
    assert (policy.max_age_s, policy.max_future_skew_s) == (2.0, 0.25)
    with pytest.raises(ValueError, match="unknown"):
        FreshnessPolicy.from_mapping({"max_age_s": 5, "max_age_seconds": 99})
    with pytest.raises(ValueError):
        FreshnessPolicy(max_future_skew_s=1.5)
    with pytest.raises(ValueError):
        FreshnessPolicy(max_clock_discrepancy_s=0)
    with pytest.raises(ValueError, match="FreshnessPolicy"):
        ProductionTelemetryProvider(PrometheusTelemetryAdapter({"o": "http://o"}, fetch=lambda *_: PROM), CHANNEL,
                                    freshness_policy={"max_age_s": 5})
    with pytest.raises(ValueError, match="conflicts"):
        ProductionTelemetryProvider(PrometheusTelemetryAdapter({"o": "http://o"}, fetch=lambda *_: PROM), CHANNEL,
                                    freshness_policy=FreshnessPolicy(max_source_skew_s=4), max_source_skew_s=3)


# ------------------------------------------------------------- classification
@pytest.mark.parametrize("age, status", [(0.0, "FRESH"), (4.999, "FRESH"), (5.0, "FRESH"),
                                         (5.001, "STALE"), (60, "STALE")])
def test_boundary_just_inside_exactly_at_and_just_outside(age, status):
    verdict = classify_observation("caliper", iso(at(-age)), iso(T0), FreshnessPolicy())
    assert verdict.status.value == status
    assert verdict.age_s == pytest.approx(age)
    assert (verdict.abstention_reason is None) == (status == "FRESH")


def test_custom_limit_moves_the_boundary_exactly():
    policy = FreshnessPolicy(max_age_s=2)
    assert classify_observation("s", iso(at(-2)), iso(T0), policy).status is FreshnessStatus.FRESH
    assert classify_observation("s", iso(at(-2.001)), iso(T0), policy).status is FreshnessStatus.STALE


@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_timestamp_is_its_own_class(value):
    verdict = classify_observation("ledger", value, iso(T0), FreshnessPolicy())
    assert verdict.status is FreshnessStatus.MISSING and verdict.age_s is None
    assert verdict.abstention_reason == "telemetry_freshness_unknown_fail_safe"
    assert verdict.frame_code == "observation_timestamp_missing:ledger"


def test_future_dated_is_not_clamped_to_fresh():
    verdict = classify_observation("caliper", iso(at(0.001)), iso(T0), FreshnessPolicy())
    assert verdict.status is FreshnessStatus.FUTURE_DATED and verdict.age_s == pytest.approx(-0.001)
    assert verdict.abstention_reason == "future_dated_telemetry_fail_safe"
    tolerant = FreshnessPolicy(max_future_skew_s=0.5)
    assert classify_observation("s", iso(at(0.4)), iso(T0), tolerant).status is FreshnessStatus.FRESH
    assert classify_observation("s", iso(at(0.6)), iso(T0), tolerant).status is FreshnessStatus.FUTURE_DATED


@pytest.mark.parametrize("value, detail", [("yesterday", "unparseable_timestamp"),
                                           ("2026-11-17T12:00:00", "timezone_missing")])
def test_unparseable_or_naive_timestamps_are_inconsistent_not_fresh(value, detail):
    verdict = classify_observation("caliper", value, iso(T0), FreshnessPolicy())
    assert verdict.status is FreshnessStatus.INCONSISTENT and verdict.detail == detail
    assert verdict.abstention_reason == "telemetry_timestamp_inconsistent_fail_safe"


def test_source_moving_backwards_is_inconsistent_but_repeating_is_not():
    policy = FreshnessPolicy()
    seen = at(-1)
    assert classify_observation("caliper", iso(at(-2)), iso(T0), policy, previous=seen).detail == "out_of_order_source"
    assert classify_observation("caliper", iso(at(-1)), iso(T0), policy, previous=seen).status is FreshnessStatus.FRESH
    assert classify_observation("caliper", iso(at(-0.5)), iso(T0), policy, previous=seen).status is FreshnessStatus.FRESH


def test_frame_assessment_reports_the_most_serious_class_first():
    result = assess_sources({"a": iso(at(-60)), "b": iso(at(10)), "c": None, "d": "yesterday", "e": iso(at(-1))},
                            iso(T0), FreshnessPolicy())
    assert result.worst is FreshnessStatus.INCONSISTENT
    assert {v.source: v.status.value for v in result.verdicts.values()} == {
        "a": "STALE", "b": "FUTURE_DATED", "c": "MISSING", "d": "INCONSISTENT", "e": "FRESH"}
    compact = result.compact_dict()
    assert compact["sources_fresh"] == 1 and set(compact["non_fresh"]) == {"a", "b", "c", "d"}


def test_clock_monitor_reports_one_step_then_recovers():
    monitor = ClockConsistencyMonitor(1.0)
    assert monitor.check(at(0), 0) is None
    assert monitor.check(at(1), 1_000_000_000) is None
    assert monitor.check(at(1.5), 2_000_000_000) is None            # 0.5 s of drift is inside the 1 s limit
    assert monitor.check(at(-29), 3_000_000_000) == pytest.approx(31.5)   # wall clock stepped back 30 s
    assert monitor.check(at(-28), 4_000_000_000) is None            # judged normally again


# ------------------------------------------------------------- summary route
def write_summary(tmp_path, round_end: datetime, name="summary", with_manifest=True):
    manifest = tmp_path / f"{name}.manifest.json"
    manifest.write_text(json.dumps({"clock": {"wall1_ms": int(round_end.timestamp() * 1000)}}), encoding="utf-8")
    summary = tmp_path / f"{name}.json"
    summary.write_text(json.dumps({
        "metrics": {"offered_tps_from_submit_events": 10.0, "latency_ms_mean": 1069.0,
                    "latency_ms_p95_nearest_rank": 1937.0, "max_inflight_backlog": 21, "round_wall_elapsed_s": 10.4},
        "counts": {"valid_workload_ledger_transactions": 101, "failed": 0, "submitted": 101},
        "inputs": {"manifest": {"path": str(manifest)}} if with_manifest else {},
        "analysis_verdict": "VALID"}), encoding="utf-8")
    return summary, manifest


def make_provider(summary, **kwargs):
    adapter = PrometheusTelemetryAdapter({"orderer1": "http://orderer1/metrics"}, fetch=lambda *_: PROM)
    return ProductionTelemetryProvider(adapter, CHANNEL, config_reader=lambda: CONFIG, caliper_summary_path=summary,
                                       required_fields=(), **kwargs)


@pytest.mark.parametrize("age, accepted", [(4.999, True), (5.0, True), (5.001, False)])
def test_summary_frame_boundary_is_exact(tmp_path, age, accepted):
    round_end = at(0)
    summary, _ = write_summary(tmp_path, round_end)
    frame = make_provider(summary).collect(run_id="r", decision_index=0, observation_timestamp_utc=iso(round_end + timedelta(seconds=age)))
    if accepted:
        frame.validate()
    else:
        with pytest.raises(TelemetryQualityError) as caught:
            frame.validate()
        assert caught.value.abstention_reason == "stale_telemetry_fail_safe"
        assert "stale" in str(caught.value)


def test_a_stale_summary_is_reported_as_stale_not_as_skew(tmp_path):
    summary, _ = write_summary(tmp_path, at(0))
    frame = make_provider(summary).collect(run_id="r", decision_index=0, observation_timestamp_utc=iso(at(600)))
    assert frame.inconsistencies == ()                                  # stale sources do not also trip the skew check
    assessment = frame.provenance["freshness_assessment"]
    assert assessment["worst"] == "STALE" and "stale_observation:caliper" in assessment["reason_codes"]
    with pytest.raises(TelemetryQualityError) as caught:
        frame.validate()
    assert caught.value.reason_code == "stale_observation" and caught.value.abstention_reason == "stale_telemetry_fail_safe"


def test_rereading_a_summary_preserves_its_original_timestamp(tmp_path):
    round_end = at(0)
    summary, _ = write_summary(tmp_path, round_end)
    provider = make_provider(summary)
    first = provider.collect(run_id="r", decision_index=0, observation_timestamp_utc=iso(at(1)))
    later = provider.collect(run_id="r", decision_index=1, observation_timestamp_utc=iso(at(30)))
    assert first.source_timestamps_utc["caliper"] == later.source_timestamps_utc["caliper"] == iso(round_end)
    assert first.freshness_seconds["caliper"] == pytest.approx(1.0)
    assert later.freshness_seconds["caliper"] == pytest.approx(30.0)    # older, never refreshed
    first.validate()
    with pytest.raises(TelemetryQualityError):
        later.validate()


def test_explicit_timestamp_cannot_refresh_a_summary(tmp_path):
    round_end = at(0)
    summary, _ = write_summary(tmp_path, round_end)
    assert caliper_snapshot_from_summary(summary, timestamp_utc=iso(round_end)).timestamp_utc == iso(round_end)
    with pytest.raises(TelemetryTimestampConflict, match="contradicts"):
        caliper_snapshot_from_summary(summary, timestamp_utc=iso(at(30)))      # "now" would refresh it
    snapshot = caliper_snapshot_from_summary(summary)
    assert snapshot.health["observation_timestamp_source"] == "recorder_manifest_wall1_ms"
    undated, _ = write_summary(tmp_path, round_end, name="undated", with_manifest=False)
    with pytest.raises(TelemetryTimestampError, match="needs an explicit observation timestamp"):
        caliper_snapshot_from_summary(undated)
    assert caliper_snapshot_from_summary(undated, timestamp_utc=iso(round_end)).health[
        "observation_timestamp_source"] == "explicit_unverified"
    assert issubclass(TelemetryTimestampError, TelemetryDataError)


def test_missing_timestamp_source_becomes_a_read_failure_with_a_reason(tmp_path):
    summary, _ = write_summary(tmp_path, at(0), with_manifest=False)
    frame = make_provider(summary).collect(run_id="r", decision_index=0, observation_timestamp_utc=iso(at(1)))
    assert any(code.startswith("caliper_read_error:TelemetryTimestampError") for code in frame.critical_missing)
    detail = frame.provenance["read_failures"]["items"][0]
    assert detail["source"] == "caliper_summary" and detail["category"] == "missing_timestamp"
    with pytest.raises(TelemetryQualityError) as caught:
        frame.validate()
    assert caught.value.abstention_reason == "telemetry_read_failed_fail_safe"


def test_undated_ledger_observation_is_excluded_not_reused(tmp_path):
    adapter = PrometheusTelemetryAdapter({"o": "http://o/metrics"}, fetch=lambda *_: PROM)
    provider = ProductionTelemetryProvider(adapter, CHANNEL, config_reader=lambda: CONFIG, required_fields=(),
                                           ledger_reader=lambda: {"block_transaction_count": 21, "timestamp_utc": None})
    frame = provider.collect(run_id="r", decision_index=0, observation_timestamp_utc=iso(at(1)))
    assert frame.provenance["ledger_observation_excluded"] is True
    assert "ledger" not in frame.source_timestamps_utc
    assert frame.provenance["metrics"]["block_transaction_count"]["value"] is None
    assert frame.provenance["read_failures"]["items"][0]["category"] == "missing_timestamp"
    frame.validate()                                                    # advisory input: excluded, not blocking


def test_future_dated_summary_gives_a_frame_not_a_crash(tmp_path):
    summary, _ = write_summary(tmp_path, at(3))                         # round "ended" 3 s after the observation
    frame = make_provider(summary).collect(run_id="r", decision_index=0, observation_timestamp_utc=iso(at(0)))
    assert frame.freshness_seconds["caliper"] == pytest.approx(-3.0)    # not clamped to zero
    assert frame.state.telemetry_age_s == 0.0                           # clamped only inside the state
    with pytest.raises(TelemetryQualityError) as caught:
        frame.validate()
    assert caught.value.abstention_reason == "future_dated_telemetry_fail_safe"
    assert "future-dated" in str(caught.value)


def test_out_of_order_source_is_inconsistent_then_recovers(tmp_path):
    summary, manifest = write_summary(tmp_path, at(8))
    provider = make_provider(summary)
    first = provider.collect(run_id="r", decision_index=0, observation_timestamp_utc=iso(at(10)))
    first.validate()
    manifest.write_text(json.dumps({"clock": {"wall1_ms": int(at(6).timestamp() * 1000)}}), encoding="utf-8")
    backwards = provider.collect(run_id="r", decision_index=1, observation_timestamp_utc=iso(at(11)))
    assert any(code.startswith("source_timestamp_out_of_order:caliper") for code in backwards.inconsistencies)
    with pytest.raises(TelemetryQualityError) as caught:
        backwards.validate()
    assert caught.value.abstention_reason == "telemetry_timestamp_inconsistent_fail_safe"
    manifest.write_text(json.dumps({"clock": {"wall1_ms": int(at(11).timestamp() * 1000)}}), encoding="utf-8")
    recovered = provider.collect(run_id="r", decision_index=2, observation_timestamp_utc=iso(at(12)))
    assert recovered.inconsistencies == ()
    recovered.validate()


class ScriptedClock:
    def __init__(self):
        self.wall, self.mono = iso(T0), 0

    def set(self, wall: datetime, mono_s: float):
        self.wall, self.mono = iso(wall), int(mono_s * 1e9)

    def utc_now(self):
        return self.wall

    def monotonic_ns(self):
        return self.mono


def test_wall_clock_step_is_detected_in_live_mode_then_recovers():
    clock = ScriptedClock()
    adapter = PrometheusTelemetryAdapter({"o": "http://o/metrics"}, fetch=lambda *_: PROM)
    provider = ProductionTelemetryProvider(adapter, CHANNEL, config_reader=lambda: CONFIG, required_fields=(), clock=clock)
    outcomes = []
    for wall, mono in ((at(0), 0), (at(1), 1), (at(-29), 2), (at(-28), 3)):
        clock.set(wall, mono)
        frame = provider.collect(run_id="r", decision_index=len(outcomes))
        outcomes.append(frame)
    assert [f.inconsistencies for f in outcomes] == [(), (), ("wall_clock_step_detected",), ()]
    assert outcomes[2].provenance["clock_consistency"]["discrepancy_s"] == pytest.approx(31.0)
    with pytest.raises(TelemetryQualityError) as caught:
        outcomes[2].validate()
    assert caught.value.abstention_reason == "telemetry_timestamp_inconsistent_fail_safe"
    outcomes[3].validate()                                              # recovery: the next frame is judged normally


def test_supplied_timestamps_skip_the_wall_vs_monotonic_check():
    clock = ScriptedClock()
    adapter = PrometheusTelemetryAdapter({"o": "http://o/metrics"}, fetch=lambda *_: PROM)
    provider = ProductionTelemetryProvider(adapter, CHANNEL, config_reader=lambda: CONFIG, required_fields=(), clock=clock)
    for index, seconds in enumerate((0, 100, -50)):
        frame = provider.collect(run_id="r", decision_index=index, observation_timestamp_utc=iso(at(seconds)))
        assert frame.provenance["clock_consistency"]["checked_against_monotonic_clock"] is False
        assert frame.inconsistencies == ()


def test_fresh_data_resuming_after_stale_data_is_accepted(tmp_path):
    summary, manifest = write_summary(tmp_path, at(0))
    provider = make_provider(summary)
    states = []
    for index, (observed, round_end) in enumerate(((at(2), at(0)), (at(40), at(0)), (at(41), at(39.5)))):
        manifest.write_text(json.dumps({"clock": {"wall1_ms": int(round_end.timestamp() * 1000)}}), encoding="utf-8")
        frame = provider.collect(run_id="r", decision_index=index, observation_timestamp_utc=iso(observed))
        try:
            frame.validate()
            states.append("accepted")
        except TelemetryQualityError as exc:
            states.append(exc.abstention_reason)
    assert states == ["accepted", "stale_telemetry_fail_safe", "accepted"]


# --------------------------------------------------- abstention reaches controllers
class FakeModel:
    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


class FakeForecaster:
    def predict(self, history, horizon):
        return 10.0


def controllers():
    return {"static": StaticController(CONFIG),
            "reactive": ReactiveController(CONFIG, congested_config=replace(CONFIG, max_message_count=40),
                                           latency_high_ms=1500, latency_low_ms=1200),
            "predictive": PredictiveController(FakeModel(), CONFIG, arrival_forecaster=FakeForecaster(),
                                               candidate_configs=[CONFIG, replace(CONFIG, max_message_count=40)])}


def frame(index, *, source_age=0.1):
    stamp = f"2026-11-17T12:00:{index:02d}Z"
    state = ControllerState(timestamp_utc=stamp, current_config=CONFIG, observed_latency_ms=100.0,
                            observed_throughput_tps=10.0, observed_arrival_rate_tps=10.0, queue_depth=0.0,
                            telemetry_age_s=0.5, arrival_history_tps=(10.0,), arrival_history_timestamps_utc=(stamp,))
    source_time = iso(datetime.fromisoformat(stamp.replace("Z", "+00:00")) - timedelta(seconds=source_age))
    return TelemetryFrame(run_id="r", decision_index=index, observation_timestamp_utc=stamp,
                          observation_monotonic_ns=1000 + index, state=state,
                          source_timestamps_utc={"caliper": source_time}, freshness_seconds={"caliper": source_age})


def test_default_policy_still_stops_the_run_and_records_the_structured_reason():
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r")
    assert runner.invalid_frame_policy == "stop"
    result = runner.run([frame(0), frame(1, source_age=6.0), frame(2)])
    assert result["record_count"] == 3 and result["stop_reasons"] == ["telemetry_validation_failed"]
    rejected = result["validation_records"][0]
    assert rejected["handled"] == "stopped" and rejected["reason_code"] == "stale_observation"
    assert rejected["abstention_reason"] == "stale_telemetry_fail_safe"


def test_abstain_policy_gives_every_controller_an_explicit_reason_and_continues():
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    result = runner.run([frame(0), frame(1, source_age=6.0), frame(2)])
    assert result["stop_reasons"] == [] and result["abstained_frame_count"] == 1
    assert result["frame_count"] == 3 and result["record_count"] == 9
    middle = [r for r in result["records"] if r["integration"]["decision_index"] == 1]
    assert len(middle) == 3
    for record in middle:
        assert record["decision"]["reason"] == "stale_telemetry_fail_safe" and record["decision"]["outcome"] == "abstain"
        assert record["state"] is None and record["application"] is None
        assert record["configuration_state"]["proposal_applied"] is False
        rejection = record["decision"]["metadata"]["telemetry_rejection"]
        assert rejection["reason_code"] == "stale_observation" and rejection["details"]["source"] == "caliper"
        assert record["integration"]["observation_rejected"]["abstention_reason"] == "stale_telemetry_fail_safe"
    assert result["validation_records"][0]["handled"] == "abstained"
    after = [r for r in result["records"] if r["integration"]["decision_index"] == 2]
    assert all(r["decision"]["reason"] != "stale_telemetry_fail_safe" for r in after)   # recovered
    assert result["live_submission_enabled"] is False and result["mode"] == "shadow"


def test_abstain_policy_never_stops_on_ordering_but_does_not_advance_past_it():
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    result = runner.run([frame(5), frame(3), frame(6)])
    assert result["abstained_frame_count"] == 1 and result["record_count"] == 9
    out_of_order = [r for r in result["records"] if r["integration"]["decision_index"] == 3]
    assert {r["decision"]["reason"] for r in out_of_order} == {"nonadvancing_telemetry_fail_safe"}


def test_structural_errors_always_stop_even_when_abstaining():
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    broken = replace(frame(1), decision_index=-1)          # structural, not an observation-quality problem
    result = runner.run([frame(0), broken, frame(2)])
    assert result["stop_reasons"] == ["telemetry_validation_failed"] and result["record_count"] == 3
    assert result["abstained_frame_count"] == 0
    with pytest.raises(ValueError, match="invalid_frame_policy"):
        IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="continue")


def test_provider_and_runner_must_share_one_freshness_limit():
    adapter = PrometheusTelemetryAdapter({"o": "http://o/metrics"}, fetch=lambda *_: PROM)
    provider = ProductionTelemetryProvider(adapter, CHANNEL, config_reader=lambda: CONFIG, required_fields=(),
                                           freshness_policy=FreshnessPolicy(max_age_s=10))
    runner = IntegratedControllerRunner({"static": StaticController(CONFIG)}, CONFIG, run_id="r")
    with pytest.raises(IntegrationSafetyError, match="freshness limit"):
        runner.run_provider(provider, samples=1)
    with pytest.raises(ValueError, match="accepted freshness limit"):
        IntegratedControllerRunner({"static": StaticController(CONFIG)}, CONFIG, run_id="r", max_telemetry_age_s=3600)


def test_quality_errors_stay_integration_safety_errors():
    with pytest.raises(IntegrationSafetyError, match="stale"):
        frame(0, source_age=6.0).validate()
