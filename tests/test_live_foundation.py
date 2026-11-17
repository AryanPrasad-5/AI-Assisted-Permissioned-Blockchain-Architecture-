from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fabric_perf.controllers.live_foundation import StreamingEWMA, StreamingWorkload, make_shadow_foundation
from fabric_perf.controllers.base import FabricConfig
from fabric_perf.controllers.telemetry import PrometheusTelemetryAdapter

ROOT = Path(__file__).resolve().parents[1]
START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def stamp(n):
    return (START + timedelta(seconds=n)).isoformat()


def history(n=600, value=12):
    return tuple(stamp(i) for i in range(n)), (value,) * n


def test_streaming_warmup_and_causal_forecast():
    f = StreamingEWMA("fixture")
    t, v = history()
    with pytest.raises(ValueError, match="600"):
        f.predict_timestamped(t[:-1], v[:-1], 60, now_utc=stamp(599))
    result = f.predict_timestamped(t, v, 60, now_utc=stamp(600))
    assert result.value_tps == 12 and result.origin_timestamp_utc == stamp(600)
    assert result.as_dict()["target_end_timestamp_utc"] == stamp(660)


def test_streaming_uses_frozen_recurrence_not_future_data():
    t, v = history()
    values = list(v)
    values[-1] = 20
    result = StreamingEWMA("fixture").predict_timestamped(t, values, 60, now_utc=stamp(600))
    assert result.value_tps == pytest.approx(0.005 * 20 + 0.995 * 12)


@pytest.mark.parametrize("change", ["gap", "duplicate", "out_of_order", "stale", "future", "invalid", "horizon"])
def test_streaming_rejects_bad_history(change):
    t, v = history()
    t, v = list(t), list(v)
    now, h = stamp(600), 60
    if change == "gap": t[300] = stamp(301)
    if change == "duplicate": t[-1] = t[-2]
    if change == "out_of_order": t[-1], t[-2] = t[-2], t[-1]
    if change == "stale": now = stamp(610)
    if change == "future": now = stamp(599)
    if change == "invalid": v[-1] = float("nan")
    if change == "horizon": h = 10
    with pytest.raises(ValueError):
        StreamingEWMA("fixture").predict_timestamped(t, v, h, now_utc=now)


def test_interruption_resets_warmup_without_filling_gaps():
    # Strict live gap policy (2026-11-17, owner Decision 2): the hole is kept as an explicit
    # missing second (formerly the segment was cleared); no bin is ever filled, so the day's
    # prefix stays incomplete and no history is offered to the forecaster for the rest of the day.
    source = StreamingWorkload("fixture")
    for i in range(600): source.add(stamp(i), 12, collected_at_utc=stamp(i+1))
    assert len(source.history()[0]) == 600
    with pytest.raises(ValueError): source.add(stamp(599), 12, collected_at_utc=stamp(600))
    source.add(stamp(601), 12, collected_at_utc=stamp(602))
    assert source.history() == ((), ())
    for i in range(602, 1201): source.add(stamp(i), 12, collected_at_utc=stamp(i+1))
    times, values, ref = source.snapshot(now_utc=stamp(1201))
    assert (times, values) == ((), ()) and ref.status == "incomplete"
    assert ref.missing_seconds == 1 and ref.gap_intervals == ((stamp(600), stamp(600), "missing"),)
    with pytest.raises(ValueError, match="600"):
        StreamingEWMA("fixture").predict_timestamped(times, values, 60, now_utc=stamp(1201))


@pytest.mark.parametrize("value", [-1, 0.5, float("inf"), True])
def test_source_invalid_does_not_mutate(value):
    source = StreamingWorkload("fixture")
    with pytest.raises(ValueError): source.add(stamp(0), value, collected_at_utc=stamp(1))
    assert source.history() == ((), ())


def test_prometheus_missing_leader_is_not_fabricated_zero():
    adapter = PrometheusTelemetryAdapter({"orderer": "fixture"}, fetch=lambda *_: 'other_metric 1\n')
    s = adapter.collect("channel", timestamp_utc=stamp(600))
    assert s.metric("raft_leader_count").value is None
    assert s.metric("latency_mean_ms").value is None
    assert s.metric("delivered_valid_tps").value is None


def test_prometheus_timeout_records_failure():
    def fail(*_): raise TimeoutError("fixture timeout")
    s = PrometheusTelemetryAdapter({"orderer": "fixture"}, fetch=fail).collect("channel", timestamp_utc=stamp(600))
    assert "orderer" in s.health["endpoint_errors"]
    assert s.metric("inflight_backlog").value is None


def test_nonfinite_prometheus_sample_is_rejected():
    s = PrometheusTelemetryAdapter({"orderer": "fixture"}, fetch=lambda *_: 'some_metric NaN\n').collect("channel", timestamp_utc=stamp(600))
    assert "orderer" in s.health["endpoint_errors"]


def test_explicit_prometheus_sample_timestamp_is_not_silently_ignored():
    s = PrometheusTelemetryAdapter({"orderer": "fixture"}, fetch=lambda *_: 'some_metric 1 1700000000000\n').collect("channel", timestamp_utc=stamp(600))
    assert "timestamp is unsupported" in s.health["endpoint_errors"]["orderer"]


def test_three_controllers_share_live_provider_frame_in_shadow():
    source = StreamingWorkload("fixture workload")
    for i in range(600): source.add(stamp(i), 12, collected_at_utc=stamp(i+1))
    config = FabricConfig(max_message_count=40)
    metrics = 'consensus_etcdraft_is_leader{channel="channel"} 1\n'
    provider, runner = make_shadow_foundation(root=ROOT,
        manifest_path=ROOT / "Integration/system_integration/offline_replay_inputs.json",
        endpoints={"orderer": "fixture"}, channel_id="channel", current_config=config,
        config_reader=lambda: config, workload=source, candidate_map={10: 10, 40: 40},
        latency_high_ms=1500, latency_low_ms=1200, congested_max_message_count=10, consecutive_samples=2,
        fetch=lambda *_: metrics)
    frame = provider.collect(run_id="live-foundation", decision_index=0, observation_timestamp_utc=stamp(600))
    assert frame.state.failed_transactions is None
    # D3 (2026-11-17): provenance is a bounded summary plus the prefix reference, not per-bin lists.
    assert frame.provenance["workload"]["last_collection_timestamp_utc"] == stamp(600)
    assert frame.provenance["workload"]["reference"]["bin_count"] == 600
    result = runner.run([frame])
    assert result["record_count"] == 3 and result["identical_input_digests"]
    assert result["mode"] == "shadow" and not result["live_submission_enabled"]
    records = {r["decision"]["controller"]: r for r in result["records"]}
    assert records["reactive"]["decision"]["outcome"] == "abstain"
    assert records["predictive"]["decision"]["predicted_arrival_rate_tps"] == 12
    assert all(not r["configuration_state"]["proposal_applied"] for r in records.values())
    assert all(r["state"]["failed_transactions"] is None for r in records.values())
    assert records["static"]["integration"]["observation_provenance"]["workload"]["source"] == "fixture workload"


def test_failed_workload_source_is_recorded_and_never_promoted():
    class Broken:
        source = "fixture"
        def history(self): raise OSError("disconnected")
    config = FabricConfig(max_message_count=40)
    provider, runner = make_shadow_foundation(root=ROOT,
        manifest_path=ROOT / "Integration/system_integration/offline_replay_inputs.json",
        endpoints={"orderer": "fixture"}, channel_id="channel", current_config=config,
        config_reader=lambda: config, workload=Broken(), candidate_map={10: 10, 40: 40},
        latency_high_ms=1500, latency_low_ms=1200, congested_max_message_count=10, consecutive_samples=2,
        fetch=lambda *_: 'consensus_etcdraft_is_leader{channel="channel"} 1\n')
    frame = provider.collect(run_id="live-foundation", decision_index=0, observation_timestamp_utc=stamp(600))
    assert any("arrival_history_read_error" in item for item in frame.critical_missing)
    result = runner.run([frame])
    assert result["record_count"] == 0 and result["stop_reasons"] == ["telemetry_validation_failed"]
    assert result["validation_records"][0]["observation"]["critical_missing"]
