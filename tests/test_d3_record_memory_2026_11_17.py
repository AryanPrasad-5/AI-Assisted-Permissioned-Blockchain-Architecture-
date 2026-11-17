"""D3 (independent audit 2026-11-17): compact live-shadow records over a shared history store.

Offline and shadow-only. Synthetic in-domain arrival counts, fixture Prometheus text and a
fixture config reader; the pinned HFBTP RF artifacts are loaded read-only where noted.
Expected EWMA values come from an independent closed form, not from the production function.
"""
from __future__ import annotations

import gc
import json
import time
import tracemalloc
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest

from fabric_perf.controllers.base import BaseController, FabricConfig, PerformancePrediction, StaticController
from fabric_perf.controllers.live_foundation import (
    LIVE_SHADOW_MAX_RECORD_BYTES, StreamingEWMA, StreamingWorkload, make_shadow_foundation, reproduce_recorded_forecast,
)
from fabric_perf.controllers.live_integration import IntegratedControllerRunner, ProductionTelemetryProvider
from fabric_perf.controllers.strategies import PredictiveController, ReactiveController
from fabric_perf.controllers.telemetry import PrometheusTelemetryAdapter
from fabric_perf.controllers.workload_history import ArrivalHistoryStore, WorkloadHistoryError, prefix_sha256

ROOT = Path(__file__).resolve().parents[1]
DAY0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
ALPHA = 0.005
METRICS = 'consensus_etcdraft_is_leader{channel="ch"} 1\n'


def stamp(i: int) -> str:
    return (DAY0 + timedelta(seconds=i)).isoformat()


def count(i: int) -> int:
    return 10 + (i % 7)  # inside HFBTP's [10, 200] tx/s range


def closed_form(values, last_index: int) -> float:
    x = np.asarray(values[: last_index + 1], dtype=float)
    k = np.arange(1, last_index + 1)
    return float((1 - ALPHA) ** last_index * x[0] + (ALPHA * (1 - ALPHA) ** (last_index - k)) @ x[1:])


def fill(source, stop: int, start: int = 0) -> None:
    for i in range(start, stop):
        source.add(stamp(i), count(i), collected_at_utc=stamp(i + 1))


def foundation(source, **kwargs):
    config = FabricConfig(max_message_count=40)
    return make_shadow_foundation(
        root=ROOT, manifest_path=ROOT / "Integration/system_integration/offline_replay_inputs.json",
        endpoints={"orderer": "fixture"}, channel_id="ch", current_config=config, config_reader=lambda: config,
        workload=source, candidate_map={10: 10, 40: 40}, latency_high_ms=1500, latency_low_ms=1200,
        congested_max_message_count=10, consecutive_samples=2, run_id="d3", fetch=lambda *_: METRICS, **kwargs)


def frames_for(provider, source, start: int, count_frames: int, *, first_index: int = 0):
    """One frame per second; the bin for second t-1 arrives before the frame at t."""
    for k in range(count_frames):
        origin = start + k
        if k:
            source.add(stamp(origin - 1), count(origin - 1), collected_at_utc=stamp(origin))
        yield provider.collect(run_id="d3", decision_index=first_index + k, observation_timestamp_utc=stamp(origin))


class FixedPerformance:
    """Deterministic stand-in for the HFBTP model (memory tests only; no model load)."""

    last_prediction_metadata = {"model_identity": "fixture", "feature_values": {}}

    def predict(self, arrival_rate_tps, config):
        return PerformancePrediction(throughput_tps=min(arrival_rate_tps, 12.0), latency_ms=float(config.max_message_count) * 10)


def light_runner(source):
    """Same provider/runner wiring as the factory, with a fixture performance predictor."""
    config = FabricConfig(max_message_count=40)
    controllers = {
        "static": StaticController(config),
        "reactive": ReactiveController(config, congested_config=replace(config, max_message_count=10),
                                       latency_high_ms=1500, latency_low_ms=1200, consecutive_samples=2),
        "predictive": PredictiveController(FixedPerformance(), config, arrival_forecaster=StreamingEWMA(source.source),
                                           candidate_configs=[replace(config, max_message_count=b) for b in (10, 40)],
                                           horizon_seconds=60),
    }
    provider = ProductionTelemetryProvider(PrometheusTelemetryAdapter({"orderer": "fixture"}, fetch=lambda *_: METRICS), "ch",
                                           config_reader=lambda: config, arrival_history_reader=source.history,
                                           workload=source, required_fields=())
    return provider, IntegratedControllerRunner(controllers, config, run_id="d3", shadow=True,
                                                max_record_bytes=LIVE_SHADOW_MAX_RECORD_BYTES)


# --------------------------------------------------------------------------------------------
def test_record_size_and_latency_do_not_scale_with_accumulated_daily_history():
    """Serialized record size is flat from 600 to 86,396 bins (it was ~8 MB/record at 86,396)."""
    sizes, timings = {}, {}
    for n in (600, 21_600, 86_396):
        source = StreamingWorkload("d3-size")
        fill(source, n)
        provider, runner = foundation(source)
        started = time.perf_counter()
        result = runner.run([provider.collect(run_id="d3", decision_index=0, observation_timestamp_utc=stamp(n))])
        timings[n] = time.perf_counter() - started
        assert result["stop_reasons"] == [] and result["record_count"] == 3
        records = {r["integration"]["strategy"]: r for r in result["records"]}
        assert records["predictive"]["decision"]["outcome"] == "propose"  # full candidate scoring exercised
        sizes[n] = {name: len(json.dumps(r, sort_keys=True)) for name, r in records.items()}
        for record in records.values():
            assert "arrival_history_tps" not in record["state"] and "arrival_history_timestamps_utc" not in record["state"]
            assert record["state"]["arrival_history_length"] == n
            assert record["state"]["arrival_history_ref"]["bin_count"] == n
    for name in ("predictive", "reactive", "static"):
        values = [sizes[n][name] for n in sizes]
        assert max(values) - min(values) < 256, (name, sizes)  # only digit-count noise
        assert max(values) < LIVE_SHADOW_MAX_RECORD_BYTES
    # Generous regression bound (measured 0.33 s at 86,396 bins on the audit machine).
    assert timings[86_396] < 5.0, timings


def test_history_is_stored_once_and_never_copied_into_decisions(tmp_path):
    log = tmp_path / "workload_history.jsonl"
    source = StreamingWorkload("d3-once", log_path=log)
    fill(source, 900)
    provider, runner = light_runner(source)
    result = runner.run(frames_for(provider, source, 900, 6))
    source.close()
    assert result["frame_count"] == 6 and result["record_count"] == 18
    # Each 1-second observation is held once in memory and written once to the append-only log.
    assert source.retained_bins() == 905
    observations = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()[1:]]
    assert [e["bin"] for e in observations if e["kind"] == "observation"] == [stamp(i) for i in range(905)]
    # No record carries interior history: a mid-prefix timestamp never appears in any record.
    serialized = json.dumps(result["records"], sort_keys=True)
    # count() rather than `not in`: pytest's failure diff of a large string would be very slow.
    assert (serialized.count(stamp(450)), serialized.count(stamp(1))) == (0, 0)
    refs = [r["decision_trace"]["history_reference"] for r in result["records"]]
    assert {(ref["bin_count"], ref["prefix_sha256"]) for ref in refs} == {
        (n, prefix_sha256([stamp(i) for i in range(n)], [count(i) for i in range(n)])) for n in range(900, 906)}


def test_forecast_is_reproducible_from_referenced_history_in_memory_and_from_log(tmp_path):
    log = tmp_path / "workload_history.jsonl"
    source = StreamingWorkload("d3-repro", log_path=log)
    fill(source, 1_800)
    provider, runner = foundation(source)
    result = runner.run(frames_for(provider, source, 1_800, 3))
    source.close()
    predictive = [r for r in result["records"] if r["integration"]["strategy"] == "predictive"]
    rebuilt = ArrivalHistoryStore.from_log(log)
    assert len(predictive) == 3
    for record in predictive:
        trace = record["decision_trace"]
        n = trace["history_reference"]["bin_count"]
        for store in (source, rebuilt):
            check = reproduce_recorded_forecast(record, store)
            assert check["exact_match"] and check["reproduced_tps"] == record["decision"]["predicted_arrival_rate_tps"]
        assert trace["forecast"]["value_tps"] == pytest.approx(closed_form([count(i) for i in range(n)], n - 1), abs=1e-10)
        assert trace["forecast"]["ewma_parameters"]["alpha"] == 0.005
        assert trace["forecast"]["ewma_parameters"]["horizon_seconds"] == 60
        assert trace["forecast"]["ewma_parameters"]["min_history_bins"] == 600
        assert trace["forecast"]["origin_timestamp_utc"] == stamp(n) == trace["observation_timestamp_utc"]
        assert trace["hfbtp_artifact"]["sha256"]["throughput_model"].startswith("B0ED880F")
        assert trace["hfbtp_artifact"]["sha256"]["latency_model"].startswith("1E7746A2")
        for candidate in trace["candidates"]:
            assert candidate["model_input_features"]["Set Transaction Arrival Rate"] == pytest.approx(trace["forecast"]["value_tps"], abs=1e-12)
            assert candidate["model_input_features"]["Block Size"] == candidate["config"]["max_message_count"]
            assert candidate["predicted_throughput_tps"] >= 0 and candidate["predicted_latency_ms"] >= 0
        assert trace["decision"]["reason"] in {"predicted_utility_improvement", "prediction_hold"}
    # Tampering with one stored count is detected by the prefix digest.
    lines = log.read_text(encoding="utf-8").splitlines()
    tampered = [json.loads(line) for line in lines]
    victim = next(e for e in tampered if e.get("kind") == "observation" and e["bin"] == stamp(700))
    victim["count"] += 1
    bad = tmp_path / "tampered.jsonl"
    bad.write_text("\n".join(json.dumps(e, sort_keys=True, separators=(",", ":")) for e in tampered) + "\n", encoding="utf-8")
    with pytest.raises(WorkloadHistoryError, match="digest"):
        reproduce_recorded_forecast(predictive[0], ArrivalHistoryStore.from_log(bad))


def _retained_growth_per_frame(history_bins: int, frames: int, tmp_path: Path | None) -> float:
    source = StreamingWorkload(f"d3-mem-{history_bins}")
    fill(source, history_bins)
    provider, runner = light_runner(source)
    marks = []

    def stream():
        for k, frame in enumerate(frames_for(provider, source, history_bins, frames)):
            if k in (5, frames - 1):
                gc.collect()
                marks.append(tracemalloc.get_traced_memory()[0])
            yield frame

    tracemalloc.start()
    try:
        out = None if tmp_path is None else tmp_path / f"stream-{history_bins}"
        result = runner.run(stream(), output_dir=out, retain_records=tmp_path is None)
    finally:
        tracemalloc.stop()
    assert result["frame_count"] == frames and result["stop_reasons"] == []
    if tmp_path is not None:
        assert result["records"] == [] and len((out / "controller_decisions.jsonl").read_text().splitlines()) == 3 * frames
    return (marks[1] - marks[0]) / (frames - 1 - 5)


def test_long_replay_memory_is_bounded(tmp_path):
    # Streaming mode: per-frame retained growth is only the store's one new bin (~182 B), not records.
    streaming = _retained_growth_per_frame(3_600, 60, tmp_path)
    assert streaming < 2_000, streaming
    # Retained-records mode: growth per frame is one compact frame of records, the same at 3,600 and 28,800 bins.
    short, long = _retained_growth_per_frame(3_600, 40, None), _retained_growth_per_frame(28_800, 40, None)
    assert short < 100_000 and long < 100_000, (short, long)
    assert abs(long - short) < 0.25 * short, (short, long)


def test_store_memory_is_bounded_by_retained_days():
    store = ArrivalHistoryStore("d3-days", retain_days=2)
    for day in range(4):
        base = day * 86_400
        for i in range(base + 3_600, base + 3_610):
            store.add(stamp(i), 1, collected_at_utc=stamp(i + 1))
    assert store.provenance()["retained_days"] == ["2026-03-03", "2026-03-04"]
    assert store.provenance()["evicted_day_count"] == 2
    assert store.retained_bins() == 2 * 3_610  # each segment indexes seconds from its midnight
    with pytest.raises(WorkloadHistoryError, match="earlier UTC day"):
        store.add(stamp(2 * 86_400 + 4_000), 1, collected_at_utc=stamp(2 * 86_400 + 4_001))


def test_three_controllers_receive_identical_observations_in_isolated_states():
    source = StreamingWorkload("d3-fair")
    fill(source, 900)
    provider, runner = light_runner(source)
    seen = {}

    class Spy(BaseController):
        def __init__(self, name, inner, mutate):
            self.name, self.inner, self.mutate = name, inner, mutate

        def decide(self, state):
            seen.setdefault(self.name, []).append((state, state.timestamp_utc, state.arrival_history_timestamps_utc,
                                                   state.arrival_history_tps, state.arrival_history_ref))
            decision = self.inner.decide(state)
            if self.mutate:  # a misbehaving arm rebinds fields on its own copy only
                state.arrival_history_tps, state.arrival_history_timestamps_utc = (), ()
                state.arrival_history_ref = None
                state.current_config = FabricConfig(max_message_count=10)
            return decision

        def configuration_applied(self, config):
            self.inner.configuration_applied(config)

    for name, harness in runner.harnesses.items():
        harness.controller = Spy(name, harness.controller, mutate=(name == "predictive"))  # runs first (sorted)
    result = runner.run(frames_for(provider, source, 900, 3))
    assert result["identical_input_digests"] and result["frame_count"] == 3
    for frame_index in range(3):
        rows = [seen[name][frame_index] for name in ("predictive", "reactive", "static")]
        assert rows[0][0] is not rows[1][0] and rows[1][0] is not rows[2][0] and rows[0][0] is not rows[2][0]  # one object per arm
        assert len({row[1:] for row in rows}) == 1                      # identical observation content
        assert len(rows[0][2]) == 900 + frame_index and rows[0][4].status == "complete"
        assert rows[0][0].arrival_history_ref is None and rows[1][0].arrival_history_ref is not None  # mutation stayed local
    records = result["records"]
    for index in range(3):
        frame_records = [r for r in records if r["integration"]["decision_index"] == index]
        assert len({r["integration"]["input_digest"] for r in frame_records}) == 1
        # The trace names the shared observation even for the arm that rebound its own copy.
        assert len({json.dumps(r["decision_trace"]["history_reference"], sort_keys=True) for r in frame_records}) == 1
        assert {r["decision_trace"]["history_reference"]["bin_count"] for r in frame_records} == {900 + index}


def test_record_size_limit_fails_closed():
    source = StreamingWorkload("d3-limit")
    fill(source, 900)
    provider, runner = light_runner(source)
    runner.max_record_bytes = 1_000  # far below a real record: the guard must stop, not grow
    result = runner.run(frames_for(provider, source, 900, 2))
    assert result["stop_reasons"] == ["record_size_limit_exceeded"] and result["record_count"] == 0
    stopped = result["validation_records"][0]
    assert stopped["record_bytes"] > 1_000 and stopped["decision_trace"]["strategy"] == "predictive"
    with pytest.raises(ValueError):
        IntegratedControllerRunner({"static": StaticController(FabricConfig())}, FabricConfig(), run_id="x", max_record_bytes=0)


def test_snapshot_is_atomic_under_a_concurrent_writer():
    """D4: bins and reference come from one locked read, so they always agree under concurrent appends.

    A consistency check, not a proof of absence of races: the GIL makes interleavings uncommon.
    """
    import threading

    store = ArrivalHistoryStore("d3-threads", store_id="t")
    total, checked, problems = 6_000, [], []

    def writer():
        for i in range(total):
            store.add(stamp(i), count(i), collected_at_utc=stamp(i + 1))

    thread = threading.Thread(target=writer)
    thread.start()
    while thread.is_alive() or not checked:
        times, counts, ref = store.snapshot()
        if ref.status != "complete":
            continue
        if len(times) != ref.bin_count or times[-1] != ref.last_bin_timestamp_utc \
                or prefix_sha256(times, counts) != ref.prefix_sha256:
            problems.append(ref.bin_count)
        checked.append(ref.bin_count)
    thread.join()
    assert problems == [] and len(set(checked)) > 1, (problems, len(checked))
