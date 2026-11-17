"""Strict live workload gap policy (owner Decision 2, 2026-11-17). Deterministic, offline.

Cases: genuine zero, missing interval, unavailable source, verified backfill, unrecoverable gap,
no interpolation/forward-fill/zero-fill, separation from historical benchmark zero-fill semantics.
Expected EWMA values come from an independent closed form.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from fabric_perf.controllers.base import ArrivalHistoryReference, ControllerState, FabricConfig, PerformancePrediction
from fabric_perf.controllers.live_foundation import StreamingEWMA, StreamingWorkload
from fabric_perf.controllers.strategies import PredictiveController
from fabric_perf.controllers.workload_history import (
    GAP_POLICY, HISTORICAL_BENCHMARK_ZERO_SEMANTICS, ArrivalHistoryStore, WorkloadHistoryError, prefix_sha256,
)

ROOT = Path(__file__).resolve().parents[1]
DAY0 = datetime(2026, 3, 1, tzinfo=timezone.utc)
ALPHA = 0.005
ARCHIVE_SHA = "a" * 64  # stands for the SHA-256 of a verified historical artifact in these fixtures


def stamp(i: int) -> str:
    return (DAY0 + timedelta(seconds=i)).isoformat()


def closed_form(values, last_index: int) -> float:
    x = np.asarray(values[: last_index + 1], dtype=float)
    k = np.arange(1, last_index + 1)
    return float((1 - ALPHA) ** last_index * x[0] + (ALPHA * (1 - ALPHA) ** (last_index - k)) @ x[1:])


def series(n: int) -> list[int]:
    return [0 if i % 5 == 0 else 10 + (i % 7) for i in range(n)]  # includes genuine zero seconds


def feed(store, values, skip=frozenset(), start: int = 0) -> None:
    for i, value in enumerate(values, start):
        if i not in skip:
            store.add(stamp(i), value, collected_at_utc=stamp(i + 1))


class FixedPerformance:
    last_prediction_metadata = {"model_identity": "fixture"}

    def predict(self, arrival_rate_tps, config):
        return PerformancePrediction(throughput_tps=min(arrival_rate_tps, 12.0), latency_ms=float(config.max_message_count))


def decide(store, now_index: int):
    """Predictive decision on the store's snapshot (fresh controller, as at a restart)."""
    times, values, ref = store.snapshot(now_utc=stamp(now_index))
    config = FabricConfig(max_message_count=40)
    controller = PredictiveController(FixedPerformance(), config, arrival_forecaster=StreamingEWMA(store.source),
                                      candidate_configs=[replace(config, max_message_count=b) for b in (10, 40)],
                                      horizon_seconds=60)
    state = ControllerState(timestamp_utc=stamp(now_index), current_config=config, telemetry_age_s=1,
                            arrival_history_tps=values, arrival_history_timestamps_utc=times, arrival_history_ref=ref)
    return controller.decide(state), ref


# ---------------------------------------------------------------------------- 1. genuine zero
def test_healthy_zero_second_is_recorded_as_a_genuine_zero():
    values = series(900)
    store = StreamingWorkload("gap-zero")
    feed(store, values)
    times, counts, ref = store.snapshot(now_utc=stamp(900))
    assert ref.status == "complete" and ref.bin_count == 900 and ref.missing_seconds == 0
    assert counts[0] == 0.0 and counts.count(0.0) == 180  # zeros kept as observed values
    decision, _ = decide(store, 900)
    assert decision.outcome in {"propose", "hold"}
    assert decision.predicted_arrival_rate_tps == pytest.approx(closed_form(values, 899), abs=1e-10)


# ---------------------------------------------------------------------------- 2. missing interval
def test_missing_interval_is_not_zero_and_forecasting_abstains():
    values = series(900)
    store = StreamingWorkload("gap-missing")
    feed(store, values, skip=set(range(300, 305)))
    times, counts, ref = store.snapshot(now_utc=stamp(900))
    assert (times, counts) == ((), ()) and ref.status == "incomplete"
    assert ref.missing_seconds == 5 and ref.unavailable_seconds == 0 and ref.complete_prefix_bins == 300
    assert ref.gap_intervals == ((stamp(300), stamp(304), "missing"),)
    decision, _ = decide(store, 900)
    assert decision.outcome == "abstain" and decision.reason == "workload_prefix_incomplete_fail_safe"
    assert decision.metadata["abstention_stage"] == "forecast" and "not interpolated" in decision.metadata["error"]


# ---------------------------------------------------------------------------- 3. unavailable source
def test_disconnected_source_is_recorded_as_unavailable(tmp_path):
    log = tmp_path / "history.jsonl"
    values = series(900)
    store = StreamingWorkload("gap-unavailable", log_path=log)
    feed(store, values[:400])
    with pytest.raises(WorkloadHistoryError, match="after it has ended"):
        store.mark_unavailable(stamp(400), stamp(419), reason="collector disconnected", reported_at_utc=stamp(419))
    with pytest.raises(WorkloadHistoryError, match="overlaps"):
        store.mark_unavailable(stamp(399), stamp(419), reason="collector disconnected", reported_at_utc=stamp(420))
    store.mark_unavailable(stamp(400), stamp(419), reason="collector disconnected", reported_at_utc=stamp(420))
    with pytest.raises(WorkloadHistoryError, match="duplicate or out-of-order"):
        store.add(stamp(410), 0, collected_at_utc=stamp(411))  # a late "0" cannot replace an outage
    feed(store, values[420:], start=420)
    store.close()
    _, _, ref = store.snapshot(now_utc=stamp(900))
    assert ref.status == "incomplete" and ref.unavailable_seconds == 20 and ref.missing_seconds == 0
    assert ref.gap_intervals == ((stamp(400), stamp(419), "unavailable"),)
    assert decide(store, 900)[0].reason == "workload_prefix_incomplete_fail_safe"
    events = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()[1:]]
    outage = [e for e in events if e["kind"] == "unavailable"]
    assert outage == [{"seq": outage[0]["seq"], "kind": "unavailable", "from": stamp(400), "to": stamp(419),
                       "reason": "collector disconnected", "reported_at": stamp(420)}]
    assert not any(e["kind"] == "observation" and stamp(400) <= e["bin"] <= stamp(419) for e in events)


# ---------------------------------------------------------------------------- 4. verified backfill
@pytest.mark.parametrize("hole", ["missing", "unavailable", "before_first_live_bin"])
def test_verified_backfill_reconstructs_the_exact_prefix(hole):
    values = series(1_200)
    uninterrupted = StreamingWorkload("gap-backfill", store_id="s")
    feed(uninterrupted, values)
    store = StreamingWorkload("gap-backfill", store_id="s")
    if hole == "missing":
        gap = range(500, 530)
        feed(store, values, skip=set(gap))
    elif hole == "unavailable":
        gap = range(500, 530)
        feed(store, values[:500])
        store.mark_unavailable(stamp(500), stamp(529), reason="exporter unhealthy", reported_at_utc=stamp(530))
        feed(store, values[530:], start=530)
    else:  # process started at 00:10:00; the day's first 600 seconds come from the verified archive
        gap = range(0, 600)
        feed(store, values[600:], start=600)
    assert store.snapshot(now_utc=stamp(1_200))[2].status == "incomplete"
    batch = store.backfill([(stamp(i), values[i]) for i in gap],
                           provenance={"source": "flashbots-archive-replay", "sha256": ARCHIVE_SHA})
    times, counts, ref = store.snapshot(now_utc=stamp(1_200))
    expected_times, expected_counts, expected_ref = uninterrupted.snapshot(now_utc=stamp(1_200))
    assert (times, counts) == (expected_times, expected_counts)
    assert ref.status == "complete" and ref.prefix_sha256 == expected_ref.prefix_sha256
    assert ref.backfilled_bins == len(gap) and ref.observed_bins == 1_200 - len(gap) and ref.backfill_batch_ids == (batch,)
    rebuilt, uninterrupted_decision = decide(store, 1_200)[0], decide(uninterrupted, 1_200)[0]
    assert rebuilt.predicted_arrival_rate_tps == uninterrupted_decision.predicted_arrival_rate_tps  # bit-identical
    assert rebuilt.predicted_arrival_rate_tps == pytest.approx(closed_form(values, 1_199), abs=1e-10)


def test_backfill_requires_provenance_and_is_atomic():
    values = series(900)
    store = StreamingWorkload("gap-atomic")
    feed(store, values, skip=set(range(300, 310)))
    before = store.snapshot(now_utc=stamp(900))[2]
    bins = [(stamp(i), values[i]) for i in range(300, 310)]
    for provenance in ({}, {"source": "archive"}, {"source": "", "sha256": ARCHIVE_SHA}, {"source": "archive", "sha256": "xyz"}):
        with pytest.raises(WorkloadHistoryError, match="provenance"):
            store.backfill(bins, provenance=provenance)
    good = {"source": "archive", "sha256": ARCHIVE_SHA}
    for bad in (bins + [(stamp(299), values[299])],          # would overwrite an observed second
                bins + [(stamp(305), values[305])],          # repeats a second
                bins + [(stamp(950), 1)],                    # beyond the latest accounted second
                bins[:-1] + [(stamp(309), -1)],              # invalid count
                bins[:-1] + [(stamp(309), 1.5)]):
        with pytest.raises(WorkloadHistoryError):
            store.backfill(bad, provenance=good)
    assert store.snapshot(now_utc=stamp(900))[2] == before  # nothing applied, event log unchanged
    store.backfill(bins[:5], provenance=good)                 # partial reconstruction
    _, _, ref = store.snapshot(now_utc=stamp(900))
    assert ref.status == "incomplete" and ref.missing_seconds == 5 and ref.backfilled_bins == 0
    store.backfill(bins[5:], provenance=good)
    assert store.snapshot(now_utc=stamp(900))[2].status == "complete"


# ---------------------------------------------------------------------------- 5. unrecoverable gap
def test_unrecoverable_gap_abstains_until_the_next_complete_utc_day():
    store = StreamingWorkload("gap-unrecoverable")
    day1 = series(86_400)
    feed(store, day1, skip={40_000})  # one lost second, never recovered
    for origin in (40_001, 60_000, 86_399):
        decision, ref = decide(store, origin)
        assert decision.reason == "workload_prefix_incomplete_fail_safe" and ref.missing_seconds == 1
    # Next UTC day: 10-minute warm-up abstention, then a forecast from the new day's prefix only.
    day2 = [7 + (i % 3) for i in range(600)]
    feed(store, [day2[0]], start=86_400)
    feed(store, day2[1:599], start=86_401)
    assert decide(store, 86_999)[0].reason == "prediction_failure_fail_safe"  # 599 bins: warm-up
    feed(store, [day2[599]], start=86_999)
    decision, ref = decide(store, 87_000)
    assert ref.status == "complete" and ref.utc_day == "2026-03-02" and ref.bin_count == 600
    assert decision.predicted_arrival_rate_tps == pytest.approx(closed_form(day2, 599), abs=1e-10)


def test_midnight_never_joins_days_and_stale_source_abstains():
    store = StreamingWorkload("gap-midnight")
    feed(store, series(86_400))
    times, values, ref = store.snapshot(now_utc=(DAY0 + timedelta(days=1, seconds=0.5)).isoformat())
    assert (times, values, ref.status, ref.utc_day) == ((), (), "empty", "2026-03-02")  # yesterday is never offered
    stale = StreamingWorkload("gap-stale")
    feed(stale, series(900))
    decision, ref = decide(stale, 910)  # complete prefix, but the source went silent for 10 s
    assert ref.status == "complete" and decision.reason == "prediction_failure_fail_safe"
    assert "stale" in decision.metadata["error"]


# ---------------------------------------------------------------------------- 6. no filling
def test_no_interpolation_forward_fill_or_zero_fill(tmp_path):
    log = tmp_path / "history.jsonl"
    values = series(900)
    store = StreamingWorkload("gap-nofill", log_path=log)
    feed(store, values, skip={450, 451, 452})
    store.close()
    observed = {e["bin"]: e["count"] for e in map(json.loads, log.read_text(encoding="utf-8").splitlines()[1:])
                if e["kind"] == "observation"}
    assert set(observed) == {stamp(i) for i in range(900)} - {stamp(450), stamp(451), stamp(452)}
    assert store.history() == ((), ()) and store.snapshot(now_utc=stamp(900))[0] == ()
    gap = [e for e in map(json.loads, log.read_text(encoding="utf-8").splitlines()[1:]) if e["kind"] == "gap_detected"]
    assert [(e["from"], e["to"]) for e in gap] == [(stamp(450), stamp(452))]
    rebuilt = ArrivalHistoryStore.from_log(log)  # replay re-derives the same gap
    assert rebuilt.snapshot(now_utc=stamp(900))[2].gap_intervals == ((stamp(450), stamp(452), "missing"),)


@pytest.mark.parametrize("change", ["duplicate", "out_of_order", "stale", "future", "fractional", "negative", "nan", "bool"])
def test_invalid_live_bins_are_rejected_without_mutation(change):
    store = StreamingWorkload("gap-invalid", store_id="s")
    feed(store, series(10))
    before = store.snapshot(now_utc=stamp(10))
    t, value, collected = stamp(10), 3, stamp(11)
    if change == "duplicate": t, collected = stamp(9), stamp(10)
    if change == "out_of_order": t, collected = stamp(5), stamp(10)
    if change == "stale": collected = stamp(20)
    if change == "future": collected = stamp(10)
    if change == "fractional": t = (DAY0 + timedelta(seconds=10.5)).isoformat()
    if change == "negative": value = -1
    if change == "nan": value = float("nan")
    if change == "bool": value = True
    with pytest.raises(ValueError):
        store.add(t, value, collected_at_utc=collected)
    assert store.snapshot(now_utc=stamp(10)) == before


def test_gap_handling_is_deterministic(tmp_path):
    def run(path):
        store = StreamingWorkload("gap-det", store_id="fixed", log_path=path)
        values = series(1_000)
        feed(store, values[:300])
        store.mark_unavailable(stamp(300), stamp(309), reason="exporter unhealthy", reported_at_utc=stamp(310))
        feed(store, values[310:], skip={700}, start=310)
        store.backfill([(stamp(i), values[i]) for i in range(300, 310)], provenance={"source": "archive", "sha256": ARCHIVE_SHA})
        store.close()
        return store.snapshot(now_utc=stamp(1_000))[2]
    first, second = run(tmp_path / "a.jsonl"), run(tmp_path / "b.jsonl")
    assert replace(first, history_log=None) == replace(second, history_log=None)
    assert (tmp_path / "a.jsonl").read_bytes() == (tmp_path / "b.jsonl").read_bytes()
    assert first.status == "incomplete" and first.missing_seconds == 1 and first.unavailable_seconds == 0


def test_reference_must_match_the_history_it_describes():
    store = StreamingWorkload("gap-ref")
    feed(store, series(700))
    times, values, ref = store.snapshot(now_utc=stamp(700))
    assert prefix_sha256(times, values) == ref.prefix_sha256 and ref.gap_policy == GAP_POLICY
    with pytest.raises(ValueError, match="bin_count"):
        ControllerState(timestamp_utc=stamp(700), arrival_history_tps=values[:-1], arrival_history_timestamps_utc=times[:-1],
                        arrival_history_ref=ref)
    with pytest.raises(ValueError, match="supplies no bins"):
        ArrivalHistoryReference.from_mapping({**ref.as_dict(), "status": "incomplete"})
    with pytest.raises(ValueError, match="history reference"):
        StreamingEWMA("other-source").predict_timestamped(times, values, 60, now_utc=stamp(700), history_ref=ref)


# ---------------------------------------------------------------------------- 7. historical benchmark
def test_historical_benchmark_zero_fill_semantics_are_preserved_separately():
    """The frozen benchmark series keep their zero-filled seconds; offline frames carry no live reference."""
    from fabric_perf.controllers.adapters import FlashbotsSeriesForecaster
    from fabric_perf.controllers.offline_replay import workload_frames

    pins = json.loads((ROOT / "Integration/system_integration/offline_replay_inputs.json").read_text(encoding="utf-8"))["files"]
    path = ROOT / pins["series"]["path"]
    if not path.is_file():
        pytest.skip(f"pinned historical series not present in this checkout: {path}")
    assert hashlib.sha256(path.read_bytes()).hexdigest().upper() == pins["series"]["sha256"]
    counts = pd.read_csv(path)["arrival_count"].to_numpy()
    assert len(counts) == 86_400 and (counts == 0).sum() > 0  # zero-filled reception-free seconds remain
    assert "zero-filled" in HISTORICAL_BENCHMARK_ZERO_SEMANTICS
    forecaster = FlashbotsSeriesForecaster(path, baseline="persistence")
    frame = next(workload_frames(forecaster, FabricConfig(max_message_count=10), run_id="hist", start=600, samples=1))
    assert frame.state.arrival_history_ref is None
    serialized = frame.state.as_dict()
    assert "arrival_history_ref" not in serialized and len(serialized["arrival_history_tps"]) == 600  # unchanged format
    assert serialized["arrival_history_tps"] == [float(v) for v in counts[:600]]
