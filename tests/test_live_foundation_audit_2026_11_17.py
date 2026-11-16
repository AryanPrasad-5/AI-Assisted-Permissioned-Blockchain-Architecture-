"""Regression tests added by the independent technical audit of 2026-11-17.

The expected EWMA values come from oracles that do not call
``fabric_perf.forecasting.horizon_benchmark``: an explicit closed-form weighted
sum and the stored frozen P2 benchmark forecasts (SHA-256 pinned to
ARCHIVE_20261114T113430Z/SHA256SUMS.txt).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from fabric_perf.controllers.base import FabricConfig
from fabric_perf.controllers.live_foundation import StreamingEWMA, StreamingWorkload, make_shadow_foundation
from fabric_perf.controllers.telemetry import ContinuousCaliperLedgerAdapter

ROOT = Path(__file__).resolve().parents[1]
ALPHA = 0.005
DAY = 86_400
FROZEN = {
    "2026-09-21": ("data/processed/flashbots/series_2026-09-21_1s.csv.gz",
                   "2D54EDD894DD7CCDA67D9DF81DE1A753D7B4CEF4B6DA0271F5FD418D2A98FFB6",
                   "experiments/results/flashbots_horizon_benchmark_v2/select/forecasts_V_H60.csv",
                   "99D73E43CF2E7D779B56EAA8027BA6B5D30E6F5B71FFDE411331802A8B41AB41"),
    "2026-10-12": ("data/processed/flashbots/series_2026-10-12_1s.csv.gz",
                   "E0CE980226C381C039D3AFBC4095F341D82A1B1104A7E902B3A9BB46E6601822",
                   "experiments/results/flashbots_horizon_benchmark_v2/final/forecasts_F_H60.csv",
                   "5F65FF6E4E152698243E37181F67D154C119FDE701C974126E0A0C8D82A7BB0B"),
}


def closed_form_state(counts, last_index: int) -> float:
    """Independent oracle: e_n = (1-a)^n x_0 + sum_{k=1..n} a (1-a)^(n-k) x_k."""
    x = np.asarray(counts[: last_index + 1], dtype=float)
    k = np.arange(1, last_index + 1)
    return float((1 - ALPHA) ** last_index * x[0] + (ALPHA * (1 - ALPHA) ** (last_index - k)) @ x[1:])


def stamps(start: datetime, n: int, offset: int = 0) -> list[str]:
    return [(start + timedelta(seconds=offset + i)).isoformat() for i in range(n)]


DAY1 = datetime(2026, 3, 1, tzinfo=timezone.utc)


def test_origin_on_next_utc_day_abstains_instead_of_joining_days():
    """D1: at origin 00:00:00 the current-day prefix is empty; yesterday's state must not be used."""
    values = [float((i * 7919) % 23) for i in range(DAY)]
    times = stamps(DAY1, DAY)
    with pytest.raises(ValueError, match="next UTC day"):
        StreamingEWMA("fixture").predict_timestamped(times, values, 60, now_utc=(DAY1 + timedelta(days=1, seconds=0.5)).isoformat())
    # Control: the last origin inside the day (23:59:59) is still accepted and matches the oracle.
    result = StreamingEWMA("fixture").predict_timestamped(times[:-1], values[:-1], 60,
                                                          now_utc=(DAY1 + timedelta(seconds=DAY - 1)).isoformat())
    assert result.origin_timestamp_utc == (DAY1 + timedelta(seconds=DAY - 1)).isoformat()
    assert result.value_tps == pytest.approx(closed_form_state(values, DAY - 2), abs=1e-10)


def test_live_source_rollover_window_cannot_forecast_from_previous_day():
    """The in-memory source still holds yesterday until the first new-day bin arrives."""
    source = StreamingWorkload("fixture")
    times = stamps(DAY1, DAY + 1)
    for i in range(DAY):
        source.add(times[i], 3, collected_at_utc=times[i + 1])
    assert len(source.history()[0]) == DAY  # complete previous-day prefix, no new-day bin yet
    next_midnight = DAY1 + timedelta(days=1)
    with pytest.raises(ValueError, match="next UTC day"):
        StreamingEWMA("fixture").predict_timestamped(*source.history(), 60,
                                                     now_utc=(next_midnight + timedelta(seconds=0.5)).isoformat())


def test_uninterrupted_feed_and_restart_reconstruction_match_independent_oracle():
    """Uninterrupted: one source fed bin by bin, one forecaster queried repeatedly.
    Restart: a new forecaster given the same complete prefix. Oracle: closed form."""
    n = 2400
    values = [(i * 7919 + (i // 97) * 13) % 31 for i in range(n)]
    source = StreamingWorkload("fixture")
    live = StreamingEWMA("fixture")
    checked = 0
    for i, value in enumerate(values):
        source.add(stamps(DAY1, 1, i)[0], value, collected_at_utc=stamps(DAY1, 1, i + 1)[0])
        origin = i + 1
        if origin >= 600 and origin % 150 == 0:
            now = stamps(DAY1, 1, origin)[0]
            uninterrupted = live.predict_timestamped(*source.history(), 60, now_utc=now).value_tps
            restarted = StreamingEWMA("fixture").predict_timestamped(stamps(DAY1, origin), values[:origin], 60, now_utc=now).value_tps
            assert uninterrupted == restarted
            assert uninterrupted == pytest.approx(closed_form_state(values, origin - 1), abs=1e-10)
            checked += 1
    assert checked == 13  # origins 600, 750, ..., 2400


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


@pytest.mark.parametrize("date", sorted(FROZEN))
def test_streaming_reconstruction_reproduces_stored_frozen_benchmark(date):
    series_rel, series_sha, forecasts_rel, forecasts_sha = FROZEN[date]
    series_path, forecasts_path = ROOT / series_rel, ROOT / forecasts_rel
    if not series_path.is_file() or not forecasts_path.is_file():
        pytest.skip(f"frozen inputs not present in this checkout: {series_rel}, {forecasts_rel}")
    assert _sha256(series_path) == series_sha
    assert _sha256(forecasts_path) == forecasts_sha
    counts = pd.read_csv(series_path)["arrival_count"].to_numpy(dtype=np.int64)
    assert len(counts) == DAY
    stored = pd.read_csv(forecasts_path)
    start = datetime.fromisoformat(f"{date}T00:00:00+00:00")
    times = stamps(start, DAY)
    rows = sorted(set(range(0, len(stored), 48)) | {len(stored) - 1})
    for row in rows:
        origin = int(stored["origin_s"].iloc[row])
        result = StreamingEWMA("frozen-check").predict_timestamped(
            times[:origin], counts[:origin].astype(float), 60, now_utc=times[origin])
        assert result.value_tps == pytest.approx(float(stored["yhat_ewma"].iloc[row]), abs=1e-12)
        assert result.value_tps == pytest.approx(closed_form_state(counts, origin - 1), abs=1e-10)
        assert result.history_last_timestamp_utc == times[origin - 1]


def _write_client_sources(tmp_path: Path, now: datetime, latency_ms: float):
    ms = lambda dt: int(dt.timestamp() * 1000)
    recorder, submits, ledger = tmp_path / "recorder.csv", tmp_path / "submits.csv", tmp_path / "ledger.csv"
    recorder.write_text("tx_id,seq_finish,create_ms,final_ms,latency_ms,status\n"
                        f"a,0,{ms(now - timedelta(seconds=3))},{ms(now - timedelta(seconds=2))},{latency_ms},success\n"
                        f"b,1,{ms(now - timedelta(seconds=2))},{ms(now - timedelta(seconds=1))},{latency_ms},success\n",
                        encoding="utf-8")
    submits.write_text("seq_submit,t_submit_evt_ms,count\n"
                       f"0,{ms(now - timedelta(seconds=3))},1\n1,{ms(now - timedelta(seconds=2))},1\n", encoding="utf-8")
    ledger.write_text("tx_id,block,index,header_type,validation_code,env_bytes,channel_id,ch_ts_ms\n"
                      f"a,10,0,3,0,100,ch,{ms(now - timedelta(seconds=2))}\n"
                      f"b,10,1,3,0,100,ch,{ms(now - timedelta(seconds=1))}\n", encoding="utf-8")
    return recorder, submits, ledger


def _foundation(workload, continuous_source=None):
    config = FabricConfig(max_message_count=40)
    return make_shadow_foundation(
        root=ROOT, manifest_path=ROOT / "Integration/system_integration/offline_replay_inputs.json",
        endpoints={"orderer": "fixture"}, channel_id="ch", current_config=config, config_reader=lambda: config,
        workload=workload, candidate_map={10: 10, 40: 40}, latency_high_ms=1500, latency_low_ms=1200,
        congested_max_message_count=10, consecutive_samples=1, continuous_source=continuous_source,
        fetch=lambda *_: 'consensus_etcdraft_is_leader{channel="ch"} 1\n')


def test_factory_wires_client_latency_so_reactive_can_decide(tmp_path):
    """D2: without a client-telemetry source the reactive arm abstains on every frame."""
    now = datetime.now(timezone.utc).replace(microsecond=0)
    source = StreamingWorkload("fixture")
    paths = _write_client_sources(tmp_path, now, latency_ms=2000)
    continuous = ContinuousCaliperLedgerAdapter(recorder_csv=paths[0], submits_csv=paths[1], ledger_csv=paths[2],
                                                channel_id="ch", run_id="audit", window_s=5)
    provider, runner = _foundation(source, continuous)
    frame = provider.collect(run_id="live-foundation", decision_index=0, observation_timestamp_utc=now.isoformat())
    assert frame.state.observed_latency_ms == 2000
    reactive = {r["decision"]["controller"]: r["decision"] for r in runner.run([frame])["records"]}["reactive"]
    assert reactive["outcome"] == "propose" and reactive["reason"] == "congestion_threshold_crossed"
    assert reactive["config"]["max_message_count"] == 10
    # Default (no client source) keeps the documented behaviour: reactive abstains.
    provider, runner = _foundation(StreamingWorkload("fixture"))
    frame = provider.collect(run_id="live-foundation", decision_index=0, observation_timestamp_utc=now.isoformat())
    reactive = {r["decision"]["controller"]: r["decision"] for r in runner.run([frame])["records"]}["reactive"]
    assert reactive["outcome"] == "abstain" and reactive["reason"] == "telemetry_missing_fail_safe"


def test_hfbtp_receives_the_streamed_forecast_in_tx_per_s():
    values = [10 + (i % 7) for i in range(900)]  # mean 13 tx/s, inside HFBTP's [10, 200] domain
    source = StreamingWorkload("fixture")
    for i, value in enumerate(values):
        source.add(stamps(DAY1, 1, i)[0], value, collected_at_utc=stamps(DAY1, 1, i + 1)[0])
    provider, runner = _foundation(source)
    frame = provider.collect(run_id="live-foundation", decision_index=0, observation_timestamp_utc=stamps(DAY1, 1, 900)[0])
    predictive = {r["decision"]["controller"]: r["decision"] for r in runner.run([frame])["records"]}["predictive"]
    expected = closed_form_state(values, 899)
    assert predictive["predicted_arrival_rate_tps"] == pytest.approx(expected, abs=1e-10)
    for row in predictive["metadata"]["candidate_utilities"]:
        features = row["prediction_metadata"]["feature_values"]
        assert features["Set Transaction Arrival Rate"] == pytest.approx(expected, abs=1e-10)
        assert features["Block Size"] == row["config"]["max_message_count"]  # identity map supplied by the caller
        assert features["Orderers"] == 3
