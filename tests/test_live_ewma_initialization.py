"""Deterministic causal reconstruction against the frozen daily benchmark."""
from datetime import datetime, timedelta, timezone

import pytest

from fabric_perf.controllers.live_foundation import EWMA_PARAMETERS, StreamingEWMA, StreamingWorkload
from fabric_perf.forecasting.horizon_benchmark import forecast_series

START = datetime(2026, 1, 1, tzinfo=timezone.utc)


def stamp(i):
    return (START + timedelta(seconds=i)).isoformat()


def predict(values, *, start=0):
    return StreamingEWMA("fixture").predict_timestamped(
        [stamp(i) for i in range(start, start + len(values))], values, 60,
        now_utc=stamp(start + len(values)))


def test_uninterrupted_and_reconstructed_restart_match_frozen_daily_forecasts():
    values = (20,) * 600 + (10,) * 600
    expected = forecast_series("ewma", values, 60, 0.005)
    uninterrupted = StreamingEWMA("fixture")
    for n in (600, 601, 900, 1200):
        times = [stamp(i) for i in range(n)]
        result = uninterrupted.predict_timestamped(times, values[:n], 60, now_utc=stamp(n))
        restarted = StreamingEWMA("fixture").predict_timestamped(times, values[:n], 60, now_utc=stamp(n))
        assert result.value_tps == expected[n] == restarted.value_tps
        # 2026-11-17 (D3): the frozen EWMA parameters are now recorded with every forecast.
        assert result.initialization_provenance == {"method": "reconstructed_utc_day_prefix", "source": "fixture", "first_bin_timestamp_utc": stamp(0), "last_bin_timestamp_utc": stamp(n-1), "bin_count": n,
                                                    "ewma_parameters": EWMA_PARAMETERS}
        assert EWMA_PARAMETERS["alpha"] == 0.005 and EWMA_PARAMETERS["horizon_seconds"] == 60 and EWMA_PARAMETERS["min_history_bins"] == 600
    assert result.value_tps == pytest.approx(10.494138, abs=1e-6)


def test_restart_without_prefix_abstains_even_after_long_warmup():
    with pytest.raises(ValueError, match="UTC-day prefix"):
        predict((10,) * 3600, start=600)
    with pytest.raises(ValueError, match="600"):
        predict((10,) * 599)


@pytest.mark.parametrize("kind", ["missing", "duplicate", "out_of_order"])
def test_invalid_prefix_is_rejected(kind):
    times = [stamp(i) for i in range(601)]
    if kind == "missing": times.pop(300)
    if kind == "duplicate": times[300] = times[299]
    if kind == "out_of_order": times[300], times[301] = times[301], times[300]
    with pytest.raises(ValueError):
        StreamingEWMA("fixture").predict_timestamped(times, [10] * len(times), 60, now_utc=stamp(601))


def test_gap_abstains_until_next_midnight_warmup():
    source = StreamingWorkload("fixture")
    for i in range(600): source.add(stamp(i), 20, collected_at_utc=stamp(i+1))
    for i in range(601, 1201): source.add(stamp(i), 10, collected_at_utc=stamp(i+1))
    # Strict live gap policy (2026-11-17): the day is incomplete, so no prefix is offered at all.
    assert source.history() == ((), ())
    with pytest.raises(ValueError, match="600"):
        StreamingEWMA("fixture").predict_timestamped(*source.history(), 60, now_utc=stamp(1201))
    for i in range(86400, 87000): source.add(stamp(i), 11, collected_at_utc=stamp(i+1))
    result = StreamingEWMA("fixture").predict_timestamped(*source.history(), 60, now_utc=stamp(87000))
    assert result.value_tps == forecast_series("ewma", [11] * 600, 60, 0.005)[600]


def test_midnight_rollover_resets_even_without_gap():
    source = StreamingWorkload("fixture")
    for i in range(86399, 86402): source.add(stamp(i), 3, collected_at_utc=stamp(i+1))
    assert source.history()[0] == (stamp(86400), stamp(86401))


def test_causality_origin_and_stale_result_cleared():
    values = [12] * 600 + [100] * 60
    f = StreamingEWMA("fixture")
    result = f.predict_timestamped([stamp(i) for i in range(600)], values[:600], 60, now_utc=stamp(600))
    assert result.value_tps == forecast_series("ewma", values, 60, 0.005)[600]
    assert result.origin_timestamp_utc == stamp(600)
    assert result.history_last_timestamp_utc == stamp(599)
    assert result.as_dict()["target_end_timestamp_utc"] == stamp(660)
    with pytest.raises(ValueError):
        f.predict_timestamped([stamp(i) for i in range(601)], values[:601], 60, now_utc=stamp(600))
    assert f.last_result is None
