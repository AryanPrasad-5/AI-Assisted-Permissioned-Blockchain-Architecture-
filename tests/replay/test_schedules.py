"""Schedule builders: Approach A is exact; Approach B uses a shared (coupled) unit-rate stream (synthetic data only)."""

from __future__ import annotations

import numpy as np
import pytest

from fabric_perf.replay import schedules as s
from fabric_perf.replay.windows import Window


def _window(offsets, length_s=60):
    return Window("d", 100, length_s, 60, np.asarray(offsets, dtype=np.int64), np.empty(0, dtype=np.int64))


def test_observed_schedule_is_exact_ties_kept_and_independent_copy():
    win = _window([0, 0, 5, 5, 5, 1999, 59_999])
    schedule = s.observed_schedule(win)
    assert schedule.tolist() == [0, 0, 5, 5, 5, 1999, 59_999]
    schedule[0] = 123
    assert win.offsets_ms[0] == 0


def test_unit_stream_is_prefix_consistent_which_is_what_makes_it_a_common_random_number_stream():
    short = s.unit_stream(11, 100)
    long = s.unit_stream(11, 50_000)
    assert np.array_equal(short[:50], long[:50])
    assert long[-1] > 50_000 and not np.array_equal(s.unit_stream(12, 100)[:50], short[:50])


def test_same_seed_same_schedule_and_different_seed_different_schedule():
    rates = np.array([5.0, 10.0, 0.0, 20.0])
    a = s.poisson_schedule(rates, 10, seed=3)
    assert np.array_equal(a, s.poisson_schedule(rates, 10, seed=3))
    assert not np.array_equal(a, s.poisson_schedule(rates, 10, seed=4))


def test_zero_rate_intervals_receive_no_arrivals_and_nothing_is_floored():
    rates = np.array([0.0, 10.0, 0.0, 20.0, 0.0])
    schedule = s.poisson_schedule(rates, 10, seed=5)
    bins = schedule // 10_000
    assert set(bins.tolist()) <= {1, 3}
    assert len(s.poisson_schedule(np.zeros(4), 10, seed=5)) == 0


def test_arrivals_are_ordered_in_bounds_and_cover_the_horizon_only():
    rates = np.full(6, 50.0)
    schedule = s.poisson_schedule(rates, 10, seed=1)
    assert (np.diff(schedule) >= 0).all() and schedule.min() >= 0 and schedule.max() < 60_000
    assert schedule.dtype == np.int64


def test_volume_is_close_to_the_integrated_intensity():
    rates = np.full(100, 30.0)  # expected 100 * 10 * 30 = 30,000
    n = len(s.poisson_schedule(rates, 10, seed=2))
    assert abs(n - 30_000) <= 5 * np.sqrt(30_000)


def test_dense_arrivals_collide_on_the_same_millisecond_and_are_not_deduplicated():
    rates = np.full(2, 5000.0)
    schedule = s.poisson_schedule(rates, 5, seed=9)
    total = 5000.0 * 10
    points = s.unit_stream(9, total)
    assert len(schedule) == int((points < total).sum())  # every Poisson point survives the ms rounding
    assert int((np.diff(schedule) == 0).sum()) > 0


def test_common_random_numbers_couple_arms_monotonically_in_intensity():
    base = np.array([5.0, 12.0, 3.0, 20.0])
    low = s.poisson_schedule(base, 15, seed=21)
    high = s.poisson_schedule(base * 1.5, 15, seed=21)
    assert len(high) >= len(low)
    assert (high[: len(low)] <= low).all()  # the k-th arrival never comes later under higher intensity


def test_time_change_places_a_constant_rate_stream_correctly():
    schedule = s.poisson_schedule(np.array([10.0]), 100, seed=6)
    points = s.unit_stream(6, 1000.0)
    expected = np.floor(points[points < 1000.0] / 10.0 * 1000).astype(np.int64)  # t = T / rate
    assert np.array_equal(schedule, expected)


@pytest.mark.parametrize("bad", [np.array([]), np.array([1.0, -1.0]), np.array([1.0, np.nan]), np.array([[1.0, 2.0]])])
def test_invalid_rates_are_rejected(bad):
    with pytest.raises(ValueError):
        s.poisson_schedule(bad, 10, seed=1)
    with pytest.raises(ValueError):
        s.poisson_schedule(np.array([1.0]), 0, seed=1)


def test_interval_rates_are_the_ex_post_interval_means():
    offsets = np.concatenate([np.full(30, 500), np.full(10, 20_500), np.full(20, 40_500)])
    assert s.interval_rates_from_counts(offsets, 60, 20).tolist() == [1.5, 0.5, 1.0]
    with pytest.raises(ValueError):
        s.interval_rates_from_counts(offsets, 60, 7)


def test_replay_rate_file_format_and_guards(tmp_path):
    path = tmp_path / "replay_rate.txt"
    s.write_replay_rate(np.array([0, 0, 7, 120]), path)
    assert path.read_text(encoding="utf-8").split("\n") == ["0", "0", "7", "120"]
    with pytest.raises(FileExistsError):
        s.write_replay_rate(np.array([0]), path)
    with pytest.raises(ValueError):
        s.write_replay_rate(np.array([5, 3]), tmp_path / "bad.txt")
    with pytest.raises(ValueError):
        s.write_replay_rate(np.array([], dtype=np.int64), tmp_path / "empty.txt")
