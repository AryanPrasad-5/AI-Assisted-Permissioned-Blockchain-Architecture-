"""Window extraction, exposure statistics, tiling and the seeded selection rule (synthetic data only)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from fabric_perf.replay import windows as w


def test_window_boundaries_preroll_and_ties():
    day = np.array([39_999, 40_000, 99_999, 100_000, 100_000, 699_999, 700_000], dtype=np.int64)
    win = w.extract_window(day, "2026-09-21", start_s=100, length_s=600, pre_roll_s=60)
    assert win.offsets_ms.tolist() == [0, 0, 599_999]  # lo inclusive, hi exclusive, tie kept
    assert win.pre_offsets_ms.tolist() == [-60_000, -1]  # [lo - pre_roll, lo)
    assert win.offsets_ms.dtype == np.int64


def test_windows_are_never_clipped_or_fractional():
    day = np.arange(10, dtype=np.int64)
    with pytest.raises(ValueError, match="never clipped"):
        w.extract_window(day, "d", start_s=30, length_s=600, pre_roll_s=60)  # pre-roll before midnight
    with pytest.raises(ValueError, match="never clipped"):
        w.extract_window(day, "d", start_s=86_400 - 599, length_s=600)  # runs past midnight
    with pytest.raises(ValueError, match="whole seconds"):
        w.extract_window(day, "d", start_s=100.5, length_s=600)
    with pytest.raises(ValueError, match="non-decreasing"):
        w.extract_window(np.array([5, 3], dtype=np.int64), "d", start_s=100, length_s=600)


def test_burst_statistics_and_the_40_boundary():
    offsets = np.concatenate([np.arange(50) * 10, 1000 + np.arange(5) * 100, 2000 + np.arange(40) * 20])
    offsets = np.sort(np.concatenate([offsets, [0]]))  # one exact tie at 0 ms -> second 0 holds 51
    stats = w.burst_stats(offsets, length_s=10)
    counts = w.second_counts(offsets, 10)
    assert counts.tolist() == [51, 5, 40, 0, 0, 0, 0, 0, 0, 0]
    assert stats["n_tx"] == 96 and stats["max_1s"] == 51
    assert stats["seconds_share_gt40"] == pytest.approx(0.1)  # only the 51; exactly 40 is not above 40
    assert stats["seconds_share_ge40"] == pytest.approx(0.2)
    assert stats["tx_share_in_seconds_gt40"] == pytest.approx(51 / 96)
    assert stats["seconds_share_lt10"] == pytest.approx(0.8)
    assert stats["tx_share_in_seconds_lt10"] == pytest.approx(5 / 96)
    assert stats["zero_gap_share"] == pytest.approx(1 / 95)


def test_tiles_are_non_overlapping_and_respect_the_preroll():
    per_second = np.repeat(np.arange(86_400, dtype=np.int64) * 1000 + 500, 10)
    table = w.tile_table(per_second, "D", length_s=600, pre_roll_s=60)
    assert table["start_s"].tolist() == list(range(600, 43_200 - 600 + 1, 600))  # tile at 0 dropped: no room for pre-roll
    assert (table["n_tx"] == 6000).all() and (table["mean_rate"] == 10).all()
    assert (table["seconds_share_gt40"] == 0).all()
    assert w.tile_table(per_second, "F")["start_s"].iloc[0] == 600
    assert w.tile_table(per_second, "V")["start_s"].iloc[0] == 43_200


def _table(n=24):
    return pd.DataFrame({
        "start_s": np.arange(n) * 600,
        "mean_rate": np.linspace(5, 20, n),
        "tx_share_in_seconds_gt40": [(i % 6) / 20 for i in range(n)],
    })


def test_selection_rule_is_applied_and_reproducible():
    table = _table()
    row, eligible = w.select_window(table, seed=w.WINDOW_SEED)
    q_lo, q_hi = table["mean_rate"].quantile([0.25, 0.75])
    cap = table["tx_share_in_seconds_gt40"].quantile(0.75)
    assert q_lo <= row["mean_rate"] <= q_hi and row["tx_share_in_seconds_gt40"] <= cap
    assert eligible > 1
    again, _ = w.select_window(table, seed=w.WINDOW_SEED)
    assert again["start_s"] == row["start_s"]
    picks = {int(w.select_window(table, seed=s)[0]["start_s"]) for s in range(30)}
    assert len(picks) > 1  # the seed matters


def test_selection_fails_loudly_on_an_empty_table():
    with pytest.raises(ValueError):
        w.select_window(_table(0))


def test_window_seed_and_pools_are_the_registered_values():
    assert w.WINDOW_SEED == 20261114
    assert w.POOLS["D"] == ("2026-09-21", 0, 43_200)
    assert w.POOLS["V"] == ("2026-09-21", 43_200, 86_400)
    assert w.POOLS["F"] == ("2026-10-12", 0, 86_400)
