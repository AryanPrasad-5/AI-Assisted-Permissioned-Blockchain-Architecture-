"""Offline tests: exact counts, millisecond preservation, pre-roll separation, zero-arrival intervals, bursts, determinism."""

import numpy as np
import pytest

from p4_replay import offsets as of
from p4_replay.tests.conftest import LO, START_S, make_archive, synthetic_day

from fabric_perf.replay.common import IntegrityError


def test_exact_transaction_count_maps_one_to_one(day, window):
    in_window = int(((day >= LO) & (day < LO + 600_000)).sum())
    assert window.measured_ms.size == in_window == len(window.measured_src_index)
    in_pre = int(((day >= LO - 60_000) & (day < LO)).sum())
    assert window.pre_ms.size == in_pre == len(window.pre_src_index)
    assert in_window > 0 and in_pre > 0


def test_millisecond_offsets_are_preserved_exactly(day, window):
    assert np.array_equal(window.measured_ms, day[window.measured_src_index] - LO)
    assert np.array_equal(window.pre_ms_rel, day[window.pre_src_index] - LO)
    assert window.measured_ms.dtype == np.int64


def test_window_boundaries_are_half_open(day, window):
    assert 0 in window.measured_ms                                    # an arrival exactly at the window start is measured
    assert window.measured_ms.max() == 599_999                        # the last arrival inside the window
    assert 600_000 not in window.measured_ms                          # an arrival exactly at the end belongs to the next window
    assert window.pre_ms_rel.min() == -60_000                         # exactly 60 s before is pre-roll
    assert window.pre_ms_rel.max() == -1                              # the last millisecond before the window is pre-roll
    assert -60_001 not in window.pre_ms_rel                           # one ms earlier is outside


def test_preroll_is_separate_and_unmeasured(window):
    assert window.pre_ms.min() >= 0 and window.pre_ms.max() < 60_000   # pre-roll round has its own clock origin
    assert set(window.pre_src_index).isdisjoint(set(window.measured_src_index))
    assert window.pre_src_index[-1] + 1 == window.measured_src_index[0]   # contiguous in the source file: nothing between or lost
    rs = of.rounds(window)
    assert [r["name"] for r in rs] == ["pre_roll", "measured"] and [r["measured"] for r in rs] == [False, True]
    assert [r["round"] for r in rs] == [0, 1]


def test_measured_window_is_exactly_600_seconds(window):
    assert window.length_s == 600 and window.length_ms == 600_000
    with pytest.raises(of.ScheduleError):
        of.validate_offsets([0, 600_000], window.length_ms)           # an offset at 600,000 ms would be outside [0, 600 s)
    with pytest.raises(ValueError):
        of.build_window(synthetic_day(), "T", "2026-10-12", 86_400 - 100, 600, 60)   # never clipped


def test_zero_arrival_interval_is_preserved(window):
    counts = of.second_count_profile(window.measured_ms, 600)
    assert counts[100:200].sum() == 0                                  # seconds 100-199 stay empty
    body = of.schedule_bytes(window.measured_ms).decode("ascii").split("\n")
    assert not any(100_000 <= int(x) < 200_000 for x in body)          # and the dispatch file has no offset inside them


def test_burst_structure_and_ties_are_preserved(day, window):
    counts = of.second_count_profile(window.measured_ms, 600)
    assert counts.max() == counts[5] >= 50                             # the 50-arrival burst survives as a burst
    source_ties = int((np.diff(day[window.measured_src_index]) == 0).sum())
    assert source_ties > 0
    assert int((np.diff(window.measured_ms) == 0).sum()) == source_ties
    body = of.schedule_bytes(window.measured_ms).decode("ascii").split("\n")
    assert len(body) == window.measured_ms.size                        # one line per arrival: ties are repeated lines, never merged
    assert int(np.sum(np.diff(window.measured_ms) <= 5)) > 0           # the sub-5 ms structure is still there


def test_one_second_counts_are_never_used_to_dispatch(window):
    # the schedule has exactly the source's distinct sub-second offsets, not n/second evenly spread
    offs = window.measured_ms
    burst = offs[(offs >= 5000) & (offs < 6000)]
    assert len(np.unique(burst)) < len(burst)                          # a uniform spread would have no ties
    assert not np.array_equal(burst, np.linspace(5000, 5999, len(burst)).astype(np.int64))


def test_schedule_bytes_are_lf_only_without_trailing_newline(window):
    body = of.schedule_bytes(window.measured_ms)
    assert b"\r" not in body and not body.endswith(b"\n")
    assert [int(x) for x in body.decode("ascii").split("\n")] == window.measured_ms.tolist()
    legacy = of.schedule_bytes(window.measured_ms, b"\r\n")
    assert legacy != body and legacy.replace(b"\r\n", b"\n") == body    # the legacy CRLF form differs only by the separator


def test_output_is_deterministic(tmp_path, day):
    a = of.build_window(day, "T", "2026-10-12", START_S, 600, 60)
    b = of.build_window(day.copy(), "T", "2026-10-12", START_S, 600, 60)
    ia, ib = of.export_schedule_files(a, tmp_path / "a"), of.export_schedule_files(b, tmp_path / "b")
    assert ia == ib
    assert (tmp_path / "a" / "schedule_pre_roll.txt").read_bytes() == (tmp_path / "b" / "schedule_pre_roll.txt").read_bytes()
    assert ia["pre_roll"]["file"] == "schedule_pre_roll.txt" and ia["measured"]["file"] == "schedule_measured.txt"
    with pytest.raises(FileExistsError):
        of.export_schedule_files(a, tmp_path / "a")                    # never overwrites


@pytest.mark.parametrize("bad", [[], [[1, 2]], [-1, 5], [5, 4], [1.5, 2.0], [0, 600_000]])
def test_validate_offsets_rejects(bad):
    with pytest.raises(of.ScheduleError):
        of.validate_offsets(bad, 600_000)


def test_validate_offsets_keeps_ties():
    out = of.validate_offsets([0, 0, 5, 5, 5, 599_999], 600_000)
    assert out.tolist() == [0, 0, 5, 5, 5, 599_999]


def test_empty_preroll_is_refused(day):
    sparse = day[(day < LO - 60_000) | (day >= LO)]                    # remove every pre-roll arrival
    w = of.build_window(sparse, "T", "2026-10-12", START_S, 600, 60)
    with pytest.raises(of.ScheduleError):
        of.pre_roll_schedule(w)


def test_tx_keys_follow_the_workload_rule_and_are_unique():
    assert of.tx_key("rp01", 1, 42) == "rp01_1_0000042"
    assert of.tx_key("rp01", 0, 42) != of.tx_key("rp01", 1, 42)        # the round index separates pre-roll from measured
    for bad in (("bad id", 1, 1), ("rp01", -1, 1), ("rp01", 1, 10_000_000), ("", 0, 0)):
        with pytest.raises(of.ScheduleError):
            of.tx_key(*bad)


def test_tx_map_reconciles_every_transaction(day, window):
    rows = of.build_tx_map("rp01", window, day_start_ms=1_791_763_200_000)
    assert len(rows) == window.pre_ms.size + window.measured_ms.size
    assert len({r["create_asset_key"] for r in rows}) == len(rows)
    measured = [r for r in rows if r["round"] == "measured"]
    assert [r["seq"] for r in measured] == list(range(len(measured)))
    assert [r["intended_offset_ms"] for r in measured] == window.measured_ms.tolist()
    assert [r["source_row_index"] for r in measured] == window.measured_src_index.tolist()
    pre = [r for r in rows if r["round"] == "pre_roll"]
    assert [r["window_offset_ms"] for r in pre] == window.pre_ms_rel.tolist()      # negative: before the window
    assert [r["intended_offset_ms"] for r in pre] == window.pre_ms.tolist()        # round-relative: starts the pre-roll clock


def test_identifiers_from_a_verified_archive(tmp_path, day, window):
    entry = make_archive(tmp_path / "2026-10-12.csv.zip", day)
    a, c = int(window.pre_src_index[0]), int(window.measured_src_index[-1]) + 1
    ids = of.load_archive_identifiers(tmp_path / "2026-10-12.csv.zip", entry, a, c)
    rows = of.build_tx_map("rp01", window, day_start_ms=1_791_763_200_000, identifiers=ids)
    assert all(r["source_hash"].startswith("0x") for r in rows)
    assert [r["source_hash"] for r in rows if r["round"] == "measured"] == [f"0x{int(i):064x}" for i in window.measured_src_index]
    # the archive timestamps agree with the trace-derived ones (checked inside build_tx_map); tamper with the timestamps to prove it
    bad = {"hash": ids["hash"], "timestamp_ms": {k: v + 1 for k, v in ids["timestamp_ms"].items()}}
    with pytest.raises(IntegrityError):
        of.build_tx_map("rp01", window, day_start_ms=1_791_763_200_000, identifiers=bad)


def test_archive_that_does_not_match_its_manifest_is_refused(tmp_path, day):
    entry = make_archive(tmp_path / "x.csv.zip", day)
    entry["sha256"] = "0" * 64
    with pytest.raises(IntegrityError):
        of.load_archive_identifiers(tmp_path / "x.csv.zip", entry, 0, 3)
