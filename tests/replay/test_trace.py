"""Synthetic-fixture tests for the ms trace builder and its reconciliation. No raw data."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import numpy as np
import pytest

from fabric_perf.replay import trace
from fabric_perf.replay.common import IntegrityError

from .helpers import DATE, DAY_START_MS, make_zip, manifest_entry, pandas_audit, synthetic_offsets, write_series


@pytest.fixture()
def day(tmp_path):
    offsets = synthetic_offsets()
    zip_path = make_zip(tmp_path / f"{DATE}.csv.zip", offsets)
    return offsets, zip_path, manifest_entry(zip_path, offsets)


def test_hash_mismatch_is_an_integrity_error(day):
    _, zip_path, entry = day
    entry["sha256"] = "0" * 64
    with pytest.raises(IntegrityError, match="SHA-256"):
        trace.build_day_trace(zip_path, entry)


def test_size_mismatch_is_an_integrity_error(day):
    _, zip_path, entry = day
    entry["compressed_bytes"] += 1
    with pytest.raises(IntegrityError, match="size"):
        trace.build_day_trace(zip_path, entry)


def test_row_count_mismatch_is_an_integrity_error(day):
    _, zip_path, entry = day
    entry["rows"] += 1
    with pytest.raises(IntegrityError, match="rows"):
        trace.build_day_trace(zip_path, entry)


def test_first_last_timestamp_must_match_manifest(day):
    _, zip_path, entry = day
    entry["utc_end"] = "2026-09-21T23:59:59.999Z"
    with pytest.raises(IntegrityError, match="utc_start/utc_end"):
        trace.build_day_trace(zip_path, entry)


def test_two_csv_entries_are_rejected(tmp_path):
    offsets = synthetic_offsets(n=50)
    zip_path = make_zip(tmp_path / "a.csv.zip", offsets, extra_entry="other.csv")
    with pytest.raises(IntegrityError, match="exactly one CSV"):
        trace.read_timestamps_ms(zip_path)


def test_non_integer_timestamp_is_rejected(tmp_path):
    zip_path = make_zip(tmp_path / "a.csv.zip", [0, 1], timestamp_text=[str(DAY_START_MS + 5), str(DAY_START_MS + 6) + ".5"])
    with pytest.raises(IntegrityError, match="non-integer"):
        trace.read_timestamps_ms(zip_path)


def test_missing_or_unparseable_timestamp_is_rejected(tmp_path):
    zip_path = make_zip(tmp_path / "a.csv.zip", [0, 1], timestamp_text=[str(DAY_START_MS + 5), ""])
    with pytest.raises(IntegrityError):
        trace.read_timestamps_ms(zip_path)
    bad = make_zip(tmp_path / "b.csv.zip", [0, 1], timestamp_text=[str(DAY_START_MS + 5), "not-a-number"])
    with pytest.raises(IntegrityError):
        trace.read_timestamps_ms(bad)


def test_timestamps_outside_the_day_are_rejected(tmp_path):
    offsets = np.array([10, 20, 86_400_000], dtype=np.int64)  # last one belongs to the next day
    zip_path = make_zip(tmp_path / "a.csv.zip", offsets)
    with pytest.raises(IntegrityError, match="outside the UTC day"):
        trace.build_day_trace(zip_path, manifest_entry(zip_path, offsets))


def test_chronological_decrease_is_refused_not_repaired(tmp_path):
    offsets = np.array([10, 30, 20, 40], dtype=np.int64)
    zip_path = make_zip(tmp_path / "a.csv.zip", offsets)
    with pytest.raises(IntegrityError, match="re-sort"):
        trace.build_day_trace(zip_path, manifest_entry(zip_path, np.sort(offsets)))


def test_offsets_are_exact_ms_and_ties_are_kept(day):
    offsets, zip_path, entry = day
    built = trace.build_day_trace(zip_path, entry)
    assert built.day_start_ms == DAY_START_MS
    assert built.offsets_ms.dtype == np.int64
    assert np.array_equal(built.offsets_ms, offsets)  # no smoothing, clipping or re-ordering
    assert len(built.offsets_ms) == len(offsets)  # nothing deduplicated
    assert int((np.diff(built.offsets_ms) == 0).sum()) == int((np.diff(offsets) == 0).sum()) > 0


def _recon(tmp_path, offsets, built, **audit_overrides):
    series_dir = tmp_path / "series"
    series_dir.mkdir(exist_ok=True)
    write_series(series_dir, offsets)
    audit, quality = pandas_audit(offsets, DATE, built.source_sha256, built.rows)
    for key, value in audit_overrides.items():
        section, field = key.split("__")
        audit["archives"][0][section][field] = value
    return trace.reconcile_day(built, audit, quality, series_dir), series_dir, audit, quality


def test_reconciliation_passes_when_everything_agrees(tmp_path, day, monkeypatch):
    offsets, zip_path, entry = day
    built = trace.build_day_trace(zip_path, entry)
    monkeypatch.setitem(trace.REGISTERED_TIE_GAPS, DATE, int((np.diff(offsets) == 0).sum()))
    report, *_ = _recon(tmp_path, offsets, built)
    assert report.ok, report.render()
    assert len(report.checks) >= 25


def test_registered_tie_count_is_enforced_independently_of_the_audit(tmp_path, day, monkeypatch):
    offsets, zip_path, entry = day
    built = trace.build_day_trace(zip_path, entry)
    monkeypatch.setitem(trace.REGISTERED_TIE_GAPS, DATE, int((np.diff(offsets) == 0).sum()) + 1)
    report, *_ = _recon(tmp_path, offsets, built)
    assert [c.name for c in report.failures] == ["exact_tie_gaps_vs_registered"]


def test_the_real_registered_tie_counts_are_the_owner_values():
    assert trace.REGISTERED_TIE_GAPS == {"2026-09-21": 46_198, "2026-10-12": 27_802}


def test_a_single_wrong_stored_second_is_detected(tmp_path, day, monkeypatch):
    import pandas as pd

    offsets, zip_path, entry = day
    built = trace.build_day_trace(zip_path, entry)
    monkeypatch.setitem(trace.REGISTERED_TIE_GAPS, DATE, int((np.diff(offsets) == 0).sum()))
    report, series_dir, audit, quality = _recon(tmp_path, offsets, built)
    path = series_dir / f"series_{DATE}_1s.csv.gz"
    frame = pd.read_csv(path)
    frame.loc[1234, "arrival_count"] += 1
    frame.to_csv(path, index=False)
    broken = trace.reconcile_day(built, audit, quality, series_dir)
    assert not broken.ok
    assert any(c.name == "series_1s_bins_differing_from_stored" and c.observed == 1 for c in broken.failures)


@pytest.mark.parametrize("override,expected_failure", [
    ("interarrival_ms__median", "interarrival_median_ms"),
    ("interarrival_ms__p95", "interarrival_p95_ms"),
    ("interarrival_ms__max", "interarrival_max_ms"),
    ("tps_1s__mean", "tps_1s_mean_nonempty"),
    ("tps_1s__max", "tps_1s_max"),
    ("tps_10s__p95", "tps_10s_p95"),
])
def test_each_audit_statistic_is_actually_compared(tmp_path, day, monkeypatch, override, expected_failure):
    offsets, zip_path, entry = day
    built = trace.build_day_trace(zip_path, entry)
    monkeypatch.setitem(trace.REGISTERED_TIE_GAPS, DATE, int((np.diff(offsets) == 0).sum()))
    series_dir = tmp_path / "series"
    series_dir.mkdir()
    write_series(series_dir, offsets)
    audit, quality = pandas_audit(offsets, DATE, built.source_sha256, built.rows)
    section, field = override.split("__")
    audit["archives"][0][section][field] = audit["archives"][0][section][field] + 1
    report = trace.reconcile_day(built, audit, quality, series_dir)
    assert expected_failure in [c.name for c in report.failures]


def test_stored_series_with_a_wrong_grid_is_rejected(tmp_path, day):
    import pandas as pd

    offsets, zip_path, entry = day
    series_dir = tmp_path / "series"
    series_dir.mkdir()
    write_series(series_dir, offsets)
    path = series_dir / f"series_{DATE}_1s.csv.gz"
    frame = pd.read_csv(path)
    frame.loc[0, "timestamp_utc"] = "2026-09-21 00:00:05+00:00"
    frame.to_csv(path, index=False)
    with pytest.raises(IntegrityError, match="grid"):
        trace.read_series(series_dir, DATE, 1)


def test_nothing_is_written_unless_reconciliation_passed(tmp_path, day, monkeypatch):
    offsets, zip_path, entry = day
    built = trace.build_day_trace(zip_path, entry)
    monkeypatch.setitem(trace.REGISTERED_TIE_GAPS, DATE, 1)  # wrong on purpose
    report, *_ = _recon(tmp_path, offsets, built)
    out = tmp_path / "out"
    with pytest.raises(IntegrityError, match="did not pass"):
        trace.write_traces([built], [report], out, [])
    assert not out.exists()


def test_written_trace_round_trips_and_detects_tampering(tmp_path, day, monkeypatch):
    offsets, zip_path, entry = day
    built = trace.build_day_trace(zip_path, entry)
    monkeypatch.setitem(trace.REGISTERED_TIE_GAPS, DATE, int((np.diff(offsets) == 0).sum()))
    report, *_ = _recon(tmp_path, offsets, built)
    out = tmp_path / "out"
    summary = trace.write_traces([built], [report], out, [Path(trace.__file__)])
    assert summary["days"][DATE]["exact_tie_gaps"] == int((np.diff(offsets) == 0).sum())
    assert np.array_equal(trace.load_day_offsets(out, DATE), offsets)
    trace.write_traces([built], [report], out, [])  # identical content: idempotent
    manifest = json.loads((out / "flashbots_ms_manifest.json").read_text(encoding="utf-8"))
    assert manifest["transform"].startswith("timestamp_ms - UTC day start")
    tampered = offsets.copy()
    tampered[0] += 1
    np.save(out / f"arrivals_{DATE}_ms.npy", tampered)
    with pytest.raises(IntegrityError, match="SHA-256 differs"):
        trace.load_day_offsets(out, DATE)
    with pytest.raises(IntegrityError, match="refusing to overwrite"):
        trace.write_traces([built], [report], out, [])


def test_replay_package_never_imports_process_or_network_modules():
    forbidden = {"subprocess", "socket", "requests", "urllib", "http", "docker", "asyncio", "multiprocessing", "ctypes"}
    root = Path(trace.__file__).parent
    for source in root.glob("*.py"):
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module.split(".")[0]]
            assert not forbidden.intersection(names), f"{source.name} imports {forbidden.intersection(names)}"
