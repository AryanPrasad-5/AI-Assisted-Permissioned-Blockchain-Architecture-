"""Shared telemetry helpers: status ordering, declared anomaly kinds, exclusive LF writers."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_telemetry_common as tc  # noqa: E402


def test_status_follows_the_worst_category():
    a = tc.Anomalies({"gone"}, {"clash"}, {"late"})
    assert a.status() == tc.COMPLETE
    a.add("late")
    assert a.status() == tc.COMPLETE_WITH_WARNINGS
    a.add("clash", "x")
    assert a.status() == tc.INCONSISTENT
    a.add("gone", "y")
    assert a.status() == tc.INCOMPLETE
    assert a.counts() == {"error": {"clash": 1, "gone": 1}, "warning": {"late": 1}}
    assert [r["category"] for r in a.rows()] == ["warning", "consistency", "completeness"]


def test_undeclared_anomaly_kind_raises():
    with pytest.raises(ValueError, match="undeclared"):
        tc.Anomalies({"gone"}, set(), set()).add("typo")


def test_worst_ordering():
    assert tc.worst([]) == tc.COMPLETE
    assert tc.worst([tc.COMPLETE, tc.COMPLETE_WITH_WARNINGS]) == tc.COMPLETE_WITH_WARNINGS
    assert tc.worst([tc.INCONSISTENT, tc.COMPLETE]) == tc.INCONSISTENT
    assert tc.worst([tc.INCONSISTENT, tc.INCOMPLETE]) == tc.INCOMPLETE


def test_csv_writer_refuses_overwrite_and_never_drops_a_field(tmp_path):
    p = tmp_path / "a.csv"
    sha = tc.write_csv_x(p, ["x", "y"], [{"x": 1, "y": None}, {"x": True, "y": [1, 2]}])
    assert p.read_bytes() == b"x,y\n1,\ntrue,1;2\n" and sha == tc.sha256_file(p)
    with pytest.raises(FileExistsError):
        tc.write_csv_x(p, ["x"], [])
    with pytest.raises(ValueError, match="not in the column list"):
        tc.write_csv_x(tmp_path / "b.csv", ["x"], [{"x": 1, "z": 2}])
    assert not (tmp_path / "b.csv").exists()
    tc.write_json_x(tmp_path / "c.json", {"k": 1})
    with pytest.raises(FileExistsError):
        tc.write_json_x(tmp_path / "c.json", {})
    with pytest.raises(FileExistsError):
        tc.write_bytes_x(tmp_path / "c.json", b"")


def test_utc_iso_keeps_nanoseconds():
    assert tc.utc_iso(1_700_000_000_123_456_789) == "2023-11-14T22:13:20.123456789Z"
    assert tc.utc_iso(None) == ""
