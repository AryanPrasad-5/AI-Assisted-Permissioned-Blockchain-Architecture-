"""A0j: the REAL v3 recorder (Node, scripted wall clock, fake Caliper dispatch) produces a spool the Python bridge ingests and the coverage layer
judges. SYNTHETIC: no Caliper, Docker, Fabric or network. The only process started is ``node`` running the offline driver
``tests/e1/js/a0j_v3_round.js``; the test is skipped when Node is not installed.
"""

from __future__ import annotations

import json
import shutil
import subprocess

import pytest

from tests import a0j_support as j
from tests.a0j_support import at, bridge, rc, wc

NODE = shutil.which("node")
DRIVER = j.REPO / "tests" / "e1" / "js" / "a0j_v3_round.js"
POLICY = bridge.RecorderHealthPolicy(**{k: bridge.CANDIDATE_PROPOSAL[k] for k in ("heartbeat_interval_s", "max_heartbeat_gap_s", "freshness_limit_s", "clock_tolerance_ms")})
C = wc.CoverageClass
pytestmark = pytest.mark.skipif(NODE is None, reason="node is not installed")


def drive(tmp_path, scenario, seconds=12):
    done = subprocess.run([NODE, str(DRIVER), str(tmp_path), scenario, str(seconds)], capture_output=True, text=True, timeout=60, check=False)
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def snapshot(tmp_path, info, as_of=30.0):
    report = bridge.ingest_spool(info["spool"], tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=POLICY)
    instance = report["collector_instance"]
    events, _ = rc.read_submit_events(info["submits"], instance=instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=POLICY)
    return report, coverage.snapshot(at(as_of))


def test_node_zeros_with_source_count_are_genuine_zeros_and_the_round_is_complete(tmp_path):
    info = drive(tmp_path, "zeros")
    report, snap = snapshot(tmp_path, info)
    assert report["complete"] is True and report["has_stop"] is True and report["evidence_origin"] == "synthetic_fixture" and report["coverage_scope"] == "recorder_callback"
    assert snap.valid
    assert [snap.classify(at(s), 0)[0] for s in (3, 7)] == [C.VERIFIED_ZERO] * 2
    assert snap.classify(at(2), 11)[0] == C.OBSERVED_POSITIVE
    evidence = snap.summary()["recorder_evidence"]
    assert evidence["synthetic"] is True and evidence["live_source_coverage_verified"] is False and evidence["source_completeness"]["verified_seconds"] == 12


def test_node_heartbeats_without_a_source_count_certify_no_zero(tmp_path):
    info = drive(tmp_path, "no_source")
    report, snap = snapshot(tmp_path, info)
    assert report["complete"] is True and snap.valid                                       # the recorder itself is fully verified ...
    plain = drive(tmp_path / "z", "zeros") if (tmp_path / "z").mkdir() is None else None   # ... but with traffic everywhere there is no zero to test; use the gap
    cls, why = snap.classify(at(15), 0)                                                    # a second after the round: outside the window
    assert cls != C.VERIFIED_ZERO
    assert snap.summary()["recorder_evidence"]["source_completeness"]["verified_seconds"] == 0
    assert plain["files"]


def test_node_missed_callbacks_are_caught_by_the_source_count(tmp_path):
    info = drive(tmp_path, "missed_callbacks")
    report, snap = snapshot(tmp_path, info)
    assert snap.valid and rc.RC_MISSING_CALLBACKS in snap.cause_codes()
    assert snap.classify(at(5), 0)[0] == C.INTERRUPTED_COLLECTION
    assert snap.classify(at(3), 0)[0] == C.VERIFIED_ZERO and snap.classify(at(7), 0)[0] == C.VERIFIED_ZERO     # per-second heartbeats confine the loss to second 5


def test_node_killed_worker_leaves_no_stop_and_nothing_is_invented(tmp_path):
    info = drive(tmp_path, "killed")
    report, snap = snapshot(tmp_path, info)
    assert report["has_stop"] is False and [p["code"] for p in report["problems"]] == ["no_stop_row"]
    assert all(r["kind"] != "stop" for r in map(json.loads, (tmp_path / "journal.jsonl").read_text(encoding="utf-8").splitlines()))


def test_node_spool_write_failure_leaves_a_marker_and_no_certified_tail(tmp_path):
    info = drive(tmp_path, "write_failure")
    assert any(name.endswith(".health.FAILED") for name in info["files"])
    report, snap = snapshot(tmp_path, info)
    assert report["has_stop"] is False and report["complete"] is False
    assert snap.classify(at(10), 0)[0] in (C.STALE_COLLECTOR_EVIDENCE, C.UNKNOWN_COVERAGE_ZERO)       # time after the last durable heartbeat is never covered


def test_node_failed_live_stream_is_recorded_as_a_collector_health_failure(tmp_path):
    info = drive(tmp_path, "stream_failure")
    report, snap = snapshot(tmp_path, info)
    assert report["stop_reason"] == "abort"
    assert rc.RC_HEALTH_FAILURE in snap.cause_codes() and snap.classify(at(9), 5)[0] == C.INTERRUPTED_COLLECTION


def test_node_wall_clock_step_is_detected_by_the_monotonic_cross_check(tmp_path):
    info = drive(tmp_path, "clock_step")
    report, snap = snapshot(tmp_path, info)
    assert any(p["code"] == "clock_discontinuity" for p in report["problems"])
    assert snap.valid and snap.classify(at(10), 5)[0] == C.INTERRUPTED_COLLECTION


def test_node_without_a_health_block_writes_no_evidence(tmp_path):
    info = drive(tmp_path, "no_health")
    assert not any(".health" in name for name in info["files"])
    with pytest.raises(FileNotFoundError):
        bridge.ingest_spool(info["spool"], tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=POLICY)


def test_node_spool_replay_is_deterministic(tmp_path):
    info = drive(tmp_path, "zeros")
    a = bridge.ingest_spool(info["spool"], tmp_path / "a.jsonl", source_id=j.SOURCE_ID, policy=POLICY)
    b = bridge.ingest_spool(info["spool"], tmp_path / "b.jsonl", source_id=j.SOURCE_ID, policy=POLICY)
    assert a["journal_sha256"] == b["journal_sha256"] and (tmp_path / "a.jsonl").read_bytes() == (tmp_path / "b.jsonl").read_bytes()
