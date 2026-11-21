"""A0l workstream F (end-to-end, offline): the REAL JavaScript workload and issue-log writer, run under a fake Caliper driver with a scripted clock, feed the Python
reader, journal bridge, coverage contract, history and Stage 3 fixture pipeline. Failure demonstrations included. SYNTHETIC / FIXTURE only.

This file starts Node (a local interpreter running the offline driver). It never starts Caliper, Fabric, Docker or WSL and opens no network connection.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from tests import a0l_support as a
from tests.a0l_support import DAY_MS, SyntheticIssueLog, Scripted, at, ilr, wc

REPO = Path(__file__).resolve().parents[1]
DEMO_PATH = REPO / "Integration" / "system_integration" / "a0l_2026-11-21" / "e2e_demo.py"
STORED = REPO / "Integration" / "system_integration" / "a0l_2026-11-21" / "demo"


def load_demo():
    spec = importlib.util.spec_from_file_location("a0l_e2e_demo", DEMO_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def demo():
    return load_demo()


@pytest.fixture(scope="module")
def summary(tmp_path_factory, demo):
    return demo.run(tmp_path_factory.mktemp("a0l_demo") / "out")


# ----------------------------------------------------------------------------------------------------- the real writer against the Python reader
def test_the_javascript_log_is_canonical_hash_chained_and_read_by_the_python_reader_without_error(tmp_path, demo):
    node = demo.run_node(tmp_path, until=6, txEvery=2, tickEveryS=1)
    lines = Path(node["log"]).read_bytes().split(bytes([10]))[:-1]
    previous = wc.GENESIS_SHA256
    for index, line in enumerate(lines, 1):
        doc = json.loads(line)
        assert wc.canonical(doc).encode("ascii") == line and doc["rseq"] == index and doc["prev"] == previous and doc["sha256"] == wc.event_digest(doc, previous)
        previous = doc["sha256"]
    reader = a.Reader(Path(node["log"]), pol=a.policy(max_tick_gap_ms=a.SHORT_GAP_MS))
    reader.poll(10)
    assert reader.tail.error is None and reader.tail.chain_head == previous and reader.tail.counts()["attempted"] == 3
    assert [json.loads(x)["kind"] for x in lines][:5] == ["start", "intent", "attempt", "outcome", "tick"]


def test_the_javascript_and_the_scripted_python_writers_use_the_same_record_shapes(tmp_path, demo):
    node = demo.run_node(tmp_path / "js", until=4, txEvery=2, tickEveryS=1)
    js_rows = [json.loads(x) for x in Path(node["log"]).read_bytes().split(bytes([10]))[:-1]]
    log = SyntheticIssueLog(tmp_path / "py.jsonl", tick_interval_ms=1000)
    gen = Scripted(log).start(-0.5)
    gen.tx(0.1)
    gen.tick(1.0)
    gen.tick(2.0)
    gen.stop(4.0)
    py_rows = [json.loads(x) for x in log.lines()]
    shapes = lambda rows: {kind: set(next(r for r in rows if r["kind"] == kind)) for kind in {r["kind"] for r in rows}}   # noqa: E731
    js_shapes, py_shapes = shapes(js_rows), shapes(py_rows)
    for kind in ("start", "intent", "attempt", "outcome", "tick", "stop"):
        assert js_shapes[kind] == py_shapes[kind], (kind, js_shapes[kind] ^ py_shapes[kind])


def test_the_real_writer_logs_before_the_call_and_the_reader_sees_unresolved_attempts_as_unresolved(tmp_path, demo):
    node = demo.run_node(tmp_path, until=6, txEvery=1, tickEveryS=1, hangTxSeq=2, cleanup=True)
    reader = a.Reader(Path(node["log"]), pol=a.policy(max_tick_gap_ms=a.SHORT_GAP_MS))
    reader.poll(10)
    assert reader.tail.error is None
    assert reader.tail.counts()["unresolved"] == 1 and reader.tail.unresolved_ns() == [2]
    assert reader.tail.writer_state()["state"] == ilr.W_CLOSED_OTHER and reader.tail.stop_record["in_flight"] == 1       # a stop with an open call is not a clean stop


def test_the_real_writer_refuses_two_workers_and_a_worker_that_does_not_match_its_index(tmp_path, demo):
    two = demo.run_node(tmp_path / "two", totalWorkers=2, until=1, cleanup=False)
    assert two["error"]["code"] == "worker_count_unsupported" and not (tmp_path / "two" / "issue.jsonl").exists()
    wrong = demo.run_node(tmp_path / "wrong", workerId="w3", workerIndex=0, until=1, cleanup=False)
    assert wrong["error"]["code"] == "worker_identity_mismatch" and not (tmp_path / "wrong" / "issue.jsonl").exists()


# ----------------------------------------------------------------------------------------------------- the valid demonstration (steps 1-12)
def test_valid_demonstration_short_scale_both_routes(summary):
    v = summary["valid_short"]
    assert v["writer"] == "closed_clean" and v["counts"]["attempted"] == 10 and v["counts"]["returned"] == 10 and v["counts"]["unresolved"] == 0
    assert v["node_status"]["failed"] is False and v["log_records"] == v["node_status"]["rseq"]
    g = v["generator_route"]
    assert g["positives_observed"] is True and g["positive_seconds"] == 10 and g["zero_seconds_verified"] == 10
    assert g["scope"] == "source_stream_observer" and g["subject"] == "generator_issuance" and g["external_mempool_coverage"] is False
    r = v["recorder_route"]
    assert r["valid"] is True and r["source_kind"] == "issue_log" and r["zero_at_1"] == "verified_zero" and r["verified_seconds"] == 20
    rec = v["reconciliation"]
    assert rec["generator"]["submission_attempts"] == 10 and rec["recorder_submit_callbacks"]["minus_generator_attempts"] == 0
    assert "counts only" in rec["recorder_submit_callbacks"]["join"]


def test_valid_demonstration_day_scale_runs_the_stage_3_fixture_path_on_both_routes(summary):
    d = summary["day_stage3"]
    g = d["generator_route"]
    assert d["attempts"] == 4321 and d["ticks"] == 76 and "no model or EWMA loaded" in d["label"]
    assert g["status"] == "COMPLETED" and g["run_verifies"] is True
    assert g["predictive_outcomes"][1:] == ["propose"] * 3 and g["static_outcomes"][1:] == ["hold"] * 3 and g["reactive_outcomes"][1:] == ["hold"] * 3
    assert g["class_counts"] == {"observed_positive": 4321, "verified_zero": 38_881} and g["abstention_reason_codes"] == [] and g["coverage_subject"] == "generator_issuance"
    assert g["live_quality"] is False and g["configuration_changes_applied"] == 0 and g["frozen_model_loaded"] is False and g["frozen_ewma_loaded"] is False
    assert d["recorder_route"]["predictive_outcomes"][1:] == ["propose"] * 3 and d["recorder_route"]["source_kind"] == "issue_log"


# ----------------------------------------------------------------------------------------------------- the required failure demonstrations
def test_failure_missing_issuance_log(summary):
    f = summary["failures"]["missing_log"]
    assert f == {"status": "log_missing", "bridge": "not_started", "journal_written": False}


def test_failure_empty_log_without_source_health_evidence(summary):
    f = summary["failures"]["empty_log_without_health"]
    assert f["records"] == 1 and f["segments"] == [] and f["zero_at_5_accepted"] is False and f["writer"] == "active_per_last_liveness_point"


def test_failure_interrupted_generator(summary):
    f = summary["failures"]["interrupted_generator"]
    assert f["stop_record"] is None and f["last_liveness_s"] == 10.0 and f["zero_at_5"] == "verified_zero" and f["zero_at_15_accepted"] is False


def test_failure_partial_record(summary):
    f = summary["failures"]["partial_record"]
    assert f["error"] is None and f["partial_tail_bytes"] > 0 and f["records"] == 51


def test_failure_duplicate_transaction_identifier_and_record(summary):
    assert summary["failures"]["duplicate_record"]["error"] == "record_duplicate"


def test_failure_missing_sequence(summary):
    assert summary["failures"]["missing_sequence"]["error"] == "record_sequence_gap"


def test_failure_failed_submission_is_counted_and_never_hidden(summary):
    f = summary["failures"]["failed_submission"]
    assert f["log_valid"] is True and f["writer_failed"] is False and f["counts"]["rejected"] == 6 and f["counts"]["returned"] == 14 and f["counts"]["attempted"] == 20


def test_failure_recorder_count_mismatch(summary):
    f = summary["failures"]["recorder_count_mismatch"]
    assert f["deficit_valid"] is True and f["deficit_second_7"] == "interrupted_collection" and f["surplus_valid"] is False and f["surplus_failure"] == "recorder_exceeds_source"


def test_failure_unknown_coverage(summary):
    f = summary["failures"]["unknown_coverage"]
    assert f["zero_at_5"] == "zero_count_unknown_coverage" and f["zero_at_5_accepted"] is False


def test_failure_worker_mismatch(summary):
    f = summary["failures"]["worker_mismatch"]
    assert f["writer_refusal"]["code"] == "worker_count_unsupported" and f["reader_error"] == "worker_identity_mismatch" and f["recorder_attestation_failure"] == "worker_identity_mismatch"


def test_failure_clock_inconsistency(summary):
    f = summary["failures"]["clock_inconsistency"]
    assert f["wall_step_error"] == "wall_clock_step" and f["reader_behind_generator"] == "generator_clock_ahead_of_reader"
    assert f["journal_rows"][0] == "start" and f["max_watermark_minus_write_time_s"] <= 0       # no heartbeat ever claims capture beyond the moment it was written


def test_failure_tampered_or_replaced_log(summary):
    assert summary["failures"]["tampered_record"]["error"] == "hash_mismatch" and summary["failures"]["replaced_log"]["error"] == "log_replaced"


def test_every_output_is_labelled_synthetic_and_names_what_it_does_not_claim(summary):
    assert summary["label"].startswith("SYNTHETIC / FIXTURE") and "no Caliper, Fabric, Docker, WSL or network" in summary["label"]
    assert {"Caliper compatibility", "Fabric behaviour", "live source completeness", "external Ethereum mempool capture"} <= set(summary["not_claimed"])
    assert set(summary["failures"]) == {"missing_log", "empty_log_without_health", "interrupted_generator", "partial_record", "duplicate_record", "missing_sequence",
                                        "failed_submission", "recorder_count_mismatch", "unknown_coverage", "worker_mismatch", "clock_inconsistency", "tampered_record",
                                        "replaced_log"}


def test_the_stored_demonstration_is_reproduced_exactly_by_a_fresh_run(summary):
    stored = json.loads((STORED / "demo_summary.json").read_text(encoding="utf-8"))
    assert json.loads(json.dumps(summary, sort_keys=True, default=str)) == stored


def test_the_stored_evidence_verifies_against_its_own_sums():
    import hashlib
    lines = (STORED / "SHA256SUMS.txt").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    for line in lines:
        digest, _sep, name = line.partition("  ")
        assert hashlib.sha256((STORED / name[2:]).read_bytes()).hexdigest() == digest, name
    log = STORED / "valid_short_issue.jsonl"
    reader = a.Reader(log, pol=a.policy(max_tick_gap_ms=a.SHORT_GAP_MS))
    reader.poll(30)
    assert reader.tail.error is None and reader.tail.counts()["attempted"] == 10 and at(0) is not None and DAY_MS > 0
