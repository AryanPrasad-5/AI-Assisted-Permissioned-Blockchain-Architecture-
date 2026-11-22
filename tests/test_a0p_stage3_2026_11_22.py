"""A0p workstream E (checklist items 40-47): the new write-timing and ingestion evidence does not weaken the coverage contract or the Stage 3 abstention rules.

The recorder route is run through the INCREMENTAL bridge (the whole-file bridge is the reference), then coverage validation, the captured history, the Stage 3 history guard and the
offline decision pipeline. Forecast and performance values are scripted fixtures; nothing is loaded, forecast or sent anywhere; none of it is a Caliper or Fabric measurement."""

from __future__ import annotations

import json
import pickle
import socket
import subprocess
from pathlib import Path

import pytest

from tests import a0j_support as j
from tests.a0j_support import at, by_controller, ch, rc
from tests.a0p_support import (NL, NODE, SOURCE_ID, SimulatedCrash, bridge, bridge_config, create_bridge, crash_at, drain_after_crash, ilt, lines_of, resume_bridge, rhi, run_timed_node,
                               sr, td, wc, write_lines)

from fabric_perf.controllers import abstention_accounting as abstention  # noqa: E402
from fabric_perf.controllers import live_foundation as lf  # noqa: E402

F = j.s.h.f


@pytest.fixture(autouse=True)
def nothing_loaded_nothing_sent(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a model load, process or network use is forbidden")
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(pickle, "load", refuse)
    monkeypatch.setattr(pickle, "loads", refuse)
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", refuse)
    real_popen = subprocess.Popen

    def only_the_offline_node_driver(*args, **kwargs):
        command = args[0] if args else kwargs.get("args")
        if not (isinstance(command, (list, tuple)) and command and command[0] == NODE):
            raise AssertionError("a process other than the offline node driver is forbidden")
        return real_popen(*args, **kwargs)
    monkeypatch.setattr(subprocess, "Popen", only_the_offline_node_driver)


def incremental(tmp_path, rec, *, name="inc", rows=5, origin=None, crash=None, resume=True):
    """Bridge a recorder day incrementally (optionally crashing and resuming) and build the strict recorder history from the resulting journal."""
    if not rec.spool_path.exists():
        rec.write()
    cfg = bridge_config(rec, tmp_path, name=name, policy=j.POLICY, max_rows_per_poll=rows)
    if crash is None:
        b, _ = create_bridge(cfg)
        b.poll_until_drained()
        b.close(source_ended=False)
    else:
        drain_after_crash(cfg, crash)
        if resume:
            b, _ = resume_bridge(cfg)
            b.poll_until_drained()
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(cfg.journal_path, source_id=rec.source_id, submit_events=events, policy=j.POLICY, run_id=j.RUN)
    binned = rc.bin_submit_events(events, j.recorder_source(origin or rec.origin, source_id=rec.source_id))
    return cfg, coverage, rc.RecorderCapturedHistory(binned, coverage=coverage, policy=j.POLICY)


def run(tmp_path, rec, *, steps=None, name="cap", **kw):
    cfg, coverage, history = incremental(tmp_path, rec, **kw)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, steps=steps, name=name)
    return cfg, history, manifest, rows, out


def day(tmp_path, **kwargs):
    return j.new_recorder(tmp_path, **kwargs)


def coverage_block(rows):
    return by_controller(rows)["predictive"][1]["predictive_evidence"]["workload_observation"]["coverage"]


# ----------------------------------------------------------------------------------------------------------------------------- 40 valid evidence
def test_40_a_journal_built_incrementally_is_the_same_evidence_and_supports_a_predictive_recommendation(tmp_path):
    rec = day(tmp_path, zero={100, 5000})
    cfg, history, manifest, rows, out = run(tmp_path, rec)
    reference = tmp_path / "whole.journal.jsonl"
    bridge.ingest_spool(rec.spool_path, reference, source_id=SOURCE_ID, policy=j.POLICY)
    assert cfg.journal_path.read_bytes() == reference.read_bytes()
    assert manifest["status"] == "COMPLETED" and j.s.h.runner.verify_run_directory(out) == []
    predictive = by_controller(rows)["predictive"]
    assert [r["outcome"] for r in predictive][1:] == ["propose"] * 3
    block = coverage_block(rows)
    assert block["mode"] == "strict_attested" and block["integrity"]["status"] == "VERIFIED" and block["abstention_reason_codes"] == []
    assert block["class_counts"]["verified_zero"] == 2 and block["recorder_only_zero_seconds"] == 0
    assert block["live_quality"] is False and block["synthetic_evidence"] is True and "NOT verified" in block["note"]
    assert block["recorder_evidence"]["coverage_scope"] == ["recorder_callback"] and block["recorder_evidence"]["live_source_coverage_verified"] is False


# ----------------------------------------------------------------------------------------------------------------------------- 41 unknown coverage
def test_41_unknown_coverage_stays_unknown_and_timing_evidence_is_not_source_completeness(tmp_path):
    rec = day(tmp_path, zero={100}, missed={2000})                                        # the callback missed a second the source issued; the recorder was healthy throughout
    cfg, history, manifest, rows, out = run(tmp_path, rec)
    for row in by_controller(rows)["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["forecast"] is None
        assert rc.RC_MISSING_CALLBACKS in row["predictive_evidence"]["workload_observation"]["coverage"]["abstention_reason_codes"]
    # a perfectly attested, perfectly timed sidecar and a complete ingest ledger change nothing about that: they are about WHEN, not about WHETHER
    ledger = td.read_ingest_ledger(cfg.ledger_path)
    assert ledger["rows"][1]["journal_append_confirmation"] == "os_fsync_returned" and ledger["has_stop"] is True
    node = run_timed_node(tmp_path / "node", stepUs=100)
    assert ilt.verify_timing(Path(node["log"]), Path(node["timing"])).status == ilt.VERIFIED
    stale_dir = tmp_path / "stale"
    stale_dir.mkdir()
    stale = j.new_recorder(stale_dir, until=30_000, traffic_until=43_204)                    # heartbeats stop at 30,000 s; the decision seconds are later
    _, _, _, stale_rows, _ = run(stale_dir, stale, name="stale")
    for row in by_controller(stale_rows)["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller"
        assert row["predictive_evidence"]["workload_observation"]["coverage"]["abstention_reason_codes"]       # coverage that stops early is not extended to the decision time


def test_41b_the_coverage_modules_do_not_read_the_timing_sidecar_or_the_ingest_ledger():
    root = Path(__file__).resolve().parents[1] / "src" / "fabric_perf" / "controllers"
    for name in ("workload_coverage.py", "recorder_coverage.py", "source_completeness.py", "issue_log_coverage.py", "captured_history.py", "collector_health.py"):
        text = (root / name).read_text(encoding="utf-8")
        for forbidden in ("issue_log_timing", "timing_durable", "recorder_health_incremental", "e1_issue_log_timing", "recorder_health_ingest"):
            assert forbidden not in text, (name, forbidden)


# ----------------------------------------------------------------------------------------------------------------------------- 42 heartbeat only
def test_42_recorder_heartbeats_alone_cannot_certify_a_generator_source_zero_even_through_the_incremental_bridge(tmp_path):
    rec = day(tmp_path, zero={100}, source_count=False)
    cfg, history, manifest, rows, out = run(tmp_path, rec)
    for row in by_controller(rows)["predictive"][1:]:
        assert row["outcome"] == "abstain" and rc.RC_NO_SOURCE_EVIDENCE in row["predictive_evidence"]["workload_observation"]["coverage"]["abstention_reason_codes"]
        assert row["predictive_evidence"]["workload_observation"]["coverage"].get("class_counts", {}).get("verified_zero", 0) == 0


# ----------------------------------------------------------------------------------------------------------------------------- 43 bridge failure
def test_43_an_incomplete_or_failed_bridge_invalidates_the_affected_coverage_and_nothing_is_certified_retroactively(tmp_path):
    rec = day(tmp_path, zero={100})
    rec.write()
    lines = lines_of(rec.spool_path)
    gap = tmp_path / "gap"
    gap.mkdir()
    rec_gap = day(gap, zero={100})
    rec_gap.write()
    spool = lines_of(rec_gap.spool_path)
    write_lines(rec_gap.spool_path, spool[:8] + spool[9:])                               # a sequence gap: the bridge halts at row 8
    cfg, history, manifest, rows, out = run(gap, rec_gap, name="gap")
    assert not (cfg.journal_path.read_bytes() == b"")
    assert len(lines_of(cfg.journal_path)) == 8
    for row in by_controller(rows)["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["frame"]["status"] == "accepted"
    for name in ("static", "reactive"):
        assert {r["outcome"] for r in by_controller(rows)[name][1:]} == {"hold"}
    interrupted = tmp_path / "interrupted"
    interrupted.mkdir()
    rec_cut = day(interrupted, zero={100})
    _, _, manifest2, rows2, _ = run(interrupted, rec_cut, name="cut", crash=crash_at("journal_after_write", 6), resume=False)     # the bridge died and was never resumed
    assert len(lines_of(bridge_config(rec_cut, interrupted, name="inc", policy=j.POLICY).journal_path)) == 6
    for row in by_controller(rows2)["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["forecast"] is None


def test_43b_a_resumed_bridge_gives_complete_evidence_for_later_decisions_and_never_rewrites_an_earlier_run(tmp_path):
    rec = day(tmp_path, zero={100})
    rec.write()
    partial = tmp_path / "partial"
    partial.mkdir()
    rec_a = day(partial, zero={100})
    _, _, manifest_a, rows_a, out_a = run(partial, rec_a, name="before", crash=crash_at("journal_after_write", 6), resume=False)
    before = {p.name: p.read_bytes() for p in out_a.iterdir() if p.is_file()}
    cfg = bridge_config(rec_a, partial, name="inc", policy=j.POLICY, max_rows_per_poll=5)
    b, _ = resume_bridge(cfg)
    b.poll_until_drained()
    _, coverage, history = None, None, None
    events, _ = rc.read_submit_events(rec_a.submits_path, instance=rec_a.instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(cfg.journal_path, source_id=SOURCE_ID, submit_events=events, policy=j.POLICY, run_id=j.RUN)
    history = rc.RecorderCapturedHistory(rc.bin_submit_events(events, j.recorder_source(rec_a.origin)), coverage=coverage, policy=j.POLICY)
    _, rows_b, out_b = j.s.h.run_captured(partial, history, name="after")
    assert [r["outcome"] for r in by_controller(rows_a)["predictive"]][1:] == ["abstain"] * 3          # decided on what existed then
    assert [r["outcome"] for r in by_controller(rows_b)["predictive"]][1:] == ["propose"] * 3          # a LATER run on later evidence
    assert {p.name: p.read_bytes() for p in out_a.iterdir() if p.is_file()} == before                  # the earlier run is not touched


# ----------------------------------------------------------------------------------------------------------------------------- 44 predictive only
def test_44_history_failures_abstain_only_the_predictive_controller_and_stay_in_the_denominator(tmp_path):
    cfg, history, manifest, rows, out = run(tmp_path, day(tmp_path, zero={100}, source_count=False))
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["reason"] == "workload_prefix_incomplete_fail_safe"
        assert abstention.classify_row(row)[0] == abstention.PREDICTIVE_HISTORY_ABSTENTION
    for name in ("static", "reactive"):
        assert [r["outcome"] for r in groups[name]][1:] == ["hold"] * 3
    result = abstention.account(rows, scheduled_frames=4, segment="a0p-synthetic")
    counts = result["controllers"]["predictive"]["category_counts"]
    assert result["consistent"] is True and counts[abstention.PREDICTIVE_HISTORY_ABSTENTION] == 3 and sum(counts.values()) == 4


# ----------------------------------------------------------------------------------------------------------------------------- 45 shared frame
def test_45_an_invalid_shared_frame_abstains_all_three_controllers_with_one_disposition(tmp_path):
    steps = F.steady(3)
    steps[2] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5, config_reader_ok=False)
    steps[3] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5)
    _, _, manifest, rows, _ = run(tmp_path, day(tmp_path, zero={100}), steps=steps)
    frames: dict = {}
    for row in rows:
        frames.setdefault(row["frame"]["index"], []).append(row)
    bad = [i for i, group in frames.items() if {r["frame"]["status"] for r in group} == {"rejected"}]
    assert bad
    for index in bad:
        group = frames[index]
        assert {r["outcome"] for r in group} == {"abstain"} and {r["abstention_scope"] for r in group} == {"frame"}
        assert {abstention.classify_row(r)[0] for r in group} == {abstention.SHARED_INVALID_FRAME}
    good = j.s.h.runner.check_comparability(rows)
    assert good["comparable"] is True and manifest["status"] == "COMPLETED"                       # all three saw the same frames, valid or not


# ----------------------------------------------------------------------------------------------------------------------------- 46-47 actuator and inference
def test_46_no_actuator_is_invoked_and_nothing_is_submitted_to_fabric(tmp_path):
    cfg, history, manifest, rows, out = run(tmp_path, day(tmp_path, zero={100}))
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert manifest["actuation"]["configuration_changes_applied"] == 0 and manifest["actuation"]["live_actuator_available"] is False
    assert all(r["actuator"]["submitted_to_fabric"] is False and r["actuator"]["live_actuator_available"] is False for r in rows)
    assert summary["real_inference_performed"] is False


def test_47_real_inference_stays_disabled_and_no_model_is_loaded(tmp_path):
    cfg, history, manifest, rows, out = run(tmp_path, day(tmp_path, zero={100}))
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert j.s.h.runner.sp.REAL_INFERENCE_AUTHORIZED is False
    with pytest.raises(AssertionError):
        pickle.load(None)                                                                   # the guard really is in force for this test


# ----------------------------------------------------------------------------------------------------------------------------- SourceCount reader unchanged
def test_47b_a_log_written_by_the_timed_writer_reads_through_the_unmodified_sourcecount_reader(tmp_path):
    node = run_timed_node(tmp_path, stepUs=100, until=6, txEvery=1, tickEveryS=1, tickIntervalMs=1000)
    ident = sr.ReaderIdentity("a0p-fx-001", 1, "w0", Path(node["log"]), "synthetic-host:node_date_now", None)
    start = json.loads(lines_of(Path(node["log"]))[0])
    read_time = ilt.ilr.ms_to_utc(start["t_ms"] + 60_000)
    ok = sr.inspect_log(ident, read_time=read_time, policy_limits={"max_tick_gap_ms": 1500})
    assert ok["evidence"]["status"] == sr.ST_VERIFIED
    late = sr.inspect_log(ident, read_time=ilt.ilr.ms_to_utc(start["t_ms"] + 60_000), policy_limits={"max_tick_gap_ms": 100})       # a too-strict liveness limit: a QUIET log, not a zero
    assert late["evidence"]["status"] != sr.ST_VERIFIED
    wrong_round = sr.ReaderIdentity("a0p-fx-001", 0, "w0", Path(node["log"]), "synthetic-host:node_date_now", None)
    assert sr.inspect_log(wrong_round, read_time=read_time, policy_limits={"max_tick_gap_ms": 1500})["evidence"]["status"] == sr.ST_INVALID
