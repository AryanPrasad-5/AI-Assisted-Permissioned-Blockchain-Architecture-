"""A0o workstream A: the SourceCount reader driver (checklist items 1-15). Synthetic scripted logs plus the STORED A0m logs; offline only."""

from __future__ import annotations

import ast
import json
import socket
import subprocess
from pathlib import Path

import pytest

from tests.a0o_support import (DAY_MS, LIMITS, REPO, STORED, a, at, gc, ident, ilr, lines_of, scripted_log, sr, tamper, wc, write_lines, NL)

SCRIPT = REPO / "scripts" / "e1" / "e1_sourcecount_reader.py"
READ = "2026-11-21T00:01:00Z"


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a process or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)


def inspect(path, **kw):
    kw.setdefault("round_index", 1)
    return sr.inspect_log(ident(path, round_index=kw.pop("round_index"), checkpoint=kw.pop("checkpoint", None)), read_time=at(25), policy_limits=LIMITS)["evidence"]


# ------------------------------------------------------------------------------------------------------------------- 1 identity
def test_01_run_round_and_worker_identity_are_explicit_and_distinct_per_round(tmp_path):
    log = scripted_log(tmp_path).log.path
    one, two = ident(log, round_index=1), ident(log, round_index=2)
    assert one.instance.endswith(":w0:r1") and two.instance.endswith(":w0:r2") and one.instance != two.instance
    for bad in ({"round_index": None}, {"round_index": True}, {"round_index": -1}, {"round_index": "1"}):
        with pytest.raises(sr.SourceCountReaderError) as exc:
            sr.ReaderIdentity(a.RUN, bad["round_index"], "w0", log, a.DOMAIN)
        assert exc.value.code == "identity_missing"
    for field, value in (("run_id", " "), ("clock_domain", ""), ("worker_id", "w1")):
        kwargs = {"run_id": a.RUN, "round_index": 1, "worker_id": "w0", "log_path": log, "clock_domain": a.DOMAIN, field: value}
        with pytest.raises(sr.SourceCountReaderError):
            sr.ReaderIdentity(**kwargs)
    with pytest.raises(sr.SourceCountReaderError) as exc:
        sr.SourceCountReader(one, policy_limits={})
    assert exc.value.code == "policy_missing"                                                  # the unapproved placeholder gap is not defaulted
    with pytest.raises(TypeError):
        sr.ReaderIdentity(a.RUN, 1, "w0", log)                                                  # a clock domain is required, not defaulted


# ------------------------------------------------------------------------------------------------------------------- 2-3 valid log and counts
def test_02_a_valid_hash_chained_log_is_verified_with_its_chain_head_and_prefix_digest(tmp_path):
    gen = scripted_log(tmp_path)
    ev = inspect(gen.log.path)
    assert ev["schema"] == "source_count_evidence/1" and ev["status"] == sr.ST_VERIFIED and ev["failure_states"] == []
    assert ev["log"]["chain_head_sha256"] == gen.log.prev and ev["log"]["writer_state"] == "closed_clean"
    assert ev["log"]["prefix_sha256"] == __import__("hashlib").sha256(gen.log.path.read_bytes()).hexdigest()
    assert ev["identity"]["run_id"] == a.RUN and ev["identity"]["round_index"] == 1 and ev["identity"]["log_round_index"] == 1
    assert ev["scope"]["external_mempool_coverage"] is False and ev["scope"]["live_source_coverage_verified"] is False and ev["scope"]["live_quality"] is False
    assert ev["descriptor"] == ilr.issue_log_descriptor(a.RUN, "w0")


def test_03_the_source_counts_match_what_the_generator_wrote_and_keep_intent_attempt_outcome_apart(tmp_path):
    gen = scripted_log(tmp_path)
    ev = inspect(gen.log.path)
    c = ev["counts"]
    assert (c["attempts"], c["intents"], c["returned"], c["rejected"], c["unresolved"], c["intent_without_attempt"]) == (gen.attempted, gen.intents, gen.outcomes, 0, 0, 0) == (40, 40, 40, 0, 0, 0)
    assert sum(i["attempts_in_interval"] for i in ev["interval"]["intervals"]) == 40
    assert ev["interval"]["intervals"][0]["start_utc"].endswith(":00.000Z") and ev["interval"]["alignment"].startswith("whole second")
    # an attempt with no outcome is UNRESOLVED and reported, never completed
    log = a.SyntheticIssueLog(tmp_path / "res.jsonl", tick_interval_ms=1000)
    gen2 = a.Scripted(log).start(-0.5)
    gen2.tx(0.1, outcome_after=None)
    gen2.tx(0.2, result="rejected")
    gen2.tick(1)
    gen2.stop(1.0, reason="cleanup")
    ev2 = inspect(log.path)
    assert ev2["counts"]["attempts"] == 2 and ev2["counts"]["unresolved"] == 1 and ev2["counts"]["rejected"] == 1
    assert ev2["status"] == sr.ST_PARTIAL and ilr.RC_UNRESOLVED in ev2["failure_states"]


# ------------------------------------------------------------------------------------------------------------------- 4-10 failures
def test_04_a_missing_log_is_unverified_never_zero(tmp_path):
    ev = inspect(tmp_path / "absent.jsonl")
    assert ev["status"] == sr.ST_UNVERIFIED and ev["failure_states"] == ["log_missing"]
    assert ev["interval"]["intervals"] == [] and ev["liveness"]["segments"] == []


@pytest.mark.parametrize("variant,code", [("edit", "hash_mismatch"), ("noncanonical", "record_not_canonical"), ("garbage", "record_malformed")])
def test_05_a_corrupt_log_is_invalid_as_a_whole(tmp_path, variant, code):
    gen = scripted_log(tmp_path, name=variant)
    lines = lines_of(gen.log.path)
    lines[10] = {"edit": tamper(lines[10], "t_ms", json.loads(lines[10])["t_ms"] + 1), "noncanonical": lines[10].replace(b",", b", ", 1), "garbage": b"{not json"}[variant]
    write_lines(gen.log.path, lines)
    ev = inspect(gen.log.path)
    assert ev["status"] == sr.ST_INVALID and ev["failure_states"] == [code] and ev["interval"]["intervals"] == [] and ev["counts"]["attempts"] is None


def test_06_a_duplicated_record_is_a_duplicate_event(tmp_path):
    gen = scripted_log(tmp_path)
    lines = lines_of(gen.log.path)
    lines.insert(11, lines[10])
    write_lines(gen.log.path, lines)
    ev = inspect(gen.log.path)
    assert ev["status"] == sr.ST_INVALID and ev["failure_states"] == ["record_duplicate"]


def test_07_a_removed_record_is_a_sequence_gap(tmp_path):
    gen = scripted_log(tmp_path)
    lines = lines_of(gen.log.path)
    del lines[10]
    write_lines(gen.log.path, lines)
    ev = inspect(gen.log.path)
    assert ev["status"] == sr.ST_INVALID and ev["failure_states"] == ["record_sequence_gap"]


def test_08_a_partial_tail_is_reported_and_nothing_after_the_last_complete_record_is_certified(tmp_path):
    gen = scripted_log(tmp_path)
    data = gen.log.path.read_bytes()
    gen.log.path.write_bytes(data[:-30])                                                       # the stop record is torn
    ev = inspect(gen.log.path)
    assert ev["status"] == sr.ST_PARTIAL and "partial_tail" in ev["failure_states"] and "incomplete_lifecycle_no_stop_record" in ev["failure_states"]
    assert ev["log"]["partial_tail_bytes"] > 0 and ev["log"]["writer_state"] != "closed_clean"


def test_09_truncation_after_reading_is_detected_in_both_the_live_tail_and_a_resume(tmp_path):
    gen = scripted_log(tmp_path)
    ck = tmp_path / "ck.json"
    reader = sr.SourceCountReader(ident(gen.log.path, checkpoint=ck), policy_limits=LIMITS)
    reader.poll(at(25))
    assert reader.evidence()["status"] == sr.ST_VERIFIED and ck.is_file()
    data = gen.log.path.read_bytes()
    gen.log.path.write_bytes(data[:len(data) // 2])
    reader.poll(at(26))
    ev = reader.evidence()
    assert ev["status"] == sr.ST_INVALID and ev["failure_states"] == ["log_truncated"]
    with pytest.raises(ilr.IssueLogError) as exc:
        reader.restart()
    assert exc.value.code == "log_truncated"                                                  # a resume never builds on a shorter log


def test_10_a_replaced_file_is_detected_even_when_its_length_is_unchanged(tmp_path):
    gen = scripted_log(tmp_path)
    reader = sr.SourceCountReader(ident(gen.log.path), policy_limits=LIMITS)
    reader.poll(at(25))
    data = bytearray(gen.log.path.read_bytes())
    data[-20] = ord("0") if data[-20] != ord("0") else ord("1")
    gen.log.path.write_bytes(bytes(data))
    reader.poll(at(26))
    assert reader.evidence()["failure_states"] == ["log_replaced"] and reader.evidence()["status"] == sr.ST_INVALID


# ------------------------------------------------------------------------------------------------------------------- 11 restart
def schedule(restart_at=None):
    return [{"read_time": at(1.5), "reveal_records": 8}, {"read_time": at(6), "reveal_records": 15, "partial_bytes": 25},
            {"read_time": at(11), "reveal_records": 15, "restart": restart_at == 2}, {"read_time": at(30), "reveal_records": None}]


def test_11_a_restart_from_the_checkpoint_gives_the_same_evidence_as_an_uninterrupted_read(tmp_path):
    gen = scripted_log(tmp_path)
    outs = []
    for tag, restart_at in (("plain", None), ("restarted", 2)):
        work = tmp_path / tag
        reader, audit = sr.replay_log(ident(work / "w.jsonl", checkpoint=work / "ck.json"), source_log=gen.log.path, schedule=schedule(restart_at), policy_limits=LIMITS)
        outs.append(audit["evidence"])
    plain, restarted = outs
    assert restarted["time"]["restarts"] == 1 and plain["time"]["restarts"] == 0
    for key in ("status", "failure_states", "counts", "interval", "liveness"):
        assert plain[key] == restarted[key], key
    assert plain["log"]["chain_head_sha256"] == restarted["log"]["chain_head_sha256"] == gen.log.prev
    assert restarted["checkpoint"]["rseq"] == plain["checkpoint"]["rseq"] and restarted["checkpoint"]["round_index"] == 1


def test_11b_restart_refusals_are_explicit(tmp_path):
    gen = scripted_log(tmp_path)
    no_ck = sr.SourceCountReader(ident(gen.log.path), policy_limits=LIMITS)
    with pytest.raises(sr.SourceCountReaderError) as exc:
        no_ck.restart()
    assert exc.value.code == "checkpoint_path_missing"
    bridged = sr.SourceCountReader(ident(gen.log.path, checkpoint=tmp_path / "ck.json"), policy_limits=LIMITS, journal_path=tmp_path / "j.jsonl", source_id=a.SOURCE_ID,
                                   origin=gc.ORIGIN_SYNTHETIC)
    with pytest.raises(sr.SourceCountReaderError) as exc:
        bridged.restart()
    assert exc.value.code == "bridge_restart_unsupported"
    ck = tmp_path / "bad.json"
    reader = sr.SourceCountReader(ident(gen.log.path, checkpoint=ck), policy_limits=LIMITS)
    reader.poll(at(25))
    doc = json.loads(ck.read_text())
    doc["rseq"] += 1
    ck.write_text(json.dumps(doc))
    with pytest.raises(ilr.IssueLogError) as exc2:
        reader.restart()
    assert exc2.value.code == "checkpoint_corrupt"
    with pytest.raises(sr.SourceCountReaderError):
        sr.replay_log(ident(gen.log.path), source_log=gen.log.path, schedule=schedule(), policy_limits=LIMITS)           # the work file must be new, the source untouched


# ------------------------------------------------------------------------------------------------------------------- 12 round isolation
def test_12_round_one_cannot_consume_round_twos_evidence(tmp_path):
    r1 = scripted_log(tmp_path, name="r1", round_index=1)
    r2 = scripted_log(tmp_path, name="r2", round_index=2, seconds=10)
    ok1, ok2 = inspect(r1.log.path, round_index=1), inspect(r2.log.path, round_index=2)
    assert ok1["status"] == ok2["status"] == sr.ST_VERIFIED and ok1["counts"]["attempts"] == 40 and ok2["counts"]["attempts"] == 20
    wrong = inspect(r2.log.path, round_index=1)
    assert wrong["status"] == sr.ST_INVALID and wrong["failure_states"] == ["round_identity_mismatch"] and wrong["counts"]["attempts"] is None and wrong["interval"]["intervals"] == []
    # the attempt ids a reader reports carry the round, so no attempt of one round can be counted by the other
    rd1, rd2 = sr.SourceCountReader(ident(r1.log.path, round_index=1), policy_limits=LIMITS), sr.SourceCountReader(ident(r2.log.path, round_index=2), policy_limits=LIMITS)
    rd1.poll(at(25))
    rd2.poll(at(25))
    ids1, ids2 = {e["id"] for e in rd1.tail.attempt_events("x")}, {e["id"] for e in rd2.tail.attempt_events("x")}
    assert ids1 and ids2 and not ids1 & ids2 and all(i.startswith(f"{a.RUN}:1:") for i in ids1) and all(i.startswith(f"{a.RUN}:2:") for i in ids2)


def test_12b_a_checkpoint_cannot_be_resumed_against_another_round(tmp_path):
    r1 = scripted_log(tmp_path, name="r1", round_index=1)
    ck = tmp_path / "ck.json"
    sr.SourceCountReader(ident(r1.log.path, round_index=1, checkpoint=ck), policy_limits=LIMITS).poll(at(25))
    with pytest.raises(ilr.IssueLogError) as exc:
        sr.RoundBoundTail.resume(r1.log.path, a.policy(max_tick_gap_ms=5000), ck, round_index=2)
    assert exc.value.code == "checkpoint_round_mismatch"
    plain = tmp_path / "plain.json"
    t = ilr.IssueLogTail(r1.log.path, a.policy(max_tick_gap_ms=5000), checkpoint_path=plain)
    t.poll(at(25))
    with pytest.raises(ilr.IssueLogError) as exc2:
        sr.RoundBoundTail.resume(r1.log.path, a.policy(max_tick_gap_ms=5000), plain, round_index=1)
    assert exc2.value.code == "checkpoint_round_mismatch"                                      # a checkpoint without a round is not trusted for any round


def test_12c_rounds_of_one_run_may_not_share_a_reader_log_checkpoint_or_journal(tmp_path):
    r1, r2 = scripted_log(tmp_path, name="r1", round_index=1), scripted_log(tmp_path, name="r2", round_index=2)
    sr.validate_round_set([ident(r1.log.path, round_index=1, checkpoint=tmp_path / "c1"), ident(r2.log.path, round_index=2, checkpoint=tmp_path / "c2")],
                          journal_paths=[tmp_path / "j1", tmp_path / "j2"])
    cases = [([ident(r1.log.path, round_index=1), ident(r1.log.path, round_index=2)], "log_shared"),
             ([ident(r1.log.path, round_index=1, checkpoint=tmp_path / "c"), ident(r2.log.path, round_index=2, checkpoint=tmp_path / "c")], "checkpoint_shared"),
             ([ident(r1.log.path, round_index=1), ident(r2.log.path, round_index=1)], "round_shared")]
    for identities, code in cases:
        with pytest.raises(sr.SourceCountReaderError) as exc:
            sr.validate_round_set(identities)
        assert exc.value.code == code
    with pytest.raises(sr.SourceCountReaderError) as exc:
        sr.validate_round_set([ident(r1.log.path, round_index=1), ident(r2.log.path, round_index=2)], journal_paths=[tmp_path / "j", tmp_path / "j"])
    assert exc.value.code == "journal_shared"
    with pytest.raises(sr.SourceCountReaderError):
        sr.validate_round_set([ident(r1.log.path, round_index=1), ident(r2.log.path, round_index=2, run_id="another-run")])


def test_12d_the_bridge_instance_carries_the_round_where_the_base_bridge_hard_codes_r0(tmp_path):
    r1, r2 = scripted_log(tmp_path, name="r1", round_index=1), scripted_log(tmp_path, name="r2", round_index=2)
    base = gc.IssueLogJournalBridge(ilr.IssueLogTail(r1.log.path, a.policy(max_tick_gap_ms=5000)), tmp_path / "bj.jsonl", source_id=a.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC)
    assert base.instance.endswith(":r0")                                                       # the defect reproduced: the unmodified bridge names every round r0
    rd1 = sr.SourceCountReader(ident(r1.log.path, round_index=1), policy_limits=LIMITS, journal_path=tmp_path / "j1.jsonl", source_id=a.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC)
    rd2 = sr.SourceCountReader(ident(r2.log.path, round_index=2), policy_limits=LIMITS, journal_path=tmp_path / "j2.jsonl", source_id=a.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC)
    assert rd1.bridge.instance.endswith(":w0:r1") and rd2.bridge.instance.endswith(":w0:r2")
    rd1.poll(at(25))
    start = [json.loads(line) for line in (tmp_path / "j1.jsonl").read_text().splitlines()][0]
    assert start["collector_instance"] == rd1.bridge.instance
    with pytest.raises(sr.SourceCountReaderError) as exc:
        sr.RoundIsolatedBridge(ilr.IssueLogTail(r1.log.path, a.policy(max_tick_gap_ms=5000)), tmp_path / "x.jsonl", source_id=a.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC)
    assert exc.value.code == "round_unbound"                                                   # a plain tail cannot be wrapped


# ------------------------------------------------------------------------------------------------------------------- 13 worker mismatch
def test_13_a_log_of_another_worker_or_run_or_worker_count_is_refused(tmp_path):
    other = scripted_log(tmp_path, name="w1", worker_id="w1")
    assert inspect(other.log.path)["failure_states"] == ["worker_identity_mismatch"]
    run = scripted_log(tmp_path, name="run", run_id="some-other-run")
    assert inspect(run.log.path)["failure_states"] == ["run_identity_mismatch"]
    two = scripted_log(tmp_path, name="two", total_workers=2)
    assert inspect(two.log.path)["failure_states"] == ["worker_count_unsupported"]
    roster = scripted_log(tmp_path, name="roster", roster=["w0", "w1"])
    assert inspect(roster.log.path)["failure_states"] == ["worker_roster_mismatch"]


def test_13b_a_descriptor_is_derived_and_a_declared_counter_or_resolution_is_never_accepted(tmp_path):
    log = scripted_log(tmp_path).log.path
    good = ilr.issue_log_descriptor(a.RUN, "w0")
    assert sr.validate_source_descriptor(None, ident(log)) == good and sr.validate_source_descriptor(dict(good), ident(log)) == good
    for patch in ({"kind": "generator_issue_counter"}, {"resolution": "total"}, {"independent_of_recorder": False}, {"counter_id": "issue-log:other:w0"}, {"timestamp_semantics": "callback_time"}):
        with pytest.raises(sr.SourceCountReaderError) as exc:
            sr.validate_source_descriptor({**good, **patch}, ident(log))
        assert exc.value.code == "source_descriptor_mismatch", patch
    # the classifier ALONE trusts a declared bare counter (A0n finding 4): the driver does not rely on it
    declared = {**good, "kind": "generator_issue_counter", "resolution": "interval"}
    assert a.sc.classify_descriptor(declared).certifies_intervals is True


# ------------------------------------------------------------------------------------------------------------------- 14 unknown coverage
def test_14_a_quiet_or_never_ticking_log_leaves_coverage_unknown_and_zero_is_never_asserted(tmp_path):
    log = a.SyntheticIssueLog(tmp_path / "quiet.jsonl", tick_interval_ms=1000)
    a.Scripted(log).start(-0.5)                                                                # a start record and then silence
    ev = inspect(log.path)
    assert ev["status"] == sr.ST_UNVERIFIED and sr.ilr.RC_GENERATOR_UNOBSERVED in ev["failure_states"] and ev["interval"]["intervals"] == []
    assert ev["counts"]["attempts"] == 0 and ev["liveness"]["segments"] == [] and ev["log"]["writer_state"] == "active_per_last_liveness_point"
    gap = scripted_log(tmp_path, name="gap", tick_every=15)                                    # ticks 15 s apart against a 5 s allowance: no observed segment
    ev2 = inspect(gap.log.path)
    assert ev2["status"] != sr.ST_VERIFIED and "generator_observation_gap" in ev2["failure_states"]
    empty = tmp_path / "empty.jsonl"
    empty.write_bytes(b"")
    ev3 = inspect(empty)
    assert ev3["status"] == sr.ST_UNVERIFIED and ev3["failure_states"] == ["issue_log_not_started"] and ev3["interval"]["intervals"] == []


# ------------------------------------------------------------------------------------------------------------------- 15 cutoff
def test_15_evidence_read_after_the_decision_cutoff_is_not_used_retroactively(tmp_path):
    gen = scripted_log(tmp_path)
    reader, audit = sr.replay_log(ident(tmp_path / "rp" / "w.jsonl"), source_log=gen.log.path, schedule=schedule(), policy_limits=LIMITS, as_of=at(7))
    ev = audit["evidence"]
    full = reader.evidence()
    assert full["counts"]["attempts"] == 40 and ev["counts"]["attempts"] < 40
    assert ev["cutoff"]["records_read_after_cutoff_excluded"] == 40 - ev["counts"]["attempts"] > 0
    assert ev["counts"]["intents"] is None and ev["counts"]["unresolved"] is None and "withheld" in ev["counts"]["note"]
    assert ev["time"]["decision_cutoff_utc"] == sr._iso(DAY_MS + 7000) and ev["time"]["read_watermark_utc"] == sr._iso(DAY_MS + 30_000)
    counted = [e for e in reader.tail.attempt_events("x") if e["read_ms"] <= DAY_MS + 7000]
    assert len(counted) == ev["counts"]["attempts"] and all(e["read_ms"] <= DAY_MS + 7000 for e in counted)      # every counted attempt had already been READ by the cutoff
    # event time and read time are different quantities: the last attempt is READ later than it was issued
    attempts = reader.tail.attempt_events("x")
    assert all(e["read_ms"] >= e["ts_ms"] for e in attempts) and any(e["read_ms"] > e["ts_ms"] for e in attempts)
    assert ev["time"]["event_time_never_replaced_by_read_time"] is True


# ------------------------------------------------------------------------------------------------------------------- modes and CLI
def test_16_follow_mode_is_refused_whatever_is_passed_and_is_only_an_interface(tmp_path):
    assert sr.LIVE_FOLLOW_ENABLED is False and sr.FOLLOW_INTERFACE["enabled"] is False
    with pytest.raises(sr.SourceCountReaderError) as exc:
        sr.follow(ident(scripted_log(tmp_path).log.path), enable=True, live=True)
    assert exc.value.code == "live_follow_refused"
    tree = ast.parse((REPO / "src/fabric_perf/controllers/sourcecount_reader.py").read_text(encoding="utf-8"))
    assigns = [n for n in ast.walk(tree) if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "LIVE_FOLLOW_ENABLED" for t in n.targets)]
    assert len(assigns) == 1 and assigns[0].value.value is False
    import importlib.util
    spec = importlib.util.spec_from_file_location("e1_sourcecount_reader", SCRIPT)
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    assert cli.main(["follow", "--live"]) == 5


def test_17_the_cli_inspects_a_stored_log_and_writes_a_new_audit_file_only(tmp_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("e1_sourcecount_reader", SCRIPT)
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    log = STORED / "round1/issuelog/a0m-dry-001_w0_r1_m-a0m-dry-001.issuelog.jsonl"
    before = log.read_bytes()
    out = tmp_path / "audit.json"
    code = cli.main(["inspect", "--run-id", "a0m-dry-001", "--round", "1", "--worker", "w0", "--clock-domain", "caliper-worker-host:node_date_now", "--max-tick-gap-ms", "5000",
                     "--log", str(log), "--read-time", READ, "--out", str(out)])
    audit = json.loads(out.read_text())
    assert code == 0 and audit["schema"] == "sourcecount_reader_audit/1" and audit["evidence"]["status"] == sr.ST_VERIFIED and audit["evidence"]["counts"]["attempts"] == 5
    assert log.read_bytes() == before
    assert cli.main(["inspect", "--run-id", "a0m-dry-001", "--round", "1", "--worker", "w0", "--clock-domain", "x:y", "--max-tick-gap-ms", "5000", "--log", str(log),
                     "--read-time", READ, "--out", str(out)]) == 2                              # the output path must not exist
    wrong_round = tmp_path / "wrong.json"
    code2 = cli.main(["inspect", "--run-id", "a0m-dry-001", "--round", "0", "--worker", "w0", "--clock-domain", "caliper-worker-host:node_date_now", "--max-tick-gap-ms", "5000",
                      "--log", str(log), "--read-time", READ, "--out", str(wrong_round)])
    assert code2 == 1 and json.loads(wrong_round.read_text())["evidence"]["failure_states"] == ["round_identity_mismatch"]
    assert cli.main(["inspect", "--run-id", "a0m-dry-001", "--round", "1", "--worker", "w0", "--clock-domain", "caliper-worker-host:node_date_now", "--max-tick-gap-ms", "5000",
                     "--log", str(log), "--read-time", "2026-11-21T00:01:00", "--out", str(tmp_path / "naive.json")]) == 2     # a naive read time is refused


def test_18_the_stored_a0m_rounds_are_each_verified_only_by_their_own_round_reader():
    for rnd, label in ((0, "wu-a0m-dry-001"), (1, "m-a0m-dry-001")):
        log = STORED / f"round{rnd}/issuelog/a0m-dry-001_w0_r{rnd}_{label}.issuelog.jsonl"
        own = sr.inspect_log(sr.ReaderIdentity("a0m-dry-001", rnd, "w0", log, "caliper-worker-host:node_date_now"), read_time=READ, policy_limits=LIMITS)["evidence"]
        other = sr.inspect_log(sr.ReaderIdentity("a0m-dry-001", 1 - rnd, "w0", log, "caliper-worker-host:node_date_now"), read_time=READ, policy_limits=LIMITS)["evidence"]
        assert own["status"] == sr.ST_VERIFIED and other["status"] == sr.ST_INVALID and other["failure_states"] == ["round_identity_mismatch"]
        assert own["identity"]["reader_instance"] == f"issue-log-reader:a0m-dry-001:w0:r{rnd}"
