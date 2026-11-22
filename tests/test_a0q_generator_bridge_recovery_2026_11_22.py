"""A0q workstreams A-C (recovery items 1-24 and the crash matrix): restart recovery of the GENERATOR-route journal bridge.

Everything is SYNTHETIC (scripted generator, scripted read times, scripted process deaths) and offline. The reader under test is ``RecoverableSourceCountReader``; the legacy
``SourceCountReader`` is unchanged and keeps refusing (test_01)."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
from pathlib import Path

import pytest

from tests import a0q_support as q
from tests.a0q_support import Death, NL, a, at, gc, ident, ilr, rec, sr, wc

GENESIS = wc.GENESIS_SHA256


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a process, container or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(os, "system", refuse)


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("a0q_base")
    world, reader, sig = q.baseline(tmp)
    return world, reader, sig


def ms(second: float) -> int:
    return ilr.utc_ms(at(second))


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def rechain(path: Path, mutate) -> None:
    """Rewrite a journal with a VALID hash chain after ``mutate(index, row)`` (so only the content, never the chain, is what the recovery must notice)."""
    lines = Path(path).read_bytes().split(NL)[:-1]
    prev, out = GENESIS, []
    for index, line in enumerate(lines, 1):
        row = json.loads(line)
        row.pop("prev"), row.pop("sha256")
        row = mutate(index, row) or row
        row["prev"] = prev
        row["sha256"] = wc.event_digest(row, prev)
        prev = row["sha256"]
        out.append(wc.canonical(row).encode("ascii"))
    Path(path).write_bytes(NL.join(out) + NL)


def rewrite_checkpoint(path: Path, mutate) -> None:
    doc = json.loads(Path(path).read_text(encoding="ascii"))
    doc.pop("self_sha256")
    mutate(doc)
    doc["self_sha256"] = hashlib.sha256(wc.canonical(doc).encode("utf-8")).hexdigest()
    Path(path).write_text(wc.canonical(doc) + "\n", encoding="ascii")


def partial_world(tmp: Path, until: int, name: str = "w"):
    """The script run through step ``until`` (exclusive), then the reader object is discarded: a process that died between two polls."""
    world, reader, _ = q.run(tmp, name=name, until=until)
    return world


def expect_refused(world, code: str, **reader_kw) -> None:
    """A restart over the files as they are must raise ``code`` and must not change the journal, the checkpoint or the log."""
    before = {p.name: sha(p) for p in (world.journal, world.ckpt, world.log_path) if p.exists()}
    with pytest.raises(Exception) as info:
        world.reopened(**reader_kw)
    assert getattr(info.value, "code", None) == code, (getattr(info.value, "code", None), str(info.value))
    assert {p.name: sha(p) for p in (world.journal, world.ckpt, world.log_path) if p.exists()} == before


# ----------------------------------------------------------------------------------------------------------------------------- 1-5 recovery
def test_01_the_legacy_reader_still_refuses_a_bridge_restart_and_the_recoverable_one_does_not(tmp_path):
    world = q.World(tmp_path, "legacy")
    legacy = sr.SourceCountReader(world.identity(), policy_limits=dict(q.LIMITS), journal_path=world.journal, source_id=q.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC)
    for i in range(6):
        world.apply(i)
        legacy.poll(at(world.steps[i].poll))
    with pytest.raises(sr.SourceCountReaderError) as info:                      # the reproduced A0o/A0p limitation, unchanged
        legacy.restart()
    assert info.value.code == "bridge_restart_unsupported"
    fresh = world.reopened()                                                    # the same files restart through the explicit successor class
    assert fresh.recovery_report["status"] == rec.ST_NOTHING and fresh.bridge is not None and fresh.restarts == 1
    for i in range(6, len(world.steps)):
        world.apply(i)
        fresh.poll(at(world.steps[i].poll))
    (tmp_path / "u").mkdir()
    _, _, uninterrupted = q.baseline(tmp_path / "u")
    assert q.sha(world.journal) == uninterrupted["journal_sha256"]


def test_02_a_clean_restart_changes_nothing(base, tmp_path):
    _, _, sig = base
    w2, _r, _ = q.run(tmp_path, name="w")                                           # the whole script, uninterrupted, in a fresh directory
    before = q.sha(w2.journal)
    again = w2.reopened()
    assert again.recovery_report["status"] == rec.ST_NOTHING and q.sha(w2.journal) == before == sig["journal_sha256"]
    assert q.signature(w2, again) == sig


def test_03_a_restart_after_a_partly_synchronised_poll_completes_exactly_the_missing_rows(base, tmp_path):
    world0, _, sig = base
    base_rows = q.journal_rows(world0.journal)
    w, r, out = q.run(tmp_path, boundary="death_after_journal_row", nth=10)         # row 10 is the gap's error row; its recovered row is not yet written
    assert out["fired"] and q.signature(w, r) == sig
    report = json.loads(w.recovery_log.read_bytes().split(NL)[0])
    assert report["status"] == rec.ST_RECOVERED and report["journal"]["rows_before"] == 10 and report["completed_rows_were_in_the_journal_before_restart"] is False
    assert [c["kind"] for c in report["completed_after_restart"]][:1] == ["recovered"]
    assert report["completed_after_restart"][0]["t_utc"] == base_rows[10]["t_utc"]                  # the ORIGINAL read time, not the restart time
    body = {k: v for k, v in report.items() if k != "self_sha256"}
    assert report["self_sha256"] == hashlib.sha256(wc.canonical(body).encode("utf-8")).hexdigest()


def test_04_a_journal_that_is_behind_the_checkpoint_is_completed_at_the_original_read_time(base, tmp_path):
    world0, _, sig = base
    w, r, out = q.run(tmp_path, boundary="death_after_checkpoint_before_sync", nth=5)
    assert out["fired"] and q.signature(w, r) == sig
    report = json.loads(w.recovery_log.read_bytes().split(NL)[0])
    base_rows = q.journal_rows(world0.journal)
    assert report["completed_after_restart"] and report["completed_after_restart"][0]["t_utc"] == base_rows[report["journal"]["rows_before"]]["t_utc"]
    assert report["recovery_wall_utc_note"] == "when the restart happened; it is NEVER used as an observation time"


def test_05_a_restart_after_the_checkpoint_and_the_sync_finished_has_nothing_to_complete(base, tmp_path):
    w = partial_world(tmp_path, 9)
    again = w.reopened()
    assert again.recovery_report["status"] == rec.ST_NOTHING and again.recovery_report["completed_after_restart"] == []
    assert again.recovery_report["journal"]["rows_before"] == again.recovery_report["journal"]["rows_after"] == 9


# ----------------------------------------------------------------------------------------------------------------------------- 6-12 replay, times, duplicates
def test_06_source_growth_during_downtime_is_read_at_the_restart_time_and_is_not_eligible_earlier(base, tmp_path):
    world0, _, sig = base
    w = partial_world(tmp_path, 10)
    before_attempts = None
    r0 = w.reopened()
    before_attempts = [(e["id"], e["read_ms"]) for e in r0.tail.attempt_events(r0.bridge.instance)]
    for i in (10, 11, 12):                                                              # the generator keeps writing while the reader is down
        w.apply(i)
    r1 = w.reopened()
    r1.poll(at(40.0))                                                                   # restart at a LATER time than the scheduled one
    events = r1.tail.attempt_events(r1.bridge.instance)
    assert [(e["id"], e["read_ms"]) for e in events][:len(before_attempts)] == before_attempts          # original read times preserved
    assert all(e["read_ms"] == ms(40.0) for e in events[len(before_attempts):]) and len(events) > len(before_attempts)
    early = r1.evidence(as_of=at(12.0))
    late = r1.evidence(as_of=at(40.0))
    assert early["counts"]["attempts"] == len(before_attempts) and early["cutoff"]["records_read_after_cutoff_excluded"] > 0           # not eligible before it was read
    assert late["counts"]["attempts"] == len(events)
    base_rows, rows = q.journal_rows(world0.journal), q.journal_rows(w.journal)
    prefix = [row for row in rows if row["t_utc"] <= wc.iso(at(10.0))]
    assert prefix == [row for row in base_rows if row["t_utc"] <= wc.iso(at(10.0))]                    # the pre-crash journal is the uninterrupted one
    assert max(row["t_utc"] for row in rows) == wc.iso(at(40.0))                                      # later rows carry the later read time


def test_07_the_time_limited_view_shows_only_what_had_been_read(base):
    world, reader, _ = base
    tail = sr.RoundBoundTail.resume(world.log_path, reader.policy, world.ckpt, round_index=q.ROUND)
    start_read = rec._read_time_of_record(tail._marks, 1)
    stop_read = rec._read_time_of_record(tail._marks, tail._rseq)
    early = rec.TimeLimitedTailView(tail, ms(3.5), start_read_ms=start_read, stop_read_ms=stop_read)
    late = rec.TimeLimitedTailView(tail, ms(60.0), start_read_ms=start_read, stop_read_ms=stop_read)
    assert early.start_record is not None and early.stop_record is None and late.stop_record is not None
    assert 0 < early.segment_count < late.segment_count == tail.segment_count
    assert len(early._attempt_ms) < len(late._attempt_ms) == len(tail._attempt_ms)
    assert all(s_read <= ms(3.5) for *_x, s_read in early.liveness_points_since(0))
    nothing = rec.TimeLimitedTailView(tail, ms(-1.0), start_read_ms=start_read, stop_read_ms=stop_read)
    assert nothing.start_record is None and nothing.segment_count == 0 and len(nothing._attempt_ms) == 0
    for forbidden in ("attempt_events", "counts", "summary", "observed_segments", "poll"):                       # a bridge that reached for more than it should fails loudly
        assert not hasattr(early, forbidden)


def test_08_original_read_times_survive_a_restart_and_none_is_the_restart_time(base, tmp_path):
    world0, reader0, sig = base
    w, r, out = q.run(tmp_path, boundary="death_after_sync", nth=12)
    assert [(e[0], e[2]) for e in q.signature(w, r)["attempts"]] == [(e[0], e[2]) for e in sig["attempts"]]
    assert {m_[1] for m_ in r.tail._marks} == {m_[1] for m_ in reader0.tail._marks}
    assert all(e[2] in {m_[1] for m_ in reader0.tail._marks} for e in sig["attempts"])


def test_09_no_decision_cutoff_sees_a_record_that_was_read_after_it(base, tmp_path):
    world0, reader0, sig = base
    w, r, _ = q.run(tmp_path, boundary="death_after_checkpoint_before_sync", nth=11)
    cov = gc.GeneratorIssueCoverage.from_bridge(r.bridge, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0))
    for second in (2, 5, 9, 14, 18):
        snap = cov.snapshot(at(second))
        events_by_cutoff = [e for e in r.tail.attempt_events(r.bridge.instance, as_of_ms=ms(second))]
        assert all(e["read_ms"] <= ms(second) for e in events_by_cutoff)
        assert [e for e in q.journal_rows(w.journal) if e["t_utc"] <= wc.iso(at(second))] == [e for e in q.journal_rows(world0.journal) if e["t_utc"] <= wc.iso(at(second))]
        assert snap.valid


def test_10_restarting_repeatedly_neither_duplicates_nor_skips_anything(base, tmp_path):
    world0, _, sig = base
    w = partial_world(tmp_path, 14)
    for _ in range(4):
        again = w.reopened()
        assert again.recovery_report["status"] == rec.ST_NOTHING
    before = q.sha(w.journal)
    for i in range(14, len(w.steps)):
        w.apply(i)
        again.poll(at(w.steps[i].poll))
    assert q.signature(w, again) == sig
    rows = q.journal_rows(w.journal)
    assert [r_["seq"] for r_ in rows] == list(range(1, len(rows) + 1)) and before != q.sha(w.journal)
    assert [r_["kind"] for r_ in rows].count("start") == 1 and [r_["kind"] for r_ in rows].count("stop") == 1


def test_11_source_sequence_and_hash_chain_stay_continuous_through_recovery(base, tmp_path):
    w, r, out = q.run(tmp_path, boundary="journal_torn_write", nth=6)
    lines = Path(w.journal).read_bytes().split(NL)[:-1]
    rows, head = rec._verify_chain(lines)
    assert [x["seq"] for x in rows] == list(range(1, len(rows) + 1)) and rows[0]["prev"] == GENESIS and head == rows[-1]["sha256"]
    assert r.tail.counts()["records"] == r.tail._rseq == len(Path(w.log_path).read_bytes().split(NL)[:-1])
    assert r.tail.chain_head == json.loads(Path(w.log_path).read_bytes().split(NL)[-2])["sha256"]
    assert r.tail.error is None


def test_12_insufficient_recovery_evidence_is_refused_with_a_precise_reason(tmp_path):
    w = partial_world(tmp_path, 8)
    marks_before = Path(w.ckpt).read_bytes()
    rewrite_checkpoint(w.ckpt, lambda d: d.update(marks=d["marks"][:-1]))               # the read time of the last records is no longer known
    expect_refused(w, "checkpoint_mismatch")
    Path(w.ckpt).write_bytes(marks_before)
    rewrite_checkpoint(w.ckpt, lambda d: d.update(marks=[[end, stamp - 5_000_000] for end, stamp in d["marks"]]))      # read times moved earlier but still self-consistent
    expect_refused(w, "journal_diverges_from_replay")                                  # the journal rows carry the true read times: the lie is found, not trusted
    Path(w.ckpt).write_bytes(marks_before)
    Path(w.ckpt).unlink()
    expect_refused(w, "journal_without_checkpoint")
    (tmp_path / "solo_dir").mkdir()
    solo = q.World(tmp_path / "solo_dir", "s")
    other = solo.new_reader()
    with pytest.raises(rec.BridgeRecoveryError) as info:
        other.restart()
    assert info.value.code == "checkpoint_missing"


# ----------------------------------------------------------------------------------------------------------------------------- 13-24 fault injection
def test_13_a_partial_source_row_is_never_consumed_and_the_journal_is_unchanged(tmp_path):
    w = partial_world(tmp_path, 11)
    w.apply(11)                                                                         # the step after the 7 s gap: transactions and a tick
    data = Path(w.log_path).read_bytes()
    Path(w.log_path).write_bytes(data[:-30])                                           # the generator was killed in the middle of its last record
    r = w.reopened()
    journal_before = q.sha(w.journal)
    entry = r.poll(at(15.2))
    assert entry["status"] == ilr.S_PROGRESS and entry["partial_tail_bytes"] > 0       # the complete records were read, the torn one was not consumed
    assert r.tail.offset == len(data[:-30]) - entry["partial_tail_bytes"]
    Path(w.log_path).write_bytes(data)                                                 # the writer finished the record
    entry = r.poll(at(15.5))
    assert entry["status"] == ilr.S_PROGRESS and r.tail.error is None and r.tail.offset == len(data)


def test_14_a_torn_journal_tail_is_removed_only_when_provably_a_prefix_of_the_next_row(tmp_path):
    w, r, out = q.run(tmp_path, boundary="journal_torn_write", nth=5)
    report = json.loads(w.recovery_log.read_bytes().split(NL)[0])
    assert report["journal"]["torn_tail_bytes_removed"] > 0 and report["journal"]["torn_tail_fragment_sha256"]
    other = tmp_path / "other"
    other.mkdir()
    w2 = partial_world(other, 8)
    with Path(w2.journal).open("ab") as handle:
        handle.write(b'{"schema":"collector_health_event/1","seq":')                        # a fragment that is NOT the next row
    expect_refused(w2, "journal_torn_tail_not_provable")
    third = tmp_path / "third"
    third.mkdir()
    w3 = partial_world(third, 8)
    with Path(w3.journal).open("ab") as handle:
        handle.write(b'{"x"')
    with pytest.raises(rec.BridgeRecoveryError) as info:
        w3.reopened(torn_tail_policy=rec.TORN_REFUSE)
    assert info.value.code == "journal_torn_tail_unreviewed"


def test_14b_a_torn_write_inside_an_idle_poll_sync_is_not_recoverable_and_says_so(tmp_path):
    """The one crash boundary the matrix does not recover: the first journal row of an IDLE poll's sync (no checkpoint mark, no complete row) leaves a fragment whose read time is
    recorded nowhere, so a replay cannot prove it. Refused (nothing is changed), never guessed."""
    with pytest.raises(rec.BridgeRecoveryError) as info:
        q.run(tmp_path, boundary="journal_torn_write", nth=14)
    assert info.value.code == "journal_torn_tail_not_provable"


def test_15_a_truncated_source_log_is_refused_and_nothing_is_written(tmp_path):
    w = partial_world(tmp_path, 12)
    data = Path(w.log_path).read_bytes()
    Path(w.log_path).write_bytes(data[:-150])
    expect_refused(w, "log_truncated")


def test_16_a_replaced_or_edited_source_log_is_refused_and_nothing_is_written(tmp_path):
    w = partial_world(tmp_path, 12)
    data = bytearray(Path(w.log_path).read_bytes())
    data[200] = ord("7") if data[200] != ord("7") else ord("8")
    Path(w.log_path).write_bytes(bytes(data))
    with pytest.raises(ilr.IssueLogError) as info:
        w.reopened()
    assert info.value.code in {"record_not_canonical", "hash_mismatch", "record_malformed", "hash_chain_broken", "log_replaced", "record_field_invalid"}
    other = tmp_path / "o"
    other.mkdir()
    w2 = partial_world(other, 12)
    same_bytes = Path(w2.log_path).read_bytes()
    Path(w2.log_path).unlink()
    Path(w2.log_path).write_bytes(same_bytes)                                           # byte-identical content, but a different file
    with pytest.raises(ilr.IssueLogError) as info:
        w2.reopened()
    assert info.value.code == "log_replaced"


def test_17_a_corrupted_checkpoint_is_refused(tmp_path):
    w = partial_world(tmp_path, 9)
    good = Path(w.ckpt).read_bytes()
    Path(w.ckpt).write_bytes(b"not json at all")
    with pytest.raises(ilr.IssueLogError) as info:
        w.reopened()
    assert info.value.code == "checkpoint_unreadable"
    Path(w.ckpt).write_bytes(good.replace(b'"offset":', b'"offset":1', 1))
    with pytest.raises(ilr.IssueLogError) as info:
        w.reopened()
    assert info.value.code == "checkpoint_corrupt"
    Path(w.ckpt).write_bytes(good)
    assert w.reopened().recovery_report["status"] == rec.ST_NOTHING                      # the refusals changed nothing


def test_18_a_checkpoint_and_journal_that_disagree_are_refused(tmp_path):
    w = partial_world(tmp_path, 6)
    older = Path(w.ckpt).read_bytes()
    for i in range(6, 12):
        w.apply(i)
    r = w.reopened()
    for i in range(6, 12):
        r.poll(at(w.steps[i].poll))
    Path(w.ckpt).write_bytes(older)                                                      # a stale checkpoint restored over a longer journal
    expect_refused(w, "journal_ahead_of_checkpoint")
    other = tmp_path / "b"
    other.mkdir()
    w2 = partial_world(other, 9)
    rechain(w2.journal, lambda i, row: row.update(events_received=row["events_received"] + 1) if row["kind"] == "heartbeat" and i == 4 else None)
    expect_refused(w2, "journal_diverges_from_replay")                                   # a re-chained edit still disagrees with what the recorded reads produce


def test_18b_an_edited_dropped_or_reordered_journal_row_breaks_the_chain_and_is_refused(tmp_path):
    w = partial_world(tmp_path, 9)
    good = Path(w.journal).read_bytes()
    lines = good.split(NL)[:-1]
    edited = list(lines)
    edited[3] = edited[3].replace(b'"events_received":', b'"events_received":9', 1)                  # content changed, digest not recomputed
    for label, variant in (("edited", edited), ("dropped", lines[:2] + lines[3:]), ("reordered", lines[:2] + [lines[3], lines[2]] + lines[4:])):
        Path(w.journal).write_bytes(NL.join(variant) + NL)
        expect_refused(w, "journal_chain_broken")
    Path(w.journal).write_bytes(good)
    assert w.reopened().recovery_report["status"] == rec.ST_NOTHING


def crashed_world(tmp: Path, boundary: str, nth: int, name: str = "w"):
    """The script run until the scripted death, with NO recovery attempted: the files are exactly what the crash left."""
    world = q.World(tmp, name)
    reader = world.new_reader()
    mp = pytest.MonkeyPatch()
    q.install(mp, boundary, nth)
    try:
        for i in range(len(world.steps)):
            world.apply(i)
            reader.poll(at(world.steps[i].poll))
    except (Death, OSError):
        pass
    finally:
        mp.undo()
    return world


def test_18c_a_fragment_is_accepted_only_if_it_is_a_prefix_of_a_row_the_replay_proves_is_missing(tmp_path):
    w = crashed_world(tmp_path, "death_after_checkpoint_before_sync", 5)                  # the journal is one sync behind: a row IS pending
    clean = Path(w.journal).read_bytes()
    with Path(w.journal).open("ab") as handle:
        handle.write(b'{"schema":"collector_health_event/1","seq":')                       # plausible-looking, but not the pending row
    expect_refused(w, "journal_torn_tail_not_provable")
    Path(w.journal).write_bytes(clean)
    assert w.reopened().recovery_report["completed_after_restart"]                           # without the fragment the same journal is completed


def test_18d_unusable_read_marks_are_refused_even_when_the_checkpoint_digest_is_valid(tmp_path):
    w = partial_world(tmp_path, 9)
    good = Path(w.ckpt).read_bytes()
    rewrite_checkpoint(w.ckpt, lambda d: d.update(marks=[[end, stamp - (index * 5000)] for index, (end, stamp) in enumerate(d["marks"])]))   # read times that go backwards
    expect_refused(w, "marks_invalid")
    Path(w.ckpt).write_bytes(good)
    rewrite_checkpoint(w.ckpt, lambda d: d["marks"].append([d["marks"][-1][0] + 5, d["marks"][-1][1] + 1000]))                               # a mark for records that do not exist
    expect_refused(w, "marks_incomplete")


def test_18e_a_row_of_another_collector_instance_or_a_regressed_time_is_refused_with_its_own_reason(tmp_path):
    w = partial_world(tmp_path, 9)
    good = Path(w.journal).read_bytes()
    rechain(w.journal, lambda i, row: row.update(collector_instance="issue-log-reader:run-a0j:w0:r7") if i == 4 else None)
    expect_refused(w, "journal_identity_mismatch")
    Path(w.journal).write_bytes(good)
    rechain(w.journal, lambda i, row: row.update(t_utc=wc.iso(at(0.0))) if i == 5 else None)                  # a row stamped earlier than the one before it
    expect_refused(w, "journal_time_regressed")


def test_19_a_checkpoint_for_another_round_is_refused(tmp_path):
    w = partial_world(tmp_path, 9)
    reader = rec.RecoverableSourceCountReader(ident(w.log_path, round_index=2, checkpoint=w.ckpt), policy_limits=dict(q.LIMITS), journal_path=w.journal, source_id=q.SOURCE_ID,
                                              origin=gc.ORIGIN_SYNTHETIC)
    with pytest.raises(ilr.IssueLogError) as info:
        reader.restart()
    assert info.value.code == "checkpoint_round_mismatch"


def test_20_another_worker_is_refused_by_identity_and_by_the_journal(tmp_path):
    w = partial_world(tmp_path, 9)
    with pytest.raises(sr.SourceCountReaderError) as info:
        ident(w.log_path, round_index=q.ROUND, checkpoint=w.ckpt, worker="w1")
    assert info.value.code == "worker_count_unsupported"
    rechain(w.journal, lambda i, row: row["issue_log"].update(worker_id="w1") if i == 1 else None)
    expect_refused(w, "journal_identity_mismatch")


def test_21_another_run_is_refused(tmp_path):
    w = partial_world(tmp_path, 9)
    other = rec.RecoverableSourceCountReader(ident(w.log_path, round_index=q.ROUND, checkpoint=w.ckpt, run_id="another-run"), policy_limits=dict(q.LIMITS),
                                             journal_path=w.journal, source_id=q.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC)
    with pytest.raises(ilr.IssueLogError) as info:
        other.restart()
    assert info.value.code in {"checkpoint_mismatch", "run_identity_mismatch"}
    rechain(w.journal, lambda i, row: row.update(run_id="another-run"))
    expect_refused(w, "journal_identity_mismatch")


def test_22_a_clock_that_steps_back_across_the_restart_is_not_accepted(tmp_path):
    w = partial_world(tmp_path, 12)
    r = w.reopened()
    last = r.last_read_ms
    entry = r.poll(at(1.0))                                                              # an earlier wall clock than evidence already recorded
    assert last > ms(1.0) and entry["error_code"] == "read_clock_regressed" and r.tail.error is not None
    ev = r.evidence()
    assert ev["status"] == sr.ST_INVALID and ev["counts"]["attempts"] is None
    other = tmp_path / "o"
    other.mkdir()
    w2 = partial_world(other, 18)                                                       # the last journal row was written by an idle poll at 21.0 (no checkpoint mark)
    assert q.journal_rows(w2.journal)[-1]["t_utc"] == wc.iso(at(21.0))
    r3 = w2.reopened()
    assert r3.last_read_ms == ms(21.0) > r3.tail._marks[-1][1]                          # the restart never lowers the latest time already on record
    assert r3.poll(at(20.5))["error_code"] == "read_clock_regressed"


def test_23_a_write_failure_is_never_continued_in_process_and_restart_gives_the_uninterrupted_journal(base, tmp_path):
    _, _, sig = base
    w = q.World(tmp_path, "w")
    reader = w.new_reader()
    import pytest as _pt
    mp = _pt.MonkeyPatch()
    q.install(mp, "journal_write_failure", 4)
    try:
        for i in range(len(w.steps)):
            w.apply(i)
            try:
                reader.poll(at(w.steps[i].poll))
            except OSError:
                mp.undo()
                with pytest.raises(rec.BridgeRecoveryError) as info:
                    reader.poll(at(w.steps[i].poll))
                assert info.value.code == "restart_required"                               # the in-memory bridge state is unknown: no further poll is accepted
                reader.restart()
                reader.poll(at(w.steps[i].poll))
    finally:
        mp.undo()
    assert q.signature(w, reader) == sig and reader.restarts == 1


def test_24_a_sync_failure_leaves_a_complete_row_that_the_restart_recognises_and_does_not_repeat(base, tmp_path):
    _, _, sig = base
    w, r, out = q.run(tmp_path, boundary="journal_sync_failure", nth=7, in_process_restart=True)
    assert out["exception"] == "OSError" and q.signature(w, r) == sig
    report = json.loads(w.recovery_log.read_bytes().split(NL)[0])
    assert report["journal"]["rows_before"] == 7 and report["journal"]["torn_tail_bytes_removed"] == 0          # the row IS on disk; it is kept, not rewritten
    rows = q.journal_rows(w.journal)
    assert len({x["sha256"] for x in rows}) == len(rows)


# ----------------------------------------------------------------------------------------------------------------------------- unsupported states
def test_24b_a_restart_after_an_integrity_failure_is_refused(tmp_path):
    w = partial_world(tmp_path, 9)
    r = w.reopened()
    w.apply(9)
    w.apply(10)
    w.apply(11)
    data = Path(w.log_path).read_bytes().split(NL)
    row = json.loads(data[-2])                                                           # a NEW record is edited before it is first read
    row["t_ms"] += 7
    data[-2] = wc.canonical(row).encode("ascii")
    Path(w.log_path).write_bytes(NL.join(data))
    r.poll(at(15.5))
    rows = q.journal_rows(w.journal)
    assert r.tail.error is not None and any(x["kind"] == "error" and x.get("error_code") == "issue_log_invalid" for x in rows)
    expect_refused(w, "restart_after_integrity_failure")
    other = tmp_path / "o"
    other.mkdir()
    w2 = partial_world(other, 9)
    r2 = w2.reopened()
    edited = Path(w2.log_path).read_bytes().split(NL)
    changed = json.loads(edited[-2])
    changed["t_ms"] += 7
    edited[-2] = wc.canonical(changed).encode("ascii")
    Path(w2.log_path).write_bytes(NL.join(edited))                                       # an already-consumed record is edited: the re-verification from byte 0 refuses it
    with pytest.raises(ilr.IssueLogError) as info:
        w2.reopened()
    assert info.value.code in {"hash_mismatch", "record_not_canonical", "log_replaced"}


def test_24c_polls_that_shared_one_read_time_are_refused_not_guessed(tmp_path):
    """The checkpoint merges polls at one read time, so a journal written by several same-time polls (one heartbeat each) cannot be rebuilt by one simulated sync: the replay
    differs and recovery refuses."""
    world = q.World(tmp_path, "s")
    for i in range(len(world.steps)):
        world.apply(i)
    limits = {**q.LIMITS, "max_records_per_poll": 7}
    mk = lambda: rec.RecoverableSourceCountReader(world.identity(), policy_limits=limits, journal_path=world.journal, source_id=q.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC)
    reader = mk()
    reader.poll(at(-0.4))                                                                # observation starts early, so later chunks write heartbeats
    reader.poll_until_drained(at(40.0))
    rows = q.journal_rows(world.journal)
    heartbeats_at_40 = [x for x in rows if x["kind"] == "heartbeat" and x["t_utc"] == wc.iso(at(40.0))]
    assert len(heartbeats_at_40) > 1                                                     # several rows at one read time
    with pytest.raises(rec.BridgeRecoveryError) as info:
        mk().restart()
    assert info.value.code in {"journal_ahead_of_checkpoint", "journal_diverges_from_replay"}


def test_24d_a_coverage_scope_or_origin_change_is_refused(tmp_path):
    w = partial_world(tmp_path, 9)
    rechain(w.journal, lambda i, row: row.update(coverage_scope="recorder_callback") if i == 1 else None)
    expect_refused(w, "coverage_scope_mismatch")
    other = tmp_path / "x"
    other.mkdir()
    w2 = partial_world(other, 9)
    with pytest.raises(rec.BridgeRecoveryError) as info:
        rec.RecoverableSourceCountReader(w2.identity(), policy_limits=dict(q.LIMITS), journal_path=w2.journal, source_id=q.SOURCE_ID, origin=gc.ORIGIN_LIVE).restart()
    assert info.value.code == "origin_mismatch"


def test_24e_an_old_checkpoint_without_round_binding_is_refused_and_never_rewritten(tmp_path):
    w = partial_world(tmp_path, 9)
    rewrite_checkpoint(w.ckpt, lambda d: (d.pop("round_index"), d.pop("reader_instance")))
    before = Path(w.ckpt).read_bytes()
    with pytest.raises(ilr.IssueLogError) as info:
        w.reopened()
    assert info.value.code == "checkpoint_round_mismatch" and Path(w.ckpt).read_bytes() == before


def test_24f_a_protected_channel_is_refused_before_any_file_is_touched(tmp_path):
    w = q.World(tmp_path, "w")
    for name in ("e1-r002", "e1-cfg001"):
        with pytest.raises(Exception) as info:
            w.new_reader(channel_id=name)
        assert "channel" in type(info.value).__name__.lower() or "protected" in str(info.value).lower() or "refus" in str(info.value).lower()
    assert not w.journal.exists() and not w.ckpt.exists()


# ----------------------------------------------------------------------------------------------------------------------------- crash matrix
def test_25_the_whole_crash_matrix_recovers_to_the_uninterrupted_evidence(tmp_path):
    result = q.run_matrix(tmp_path)
    counts = result["counts"]
    assert counts.get("DIFFERENT", 0) == 0
    assert counts["IDENTICAL"] >= 140 and counts["REFUSED"] == 1 and counts["NOT_REACHED"] == 19
    refused = [e for e in result["results"] if e["outcome"] == "REFUSED"]
    assert [(e["boundary"], e["n"], e["code"]) for e in refused] == [("journal_torn_write", 14, "journal_torn_tail_not_provable")]
    boundaries = {e["boundary"] for e in result["results"] if e["outcome"] == "IDENTICAL"}
    assert boundaries == set(q.BOUNDARIES)
