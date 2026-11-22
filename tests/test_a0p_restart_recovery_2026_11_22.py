"""A0p workstream C (checklist items 23-30): restart-safe recovery of the incremental recorder-health bridge at every crash boundary.

A crash is a ``BaseException`` raised by a fault hook at a named boundary (the process "dies" there); the bridge is then rebuilt from the files with ``resume``. SYNTHETIC scripted spools and
clocks only."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from tests.a0p_support import (FAST, NL, SOURCE_ID, BridgeClock, SimulatedCrash, bridge, bridge_config, crash_at, create_bridge, drain_after_crash, j, lines_of, resume_bridge,
                               rhi, spool_recorder, td, whole_file_journal, wc, write_lines)

BridgeError = bridge.BridgeError


def stepped_recorder(tmp: Path):
    """The 20-second fixture with a recorder wall-clock step from row 4 on: the spool row 4 produces TWO journal rows (a bridge-detected error, then the heartbeat)."""
    rec = spool_recorder(tmp)
    rows = [json.loads(line) for line in lines_of(rec.spool_path)]
    for row in rows[3:]:
        row["mono_ms"] += 5000.0
    write_lines(rec.spool_path, [json.dumps(r).encode("ascii") for r in rows])
    return rec


def finish(cfg, clock=None):
    b, _ = resume_bridge(cfg, clock)
    b.poll_until_drained()
    return b, b.close(source_ended=True)


def check_complete_and_exact(rec, cfg, tmp, expect_complete=True):
    """The journal equals the whole-file journal (no duplicate, no skip, same chain) and every journal row is covered by an ingest stamp or a recovery row."""
    assert cfg.journal_path.read_bytes() == whole_file_journal(rec, tmp, "ref_" + cfg.journal_path.name.split(".")[0])
    events, torn, _ = wc.read_journal_file(cfg.journal_path)
    assert not torn and [e["seq"] for e in events] == list(range(1, len(events) + 1))
    ledger = td.read_ingest_ledger(cfg.ledger_path)
    covered = set()
    for row in ledger["rows"]:
        if row["kind"] in ("ingest", "recovery") and row.get("journal_seq_last") is not None:
            covered |= set(range(row["journal_seq_first"], row["journal_seq_last"] + 1))
    assert covered == set(range(1, len(events) + 1))
    spool_n = [e["spool_n"] for e in events]
    assert spool_n == sorted(spool_n) and set(spool_n) == set(range(1, max(spool_n) + 1))             # no spool row skipped, none duplicated out of order
    return events, ledger


# ----------------------------------------------------------------------------------------------------------------------------- 23 before append
@pytest.mark.parametrize("point,occurrence", [("poll_start", 1), ("poll_start", 3), ("after_read", 1), ("after_read", 2), ("journal_before_write", 1), ("journal_before_write", 9)])
def test_23_a_restart_before_the_journal_append_loses_nothing_and_duplicates_nothing(tmp_path, point, occurrence):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=3)
    crashed, _ = drain_after_crash(cfg, crash_at(point, occurrence))
    assert crashed is True
    b, report = finish(cfg)
    events, ledger = check_complete_and_exact(rec, cfg, tmp_path)
    assert report["complete"] is True and len(events) == 22
    assert not [r for r in ledger["rows"] if r["kind"] == "recovery"]                    # nothing was appended without a stamp: there is nothing to recover


# ----------------------------------------------------------------------------------------------------------------------------- 24 after append
@pytest.mark.parametrize("point,occurrence", [("journal_after_write", 1), ("journal_after_write", 5), ("after_journal_append", 2), ("after_journal_append", 11)])
def test_24_a_restart_after_the_journal_append_records_the_ingest_time_as_unknown_not_as_the_recovery_time(tmp_path, point, occurrence):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=3)
    crashed, _ = drain_after_crash(cfg, crash_at(point, occurrence))
    assert crashed is True
    b, report = finish(cfg)
    events, ledger = check_complete_and_exact(rec, cfg, tmp_path)
    recovery = [r for r in ledger["rows"] if r["kind"] == "recovery"]
    assert len(recovery) == 1 and recovery[0]["ingest_time"] == "unknown" and "NOT the ingest time" in recovery[0]["note"]
    assert {"action": "ledger_recovery_row_written", "journal_rows_without_stamp": 1} in report["recovery_actions"]
    result = td.run_sampler_a0p(origin="synthetic_fixture", spool=rec.spool_path, ingest_ledger=cfg.ledger_path)["measurements"]["T06"]
    assert result["unknown_ingest_rows"] == 1 and any(e["reason"] == "missing: ingest_time_unknown" for e in result["exclusions"])      # never filled with an estimate


# ----------------------------------------------------------------------------------------------------------------------------- 25 before checkpoint
@pytest.mark.parametrize("point,occurrence", [("after_ledger_append", 4), ("before_checkpoint", 1), ("before_checkpoint", 3)])
def test_25_a_restart_before_the_checkpoint_is_persisted_rebuilds_it_from_the_journal(tmp_path, point, occurrence):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=3)
    crashed, _ = drain_after_crash(cfg, crash_at(point, occurrence))
    assert crashed is True
    stale = json.loads(cfg.checkpoint_path.read_text(encoding="ascii"))
    b, report = finish(cfg)
    events, ledger = check_complete_and_exact(rec, cfg, tmp_path)
    resumed = [a for a in report["recovery_actions"] if a["action"] == "resumed"][0]
    assert resumed["checkpoint_was_stale"] is True and stale["journal_records"] < len(events)
    fresh = json.loads(cfg.checkpoint_path.read_text(encoding="ascii"))
    assert fresh["journal_records"] == 22 and fresh["journal_last_sha256"] == events[-1]["sha256"] and fresh["spool_last_n"] == 22


def test_25b_a_crash_after_the_checkpoint_resumes_from_it_exactly(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=3)
    crashed, _ = drain_after_crash(cfg, crash_at("after_checkpoint", 2))
    assert crashed is True
    b, report = finish(cfg)
    events, ledger = check_complete_and_exact(rec, cfg, tmp_path)
    assert [a for a in report["recovery_actions"] if a["action"] == "resumed"][0]["checkpoint_was_stale"] is False
    assert not [r for r in ledger["rows"] if r["kind"] == "recovery"]


# ----------------------------------------------------------------------------------------------------------------------------- 26 stale checkpoint
def test_26_a_stale_checkpoint_is_reconciled_with_the_journal_not_trusted(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=4)
    b, _ = create_bridge(cfg)
    b.poll()
    old = cfg.checkpoint_path.read_bytes()
    b.poll()
    b.poll()
    cfg.checkpoint_path.write_bytes(old)                                              # a checkpoint from two polls ago comes back (a restored backup, a lost write)
    resumed, report = finish(cfg)
    check_complete_and_exact(rec, cfg, tmp_path)
    assert [a for a in report["recovery_actions"] if a["action"] == "resumed"][0]["checkpoint_was_stale"] is True


# ----------------------------------------------------------------------------------------------------------------------------- 27 conflicts
def _rewrite_checkpoint(path: Path, **changes) -> None:
    body = {k: v for k, v in json.loads(path.read_text(encoding="ascii")).items() if k != "self_sha256"}
    body.update(changes)
    path.write_text(wc.canonical({**body, "self_sha256": hashlib.sha256(wc.canonical(body).encode("utf-8")).hexdigest()}) + "\n", encoding="ascii")


def test_27_a_checkpoint_that_disagrees_with_the_journal_is_never_followed(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=6)
    b, _ = create_bridge(cfg)
    b.poll()
    b.poll()
    snapshot = {p.name: p.read_bytes() for p in (cfg.journal_path, cfg.ledger_path, cfg.checkpoint_path)}
    _rewrite_checkpoint(cfg.checkpoint_path, journal_last_sha256="a" * 64)
    with pytest.raises(BridgeError) as conflict:
        resume_bridge(cfg)
    assert conflict.value.code == "journal_checkpoint_conflict"
    cfg.checkpoint_path.write_bytes(snapshot[cfg.checkpoint_path.name])
    _rewrite_checkpoint(cfg.checkpoint_path, journal_records=40)
    with pytest.raises(BridgeError) as behind:
        resume_bridge(cfg)
    assert behind.value.code == "journal_behind_checkpoint"
    cfg.checkpoint_path.write_bytes(snapshot[cfg.checkpoint_path.name])
    cfg.checkpoint_path.write_text(cfg.checkpoint_path.read_text().replace("spool_offset", "spool_offsex"))
    with pytest.raises(BridgeError) as corrupt:
        resume_bridge(cfg)
    assert corrupt.value.code == "checkpoint_corrupt"
    cfg.checkpoint_path.unlink()
    with pytest.raises(BridgeError) as missing:
        resume_bridge(cfg)
    assert missing.value.code == "checkpoint_missing"
    assert {p.name: p.read_bytes() for p in (cfg.journal_path, cfg.ledger_path)} == {k: v for k, v in snapshot.items() if k != cfg.checkpoint_path.name}     # refusals wrote nothing


def test_27b_a_journal_shorter_than_the_checkpoint_or_a_spool_that_no_longer_produces_it_fails_closed(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=8)
    b, _ = create_bridge(cfg)
    b.poll()
    lines = lines_of(cfg.journal_path)
    write_lines(cfg.journal_path, lines[:5])
    with pytest.raises(BridgeError) as short:
        resume_bridge(cfg)
    assert short.value.code == "journal_behind_checkpoint"
    other = tmp_path / "o"
    other.mkdir()
    rec2 = spool_recorder(other)
    cfg2 = bridge_config(rec2, other, max_rows_per_poll=8)
    b2, _ = create_bridge(cfg2)
    b2.poll()
    data = rec2.spool_path.read_bytes()
    rec2.spool_path.write_bytes(data.replace(b'"errors_since_last": 0', b'"errors_since_last": 1', 1))         # same length, different content
    with pytest.raises(BridgeError) as replaced:
        resume_bridge(cfg2)
    assert replaced.value.code == "spool_replaced"
    truncated = tmp_path / "t"
    truncated.mkdir()
    rec3 = spool_recorder(truncated)
    cfg3 = bridge_config(rec3, truncated, max_rows_per_poll=8)
    create_bridge(cfg3)[0].poll()
    rec3.spool_path.write_bytes(rec3.spool_path.read_bytes()[:1500])
    with pytest.raises(BridgeError) as cut:
        resume_bridge(cfg3)
    assert cut.value.code == "spool_truncated"


def test_27c_a_journal_row_that_the_spool_does_not_produce_fails_closed(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=8)
    create_bridge(cfg)[0].poll()
    lines = lines_of(cfg.journal_path)
    row = json.loads(lines[2])
    row["events_received"] = 1234                                                      # a self-consistent forgery: re-chained so only the spool can expose it
    forged = []
    prev = "0" * 64
    for index, line in enumerate(lines):
        body = json.loads(line)
        if index == 2:
            body = row
        body = {k: v for k, v in body.items() if k not in ("prev", "sha256")}
        body["prev"] = prev
        body["sha256"] = wc.event_digest(body, prev)
        prev = body["sha256"]
        forged.append(wc.canonical(body).encode("utf-8"))
    write_lines(cfg.journal_path, forged)
    _rewrite_checkpoint(cfg.checkpoint_path, journal_last_sha256=prev)
    with pytest.raises(BridgeError) as exc:
        resume_bridge(cfg)
    assert exc.value.code == "journal_diverges_from_spool"


# ----------------------------------------------------------------------------------------------------------------------------- 28 partial destination append
def test_28_a_torn_journal_tail_that_is_provably_the_bridges_own_interrupted_row_is_removed_and_rewritten(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=3)
    crashed, _ = drain_after_crash(cfg, crash_at("journal_mid_write", 6, torn=40))
    assert crashed is True
    raw = cfg.journal_path.read_bytes()
    assert not raw.endswith(NL) and len(raw.rsplit(NL, 1)[1]) == 40                  # the torn fragment is really there
    b, report = finish(cfg)
    events, ledger = check_complete_and_exact(rec, cfg, tmp_path)
    removed = [a for a in report["recovery_actions"] if a["action"] == "journal_torn_tail_removed"]
    assert len(removed) == 1 and removed[0]["bytes"] == 40 and "strict prefix" in removed[0]["proof"]


def test_28b_a_torn_tail_is_refused_by_policy_or_when_it_is_not_the_bridges_own_row(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=3)
    drain_after_crash(cfg, crash_at("journal_mid_write", 6, torn=40))
    strict = rhi.BridgeConfig(cfg.spool_path, cfg.journal_path, cfg.ledger_path, cfg.checkpoint_path, source_id=SOURCE_ID, run_id=j.RUN, policy=FAST,
                              spool_clock_domain="recorder-host", bridge_clock_domain="bridge-host", max_rows_per_poll=3, torn_tail_policy="refuse")
    with pytest.raises(BridgeError) as refused:
        resume_bridge(strict)
    assert refused.value.code in ("journal_torn_tail_unreviewed", "checkpoint_binding_mismatch")
    garbage = tmp_path / "g"
    garbage.mkdir()
    rec2 = spool_recorder(garbage)
    cfg2 = bridge_config(rec2, garbage, max_rows_per_poll=5)
    create_bridge(cfg2)[0].poll()
    with cfg2.journal_path.open("ab") as handle:
        handle.write(b'{"someone":"else wrote this')
    b, _ = resume_bridge(cfg2)
    out = b.poll()
    assert out["error"]["code"] == "journal_torn_tail_not_provable" and cfg2.journal_path.read_bytes().endswith(b'wrote this')        # kept for review, not deleted


def test_28c_a_journal_durability_failure_is_sticky_and_a_restart_reconciles_the_row_that_was_written(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=4)
    calls = {"n": 0}

    def flaky_fsync(fd):
        calls["n"] += 1
        if calls["n"] == 6:                                                            # creation: ledger sync (1) + checkpoint (2); then journal rows ...
            raise OSError("injected fsync failure")
        import os
        os.fsync(fd)
    clock = BridgeClock()
    b = rhi.IncrementalRecorderBridge.create(cfg, wall_clock=clock.wall, mono_us=clock.mono, fsync_fn=flaky_fsync)
    out = b.poll()
    assert out["status"] == "error" and out["error"]["code"] == "journal_write_failed"
    assert b.poll()["status"] == "error" and b.report()["complete"] is False
    before = len(lines_of(cfg.journal_path))
    resumed, report = finish(cfg)
    events, ledger = check_complete_and_exact(rec, cfg, tmp_path)
    assert before <= len(events) == 22 and report["complete"] is True


# ----------------------------------------------------------------------------------------------------------------------------- 29 contamination
def test_29_a_journal_ledger_or_spool_of_another_run_is_never_continued(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=8)
    create_bridge(cfg)[0].poll()
    foreign_run = rhi.BridgeConfig(cfg.spool_path, cfg.journal_path, cfg.ledger_path, cfg.checkpoint_path, source_id=SOURCE_ID, run_id="another-run", policy=FAST,
                                   spool_clock_domain="recorder-host", bridge_clock_domain="bridge-host", max_rows_per_poll=8)
    with pytest.raises(BridgeError) as run:
        resume_bridge(foreign_run)
    assert run.value.code == "checkpoint_binding_mismatch"
    foreign_domain = rhi.BridgeConfig(cfg.spool_path, cfg.journal_path, cfg.ledger_path, cfg.checkpoint_path, source_id=SOURCE_ID, run_id=j.RUN, policy=FAST,
                                      spool_clock_domain="a-different-host", bridge_clock_domain="bridge-host", max_rows_per_poll=8)
    with pytest.raises(BridgeError) as domain:
        resume_bridge(foreign_domain)
    assert domain.value.code == "checkpoint_binding_mismatch"                           # a clock-domain declaration cannot be changed mid-round
    other = tmp_path / "other"
    other.mkdir()
    rec_other = spool_recorder(other, source_id="another-source")
    cfg_other = rhi.BridgeConfig(rec_other.spool_path, other / "x.journal.jsonl", other / "x.ledger.jsonl", other / "x.ckpt.json", source_id="another-source", run_id=j.RUN,
                                 policy=FAST, spool_clock_domain="recorder-host", bridge_clock_domain="bridge-host", max_rows_per_poll=8)
    create_bridge(cfg_other)[0].poll()
    swapped = tmp_path / "swapped"
    swapped.mkdir()
    for suffix in ("journal.jsonl", "ledger.jsonl", "ckpt.json"):                       # another source's journal under this round's names
        shutil.copyfile(other / f"x.{suffix}", swapped / f"b.{suffix.replace('ckpt.json', 'checkpoint.json')}")
    shutil.copyfile(rec.spool_path, swapped / rec.spool_path.name)
    cfg_swapped = rhi.BridgeConfig(swapped / rec.spool_path.name, swapped / "b.journal.jsonl", swapped / "b.ledger.jsonl", swapped / "b.checkpoint.json", source_id=SOURCE_ID,
                                   run_id=j.RUN, policy=FAST, spool_clock_domain="recorder-host", bridge_clock_domain="bridge-host", max_rows_per_poll=8)
    with pytest.raises(BridgeError) as swap:
        resume_bridge(cfg_swapped)
    assert swap.value.code in ("checkpoint_binding_mismatch", "ledger_identity_mismatch", "journal_identity_mismatch")


# ----------------------------------------------------------------------------------------------------------------------------- 30 deterministic replay
POINTS = ["poll_start", "after_read", "journal_before_write", "journal_after_write", "after_journal_append", "after_ledger_append", "before_checkpoint", "after_checkpoint"]


@pytest.mark.parametrize("builder", ["plain", "stepped"])
def test_30_every_crash_boundary_at_every_row_ends_in_the_same_journal(tmp_path, builder):
    rec = (stepped_recorder if builder == "stepped" else spool_recorder)(tmp_path)
    reference = whole_file_journal(rec, tmp_path, "ref")
    runs = 0
    for point in POINTS:
        for occurrence in (1, 2, 3, 7, 12):
            sub = tmp_path / f"{point}_{occurrence}"
            sub.mkdir()
            shutil.copyfile(rec.spool_path, sub / rec.spool_path.name)
            cfg = rhi.BridgeConfig(sub / rec.spool_path.name, sub / "b.journal.jsonl", sub / "b.ledger.jsonl", sub / "b.checkpoint.json", source_id=SOURCE_ID, run_id=j.RUN,
                                   policy=FAST, spool_clock_domain="recorder-host", bridge_clock_domain="bridge-host", max_rows_per_poll=3)
            crashed, _ = drain_after_crash(cfg, crash_at(point, occurrence))
            if not crashed:
                continue
            b, report = finish(cfg)
            assert cfg.journal_path.read_bytes() == reference, (point, occurrence)
            ledger = td.read_ingest_ledger(cfg.ledger_path)
            assert ledger["has_stop"] and ledger["torn_tail_bytes"] == 0
            runs += 1
    assert runs >= 30                                                                  # the matrix really crashed the bridge many times
    tornruns = 0                                                                       # torn journal writes at several rows, same end state
    for occurrence in (1, 4, 9, 15):
        sub = tmp_path / f"torn_{occurrence}"
        sub.mkdir()
        shutil.copyfile(rec.spool_path, sub / rec.spool_path.name)
        cfg = rhi.BridgeConfig(sub / rec.spool_path.name, sub / "b.journal.jsonl", sub / "b.ledger.jsonl", sub / "b.checkpoint.json", source_id=SOURCE_ID, run_id=j.RUN,
                               policy=FAST, spool_clock_domain="recorder-host", bridge_clock_domain="bridge-host", max_rows_per_poll=3)
        crashed, _ = drain_after_crash(cfg, crash_at("journal_mid_write", occurrence, torn=33))
        assert crashed
        finish(cfg)
        assert cfg.journal_path.read_bytes() == reference
        tornruns += 1
    assert tornruns == 4


def test_30b_a_half_written_two_row_unit_is_completed_after_a_restart(tmp_path):
    rec = stepped_recorder(tmp_path)
    reference = whole_file_journal(rec, tmp_path, "ref")
    events, _, _ = wc.read_journal_file(tmp_path / "ref.journal.jsonl")
    first_error = next(i for i, e in enumerate(events, 1) if e["kind"] == "error")
    assert events[first_error]["spool_n"] == events[first_error - 1]["spool_n"] == 4          # one spool row, two journal rows
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=2)
    crashed, _ = drain_after_crash(cfg, crash_at("journal_after_write", first_error))        # dies after the error row, before the heartbeat row of the same spool row
    assert crashed and len(lines_of(cfg.journal_path)) == first_error
    b, report = finish(cfg)
    assert cfg.journal_path.read_bytes() == reference
    assert {"action": "half_written_unit_completed", "journal_seq": first_error + 1, "spool_n": 4} in report["recovery_actions"]
    ledger = td.read_ingest_ledger(cfg.ledger_path)
    done = [r for r in ledger["rows"] if r["kind"] == "ingest" and r.get("completed_after_restart")]
    assert len(done) == 1 and done[0]["journal_seq_first"] == first_error + 1
    result = td.run_sampler_a0p(origin="synthetic_fixture", ingest_ledger=cfg.ledger_path)["measurements"]["T06"]
    assert any(e["reason"] == "invalid: completed_after_restart" for e in result["exclusions"])        # its read stamp is the recovery time, so it is not used as a poll read


def test_30c_the_resumed_bridge_keeps_the_original_stamps_and_a_second_restart_changes_nothing(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=4)
    b, clock = create_bridge(cfg)
    b.poll()
    stamped = lines_of(cfg.ledger_path)
    resumed, _ = resume_bridge(cfg)
    assert lines_of(cfg.ledger_path)[: len(stamped)] == stamped                          # earlier ingest stamps are never rewritten
    resumed.poll_until_drained()
    resumed.close(source_ended=True)
    before = {p.name: p.read_bytes() for p in (cfg.journal_path, cfg.ledger_path)}
    again, _ = resume_bridge(cfg)
    again.poll_until_drained()
    assert cfg.journal_path.read_bytes() == before[cfg.journal_path.name]
    assert cfg.ledger_path.read_bytes().startswith(before[cfg.ledger_path.name])
