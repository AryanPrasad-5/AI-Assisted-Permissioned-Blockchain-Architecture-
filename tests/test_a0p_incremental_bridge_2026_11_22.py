"""A0p workstream B (checklist items 12-22): the incremental recorder-health bridge. SYNTHETIC scripted spools and a scripted bridge clock; nothing is a measurement and nothing runs
a process, a network call or a model."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pytest

from tests.a0p_support import (FAST, NL, SOURCE_ID, BridgeClock, bridge, bridge_config, create_bridge, j, lines_of, resume_bridge, rhi, short_recorder,
                               spool_recorder, td, whole_file_journal, wc, write_lines)

BridgeError = bridge.BridgeError


def drain(cfg, **kw):
    b, clock = create_bridge(cfg, **kw)
    b.poll_until_drained()
    return b, clock


# ----------------------------------------------------------------------------------------------------------------------------- 12 valid incremental append
def test_12_an_incremental_run_produces_the_whole_file_journal_byte_for_byte(tmp_path):
    rec = spool_recorder(tmp_path)
    whole = whole_file_journal(rec, tmp_path)
    b, _ = drain(bridge_config(rec, tmp_path, max_rows_per_poll=3))
    report = b.close(source_ended=True)
    assert (tmp_path / "b.journal.jsonl").read_bytes() == whole and report["complete"] is True and report["problems"] == []
    assert report["journal_records"] == 22 and report["spool_rows_ingested"] == 22 and report["has_stop"] is True and report["schema"] == "recorder_health_incremental_report/1"
    events, torn, _ = wc.read_journal_file(tmp_path / "b.journal.jsonl")
    assert not torn and [e["seq"] for e in events] == list(range(1, 23)) and {e["spool_n"] for e in events} == set(range(1, 23))
    assert {e["coverage_scope"] for e in events if e["kind"] == "start"} == {"recorder_callback"}            # the same scope: recorder operations, not source completeness


def test_12b_the_whole_file_bridge_is_unchanged_and_not_selected_by_the_incremental_one(tmp_path):
    rec = spool_recorder(tmp_path)
    report = bridge.ingest_spool(rec.spool_path, tmp_path / "w.journal.jsonl", source_id=SOURCE_ID, policy=FAST)
    assert report["schema"] == "recorder_health_ingest_report/1" and report["complete"] is True
    with pytest.raises(BridgeError) as exc:
        rhi.IncrementalRecorderBridge(bridge_config(rec, tmp_path), wall_clock=None, mono_us=None, fsync_fn=None, fault=None, _token=object())
    assert exc.value.code == "use_create_or_resume"


# ----------------------------------------------------------------------------------------------------------------------------- 13 bounded polling
def test_13_each_poll_consumes_at_most_the_configured_number_of_rows_and_bytes(tmp_path):
    rec = spool_recorder(tmp_path)
    b, _ = create_bridge(bridge_config(rec, tmp_path, max_rows_per_poll=5))
    sizes = []
    while True:
        out = b.poll()
        if out["status"] != "progress":
            break
        sizes.append(out["rows_ingested"])
    assert sizes == [5, 5, 5, 5, 2] and out["status"] == "no_new_data" and b.poll(max_rows=1)["status"] == "no_new_data"
    byte_bound = bridge_config(rec, tmp_path, name="c", max_bytes_per_poll=3000, max_line_bytes=2000)
    c, _ = create_bridge(byte_bound)
    first = c.poll()
    assert first["status"] == "progress" and first["spool_offset"] <= 3000 and first["rows_ingested"] < 22
    c.poll_until_drained()
    assert c.report()["spool_rows_ingested"] == 22
    with pytest.raises(BridgeError):
        bridge_config(rec, tmp_path, name="d", max_bytes_per_poll=10, max_line_bytes=2000)
    assert create_bridge(bridge_config(rec, tmp_path, name="e"))[0].poll(max_rows=0)["error"]["code"] == "config_invalid"


# ----------------------------------------------------------------------------------------------------------------------------- 14 partial trailing record
def test_14_a_partial_trailing_spool_row_is_reported_not_consumed_and_completed_later(tmp_path):
    rec = spool_recorder(tmp_path)
    full = rec.spool_path.read_bytes()
    lines = full.split(NL)[:-1]
    rec.spool_path.write_bytes(NL.join(lines[:10]) + NL + lines[10][:25])
    b, _ = create_bridge(bridge_config(rec, tmp_path))
    out = b.poll()
    assert out["rows_ingested"] == 10 and out["partial_tail_bytes"] == 25
    codes = [p["code"] for p in b.report()["problems"]]
    assert "spool_partial_trailing_row" in codes and b.report()["journal_records"] == 10
    assert b.poll()["status"] == "partial_tail"
    with rec.spool_path.open("ab") as handle:                                          # the writer finishes its row (and writes the rest of the spool)
        handle.write(full[len(NL.join(lines[:10]) + NL) + 25:])
    b.poll_until_drained()
    assert b.report()["spool_rows_ingested"] == 22 and (tmp_path / "b.journal.jsonl").read_bytes() == whole_file_journal(rec, tmp_path)


# ----------------------------------------------------------------------------------------------------------------------------- 15 duplicates
def test_15_an_identical_duplicate_row_is_dropped_and_a_conflicting_one_fails_the_bridge(tmp_path):
    rec = spool_recorder(tmp_path)
    lines = lines_of(rec.spool_path)
    write_lines(rec.spool_path, lines[:6] + [lines[5]] + lines[6:])
    b, _ = drain(bridge_config(rec, tmp_path))
    report = b.report()
    assert report["spool_duplicates_dropped"] == 1 and {"code": "spool_duplicate_dropped", "spool_n": 6} in report["problems"] and report["journal_records"] == 22
    other = tmp_path / "conflict"
    other.mkdir()
    rec2 = spool_recorder(other)
    conflicting = json.loads(lines[5])
    conflicting["events_received"] = 999
    write_lines(rec2.spool_path, lines[:6] + [json.dumps(conflicting).encode("ascii")] + lines[6:])
    c, _ = drain(bridge_config(rec2, other))
    assert c.error.code == "spool_row_conflict" and c.report()["journal_records"] == 6 and c.report()["complete"] is False


# ----------------------------------------------------------------------------------------------------------------------------- 16 sequence gap
def test_16_a_gap_in_the_spool_sequence_halts_ingestion_at_the_gap(tmp_path):
    rec = spool_recorder(tmp_path)
    lines = lines_of(rec.spool_path)
    write_lines(rec.spool_path, lines[:8] + lines[9:])
    b, _ = drain(bridge_config(rec, tmp_path))
    assert b.error.code == "spool_sequence_gap" and "expected n=9" in str(b.error)
    report = b.report()
    assert report["journal_records"] == 8 and report["spool_last_n"] == 8 and report["complete"] is False
    assert b.poll()["status"] == "error" and b.report()["journal_records"] == 8        # sticky: nothing after the gap is trusted or written


# ----------------------------------------------------------------------------------------------------------------------------- 17 hash mismatch
def test_17_an_altered_journal_row_is_detected_before_the_bridge_extends_it(tmp_path):
    rec = spool_recorder(tmp_path)
    b, _ = create_bridge(bridge_config(rec, tmp_path, max_rows_per_poll=10))
    b.poll()                                                                           # 10 rows in the journal, the spool not drained
    path = tmp_path / "b.journal.jsonl"
    lines = lines_of(path)
    row = json.loads(lines[4])
    row["health"] = "degraded"
    write_lines(path, lines[:4] + [wc.canonical(row).encode("utf-8")] + lines[5:])
    with pytest.raises(BridgeError) as exc:
        resume_bridge(bridge_config(rec, tmp_path))
    assert exc.value.code == "journal_not_verified" and len(lines_of(path)) == 10       # nothing was appended
    rewired = tmp_path / "rewired"
    rewired.mkdir()
    rec2 = spool_recorder(rewired)
    b2, _ = create_bridge(bridge_config(rec2, rewired, max_rows_per_poll=10))
    b2.poll()
    ledger = rewired / "b.ledger.jsonl"
    rows = lines_of(ledger)
    ledger_row = json.loads(rows[2])
    ledger_row["spool_n"] = 99
    write_lines(ledger, rows[:2] + [wc.canonical(ledger_row).encode("ascii")] + rows[3:])
    with pytest.raises(BridgeError) as exc2:
        resume_bridge(bridge_config(rec2, rewired))
    assert exc2.value.code == "ledger_not_verified"


# ----------------------------------------------------------------------------------------------------------------------------- 18 identity
@pytest.mark.parametrize("what,expected", [("source", "source_identity_mismatch"), ("run", "run_identity_mismatch"), ("worker", "spool_identity_mismatch"),
                                           ("instance", "spool_identity_mismatch")])
def test_18_a_spool_of_another_source_run_worker_or_instance_is_refused_before_a_journal_exists(tmp_path, what, expected):
    rec = spool_recorder(tmp_path)
    kw = {"source": {}, "run": {}, "worker": {"worker": 7}, "instance": {"collector_instance": "rec:other:w0:r0"}}[what]
    cfg = bridge_config(rec, tmp_path, **kw)
    if what == "source":
        cfg = rhi.BridgeConfig(cfg.spool_path, cfg.journal_path, cfg.ledger_path, cfg.checkpoint_path, source_id="another-source", run_id=j.RUN, policy=FAST,
                               spool_clock_domain="r", bridge_clock_domain="b")
    if what == "run":
        cfg = rhi.BridgeConfig(cfg.spool_path, cfg.journal_path, cfg.ledger_path, cfg.checkpoint_path, source_id=SOURCE_ID, run_id="another-run", policy=FAST,
                               spool_clock_domain="r", bridge_clock_domain="b")
    b, _ = create_bridge(cfg)
    out = b.poll()
    assert out["status"] == "error" and out["error"]["code"] == expected and out["journal_records"] == 0
    assert not cfg.journal_path.exists() and b.report()["complete"] is False


def test_18b_a_row_of_another_identity_in_the_middle_of_the_spool_halts_the_bridge(tmp_path):
    rec = spool_recorder(tmp_path)
    lines = lines_of(rec.spool_path)
    stray = json.loads(lines[7])
    stray["collector_instance"] = "rec:somebody-else:w0:r0"
    write_lines(rec.spool_path, lines[:7] + [json.dumps(stray).encode("ascii")] + lines[8:])
    b, _ = drain(bridge_config(rec, tmp_path))
    assert b.error.code == "spool_identity_mismatch" and b.report()["journal_records"] == 7


def test_18c_a_protected_channel_is_refused_before_any_file_is_created(tmp_path):
    from fabric_perf.controllers import channel_selection as cs
    rec = spool_recorder(tmp_path)
    with pytest.raises(cs.ChannelSelectionError):
        bridge_config(rec, tmp_path, channel_id="e1-r002")
    assert not (tmp_path / "b.journal.jsonl").exists() and not (tmp_path / "b.ledger.jsonl").exists()


def test_18d_existing_outputs_are_never_overwritten(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path)
    drain(cfg)
    with pytest.raises(BridgeError) as exc:
        create_bridge(cfg)
    assert exc.value.code == "output_exists"


# ----------------------------------------------------------------------------------------------------------------------------- 19 truncated / 20 replaced
def test_19_a_truncated_spool_fails_closed_and_the_journal_is_untouched(tmp_path):
    rec = spool_recorder(tmp_path)
    b, _ = create_bridge(bridge_config(rec, tmp_path, max_rows_per_poll=10))
    b.poll()
    before = (tmp_path / "b.journal.jsonl").read_bytes()
    rec.spool_path.write_bytes(rec.spool_path.read_bytes()[:2000])
    out = b.poll()
    assert out["error"]["code"] == "spool_truncated" and (tmp_path / "b.journal.jsonl").read_bytes() == before


def test_20_a_replaced_spool_fails_closed(tmp_path):
    rec = spool_recorder(tmp_path)
    b, _ = create_bridge(bridge_config(rec, tmp_path, max_rows_per_poll=10))
    b.poll()
    original = rec.spool_path.read_bytes()
    replacement = tmp_path / "replacement.jsonl"
    replacement.write_bytes(original.replace(b'"healthy"', b'"degrad"', 1) if b'"healthy"' in original[:3000] else original[:-1] + b"X")
    os.replace(replacement, rec.spool_path)                                           # a different file under the same name
    out = b.poll()
    assert out["status"] == "error" and out["error"]["code"] in ("spool_replaced", "spool_truncated")
    gone = tmp_path / "gone"
    gone.mkdir()
    rec2 = spool_recorder(gone)
    c, _ = create_bridge(bridge_config(rec2, gone, max_rows_per_poll=10))
    c.poll()
    rec2.spool_path.unlink()
    assert c.poll()["error"]["code"] == "spool_missing_after_start"
    assert create_bridge(bridge_config(rec, tmp_path, name="n"))[0].poll()["status"] in ("progress", "error")


# ----------------------------------------------------------------------------------------------------------------------------- 21 interrupted collection
def test_21_a_spool_without_a_stop_is_reported_as_interrupted_and_no_stop_is_invented(tmp_path):
    rec = short_recorder(tmp_path, stop=False)
    rec.write()
    b, _ = drain(bridge_config(rec, tmp_path))
    waiting = b.report()
    assert [p["code"] for p in waiting["problems"]] == ["no_stop_row"] and waiting["has_stop"] is False
    ended = b.close(source_ended=True)
    assert "interrupted_collection" in [p["code"] for p in ended["problems"]] and ended["complete"] is False
    events, _, _ = wc.read_journal_file(tmp_path / "b.journal.jsonl")
    assert "stop" not in {e["kind"] for e in events}                                   # never synthesised: time after the last watermark stays uncovered
    ledger = td.read_ingest_ledger(tmp_path / "b.ledger.jsonl")
    assert ledger["has_stop"] is True and ledger["rows"][-1]["source_ended"] is True and ledger["rows"][-1]["has_stop"] is False


# ----------------------------------------------------------------------------------------------------------------------------- 22 ingestion time
def test_22_ingestion_times_are_recorded_per_row_in_the_ledger_and_never_in_the_journal(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg_a = bridge_config(rec, tmp_path, name="a", spool_domain="recorder-host", bridge_domain="bridge-host")
    clock_a = BridgeClock(step_ms=2.0, mono_step_us=75)
    drain(cfg_a, clock=clock_a)
    cfg_b = bridge_config(rec, tmp_path, name="b", spool_domain="recorder-host", bridge_domain="bridge-host")
    clock_b = BridgeClock(start=rhi.datetime(2030, 1, 2, 3, 4, 5, tzinfo=rhi.timezone.utc), step_ms=9.0, mono_step_us=900)
    drain(cfg_b, clock=clock_b)
    assert (tmp_path / "a.journal.jsonl").read_bytes() == (tmp_path / "b.journal.jsonl").read_bytes()    # the journal does not depend on when the bridge ran
    ledger_a, ledger_b = td.read_ingest_ledger(tmp_path / "a.ledger.jsonl"), td.read_ingest_ledger(tmp_path / "b.ledger.jsonl")
    ingest_a = [r for r in ledger_a["rows"] if r["kind"] == "ingest"]
    assert len(ingest_a) == 22 and [r["spool_n"] for r in ingest_a] == list(range(1, 23))
    first = ingest_a[0]
    assert first["spool_t_utc"] != first["read_wall_utc"] and first["read_wall_utc"] <= first["journal_append_start_wall_utc"] <= first["journal_append_return_wall_utc"]
    assert first["read_mono_us"] <= first["journal_append_start_mono_us"] <= first["journal_append_return_mono_us"]
    assert first["journal_append_confirmation"] == "os_fsync_returned" and first["spool_clock_domain"] == "recorder-host" and first["bridge_clock_domain"] == "bridge-host"
    assert [r["journal_append_return_wall_utc"] for r in ingest_a] != [r["journal_append_return_wall_utc"] for r in [x for x in ledger_b["rows"] if x["kind"] == "ingest"]]
    events, _, _ = wc.read_journal_file(tmp_path / "a.journal.jsonl")
    assert not [k for e in events for k in e if "ingest" in k or "read_" in k or "append" in k]
    assert ledger_a["head"]["append_semantics"].endswith("not a guarantee against every storage failure")


def test_22b_a_naive_bridge_clock_is_refused_and_a_clock_step_in_the_spool_opens_a_journal_error(tmp_path):
    rec = spool_recorder(tmp_path)
    naive = rhi.IncrementalRecorderBridge.create(bridge_config(rec, tmp_path, name="n"), wall_clock=lambda: rhi.datetime(2026, 1, 1), mono_us=lambda: 1)
    assert naive.poll()["error"]["code"] == "timestamp_naive"
    stepped = tmp_path / "step"
    stepped.mkdir()
    rec2 = spool_recorder(stepped)
    rows = [json.loads(line) for line in lines_of(rec2.spool_path)]
    for row in rows[8:]:
        row["mono_ms"] += 5000.0                                                        # the recorder's wall clock stepped back relative to its monotonic clock
    write_lines(rec2.spool_path, [json.dumps(r).encode("ascii") for r in rows])
    b, _ = drain(bridge_config(rec2, stepped))
    report = b.report()
    assert {"code": "clock_discontinuity", "spool_n": 9} in report["problems"] and report["complete"] is False
    events, _, _ = wc.read_journal_file(stepped / "b.journal.jsonl")
    assert [e["error_code"] for e in events if e["kind"] == "error"] == ["clock_discontinuity"] and not any(e["kind"] == "recovered" for e in events)


def test_22c_a_malformed_spool_timestamp_halts_the_bridge(tmp_path):
    rec = spool_recorder(tmp_path)
    lines = lines_of(rec.spool_path)
    row = json.loads(lines[4])
    row["t_utc"] = "not-a-time"
    write_lines(rec.spool_path, lines[:4] + [json.dumps(row).encode("ascii")] + lines[5:])
    b, _ = drain(bridge_config(rec, tmp_path))
    assert b.error.code == "spool_timestamp_invalid" and b.report()["journal_records"] == 4
