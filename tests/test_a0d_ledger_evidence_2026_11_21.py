"""Task A0d (2026-11-21), blocker B-ledger: continuous committed-ledger evidence, offline.

Every source here is a synthetic ``ScriptedSource``; no peer, Docker, Caliper or network is touched. These tests establish
SOFTWARE behaviour (counting, continuity, recovery, fail-closed rules). They cannot establish that a real Fabric peer
delivers what the synthetic source does.
"""

import csv
import json
from datetime import timedelta
from pathlib import Path

import pytest
from tests.a0d_support import CHANNEL, T0, FakeClock, ScriptedSource, make_block, tx, valid_block

from fabric_perf.controllers import channel_selection as cs
from fabric_perf.controllers.ledger_evidence import (
    BLOCK_COLUMNS,
    FILES,
    KIND_BASELINE,
    KIND_FAILURE,
    KIND_INTERVAL,
    KIND_RESUME,
    OBS_COLUMNS,
    STATE_FAILED,
    STATE_HALTED,
    STATE_INITIALIZING,
    STATE_MEASURING,
    STATE_NO_INTERVAL_IN_WINDOW,
    STATE_NO_OBSERVATIONS,
    STATE_RESUMED,
    TX_COLUMNS,
    LedgerCollector,
    LedgerEvidenceError,
    LedgerReadError,
    parse_observation_row,
    read_observation_journal,
    windowed_committed_rate,
)


def collector(tmp_path, source=None, clock=None, resume=False, name="journal"):
    source = source or ScriptedSource(10)
    clock = clock or FakeClock()
    return LedgerCollector(source, channel_id=CHANNEL, out_dir=tmp_path / name, collector_run_id="c1", clock=clock,
                           resume=resume), source, clock


def rows(path: Path):
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def tx_rows(c):
    return rows(c.paths["tx"])


def run_interval(c, source, clock, seconds, *blocks):
    """Commit blocks, advance the clock, poll once."""
    source.commit(*blocks)
    clock.advance(seconds)
    return c.poll()


# =========================================================== 1. first valid observation
def test_first_observation_is_a_baseline_with_no_interval_and_no_rate(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(10))
    first = c.poll()
    assert (first.kind, first.status, first.rate_valid, first.height) == (KIND_BASELINE, "ok", False, 10)
    assert first.valid is None and first.interval_s is None and first.prev_seq is None            # not zero: absent
    rate = windowed_committed_rate(read_observation_journal(c.paths["observations"]), now=clock(), window_s=5)
    assert rate.state == STATE_INITIALIZING and rate.value is None and "baseline" in rate.reason
    assert tx_rows(c) == [] and rows(c.paths["blocks"]) == []                                       # blocks before the baseline are not counted


def test_a_journal_with_no_rows_is_no_observations_not_zero():
    rate = windowed_committed_rate([], now=T0, window_s=5)
    assert (rate.state, rate.value) == (STATE_NO_OBSERVATIONS, None)


# =========================================================== 2. correct committed counting + 11. invalid status
def test_counting_separates_committed_invalid_and_other_envelopes(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(10))
    c.poll()
    block = make_block(10, [tx("a", 0, 0), tx("b", 1, 0), tx("c", 2, 0), tx("d", 3, 11), tx("cfg", 4, 0, header_type=1)])
    row = run_interval(c, source, clock, 2.0, block)
    assert (row.kind, row.rate_valid, row.blocks) == (KIND_INTERVAL, True, 1)
    assert (row.tx_total, row.endorser, row.valid, row.invalid, row.other) == (5, 4, 3, 1, 1)
    assert row.interval_s == 2.0
    rate = windowed_committed_rate(read_observation_journal(c.paths["observations"]), now=clock(), window_s=5)
    assert rate.state == STATE_MEASURING and rate.value == pytest.approx(3 / 2.0)                  # only VALID endorser envelopes
    assert (rate.valid, rate.invalid) == (3, 1)
    written = tx_rows(c)
    assert [r["tx_id"] for r in written] == ["a", "b", "c", "d"]                                    # the config envelope is not application evidence
    assert {r["header_type"] for r in written} == {"3"} and [r["validation_code"] for r in written] == ["0", "0", "0", "11"]
    assert list(written[0]) == TX_COLUMNS and rows(c.paths["blocks"])[0]["other_tx"] == "1"


def test_an_invalid_status_is_never_counted_as_committed(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    row = run_interval(c, source, clock, 1.0, make_block(0, [tx("x", 0, 11), tx("y", 1, 12)]))
    assert (row.valid, row.invalid) == (0, 2)
    rate = windowed_committed_rate(read_observation_journal(c.paths["observations"]), now=clock(), window_s=5)
    assert rate.value == 0.0 and rate.invalid == 2                                                  # a real zero committed, two invalid


def test_an_endorser_envelope_without_a_validation_code_halts_the_journal(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    row = run_interval(c, source, clock, 1.0, make_block(0, [tx("x", 0, None)]))
    assert (row.status, row.kind) == ("inconsistent", KIND_FAILURE) and "validation_code_missing" in row.detail and c.halted
    assert tx_rows(c) == []                                                                          # nothing from a block that failed its checks


# =========================================================== 3. consecutive observations / windowing
def test_windowed_rate_uses_whole_observed_intervals_inside_the_window(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    for count in (2, 0, 4, 6):
        run_interval(c, source, clock, 1.0, valid_block(source.current_height, count))
    records = read_observation_journal(c.paths["observations"])
    rate = windowed_committed_rate(records, now=clock(), window_s=2.5)                              # intervals ending at t+2, t+3, t+4 (> now-2.5)
    assert rate.state == STATE_MEASURING and rate.observations_used == 3
    assert rate.valid == 10 and rate.interval_s == pytest.approx(3.0) and rate.value == pytest.approx(10 / 3.0)
    assert rate.interval_start == T0 + timedelta(seconds=1) and rate.interval_end == T0 + timedelta(seconds=4)
    assert rate.window_covered is True
    whole = windowed_committed_rate(records, now=clock(), window_s=60)
    assert whole.valid == 12 and whole.interval_s == pytest.approx(4.0) and whole.window_covered is False   # the baseline is later than the window start


def test_rate_is_stamped_with_ledger_observation_time_not_with_now(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 2.0, valid_block(0, 4))
    later = clock() + timedelta(seconds=3)
    rate = windowed_committed_rate(read_observation_journal(c.paths["observations"]), now=later, window_s=10)
    assert rate.interval_end == T0 + timedelta(seconds=2) and rate.interval_end != later


# =========================================================== 4. genuine zero + 12. empty block
def test_a_fully_observed_interval_with_no_new_blocks_is_a_real_zero(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(5))
    c.poll()
    clock.advance(2.0)
    row = c.poll()
    assert (row.blocks, row.valid, row.rate_valid) == (0, 0, True)
    rate = windowed_committed_rate(read_observation_journal(c.paths["observations"]), now=clock(), window_s=5)
    assert rate.state == STATE_MEASURING and rate.value == 0.0 and rate.value is not None


def test_an_empty_block_counts_as_a_block_with_no_transactions(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    row = run_interval(c, source, clock, 1.0, make_block(0, []))
    assert (row.blocks, row.tx_total, row.valid) == (1, 0, 0) and row.status == "ok"
    assert rows(c.paths["blocks"])[0]["tx_total"] == "0" and tx_rows(c) == []


# =========================================================== 5. missing observation
def test_an_unreadable_height_is_a_failure_row_and_the_value_is_missing_not_zero(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 3))
    source.fail_height = True
    clock.advance(1.0)
    failed = c.poll()
    assert (failed.kind, failed.status, failed.height) == (KIND_FAILURE, "read_failed", None) and failed.valid is None
    rate = windowed_committed_rate(read_observation_journal(c.paths["observations"]), now=clock(), window_s=5)
    assert rate.state == STATE_FAILED and rate.value is None and "read_failed" in rate.reason


def test_the_next_complete_observation_spans_the_gap_without_losing_or_inventing_anything(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 2))                                          # seq 2, complete
    source.fail_height = True
    clock.advance(1.0)
    c.poll()                                                                                         # seq 3, failure
    source.fail_height = False
    row = run_interval(c, source, clock, 1.0, valid_block(1, 5))                                     # seq 4 spans 2 s since seq 2
    assert (row.prev_seq, row.interval_s, row.valid, row.rate_valid) == (2, 2.0, 5, True)
    assert not c.halted and c.recovery == {"resumed": False}


# =========================================================== 6. stale observation
def test_an_observation_older_than_the_window_gives_no_value_never_the_old_one_as_current(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 8))
    records = read_observation_journal(c.paths["observations"])
    fresh = windowed_committed_rate(records, now=clock(), window_s=5)
    stale = windowed_committed_rate(records, now=clock() + timedelta(seconds=30), window_s=5)
    assert fresh.value == 8.0
    assert stale.state == STATE_NO_INTERVAL_IN_WINDOW and stale.value is None and "outside the 5s window" in stale.reason


# =========================================================== 7. duplicates
def test_a_committed_transaction_id_seen_twice_halts_the_journal(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, make_block(0, [tx("same", 0, 0)]))
    row = run_interval(c, source, clock, 1.0, make_block(1, [tx("same", 0, 0)]))
    assert row.status == "inconsistent" and "duplicate_committed_tx" in row.detail and c.halted
    assert [r["tx_id"] for r in tx_rows(c)] == ["same"]                                              # counted once


def test_a_later_duplicate_flagged_invalid_by_fabric_is_counted_invalid_not_committed(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, make_block(0, [tx("same", 0, 0)]))
    row = run_interval(c, source, clock, 1.0, make_block(1, [tx("same", 0, 9)]))                    # 9 = DUPLICATE_TXID
    assert (row.valid, row.invalid, row.status) == (0, 1, "ok") and not c.halted


def test_polling_again_without_new_blocks_never_counts_a_transaction_twice(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 3))
    for _ in range(3):
        clock.advance(1.0)
        again = c.poll()
        assert (again.blocks, again.valid) == (0, 0)
    assert len(tx_rows(c)) == 3 and source.fetches == [0]                                            # block 0 fetched exactly once


def test_two_committed_envelopes_with_one_id_inside_a_single_block_halt(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    row = run_interval(c, source, clock, 1.0, make_block(0, [tx("d", 0, 0), tx("d", 1, 0)]))
    assert row.status == "inconsistent" and "duplicate_committed_tx" in row.detail


# =========================================================== 8. block-number gap / mismatch
def test_a_block_that_decodes_to_another_number_halts(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    source.blocks[0] = valid_block(5, 1)                                                              # asked for 0, served 5
    source.current_height = 1
    clock.advance(1.0)
    row = c.poll()
    assert row.status == "inconsistent" and "number_mismatch" in row.detail and c.halted


def test_a_broken_hash_chain_halts(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 1))
    row = run_interval(c, source, clock, 1.0, make_block(1, [tx("n", 0)], previous="f" * 64))
    assert row.status == "inconsistent" and "chain_break" in row.detail


def test_a_block_missing_inside_the_range_is_an_incomplete_fetch_and_is_recovered_later_exactly_once(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    source.commit(valid_block(0, 2), valid_block(1, 3))
    source.fail_blocks = {1}
    clock.advance(1.0)
    incomplete = c.poll()
    assert (incomplete.status, incomplete.kind) == ("fetch_incomplete", KIND_FAILURE) and "block 1" in incomplete.detail
    assert tx_rows(c) == [] and rows(c.paths["blocks"]) == []                                         # an incomplete poll writes no partial evidence
    source.fail_blocks = set()
    clock.advance(1.0)
    ok = c.poll()
    assert (ok.valid, ok.blocks, ok.prev_seq, ok.interval_s) == (5, 2, 1, 2.0)
    assert len(tx_rows(c)) == 5


# =========================================================== 9. counter rollback
def test_a_height_that_goes_backwards_halts_and_a_halted_collector_refuses_more_polls(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 1), valid_block(1, 1))
    source.height_value_override = 1
    clock.advance(1.0)
    row = c.poll()
    assert (row.status, row.kind) == ("height_rollback", KIND_FAILURE) and c.halted
    with pytest.raises(LedgerEvidenceError, match="halted"):
        c.poll()
    rate = windowed_committed_rate(read_observation_journal(c.paths["observations"]), now=clock(), window_s=5)
    assert rate.state == STATE_HALTED and rate.value is None
    with pytest.raises(LedgerEvidenceError, match="halted"):
        LedgerCollector(source, channel_id=CHANNEL, out_dir=c.out_dir, collector_run_id="c2", clock=clock, resume=True)


# =========================================================== 10. channel mismatch + protected channels
def test_an_envelope_of_another_channel_halts(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    row = run_interval(c, source, clock, 1.0, make_block(0, [tx("x", 0, channel="e1-other")]))
    assert row.status == "inconsistent" and "channel_mismatch" in row.detail


@pytest.mark.parametrize("channel", sorted(cs.PROTECTED_REFERENCE_CHANNELS) + ["", "NotAChannel", "e1-"])
def test_the_collector_refuses_protected_missing_and_malformed_channels(tmp_path, channel):
    with pytest.raises(cs.ChannelSelectionError):
        LedgerCollector(ScriptedSource(0), channel_id=channel, out_dir=tmp_path / "j", collector_run_id="c")
    assert not (tmp_path / "j").exists()                                                              # nothing is created for a refused channel


def test_resuming_a_journal_for_another_channel_is_refused(tmp_path):
    c, _, _ = collector(tmp_path)
    c.poll()
    with pytest.raises(LedgerEvidenceError, match="for channel"):
        LedgerCollector(ScriptedSource(0), channel_id="e1-different", out_dir=c.out_dir, collector_run_id="c2", resume=True)


# =========================================================== 13/14. interruption and restart
def test_restart_resumes_without_double_counting_and_the_first_row_carries_no_rate(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 3))
    before = tx_rows(c)
    source.commit(valid_block(1, 2))                                                                   # committed while the collector was down
    clock.advance(60.0)
    c2, _, _ = collector(tmp_path, source, clock, resume=True)
    assert c2.recovery["resumed"] and c2.recovery["next_observation_is"] == KIND_RESUME
    resumed = c2.poll()
    assert (resumed.kind, resumed.rate_valid, resumed.valid, resumed.interval_s) == (KIND_RESUME, False, 2, 60.0)
    assert resumed.seq == 3 and tx_rows(c2)[:3] == before and len(tx_rows(c2)) == 5
    rate = windowed_committed_rate(read_observation_journal(c2.paths["observations"]), now=clock(), window_s=300)
    assert rate.state == STATE_RESUMED and rate.value is None                                          # the downtime interval is not a rate
    nxt = run_interval(c2, source, clock, 2.0, valid_block(2, 4))
    assert (nxt.kind, nxt.rate_valid, nxt.valid, nxt.prev_seq) == (KIND_INTERVAL, True, 4, 3)
    after = windowed_committed_rate(read_observation_journal(c2.paths["observations"]), now=clock(), window_s=300)
    assert after.state == STATE_MEASURING and after.value == pytest.approx(2.0) and after.observations_used == 1   # only post-restart intervals


def test_restart_after_a_failed_poll_reuses_the_journal_and_never_refetches_recorded_blocks(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 2))
    source.fetches.clear()
    c2, _, _ = collector(tmp_path, source, clock, resume=True)
    clock.advance(1.0)
    c2.poll()
    assert source.fetches == []                                                                         # block 0 is already on disk


# =========================================================== 15. partial output files
def test_a_torn_last_line_is_preserved_aside_and_removed_before_appending(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 2))
    with c.paths["observations"].open("ab") as fh:
        fh.write(b"3,interval,ok,1,e1-offline.a0d,c1,2026-11-21T10:0")                                  # a write cut short, no newline
    c2, _, _ = collector(tmp_path, source, clock, resume=True)
    assert c2.recovery["torn_tail_bytes_removed"]["observations"] > 0
    sidecar = c.paths["observations"].with_name("ledger_observations.csv.torn_tail")
    assert sidecar.read_bytes().startswith(b"3,interval,ok")                                            # evidence kept, not silently dropped
    clock.advance(1.0)
    row = c2.poll()
    assert row.seq == 3 and len(read_observation_journal(c2.paths["observations"])) == 3


def test_transaction_rows_written_before_a_crash_are_reused_not_duplicated(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    source.commit(valid_block(0, 3))
    clock.advance(1.0)
    # Simulate a crash after the transaction rows were appended but before the block row and the observation.
    first = source.blocks[0].txs[0]
    with c.paths["tx"].open("a", newline="", encoding="utf-8") as fh:
        csv.writer(fh, lineterminator="\n").writerow([first.tx_id, 0, 0, 3, 0, 100, CHANNEL, first.ch_ts_ms, 2, "2026-11-21T10:00:01.000+00:00"])
    c2, _, _ = collector(tmp_path, source, clock, resume=True)
    assert c2.recovery["orphan_tx_blocks"] == [0]
    row = c2.poll()
    assert (row.valid, row.status) == (3, "ok")
    ids = [r["tx_id"] for r in tx_rows(c2)]
    assert len(ids) == len(set(ids)) == 3


def test_a_block_recorded_without_its_observation_is_reused_on_restart(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 1))
    c.paths["observations"].write_bytes(c.paths["observations"].read_bytes().rsplit(b"\n", 2)[0] + b"\n")   # lose the last observation row only
    c2, _, _ = collector(tmp_path, source, clock, resume=True)
    assert c2.recovery["orphan_blocks_reused"] == [0]
    source.fetches.clear()
    clock.advance(1.0)
    row = c2.poll()
    assert source.fetches == [] and row.valid == 1 and row.kind == KIND_RESUME and len(tx_rows(c2)) == 1


@pytest.mark.parametrize("corrupt", ["header", "seq_gap", "count_sum", "blocks_missing"])
def test_a_corrupt_journal_is_refused_on_resume(tmp_path, corrupt):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    run_interval(c, source, clock, 1.0, valid_block(0, 2))
    path = c.paths["observations"]
    text = path.read_text(encoding="utf-8")
    if corrupt == "header":
        path.write_text(text.replace("obs_seq", "seq", 1), encoding="utf-8")
    elif corrupt == "seq_gap":
        path.write_text(text.replace("\n2,interval", "\n7,interval"), encoding="utf-8")
    elif corrupt == "count_sum":
        lines = text.splitlines()
        cells = next(csv.reader([lines[2]]))
        cells[OBS_COLUMNS.index("valid_tx")] = "1"                                                      # 1 valid + 0 invalid != 2 endorser
        buffer = Path(tmp_path / "x.csv")
        with buffer.open("w", newline="", encoding="utf-8") as fh:
            csv.writer(fh, lineterminator="\n").writerow(cells)
        lines[2] = buffer.read_text(encoding="utf-8").strip()
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    else:
        c.paths["blocks"].write_text(",".join(BLOCK_COLUMNS) + "\n", encoding="utf-8")
    with pytest.raises(LedgerEvidenceError):
        LedgerCollector(source, channel_id=CHANNEL, out_dir=c.out_dir, collector_run_id="c2", clock=clock, resume=True)


def test_a_new_journal_is_never_started_in_a_directory_that_has_content(tmp_path):
    c, _, _ = collector(tmp_path)
    with pytest.raises(LedgerEvidenceError, match="non-empty"):
        LedgerCollector(ScriptedSource(0), channel_id=CHANNEL, out_dir=c.out_dir, collector_run_id="c2")


# =========================================================== 16. invalid timestamps
def test_a_naive_clock_is_refused(tmp_path):
    naive = lambda: T0.replace(tzinfo=None)
    with pytest.raises(LedgerEvidenceError, match="timezone-aware"):
        LedgerCollector(ScriptedSource(0), channel_id=CHANNEL, out_dir=tmp_path / "j", collector_run_id="c", clock=naive)


def test_a_clock_that_turns_naive_mid_run_is_refused_not_recorded(tmp_path):
    state = {"naive": False}
    clock = lambda: T0.replace(tzinfo=None) if state["naive"] else T0
    c = LedgerCollector(ScriptedSource(0), channel_id=CHANNEL, out_dir=tmp_path / "j", collector_run_id="c", clock=clock)
    state["naive"] = True
    with pytest.raises(LedgerEvidenceError, match="timezone-aware"):
        c.poll()
    assert len(rows(c.paths["observations"])) == 0


@pytest.mark.parametrize("bad", ["", "not-a-time", "2026-11-21T10:00:00", "2026-13-40T00:00:00+00:00"])
def test_the_strict_reader_refuses_an_invalid_or_zoneless_observation_timestamp(tmp_path, bad):
    c, _, _ = collector(tmp_path)
    c.poll()
    row = dict(rows(c.paths["observations"])[0])
    row["observed_utc"] = bad
    with pytest.raises(LedgerEvidenceError):
        parse_observation_row(row)


# =========================================================== 17. non-positive interval
def test_a_clock_that_does_not_advance_gives_a_failure_row_not_a_rate(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    source.commit(valid_block(0, 5))
    row = c.poll()                                                                                       # same instant
    assert (row.status, row.kind) == ("nonpositive_interval", KIND_FAILURE) and row.valid is None and not c.halted
    rate = windowed_committed_rate(read_observation_journal(c.paths["observations"]), now=clock(), window_s=5)
    assert rate.state == STATE_FAILED and rate.value is None
    assert tx_rows(c) == []                                                                              # the blocks are counted by the next positive interval
    clock.advance(1.0)
    assert c.poll().valid == 5


def test_a_clock_stepping_backwards_is_not_counted_and_does_not_poison_the_journal(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    clock.advance(2.0)
    c.poll()
    clock.advance(-5.0)
    assert c.poll().status == "nonpositive_interval"
    clock.advance(10.0)
    assert c.poll().status == "ok"
    assert [r.status for r in read_observation_journal(c.paths["observations"])] == ["ok", "ok", "nonpositive_interval", "ok"]


def test_the_strict_reader_refuses_a_nonpositive_interval_row():
    base = {name: "" for name in OBS_COLUMNS}
    base.update(obs_seq="2", kind="interval", status="ok", rate_valid="1", channel_id=CHANNEL, collector_run_id="c",
                observed_utc="2026-11-21T10:00:01.000+00:00", height="3", prev_seq="1", prev_height="3",
                prev_observed_utc="2026-11-21T10:00:01.000+00:00", interval_s="0", blocks_in_interval="0", tx_total="0",
                endorser_tx="0", valid_tx="0", invalid_tx="0", other_tx="0")
    with pytest.raises(LedgerEvidenceError):
        parse_observation_row(base)


# =========================================================== bounded loop and files
def test_follow_requires_explicit_bounds_and_stops_at_a_halt(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    for bad in ({"max_polls": 0, "poll_interval_s": 1}, {"max_polls": 1, "poll_interval_s": 0}, {"max_polls": True, "poll_interval_s": 1}):
        with pytest.raises(ValueError):
            c.follow(**bad)
    with pytest.raises(TypeError):
        c.follow()                                                                                        # no default bound
    pauses = []
    done = c.follow(max_polls=3, poll_interval_s=1.5, sleep=lambda s: (pauses.append(s), clock.advance(s)))
    assert len(done) == 3 and pauses == [1.5, 1.5]


def test_the_journal_files_exist_with_the_documented_headers(tmp_path):
    c, _, _ = collector(tmp_path)
    assert {key: path.name for key, path in c.paths.items()} == FILES
    meta = json.loads(c.paths["meta"].read_text(encoding="utf-8"))
    assert meta["channel_id"] == CHANNEL and "VALID" in meta["definition"] and "ENDORSER" in meta["definition"]
    assert c.paths["tx"].read_text(encoding="utf-8").strip() == ",".join(TX_COLUMNS)


# =========================================================== the data hash is checked on its own (not masked by the chain check)
def test_a_data_hash_that_does_not_match_the_envelopes_halts_even_when_the_chain_is_intact(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    row = run_interval(c, source, clock, 1.0, make_block(0, [tx("a", 0)], data_hash_ok=False))
    assert (row.status, row.kind) == ("inconsistent", KIND_FAILURE) and "data_hash_mismatch" in row.detail and c.halted
    assert tx_rows(c) == [] and rows(c.paths["blocks"]) == []


def test_a_validation_flag_length_that_differs_from_the_envelope_count_halts(tmp_path):
    c, source, clock = collector(tmp_path, ScriptedSource(0))
    c.poll()
    row = run_interval(c, source, clock, 1.0, make_block(0, [tx("a", 0), tx("b", 1)], flags_len=1))
    assert row.status == "inconsistent" and "flags_length_mismatch" in row.detail
