"""A0i coverage semantics (checklist items 1-15, plus the collector-health writer). Offline, synthetic, no model, no network.

A genuine zero is believed only inside coverage that a hash-chained collector-health journal verifies; everything else is unknown, stale,
interrupted, a gap, missing or invalid, and is never represented as zero.
"""

from __future__ import annotations

import json
import socket
import subprocess

import pytest

from tests import a0i_support as s
from tests.a0i_support import acc, at, ch, chj, wc

C = wc.CoverageClass


@pytest.fixture(autouse=True)
def no_process_or_network(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("process or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)


def snap(coverage, second: float):
    return coverage.snapshot(at(second))


# ------------------------------------------------------------------------------------------------ 1 verified positive
def test_01_a_positive_count_inside_verified_coverage_is_observed(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=100)
    cls, why = snap(s.coverage_for(path), 100).classify(at(50), 7)
    assert cls == C.OBSERVED_POSITIVE and "verified" in why


# --------------------------------------------------------------------------------------------------- 2 verified zero
def test_02_a_zero_inside_verified_coverage_is_a_genuine_zero_and_stays_zero_in_the_history(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=100)
    coverage = s.coverage_for(path)
    assert snap(coverage, 100).classify(at(50), 0)[0] == C.VERIFIED_ZERO
    history = s.captured(coverage, {50: 0})
    history.advance_to(at(100))
    times, counts, reference = history.store.snapshot(now_utc=at(100).isoformat())
    assert reference.status == "complete" and len(times) == 100 and counts[50] == 0          # zero, not missing
    assert history.coverage_counts["verified_zero"] == 1 and history.coverage_provenance(at(100))["contract_satisfied"] is True


# ---------------------------------------------------------------------------------------------------- 3 unknown zero
def test_03_a_zero_with_no_coverage_evidence_is_unknown_and_never_zero(tmp_path):
    coverage = wc.VerifiedCoverage.no_evidence(s.SOURCE_ID, "the archive carries no collector evidence")
    assert snap(coverage, 100).classify(at(50), 0)[0] == C.UNKNOWN_COVERAGE_ZERO
    assert snap(coverage, 100).classify(at(50), 3)[0] == C.OBSERVED_POSITIVE                  # a positive proves nothing about the zeros around it
    history = s.captured(coverage, {50: 0})
    history.advance_to(at(100))
    times, counts, reference = history.store.snapshot(now_utc=at(100).isoformat())
    assert (times, counts) == ((), ()) and reference.status == "incomplete" and reference.unavailable_seconds == 1
    assert history.coverage_counts["zero_count_unknown_coverage"] == 1
    provenance = history.coverage_provenance(at(100))
    assert provenance["contract_satisfied"] is False and provenance["mode"] == "strict_no_evidence"


def test_03b_a_zero_before_the_collector_started_is_unknown_even_with_a_valid_journal(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=200, start=100)
    sn = snap(s.coverage_for(path), 200)
    assert sn.classify(at(50), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and sn.classify(at(150), 0)[0] == C.VERIFIED_ZERO
    assert sn.classify(at(50), 9)[0] == C.COVERAGE_GAP            # in attested mode even a positive needs verified coverage: the count may be partial


# ------------------------------------------------------------------------------------------------ 4 missing observation
def test_04_a_missing_observation_is_never_read_as_zero(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=100)
    coverage = s.coverage_for(path)
    assert snap(coverage, 100).classify(at(50), None)[0] == C.MISSING_OBSERVATION
    history = s.captured(coverage, {50: None})
    history.advance_to(at(100))
    _, counts, reference = history.store.snapshot(now_utc=at(100).isoformat())
    assert counts == () and reference.status == "incomplete" and reference.missing_seconds >= 1
    assert history.coverage_counts["missing_observation"] == 1
    assert history.coverage_provenance(at(100))["contract_satisfied"] is False


# ------------------------------------------------------------------------------------------------ 5 stale attestation
def test_05_stale_evidence_cannot_validate_later_intervals_and_later_evidence_is_not_used_early(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 100)                                    # the collector then goes silent ...
    s.beat(journal, clock, 300, 299)                               # ... and a LATER heartbeat certifies up to 299 (invisible at 200)
    coverage = s.coverage_for(path)
    early = snap(coverage, 200)
    assert early.classify(at(50), 0)[0] == C.VERIFIED_ZERO
    assert early.classify(at(150), 0)[0] == C.STALE_COLLECTOR_EVIDENCE and early.classify(at(150), 5)[0] == C.STALE_COLLECTOR_EVIDENCE
    assert snap(coverage, 300).classify(at(150), 0)[0] == C.VERIFIED_ZERO                  # only once that evidence exists at the decision time


def test_05b_a_stale_second_is_deferred_not_recorded_and_the_history_is_not_current(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 700)
    history = s.captured(s.coverage_for(path))
    history.advance_to(at(800))
    assert history._next == 700 and history.coverage_counts["stale_collector_evidence"] == 0       # nothing was marked, guessed or fed
    evidence = history.evidence(at(800))
    assert evidence["validation_outcome"] == ch.VALID_STALE_COVERAGE and evidence["valid_bin_count"] == 700
    assert evidence["unavailable_seconds"] == 0 and any("pending" in r for r in evidence["reasons"])
    assert evidence["coverage_provenance"]["deferred"]["class"] == "stale_collector_evidence"
    assert evidence["coverage_provenance"]["contract_satisfied"] is False
    s.beat(journal, clock, 850, 850)                                # fresh evidence arrives (a live collector's journal is re-read)
    history.coverage = s.coverage_for(path)
    history.advance_to(at(850))
    assert history._next == 850 and history.evidence(at(850))["validation_outcome"] == ch.VALID_COMPLETE


# ------------------------------------------------------------------------------------------- 6 interrupted collection
def test_06_an_error_invalidates_the_affected_coverage_until_an_explicit_recovery(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 100)
    s.beat(journal, clock, 500)
    clock.at(500.5)
    journal.error("E_LOST_EVENTS", "the collector dropped events")
    clock.at(520.5)
    journal.recovered(at(521))
    s.beat(journal, clock, 600)
    sn = snap(s.coverage_for(path), 700)
    assert sn.classify(at(499), 0)[0] == C.VERIFIED_ZERO
    for second in (500, 510, 520):
        for count in (0, 11):                                       # a positive count in an interrupted second is a lower bound, not a rate
            cls, why = sn.classify(at(second), count)
            assert cls == C.INTERRUPTED_COLLECTION and "collector_error:E_LOST_EVENTS" in why
    assert sn.classify(at(530), 0)[0] == C.VERIFIED_ZERO


def test_06b_an_error_never_recovered_leaves_everything_after_it_uncovered(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 100)
    clock.at(100.5)
    journal.error("E_DISK", "disk full", affects_from_utc=at(90))              # may revoke certified time (the safe direction)
    s.beat(journal, clock, 400, 399, health="error", errors_since_last=1)
    sn = snap(s.coverage_for(path), 500)
    assert sn.classify(at(95), 0)[0] == C.INTERRUPTED_COLLECTION and sn.classify(at(450), 0)[0] == C.INTERRUPTED_COLLECTION
    assert sn.classify(at(80), 0)[0] == C.VERIFIED_ZERO


def test_06c_the_writer_refuses_misuse_and_appends_nothing(tmp_path):
    journal, clock, path = s.new_journal(tmp_path, started=False)
    with pytest.raises(wc.CoverageEvidenceError, match="collector_not_started"):
        s.beat(journal, clock, 10)
    clock.at(-0.5)
    journal.start(observation_start_utc=s.at(0))
    with pytest.raises(wc.CoverageEvidenceError, match="collector_already_started"):
        journal.start(observation_start_utc=s.at(0))
    with pytest.raises(wc.CoverageEvidenceError, match="recovery_without_error"):
        journal.recovered(s.at(5))
    clock.at(-0.4)
    with pytest.raises(wc.CoverageEvidenceError, match="retroactive"):
        chj.CollectorHealthJournal(tmp_path / "other.jsonl", source_id="x", collector_id="c", run_id="r", clock=clock).start(observation_start_utc=s.at(-5))
    rows = path.read_text(encoding="utf-8").splitlines()
    assert len(rows) == 1                                           # only the one valid start
    with pytest.raises(FileExistsError):
        chj.CollectorHealthJournal(path, source_id="x", collector_id="c", run_id="r", clock=clock)          # evidence is never overwritten


# ------------------------------------------------------------------------------------------------ 7 restart boundary
def test_07_a_restart_without_a_stop_leaves_the_time_between_uncovered(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 1000)                                    # inst-1 dies here: no stop record
    second = chj.CollectorHealthJournal(path, source_id=s.SOURCE_ID, collector_id="col", run_id=s.RUN_ID, clock=clock, resume=True,
                                        collector_instance="inst-2")
    clock.at(1050.5)
    second.start(observation_start_utc=at(1060), coverage_scope=wc.SCOPE_SOURCE_STREAM)       # A0k: a restart keeps the declared scope
    s.beat(second, clock, 1100)
    sn = snap(s.coverage_for(path), 1200)
    assert sn.classify(at(999), 0)[0] == C.VERIFIED_ZERO
    for count in (0, 8):
        cls, why = sn.classify(at(1030), count)
        assert cls == C.INTERRUPTED_COLLECTION and why == "restart_boundary"
    assert sn.classify(at(1070), 0)[0] == C.VERIFIED_ZERO
    assert sn.summary()["integrity"]["status"] == "VERIFIED"


def test_07b_a_clean_stop_ends_coverage_at_its_watermark_and_the_next_start_leaves_a_gap(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 100)
    clock.at(150.5)
    journal.stop(at(150), events_received=0)
    sn = snap(s.coverage_for(path), 400)
    assert sn.classify(at(120), 0)[0] == C.VERIFIED_ZERO
    assert sn.classify(at(200), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and "terminated" in sn.classify(at(200), 0)[1]
    assert sn.summary()["collector_active"] is False


def test_07c_a_torn_final_write_is_reported_not_repaired_and_blocks_a_resume(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 100)
    original = path.read_bytes()
    path.write_bytes(original + b'{"schema":"collector_health_event/1","seq":3')                # a crash mid-write
    events, torn, _ = wc.read_journal_file(path)
    assert torn is True and len(events) == 2 and path.read_bytes().startswith(original)
    with pytest.raises(wc.CoverageEvidenceError, match="journal_torn_tail"):
        chj.CollectorHealthJournal(path, source_id=s.SOURCE_ID, collector_id="col", run_id=s.RUN_ID, clock=clock, resume=True)
    assert snap(s.coverage_for(path), 150).classify(at(50), 0)[0] == C.VERIFIED_ZERO              # the verified rows still count


# ---------------------------------------------------------------------------------------------------- 8 duplicate events
def _events(seqs, ids=None, instance="inst-1"):
    ids = ids or [f"e{n}" for n in seqs]
    return [{"id": i, "ts_ms": s.DAY_MS + 10_000 + 1_000 * n, "seq": n, "instance": instance} for i, n in zip(ids, seqs)]


def test_08_duplicate_events_cannot_inflate_a_count_or_hide_a_hole(tmp_path):
    journal, clock, path = s.new_journal(tmp_path, first_event_seq=1)
    s.beat(journal, clock, 100, events=3, last_event_seq=3)
    observed = _events([1, 2, 3]) + _events([2])                                   # event e2 is delivered twice
    sn = snap(s.coverage_for(path, observed_events=observed), 100)
    assert sn.valid and sn.crosscheck["unique_events"] == 3 and sn.crosscheck["duplicates_dropped"] == 1
    assert sn.classify(at(50), 0)[0] == C.VERIFIED_ZERO
    # a heartbeat that counted the duplicate disagrees with the data: contradictory, so the whole attestation is invalid
    journal2, clock2, path2 = s.new_journal(tmp_path, name="inflated.jsonl", first_event_seq=1)
    s.beat(journal2, clock2, 100, events=4, last_event_seq=3)
    bad = snap(s.coverage_for(path2, observed_events=observed), 100)
    assert not bad.valid and bad.failure_code == "heartbeat_event_count_mismatch"
    assert bad.classify(at(50), 0)[0] == C.INVALID_ATTESTATION


def test_08b_a_sequence_hole_is_an_interruption_between_its_neighbours(tmp_path):
    journal, clock, path = s.new_journal(tmp_path, first_event_seq=1)
    s.beat(journal, clock, 100, events=3, last_event_seq=5)
    observed = _events([1, 2, 5])                                                  # events 3 and 4 never arrived
    sn = snap(s.coverage_for(path, observed_events=observed), 100)
    assert sn.valid and sn.crosscheck["sequence_evidence"] == {"inst-1": "checked"}
    cls, why = sn.classify(at(12), 0)                                              # between event 2 (11 s) and event 5 (15 s)
    assert cls == C.INTERRUPTED_COLLECTION and "event_sequence_gap:3-4" in why
    assert sn.classify(at(50), 0)[0] == C.VERIFIED_ZERO


def test_08c_binning_drops_duplicate_transaction_ids_before_counting():
    source = s.h.source()
    binned = ch.bin_transactions([("0xa", s.DAY_MS + 5_000), ("0xa", s.DAY_MS + 5_100), ("0xb", s.DAY_MS + 5_900)], source)
    assert binned.counts[5] == 2 and binned.report["duplicates_dropped"] == 1


# ------------------------------------------------------------------------------------------------ 9 overlapping intervals
def test_09_two_collectors_claiming_the_same_time_make_the_attestation_contradictory():
    coverage = s.forged_coverage([s.start_row(-0.5, 0), s.heartbeat_row(100),
                                  {**s.start_row(100.5, 101), "collector_instance": "inst-2"},     # inst-2 takes over ...
                                  s.heartbeat_row(150)])                                          # ... but inst-1 keeps certifying time
    sn = snap(coverage, 2000)
    assert not sn.valid and sn.failure_code == "instance_mismatch"
    assert sn.classify(at(50), 0)[0] == C.INVALID_ATTESTATION and sn.classify(at(50), 5)[0] == C.INVALID_ATTESTATION


def test_09a_an_error_open_when_the_process_dies_stays_uncovered_up_to_the_next_start():
    coverage = s.forged_coverage([s.start_row(-0.5, 0), s.heartbeat_row(100),
                                  {"kind": "error", "t_utc": s.iso(100.5), "error_code": "E1", "message": "x"},
                                  {**s.start_row(300.5, 301), "collector_instance": "inst-2"}, {**s.heartbeat_row(400), "collector_instance": "inst-2"}])
    sn = snap(coverage, 500)
    assert sn.valid and sn.classify(at(200), 0)[0] == C.INTERRUPTED_COLLECTION and "collector_error:E1" in sn.classify(at(200), 0)[1]
    assert sn.classify(at(350), 0)[0] == C.VERIFIED_ZERO and sn.classify(at(99), 0)[0] == C.VERIFIED_ZERO


def test_09b_a_retroactive_start_claim_is_contradictory():
    sn = snap(s.forged_coverage([s.start_row(500, 0)]), 600)                       # recorded at 500 s, claims observation from 0 s
    assert not sn.valid and sn.failure_code == "retroactive_coverage_claim"


def test_09c_the_asserted_intervals_attestation_still_refuses_overlap():
    with pytest.raises(ch.CapturedHistoryError, match="ordered and non-overlapping"):
        ch.CoverageAttestation(((s.iso(0), s.iso(100)), (s.iso(50), s.iso(200))), "someone", "a basis")


# ---------------------------------------------------------------------------------------------- 10 partial interval coverage
def test_10_coverage_of_part_of_the_requested_interval_leaves_the_rest_unverified(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 400)
    clock.at(400.5)
    journal.stop(at(400), events_received=0)
    coverage = s.coverage_for(path)
    history = s.captured(coverage, {450: 0, 100: 0})
    history.advance_to(at(600))
    sn = snap(coverage, 600)
    classes = [sn.classify(at(i), (history.binned.counts[i])) [0] for i in (399, 400, 450, 599)]
    assert classes == [C.OBSERVED_POSITIVE, C.COVERAGE_GAP, C.UNKNOWN_COVERAGE_ZERO, C.COVERAGE_GAP]
    assert history.coverage_counts["verified_zero"] == 1 and history.coverage_counts["zero_count_unknown_coverage"] == 1
    assert history.evidence(at(600))["validation_outcome"] == ch.VALID_INCOMPLETE and history.evidence(at(600))["valid_bin_count"] == 0
    assert any("outside verified coverage" in r and "coverage_gap" in r for r in history.evidence(at(600))["reasons"])


# ---------------------------------------------------------------------------------------------- 11 invalid source identity
def test_11_evidence_for_another_source_run_or_channel_is_invalid(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=100, channel_id="e1-val-a")
    wrong_source = snap(s.coverage_for(path, source_id="another-source"), 100)
    assert wrong_source.failure_code == "source_identity_mismatch" and wrong_source.classify(at(50), 0)[0] == C.INVALID_ATTESTATION
    assert snap(s.coverage_for(path, run_id="another-run"), 100).failure_code == "run_identity_mismatch"
    assert snap(s.coverage_for(path, channel_id="e1-val-b"), 100).failure_code == "channel_identity_mismatch"
    assert snap(s.coverage_for(path, channel_id="e1-val-a"), 100).valid
    with pytest.raises(ch.CapturedHistoryError, match="coverage_source_mismatch"):
        ch.CapturedHistory(s.h.binned(), coverage=s.coverage_for(path, source_id="another-source"))


def test_11b_a_source_with_no_identity_is_refused():
    with pytest.raises(wc.CoverageEvidenceError, match="source_identity_missing"):
        wc.VerifiedCoverage.no_evidence("", "x")
    with pytest.raises(wc.CoverageEvidenceError, match="identity_missing"):
        chj.CollectorHealthJournal(s.REPO / "never_created.jsonl", source_id="", collector_id="c", run_id="r")


# ----------------------------------------------------------------------------------------------- 12 malformed timestamps
@pytest.mark.parametrize("row,code", [
    ({**s.heartbeat_row(10), "t_utc": "last tuesday"}, "timestamp_malformed"),
    ({**s.heartbeat_row(10), "t_utc": "2026-09-21T00:00:10"}, "timestamp_naive"),
    ({**s.heartbeat_row(10), "observed_through_utc": "2026-09-21T00:00:09.500+00:00"}, "timestamp_not_whole_second"),
    ({**s.heartbeat_row(10), "observed_through_utc": "not a time"}, "timestamp_malformed"),
])
def test_12_malformed_timestamps_make_the_attestation_invalid(row, code):
    sn = snap(s.forged_coverage([s.start_row(-0.5, 0), row]), 100)
    assert sn.failure_code == code and sn.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


def test_12b_a_regressing_timestamp_is_refused():
    sn = snap(s.forged_coverage([s.start_row(-0.5, 0), s.heartbeat_row(50), s.heartbeat_row(40)]), 100)
    assert sn.failure_code == "timestamp_regressed"


# ------------------------------------------------------------------------------------------ 13 contradictory health evidence
@pytest.mark.parametrize("row,code", [
    (s.heartbeat_row(10, errors=2), "contradictory_health_evidence"),                         # healthy but counted errors
    (s.heartbeat_row(10, watermark=20), "future_watermark"),                                   # claims capture it cannot have made yet
    (s.heartbeat_row(10, health="great"), "event_field_invalid"),
    ({**s.heartbeat_row(10), "collector_instance": "someone-else"}, "instance_mismatch"),
])
def test_13_contradictory_or_impossible_health_evidence_invalidates_everything(row, code):
    sn = snap(s.forged_coverage([s.start_row(-0.5, 0), row]), 100)
    assert sn.failure_code == code and sn.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


def test_13b_a_watermark_that_moves_backwards_is_contradictory():
    sn = snap(s.forged_coverage([s.start_row(-0.5, 0), s.heartbeat_row(50), s.heartbeat_row(60, watermark=40)]), 100)
    assert sn.failure_code == "watermark_regressed"


def test_13c_a_self_reported_degraded_state_does_not_certify_and_a_gap_in_heartbeats_cannot_be_certified_across():
    sn = snap(s.forged_coverage([s.start_row(-0.5, 0), s.heartbeat_row(10, health="degraded"), s.heartbeat_row(20)]), 100)
    assert sn.valid and sn.classify(at(5), 0)[0] == C.INTERRUPTED_COLLECTION and sn.classify(at(15), 0)[0] == C.VERIFIED_ZERO
    tight = wc.VerifiedCoverage(source_id=s.SOURCE_ID, mode=wc.MODE_ATTESTED, run_id=s.RUN_ID, policy=wc.CoveragePolicy(max_heartbeat_gap_s=5.0),
                                events=s.forge([s.start_row(-0.5, 0), s.heartbeat_row(4), s.heartbeat_row(60)]))
    t = snap(tight, 100)
    assert t.classify(at(2), 0)[0] == C.VERIFIED_ZERO
    cls, why = t.classify(at(30), 0)
    assert cls == C.COVERAGE_GAP and why == "heartbeat_gap"


def test_13d_a_collector_that_says_healthy_but_never_started_or_whose_journal_is_empty_certifies_nothing():
    assert snap(s.forged_coverage([s.heartbeat_row(10)]), 100).failure_code == "journal_not_started"
    with pytest.raises(wc.CoverageEvidenceError, match="journal_empty"):
        wc.VerifiedCoverage(source_id=s.SOURCE_ID, mode=wc.MODE_ATTESTED, events=[])


# ------------------------------------------------------------------------------------------------ 14 failed integrity check
def test_14_an_altered_journal_row_invalidates_the_whole_attestation_even_the_rows_before_it(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=100)
    assert snap(s.coverage_for(path), 100).valid
    text = path.read_text(encoding="utf-8").replace('"events_received":0', '"events_received":7', 1)
    path.write_text(text, encoding="utf-8")
    sn = snap(s.coverage_for(path), 100)
    assert sn.failure_code == "hash_mismatch"
    assert sn.classify(at(5), 0)[0] == C.INVALID_ATTESTATION and sn.classify(at(5), 5)[0] == C.INVALID_ATTESTATION


def test_14b_a_dropped_or_reordered_row_breaks_the_chain(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=43_203, name="long.jsonl")
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 16
    dropped = tmp_path / "dropped.jsonl"
    dropped.write_text("\n".join(lines[:1] + lines[2:]) + "\n", encoding="utf-8")
    assert snap(s.coverage_for(dropped), 50_000).failure_code == "event_sequence_broken"
    swapped = tmp_path / "swapped.jsonl"
    swapped.write_text("\n".join([lines[0], lines[2], lines[1]] + lines[3:]) + "\n", encoding="utf-8")
    assert not snap(s.coverage_for(swapped), 50_000).valid
    assert snap(s.coverage_for(path), 50_000).valid
    retargeted = tmp_path / "prev_field.jsonl"                                       # only the recorded link to the previous row is altered
    retargeted.write_text(path.read_text(encoding="utf-8").replace('"prev":"' + lines[0].split('"sha256":"')[1][:12], '"prev":"ffffffffffff', 1), encoding="utf-8")
    assert snap(s.coverage_for(retargeted), 50_000).failure_code == "hash_chain_broken"


def test_14c_an_attestation_document_cannot_claim_more_than_its_journal_verifies(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=100)
    coverage = s.coverage_for(path)
    source = {"dataset_id": "synthetic", "dataset_sha256": None, "kind": "synthetic", "is_fabric_observation": False}
    doc = wc.build_attestation(coverage, source=source, as_of=at(100), created_utc="2026-11-21T00:00:00+00:00", journal_path=str(path))
    assert wc.check_attestation(doc, coverage) == []
    forged = json.loads(json.dumps(doc))
    forged["coverage_intervals"].append([s.iso(5000), s.iso(6000)])                 # claims time the journal does not verify
    assert any("coverage_intervals" in p for p in wc.check_attestation(forged, coverage))
    other = json.loads(json.dumps(doc))
    other["journal"]["sha256"] = "0" * 64
    assert any("journal sha256" in p for p in wc.check_attestation(other, coverage))


def test_14d_the_attestation_document_carries_the_required_fields_and_explicit_nulls(tmp_path):
    _, _, path = s.covered_journal(tmp_path, until=100)
    source = {"dataset_id": "synthetic", "dataset_sha256": None, "kind": "synthetic", "is_fabric_observation": False}
    doc = wc.build_attestation(s.coverage_for(path), source=source, as_of=at(100), created_utc="2026-11-21T00:00:00+00:00", journal_path=str(path))
    for key in ("schema", "schema_version", "created_utc", "source", "run_id", "channel_id", "observation_interval", "coverage_intervals",
                "collector", "events", "timestamp_provenance", "integrity", "failure_reason", "journal", "policy", "mode"):
        assert key in doc, key
    assert doc["schema"] == wc.COVERAGE_ATTESTATION_SCHEMA and doc["integrity"]["status"] == "VERIFIED" and doc["failure_reason"] is None
    assert doc["collector"]["heartbeat_count"] == 1 and doc["journal"]["chain_head_sha256"] and doc["source"]["dataset_sha256"] is None
    none = wc.build_attestation(wc.VerifiedCoverage.no_evidence(s.SOURCE_ID, "no collector evidence in the archive"), source=source,
                                as_of=at(100), created_utc="2026-11-21T00:00:00+00:00")
    assert none["integrity"]["status"] == "NO_EVIDENCE" and none["collector"] is None and none["journal"] is None and none["coverage_intervals"] == []
    assert none["failure_reason"] == "no collector evidence in the archive" and none["events"]["sequence_evidence"] is None


# ----------------------------------------------------------------------------------- 15 benchmark zero-fill separation
def test_15_benchmark_zero_fill_is_labelled_non_live_and_can_never_satisfy_the_contract(tmp_path):
    history = ch.CapturedHistory(s.h.binned({50: 0}), zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL)
    history.advance_to(at(700))
    evidence = history.evidence(at(700))
    provenance = evidence["coverage_provenance"]
    assert evidence["validation_outcome"] == ch.VALID_COMPLETE and evidence["coverage_mode"] == wc.MODE_BENCHMARK
    assert provenance["contract_satisfied"] is False and provenance["live_quality"] is False and "never live-quality" in provenance["note"]
    assert evidence["live_semantics"] is False and history.counts_fed["zero_assumed_benchmark"] == 1
    assert provenance["class_counts"]["unverified_historical_zero_fill"] == 1 and "verified_zero" not in provenance["class_counts"]
    _, _, path = s.covered_journal(tmp_path, until=100)
    with pytest.raises(ch.CapturedHistoryError, match="zero_policy_conflict"):
        ch.CapturedHistory(s.h.binned(), zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL, coverage=s.coverage_for(path))
    asserted = ch.CoverageAttestation(((s.iso(0), s.iso(100)),), "someone", "a basis")
    with pytest.raises(ch.CapturedHistoryError, match="zero_policy_conflict"):
        ch.CapturedHistory(s.h.binned(), attestation=asserted, coverage=s.coverage_for(path))
    with pytest.raises(wc.CoverageEvidenceError, match="coverage_mode_invalid"):
        wc.VerifiedCoverage(source_id=s.SOURCE_ID, mode=wc.MODE_BENCHMARK)


def test_15b_the_default_stays_strict_and_unchanged_and_the_asserted_attestation_is_not_the_contract():
    history = ch.CapturedHistory(s.h.binned({50: 0}))
    history.advance_to(at(700))
    assert history.coverage_mode == wc.MODE_NO_EVIDENCE and history.zero_policy == ch.ZERO_POLICY_VERIFIED_ONLY
    assert history.evidence(at(700))["validation_outcome"] == ch.VALID_INCOMPLETE
    asserted = ch.CapturedHistory(s.h.binned({50: 0}), attestation=ch.CoverageAttestation(((s.iso(0), s.iso(100)),), "someone", "a basis"))
    asserted.advance_to(at(700))
    assert asserted.coverage_mode == wc.MODE_LEGACY_ASSERTED and asserted.coverage_provenance(at(700))["contract_satisfied"] is False


def test_the_coverage_modules_import_no_process_network_model_or_numeric_library():
    import ast
    from pathlib import Path
    forbidden = {"subprocess", "socket", "urllib", "http", "requests", "pickle", "joblib", "sklearn", "numpy", "pandas", "shutil"}
    for name in ("workload_coverage", "collector_health", "abstention_accounting"):
        tree = ast.parse((s.REPO / "src" / "fabric_perf" / "controllers" / f"{name}.py").read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        imported |= {(n.module or "").split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0}
        assert not imported & forbidden, (name, imported & forbidden)
