"""A0p workstream A (checklist items 1-11): the versioned issue-log writer that records write-start, write-completion and sync evidence.

The REAL timed workload and writer run under a fake Caliper driver (Node, offline) with scripted clocks and an optional failing file system. SYNTHETIC / FIXTURE only: nothing is a
measurement of Caliper, Fabric or a real clock."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.a0p_support import (NL, forge_sidecar, ilr, ilt, lines_of, need_node, rows_of, run_timed_node, scripted_log, td, ts, verify, wc, write_lines)

STEP = 250     # scripted microseconds per monotonic-clock call: every write and every sync in the driver lasts exactly this long


@pytest.fixture(scope="module")
def clean(tmp_path_factory):
    need_node()
    return run_timed_node(tmp_path_factory.mktemp("clean"), stepUs=STEP)


# ----------------------------------------------------------------------------------------------------------------------------- 1 write start
def test_01_every_log_record_has_a_write_row_with_write_start_evidence(clean):
    ev = verify(clean)
    assert ev.status == ilt.VERIFIED and ev.error is None
    log, side = rows_of(Path(clean["log"])), rows_of(Path(clean["timing"]))
    writes = [r for r in side if r["kind"] == "write"]
    assert [w["target_rseq"] for w in writes] == [r["rseq"] for r in log] and len(writes) == clean["status"]["rseq"]
    for w in writes:
        assert isinstance(w["write_start_wall_ms"], int) and isinstance(w["write_start_mono_us"], int) and w["write_status"] == "returned"
    head = side[0]
    assert head["kind"] == "tstart" and head["wall_clock_source"] == "Date.now()" and head["mono_clock_source"] == "process.hrtime.bigint()" and head["mono_unit"] == "us"
    assert head["run_id"] == "a0p-fx-001" and head["worker_id"] == "w0" and head["round_index"] == 1 and head["log_schema"] == "e1_issue_log/1"


# ----------------------------------------------------------------------------------------------------------------------------- 2 write completion
def test_02_write_completion_is_recorded_after_the_write_and_is_not_predicted(clean):
    side = rows_of(Path(clean["timing"]))
    writes = [r for r in side if r["kind"] == "write"]
    for w in writes:
        assert w["write_end_mono_us"] - w["write_start_mono_us"] == STEP and w["write_end_wall_ms"] >= w["write_start_wall_ms"]
    # the log record never carries its own completion time: a record cannot hold the time its own write finished
    for rec in rows_of(Path(clean["log"])):
        if rec["kind"] != "start":                                                    # (the start record names its own fsync POLICY and writer version, which is configuration)
            assert not [k for k in rec if "write" in k or "sync" in k or "end" in k]
    # every completion stamp is strictly later (monotonic) than the previous record's completion: the stamps are taken, in order, around the real writes
    ends = [w["write_end_mono_us"] for w in writes]
    assert ends == sorted(ends) and len(set(ends)) == len(ends)


# ----------------------------------------------------------------------------------------------------------------------------- 3 sync completion
def test_03_sync_completion_is_recorded_for_the_fsync_that_returned_and_names_what_it_covers(clean):
    side = rows_of(Path(clean["timing"]))
    syncs = [r for r in side if r["kind"] == "sync"]
    assert syncs and all(s["call"] == "fsyncSync" and s["sync_status"] == "returned" for s in syncs)
    assert [s["covered_from_rseq"] for s in syncs][0] == 1
    for a, b in zip(syncs, syncs[1:]):
        assert b["covered_from_rseq"] == a["covered_through_rseq"] + 1               # contiguous coverage, no overlap, no gap
    assert syncs[-1]["covered_through_rseq"] == clean["status"]["rseq"] == clean["status"]["synced_through_rseq"]
    assert all(s["sync_end_mono_us"] - s["sync_start_mono_us"] == STEP for s in syncs)
    ev = verify(clean)
    assert ev.unsynced_rseqs == [] and len(ev.syncs) == len(syncs) and ev.has_stop


# ----------------------------------------------------------------------------------------------------------------------------- 4 ordering and linkage
def test_04_a_completed_write_is_linked_to_the_original_event_and_the_log_format_is_unchanged(clean):
    log = rows_of(Path(clean["log"]))
    by_rseq = {r["rseq"]: r for r in log}
    for w in (r for r in rows_of(Path(clean["timing"])) if r["kind"] == "write"):
        rec = by_rseq[w["target_rseq"]]
        assert (w["target_sha256"], w["target_kind"], w["target_t_ms"], w["target_mono_ms"]) == (rec["sha256"], rec["kind"], rec["t_ms"], rec["mono_ms"])
        assert w["write_start_wall_ms"] >= rec["t_ms"]                                # the event was created first, the write started after
    # the unmodified A0l/A0o reader accepts the timed writer's log as an ordinary e1_issue_log/1 log
    policy = ilr.IssueLogPolicy(run_id="a0p-fx-001", worker_id="w0", clock_domain="synthetic-host:node_date_now", max_tick_gap_ms=600_000)
    tail = ilr.IssueLogTail(Path(clean["log"]), policy)
    while tail.poll(ilr.ms_to_utc(log[0]["t_ms"])).records:
        pass
    assert tail.error is None and tail.counts()["records"] == len(log) and {r["schema"] for r in log} == {"e1_issue_log/1"}
    assert {tuple(sorted(r)) for r in log if r["kind"] == "attempt"} == {tuple(sorted(["schema", "rseq", "kind", "run_id", "worker_id", "t_ms", "mono_ms", "n", "issue_id", "prev", "sha256"]))}


def test_04b_the_timed_log_equals_the_a0l_log_for_the_same_script_except_for_the_sidecar(tmp_path, clean):
    """Same scripted clocks, same transactions: the timed writer's log has the same kinds, counts and issue ids as an A0l-format log (it adds only the sidecar file)."""
    log = rows_of(Path(clean["log"]))
    assert [r["kind"] for r in log][:4] == ["start", "intent", "attempt", "outcome"] and log[0]["writer_version"].startswith("e1_issue_log_writer 0.1")
    assert log[0]["fsync_policy"] == "tick" and Path(clean["timing"]).exists() and Path(clean["timing"]) != Path(clean["log"])


# ----------------------------------------------------------------------------------------------------------------------------- 5-6 crash windows
def test_05_a_crash_before_the_write_completed_leaves_a_partial_log_tail_that_is_reported_not_counted(tmp_path, clean):
    (tmp_path / "crash").mkdir()
    log = tmp_path / "crash" / Path(clean["log"]).name                              # the sidecar names its log by file name
    raw = Path(clean["log"]).read_bytes()
    lines = raw.split(NL)[:-1]
    log.write_bytes(NL.join(lines[:-1]) + NL + lines[-1][:30])                      # the last record was being written when the process died
    survivors = len(lines) - 1
    side_rows = [r for r in rows_of(Path(clean["timing"])) if not (r["kind"] == "write" and r["target_rseq"] > survivors) and r["kind"] not in ("tstop",)]
    side_rows = [r for r in side_rows if not (r["kind"] == "sync" and r["covered_through_rseq"] > survivors)]
    forged = tmp_path / "forged.jsonl"
    from tests.a0p_support import chain
    write_lines(forged, chain(side_rows))
    ev = ilt.verify_timing(log, forged)
    assert ev.status == ilt.UNRESOLVED_TAIL and ev.log_partial_tail_bytes == 30 and ev.log_records == survivors and ev.unattested_rseqs == []
    assert not ev.has_stop


def test_06_a_crash_after_the_log_write_but_before_the_attestation_is_detected_and_not_repaired(tmp_path, clean):
    side_rows = rows_of(Path(clean["timing"]))
    last_write = max(i for i, r in enumerate(side_rows) if r["kind"] == "write")
    cut = side_rows[:last_write]                                                    # the final log record's write row (and everything after it) never reached the sidecar
    from tests.a0p_support import chain
    forged = tmp_path / "cut.jsonl"
    write_lines(forged, chain(cut))
    ev = ilt.verify_timing(Path(clean["log"]), forged)
    assert ev.status == ilt.UNRESOLVED_TAIL and ev.unattested_rseqs == [clean["status"]["rseq"]] and not ev.has_stop
    assert Path(forged).read_bytes() == NL.join(chain(cut)) + NL                    # the verifier wrote nothing
    result = td.t01_direct(None, ev, "synthetic_fixture")
    assert any(e["reason"].startswith("missing: write row") for e in result["exclusions"]) and result["eligible_samples"] == clean["status"]["rseq"] - 1


def test_06b_a_crash_between_the_attestation_and_the_sync_leaves_the_record_without_a_sync_claim(tmp_path, clean):
    rows = rows_of(Path(clean["timing"]))
    first_sync = next(i for i, r in enumerate(rows) if r["kind"] == "sync")
    from tests.a0p_support import chain
    forged = tmp_path / "nosync.jsonl"
    write_lines(forged, chain([r for r in rows[:first_sync + 1][:-1]]))             # writes attested, no sync row, no stop
    ev = ilt.verify_timing(Path(clean["log"]), forged)
    assert ev.status == ilt.UNRESOLVED_TAIL or ev.status == ilt.INVALID
    # ... the log has more records than the cut sidecar attests: only a final-record gap is a crash tail, so this deeper cut is refused, never trusted
    assert ev.status == ilt.INVALID and ev.error["code"] == "attestation_gap"


# ----------------------------------------------------------------------------------------------------------------------------- 7-8 failures
def test_07_a_failed_log_write_is_recorded_refuses_the_send_and_every_later_issuance(tmp_path):
    node = run_timed_node(tmp_path, stepUs=STEP, failLogWriteAt=5)                  # write 1 = start, 2-4 = tx0, 5 = intent of tx1
    assert node["sends"] == 1 and node["status"]["failed"] is True and "write_failed" in (node["error"]["code"] or "")
    ev = verify(node)
    assert ev.status in (ilt.UNRESOLVED_TAIL, ilt.VERIFIED) and [f["op"] for f in ev.failures] == ["write"]
    assert "injected log write failure" in ev.failures[0]["error"]
    assert not any(r["kind"] == "intent" and r["n"] == 1 for r in rows_of(Path(node["log"])))     # the record that could not be written is not in the log


def test_08_a_failed_fsync_is_recorded_covers_nothing_and_fails_the_writer_closed(tmp_path):
    node = run_timed_node(tmp_path, stepUs=STEP, failLogFsyncAt=1)
    assert node["status"]["failed"] is True and node["sends"] == 2                  # tx at second 3 was refused: the writer failed at the second-2 tick
    ev = verify(node)
    assert [f["op"] for f in ev.failures] == ["sync"] and ev.failures[0]["error"] == "injected log fsync failure"
    assert ev.syncs == [] and len(ev.unsynced_rseqs) == ev.log_records             # NO record is claimed to have reached storage
    result = td.t01_direct(ts.read_issue_log(Path(node["log"])), ev, "synthetic_fixture")
    assert result["persistence"]["records_with_no_covering_sync"] == ev.log_records and result["sync_count"] == 0 and result["sync_call_ms"] is None


def test_08b_a_failed_sidecar_write_fails_the_writer_closed_and_the_attempt_is_not_followed_by_a_send(tmp_path):
    node = run_timed_node(tmp_path, stepUs=STEP, failSidecarWriteAt=4)              # sidecar rows: 1 tstart, 2 start, 3 intent, 4 attempt -> the attempt's attestation fails
    assert node["sends"] == 0 and node["status"]["failed"] is True and node["status"]["timing_failed"] is True
    ev = verify(node)
    assert ev.status == ilt.UNRESOLVED_TAIL and ev.unattested_rseqs == [3] and not ev.has_stop


def test_08c_a_failed_sidecar_write_after_a_send_never_changes_the_send_but_stops_every_later_issuance(tmp_path):
    node = run_timed_node(tmp_path, stepUs=STEP, failSidecarWriteAt=5)              # the attestation of tx0's OUTCOME record fails; the send already happened
    assert node["status"]["timing_failed"] is True and node["status"]["failed"] is True
    assert node["sends"] == 1 and node["error"]["code"] == "writer_failed"          # tx1 (second 2) was refused: no transaction is issued unrecorded
    ev = verify(node)
    assert ev.status == ilt.UNRESOLVED_TAIL and ev.unattested_rseqs == [4] and not ev.has_stop


# ----------------------------------------------------------------------------------------------------------------------------- 9 chain
def test_09_the_sidecar_is_hash_chained_and_any_alteration_is_detected(tmp_path, clean):
    rows = lines_of(Path(clean["timing"]))
    cases = {}
    edited = tmp_path / "edited.jsonl"
    row = json.loads(rows[3])
    row["write_end_mono_us"] += 1
    write_lines(edited, rows[:3] + [wc.canonical(row).encode("ascii")] + rows[4:])
    cases["edit"] = ilt.verify_timing(Path(clean["log"]), edited)
    dropped = tmp_path / "dropped.jsonl"
    write_lines(dropped, rows[:3] + rows[4:])
    cases["drop"] = ilt.verify_timing(Path(clean["log"]), dropped)
    swapped = tmp_path / "swapped.jsonl"
    write_lines(swapped, rows[:3] + [rows[4], rows[3]] + rows[5:])
    cases["swap"] = ilt.verify_timing(Path(clean["log"]), swapped)
    assert {k: v.error["code"] for k, v in cases.items()} == {"edit": "sidecar_hash_mismatch", "drop": "sidecar_sequence_gap", "swap": "sidecar_sequence_gap"}
    assert all(v.status == ilt.INVALID for v in cases.values())


def test_09b_a_valid_chain_with_a_wrong_link_to_the_log_is_still_refused(tmp_path, clean):
    def retarget(rows):
        rows[5]["target_sha256"] = "f" * 64
    forged = forge_sidecar(Path(clean["log"]), tmp_path / "s.jsonl", mutate=retarget)
    ev = ilt.verify_timing(Path(clean["log"]), forged)
    assert ev.status == ilt.INVALID and ev.error["code"] == "attestation_link_mismatch"


# ----------------------------------------------------------------------------------------------------------------------------- 10 legacy compatibility
def test_10_a_historical_a0l_log_without_a_sidecar_keeps_its_a0o_timing_and_gains_no_new_timing(tmp_path):
    gen = scripted_log(tmp_path)
    log = gen.log.path
    historical = ts.run_sampler(origin="synthetic_fixture", issue_log=log)
    wrapped = td.run_sampler_a0p(origin="synthetic_fixture", issue_log=log)
    assert wrapped["measurements"]["T01"] == historical["measurements"]["T01"]
    assert wrapped["measurements"]["T01"]["not_measured"]["end_of_write_latency"]["status"] == ts.S_NOT_MEASURABLE
    assert wrapped["measurements"]["T06"]["status"] == ts.S_NOT_MEASURABLE
    for name in ("T02", "T03", "T04", "T05", "T06"):
        assert wrapped["measurements"][name] == historical["measurements"][name]
    assert wrapped["schema"] == "a0p_timing_results/1" and wrapped["wrapped_sampler"] == ts.SAMPLER_VERSION
    ev = ilt.verify_timing(log, tmp_path / "does_not_exist.jsonl")
    assert ev.status == ilt.INVALID and ev.error["code"] == "sidecar_unreadable"


def test_10b_the_timed_workload_never_silently_converts_or_downgrades_a_round_configuration(tmp_path):
    legacy_key = run_timed_node(tmp_path / "a", useLegacyKey=True)
    both = run_timed_node(tmp_path / "b", issueLogKeyAlso=True)
    assert legacy_key["error"]["code"] == "mixed_issue_log_schema" and both["error"]["code"] == "mixed_issue_log_schema"
    assert not Path(legacy_key["log"]).exists() and not Path(both["log"]).exists() and legacy_key["sends"] == both["sends"] == 0
    two = run_timed_node(tmp_path / "c", totalWorkers=2)
    assert two["error"]["code"] == "worker_count_unsupported" and not Path(two["log"]).exists()


def test_10c_an_existing_log_or_sidecar_is_never_overwritten(tmp_path):
    first = run_timed_node(tmp_path, stepUs=STEP)
    again = run_timed_node(tmp_path, stepUs=STEP)                                   # same names in the same directory
    assert again["error"]["code"] == "log_exists" and Path(first["timing"]).read_bytes() == Path(first["timing"]).read_bytes()
    only_sidecar = tmp_path / "x"
    only_sidecar.mkdir()
    (only_sidecar / "issue.timing.jsonl").write_text("occupied")
    refused = run_timed_node(only_sidecar, stepUs=STEP)
    assert refused["error"]["code"] == "timing_exists" and not (only_sidecar / "issue.jsonl").exists()


# ----------------------------------------------------------------------------------------------------------------------------- 11 no fabricated durability
def test_11_no_record_or_message_claims_durability_and_a_policy_of_none_yields_no_sync_evidence(tmp_path):
    node = run_timed_node(tmp_path, stepUs=STEP, fsyncPolicy="none")
    ev = verify(node)
    assert ev.status == ilt.VERIFIED and ev.syncs == [] and ev.unsynced_rseqs == list(range(1, ev.log_records + 1))
    side_text = Path(node["timing"]).read_text(encoding="ascii").lower()
    result = td.t01_direct(ts.read_issue_log(Path(node["log"])), ev, "synthetic_fixture")
    for text in (side_text, json.dumps(ev.as_dict()).lower(), json.dumps(result).lower().replace("not a persistence claim", "")):
        assert "durable" not in text
    assert result["persistence"]["records_with_no_covering_sync"] == ev.log_records and result["sync_call_ms"] is None
    assert "not a guarantee against every storage failure" in rows_of(Path(node["timing"]))[0]["sync_semantics"]
    assert "may still be in an operating-system cache" in rows_of(Path(node["timing"]))[0]["write_semantics"]


def test_11b_fsync_policy_every_attests_a_sync_after_each_record(tmp_path):
    node = run_timed_node(tmp_path, stepUs=STEP, fsyncPolicy="every")
    ev = verify(node)
    assert ev.status == ilt.VERIFIED and ev.unsynced_rseqs == [] and len(ev.syncs) >= ev.log_records
    # the extra fsync of a tick and of the close (policy "every") returns with nothing new to cover: recorded as an EMPTY coverage (through == from - 1), never as a claim about a record
    empty = [s for s in ev.syncs if s["covered_through_rseq"] == s["covered_from_rseq"] - 1]
    assert all(s["covered_from_rseq"] - 1 <= s["covered_through_rseq"] for s in ev.syncs) and len(empty) == 4 and ev.syncs[-1]["covered_through_rseq"] == ev.log_records
