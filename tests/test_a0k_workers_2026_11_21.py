"""A0k workstream D (checklist items 20-29): worker-aware coverage. One worker's complete evidence never stands for another's; a missing, duplicate,
unexpected or mislabelled worker never looks like complete source coverage. Synthetic scripted workers only: no real Caliper run exists.
"""

from __future__ import annotations

import pytest

from tests import a0j_support as j
from tests import a0k_support as k
from tests.a0k_support import C, DOMAIN, FAST, SHORT, at, rc, wk


def two_workers(tmp_path, **kwargs):
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: k.short_worker(tmp_path, 1, zero={5})}
    return recs, k.worker_coverage(tmp_path, recs, **kwargs)


# ------------------------------------------------------------------------------------------------------------- 20 a complete two-worker interval
def test_20_two_workers_with_complete_evidence_certify_a_positive_second_and_a_genuine_zero(tmp_path):
    _recs, (multi, _per) = two_workers(tmp_path)
    snap = multi.snapshot(at(SHORT + 1))
    assert snap.valid and snap.blocks == () and sorted(snap.workers) == [0, 1]
    assert snap.classify(at(4), 40)[0] == C.OBSERVED_POSITIVE
    cls, why = snap.classify(at(5), 0)
    assert cls == C.VERIFIED_ZERO and "2 expected worker(s)" in why
    assert snap.summary()["recorder_evidence"]["live_source_coverage_verified"] is False


def test_20b_one_workers_zero_second_is_a_positive_total_when_the_other_worker_saw_traffic(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: k.short_worker(tmp_path, 1)}
    multi, _per = k.worker_coverage(tmp_path, recs)
    assert multi.snapshot(at(SHORT + 1)).classify(at(5), recs[1].units[5])[0] == C.OBSERVED_POSITIVE


def test_20c_the_union_of_worker_submit_events_feeds_one_recorder_history(tmp_path):
    recs, (multi, per) = two_workers(tmp_path)
    merged, report = wk.merge_worker_submit_events({i: rc.read_submit_events(r.submits_path, instance=r.instance)[0] for i, r in recs.items()})
    assert report["workers"] == [0, 1] and report["events"] == len(merged) and report["cross_worker_timestamp_ties"] == len(merged) // 2
    assert "never matched" in report["ordering"]
    history = rc.RecorderCapturedHistory(rc.bin_submit_events(merged, j.recorder_source()), coverage=multi, policy=FAST)
    history.advance_to(at(SHORT + 1))
    assert history.coverage_counts[C.VERIFIED_ZERO.value] == 1 and history.coverage_counts[C.OBSERVED_POSITIVE.value] >= 18
    block = history.coverage_provenance(at(SHORT + 1))
    assert block["worker_coverage"]["schema"] == wk.WORKER_SCHEMA and block["live_quality"] is False and block["synthetic_evidence"] is True
    assert block["worker_coverage"]["events_are_never_matched_across_workers"] is True


def test_20d_a_single_worker_through_the_multi_worker_class_agrees_with_the_recorder_class(tmp_path):
    rec = k.short_worker(tmp_path, 0, zero={5})
    multi, per = k.worker_coverage(tmp_path, {0: rec})
    for second, count in ((4, 4), (5, 0), (9, 9)):
        assert multi.snapshot(at(SHORT + 1)).classify(at(second), count)[0] == per[0].snapshot(at(SHORT + 1)).classify(at(second), count)[0]


# ------------------------------------------------------------------------------------------------------------------------------ 21 missing worker
def test_21_a_missing_worker_leaves_every_second_unverified_and_is_not_an_invalid_attestation(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: k.short_worker(tmp_path, 1, zero={5})}
    multi, _per = k.worker_coverage(tmp_path, recs, drop=(1,))
    snap = multi.snapshot(at(SHORT + 1))
    assert snap.valid and snap.blocks[0][0] == wk.RC_WORKER_MISSING
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO                           # worker 0 alone is complete, and it is not enough
    assert snap.classify(at(4), 4)[0] == C.COVERAGE_GAP                                    # a positive total with one worker silent is a lower bound
    assert wk.RC_WORKER_MISSING in snap.cause_codes()


def test_21b_a_roster_of_three_with_two_reporting_names_the_missing_one(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0), 1: k.short_worker(tmp_path, 1)}
    roster = wk.WorkerRoster.from_benchmark_config(3, DOMAIN)
    multi, _per = k.worker_coverage(tmp_path, recs, roster=roster)
    assert "[2]" in multi.snapshot(at(SHORT + 1)).blocks[0][1]


# ------------------------------------------------------------------------------------------------------------------------------ 22 duplicate worker
def test_22_a_duplicate_worker_identity_invalidates_the_whole_attestation(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: k.short_worker(tmp_path, 1, zero={5})}
    multi, _per = k.worker_coverage(tmp_path, recs, duplicate=0)
    snap = multi.snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == wk.RC_WORKER_DUPLICATE
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION and snap.classify(at(4), 8)[0] == C.INVALID_ATTESTATION


def test_22b_two_workers_with_the_same_event_ids_collide_in_the_merge(tmp_path):
    rec = k.short_worker(tmp_path, 0)
    events, _ = rc.read_submit_events(_submits(rec), instance=rec.instance)
    with pytest.raises(wk.WorkerContractError) as excinfo:
        wk.merge_worker_submit_events({0: events, 1: events})
    assert excinfo.value.code == "worker_event_id_collision"


def _submits(rec):
    rec.write()
    return rec.submits_path


# ----------------------------------------------------------------------------------------------------------------------- 23 unknown worker roster
def test_23_an_unknown_roster_means_no_interval_can_be_called_complete(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0, zero={5})}
    multi, _per = k.worker_coverage(tmp_path, recs, roster=wk.WorkerRoster.unknown())
    snap = multi.snapshot(at(SHORT + 1))
    assert snap.valid and snap.blocks[0][0] == wk.RC_ROSTER_UNKNOWN
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and snap.classify(at(4), 4)[0] == C.COVERAGE_GAP
    assert wk.RC_ROSTER_UNKNOWN in snap.cause_codes() and snap.summary()["worker_coverage"]["roster"]["known"] is False


def test_23b_a_roster_needs_a_declared_basis_and_a_clock_domain_per_worker():
    with pytest.raises(wk.WorkerContractError):
        wk.WorkerRoster({0: DOMAIN}, basis=None)
    with pytest.raises(wk.WorkerContractError):
        wk.WorkerRoster({0: " "}, basis="benchmark_config")
    with pytest.raises(wk.WorkerContractError):
        wk.WorkerRoster({-1: DOMAIN}, basis="benchmark_config")
    with pytest.raises(wk.WorkerContractError):
        wk.WorkerRoster.from_benchmark_config(0, DOMAIN)
    roster = wk.WorkerRoster.from_benchmark_config(2, DOMAIN)
    assert roster.known and "declared" in roster.as_dict()["verification"] and roster.basis == "benchmark_config"


def test_23c_the_actual_e1_workloads_refuse_more_than_one_worker_so_a_multi_worker_run_cannot_happen_today():
    for relative in ("fabric-testbed/caliper/workloads/e1_create_asset.js", "fabric-testbed/caliper/workloads/e1_trace_replay.js"):
        assert "totalWorkers !== 1" in (j.REPO / relative).read_text(encoding="utf-8"), relative
    assert '"workers": {"number": 1}' in (j.REPO / "scripts/e1/e1_la2_render.py").read_text(encoding="utf-8")
    keys = (j.REPO / "fabric-testbed/caliper/workloads/e1_workload_core.js").read_text(encoding="utf-8")
    assert "function keyFor(runId, roundIndex, seq)" in keys                                # no worker index: two workers would collide on asset keys


# ----------------------------------------------------------------------------------------------------------------------- 24 worker interruption
def test_24_a_worker_that_loses_events_uncovers_its_seconds_for_the_whole_combination(tmp_path):
    w1 = k.short_worker(tmp_path, 1, zero={5}, stop=False)
    w1.rows = [r for r in w1.rows if not (r["kind"] == "heartbeat" and 9 <= int(r["t_utc"][17:19]) <= 11)]      # its heartbeats 9..11 never arrive
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: w1}
    multi, _per = k.worker_coverage(tmp_path, recs)
    snap = multi.snapshot(at(SHORT + 1))
    assert snap.classify(at(5), 0)[0] == C.VERIFIED_ZERO
    cls, why = snap.classify(at(10), 20)
    assert cls != C.OBSERVED_POSITIVE and why.startswith("worker 1")


def test_24b_a_worker_whose_evidence_is_invalid_invalidates_the_combination_not_just_itself(tmp_path):
    w1 = k.short_worker(tmp_path, 1, zero={5})
    w1.rows[3]["events_received"] += 1                                                       # its heartbeat count no longer matches its own submits file
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: w1}
    multi, _per = k.worker_coverage(tmp_path, recs)
    snap = multi.snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == wk.RC_WORKER_INVALID and "worker 1" in snap.failure_reason and "heartbeat_event_count_mismatch" in snap.failure_reason
    assert snap.classify(at(5), 0)[0] == C.INVALID_ATTESTATION


def test_24c_one_worker_without_a_source_count_leaves_the_zero_unverified_for_the_whole_combination(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: k.short_worker(tmp_path, 1, zero={5}, source_count=False)}
    multi, _per = k.worker_coverage(tmp_path, recs)
    snap = multi.snapshot(at(SHORT + 1))
    cls, why = snap.classify(at(5), 0)
    assert cls == C.UNKNOWN_COVERAGE_ZERO and why.startswith("worker 1") and wc_cause() in why
    assert snap.recorder_only_zero_seconds([0] * 30, j.DAY_START, 20) >= 1


def wc_cause() -> str:
    return k.wc.RECORDER_ONLY_CAUSE


# ------------------------------------------------------------------------------------------------------------------------ 25 conflicting counts
def test_25_a_declared_total_that_disagrees_with_the_workers_source_counts_is_a_contradiction(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: k.short_worker(tmp_path, 1, zero={5})}
    total = sum(r.source_through(SHORT) for r in recs.values())                                # what the last heartbeat (watermark 20) reports
    ok, per = k.worker_coverage(tmp_path, recs, declared_total=total)
    assert ok.snapshot(at(SHORT + 1)).valid
    bad = wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN), workers=list(per.items()), declared_source_total=total + 3)
    snap = bad.snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == wk.RC_TOTALS and str(total + 3) in snap.failure_reason


def test_25b_a_negative_or_non_integer_declared_total_is_refused_at_construction(tmp_path):
    rec = k.short_worker(tmp_path, 0)
    _report, cov, _events = k.recorder_coverage(rec, tmp_path)
    for bad in (-1, 1.5, True):
        with pytest.raises(wk.WorkerContractError):
            wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(1, DOMAIN), workers=[(0, cov)], declared_source_total=bad)


# ---------------------------------------------------------------------------------------------------------- 26 unsynchronized worker timestamps
def test_26_workers_that_declare_different_clock_domains_are_never_joined(tmp_path):
    roster = wk.WorkerRoster({0: "host-a:node_date_now", 1: "host-b:node_date_now"}, "benchmark_config", "synthetic")
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: k.short_worker(tmp_path, 1, zero={5})}
    multi, _per = k.worker_coverage(tmp_path, recs, roster=roster)
    snap = multi.snapshot(at(SHORT + 1))
    assert snap.valid and snap.blocks[0][0] == wk.RC_CLOCK_DOMAINS
    assert snap.classify(at(5), 0)[0] == C.UNKNOWN_COVERAGE_ZERO and snap.classify(at(4), 8)[0] == C.COVERAGE_GAP
    assert wk.RC_CLOCK_DOMAINS in snap.cause_codes()


def test_26b_the_merge_keeps_a_stable_order_and_counts_ties_without_resolving_them(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0), 1: k.short_worker(tmp_path, 1)}
    merged, report = wk.merge_worker_submit_events({i: rc.read_submit_events(_submits(r), instance=r.instance)[0] for i, r in recs.items()})
    keys = [(e["ts_ms"], e["instance"], e["seq"]) for e in merged]
    assert keys == sorted(keys) and report["cross_worker_timestamp_ties"] > 0 and len(merged) == sum(len(rc.read_submit_events(r.submits_path, instance=r.instance)[0]) for r in recs.values())


# -------------------------------------------------------------------------------------------------------------------- 27 partial worker coverage
def test_27_a_worker_that_starts_observing_late_makes_the_earlier_seconds_unverified_for_everyone(tmp_path):
    late = j.SyntheticRecorder(tmp_path, name="w1", instance="rec:run-a0j:w1:r0", worker=1, declared_interval_ms=1000, source_count=True).start(t=9.5, observation=10)
    late.traffic(range(10, SHORT + 1), zero={15})
    for second in range(11, SHORT + 1):
        late.beat(second)
    late.stop(SHORT)
    first = k.short_worker(tmp_path, 0, zero={15})
    multi, _per = k.worker_coverage(tmp_path, {0: first, 1: late})
    snap = multi.snapshot(at(SHORT + 1))
    assert snap.valid and snap.classify(at(4), 4)[0] != C.OBSERVED_POSITIVE                    # worker 1 was not observing yet: the total is a lower bound
    assert snap.classify(at(15), 0)[0] == C.VERIFIED_ZERO and snap.classify(at(12), 12)[0] == C.OBSERVED_POSITIVE


# --------------------------------------------------------------------------------------------------------------------------- 28 restart boundary
def test_28_a_worker_restart_leaves_the_gap_uncovered_for_the_combination(tmp_path):
    first = j.SyntheticRecorder(tmp_path, name="w1a", instance="rec:run-a0j:w1:r0", worker=1, declared_interval_ms=1000, source_count=True).start()
    first.traffic(range(0, 7))
    for second in range(1, 7):
        first.beat(second)                                                                   # the worker dies at second 6: no stop
    second = j.SyntheticRecorder(tmp_path, name="w1b", instance="rec:run-a0j:w1:r1", worker=1, declared_interval_ms=1000, source_count=True).start(t=9.5, observation=10)
    second.traffic(range(10, SHORT + 1), zero={15})
    for sec in range(11, SHORT + 1):
        second.beat(sec)
    second.stop(SHORT)
    first.write()
    second.write()
    journal = tmp_path / "journal_w1.jsonl"
    bridge = j.bridge
    bridge.ingest_spool(first.spool_path, journal, source_id=j.SOURCE_ID, policy=FAST)
    bridge.ingest_spool(second.spool_path, journal, source_id=j.SOURCE_ID, policy=FAST, resume=True)
    events = rc.read_submit_events(first.submits_path, instance=first.instance)[0] + rc.read_submit_events(second.submits_path, instance=second.instance)[0]
    restarted = rc.RecorderVerifiedCoverage.from_recorder_journal(journal, source_id=j.SOURCE_ID, submit_events=events, policy=FAST, run_id=j.RUN)
    steady = k.short_worker(tmp_path, 0, zero={15})
    steady_cov = k.recorder_coverage(steady, tmp_path, journal_name="journal_w0.jsonl")[1]
    multi = wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN), workers=[(0, steady_cov), (1, restarted)])
    snap = multi.snapshot(at(SHORT + 1))
    assert snap.valid
    cls, why = snap.classify(at(8), 8)
    assert cls == C.INTERRUPTED_COLLECTION and why.startswith("worker 1") and "restart_boundary" in why
    assert snap.classify(at(15), 0)[0] == C.VERIFIED_ZERO and wk.RC_WORKER_MISSING not in snap.cause_codes()
    assert "collector_restart_gap" in snap.cause_codes()


# ------------------------------------------------------------------------------------------------------------------- 29 worker identity mismatch
def test_29_a_journal_that_says_it_belongs_to_another_worker_is_refused(tmp_path):
    rec = k.short_worker(tmp_path, 0, zero={5})
    _report, cov, _events = k.recorder_coverage(rec, tmp_path)
    multi = wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN),
                                   workers=[(1, cov)])                                       # worker 0's journal presented as worker 1's
    snap = multi.snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == wk.RC_WORKER_IDENTITY and "worker 1" in snap.failure_reason


def test_29b_a_worker_that_is_not_in_the_roster_is_refused(tmp_path):
    rec = k.short_worker(tmp_path, 5)
    _report, cov, _events = k.recorder_coverage(rec, tmp_path)
    multi = wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN), workers=[(5, cov)])
    snap = multi.snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == wk.RC_WORKER_UNEXPECTED


def test_29c_journals_of_different_runs_are_never_combined(tmp_path):
    recs = {0: k.short_worker(tmp_path, 0, zero={5}), 1: k.short_worker(tmp_path, 1, zero={5})}
    for row in recs[1].rows:
        row["run_id"] = "run-other"
    covs = []
    for index, rec in recs.items():
        rec.write()
        j.bridge.ingest_spool(rec.spool_path, tmp_path / f"jr{index}.jsonl", source_id=j.SOURCE_ID, policy=FAST)
        events = rc.read_submit_events(rec.submits_path, instance=rec.instance)[0]
        covs.append((index, rc.RecorderVerifiedCoverage.from_recorder_journal(tmp_path / f"jr{index}.jsonl", source_id=j.SOURCE_ID, submit_events=events, policy=FAST)))
    snap = wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN), workers=covs).snapshot(at(SHORT + 1))
    assert not snap.valid and snap.failure_code == wk.RC_WORKER_RUN


def test_29d_a_worker_coverage_must_be_a_single_workers_own_recorder_coverage(tmp_path):
    recs, (multi, per) = two_workers(tmp_path)
    with pytest.raises(wk.WorkerContractError) as excinfo:
        wk.MultiWorkerCoverage(source_id=j.SOURCE_ID, roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN), workers=[(0, multi), (1, per[1])])
    assert excinfo.value.code == "worker_coverage_type_invalid"
    with pytest.raises(wk.WorkerContractError) as excinfo2:
        wk.MultiWorkerCoverage(source_id="another-source", roster=wk.WorkerRoster.from_benchmark_config(2, DOMAIN), workers=[(0, per[0])])
    assert excinfo2.value.code == "worker_source_mismatch"


def test_29e_the_multi_worker_class_refuses_to_be_attached_to_anything_but_recorder_derived_arrivals(tmp_path):
    from tests import a0h_support as h
    recs, (multi, _per) = two_workers(tmp_path)
    mismatched = rc.RecorderArrivalSource(source_id="other", utc_day=j.DAY)
    with pytest.raises(Exception):
        rc.RecorderCapturedHistory(rc.bin_submit_events([], mismatched), coverage=multi, policy=FAST)
