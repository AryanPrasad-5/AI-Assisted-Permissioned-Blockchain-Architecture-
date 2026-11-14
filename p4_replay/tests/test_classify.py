"""Replay-specific classification: every class is reached by its own named fault, a healthy class needs positive evidence for everything (the ledger holding valid
transactions is never enough), missing evidence is UNDETERMINED, thresholds PROPOSED give only a provisional healthy class, and P1b's constant-rate rule is shown to be
unusable on a bursty schedule. Synthetic data only."""

import numpy as np
import pytest

from p4_replay import classify as cl, fidelity as fd, offsets as of

T0 = {"pre_roll": 1_700_000_000_000, "measured": 1_700_000_100_000}
PRE = [0, 1000, 1000, 30_000, 59_999]
BURST = [20_000 + (i * 19) // 2 for i in range(50)]                                         # 50 arrivals inside second 20: a burst second (>= 40 tx/s)
MEAS = sorted([0, 0, 5, 1000, 1004, 1004, 2500, 5000, 5001, 5002, 9000, 9500, 12_000, 12_000, 30_000, 31_000, 40_000, 100_000, 599_999] + BURST)
LAG = 7


def make_run(lag=LAG, meas=MEAS) -> fd.RunInput:
    rounds, ledger = [], [{"tx_id": "cfg", "validation_code": "0", "header_type": "1"}]
    for idx, (name, offs, length) in enumerate((("pre_roll", PRE, 60), ("measured", meas, 600))):
        keys = [of.tx_key("rp01", idx, i) for i in range(len(offs))]
        dispatch = [{"seq": i, "intended_offset_ms": o, "intended_ms": T0[name] + o, "dispatch_ms": T0[name] + o + lag, "lag_ms": lag} for i, o in enumerate(offs)]
        recorder = [{"tx_id": f"tx{idx}{i:03d}", "create_ms": str(T0[name] + o + lag), "final_ms": str(T0[name] + o + lag + 1500), "status": "success", "verified": "true"}
                    for i, o in enumerate(offs)]
        ledger += [{"tx_id": r["tx_id"], "validation_code": "0", "header_type": "3"} for r in recorder]
        rounds.append(fd.RoundInput(name, length, list(offs), keys, dispatch, 1, recorder))
    return fd.RunInput("rp01", rounds, ledger, True)


def burst_of(run, thr=None):
    m = next(r for r in run.rounds if r.name == "measured")
    return cl.burst_fidelity(m.dispatch, m.scheduled_offsets_ms, m.length_s, (thr or cl.ClassThresholds()).burst_rate_tps)


HEALTHY_CPU = {"available": True, "mean_pct": 12.0, "burst_mean_pct": 25.0}


def classify_run(run, *, thr=None, clock="PASS", wall=(0.2, 0.3), cpu=None, inflight=True):
    thr = thr or cl.ClassThresholds()
    fid = fd.evaluate(run, thr.fidelity)
    m = next(r for r in run.rounds if r.name == "measured")
    return cl.classify(cl.ClassInputs(run.run_id, fid, clock, list(wall), HEALTHY_CPU if cpu is None else cpu, burst_of(run, thr),
                                      cl.inflight_profile(m.recorder) if inflight else {}), thr)


APPROVED = cl.approved_class_thresholds(fd.approved_thresholds({"rel_lag_p99_ms": 20, "rel_lag_max_ms": 100, "missed_schedule_lag_ms": 500, "per_second_count_tv": 0.02,
                                                                 "drain_deadline_s": 10}, "ab" * 32), 0.10)


# ----------------------------------------------------------------------------------------------- healthy needs positive evidence
def test_a_clean_run_is_healthy_only_with_approved_thresholds():
    r = classify_run(make_run(), thr=APPROVED)
    assert r["class"] == cl.HEALTHY and r["labels"] == [] and r["usable_as_fabric_result"] is True and r["missing_evidence"] == []
    p = classify_run(make_run())                                                            # the same run under PROPOSED thresholds
    assert p["class"] == cl.HEALTHY_PROVISIONAL and p["usable_as_fabric_result"] is False and "PROPOSED" in p["label_basis"]


@pytest.mark.parametrize("kwargs,what", [
    ({"clock": None}, "clock series check"),
    ({"wall": (0.2, None)}, "wall-minus-monotonic"),
    ({"cpu": {"available": False}}, "CPU samples over the measured round"),
    ({"cpu": {"available": True, "mean_pct": 10.0, "burst_mean_pct": None}}, "CPU samples inside the burst seconds"),
])
def test_missing_evidence_is_never_read_as_healthy(kwargs, what):
    r = classify_run(make_run(), thr=APPROVED, **kwargs)
    assert r["class"] == cl.UNDETERMINED and any(what in m for m in r["missing_evidence"]) and r["usable_as_fabric_result"] is False


def test_a_missing_dispatch_log_or_recorder_is_undetermined_not_a_limited_class():
    run = make_run()
    run.rounds[1].dispatch = None
    run.rounds[0].recorder = None
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.UNDETERMINED and r["labels"] == [] and len(r["missing_evidence"]) >= 5


def test_valid_transactions_in_the_ledger_do_not_make_a_failed_schedule_healthy():
    run = make_run()
    run.rounds[1].dispatch.pop(5)                                                           # one scheduled transaction was never dispatched, yet the ledger is all VALID
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.SCHEDULER and r["class"] != cl.HEALTHY and "S1" in {c["id"] for c in r["failed"][cl.SCHEDULER]}


# ----------------------------------------------------------------------------------------------- each class by its own fault
def test_submission_failure():
    run = make_run()
    run.rounds[1].recorder[2]["status"] = "failed"
    assert classify_run(run, thr=APPROVED)["class"] == cl.SUBMISSION


def test_missing_and_repeated_transactions_are_submission_failures():
    run = make_run()
    gone = run.rounds[1].recorder.pop(5)
    run.ledger = [x for x in run.ledger if x["tx_id"] != gone["tx_id"]]
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.SUBMISSION and {"S4", "S10"} <= {c["id"] for c in r["failed"][cl.SUBMISSION]}
    dup = make_run()
    dup.rounds[1].recorder.append(dict(dup.rounds[1].recorder[3]))
    assert classify_run(dup, thr=APPROVED)["class"] == cl.SUBMISSION


def test_ledger_reconciliation_mismatch_is_a_submission_failure_with_its_subtype():
    run = make_run()
    run.ledger.append({"tx_id": "stranger", "validation_code": "0", "header_type": "3"})
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.SUBMISSION and "SUBMISSION_FAILURE:RECONCILIATION_MISMATCH" in r["subtypes"]


def test_invalid_transaction():
    run = make_run()
    run.ledger[3]["validation_code"] = "11"
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.INVALID_TX and r["labels"] == [cl.INVALID_TX]


@pytest.mark.parametrize("kwargs,subtype", [({"clock": "STEP_IN_RUN_WINDOW"}, "CLOCK_FAILURE:STEP_IN_RUN_WINDOW"), ({"clock": "COVERAGE_GAP"}, "CLOCK_FAILURE:COVERAGE_GAP"),
                                            ({"clock": "CHECK_ERROR"}, "CLOCK_FAILURE:CHECK_ERROR"), ({"wall": (0.2, 3.0)}, "CLOCK_FAILURE:WALL_MINUS_MONOTONIC")])
def test_clock_failure(kwargs, subtype):
    r = classify_run(make_run(), thr=APPROVED, **kwargs)
    assert r["class"] == cl.CLOCK and subtype in r["subtypes"]


def test_clock_gate_not_passed_is_a_clock_failure():
    run = make_run()
    run.clock_gate_pass = False
    assert classify_run(run, thr=APPROVED)["class"] == cl.CLOCK


def test_scheduler_limited_when_a_dispatch_is_far_late_and_the_client_shows_no_cpu_evidence():
    run = make_run()
    run.rounds[1].dispatch[6]["dispatch_ms"] += 2000                                        # one dispatch two seconds late: a missed schedule
    run.rounds[1].dispatch[6]["lag_ms"] += 2000
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.SCHEDULER and "N3" in {c["id"] for c in r["failed"][cl.SCHEDULER]} and r["attribution"].startswith("UNATTRIBUTED_BY_EXCLUSION")


def test_the_same_timing_failure_with_cpu_at_the_line_is_client_limited():
    run = make_run()
    run.rounds[1].dispatch[6]["dispatch_ms"] += 2000
    run.rounds[1].dispatch[6]["lag_ms"] += 2000
    r = classify_run(run, thr=APPROVED, cpu={"available": True, "mean_pct": 85.0, "burst_mean_pct": 97.0})
    assert r["class"] == cl.CLIENT and cl.SCHEDULER in r["labels"] and r["attribution"].startswith("CLIENT")


def test_client_limited_by_cpu_alone_as_round_mean_or_burst_mean():
    assert classify_run(make_run(), thr=APPROVED, cpu={"available": True, "mean_pct": 80.0, "burst_mean_pct": 30.0})["subtypes"] == ["CLIENT_LIMITED_REPLAY:CPU_ROUND_MEAN"]
    burst_only = classify_run(make_run(), thr=APPROVED, cpu={"available": True, "mean_pct": 20.0, "burst_mean_pct": 91.0})
    assert burst_only["class"] == cl.CLIENT and burst_only["subtypes"] == ["CLIENT_LIMITED_REPLAY:CPU_BURST_MEAN"]       # a burst saturation a round mean would average away


def test_fidelity_limited_when_every_transaction_is_dispatched_but_the_lag_distribution_is_off():
    run = make_run()
    for i, d in enumerate(run.rounds[1].dispatch):
        if i % 4 == 0:
            d["dispatch_ms"] += 150
            d["lag_ms"] += 150                                                              # under the 500 ms missed-schedule line, over the 100 ms maximum
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.FIDELITY and cl.SCHEDULER not in r["labels"] and {"N1", "N2"} <= {c["id"] for c in r["failed"][cl.FIDELITY]}


def test_a_flattened_burst_is_caught_by_the_burst_check_when_every_other_fidelity_check_passes():
    """A realistic-size schedule (about 3,000 background arrivals, so the whole-run distance is diluted as it is on the real 6,229-arrival window). 12 of the 50 arrivals of
    the burst second sit at its very end (offsets 20,985-20,996 ms); dispatching them 15 ms late pushes them into the next second. The lag percentiles, the missed-schedule count and the whole-run
    per-second distance all pass; only the per-burst-second count deviation shows it."""
    background = [s * 1000 + 100 * k for s in range(600) if not 19 <= s <= 21 for k in range(5)]                 # 5 tx/s outside the burst
    meas = sorted(background + [0] + [20_000 + i * 20 for i in range(38)] + [20_985 + i for i in range(12)])
    run = make_run(meas=meas)
    for d in run.rounds[1].dispatch:
        if 20_985 <= d["intended_offset_ms"] < 20_997:
            d["dispatch_ms"] += 15
            d["lag_ms"] += 15
    r = classify_run(run, thr=APPROVED)
    fid = fd.evaluate(run, APPROVED.fidelity)
    assert fid["failed_checks"] == [] and fid["metrics"]["rounds"]["measured"]["per_second"]["tv_distance"] < 0.01    # Phase 1's checks all pass: the flattened burst is invisible to them
    b = burst_of(run, APPROVED)
    assert b["applicable"] and b["max_rel_dev"] == pytest.approx(12 / 50)
    assert r["class"] == cl.FIDELITY and [c["id"] for c in r["failed"][cl.FIDELITY]] == ["N6"]
    assert "FIDELITY_LIMITED_REPLAY:BURST_FLATTENED_OR_SHIFTED" in r["subtypes"]


def test_the_burst_check_is_not_applicable_without_a_burst_second_and_missing_without_a_log():
    quiet = [o for o in MEAS if o not in BURST]
    run = make_run(meas=quiet)
    assert burst_of(run)["applicable"] is False and classify_run(run, thr=APPROVED, cpu={"available": True, "mean_pct": 5.0, "burst_mean_pct": None})["class"] == cl.HEALTHY
    none = make_run()
    none.rounds[1].dispatch = None
    assert cl.burst_fidelity(None, MEAS, 600, 40)["evaluated"] is False


def test_a_drain_deadline_failure_is_a_fabric_overload_indicator_not_a_replay_class():
    run = make_run()
    for r in run.rounds[1].recorder[-3:]:
        r["final_ms"] = str(float(r["final_ms"]) + 60_000)                                  # three transactions committed a minute late
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.HEALTHY and r["fabric_overload_indicators"]["drain_deadline_exceeded"] is True and "N5@measured" in r["informational"]["drain_failures"]


def test_a_preroll_timing_deviation_is_flagged_but_does_not_class_the_run():
    run = make_run()
    run.rounds[0].dispatch[2]["dispatch_ms"] += 400
    run.rounds[0].dispatch[2]["lag_ms"] += 400
    r = classify_run(run, thr=APPROVED)
    assert r["class"] == cl.HEALTHY and any(f.endswith("@pre_roll") for f in r["preroll_timing_flags"])
    broken = make_run()
    broken.rounds[0].recorder[1]["status"] = "failed"                                       # a STRUCTURAL failure in the pre-roll still classes the run
    assert classify_run(broken, thr=APPROVED)["class"] == cl.SUBMISSION


def test_precedence_picks_the_most_invalidating_label_and_keeps_all_of_them():
    run = make_run()
    run.ledger[3]["validation_code"] = "11"
    run.rounds[1].dispatch.pop(5)
    r = classify_run(run, thr=APPROVED, clock="STEP_IN_RUN_WINDOW", cpu={"available": True, "mean_pct": 90.0, "burst_mean_pct": 90.0})
    assert r["class"] == cl.CLOCK and r["labels"][0] == cl.CLOCK and {cl.INVALID_TX, cl.CLIENT, cl.SCHEDULER} <= set(r["labels"])
    assert r["labels"] == [x for x in cl.PRECEDENCE if x in r["labels"]]


def test_an_unmapped_failing_check_can_never_vanish():
    fid = fd.evaluate(make_run(), APPROVED.fidelity)
    fid["checks"].append({"id": "Z9", "round": None, "name": "future check", "kind": "STRUCTURAL", "ok": False, "observed": 1, "expected": 0, "note": ""})
    r = cl.classify(cl.ClassInputs("rp01", fid, "PASS", [0.1, 0.1], HEALTHY_CPU, burst_of(make_run())), APPROVED)
    assert r["class"] == cl.UNDETERMINED and any("Z9" in m for m in r["missing_evidence"])


def test_classification_is_deterministic_and_documents_what_it_cannot_assess():
    a, b = classify_run(make_run(), thr=APPROVED), classify_run(make_run(), thr=APPROVED)
    assert a == b
    assert len(a["cannot_assess"]) >= 8 and any("seq to tx_id join is INFERRED" in c for c in a["cannot_assess"]) and any("10 ms resolution" in c for c in a["cannot_assess"])


# ----------------------------------------------------------------------------------------------- why P1b's constant-rate rule is not reused
def test_p1b_constant_rate_rules_are_wrong_for_a_bursty_schedule():
    """Facts from the frozen P1b code, applied to this schedule: the constant-rate pacing model puts the k-th arrival at k / rate, so on a bursty schedule it reports a large
    'lateness' for a PERFECTLY realised replay; and its backlog limit of 200 was set for constant rates up to 40 tx/s, while the replay's peak second (98 tx/s on F_82800,
    P4 draft) is 2.45 times that rate."""
    import e1_quicklook_p1b as p1b
    offs = np.array(MEAS, dtype=float)
    creates = (offs + 7.0).tolist()                                                         # a perfect replay: every create at its intended offset plus a constant 7 ms
    rate = len(MEAS) / 600.0
    pacing = p1b.v2_pacing(creates, len(MEAS), rate, 600.0, 11.4)
    assert pacing["lateness_p99_ms"] > 1000 and pacing["pass"] is False                     # P1b would call a perfect replay client-limited
    assert 98 > p1b.TIER1_MAX_RATE_TPS and p1b.THR.sat_backlog == 200                        # the replay's peak second lies far outside the rates the P1b classification was approved for
    r = classify_run(make_run(), thr=APPROVED)
    assert r["class"] == cl.HEALTHY                                                         # the replay classification, using the schedule itself, calls it healthy


def test_one_bad_burst_second_among_several_is_not_averaged_away():
    """Three burst seconds; only the middle one is distorted (8 of its 50 arrivals pushed into the next second: 0.16 deviation). The mean over burst seconds is 0.053, under the
    0.10 limit, so only the MAXIMUM deviation catches it."""
    bursts = [s * 1000 + (i * 19) // 2 for s in (20, 30, 40) for i in range(50)]
    background = [s * 1000 + 100 * k for s in range(600) if s not in (20, 30, 40) for k in range(5)]
    meas = sorted(background + bursts)
    run = make_run(meas=meas)
    shifted = [d for d in run.rounds[1].dispatch if 30_000 <= d["intended_offset_ms"] < 31_000][-8:]
    for d in shifted:
        d["dispatch_ms"] += 1000
        d["lag_ms"] += 1000
    b = burst_of(run, APPROVED)
    assert b["burst_seconds"] == 3 and b["max_rel_dev"] == pytest.approx(8 / 50) and b["mean_rel_dev"] < 0.10 < b["max_rel_dev"]
    assert any(c["id"] == "N6" for c in classify_run(run, thr=APPROVED)["failed"][cl.FIDELITY])
