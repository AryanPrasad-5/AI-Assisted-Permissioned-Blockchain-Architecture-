"""Phase 2C, Package C: median-referenced dispatch metrics and the 2C classification. Every class is reached by its own named fault; N1/N3/N4 gate, N5 is an overload indicator,
N2 and N6 are diagnostics that never label a run; missing evidence is never healthy; the first-dispatch reference problem of Phase 2 is shown and corrected; the median's own blind
spot is shown and not hidden. Synthetic data only."""

import numpy as np
import pytest

from p4_replay import classify as c1, fidelity as fd, offsets as of, replay_classify as rc, replay_fidelity as rf

T0 = {"pre_roll": 1_700_000_000_000, "measured": 1_700_000_100_000}
PRE = [0, 1000, 1000, 30_000, 59_999]
BURST = [20_000 + (i * 19) // 2 for i in range(50)]
MEAS = sorted([0, 0, 5, 1000, 1004, 1004, 2500, 5000, 5001, 5002, 9000, 9500, 12_000, 12_000, 30_000, 31_000, 40_000, 100_000, 599_999] + BURST)
LONG = list(range(0, 600_000, 600))                                                          # 1,000 dispatches: enough for a 99th percentile to ignore one
LAG = 7
CLEAN_CLOCK = {"ok": True, "verdict": "PASS", "reasons": []}
HEALTHY_CPU = {"available": True, "mean_pct": 12.0, "burst_mean_pct": 25.0}
APPROVED = rc.approved_replay_thresholds(rc.ReplayThresholds(), "ab" * 32)
PROPOSED = rc.ReplayThresholds()


def make_run(lag_of=lambda name, i: LAG, meas=MEAS) -> fd.RunInput:
    rounds, ledger = [], [{"tx_id": "cfg", "validation_code": "0", "header_type": "1"}]
    for idx, (name, offs, length) in enumerate((("pre_roll", PRE, 60), ("measured", meas, 600))):
        keys = [of.tx_key("rp01", idx, i) for i in range(len(offs))]
        lags = [lag_of(name, i) for i in range(len(offs))]
        dispatch = [{"seq": i, "intended_offset_ms": o, "intended_ms": T0[name] + o, "dispatch_ms": T0[name] + o + lags[i], "lag_ms": lags[i]} for i, o in enumerate(offs)]
        recorder = [{"tx_id": f"tx{idx}{i:04d}", "create_ms": str(T0[name] + o + lags[i]), "final_ms": str(T0[name] + o + lags[i] + 1500), "status": "success", "verified": "true"}
                    for i, o in enumerate(offs)]
        ledger += [{"tx_id": r["tx_id"], "validation_code": "0", "header_type": "3"} for r in recorder]
        rounds.append(fd.RoundInput(name, length, list(offs), keys, dispatch, 1, recorder))
    return fd.RunInput("rp01", rounds, ledger, True)


def classify2(run, thr=APPROVED, *, clock=CLEAN_CLOCK, wall=(0.2, 0.3), cpu=None):
    m = next(r for r in run.rounds if r.name == "measured")
    d = m.dispatch
    ok_seq = bool(d) and len({r["seq"] for r in d}) == len(d)
    inp = rc.ReplayInputs(run.run_id, fd.evaluate(run, thr.fidelity_thresholds()), clock, rf.lag_metrics(d) if ok_seq else {}, rf.missed_schedules(d, len(m.scheduled_offsets_ms), thr.n3_missed_ms) if ok_seq else {},
                          rf.count_tv(d, m.scheduled_offsets_ms, m.length_s) if ok_seq else {}, rf.burst_deviation(d, m.scheduled_offsets_ms, m.length_s, thr.burst_rate_tps) if d else None,
                          rf.drift(d) if d else {}, list(wall), HEALTHY_CPU if cpu is None else cpu, c1.inflight_profile(m.recorder), None, dispatch_missing=d is None)
    return rc.classify_replay(inp, thr)


def classify_phase2(run, *, clock="PASS"):
    """The Phase 2 classifier (first-dispatch reference) on the same run, for the contrast."""
    thr = c1.ClassThresholds()
    m = next(r for r in run.rounds if r.name == "measured")
    return c1.classify(c1.ClassInputs(run.run_id, fd.evaluate(run, thr.fidelity), clock, [0.2, 0.3], HEALTHY_CPU, c1.burst_fidelity(m.dispatch, m.scheduled_offsets_ms, m.length_s, thr.burst_rate_tps),
                                      c1.inflight_profile(m.recorder)), thr)


# ----------------------------------------------------------------------------------------------- the reference
def test_a_slow_first_dispatch_makes_phase_2_label_a_healthy_run_and_the_median_does_not():
    slow_first = make_run(lambda name, i: LAG + 150 if (name == "measured" and i == 0) else LAG, meas=LONG)
    assert classify_phase2(slow_first)["class"] == c1.FIDELITY, "the Phase 2 first-dispatch reference mislabels it"
    r = classify2(slow_first)
    assert r["class"] == rc.HEALTHY and r["labels"] == []
    ref = r["diagnostics"]["reference"]
    assert ref["median_lag_ms"] == LAG and ref["first_lag_ms"] == LAG + 150 and ref["reference_gap_ms"] == 150
    assert r["diagnostics"]["vs_first_dispatch_phase1_reference"]["abs_p99_ms"] > 100, "the first-dispatch figures stay visible beside the median ones"


def test_the_median_reference_values():
    m = next(r for r in make_run().rounds if r.name == "measured")
    lag = rf.lag_metrics(m.dispatch)
    assert lag["abs_p99_ms"] == 0 and lag["abs_max_ms"] == 0 and lag["median_inside_carried_offset_band"] is True and lag["reference"] == "median"


def test_a_300_ms_delay_after_the_first_dispatch_covering_the_round_is_NOT_caught_by_the_gates_and_is_visible_in_the_diagnostics():
    """THE LIMIT OF THE MEDIAN, kept in a test so it is not forgotten. Every dispatch except the first is 300 ms late (above N1's 20 ms and N2's 100 ms, below N3's 500 ms): that
    lateness is the reference, N1 (a 99th percentile) sees one deviant dispatch in 1,000, N3 tolerates 300 ms, so the run is HEALTHY. Phase 2's first-dispatch reference flags it, by
    the luck that the first dispatch precedes the delay. The diagnostics carry the evidence."""
    late = make_run(lambda name, i: LAG + 300 if (name == "measured" and i > 0) else LAG, meas=LONG)
    r = classify2(late)
    assert r["class"] == rc.HEALTHY, "the gates cannot see it"
    assert classify_phase2(late)["class"] in (c1.FIDELITY, c1.SCHEDULER)
    assert r["diagnostics"]["vs_first_dispatch_phase1_reference"]["abs_p99_ms"] >= 299
    ref = r["diagnostics"]["reference"]
    assert ref["median_inside_carried_offset_band"] is False and ref["median_lag_ms"] == LAG + 300
    assert r["diagnostics"]["N2_max_abs_lag_deviation_ms"]["exceeds_reference"] is True


def test_a_delay_over_500_ms_that_leaves_one_dispatch_unaffected_is_caught_by_n3():
    late = make_run(lambda name, i: LAG + 600 if (name == "measured" and i > 0) else LAG, meas=LONG)
    r = classify2(late)
    assert r["class"] == rc.SCHEDULER and "N3" in {c["id"] for c in r["failed"][rc.SCHEDULER]}


def test_a_delay_present_from_the_first_dispatch_is_invisible_to_every_relative_reference_and_only_the_median_band_shows_it():
    late = make_run(lambda name, i: LAG + 300 if name == "measured" else LAG, meas=LONG)
    assert classify2(late)["class"] == rc.HEALTHY and classify_phase2(late)["class"] in (c1.HEALTHY, c1.HEALTHY_PROVISIONAL)
    assert classify2(late)["diagnostics"]["reference"]["median_inside_carried_offset_band"] is False


def test_a_delay_shared_by_between_1_and_99_percent_of_dispatches_is_flagged_on_either_side_of_the_median():
    for frac, name in ((0.30, "minority late"), (0.70, "majority late")):
        n = len(LONG)
        run = make_run(lambda nm, i, f=frac: LAG + 600 if (nm == "measured" and i >= n * (1 - f)) else LAG, meas=LONG)
        r = classify2(run)
        assert r["class"] in (rc.FIDELITY, rc.SCHEDULER) and "N1" in {c["id"] for c in r["failed"].get(rc.FIDELITY, [])}, name


def test_a_slow_drift_is_reported_by_the_head_and_tail_figures():
    run = make_run(lambda name, i: LAG + (i * 0.2 if name == "measured" else 0) // 1, meas=LONG)
    d = rf.drift(next(r for r in run.rounds if r.name == "measured").dispatch)
    assert d["available"] and d["tail_minus_head_ms"] > 150


def test_count_distance_is_anchored_on_the_median_not_the_first_dispatch():
    m = next(r for r in make_run(lambda name, i: LAG + 150 if (name == "measured" and i == 0) else LAG).rounds if r.name == "measured")
    first_anchor = fd.realised_count_tv(m.dispatch, m.scheduled_offsets_ms, m.length_s)["tv_distance"]
    median_anchor = rf.count_tv(m.dispatch, m.scheduled_offsets_ms, m.length_s)["tv_distance"]
    assert median_anchor == 0.0 and first_anchor > median_anchor


def test_n3_is_two_sided_and_counts_undispatched_entries():
    early = make_run(lambda name, i: LAG - 700 if (name == "measured" and i in range(0, 8)) else LAG, meas=LONG)
    m = next(r for r in early.rounds if r.name == "measured")
    assert rf.missed_schedules(m.dispatch, len(m.scheduled_offsets_ms), 500.0)["over_limit"] == 8
    lost = make_run()
    next(r for r in lost.rounds if r.name == "measured").dispatch.pop(5)
    r = classify2(lost)
    assert r["class"] == rc.SCHEDULER and r["subtypes"].count("SCHEDULER_LIMITED_REPLAY:UNDISPATCHED") == 1


# ----------------------------------------------------------------------------------------------- gating, indicator, diagnostics
def test_a_clean_run_is_healthy_only_with_approved_thresholds():
    r = classify2(make_run())
    assert r["class"] == rc.HEALTHY and r["usable_as_fabric_result"] is True and r["missing_evidence"] == []
    p = classify2(make_run(), PROPOSED)
    assert p["class"] == rc.HEALTHY_PROVISIONAL and p["usable_as_fabric_result"] is False and "PROPOSED" in p["label_basis"]


def test_n1_gates_with_the_fidelity_class():
    run = make_run(lambda name, i: LAG + (40 if (name == "measured" and i % 7 == 0) else 0), meas=LONG)          # 14 % of dispatches 40 ms off the median
    r = classify2(run)
    assert r["class"] == rc.FIDELITY and {"N1"} <= {c["id"] for c in r["failed"][rc.FIDELITY]}


def test_n4_gates_with_the_fidelity_class():
    shifted = make_run(lambda name, i: LAG + (1100 if (name == "measured" and i % 2 == 0) else 0), meas=LONG)     # half the dispatches a second late: count distance far above 0.02
    r = classify2(shifted, rc.ReplayThresholds(n1_lag_p99_ms=1e9, n3_missed_ms=1e9))
    assert r["class"] == rc.FIDELITY and [c["id"] for c in r["failed"][rc.FIDELITY]] == ["N4"]


def test_n5_is_an_overload_indicator_and_never_a_class():
    run = make_run()
    for r_ in next(x for x in run.rounds if x.name == "measured").recorder:
        r_["final_ms"] = str(int(r_["final_ms"]) + 30_000)                                   # commits 30 s late: well past the 10 s drain deadline
    r = classify2(run)
    assert r["class"] == rc.HEALTHY and r["fabric_overload_indicators"]["drain_deadline_exceeded"] is True and "N5@measured" in r["informational"]["drain_failures"]
    assert r["labels"] == [] and "N5" not in str(r["failed"])


def test_n2_and_n6_are_diagnostics_that_never_label_a_run():
    """One dispatch 300 ms late exceeds the N2 reference (100 ms); it is a single entry, so N1 (p99), N3 (500 ms) and N4 stay clean. Flattening a burst second is N6."""
    one = make_run(lambda name, i: LAG + (300 if (name == "measured" and i == 700) else 0), meas=LONG)
    r = classify2(one)
    assert r["class"] == rc.HEALTHY and r["labels"] == []
    assert r["diagnostics"]["N2_max_abs_lag_deviation_ms"]["exceeds_reference"] is True and r["diagnostics"]["N2_max_abs_lag_deviation_ms"]["gates"] is False
    longb = sorted(LONG + BURST)                                                                  # 1,050 dispatches, 50 of them one burst second: the burst is 5 % of the round
    flat = make_run(lambda name, i: LAG + (1000 if (name == "measured" and BURST[0] <= longb[i] <= BURST[-1]) else 0), meas=longb)      # the whole burst moved a second later
    f = classify2(flat, rc.ReplayThresholds(n1_lag_p99_ms=1e9, n3_missed_ms=1e9, n4_count_tv=1.0, status="APPROVED"))
    assert f["class"] == rc.HEALTHY and f["diagnostics"]["N6_burst_second_count_deviation"]["exceeds_reference"] is True and f["diagnostics"]["N6_burst_second_count_deviation"]["gates"] is False


def test_the_gating_and_diagnostic_sets_are_exactly_the_owner_recommendation():
    assert rc.GATING == {"N1": rc.FIDELITY, "N3": rc.SCHEDULER, "N4": rc.FIDELITY} and rc.DIAGNOSTIC_ONLY == ("N2", "N6") and rc.INDICATOR_ONLY == ("N5",)
    t = rc.ReplayThresholds()
    assert (t.n1_lag_p99_ms, t.n3_missed_ms, t.n4_count_tv, t.n5_drain_s, t.n2_lag_max_ms, t.n6_burst_dev) == (20.0, 500.0, 0.02, 10.0, 100.0, 0.10)
    assert t.as_dict()["gating"] == {"N1_lag_p99_ms": 20.0, "N3_missed_ms": 500.0, "N4_count_tv": 0.02}


# ----------------------------------------------------------------------------------------------- every class by its own fault
def test_submission_failure_invalid_transaction_and_reconciliation():
    a = make_run()
    next(x for x in a.rounds if x.name == "measured").recorder[2]["status"] = "failed"
    assert classify2(a)["class"] == rc.SUBMISSION
    b = make_run()
    b.ledger[3]["validation_code"] = "11"
    assert classify2(b)["class"] == rc.INVALID_TX
    c = make_run()
    c.ledger.append({"tx_id": "stranger", "validation_code": "0", "header_type": "3"})
    r = classify2(c)
    assert r["class"] == rc.SUBMISSION and "SUBMISSION_FAILURE:RECONCILIATION_MISMATCH" in r["subtypes"]


def test_client_limited_by_round_mean_and_by_burst_mean():
    assert classify2(make_run(), cpu={"available": True, "mean_pct": 85.0, "burst_mean_pct": 25.0})["class"] == rc.CLIENT
    r = classify2(make_run(), cpu={"available": True, "mean_pct": 30.0, "burst_mean_pct": 91.0})
    assert r["class"] == rc.CLIENT and "CLIENT_LIMITED_REPLAY:CPU_BURST_MEAN" in r["subtypes"]


def test_precedence_clock_over_submission_over_client():
    run = make_run()
    next(x for x in run.rounds if x.name == "measured").recorder[2]["status"] = "failed"
    r = classify2(run, clock={"ok": False, "verdict": "STEP_IN_RUN_WINDOW", "reasons": ["x"]}, cpu={"available": True, "mean_pct": 99.0, "burst_mean_pct": 99.0})
    assert r["class"] == rc.CLOCK and r["labels"][:3] == [rc.CLOCK, rc.SUBMISSION, rc.CLIENT]


@pytest.mark.parametrize("clock", [{}, None, {"ok": False, "verdict": "COVERAGE_GAP", "reasons": ["hole"]}, {"verdict": "PASS"}])
def test_clock_evidence_missing_or_not_ok_is_clock_failure_never_healthy(clock):
    r = classify2(make_run(), clock=clock)
    assert r["class"] == rc.CLOCK and r["usable_as_fabric_result"] is False


def test_wall_minus_monotonic_beyond_two_ms_is_clock_failure():
    r = classify2(make_run(), wall=(0.2, 3.0))
    assert r["class"] == rc.CLOCK and "CLOCK_FAILURE:WALL_MINUS_MONOTONIC" in r["subtypes"]


@pytest.mark.parametrize("kwargs,what", [({"wall": (0.2, None)}, "wall-minus-monotonic"), ({"cpu": {"available": False}}, "CPU samples over the measured round"),
                                         ({"cpu": {"available": True, "mean_pct": 10.0, "burst_mean_pct": None}}, "CPU samples inside the burst seconds")])
def test_missing_non_clock_evidence_is_undetermined(kwargs, what):
    r = classify2(make_run(), **kwargs)
    assert r["class"] == rc.UNDETERMINED and any(what in m for m in r["missing_evidence"])


def test_a_missing_dispatch_log_is_undetermined_not_a_limited_class():
    run = make_run()
    next(x for x in run.rounds if x.name == "measured").dispatch = None
    r = classify2(run)
    assert r["class"] == rc.UNDETERMINED and r["labels"] == [] and any("dispatch log" in m for m in r["missing_evidence"])


def test_valid_transactions_in_the_ledger_never_make_a_failed_schedule_healthy():
    run = make_run()
    next(x for x in run.rounds if x.name == "measured").dispatch.pop(5)
    assert classify2(run)["class"] != rc.HEALTHY


def test_all_eight_classes_are_the_owner_listed_strings():
    assert {rc.CLOCK, rc.SUBMISSION, rc.INVALID_TX, rc.CLIENT, rc.SCHEDULER, rc.FIDELITY, rc.UNDETERMINED, rc.HEALTHY} == {
        "CLOCK_FAILURE", "SUBMISSION_FAILURE", "INVALID_TRANSACTION", "CLIENT_LIMITED_REPLAY", "SCHEDULER_LIMITED_REPLAY", "FIDELITY_LIMITED_REPLAY", "UNDETERMINED_REPLAY", "HEALTHY_REPLAY"}
    assert rc.HEALTHY_PROVISIONAL == "HEALTHY_REPLAY_PROVISIONAL" and rc.CONTINUABLE == {rc.CLIENT, rc.SCHEDULER, rc.FIDELITY}


def test_the_cannot_assess_list_states_the_medians_limit_and_no_longer_lists_the_first_dispatch_reference():
    text = " ".join(rc.CANNOT_ASSESS)
    assert "MEDIAN reference cannot expose" in text and "99 %" in text and "reference for relative dispatch lag is the first dispatch" not in text
    assert "no empirical basis" in text
