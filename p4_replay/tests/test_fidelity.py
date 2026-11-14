"""Fidelity detection: a perfect synthetic run passes PROVISIONALLY; every failure mode is detected by its own named check and is
labelled FIDELITY_LIMITED, never silently accepted. Nothing here is a Fabric result."""

import copy
import csv

import pytest

from p4_replay import fidelity as fd
from p4_replay import offsets as of

T0 = {"pre_roll": 1_700_000_000_000, "measured": 1_700_000_100_000}
PRE = [0, 1000, 1000, 30_000, 59_999]
MEAS = [0, 0, 5, 1000, 1004, 1004, 2500, 5000, 5001, 5002, 9000, 9500, 12_000, 12_000, 20_000, 30_000, 31_000, 40_000, 100_000, 599_999]
LAG = 7                                                  # a constant logger offset of a few ms, as in run 005


def make_run(lag=LAG) -> fd.RunInput:
    rounds, ledger = [], [{"tx_id": "cfg", "validation_code": "0", "header_type": "1"}]
    for idx, (name, offs, length) in enumerate((("pre_roll", PRE, 60), ("measured", MEAS, 600))):
        keys = [of.tx_key("rp01", idx, i) for i in range(len(offs))]
        dispatch = [{"seq": i, "intended_offset_ms": o, "intended_ms": T0[name] + o, "dispatch_ms": T0[name] + o + lag, "lag_ms": lag} for i, o in enumerate(offs)]
        recorder = [{"tx_id": f"tx{idx}{i:03d}", "create_ms": str(T0[name] + o + lag), "final_ms": str(T0[name] + o + lag + 1500), "status": "success", "verified": "true"}
                    for i, o in enumerate(offs)]
        ledger += [{"tx_id": r["tx_id"], "validation_code": "0", "header_type": "3"} for r in recorder]
        rounds.append(fd.RoundInput(name, length, list(offs), keys, dispatch, 1, recorder))
    return fd.RunInput("rp01", rounds, ledger, True)


def failed(result) -> set:
    return set(result["failed_checks"])


def test_a_perfect_run_passes_only_provisionally():
    r = fd.evaluate(make_run())
    assert r["failed_checks"] == [] and r["verdict"] == "FIDELITY_PASS_PROVISIONAL"
    assert r["threshold_status"] == "PROPOSED" and r["usable_as_fabric_result"] is False       # PROPOSED thresholds never give a final pass
    m = r["metrics"]["rounds"]["measured"]
    assert m["lag"]["rel_first_abs_max_ms"] == 0 and m["missed_schedules"] == 0 and m["per_second"]["tv_distance"] == 0
    assert m["order_join"]["kind"] == "INFERRED_ORDER_JOIN" and m["order_join"]["ambiguous_tie_groups"] >= 1   # ties make the seq<->tx_id join ambiguous
    assert r["metrics"]["ledger"]["reconciliation"] == "COMPLETE_AT_TX_ID_LEVEL"
    assert r["metrics"]["ledger"]["key_level_reconciliation"].startswith("NOT_AVAILABLE")
    assert "key-level ledger reconciliation (NOT_AVAILABLE)" in r["not_evaluated_here"]


def test_only_approved_thresholds_give_a_final_pass():
    thr = fd.approved_thresholds({"rel_lag_p99_ms": 20, "rel_lag_max_ms": 100, "missed_schedule_lag_ms": 500, "per_second_count_tv": 0.02, "drain_deadline_s": 10}, "ab" * 32)
    r = fd.evaluate(make_run(), thr)
    assert r["verdict"] == "FIDELITY_PASS" and r["usable_as_fabric_result"] is True and r["thresholds"]["approved_by_prereg_sha256"] == "ab" * 32
    with pytest.raises(ValueError):
        fd.approved_thresholds({"rel_lag_p99_ms": 20}, "ab" * 32)                              # a partial set is refused


def test_constant_logger_offset_is_not_a_fidelity_failure():
    r = fd.evaluate(make_run(lag=8))
    assert r["failed_checks"] == []                                                          # lag relative to the FIRST dispatch (P4 SF-2)


def test_scheduled_versus_dispatched_count_mismatch():
    run = make_run()
    run.rounds[1].dispatch.pop()                                                             # one arrival was never dispatched
    r = fd.evaluate(run)
    assert r["verdict"] == "FIDELITY_LIMITED" and {"S1@measured", "S2@measured", "N3@measured"} <= failed(r)


def test_duplicate_submission_in_dispatch_log_and_in_recorder():
    run = make_run()
    run.rounds[1].dispatch.append(dict(run.rounds[1].dispatch[3]))                           # seq 3 dispatched twice
    run.rounds[1].recorder.append(dict(run.rounds[1].recorder[3]))                           # and its tx_id appears twice
    r = fd.evaluate(run)
    assert r["verdict"] == "FIDELITY_LIMITED" and {"S1@measured", "S2@measured", "S4@measured", "S5@measured"} <= failed(r)


def test_tx_id_repeated_across_rounds_is_a_duplicate_submission():
    run = make_run()
    run.rounds[1].recorder[0]["tx_id"] = run.rounds[0].recorder[0]["tx_id"]
    assert "S5x" in failed(fd.evaluate(run))


def test_missing_transaction_is_found_in_recorder_and_ledger():
    run = make_run()
    gone = run.rounds[1].recorder.pop(5)
    run.ledger = [row for row in run.ledger if row["tx_id"] != gone["tx_id"]]
    r = fd.evaluate(run)
    assert {"S4@measured", "S10"} <= failed(r) and r["verdict"] == "FIDELITY_LIMITED"


def test_unexpected_ledger_transaction_breaks_reconciliation():
    run = make_run()
    run.ledger.append({"tx_id": "stranger", "validation_code": "0", "header_type": "3"})
    r = fd.evaluate(run)
    assert {"S7", "S10"} <= failed(r) and r["metrics"]["ledger"]["reconciliation"] == "INCOMPLETE"


def test_invalid_transaction_is_counted():
    run = make_run()
    run.ledger[3]["validation_code"] = "11"                                                  # MVCC_READ_CONFLICT-style invalidation
    r = fd.evaluate(run)
    assert "S8" in failed(r) and r["metrics"]["ledger"]["invalid"] == 1


def test_submission_failure_is_counted():
    run = make_run()
    run.rounds[1].recorder[2]["status"] = "failed"
    r = fd.evaluate(run)
    assert "S6@measured" in failed(r) and r["metrics"]["rounds"]["measured"]["submission_failures"] == 1


def test_excessive_dispatch_lag_and_missed_schedule():
    run = make_run()
    run.rounds[1].dispatch[12]["dispatch_ms"] += 600                                         # one arrival dispatched 0.6 s late
    run.rounds[1].dispatch[12]["lag_ms"] += 600
    r = fd.evaluate(run)
    assert {"N2@measured", "N3@measured"} <= failed(r) and r["metrics"]["rounds"]["measured"]["missed_schedules"] == 1
    mild = make_run()
    mild.rounds[1].dispatch[12]["dispatch_ms"] += 150
    mild.rounds[1].dispatch[12]["lag_ms"] += 150
    rm = fd.evaluate(mild)
    assert "N2@measured" in failed(rm) and "N3@measured" not in failed(rm)                   # late beyond the max, but not yet "missed"


def test_burst_pushed_into_the_next_second_changes_the_realised_distribution():
    run = make_run()
    for i in (7, 8, 9):                                                                      # the three arrivals in second 5
        run.rounds[1].dispatch[i]["dispatch_ms"] += 1000
        run.rounds[1].dispatch[i]["lag_ms"] += 1000
    r = fd.evaluate(run)
    assert "N4@measured" in failed(r) and r["metrics"]["rounds"]["measured"]["per_second"]["max_abs_second_difference"] >= 2


def test_wrong_schedule_used_is_detected():
    run = make_run()
    run.rounds[1].dispatch[4]["intended_offset_ms"] += 1
    assert "S3@measured" in failed(fd.evaluate(run))


def test_clock_gate_failure_and_missing_clock_evidence():
    run = make_run()
    run.clock_gate_pass = False
    assert "C1" in failed(fd.evaluate(run))
    run.clock_gate_pass = None
    r = fd.evaluate(run)
    assert "C1" in failed(r) and any("EVIDENCE_MISSING" in c["note"] for c in r["checks"] if c["id"] == "C1")


def test_unexpected_backlog_is_detected_by_the_drain_deadline():
    run = make_run()
    last = max(r["dispatch_ms"] for r in run.rounds[1].dispatch)
    run.rounds[1].recorder[-1]["final_ms"] = str(last + 30_000)                              # committed 30 s after the last dispatch
    r = fd.evaluate(run)
    assert "N5@measured" in failed(r) and r["metrics"]["rounds"]["measured"]["drain_after_last_dispatch_s"] == pytest.approx(30.0)


def test_missing_evidence_never_passes():
    run = make_run()
    run.rounds[1].dispatch = None
    run.rounds[0].recorder = None
    run.ledger = None
    r = fd.evaluate(run)
    assert r["verdict"] == "FIDELITY_LIMITED"
    assert {"S1@measured", "N1@measured", "S4@pre_roll", "S7", "S8"} <= failed(r)
    assert all("EVIDENCE_MISSING" in c["note"] for c in r["checks"] if c["ok"] is False and c["id"] in ("S1", "S7"))


def test_every_check_is_either_structural_numeric_or_informational():
    r = fd.evaluate(make_run())
    assert {c["kind"] for c in r["checks"]} <= {"STRUCTURAL", "NUMERIC", "INFORMATIONAL"}
    assert all(c["ok"] is None for c in r["checks"] if c["kind"] == "INFORMATIONAL")
    for c in r["checks"]:
        if c["kind"] == "NUMERIC":
            assert "PROPOSED" in str(c["expected"])                                         # every numeric criterion names its status


def test_short_gap_metrics_are_informational():
    r = fd.evaluate(make_run())
    sg = r["metrics"]["rounds"]["measured"]["short_gaps"]
    assert sg["pairs_with_scheduled_gap_le_5ms"] >= 5 and "realised_gap_max_ms_where_scheduled_le_5ms" in sg


# ----------------------------------------------------------------------------------------------- file readers
def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def test_dispatch_log_reader_rejects_bad_input(tmp_path):
    write_csv(tmp_path / "d.csv", fd.DISPATCH_HEADER, [[0, 1, 2, 3, 1]])
    assert fd.read_dispatch_log(tmp_path / "d.csv")[0]["seq"] == 0
    write_csv(tmp_path / "bad_header.csv", ["seq", "x"], [[0, 1]])
    write_csv(tmp_path / "bad_value.csv", fd.DISPATCH_HEADER, [[0, "a", 2, 3, 1]])
    for name in ("bad_header.csv", "bad_value.csv", "absent.csv"):
        with pytest.raises(fd.EvidenceError):
            fd.read_dispatch_log(tmp_path / name)


def test_run_input_from_files_turns_unreadable_sources_into_failed_checks(tmp_path):
    tx_rows = [{"round": "pre_roll", "seq": str(i), "create_asset_key": of.tx_key("rp01", 0, i)} for i in range(len(PRE))] + \
              [{"round": "measured", "seq": str(i), "create_asset_key": of.tx_key("rp01", 1, i)} for i in range(len(MEAS))]
    run = fd.run_input_from_files("rp01", tx_rows, {"pre_roll": PRE, "measured": MEAS}, dispatch_dir=tmp_path, recorder_dir=tmp_path,
                                  ledger_csv=tmp_path / "ledger_tx.csv", clock_gate_pass=True)
    assert run.rounds[0].dispatch is None and run.ledger is None and len(run.evidence_errors) >= 4
    r = fd.evaluate(run)
    assert r["verdict"] == "FIDELITY_LIMITED" and r["evidence_errors"]
