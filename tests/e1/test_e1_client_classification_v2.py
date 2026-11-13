"""Fabric-free tests for scripts/e1/e1_client_classification_v2.py (erratum E1-QL-ERR-001). Synthetic inputs only; nothing here is an E1 result.

The module derives a corrected CLIENT_LIMITED / SATURATED classification next to the original one. These tests pin four things:
  1. it contains no number the frozen driver did not already contain (and not the withdrawn 10 ms clause);
  2. it differs from the frozen classify() through clause 1 only (the SATURATED rule and the other CLIENT_LIMITED clauses are the same);
  3. it never reads missing evidence as healthy, and never names a component without a measurement;
  4. the derived table keeps the original labels verbatim, is deterministic and refuses to overwrite.
"""
import hashlib
import itertools
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "e1"))

import e1_client_classification_v2 as cc  # noqa: E402
import e1_quicklook as ql  # noqa: E402

# What the 18 executed P1a runs actually showed (audit values): event-loop p99 11.30-11.57 ms, CPU 6.5-12.1 %, lateness 2.0 ms, A/lambda 1.0004-1.0008.
P1A_LIKE = {"rate": 20.0, "ratio_A_over_lambda": 1.0004, "lateness_p99_ms": 2.0, "event_loop_p99_ms": 11.5, "caliper_cpu_mean_pct": 11.5,
            "failure_rate": 0.0, "max_inflight_backlog": 20, "interior_valid_rate_tps": 20.0}


def v1_labels(m):
    vrate = {"rate": m["interior_valid_rate_tps"]}
    pacing_pass = abs(m["ratio_A_over_lambda"] - 1) <= ql.THR.pacing_ratio_tol and m["lateness_p99_ms"] <= ql.THR.lateness_p99_ms \
        and m["event_loop_p99_ms"] <= ql.THR.event_loop_p99_ms
    return ql.classify(m["rate"], vrate, m["max_inflight_backlog"], m["failure_rate"], m["event_loop_p99_ms"], m["caliper_cpu_mean_pct"], pacing_pass)


# ---------------------------------------------------------------------------------------------- 1. no new numbers
def test_every_threshold_equals_the_frozen_driver_number():
    t, f = cc.THR2, ql.THR
    assert (t.pacing_ratio_tol, t.lateness_p99_ms, t.event_loop_p99_v2_ms) == (f.pacing_ratio_tol, f.lateness_p99_ms, f.event_loop_p99_ms)
    assert (t.client_cpu_pct, t.failure_rate_max, t.sat_valid_ratio, t.sat_backlog) == (f.client_cpu_pct, f.failure_rate_max, f.sat_valid_ratio, f.sat_backlog)


def test_the_withdrawn_clause_is_not_a_threshold_of_the_new_rule():
    assert not any("client_event_loop" in k for k in cc.ThresholdsV2.__dataclass_fields__)
    assert ql.THR.client_event_loop_p99_ms == 10.0          # the frozen number is still in the frozen driver, untouched


# ---------------------------------------------------------------------------------------------- 2. what changes and what does not
def test_the_observed_p1a_numbers_are_client_limited_under_the_frozen_rule_and_healthy_under_v2():
    assert v1_labels(P1A_LIKE) == ["CLIENT_LIMITED"]
    c = cc.classify_v2(P1A_LIKE)
    assert c["labels_v2"] == [] and c["class_v2"] == cc.HEALTHY and c["flags"]["v2_gate_pass"] is True and c["missing_evidence"] == []


@pytest.mark.parametrize("el", [10.01, 11.5, 15.0, 20.0])
def test_an_event_loop_p99_between_10_and_20_ms_no_longer_labels_a_run(el):
    c = cc.classify_v2({**P1A_LIKE, "event_loop_p99_ms": el})
    assert c["labels_v2"] == [] and c["flags"]["el_v2_gate_ok"] is True


def test_an_event_loop_p99_above_20_ms_fails_the_v2_gate_but_is_not_read_as_client_saturation():
    c = cc.classify_v2({**P1A_LIKE, "event_loop_p99_ms": 20.5})
    assert c["flags"]["el_v2_gate_ok"] is False and c["flags"]["v2_gate_pass"] is False
    assert c["labels_v2"] == [] and not cc.ramp_may_continue("INVALID_SCHEDULE", c["class_v2"])      # the verdict stops the ramp, not a label
    assert ql.THR.event_loop_p99_ms == 20.0


@pytest.mark.parametrize("change,expected", [
    ({"caliper_cpu_mean_pct": 80.0}, ["CLIENT_LIMITED"]), ({"caliper_cpu_mean_pct": 79.99}, []),
    ({"lateness_p99_ms": 5.01}, ["CLIENT_LIMITED"]), ({"lateness_p99_ms": 5.0}, []),
    ({"ratio_A_over_lambda": 1.0201}, ["CLIENT_LIMITED"]), ({"ratio_A_over_lambda": 0.9799}, ["CLIENT_LIMITED"]), ({"ratio_A_over_lambda": 1.0199}, []),
    ({"ratio_A_over_lambda": 0.9801}, []),
    ({"interior_valid_rate_tps": 18.99}, ["SATURATED"]), ({"interior_valid_rate_tps": 19.0}, []),
    ({"max_inflight_backlog": 201}, ["SATURATED"]), ({"max_inflight_backlog": 200}, []),
    ({"failure_rate": 0.0101}, ["SATURATED"]), ({"failure_rate": 0.01}, []),
    ({"caliper_cpu_mean_pct": 85.0, "max_inflight_backlog": 300}, ["CLIENT_LIMITED", "SATURATED"]),
])
def test_label_boundaries(change, expected):
    assert cc.classify_v2({**P1A_LIKE, **change})["labels_v2"] == expected


def test_v2_differs_from_the_frozen_rule_only_where_the_event_loop_probe_alone_fired():
    """Brute force over a grid: the SATURATED label is identical, and CLIENT_LIMITED differs only where the event-loop probe alone fired
    (clause 1, p99 > 10 ms, or the event-loop component of the V2 pacing gate, p99 > 20 ms). Pacing and CPU decide the rest, as before."""
    grid = itertools.product([5.0, 10.5, 12.0, 19.9, 20.1, 30.0], [5.0, 79.9, 80.0, 95.0], [1.0, 4.9, 5.1], [0.97, 1.0, 1.019, 1.03],
                             [0.0, 0.02], [10, 200, 201], [20.0, 19.0, 18.9])
    differing = 0
    for el, cpu, late, ratio, fail, backlog, vrate in grid:
        m = {"rate": 20.0, "ratio_A_over_lambda": ratio, "lateness_p99_ms": late, "event_loop_p99_ms": el, "caliper_cpu_mean_pct": cpu,
             "failure_rate": fail, "max_inflight_backlog": backlog, "interior_valid_rate_tps": vrate}
        a, b = v1_labels(m), cc.classify_v2(m)["labels_v2"]
        assert ("SATURATED" in a) == ("SATURATED" in b)
        if ("CLIENT_LIMITED" in a) != ("CLIENT_LIMITED" in b):
            differing += 1
            assert "CLIENT_LIMITED" in a and "CLIENT_LIMITED" not in b
            assert el > 10.0 and cpu < 80.0 and abs(ratio - 1) <= 0.02 and late <= 5.0      # the probe was the only reason
    assert differing > 0


# ---------------------------------------------------------------------------------------------- 3. missing evidence, attribution, ramp rule
def test_missing_evidence_is_never_healthy():
    for k in ("ratio_A_over_lambda", "lateness_p99_ms", "caliper_cpu_mean_pct", "failure_rate", "max_inflight_backlog", "interior_valid_rate_tps"):
        c = cc.classify_v2({**P1A_LIKE, k: None})
        assert c["class_v2"] == cc.UNDETERMINED and c["missing_evidence"], k
    # a violated limit is still reported even when something else is missing
    c = cc.classify_v2({**P1A_LIKE, "failure_rate": None, "max_inflight_backlog": 500})
    assert c["labels_v2"] == ["SATURATED"] and c["missing_evidence"] == ["failures_ok"]


def test_ramp_may_continue_only_from_a_valid_healthy_run():
    assert cc.ramp_may_continue("VALID", cc.HEALTHY) is True
    assert cc.ramp_may_continue("VALID", "SATURATED") is False
    assert cc.ramp_may_continue("VALID", "CLIENT_LIMITED") is False
    assert cc.ramp_may_continue("VALID", "CLIENT_LIMITED+SATURATED") is False
    assert cc.ramp_may_continue("VALID", cc.UNDETERMINED) is False
    assert cc.ramp_may_continue("INVALID_SCHEDULE", cc.HEALTHY) is False
    assert cc.ramp_may_continue("SKIPPED_PRECONDITION", cc.HEALTHY) is False
    assert cc.ramp_may_continue(None, None) is False


def test_attribution_names_a_component_only_on_a_measurement():
    cpu = {"orderer.example.com": 5.0, "peer0.org1.example.com": 91.0, "caliper-benchmark-001": 12.0}
    assert cc.attribute([], cpu)["attribution"] == "NONE"
    a = cc.attribute(["SATURATED"], cpu)
    assert a["attribution"] == "AT_CPU_LIMIT: peer0.org1.example.com" and a["cpu_at_limit_components"] == ["peer0.org1.example.com"]
    assert cc.attribute(["SATURATED"], {**cpu, "peer0.org1.example.com": 40.0})["attribution"].startswith("UNATTRIBUTED")
    assert cc.attribute(["SATURATED"], None)["attribution"].startswith("UNATTRIBUTED")
    assert cc.attribute(["CLIENT_LIMITED"], {**cpu, "caliper-benchmark-001": 83.0})["attribution"].startswith("CLIENT")
    assert cc.attribute(["CLIENT_LIMITED", "SATURATED"], cpu)["attribution"].startswith("AMBIGUOUS")
    # the Caliper container at its line is not a Fabric component, so saturation is not attributed to Fabric because of it
    assert cc.attribute(["SATURATED"], {**cpu, "peer0.org1.example.com": 10.0, "caliper-benchmark-001": 85.0})["attribution"].startswith("UNATTRIBUTED")


# ---------------------------------------------------------------------------------------------- 4. the derived table
def _row(seq, run_id, B=20, rate=10, kind="sweep"):
    return SimpleNamespace(seq=seq, run_id=run_id, channel=f"e1-{run_id}-b{B}-l{rate}", kind=kind, B=B, rate=rate, replicate=1)


def _recomputed(labels, verdict="VALID", **over):
    ev = {"ratio_A_over_lambda": 1.0008, "lateness_p99_ms": 2.0, "event_loop_p99_ms": 11.49, "pass": True}
    met = {"interior_valid_rate_tps": 10.0, "failure_rate": 0.0, "max_inflight_backlog": 20, "caliper_cpu_mean_pct": 7.0, "event_loop_p99_ms": 11.49}
    met.update(over)
    return {"labels": labels, "verdict": verdict, "metrics": met, "gates": [{"id": "V2-pacing", "status": "PASS", "evidence": ev}]}


def test_executed_row_keeps_the_original_label_verbatim_and_adds_the_corrected_one():
    rec = {"verdict": "VALID", "labels": ["CLIENT_LIMITED"]}
    r = cc.build_row(_row(1, "ql01"), rec, None, _recomputed(["CLIENT_LIMITED"]), {"p50_ms": 10.15, "p99_ms": 11.49}, {"caliper-benchmark-001": 7.0})
    assert r["original_labels"] == "CLIENT_LIMITED" and r["original_verdict"] == "VALID"
    assert r["original_labels_reproduced_by_driver_code"] == "true" and r["original_clause1_el_gt_10ms"] == "true"
    assert r["corrected_labels"] == "" and r["corrected_class"] == "HEALTHY" and r["attribution"] == "NONE"
    assert r["ramp_may_continue_original"] == "false" and r["ramp_may_continue_corrected"] == "true"
    assert r["original_el_p99_ms"] == "11.49" and r["el_p99_ms_v2_gate_diagnostic"] == "11.49"


def test_a_recomputation_that_disagrees_with_the_record_is_flagged_not_hidden():
    rec = {"verdict": "VALID", "labels": []}
    r = cc.build_row(_row(1, "ql01"), rec, None, _recomputed(["CLIENT_LIMITED"]), {"p50_ms": 10.1, "p99_ms": 11.0}, None)
    assert r["original_labels_reproduced_by_driver_code"] == "false"


def test_skipped_row_is_not_run_and_carries_the_original_skip_reason():
    r = cc.build_row(_row(18, "ql18", 40, 15, "ramp"), None, {"verdict": "SKIPPED_PRECONDITION", "reason": "ql17 labelled ['CLIENT_LIMITED']"}, None, None, None)
    assert r["status"] == "SKIPPED" and r["corrected_class"] == "NOT_RUN" and "ql17 labelled" in r["note"] and r["original_verdict"] == "SKIPPED_PRECONDITION"
    assert set(r) == set(cc.CSV_COLUMNS)


def test_undetermined_row_when_a_metric_is_missing():
    r = cc.build_row(_row(1, "ql01"), {"verdict": "VALID", "labels": []}, None, _recomputed([], failure_rate=None), {"p50_ms": 10.1, "p99_ms": 11.0}, None)
    assert r["corrected_class"] == "UNDETERMINED" and "failures_ok" in r["note"] and r["ramp_may_continue_corrected"] == "false"


def test_csv_is_deterministic_and_never_overwritten(tmp_path):
    rows = [cc.build_row(_row(1, "ql01"), {"verdict": "VALID", "labels": ["CLIENT_LIMITED"]}, None, _recomputed(["CLIENT_LIMITED"]), {"p50_ms": 10.15, "p99_ms": 11.49}, None)]
    t1, t2 = cc.csv_text(rows), cc.csv_text(rows)
    assert t1 == t2 and t1.splitlines()[0] == ",".join(cc.CSV_COLUMNS) and "\r" not in t1
    p = tmp_path / "d.csv"
    assert cc.write_new(p, t1) == hashlib.sha256(t1.encode()).hexdigest()
    with pytest.raises(FileExistsError):
        cc.write_new(p, t1)
    assert p.read_text(encoding="utf-8") == t1


def test_json_provenance_round_trip_is_plain_json():
    assert json.loads(json.dumps(cc.asdict(cc.THR2)))["client_cpu_pct"] == 80.0
