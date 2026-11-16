"""Phase 2C, Package C: clock evidence is computed from the continuous probe record and fails closed. The Phase 2 constant `clock_gate_pass=True` is gone. Synthetic samples only."""

import re
from pathlib import Path

import pytest

from p4_replay import REPO_ROOT, clock_evidence as ce, launch_ids as li
from p4_replay.tests import support_2c as s2

BASE = 1_800_000_000_000_000_000
MS = 10 ** 6
GOOD = dict(series_boot_id="boot-1", current_boot_id="boot-1", preflight_clock_gate_pass=True, probe_alive=True)


def samples(n=121, interval_ms=1000, jump_at=None, jump_ms=0.0, skip=()):
    out = []
    for i in range(n):
        if i in skip:
            continue
        out.append({"i": i, "real_ns": BASE + i * interval_ms * MS + (int(jump_ms * MS) if jump_at is not None and i >= jump_at else 0), "mono_ns": 10 ** 15 + i * interval_ms * MS})
    return out


def window(a=10, b=100, interval_ms=1000):
    return {"run_id": "rp01", "start_ns": BASE + a * interval_ms * MS, "end_ns": BASE + b * interval_ms * MS, "start_utc": "s", "end_utc": "e"}


def test_a_complete_record_passes_and_reports_its_density():
    r = ce.evaluate_clock_evidence(samples(), window(), **GOOD)
    assert r["ok"] is True and r["verdict"] == "PASS" and r["reasons"] == []
    assert r["density"]["ratio"] >= 0.95 and r["density"]["median_interval_ms"] == 1000.0 and r["p1b_series_check"]["verdict"] == "PASS"


@pytest.mark.parametrize("label,kwargs,smp,win,verdict", [
    ("no window", {}, samples(), None, "NO_RUN_WINDOW"),
    ("no samples", {}, [], window(), "NO_DATA"),
    ("one sample", {}, samples(1), window(), "NO_DATA"),
    ("samples None", {}, None, window(), "NO_DATA"),
    ("preflight false", {"preflight_clock_gate_pass": False}, samples(), window(), "PREFLIGHT_CLOCK_NOT_PASS"),
    ("preflight missing", {"preflight_clock_gate_pass": None}, samples(), window(), "PREFLIGHT_CLOCK_NOT_PASS"),
    ("boot changed", {"current_boot_id": "boot-2"}, samples(), window(), "BOOT_CHANGED"),
    ("boot unknown", {"series_boot_id": None, "current_boot_id": None}, samples(), window(), "BOOT_CHANGED"),
    ("probe dead", {"probe_alive": False}, samples(), window(), "PROBE_NOT_ALIVE"),
    ("probe state unknown", {"probe_alive": None}, samples(), window(), "PROBE_NOT_ALIVE"),
    ("a 500 ms step inside the window", {}, samples(jump_at=50, jump_ms=500.0), window(), "STEP_IN_RUN_WINDOW"),
    ("a 5 ms step inside the window (above the 1 ms limit)", {}, samples(jump_at=50, jump_ms=5.0), window(), "STEP_IN_RUN_WINDOW"),
    ("a 20 s sampling hole inside the window", {}, samples(skip=range(40, 60)), window(), "COVERAGE_GAP"),
    ("the record ends before the window does", {}, samples(n=80), window(), "COVERAGE_GAP"),
])
def test_every_missing_or_incomplete_evidence_fails_closed(label, kwargs, smp, win, verdict):
    r = ce.evaluate_clock_evidence(smp, win, **{**GOOD, **kwargs})
    assert r["ok"] is False and r["verdict"] == verdict, (label, r["verdict"], r["reasons"])
    assert r["reasons"], "a failed verdict names its reason"


def test_a_step_before_the_run_window_does_not_fail_that_runs_window():
    """A4 semantics carried from P1b: a step outside the window leaves the window's own offsets constant. It is reported, not silently dropped."""
    r = ce.evaluate_clock_evidence(samples(jump_at=2, jump_ms=500.0), window(), **GOOD)
    assert r["ok"] is True and r["p1b_series_check"]["steps_total"] == 1


def test_enough_coverage_but_too_few_samples_is_insufficient():
    """Coverage passes (every hole under 2 s) yet the window holds about half the samples the record's own interval implies."""
    smp = [{"i": i, "real_ns": BASE + i * 1000 * MS, "mono_ns": 10 ** 15 + i * 1000 * MS} for i in range(60)]
    t = 59 * 1000
    while t < 59 * 1000 + 100_000:
        t += 1900
        smp.append({"i": len(smp), "real_ns": BASE + t * MS, "mono_ns": 10 ** 15 + t * MS})
    win = {"run_id": "rp01", "start_ns": BASE + 61_000 * MS, "end_ns": BASE + 150_000 * MS, "start_utc": "s", "end_utc": "e"}
    r = ce.evaluate_clock_evidence(smp, win, **GOOD)
    assert r["verdict"] == "INSUFFICIENT_SAMPLES" and r["ok"] is False and r["density"]["ratio"] < 0.95


def test_a_non_increasing_monotonic_stamp_in_the_window_is_flagged():
    smp = samples()
    smp.insert(51, dict(smp[50]))                                                            # the same sample twice
    r = ce.evaluate_clock_evidence(smp, window(), **GOOD)
    assert r["verdict"] == "NON_MONOTONIC" and r["ok"] is False


def test_only_pass_is_ok_across_every_verdict():
    seen = set()
    for kw in ({}, {"probe_alive": False}, {"current_boot_id": "x"}, {"preflight_clock_gate_pass": False}):
        r = ce.evaluate_clock_evidence(samples(), window(), **{**GOOD, **kw})
        seen.add((r["verdict"], r["ok"]))
    assert all(ok == (v == "PASS") for v, ok in seen)


# ----------------------------------------------------------------------------------------------- from files
def write_csv(path: Path, smp, header="i,real_ns,mono_ns", extra_tail=""):
    path.write_text(header + "\n" + "".join(f"{x['i']},{x['real_ns']},{x['mono_ns']}\n" for x in smp) + extra_tail, encoding="utf-8")


def test_from_a_file_it_passes_and_counts_a_torn_last_row(tmp_path):
    p = tmp_path / "clock_samples.csv"
    write_csv(p, samples(), extra_tail="121,17999")                                           # a torn last line
    r = ce.evaluate_from_files(p, window(), **GOOD)
    assert r["ok"] is True and r["torn_rows_dropped"] == 1


@pytest.mark.parametrize("make", ["missing", "bad_header", "empty"])
def test_an_unreadable_or_empty_record_is_a_failed_verdict_never_an_exception(tmp_path, make):
    p = tmp_path / "clock_samples.csv"
    if make == "bad_header":
        write_csv(p, samples(), header="a,b,c")
    elif make == "empty":
        p.write_text("i,real_ns,mono_ns\n", encoding="utf-8")
    r = ce.evaluate_from_files(p, window(), **GOOD)
    assert r["ok"] is False and r["verdict"] in ("EVIDENCE_UNREADABLE", "NO_DATA")


# ----------------------------------------------------------------------------------------------- the pre-run value
def test_the_pre_run_value_is_real_evidence():
    smp = samples(60)
    now = smp[-1]["real_ns"] + 3 * 1000 * MS
    ok = ce.pre_run_clock_ok(smp, now_ns=now, preflight_clock_gate_pass=True, probe_alive=True, series_boot_id="b", current_boot_id="b")
    assert ok["ok"] is True and ok["newest_sample_age_ms"] == pytest.approx(3000.0)


@pytest.mark.parametrize("kw,fragment", [
    ({"now_ns": BASE + 59_000 * MS + 60_000 * MS}, "old"),
    ({"preflight_clock_gate_pass": False}, "pre-flight"),
    ({"preflight_clock_gate_pass": None}, "pre-flight"),
    ({"probe_alive": False}, "probe"),
    ({"probe_alive": None}, "probe"),
    ({"current_boot_id": "other"}, "boot"),
    ({"now_ns": BASE}, "future"),
])
def test_the_pre_run_value_fails_on_each_missing_piece(kw, fragment):
    args = dict(now_ns=BASE + 62_000 * MS, preflight_clock_gate_pass=True, probe_alive=True, series_boot_id="b", current_boot_id="b")
    args.update(kw)
    r = ce.pre_run_clock_ok(samples(60), **args)
    assert r["ok"] is False and any(fragment in x for x in r["reasons"])


def test_the_pre_run_value_needs_two_samples():
    for smp in ([], None, samples(1)):
        r = ce.pre_run_clock_ok(smp, now_ns=BASE, preflight_clock_gate_pass=True, probe_alive=True, series_boot_id="b", current_boot_id="b")
        assert r["ok"] is False and any("fewer than 2" in x for x in r["reasons"])


# ----------------------------------------------------------------------------------------------- the vacuous constant is gone, and the evidence drives the class
def literal_true_clock_gates(module: str) -> list[int]:
    """Line numbers where the code (not a docstring or comment) passes or assigns the literal True to clock_gate_pass."""
    import ast
    tree = ast.parse((REPO_ROOT / "p4_replay" / f"{module}.py").read_text(encoding="utf-8"))
    hits = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.keyword) and n.arg == "clock_gate_pass" and isinstance(n.value, ast.Constant) and n.value.value is True]
    hits += [n.lineno for n in ast.walk(tree) if isinstance(n, ast.Assign) and isinstance(n.value, ast.Constant) and n.value.value is True
             and any(isinstance(t, ast.Name) and t.id == "clock_gate_pass" for t in n.targets)]
    return hits


def test_no_phase_2c_module_passes_a_literal_true_clock_gate():
    for name in ("launch_series", "replay_classify", "clock_evidence", "live_entry"):
        assert literal_true_clock_gates(name) == [], name
    # the Phase 2 module keeps its constant (untouched, superseded): the contrast is the point
    assert literal_true_clock_gates("series") != []


def test_a_clock_step_in_the_run_makes_the_run_clock_failure_although_the_preflight_passed(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY, w=s2.world(clock_step_in_run={li.DRY_RUN_ID}))
    summary = L.run()
    run = summary["runs"][0]
    assert run["class"] == "CLOCK_FAILURE" and run["clock_evidence_verdict"] == "STEP_IN_RUN_WINDOW"
    assert summary["status"] == "HALTED" and "CLOCK_FAILURE" in summary["halted"]


def test_a_dead_clock_probe_at_the_end_of_a_run_fails_closed(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY, w=s2.world(probe_dies_in_run={li.DRY_RUN_ID}))
    summary = L.run()
    assert summary["status"] == "HALTED" and summary["runs"][0]["class"] == "CLOCK_FAILURE" and summary["runs"][0]["clock_evidence_verdict"] == "PROBE_NOT_ALIVE"
