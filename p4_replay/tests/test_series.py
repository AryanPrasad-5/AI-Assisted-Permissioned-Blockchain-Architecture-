"""Series orchestration against a FAKE backend: the whole planned sequence (preflight, dry run, six seeded measured runs, between-run validation, classification,
stop-on-failure, reconciliation, summary), every mandatory-gate failure, keep-awake cleanup, and the refusals. Nothing live runs; every live step stays LIVE_UNTESTED.
Seeds, hashes and times in these tests are TEST VALUES: the run-order seed is the owner's and none is chosen here."""

import json
import shutil
from pathlib import Path, PurePosixPath

import pytest

from p4_replay import classify as cl, driver as dr, keepawake as ka, preflight as pf, prereg as pr, series as se, series_approval as sa, snapshot as sn
from p4_replay.tests import support as sp
from p4_replay.tests.support import Faults, World

p1b = dr.p1b_module()
ORIGINAL_STEPS, ORIGINAL_EVAL = dr.run_steps, dr.evaluate_run_dir
MEASURED = ["rp01", "rp02", "rp03", "rp04", "rp05", "rp06"]


def run_ids(summary):
    return [r["run_id"] for r in summary["runs"]]


def cmd_text(be):
    return [" ".join(c) for c in be.calls + be.spawns]


def nothing_was_started(parts, summary):
    be = parts["be"]
    assert summary["runs"] == [] and summary.get("started_runs", []) == []
    assert not any("e1_channel_run.py" in c for c in cmd_text(be))                    # no channel was created
    assert not any("e1_clock_rate_probe" in c for c in cmd_text(be))                   # no clock probe was started
    assert not parts["ctx"].root.exists()                                              # no series root


# ----------------------------------------------------------------------------------------------- the full planned sequence
def test_full_series_runs_the_dry_run_then_six_seeded_measured_runs(tmp_path):
    parts = sp.prepare_series(tmp_path)
    summary = sp.run_series(parts)
    assert summary["status"] == "COMPLETE", summary.get("halted") or summary.get("failed_gates")
    assert run_ids(summary) == [r.run_id for r in parts["staged"].rows] and [r["kind"] for r in summary["runs"]] == ["dry_run"] + ["measured"] * 6
    assert all(r["verdict"] == "REPLAY_RECORDED" and r["class"] == cl.HEALTHY and r["validation_ok"] for r in summary["runs"])      # APPROVED thresholds: a final class
    assert summary["reconciliation"]["ok"] and summary["fingerprint_differences"] == [] and summary["clock_series_check"]["verdict"] == "PASS"
    assert summary["claims"]["live_scheduling_fidelity_validated"] is False and summary["live_status"] == "LIVE_UNTESTED"
    root = parts["ctx"].root
    for name in ("session_manifest.json", "series_summary.json", "series_events.log", "fingerprint_compare.json", "keep_awake_cleanup.json"):
        assert (root / name).is_file(), name
    for r in summary["runs"]:
        for name in ("classification.json", "series_verdict.json", "fidelity.json", "run_verdict.json", "facts.json"):
            assert (root / r["run_id"] / name).is_file(), (r["run_id"], name)
        assert json.loads((root / r["run_id"] / "fidelity.json").read_text())["threshold_status"] == "APPROVED"      # the executor used the approved thresholds, not the Phase 1 defaults
    report = json.loads((tmp_path / "preflight" / "preflight_report.json").read_text())
    assert report["all_pass"] and report["evaluated"] == list(pf.GATE_ORDER) and report["snapshot"] == sn.SNAPSHOT_ID


def test_run_order_follows_the_seed_and_cleanup_happens_on_success(tmp_path):
    parts = sp.prepare_series(tmp_path)
    summary = sp.run_series(parts)
    assert [r["B"] for r in summary["runs"][1:]] == [10, 40, 40, 10, 10, 40]               # golden order of TEST seed 7: any change of the generator shows here
    be = parts["be"]
    assert be.proc_keep.returncode == -15 and len(be.taskkills) == 1 and be.taskkills[0][:3] == ["taskkill.exe", "/PID", str(sp.KEEP_PID)]
    assert be.proc_probe.returncode is not None                                           # the continuous clock probe was stopped
    assert dr.run_steps is ORIGINAL_STEPS and dr.evaluate_run_dir is ORIGINAL_EVAL        # the executor overrides were restored


def test_dry_run_only_phase_runs_one_run_with_proposed_thresholds_and_is_provisional(tmp_path):
    parts = sp.prepare_series(tmp_path, phase="DRY_RUN_ONLY", status="PROPOSED")
    summary = sp.run_series(parts)
    assert summary["status"] == "COMPLETE" and run_ids(summary) == ["rp00"]
    assert summary["runs"][0]["class"] == cl.HEALTHY_PROVISIONAL                            # PROPOSED thresholds never give a final healthy class
    assert json.loads((parts["ctx"].root / "rp00" / "fidelity.json").read_text())["threshold_status"] == "PROPOSED"
    assert not any("rp01" in c for c in cmd_text(parts["be"]))


def test_approved_host_guard_and_timeout_reach_the_executor(tmp_path):
    parts = sp.prepare_series(tmp_path, phase="DRY_RUN_ONLY", status="PROPOSED", approval_mutate={"caliper_timeout_s": "1500", "host_max_seconds": "1800"})
    sp.run_series(parts)
    host = next(c for c in parts["be"].spawns if "e1_host_sampler.py" in " ".join(c))
    assert host[host.index("--max-seconds") + 1] == "1800"                                  # the approved guard, not the Phase 1 constant 1,500
    assert parts["session"].thr.caliper_timeout_s == 1500.0


# ----------------------------------------------------------------------------------------------- refusals
def test_no_approval_means_the_owner_decisions_are_unresolved_and_nothing_starts(tmp_path):
    parts = sp.prepare_series(tmp_path)
    parts["approval"] = None
    with pytest.raises(sa.UnresolvedPreregistration, match="D1.*D8"):
        sp.run_series(parts)
    assert parts["be"].calls == [] and parts["be"].spawns == [] and not parts["ctx"].root.exists()


def test_a_real_backend_is_refused_before_any_call_or_file(tmp_path):
    parts = sp.prepare_series(tmp_path)
    real = object.__new__(p1b.Backend)                                                      # never initialised: any use would fail loudly
    parts["session"] = p1b.Session(ctx=parts["ctx"], be=real, thr=dr.replay_thresholds())
    with pytest.raises(dr.LiveExecutionRefused, match="not enabled"):
        sp.run_series(parts)
    assert not parts["ctx"].root.exists() and not (tmp_path / "preflight").exists() and not (tmp_path / "preflight_keep_awake.pid").exists()


def test_a_ctx_with_another_snapshot_is_refused_before_any_call(tmp_path):
    import dataclasses
    parts = sp.prepare_series(tmp_path)
    parts["session"].ctx = dataclasses.replace(parts["ctx"], win_snapshot=r"D:\PLAN_BLOCK18\evidence_archive\ARCHIVE_20261112T213119Z")      # the P1b default snapshot
    with pytest.raises(sn.SnapshotError, match="pinned"):
        sp.run_series(parts)
    assert parts["be"].calls == [] and parts["be"].spawns == []


def test_keep_awake_helper_that_does_not_start_stops_everything(tmp_path):
    parts = sp.prepare_series(tmp_path, World(keep_dies_at_start=True))
    with pytest.raises(ka.KeepAwakeError, match="did not stay up"):
        sp.run_series(parts)
    be = parts["be"]
    assert not any("e1_channel_run.py" in c for c in cmd_text(be)) and not parts["ctx"].root.exists()
    assert not (tmp_path / "preflight").exists()                                            # not even the preflight ran


# ----------------------------------------------------------------------------------------------- every mandatory gate failure prevents execution
GATE_CASES = {
    "S3": lambda w, p: setattr(w, "win_available_mb", 1000.0),
    "S2": lambda w, p: setattr(w, "power_line", "Offline"),
    "S4": lambda w, p: setattr(w, "disk_gb", 5.0),
    "S5": lambda w, p: w.not_running.add("orderer2.example.com"),
    "S5h": lambda w, p: w.health.update({9446: 503}),
    "S6": lambda w, p: w.raft_active.update({"orderer3.example.com": 2}),
    "S8": lambda w, p: setattr(w, "existing_channels", ("e1-r002", "e1-cfg001", p["staged"].rows[0].channel)),
    "R4h": lambda w, p: w.processes.append((500, 1, "python3 -B e1_quicklook_p1b.py run --execute")),
    "R4c": lambda w, p: w.caliper_top.append("77 node caliper launch manager"),
    "R4o": lambda w, p: w.other_containers.append("caliper-other"),
    "S1": lambda w, p: setattr(w, "s1_text", "SOURCE_ADDED repo_p4_replay/x.py\nverify: checked=100 mismatched=0 missing=0 unlisted=0 source_drift=1\n"),
    "S1c": lambda w, p: setattr(w, "s1_text", "SOURCE_CHANGED repo_scripts/e1/e1_quicklook_p1b.py\nverify: checked=100 mismatched=0 missing=0 unlisted=0 source_drift=1\n"),
    "S1m": lambda w, p: setattr(w, "s1_text", "MISMATCH data/x\nverify: checked=100 mismatched=1 missing=0 unlisted=0 source_drift=0\n"),
}
GATE_OF = {"S5h": "S5", "R4h": "R4", "R4c": "R4", "R4o": "R4", "S1c": "S1", "S1m": "S1"}


@pytest.mark.parametrize("case", sorted(GATE_CASES))
def test_a_failed_mandatory_gate_prevents_execution(tmp_path, case):
    w = World()
    parts = sp.prepare_series(tmp_path, w)
    GATE_CASES[case](w, parts)
    summary = sp.run_series(parts)
    gate = GATE_OF.get(case, case)
    assert summary["status"] == "PREFLIGHT_FAILED" and gate in summary["failed_gates"], summary
    nothing_was_started(parts, summary)
    assert parts["be"].taskkills and parts["be"].proc_keep.returncode == -15               # the keep-awake helper was cleaned up on this failure path too
    report = json.loads((tmp_path / "preflight" / "preflight_report.json").read_text())
    assert report["all_pass"] is False and len(report["gates"]) == len(pf.GATE_ORDER)       # every gate was still evaluated and recorded


def test_clock_gate_failure_prevents_execution(tmp_path):
    parts = sp.prepare_series(tmp_path)
    parts["clock_dir"] = sp.write_clock_dir(tmp_path / "bad", verdict="FAIL")
    summary = sp.run_series(parts)
    assert summary["status"] == "PREFLIGHT_FAILED" and "S9" in summary["failed_gates"]
    nothing_was_started(parts, summary)


def test_missing_clock_evidence_prevents_execution(tmp_path):
    parts = sp.prepare_series(tmp_path)
    summary = sp.run_series(parts, clock_dir=None)
    assert summary["status"] == "PREFLIGHT_FAILED" and "S9" in summary["failed_gates"]


def test_snapshot_identity_mismatch_prevents_execution(tmp_path):
    parts = sp.prepare_series(tmp_path)
    summary = sp.run_series(parts, snapshot_path=tmp_path / "ARCHIVE_NOT_THE_SNAPSHOT")
    assert summary["status"] == "PREFLIGHT_FAILED" and "R1" in summary["failed_gates"]
    nothing_was_started(parts, summary)


def test_tampered_staged_schedule_prevents_execution(tmp_path):
    parts = sp.prepare_series(tmp_path)
    target = parts["staged"].stage_dir / "runs" / "rp03" / "schedule_measured.txt"
    target.write_bytes(target.read_bytes() + b"\n999999")
    summary = sp.run_series(parts)
    assert summary["status"] == "PREFLIGHT_FAILED" and "R3" in summary["failed_gates"]
    nothing_was_started(parts, summary)


def _tamper_listed_non_schedule_file(stage):
    f = stage / "plans" / "rp04.plan.json"
    f.write_bytes(f.read_bytes() + b" ")


def _add_unlisted_file(stage):
    (stage / "stray_note.txt").write_text("not part of the signed stage", encoding="utf-8")


@pytest.mark.parametrize("tamper", [_tamper_listed_non_schedule_file, _add_unlisted_file])
def test_a_staged_tree_that_no_longer_matches_its_hash_list_prevents_execution(tmp_path, tamper):
    parts = sp.prepare_series(tmp_path)
    tamper(parts["staged"].stage_dir)
    summary = sp.run_series(parts)
    assert summary["status"] == "PREFLIGHT_FAILED" and "R3" in summary["failed_gates"]
    report = json.loads((tmp_path / "preflight" / "preflight_report.json").read_text())
    r3 = next(g for g in report["gates"] if "R3" in json.dumps(g)[:80])
    assert "stage verify" in r3["detail"]                                                   # the tree re-hash itself caught it, not only the schedule checks
    nothing_was_started(parts, summary)


def test_invalid_configuration_prevents_execution(tmp_path):
    parts = sp.prepare_series(tmp_path)
    parts["env_doc"]["chaincode"] = {"name": "other", "version": "1.0", "sequence": 1}
    summary = sp.run_series(parts)
    assert summary["status"] == "PREFLIGHT_FAILED" and "R2" in summary["failed_gates"]
    nothing_was_started(parts, summary)


def test_series_root_inside_the_wsl_e1_snapshot_group_is_refused(tmp_path):
    parts = sp.prepare_series(tmp_path)
    summary = sp.run_series(parts, series_root_prefix=str(parts["ctx"].root.parent).replace("\\", "/") + "/elsewhere")
    assert summary["status"] == "PREFLIGHT_FAILED" and "R2" in summary["failed_gates"]


def test_an_existing_series_root_or_run_folder_is_never_reused(tmp_path):
    parts = sp.prepare_series(tmp_path)
    parts["ctx"].root.mkdir(parents=True)
    summary = sp.run_series(parts)
    assert summary["status"] == "PREFLIGHT_FAILED" and {"S11", "R5"} <= set(summary["failed_gates"])
    parts2 = sp.prepare_series(tmp_path / "second")
    (parts2["ctx"].caliper_ws / "runs" / "rp02").mkdir(parents=True)
    summary2 = sp.run_series(parts2)
    assert summary2["status"] == "PREFLIGHT_FAILED" and "S11" in summary2["failed_gates"]


def test_a_changed_frozen_p1b_file_prevents_execution(tmp_path, monkeypatch):
    parts = sp.prepare_series(tmp_path)
    monkeypatch.setitem(pf.FROZEN_P1B_SHA256, "scripts/e1/e1_host_sampler.py", "0" * 64)
    summary = sp.run_series(parts)
    assert summary["status"] == "PREFLIGHT_FAILED" and "S10" in summary["failed_gates"]


def test_a_gate_that_raises_fails_closed(tmp_path, monkeypatch):
    parts = sp.prepare_series(tmp_path)
    monkeypatch.setattr(pf, "gate_r4_no_conflicting_process", lambda be, pid: (_ for _ in ()).throw(RuntimeError("boom")))
    summary = sp.run_series(parts)
    assert summary["status"] == "PREFLIGHT_FAILED" and "R4" in summary["failed_gates"]


# ----------------------------------------------------------------------------------------------- stop-on-failure and between-run validation
def test_a_limited_measured_run_halts_the_series_by_default(tmp_path):
    lag = lambda idx, i, off: 150 if (idx == 1 and i % 3 == 0) else 0                      # noqa: E731  (a dispatch-timing deviation in the measured round)
    parts = sp.prepare_series(tmp_path, World(faults={"rp02": Faults(extra_lag_ms=lag)}))
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and "SC7" in summary["halted"]
    assert run_ids(summary) == ["rp00", "rp01", "rp02"] and summary["runs"][2]["class"] in cl.CONTINUABLE
    assert not any("e1-rp03" in c for c in cmd_text(parts["be"]))                           # the next run was never started


def test_the_owner_can_pre_approve_continuing_past_a_limited_run(tmp_path):
    lag = lambda idx, i, off: 150 if (idx == 1 and i % 3 == 0) else 0                      # noqa: E731
    parts = sp.prepare_series(tmp_path, World(faults={"rp02": Faults(extra_lag_ms=lag)}), approval_mutate={"continue_after_limited_measured": "continue"})
    summary = sp.run_series(parts)
    assert summary["status"] == "COMPLETE_WITH_LIMITED_RUNS" and len(summary["runs"]) == 7 and summary["limited_runs"] == ["rp02"]


def test_a_limited_dry_run_halts_before_any_measured_run(tmp_path):
    lag = lambda idx, i, off: 150 if (idx == 1 and i % 3 == 0) else 0                      # noqa: E731
    parts = sp.prepare_series(tmp_path, World(faults={"rp00": Faults(extra_lag_ms=lag)}), approval_mutate={"continue_after_limited_measured": "continue"})
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and "SC3" in summary["halted"] and run_ids(summary) == ["rp00"]      # continuing is never allowed past the dry run
    assert not any("e1-rp01" in c for c in cmd_text(parts["be"]))


@pytest.mark.parametrize("fault,expected", [
    (Faults(fail_submission_seq=3), cl.SUBMISSION),
    (Faults(invalid_seq=4), cl.INVALID_TX),
    (Faults(drop_recorder_row=True), cl.SUBMISSION),
    (Faults(wall_minus_mono=5.0), cl.CLOCK),
])
def test_hard_failures_halt_even_when_continuing_was_pre_approved(tmp_path, fault, expected):
    parts = sp.prepare_series(tmp_path, World(faults={"rp02": fault}), approval_mutate={"continue_after_limited_measured": "continue"})
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and run_ids(summary) == ["rp00", "rp01", "rp02"]
    assert summary["runs"][2]["class"] == expected and "SC6" in summary["halted"]


def test_client_cpu_at_the_line_in_a_burst_is_client_limited(tmp_path):
    parts = sp.prepare_series(tmp_path, World(faults={"rp01": Faults(cpu_burst=95.0)}))
    summary = sp.run_series(parts)
    assert summary["runs"][1]["class"] == cl.CLIENT and "CLIENT_LIMITED_REPLAY:CPU_BURST_MEAN" in summary["runs"][1]["subtypes"] and summary["status"] == "HALTED"


def test_a_clock_step_inside_a_run_window_is_a_clock_failure_and_halts(tmp_path):
    parts = sp.prepare_series(tmp_path, World(clock_step_in_run={"rp03"}), approval_mutate={"continue_after_limited_measured": "continue"})
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and run_ids(summary)[-1] == "rp03"
    last = summary["runs"][-1]
    assert last["class"] == cl.CLOCK and last["clock_series_verdict"] == "STEP_IN_RUN_WINDOW" and "CLOCK_FAILURE:STEP_IN_RUN_WINDOW" in last["subtypes"]
    assert all(r["class"] == cl.HEALTHY for r in summary["runs"][:-1])                      # the earlier runs are judged on their own windows


def test_a_clock_probe_sampling_hole_inside_a_run_window_is_a_clock_failure(tmp_path):
    parts = sp.prepare_series(tmp_path, World(clock_gap_in_run={"rp01"}))
    summary = sp.run_series(parts)
    assert summary["runs"][1]["class"] == cl.CLOCK and summary["runs"][1]["clock_series_verdict"] == "COVERAGE_GAP" and summary["status"] == "HALTED"


def test_a_configuration_that_changed_during_a_run_makes_it_invalid_and_halts(tmp_path):
    parts = sp.prepare_series(tmp_path, World(config_drift_runs={"rp02"}))
    summary = sp.run_series(parts)
    last = summary["runs"][-1]
    assert last["run_id"] == "rp02" and last["verdict"] == "INVALID_INFRA" and last["verdict_executor"] == "REPLAY_RECORDED" and summary["status"] == "HALTED"


def test_a_caliper_abort_halts_with_no_classification(tmp_path):
    parts = sp.prepare_series(tmp_path, World(caliper_abort_runs={"rp01"}))
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and summary["runs"][-1]["verdict"] == "ABORTED_INFRA" and summary["runs"][-1]["class"] is None
    assert not (parts["ctx"].rd("rp01") / "classification.json").exists()                  # a run that did not finish is not classified


def test_two_consecutive_invalid_runs_halt_and_one_does_not(tmp_path):
    one = sp.prepare_series(tmp_path / "one", World(fail_commands={("rp02", "e1_block_fetch.py")}))
    s1 = sp.run_series(one)
    assert s1["runs"][2]["verdict"] == "INVALID_DATA" and len(s1["runs"]) == 7              # one INVALID_DATA run does not stop the series
    assert s1["status"] == "COMPLETE_WITH_INVALID_RUNS" and s1["invalid_runs"] == ["rp02"]
    two = sp.prepare_series(tmp_path / "two", World(fail_commands={("rp02", "e1_block_fetch.py"), ("rp03", "e1_block_fetch.py")}))
    s2 = sp.run_series(two)
    assert s2["status"] == "HALTED" and "SC5" in s2["halted"] and run_ids(s2)[-1] == "rp03"


def test_a_container_restart_between_runs_halts_before_the_next_run(tmp_path):
    parts = sp.prepare_series(tmp_path, World(restart_after_calls=5))
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and "pre-run check failed" in summary["halted"] and "restarted" in " ".join(
        l for l in (parts["ctx"].root / "series_events.log").read_text().splitlines() if "FAIL" in l)
    assert run_ids(summary) == ["rp00", "rp01", "rp02"] and not parts["ctx"].rd("rp03").exists()


def test_the_keep_awake_helper_dying_mid_series_halts_before_the_next_run(tmp_path):
    parts = sp.prepare_series(tmp_path, World(keep_dies_in_run={"rp01"}))
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and "keep-awake helper alive" in summary["halted"] and run_ids(summary) == ["rp00", "rp01"]
    assert len(parts["be"].taskkills) == 1                                                  # cleanup still ran


def test_a_clock_probe_that_exits_at_once_halts_before_any_run(tmp_path):
    parts = sp.prepare_series(tmp_path, World(probe_dies_at_start=True))
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and "exited at once" in summary["halted"] and summary["runs"] == []
    assert not any("e1-rp00" in c for c in cmd_text(parts["be"]))


def test_the_clock_probe_dying_mid_series_halts_before_the_next_run(tmp_path):
    parts = sp.prepare_series(tmp_path, World(probe_dies_in_run={"rp01"}))
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and "continuous clock probe running" in summary["halted"] and run_ids(summary) == ["rp00", "rp01"]
    assert not parts["ctx"].rd("rp02").exists()                                             # no run started without a live clock record


def test_a_reference_channel_that_changed_marks_the_series_failed_integrity(tmp_path):
    parts = sp.prepare_series(tmp_path, World(fingerprint_changes_after=2))
    summary = sp.run_series(parts)
    assert summary["status"] == "FAILED_INTEGRITY" and any("e1-r002.height" in d for d in summary["fingerprint_differences"])


# ----------------------------------------------------------------------------------------------- the stop policy as a table
@pytest.mark.parametrize("verdict,cls,dry,consec,policy,halt,why", [
    ("ABORTED_INFRA", None, False, 0, False, True, "SC4"),
    ("INVALID_INFRA", None, False, 0, False, True, "SC4"),
    ("DRIVER_ERROR", None, False, 0, False, True, "SC4"),
    ("INVALID_DATA", None, True, 0, False, True, "SC3"),
    ("INVALID_DATA", None, False, 0, False, False, "not yet"),
    ("INVALID_DATA", None, False, 1, False, True, "SC5"),
    ("REPLAY_RECORDED", cl.HEALTHY, True, 0, False, False, "dry run healthy"),
    ("REPLAY_RECORDED", cl.HEALTHY_PROVISIONAL, True, 0, False, False, "dry run healthy"),
    ("REPLAY_RECORDED", cl.FIDELITY, True, 0, True, True, "SC3"),
    ("REPLAY_RECORDED", cl.UNDETERMINED, True, 0, True, True, "SC3"),
    ("REPLAY_RECORDED", cl.HEALTHY, False, 0, False, False, "healthy"),
    ("REPLAY_RECORDED", cl.FIDELITY, False, 0, False, True, "SC7"),
    ("REPLAY_RECORDED", cl.FIDELITY, False, 0, True, False, "pre-approved"),
    ("REPLAY_RECORDED", cl.SCHEDULER, False, 0, True, False, "pre-approved"),
    ("REPLAY_RECORDED", cl.CLIENT, False, 0, True, False, "pre-approved"),
    ("REPLAY_RECORDED", cl.CLOCK, False, 0, True, True, "SC6"),
    ("REPLAY_RECORDED", cl.SUBMISSION, False, 0, True, True, "SC6"),
    ("REPLAY_RECORDED", cl.INVALID_TX, False, 0, True, True, "SC6"),
    ("REPLAY_RECORDED", cl.UNDETERMINED, False, 0, True, True, "SC6"),
    ("REPLAY_RECORDED", None, False, 0, False, True, "unexpected"),
    ("SOMETHING_NEW", None, False, 0, True, True, "unexpected"),
])
def test_stop_policy_table(tmp_path, verdict, cls, dry, consec, policy, halt, why):
    staged = sp.build_stage(tmp_path)
    appr = sp.approval_for(staged, mutate={"continue_after_limited_measured": "continue" if policy else "halt"})
    got_halt, reason, _ = se.decide_after_run(verdict=verdict, cls=cls, is_dry_run=dry, consecutive_invalid=consec, approval=appr)
    assert got_halt is halt and why in reason


# ----------------------------------------------------------------------------------------------- determinism, reconciliation, summary
def test_run_ordering_is_deterministic_bounded_and_depends_on_the_seed():
    a, b = dr.build_matrix(7), dr.build_matrix(7)
    assert a == b and dr.matrix_csv(a) == dr.matrix_csv(b)
    orders = {tuple(r.B for r in dr.build_matrix(s)[1:]) for s in range(30)}
    assert len(orders) > 2                                                                  # the seed changes the order
    for order in orders:
        assert sorted(order) == [10, 10, 10, 40, 40, 40]
        assert all(len(set(order[i:i + 3])) > 1 for i in range(4))                          # never three of one configuration in a row
    assert [r.kind for r in dr.build_matrix(30)] == ["dry_run"] + ["measured"] * 6          # the dry run is first and never randomised


def test_two_identical_series_give_identical_outcomes(tmp_path):
    outcomes = []
    for name in ("a", "b"):
        parts = sp.prepare_series(tmp_path / name)
        summary = sp.run_series(parts)
        outcomes.append([{k: v for k, v in r.items() if k not in ("p95_ms_descriptive",)} for r in summary["runs"]] + [summary["status"], summary["reconciliation"]["totals"]])
    assert outcomes[0] == outcomes[1]
    classes = [json.loads((tmp_path / n / "series_root" / "S" / "rp03" / "classification.json").read_text()) for n in ("a", "b")]
    assert classes[0] == classes[1]                                                         # the classification record is byte-for-byte reproducible from the same files


def test_final_reconciliation_flags_a_missing_ledger_transaction_and_a_cross_run_repeat(tmp_path):
    parts = sp.prepare_series(tmp_path)
    summary = sp.run_series(parts)
    assert summary["reconciliation"]["ok"] and summary["reconciliation"]["totals"]["scheduled"] == summary["reconciliation"]["totals"]["ledger_endorser"]
    rows, ctx, manifest = [r for r in parts["staged"].rows if r.run_id in ("rp01", "rp02")], parts["ctx"], parts["staged"].manifest
    ledger = ctx.rd("rp01") / "ledger" / "ledger" / "ledger_tx.csv"
    original = ledger.read_text()
    ledger.write_text("\n".join(original.splitlines()[:-1]) + "\n")                         # the last endorser transaction vanished from the ledger list
    bad = se.final_reconciliation(ctx, rows, manifest)
    assert not bad["ok"] and not bad["per_run"]["rp01"]["ok"] and bad["per_run"]["rp02"]["ok"]
    ledger.write_text(original)
    rec2 = se.recorder_csv(ctx.recorder_dir("rp02"), "rp02", "measured")
    rec1 = se.recorder_csv(ctx.recorder_dir("rp01"), "rp01", "measured")
    lines = rec2.read_text().splitlines()
    first = rec1.read_text().splitlines()[1].split(",")[0]
    lines[1] = first + "," + lines[1].split(",", 1)[1]                                       # rp02 reuses a tx_id of rp01
    rec2.write_text("\n".join(lines) + "\n")
    assert se.final_reconciliation(ctx, rows, manifest)["cross_run_repeated_tx_id"] >= 1


def test_summary_reports_a_descriptive_p95_and_makes_no_law_or_superiority_claim(tmp_path):
    parts = sp.prepare_series(tmp_path)
    summary = sp.run_series(parts)
    assert all(r["p95_ms_descriptive"] == pytest.approx(1500.0) for r in summary["runs"])  # the fake records 1,500 ms for every transaction
    text = json.dumps(summary)
    assert summary["claims"]["law_predictions_evaluated"] is False and "n = 3" in summary["claims"]["note"]
    assert "LP1" not in text and "LP4" not in text                                           # LP evaluation is not implemented; LP4 is not part of the A-only experiment


def test_listing_of_decisions_and_the_run_command_are_offline(capsys):
    assert se.main(["readiness"]) == 3                                                       # no approval: owner decisions unresolved
    out = capsys.readouterr().out
    assert "UNRESOLVED" in out and "D1" in out and "D8" in out
    with pytest.raises(dr.LiveExecutionRefused, match="closed"):
        se.main(["run"])


def test_a_subset_of_gates_can_never_start_a_series(tmp_path):
    parts = sp.prepare_series(tmp_path)
    results, report = pf.run_replay_preflight(parts["ctx"], parts["be"], parts["staged"].rows, tmp_path / "pf_subset", approval=parts["approval"], stage_dir=parts["staged"].stage_dir,
                                              clock_dir=parts["clock_dir"], helper_pid=None, env_doc=parts["env_doc"], own_pid=300, gates={"S3", "S4"})
    assert [g.status for g in results] == ["PASS", "PASS"] and report["all_pass"] is False and report["evaluated"] == ["S3", "S4"]


def test_staged_schedules_that_differ_from_the_approved_hash_prevent_execution(tmp_path):
    import dataclasses
    parts = sp.prepare_series(tmp_path)
    parts["approval"] = dataclasses.replace(parts["approval"], stage_sums_sha256="0" * 64)      # the owner approved other schedule files than the staged ones
    summary = sp.run_series(parts)
    assert summary["status"] == "PREFLIGHT_FAILED" and "R3" in summary["failed_gates"]
    nothing_was_started(parts, summary)


# ----------------------------------------------------------------------------------------------- the interlock, the S1 allowance binding, fail-closed evaluation
class DuckBackend:
    """Does what a backend does, inherits from nothing and does not declare OFFLINE_FAKE: Phase 1's type-based guard alone would let it through."""
    def __getattr__(self, name):
        if name == "OFFLINE_FAKE":
            raise AttributeError(name)                                                       # it does not declare the marker
        raise AssertionError(f"a call to {name} reached an undeclared backend")


def test_a_backend_that_does_not_declare_itself_offline_is_refused_everywhere(tmp_path):
    from p4_replay import guard
    parts = sp.prepare_series(tmp_path)
    parts["session"] = p1b.Session(ctx=parts["ctx"], be=DuckBackend(), thr=dr.replay_thresholds())
    with pytest.raises(dr.LiveExecutionRefused, match="OFFLINE_FAKE"):
        sp.run_series(parts)
    with pytest.raises(dr.LiveExecutionRefused, match="OFFLINE_FAKE"):
        pf.run_replay_preflight(parts["ctx"], DuckBackend(), parts["staged"].rows, tmp_path / "pf_duck", approval=parts["approval"], stage_dir=parts["staged"].stage_dir,
                                clock_dir=None, helper_pid=None, env_doc=None, own_pid=1)
    with pytest.raises(dr.LiveExecutionRefused, match="OFFLINE_FAKE"):
        with ka.KeepAwake(DuckBackend(), 60, tmp_path / "k.pid", str(tmp_path / "k.pid")):
            pass
    assert not parts["ctx"].root.exists() and not (tmp_path / "pf_duck").exists()
    guard.require_offline_backend(sp.FakeSeriesBackend())                                  # a declared fake passes


def signed_list(rel="repo_p4_replay/PHASE2.md"):
    target = sn.repo_path_of(rel)
    digest = __import__("hashlib").sha256(target.read_bytes()).hexdigest()
    return sn.signed_additions_text({rel: digest}).encode("utf-8")


def test_the_s1_allowance_runs_through_the_series_only_when_it_is_what_the_owner_signed(tmp_path):
    import hashlib
    body = signed_list()
    evidence = hashlib.sha256(body).hexdigest()
    w = World(s1_text="SOURCE_ADDED repo_p4_replay/PHASE2.md\nverify: checked=100 mismatched=0 missing=0 unlisted=0 source_drift=1\n")
    ok = sp.prepare_series(tmp_path / "ok", w, phase="DRY_RUN_ONLY", status="PROPOSED", approval_mutate={"s1_evidence_sha256": evidence})
    summary = sp.run_series(ok, s1_allowed_text=body)
    assert summary["status"] == "COMPLETE"                                                  # the added file is covered by the signed list and still has its hash
    strict = sp.prepare_series(tmp_path / "strict", w, phase="DRY_RUN_ONLY", status="PROPOSED", approval_mutate={"s1_evidence_sha256": evidence})
    assert sp.run_series(strict)["status"] == "PREFLIGHT_FAILED"                            # no allowance supplied: the same drift fails S1
    for name, text, mutate in (("wrong_hash", body, {"s1_evidence_sha256": "00" * 32}), ("edited_list", body + b"# edited\n", {"s1_evidence_sha256": evidence}),
                               ("wrong_route", body, {"s1_evidence_sha256": evidence, "s1_route": "THIRD_SNAPSHOT"})):
        parts = sp.prepare_series(tmp_path / name, w, phase="DRY_RUN_ONLY", status="PROPOSED", approval_mutate=mutate)
        with pytest.raises(sn.SnapshotError):
            sp.run_series(parts, s1_allowed_text=text)
        assert parts["be"].calls == [] and parts["be"].spawns == []                         # refused before anything started


def test_an_allowed_file_that_changed_after_signing_fails_s1_in_the_series(tmp_path):
    import hashlib
    rel = "repo_p4_replay/PHASE2.md"
    stale = sn.signed_additions_text({rel: "ab" * 32}).encode("utf-8")                       # signed for other content than the file now has
    w = World(s1_text=f"SOURCE_ADDED {rel}\nverify: checked=100 mismatched=0 missing=0 unlisted=0 source_drift=1\n")
    parts = sp.prepare_series(tmp_path, w, phase="DRY_RUN_ONLY", status="PROPOSED", approval_mutate={"s1_evidence_sha256": hashlib.sha256(stale).hexdigest()})
    summary = sp.run_series(parts, s1_allowed_text=stale)
    assert summary["status"] == "PREFLIGHT_FAILED" and "S1" in summary["failed_gates"]


def test_a_crash_while_judging_a_run_is_a_driver_error_and_halts(tmp_path, monkeypatch):
    parts = sp.prepare_series(tmp_path)
    monkeypatch.setattr(cl, "classify_run_dir", lambda *a, **k: (_ for _ in ()).throw(ValueError("cannot read the evidence")))
    summary = sp.run_series(parts)
    assert summary["status"] == "HALTED" and run_ids(summary) == ["rp00"]
    e = summary["runs"][0]
    assert e["verdict"] == "DRIVER_ERROR" and e["class"] is None and "cannot read the evidence" in e["error"] and "Traceback" in e["traceback"] and e["verdict_executor"] == "REPLAY_RECORDED"
    assert dr.run_steps is ORIGINAL_STEPS                                                    # and the overrides were still restored


def test_no_p1b_gate_threshold_is_weakened_by_the_series(tmp_path):
    import dataclasses
    parts = sp.prepare_series(tmp_path, phase="DRY_RUN_ONLY", status="PROPOSED")
    sp.run_series(parts)
    mine, frozen = dataclasses.asdict(parts["session"].thr), dataclasses.asdict(p1b.THR)
    assert {k for k in mine if mine[k] != frozen[k]} == {"caliper_timeout_s"}               # the run-length limit only; the owner's memory, power, health and disk numbers are P1b's
    report = json.loads((tmp_path / "preflight" / "preflight_report.json").read_text())
    assert report["thresholds_p1b_unchanged"] == frozen and frozen["win_avail_mb_start"] == 1500.0 and frozen["win_commit_pct_start"] == 85.0


def test_a_matrix_outside_the_approved_scope_is_refused(tmp_path):
    import dataclasses
    parts = sp.prepare_series(tmp_path)
    rows = list(parts["staged"].rows)
    rows[3] = dataclasses.replace(rows[3], B=45)                                             # B outside {10, 40}
    summary = se.run_replay_series(parts["session"], rows, parts["staged"].stage_dir, parts["approval"], own_pid=300, clock_dir=parts["clock_dir"], env_doc=parts["env_doc"],
                                   preflight_dir=parts["preflight_dir"], helpers=parts["helpers"], node=False, series_root_prefix=str(parts["ctx"].root.parent).replace("\\", "/"))
    assert summary["status"] == "PREFLIGHT_FAILED" and "R2" in summary["failed_gates"]
    nothing_was_started(parts, summary)
