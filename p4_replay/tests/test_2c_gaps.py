"""Phase 2C: tests added because the mutation check found defects the first suite let through (survivors F07, F08, R15, K14, A04, L04, L07, S02, S06, S07, S08). Each test names the
mutant it exists for. S09 (a subset of gates can pass) is not killed: the preflight loop always evaluates every gate, so that clause is a second line of defence that no input reaches."""

import dataclasses
import json
import shutil

import numpy as np
import pytest

from p4_replay import analysis_primary as an, driver as dr, keepawake_v2 as k2, launch_ids as li, launch_series as ls, replay_classify as rc, replay_fidelity as rf
from p4_replay.tests import support_2c as s2
from p4_replay.tests.test_2c_analysis import write_run
from p4_replay.tests.test_2c_replay_class import classify2, make_run

p1b = dr.p1b_module()


# ----------------------------------------------------------------------------------------------- F07, F08: exact values
def disp(offsets, lags):
    return [{"seq": i, "intended_offset_ms": o, "intended_ms": 10_000 + o, "dispatch_ms": 10_000 + o + lags[i], "lag_ms": lags[i]} for i, o in enumerate(offsets)]


def test_f08_the_count_distance_is_half_the_summed_difference_over_n():
    offs = [0, 500, 1000, 1500]
    d = disp(offs, [7, 607, 7, 7])                                                            # the second dispatch moves from second 0 into second 1
    tv = rf.count_tv(d, offs, 2)
    assert tv["tv_distance"] == pytest.approx(0.25) and tv["seconds_with_difference"] == 2 and tv["max_abs_second_difference"] == 1


def test_f07_the_burst_deviation_reports_the_maximum_and_the_mean_separately():
    offs = [0, 100, 200, 300, 1000, 1100, 1200, 1300, 2000, 2100, 2200, 2300]
    d = disp(offs, [7, 7, 2007, 2007] + [7] * 8)                                              # two dispatches of second 0 land in second 2
    b = rf.burst_deviation(d, offs, 3, 2)
    assert b["burst_seconds"] == 3 and b["max_rel_dev"] == pytest.approx(0.5) and b["mean_rel_dev"] == pytest.approx(1 / 3)


# ----------------------------------------------------------------------------------------------- R15
@pytest.mark.parametrize("clock", [{}, None])
def test_r15_an_unevaluated_clock_has_its_own_subtype(clock):
    r = classify2(make_run(), clock=clock)
    assert r["class"] == rc.CLOCK and "CLOCK_FAILURE:CLOCK_EVIDENCE_NOT_EVALUATED" in r["subtypes"]


# ----------------------------------------------------------------------------------------------- K14
def test_k14_every_cleanup_outcome_is_a_declared_one_and_ok_means_declared_ok(tmp_path):
    seen = {}
    scenarios = {"already_exited": {}, "terminated": {"survives_terminate": True}, "reused": {"pid_reused_after": True}, "query_fails": {"query_fails_after": True, "survives_terminate": True},
                 "kill_fails": {"survives_terminate": True, "taskkill_fails": True}}
    for name, knobs in scenarios.items():
        be = s2.FakeLaunchBackend(s2.world())
        pid = tmp_path / f"{name}.pid"
        k = k2.KeepAwakeV2(be, 600, pid, str(pid), backend_check=lambda b: None, wait_s=3.0)
        be.survives_terminate = knobs.get("survives_terminate", False)
        be.taskkill_fails = knobs.get("taskkill_fails", False)
        with k:
            if knobs.get("pid_reused_after"):
                be.keep_alive, be.pid_reused = False, True
            if knobs.get("query_fails_after"):
                be.query_fails = True
        seen[name] = k.cleanup_record
    outcomes = {r["outcome"] for r in seen.values()}
    assert outcomes <= set(k2.OK_OUTCOMES) | set(k2.FAILED_OUTCOMES), outcomes - (set(k2.OK_OUTCOMES) | set(k2.FAILED_OUTCOMES))
    assert not (set(k2.OK_OUTCOMES) & set(k2.FAILED_OUTCOMES))
    assert all(r["ok"] == (r["outcome"] in k2.OK_OUTCOMES) and (r["ok"] or r["outcome"] in k2.FAILED_OUTCOMES) for r in seen.values()) and len(outcomes) >= 4


# ----------------------------------------------------------------------------------------------- A04
def test_a04_a_run_with_failed_transactions_is_excluded_even_if_its_class_says_healthy(tmp_path):
    p_ok, h_ok = write_run(tmp_path, "ok", [100.0] * 30)
    p_bad, h_bad = write_run(tmp_path, "bad", [100.0] * 30, failed=2)
    runs = [an.RunInput("rp01", 10, 1, 1, "HEALTHY_REPLAY", p_ok, h_ok), an.RunInput("rp02", 40, 1, 2, "HEALTHY_REPLAY", p_bad, h_bad)]
    m = an.analyze(runs)["measured_outcomes"]
    bad = next(r for r in m["runs"] if r["run_id"] == "rp02")
    assert bad["usable_for_contrast"] is False and "2 failed transactions" in bad["excluded_because"] and bad["n_failed"] == 2 and m["contrast_status"] == "NOT_COMPUTABLE"


# ----------------------------------------------------------------------------------------------- L04, L07
def test_l04_a_channel_that_already_exists_is_refused_even_if_it_has_the_right_shape(monkeypatch):
    monkeypatch.setattr(p1b, "EXISTING_CHANNELS", frozenset(set(p1b.EXISTING_CHANNELS) | {li.dry_rows()[0].channel}))
    with pytest.raises(li.LaunchIdError, match="not the fresh channel"):
        li.validate_launch_rows(li.DRY, li.dry_rows())


def test_l07_each_arm_needs_replicates_one_two_three():
    rows = li.measured_rows(7)
    b10 = [i for i, r in enumerate(rows) if r.B == 10]
    rows[b10[2]] = dataclasses.replace(rows[b10[2]], replicate=7, channel=f"e1-{rows[b10[2]].run_id}-b10-r7")
    with pytest.raises(li.LaunchIdError, match="needs replicates 1, 2, 3"):
        li.validate_launch_rows(li.MEASURED, rows)


# ----------------------------------------------------------------------------------------------- S02
def test_s02_an_exact_backend_that_declares_the_fake_marker_is_refused_in_live_mode(monkeypatch):
    be = object.__new__(p1b.Backend)
    be.OFFLINE_FAKE = True                                                                     # type is exactly p1b.Backend, only the declared marker says fake
    monkeypatch.setattr(dr, "LIVE_EXECUTION_ENABLED", True)
    with pytest.raises(dr.LiveExecutionRefused, match="fake backend"):
        ls.require_backend_for_mode(be, ls.MODE_LIVE)


# ----------------------------------------------------------------------------------------------- S06, S07, S08: gate R6 reads the dry-run summary's content and hash
def dry_done(tmp_path):
    D = s2.prepare_launch(tmp_path, li.DRY)
    D.run()
    return D, D.ctx.root / "series_summary.json"


def measured_against(tmp_path, D, summary_for_authorization, summary_given):
    M = s2.prepare_launch(tmp_path, li.MEASURED, w=s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0")), dry_summary=summary_for_authorization,
                          snap_id="ARCHIVE_20261121T090000Z", snap_finished="2026-11-21T09:00:00.000000Z", tag="_m")
    M.dry_summary_path = summary_given
    return M


def tampered_copy(tmp_path, summary, name, mutate):
    doc = json.loads(summary.read_text())
    mutate(doc)
    p = tmp_path / name
    p.write_text(json.dumps(doc), encoding="utf-8")
    return p


def test_s06_a_dry_summary_that_does_not_hash_to_the_authorized_one_fails_r6_alone(tmp_path):
    D, summary = dry_done(tmp_path)
    other = tmp_path / "same_content_other_bytes.json"
    other.write_text(summary.read_text() + "\n\n", encoding="utf-8")                           # valid content, different bytes
    out = measured_against(tmp_path, D, summary, other).run()
    assert out["status"] == "PREFLIGHT_FAILED" and out["failed_gates"] == ["R6"]


@pytest.mark.parametrize("name,mutate", [("class", lambda d: d["runs"][0].update({"class": "FIDELITY_LIMITED_REPLAY"})), ("status", lambda d: d.update({"status": "HALTED"})),
                                         ("halted", lambda d: d.update({"halted": "something"})), ("launch kind", lambda d: d.update({"launch_kind": "MEASURED_SERIES"})),
                                         ("run id", lambda d: d["runs"][0].update({"run_id": "rp91"})), ("cleanup", lambda d: d["keep_awake_cleanup"].update({"ok": False})),
                                         ("two runs", lambda d: d["runs"].append(dict(d["runs"][0])))])
def test_s07_s08_a_dry_summary_whose_content_is_not_a_completed_healthy_dry_run_fails_r6_even_with_the_right_hash(tmp_path, name, mutate):
    D, summary = dry_done(tmp_path)
    bad = tampered_copy(tmp_path, summary, f"bad_{name.replace(' ', '_')}.json", mutate)
    out = measured_against(tmp_path, D, bad, bad).run()
    assert out["status"] == "PREFLIGHT_FAILED" and out["failed_gates"] == ["R6"], (name, out.get("failed_gates"))


# ----------------------------------------------------------------------------------------------- S18
def test_s18_a_probe_that_is_alive_but_writes_no_samples_stops_the_launch_before_the_run(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    L.be.gap_until = 10 ** 9                                                                   # the fake probe process stays up but never writes a sample
    out = L.run()
    assert out["status"] == "HALTED" and "pre-run clock evidence failed" in out["halted"] and out["runs"] == []
    assert not any("e1-rp90" in c for c in s2.cmd_text(L.be)), "the run was never started"


# ----------------------------------------------------------------------------------------------- S24, S25, S27, S28, S32 (found in the second mutation pass)
def test_s24_a_staged_matrix_that_is_not_this_launchs_matrix_fails_r3(tmp_path):
    """A complete, correctly hashed staged tree and authorization for a DIFFERENT run order: only the matrix comparison can refuse it."""
    other = next(s for s in range(50) if [r.B for r in li.measured_rows(s)] != [r.B for r in li.measured_rows(s2.TEST_SEED)])
    L7 = s2.prepare_launch(tmp_path / "a", li.MEASURED)
    L9 = s2.prepare_launch(tmp_path / "b", li.MEASURED, seed=other)
    L9.rows = L7.rows                                                                          # rows of one order, a staged tree, approval and authorization of another
    out = L9.run()
    assert out["status"] == "PREFLIGHT_FAILED" and "R3" in out["failed_gates"]
    detail = next(g["detail"] for g in json.loads((L9.preflight_dir / "preflight_report.json").read_text())["gates"] if g["id"] == "R3")
    assert "replay_matrix.csv differs" in detail and "stage verify" not in detail and "authorized stage_sums_sha256" not in detail


def test_s25_a_staged_tree_that_does_not_hash_to_the_authorized_value_fails_r3(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY, auth_mutate={"stage_sums_sha256": "1" * 64})
    out = L.run()
    assert out["status"] == "PREFLIGHT_FAILED" and "R3" in out["failed_gates"]
    assert "authorized stage_sums_sha256" in next(g["detail"] for g in json.loads((L.preflight_dir / "preflight_report.json").read_text())["gates"] if g["id"] == "R3")


def test_s27_a_manifest_that_omits_a_reviewed_file_outside_p4_replay_fails_s10(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    omitted = "scripts/e1/e1_log_collector.py"
    assert omitted in L.source_manifest
    L.source_manifest = {k: v for k, v in L.source_manifest.items() if k != omitted}         # the tree check cannot see it: it only watches p4_replay
    out = L.run()
    assert out["status"] == "PREFLIGHT_FAILED" and out["failed_gates"] == ["S10"]
    assert "does not cover the reviewed set" in next(g["detail"] for g in json.loads((L.preflight_dir / "preflight_report.json").read_text())["gates"] if g["id"] == "S10")


def test_s28_a_snapshot_that_is_not_the_pinned_one_fails_both_r1_and_s1(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    (L.snapshot["root"] / "SHA256SUMS.txt").write_text("changed\n")
    out = L.run()
    assert out["status"] == "PREFLIGHT_FAILED" and out["failed_gates"] == ["R1", "S1"]
    assert not any("archive_evidence.py" in c for c in s2.cmd_text(L.be)), "S1 refused before it ran the 18-minute verify against a snapshot that is not the pinned one"


def test_s32_a_dry_launch_that_names_an_earlier_dry_run_fails_r6(tmp_path):
    D = s2.prepare_launch(tmp_path / "d", li.DRY)
    D.run()
    L = s2.prepare_launch(tmp_path / "e", li.DRY)
    out = L.run(dry_summary_path=D.ctx.root / "series_summary.json")
    assert out["status"] == "PREFLIGHT_FAILED" and out["failed_gates"] == ["R6"]
