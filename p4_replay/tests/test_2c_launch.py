"""Phase 2C, Packages A and C at launch level, on the FAKE backend only. Two-launch safety (a dry run then the measured series), no overwrite or resume or replacement, every binding
checked before anything starts, the backend must match the mode, no gate can be skipped, and the stop policy fails closed. None of this is a live test."""

import dataclasses
import inspect
import json

import pytest

from p4_replay import analysis_primary as an, driver as dr, launch_approval as la, launch_ids as li, launch_series as ls, pins as pn, replay_classify as rc, series as se, snapshot as sn
from p4_replay.tests import support as sp, support_2c as s2
from p4_replay.tests.support import Faults

p1b = dr.p1b_module()
LATE = lambda idx, i, o: 600 if (idx == 1 and i % 2 == 0) else 0                                   # half of the measured dispatches 600 ms late: median-referenced N1 fails


def cmds(L):
    return s2.cmd_text(L.be)


def started(L, rid):
    return any(f"e1-{rid}" in c for c in cmds(L))


def dry_then_measured(tmp_path, *, dry_world=None, meas_world=None, **meas_kw):
    D = s2.prepare_launch(tmp_path, li.DRY, w=dry_world)
    ds = D.run()
    channels = ("e1-r002", "e1-cfg001", D.rows[0].channel)
    M = s2.prepare_launch(tmp_path, li.MEASURED, w=meas_world or s2.world(existing_channels=channels), dry_summary=D.ctx.root / "series_summary.json", snap_id="ARCHIVE_20261121T090000Z",
                          snap_finished="2026-11-21T09:00:00.000000Z", tag="_m", **meas_kw)
    return D, ds, M


# ----------------------------------------------------------------------------------------------- problem 1: a dry run does not contaminate the measured launch
def test_a_dry_launch_then_the_measured_launch_do_not_collide_and_leave_the_dry_evidence_untouched(tmp_path):
    D, ds, M = dry_then_measured(tmp_path)
    assert ds["status"] == "COMPLETE" and ds["runs"][0]["run_id"] == "rp90" and ds["runs"][0]["class"] == rc.HEALTHY_PROVISIONAL and ds["keep_awake_cleanup"]["ok"] is True
    dry_ws, dry_root = D.ctx.caliper_ws / "runs" / "rp90", D.ctx.root
    before = (s2.tree_digest(dry_root), s2.tree_digest(dry_ws))
    assert dry_ws.is_dir() and any(c for c in s2.cmd_text(D.be) if "e1-rp90-b10-r0" in c)
    ms = M.run()
    assert ms["status"] == "COMPLETE", ms.get("failed_gates") or ms.get("halted")
    assert [(r["run_id"], r["class"]) for r in ms["runs"]] == [(f"rp0{i}", rc.HEALTHY) for i in range(1, 7)]
    assert (s2.tree_digest(dry_root), s2.tree_digest(dry_ws)) == before, "the measured launch changed the dry run's evidence"
    assert D.ctx.root != M.ctx.root and not any("rp90" in c for c in cmds(M) if "e1_channel_run" in c or "caliper" in c.lower() and "docker exec" in c)
    assert not any("e1-rp0" in c for c in s2.cmd_text(D.be)), "the dry launch never touched a measured identifier"


def test_the_measured_launch_refuses_to_start_on_a_dry_run_that_did_not_complete(tmp_path):
    D = s2.prepare_launch(tmp_path, li.DRY, w=s2.world(faults={"rp90": Faults(extra_lag_ms=LATE)}))
    ds = D.run()
    assert ds["status"] == "HALTED" and ds["runs"][0]["class"] in rc.CONTINUABLE
    M = s2.prepare_launch(tmp_path, li.MEASURED, dry_summary=D.ctx.root / "series_summary.json", snap_id="ARCHIVE_20261121T090000Z", snap_finished="2026-11-21T09:00:00.000000Z", tag="_m")
    ms = M.run()
    assert ms["status"] == "PREFLIGHT_FAILED" and ms["failed_gates"] == ["R6"] and not M.ctx.root.exists() and not any(started(M, f"rp0{i}") for i in range(1, 7))


def test_a_baseline_snapshot_taken_before_the_dry_run_is_refused_by_r6(tmp_path):
    D = s2.prepare_launch(tmp_path, li.DRY)
    D.run()
    M = s2.prepare_launch(tmp_path, li.MEASURED, dry_summary=D.ctx.root / "series_summary.json", snap_id="ARCHIVE_20261110T090000Z", snap_finished="2026-11-10T09:00:00.000000Z", tag="_m")
    ms = M.run()
    assert ms["status"] == "PREFLIGHT_FAILED" and ms["failed_gates"] == ["R6"]
    report = json.loads((M.preflight_dir / "preflight_report.json").read_text())
    assert "does not contain the dry run's residue" in next(g["detail"] for g in report["gates"] if g["id"] == "R6")


def test_drift_from_the_dry_runs_output_fails_s1_against_an_older_snapshot_and_passes_against_one_taken_after_it(tmp_path):
    residue = ("SOURCE_ADDED wsl_e1/caliper-ws-benchmark/runs/rp90/dispatch_pre_roll.csv\nSOURCE_ADDED wsl_e1/caliper-ws-benchmark/runs/rp90/recorder/rp90_w0_r1_m-rp90.csv\n"
               "verify: checked=100 mismatched=0 missing=0 unlisted=0 source_drift=2\n")
    D, _, _ = dry_then_measured(tmp_path / "a")
    contaminated = s2.prepare_launch(tmp_path / "a", li.MEASURED, w=s2.world(s1_text=residue, existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0")), dry_summary=D.ctx.root / "series_summary.json",
                                     snap_id="ARCHIVE_20261121T090000Z", snap_finished="2026-11-21T09:00:00.000000Z", tag="_c", launch_n=2)
    out = contaminated.run()
    assert out["status"] == "PREFLIGHT_FAILED" and out["failed_gates"] == ["S1"]
    assert "unexpected 2" in next(g["detail"] for g in json.loads((contaminated.preflight_dir / "preflight_report.json").read_text())["gates"] if g["id"] == "S1")


def test_one_launch_id_one_series_root_and_the_ids_name_their_kind(tmp_path):
    D, _, M = dry_then_measured(tmp_path)
    M.run()
    assert D.ctx.root.name == "p4r-dry-01" and M.ctx.root.name == "p4r-meas-01" and json.loads((M.ctx.root / "launch_manifest.json").read_text())["launch_kind"] == li.MEASURED


# ----------------------------------------------------------------------------------------------- no overwrite, no resume, no replacement
def test_a_halted_measured_launch_cannot_be_resumed_or_replaced_and_its_evidence_is_not_touched(tmp_path):
    D, _, M = dry_then_measured(tmp_path, meas_world=None)
    M.w.clock_step_in_run.add("rp03")
    summary = M.run()
    assert summary["status"] == "HALTED" and [r["run_id"] for r in summary["runs"]] == ["rp01", "rp02", "rp03"] and summary["runs"][2]["class"] == rc.CLOCK
    snapshot_before = (s2.tree_digest(M.ctx.root), s2.tree_digest(M.ctx.caliper_ws / "runs"))
    # (a) the same launch again, on a fresh backend whose world now holds the channels the first attempt created
    consumed = ("e1-r002", "e1-cfg001", "e1-rp90-b10-r0") + tuple(r.channel for r in M.rows[:3])
    be2 = s2.FakeLaunchBackend(s2.world(existing_channels=consumed))
    M.session = p1b.Session(ctx=M.ctx, be=be2, thr=dr.replay_thresholds())
    M.be, M.preflight_dir = be2, tmp_path / "preflight_meas_again"
    again = M.run()
    assert again["status"] == "PREFLIGHT_FAILED" and {"S8", "S11", "R5"} <= set(again["failed_gates"]) and again["started_runs"] == []
    assert not any("e1_channel_run" in c for c in s2.cmd_text(be2)), "no channel was created by the second attempt"
    assert (s2.tree_digest(M.ctx.root), s2.tree_digest(M.ctx.caliper_ws / "runs")) == snapshot_before
    assert (M.preflight_dir / "launch_summary_not_started.json").is_file() and json.loads((M.preflight_dir / "keep_awake_cleanup.json").read_text())["ok"] is True, \
        "the refused attempt recorded itself in its own new folder, not in the earlier launch's root"
    # (b) a NEW launch id over the same run ids: the identifiers themselves are consumed
    M3 = s2.prepare_launch(tmp_path, li.MEASURED, w=s2.world(existing_channels=consumed), dry_summary=D.ctx.root / "series_summary.json", snap_id="ARCHIVE_20261122T090000Z",
                           snap_finished="2026-11-22T09:00:00.000000Z", tag="_m3", launch_n=2)
    third = M3.run()
    assert third["status"] == "PREFLIGHT_FAILED" and {"S8", "S11"} <= set(third["failed_gates"]) and not M3.ctx.root.exists()
    # (c) there is no spare identifier to replace a failed run with
    with pytest.raises(li.LaunchIdError):
        li.validate_launch_rows(li.MEASURED, M.rows[:2] + [dataclasses.replace(M.rows[2], run_id="rp07", channel="e1-rp07-b10-r1")] + M.rows[3:])


def test_the_preflight_folder_and_series_root_are_create_exclusive(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    L.preflight_dir.mkdir()
    with pytest.raises(FileExistsError):
        L.run()
    assert list(L.preflight_dir.iterdir()) == [] and not L.ctx.root.exists(), "a folder that pre-existed belongs to someone else: nothing was written into it"
    assert L.be.keep_killed or not L.be.keep_alive, "the helper that had been started was still cleaned up"


def test_a_run_never_overwrites_its_own_evidence(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    L.run()
    p = L.ctx.rd("rp90") / "classification.json"
    with pytest.raises(FileExistsError):
        p1b.write_json_new(p, {"x": 1})
    assert json.loads(p.read_text())["class"] == rc.HEALTHY_PROVISIONAL


# ----------------------------------------------------------------------------------------------- a failed approval stops execution, before any backend call
def nothing_started(L):
    return L.be.calls == [] and L.be.spawns == [] and not L.ctx.root.exists() and not L.preflight_dir.exists()


@pytest.mark.parametrize("what", ["approval_hash", "pins_hash", "kind", "root_name", "retired_ids", "snapshot", "no_staged_manifest", "thresholds_proposed_for_measured"])
def test_a_failed_binding_stops_execution_before_anything_starts(tmp_path, what):
    kind = li.MEASURED if what == "thresholds_proposed_for_measured" else li.DRY
    L = s2.prepare_launch(tmp_path, kind, dry_summary=None, auth_mutate={"series_approval_sha256": "0" * 64} if what == "approval_hash" else {"pins_sha256": "1" * 64} if what == "pins_hash" else None)
    if what == "kind":
        L.approval = la.check_launch_approval(la.example_approval_text(kind=li.MEASURED, thresholds_status="APPROVED", prereg_sha256=pn.sha256_file(L.prereg_path)).encode(), expected_kind=li.MEASURED,
                                              prereg_sha256=pn.sha256_file(L.prereg_path))
    elif what == "root_name":
        L.session.ctx = dataclasses.replace(L.ctx, root=L.ctx.root.parent / "p4r-dry-09")
    elif what == "retired_ids":
        L.rows = dr.build_matrix(7)[:1]
    elif what == "snapshot":
        L.session.ctx = dataclasses.replace(L.ctx, win_snapshot=sn.SNAPSHOT_PATH_WIN)
    elif what == "no_staged_manifest":
        (L.staged.stage_dir / "series_manifest.json").unlink()
    elif what == "thresholds_proposed_for_measured":
        L.approval = dataclasses.replace(L.approval, thresholds=dataclasses.replace(L.approval.thresholds, status="PROPOSED"))
    with pytest.raises((SystemExit, ValueError, FileNotFoundError, sn.SnapshotError)):
        L.run()
    assert nothing_started(L), what


@pytest.mark.parametrize("route,text,fragment", [("SNAPSHOT_EXACT", "repo_p4_replay/x.py  " + "a" * 64 + "\n", "SNAPSHOT_EXACT"), ("SIGNED_DRIFT_UPDATE", None, "no drift list"),
                                                  ("SIGNED_DRIFT_UPDATE", "repo_p4_replay/x.py  " + "b" * 64 + "\n", "not what the owner signed")])
def test_the_s1_allowance_must_be_exactly_what_the_authorization_names(tmp_path, route, text, fragment):
    L = s2.prepare_launch(tmp_path, li.DRY, s1_route=route, s1_list_text="repo_p4_replay/x.py  " + "a" * 64 + "\n" if route == "SIGNED_DRIFT_UPDATE" else None)
    with pytest.raises(sn.SnapshotError, match=fragment):
        L.run(s1_allowed_text=text)
    assert nothing_started(L)


def test_a_signed_allowance_passes_only_while_the_file_still_has_the_signed_hash(tmp_path):
    path = "repo_p4_replay/live_entry.py"
    real = pn.sha256_file(s2.REPO_ROOT / "p4_replay" / "live_entry.py")
    drift = f"{path}\nverify: checked=100 mismatched=0 missing=0 unlisted=0 source_drift=1\n"
    good = f"{path}  {real}\n"
    L = s2.prepare_launch(tmp_path / "ok", li.DRY, w=s2.world(s1_text=f"SOURCE_ADDED {drift}"), s1_route="SIGNED_DRIFT_UPDATE", s1_list_text=good)
    assert L.run(s1_allowed_text=good)["status"] == "COMPLETE"
    stale = f"{path}  {'0' * 64}\n"
    S = s2.prepare_launch(tmp_path / "stale", li.DRY, w=s2.world(s1_text=f"SOURCE_ADDED {drift}"), s1_route="SIGNED_DRIFT_UPDATE", s1_list_text=stale)
    out = S.run(s1_allowed_text=stale)
    assert out["status"] == "PREFLIGHT_FAILED" and out["failed_gates"] == ["S1"]


# ----------------------------------------------------------------------------------------------- the backend must match the mode
class Hybrid(p1b.Backend):
    OFFLINE_FAKE = True


class Sub(p1b.Backend):
    pass


def test_the_backend_rules_by_mode(monkeypatch):
    fake, real = s2.FakeLaunchBackend(), object.__new__(p1b.Backend)
    ls.require_backend_for_mode(fake, ls.MODE_OFFLINE)
    for bad in (real, object(), object.__new__(Hybrid), object.__new__(Sub)):
        with pytest.raises(dr.LiveExecutionRefused):
            ls.require_backend_for_mode(bad, ls.MODE_OFFLINE)
    for bad in (fake, object(), object.__new__(Hybrid), object.__new__(Sub), real):             # a fake, an undeclared object, a subclass, and (flag False) the exact class
        with pytest.raises(dr.LiveExecutionRefused):
            ls.require_backend_for_mode(bad, ls.MODE_LIVE)
    monkeypatch.setattr(dr, "LIVE_EXECUTION_ENABLED", True)
    ls.require_backend_for_mode(real, ls.MODE_LIVE)                                              # only the exact class, only with the flag
    for bad in (fake, object.__new__(Hybrid), object.__new__(Sub), object()):
        with pytest.raises(dr.LiveExecutionRefused):
            ls.require_backend_for_mode(bad, ls.MODE_LIVE)
    with pytest.raises(ValueError):
        ls.require_backend_for_mode(fake, "SOMETHING")


@pytest.mark.parametrize("bad", ["real", "undeclared"])
def test_run_launch_refuses_a_real_or_undeclared_backend_before_any_call(tmp_path, bad):
    L = s2.prepare_launch(tmp_path, li.DRY)
    L.session.be = object.__new__(p1b.Backend) if bad == "real" else object()
    with pytest.raises(dr.LiveExecutionRefused):
        L.run()
    assert not L.ctx.root.exists() and not L.preflight_dir.exists() and L.be.calls == [] and L.be.spawns == []


def test_the_live_flag_is_false_and_nothing_in_the_launch_path_flips_it(tmp_path):
    assert dr.LIVE_EXECUTION_ENABLED is False
    L = s2.prepare_launch(tmp_path, li.DRY)
    L.run()
    assert dr.LIVE_EXECUTION_ENABLED is False


# ----------------------------------------------------------------------------------------------- no gate can be skipped
def test_the_preflight_has_no_parameter_that_skips_or_subsets_a_gate():
    for fn in (ls.run_launch_preflight, ls.run_launch, ls._core):
        names = set(inspect.signature(fn).parameters)
        assert not ({"gates", "skip", "only", "subset", "skip_gates", "force", "waive", "bypass"} & names), (fn.__name__, names)
    assert ls.GATE_ORDER_V2 == ("S3", "S2", "S4", "S5", "S6", "R1", "R2", "R3", "S7", "S8", "S9", "R4", "S10", "S11", "R5", "R6", "S1") and len(set(ls.GATE_ORDER_V2)) == 17


def test_every_gate_is_evaluated_and_required_and_the_p1b_thresholds_are_untouched(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    assert L.run()["status"] == "COMPLETE"
    report = json.loads((L.preflight_dir / "preflight_report.json").read_text())
    assert report["all_pass"] is True and report["evaluated"] == list(ls.GATE_ORDER_V2) == report["required"] and all(g["status"] == "PASS" for g in report["gates"])
    t = report["thresholds_p1b_unchanged"]
    assert (t["win_avail_mb_start"], t["win_commit_pct_start"], t["win_pages_median_start"]) == (1500.0, 85.0, 100.0), "S3's owner thresholds ('do not waive') are P1b's"


def _w(**kw):
    return s2.world(**kw)


def gate_cases():
    def tamper_snapshot(L):
        (L.snapshot["root"] / "SHA256SUMS.txt").write_text("changed\n")

    def tamper_stage(L):
        (L.staged.stage_dir / "runs" / "rp90" / "schedule_measured.txt").write_text("0\n")

    def tamper_manifest(L):
        L.source_manifest = dict(L.source_manifest, **{"p4_replay/pins.py": "0" * 64})

    def stale_folder(L):
        (L.ctx.caliper_ws / "runs" / "rp90").mkdir(parents=True)

    def bad_clock(L):
        L.clock_dir = sp.write_clock_dir(L.tmp / "bad_clock", verdict="FAIL")

    def no_getinfo(L):
        real = L.be.run
        L.be.run = lambda argv, env=None, timeout=None: (p1b.CmdResult(code=1, stdout=b"", stderr=b"x") if "getinfo" in " ".join(map(str, argv)) else real(argv, env, timeout))

    return [
        ("S3", {"w": dict(win_available_mb=1000.0)}, None, {}), ("S2", {"w": dict(power_line="Battery")}, None, {}), ("S4", {"w": dict(disk_gb=5.0)}, None, {}),
        ("S5", {"w": dict(not_running={"caliper-benchmark-001"})}, None, {}), ("S6", {"w": dict(raft_active={"orderer.example.com": 2})}, None, {}),
        ("R1", {}, tamper_snapshot, {}), ("R2", {}, None, {"series_root_prefix": "/somewhere/else"}), ("R3", {}, tamper_stage, {}), ("S7", {}, no_getinfo, {}),
        ("S8", {"w": dict(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"))}, None, {}), ("S9", {}, bad_clock, {}),
        ("R4", {"w": dict(caliper_top=["1 sleep infinity", "42 node /x/node_modules/.bin/caliper launch manager"])}, None, {}), ("S10", {}, tamper_manifest, {}),
        ("S11", {}, stale_folder, {}), ("S1", {"w": dict(s1_text="SOURCE_ADDED repo_p4_replay/new.py\nverify: checked=100 mismatched=0 missing=0 unlisted=0 source_drift=1\n")}, None, {}),
    ]


@pytest.mark.parametrize("gid,world_kw,mutate,run_kw", gate_cases(), ids=[c[0] for c in gate_cases()])
def test_each_failed_gate_prevents_any_run_and_the_helper_is_still_cleaned_up(tmp_path, gid, world_kw, mutate, run_kw):
    L = s2.prepare_launch(tmp_path, li.DRY, w=_w(**world_kw.get("w", {})))
    if mutate:
        mutate(L)
    out = L.run(**run_kw)
    assert out["status"] == "PREFLIGHT_FAILED" and gid in out["failed_gates"] and out["started_runs"] == [], out["failed_gates"]
    assert not L.ctx.root.exists() and not any("e1_channel_run" in c for c in cmds(L)) and not any("clock_rate_probe" in " ".join(s) for s in L.be.spawns)
    report = json.loads((L.preflight_dir / "preflight_report.json").read_text())
    assert report["evaluated"] == list(ls.GATE_ORDER_V2) and report["all_pass"] is False, "every gate was still evaluated"
    assert out["keep_awake_cleanup"]["ok"] is True


def test_r6_needs_the_dry_run_summary_for_a_measured_launch(tmp_path):
    L = s2.prepare_launch(tmp_path, li.MEASURED)
    out = L.run(dry_summary_path=None)
    assert out["status"] == "PREFLIGHT_FAILED" and out["failed_gates"] == ["R6"]


def test_r5_fails_when_the_series_root_already_exists(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    L.ctx.root.mkdir(parents=True)
    out = L.run()
    assert out["status"] == "PREFLIGHT_FAILED" and {"R5", "S11"} <= set(out["failed_gates"])


def test_several_failed_gates_are_all_reported(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY, w=_w(win_available_mb=500.0, disk_gb=1.0, power_line="Battery"))
    out = L.run()
    assert {"S3", "S4", "S2"} <= set(out["failed_gates"])


# ----------------------------------------------------------------------------------------------- the stop policy fails closed
def measured(tmp_path, w=None, **kw):
    D = s2.prepare_launch(tmp_path, li.DRY)
    D.run()
    world = w or s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"))
    return s2.prepare_launch(tmp_path, li.MEASURED, w=world, dry_summary=D.ctx.root / "series_summary.json", snap_id="ARCHIVE_20261121T090000Z", snap_finished="2026-11-21T09:00:00.000000Z",
                             tag="_m", **kw)


def ids(summary):
    return [r["run_id"] for r in summary["runs"]]


def preserved(L, rid):
    rd = L.ctx.rd(rid)
    return rd.is_dir() and (rd / "facts.json").is_file() and (rd / "series_verdict.json").is_file()


STOPS = [
    ("clock", "rp03", {"clock_step_in_run": {"rp03"}}, "CLOCK_FAILURE", "SC6"),
    ("network", "rp02", {"caliper_abort_runs": {"rp02"}}, None, "SC4"),
    ("configuration", "rp02", {"config_drift_runs": {"rp02"}}, None, "SC4"),
    ("reconciliation", "rp02", {"faults": {"rp02": Faults(drop_recorder_row=True)}}, "SUBMISSION_FAILURE", "SC6"),
    ("invalid transaction", "rp02", {"faults": {"rp02": Faults(invalid_seq=3)}}, "INVALID_TRANSACTION", "SC6"),
    ("limited fidelity", "rp02", {"faults": {"rp02": Faults(extra_lag_ms=LATE)}}, "FIDELITY_LIMITED_REPLAY", "SC7"),
    ("limited client", "rp02", {"faults": {"rp02": Faults(cpu_base=90.0)}}, "CLIENT_LIMITED_REPLAY", "SC7"),
]


@pytest.mark.parametrize("name,last,world_kw,cls,sc", STOPS, ids=[s[0] for s in STOPS])
def test_each_halt_condition_halts_without_retry_and_keeps_the_failed_runs_evidence(tmp_path, name, last, world_kw, cls, sc):
    L = measured(tmp_path, s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"), **world_kw))
    out = L.run()
    n = int(last[-1])
    assert out["status"] == "HALTED" and ids(out) == [f"rp0{i}" for i in range(1, n + 1)] and sc in out["halted"], (out["status"], out["halted"])
    if cls:
        assert out["runs"][-1]["class"] == cls
    assert not any(started(L, f"rp0{i}") for i in range(n + 1, 7)), "no later run was started, none was retried or replaced"
    assert sum(1 for c in cmds(L) if "e1_channel_run" in c and f"e1-{last}-" in c) == 1, "the failing run was not retried"
    assert preserved(L, last) and (L.ctx.root / "series_summary.json").is_file() and out["keep_awake_cleanup"]["ok"] is True


def test_an_integrity_failure_between_runs_halts_before_the_next_run(tmp_path):
    L = measured(tmp_path, s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"), fingerprint_changes_after=2))
    out = L.run()
    assert out["status"] == "HALTED" and ids(out) == ["rp01"] and "SC10" in out["halted"] and out["runs"][0]["integrity_differences"] and not started(L, "rp02")
    assert out["fingerprint_differences"], "the end-of-launch comparison reports it too"


def test_hard_failures_halt_even_when_the_owner_pre_approved_continuing(tmp_path):
    L = measured(tmp_path, s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"), clock_step_in_run={"rp02"}), approval_mutate={"continue_after_limited_measured": "continue"})
    out = L.run()
    assert out["status"] == "HALTED" and ids(out) == ["rp01", "rp02"] and out["runs"][-1]["class"] == rc.CLOCK


def test_a_limited_measured_run_continues_only_when_pre_approved_and_is_never_replaced(tmp_path):
    L = measured(tmp_path, s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"), faults={"rp02": Faults(extra_lag_ms=LATE)}), approval_mutate={"continue_after_limited_measured": "continue"})
    out = L.run()
    assert out["status"] == "COMPLETE_WITH_LIMITED_RUNS" and out["limited_runs"] == ["rp02"] and len(out["runs"]) == 6 and out["runs"][1]["class"] == rc.FIDELITY
    assert sum(1 for c in cmds(L) if "e1_channel_run" in c and "e1-rp02-" in c) == 1, "the limited run was kept, not rerun"


def test_a_limited_dry_run_always_halts(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY, w=s2.world(faults={"rp90": Faults(extra_lag_ms=LATE)}), approval_mutate={"continue_after_limited_measured": "continue"})
    out = L.run()
    assert out["status"] == "HALTED" and "SC3" in out["halted"]


def test_two_consecutive_invalid_runs_halt_and_one_does_not(tmp_path):
    one = measured(tmp_path / "one", s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"), fail_commands={("rp02", "e1_block_fetch.py")}))
    a = one.run()
    assert a["status"] == "COMPLETE_WITH_INVALID_RUNS" and a["invalid_runs"] == ["rp02"] and len(a["runs"]) == 6
    two = measured(tmp_path / "two", s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"), fail_commands={("rp02", "e1_block_fetch.py"), ("rp03", "e1_block_fetch.py")}))
    b = two.run()
    assert b["status"] == "HALTED" and "SC5" in b["halted"] and ids(b) == ["rp01", "rp02", "rp03"]


def test_a_dead_clock_probe_at_the_start_halts_before_any_run(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY, w=s2.world(probe_dies_at_start=True))
    out = L.run()
    assert out["status"] == "HALTED" and "exited at once" in out["halted"] and out["runs"] == [] and not started(L, "rp90")


def test_the_keep_awake_helper_dying_mid_launch_halts_at_the_next_precheck(tmp_path):
    L = measured(tmp_path, s2.world(existing_channels=("e1-r002", "e1-cfg001", "e1-rp90-b10-r0"), keep_dies_in_run={"rp01"}))
    out = L.run()
    assert out["status"] == "HALTED" and ids(out) == ["rp01"] and "pre-run check failed" in out["halted"] and not started(L, "rp02")


def test_an_interruption_finalises_cleans_up_and_restores_the_executor(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)

    def interrupted(s, rec, plan_raw, env):
        raise KeyboardInterrupt

    L.helpers["run_caliper"] = interrupted
    steps, evaluate = dr.run_steps, dr.evaluate_run_dir
    L.be.survives_terminate = True
    with pytest.raises(KeyboardInterrupt):
        L.run()
    assert dr.run_steps is steps and dr.evaluate_run_dir is evaluate, "the executor overrides were restored"
    summary = json.loads((L.ctx.root / "series_summary.json").read_text())
    assert summary["status"] == "HALTED" and "INTERRUPTED KeyboardInterrupt" in summary["halted"] and summary["keep_awake_cleanup"]["outcome"] == "TERMINATED_VERIFIED"
    assert (L.ctx.root / "keep_awake_cleanup.json").is_file() and L.be.keep_killed


def test_a_cleanup_failure_is_recorded_in_the_summary_and_the_status(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    L.be.survives_terminate = True
    L.be.taskkill_fails = True
    out = L.run()
    assert out["status"] == "COMPLETE_KEEP_AWAKE_CLEANUP_FAILED" and out["keep_awake_cleanup"]["outcome"] == "STILL_RUNNING_AFTER_KILL"
    on_disk = json.loads((L.ctx.root / "keep_awake_cleanup.json").read_text())
    assert on_disk["ok"] is False and json.loads((L.ctx.root / "series_summary.json").read_text())["keep_awake_cleanup"]["ok"] is False


# ----------------------------------------------------------------------------------------------- classification inside a launch, and the other Package C outputs
def test_the_executor_is_given_real_clock_evidence_and_the_approved_thresholds_label_fidelity_json(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    L.run()
    fidelity = json.loads((L.ctx.rd("rp90") / "fidelity.json").read_text())
    c1 = next(c for c in fidelity["checks"] if c["id"] == "C1")
    assert c1["ok"] is True and fidelity["threshold_status"] == "PROPOSED" and fidelity["thresholds"]["rel_lag_p99_ms"] == 20.0
    cls = json.loads((L.ctx.rd("rp90") / "classification.json").read_text())
    assert cls["clock_evidence_full"]["verdict"] == "PASS" and cls["clock_evidence_full"]["density"]["ratio"] >= 0.95 and cls["diagnostics"]["reference"]["reference"] == "median"
    assert cls["class"] == rc.HEALTHY_PROVISIONAL and cls["thresholds"]["gating"]["N1_lag_p99_ms"] == 20.0 and cls["thresholds"]["diagnostic_only"]["N2_lag_max_ms"] == 100.0


def test_the_summary_states_what_cannot_be_removed(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    out = L.run()
    assert any("backlog watchdog" in x for x in out["limits"]) and any("never resumed" in x for x in out["limits"]) and out["claims"]["live_scheduling_fidelity_validated"] is False


def test_the_measured_launch_writes_analysis_inputs_that_the_primary_analysis_reads(tmp_path):
    M = measured(tmp_path)
    out = M.run()
    assert out["status"] == "COMPLETE"
    man = json.loads((M.ctx.root / "analysis_inputs.json").read_text())
    assert man["schema"] == an.SCHEMA and [r["run_id"] for r in man["runs"]] == ["rp01", "rp02", "rp03", "rp04", "rp05", "rp06"] and all(r["class"] == "HEALTHY_REPLAY" for r in man["runs"])
    for r in man["runs"]:
        assert r["recorder_csv"].startswith(str(M.ctx.root)) and pn.sha256_file(__import__("pathlib").Path(r["recorder_csv"])) == r["recorder_sha256"]
    report = an.analyze(an.load_manifest(M.ctx.root / "analysis_inputs.json"))
    m = report["measured_outcomes"]
    assert m["contrast_status"] == "COMPUTED_n3_per_arm" and {r["p95_ms"] for r in m["runs"]} == {1500.0} and m["contrast"]["difference_of_medians_ms"] == 0.0
    assert [r["B"] for r in m["runs"]] == [r.B for r in M.rows]
    dry = s2.prepare_launch(tmp_path / "d", li.DRY)
    assert dry.run()["analysis_inputs"] is None, "no analysis inputs for a dry run"
