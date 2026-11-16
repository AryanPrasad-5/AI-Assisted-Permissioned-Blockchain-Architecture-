"""Phase 2C, Package A problem 2: the live entry point. Since the reviewed edit N9 (2026-11-15) the entry flag is True; `launch` still refuses unless every check passes, and with the flag False it refuses before it reads anything. Every check that guards an enabled launch is
tested here offline: the artifacts are re-hashed, a failed approval stops execution, the backend is built only after the checks and must be the exact class, and the single place that
could switch the live flag does so only inside a context that restores it. No test starts a real backend: where a test changes LIVE_ENTRY_ENABLED it does so with monkeypatch, in memory,
for one test, and the backend it uses is a sentinel or a fake."""

import ast
import json
import re
from pathlib import Path, PurePosixPath

import pytest

from p4_replay import REPO_ROOT, driver as dr, launch_ids as li, launch_series as ls, live_entry as le, pins as pn
from p4_replay.tests import support_2c as s2

p1b = dr.p1b_module()
PKG = REPO_ROOT / "p4_replay"
NON_TEST = sorted(p for p in PKG.glob("*.py"))


def rewrite_auth(L, **kv):
    text = L.files["authorization"].read_text()
    for k, v in kv.items():
        text = re.sub(rf"^{k}=.*$", f"{k}={v}", text, flags=re.M)
    L.files["authorization"].write_text(text)


def names_failed(plan):
    return [n for n, ok, _ in plan.checks if not ok]


def good(tmp_path, kind=li.DRY, **kw):
    L = s2.prepare_launch(tmp_path, kind, dry_summary=None if kind == li.DRY else None, **kw)
    return L


# ----------------------------------------------------------------------------------------------- disabled by default
def test_the_entry_flag_is_one_reviewed_literal_and_the_inner_live_flag_is_off_at_rest():
    """N9 (2026-11-15): the reviewed build enables the entry. The pin moves with it: a literal True, assigned once, and the INNER flag stays False at rest (only the context manager sets it)."""
    assert le.LIVE_ENTRY_ENABLED is True and dr.LIVE_EXECUTION_ENABLED is False
    tree = ast.parse((PKG / "live_entry.py").read_text(encoding="utf-8"))
    consts = [n for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "LIVE_ENTRY_ENABLED" for t in n.targets)]
    assert len(consts) == 1 and isinstance(consts[0].value, ast.Constant) and consts[0].value.value is True, "a literal True, assigned once"


def test_the_enabled_build_refuses_a_launch_without_the_owner_artifacts_and_builds_no_backend(tmp_path, monkeypatch):
    built = []
    monkeypatch.setattr(le, "_construct_live_backend", lambda: built.append("backend"))
    with pytest.raises(le.LiveEntryRefused, match="authorization"):
        le.launch(authorization=tmp_path / "none", approval=tmp_path / "none", pins=tmp_path / "none", source_manifest=tmp_path / "none", prereg=tmp_path / "none",
                  stage_root=tmp_path / "none", clock_dir=tmp_path / "none")
    assert built == [] and dr.LIVE_EXECUTION_ENABLED is False


def test_launch_is_refused_before_it_reads_a_file_or_builds_a_backend_when_the_flag_is_false(tmp_path, monkeypatch):
    monkeypatch.setattr(le, "LIVE_ENTRY_ENABLED", False)                       # the safeguard still exists in code: with the flag False nothing is read
    called = []
    monkeypatch.setattr(le, "verify_entry", lambda **kw: called.append("verify_entry"))
    monkeypatch.setattr(le, "_construct_live_backend", lambda: called.append("backend"))
    with pytest.raises(le.LiveEntryRefused, match="disabled"):
        le.launch(authorization=tmp_path / "none", approval=tmp_path / "none", pins=tmp_path / "none", source_manifest=tmp_path / "none", prereg=tmp_path / "none",
                  stage_root=tmp_path / "none", clock_dir=tmp_path / "none")
    assert called == []


def test_no_argument_environment_variable_or_module_attribute_reaches_the_switch():
    tree = ast.parse((PKG / "live_entry.py").read_text(encoding="utf-8"))
    attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert not ({"environ", "getenv", "putenv"} & (attrs | names)), "no environment variable is read"
    flags = [c.args[0].value for c in ast.walk(tree) if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute) and c.func.attr == "add_argument" and c.args and isinstance(c.args[0], ast.Constant)]
    assert not any(re.search(r"enable|live|force|bypass|skip", str(f)) for f in flags), flags
    launch_params = set(__import__("inspect").signature(le.launch).parameters)
    assert not ({"enable", "enabled", "force", "live", "bypass", "skip"} & launch_params)


def test_the_cli_reports_status_and_refuses_launch_when_disabled_or_when_an_artifact_is_missing(tmp_path, capsys, monkeypatch):
    L = good(tmp_path)
    assert le.main(["status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["LIVE_ENTRY_ENABLED"] is True and status["driver.LIVE_EXECUTION_ENABLED"] is False and len(status["gates"]) == 17 and status["retired_run_ids"] == ["rp00"]
    args = ["launch", "--authorization", str(L.files["authorization"]), "--approval", str(L.files["approval"]), "--pins", str(L.files["pins"]), "--source-manifest", str(L.source_manifest_path),
            "--prereg", str(L.prereg_path), "--stage-root", str(L.staged.stage_dir), "--clock-dir", str(L.clock_dir)]
    monkeypatch.setattr(le, "LIVE_ENTRY_ENABLED", False)
    assert le.main(args) == 4 and "disabled" in capsys.readouterr().out
    assert not L.ctx.root.exists()
    monkeypatch.setattr(le, "LIVE_ENTRY_ENABLED", True)                         # enabled, but one named artifact does not exist: refused, and no backend is ever built
    monkeypatch.setattr(le, "_construct_live_backend", lambda: pytest.fail("a backend was built although an artifact check failed"))
    missing = list(args)
    missing[missing.index("--authorization") + 1] = str(tmp_path / "no_such_authorization")
    assert le.main(missing) == 4 and "refused" in capsys.readouterr().out
    assert not L.ctx.root.exists()


def test_plan_and_budget_commands_are_offline_listings(tmp_path, capsys):
    assert le.main(["plan", "--kind", "DRY_RUN"]) == 0
    assert "rp90,e1-rp90-b10-r0" in capsys.readouterr().out
    assert le.main(["plan", "--kind", "MEASURED_SERIES", "--seed", "7"]) == 0
    out = capsys.readouterr().out
    assert [ln.split(",")[1] for ln in out.splitlines()[1:]] == list(li.MEASURED_RUN_IDS) and "rp90" not in out and "rp00" not in out
    L = good(tmp_path)
    assert le.main(["budget", "--stage-root", str(L.staged.stage_dir)]) == 0
    b = json.loads(capsys.readouterr().out)
    assert b["total_s"] > 3600 and b["preflight_s"] >= 3600 and len(b["components"]) > 20
    with pytest.raises(SystemExit):
        le.main(["plan", "--kind", "OTHER"])


# ----------------------------------------------------------------------------------------------- verify_entry: every check
@pytest.mark.parametrize("kind", [li.DRY, li.MEASURED])
def test_a_consistent_set_of_artifacts_passes_every_check(tmp_path, kind):
    if kind == li.MEASURED:
        D = s2.prepare_launch(tmp_path / "dry", li.DRY)
        D.run()
        L = s2.prepare_launch(tmp_path, li.MEASURED, dry_summary=D.ctx.root / "series_summary.json")
    else:
        L = s2.prepare_launch(tmp_path, kind)
    plan = le.verify_entry(**L.entry_kwargs(), s1_list=None)
    assert plan.ok is True and names_failed(plan) == [] and len(plan.checks) >= 16, plan.failed()
    assert plan.rows == L.rows and plan.auth.launch_id == L.launch_id and set(plan.artifact_hashes) >= {"authorization_sha256", "entry_sha256", "series_approval_sha256", "source_manifest_sha256"}


def test_verify_entry_is_read_only_and_starts_nothing(tmp_path):
    L = good(tmp_path)
    before = (s2.tree_digest(tmp_path), s2.tree_digest(PKG))
    le.verify_entry(**L.entry_kwargs())
    assert (s2.tree_digest(tmp_path), s2.tree_digest(PKG)) == before and L.be.calls == [] and L.be.spawns == []


TAMPERS = {
    "entry hash": (lambda L: rewrite_auth(L, entry_sha256="0" * 64), "entry module hash"),
    "approval bytes": (lambda L: L.files["approval"].write_bytes(L.files["approval"].read_bytes() + b"# edited\n"), "series approval"),
    "approval invalid": (lambda L: L.files["approval"].write_bytes(re.sub(rb"^lp1_mode=.*$", b"lp1_mode=MAGNITUDE", L.files["approval"].read_bytes(), flags=re.M)), "series approval"),
    "pins bytes": (lambda L: L.files["pins"].write_bytes(L.files["pins"].read_bytes() + b" "), "pins file"),
    "snapshot content": (lambda L: (L.snapshot["root"] / "SHA256SUMS.txt").write_text("changed\n"), "snapshot identity"),
    "source manifest bytes": (lambda L: L.source_manifest_path.write_bytes(L.source_manifest_path.read_bytes() + b"\n# x\n"), "source manifest"),
    "staged file": (lambda L: (L.staged.stage_dir / "runs" / "rp90" / "schedule_measured.txt").write_text("0\n"), "staged tree"),
    "staged sums": (lambda L: rewrite_auth(L, stage_sums_sha256="1" * 64), "staged tree"),
    "s1 route": (lambda L: rewrite_auth(L, s1_route="SIGNED_DRIFT_UPDATE", s1_evidence_sha256="a" * 64), "S1 drift list"),
    "dry summary given to a dry launch": (lambda L: setattr(L, "dry_summary_path", L.tmp / "x.json") or (L.tmp / "x.json").write_text("{}"), "dry-run summary"),
}


@pytest.mark.parametrize("name", list(TAMPERS))
def test_every_tampered_artifact_fails_its_own_check_and_the_set_is_refused(tmp_path, name):
    L = good(tmp_path)
    mutate, fragment = TAMPERS[name]
    mutate(L)
    plan = le.verify_entry(**L.entry_kwargs())
    assert plan.ok is False and any(fragment in n for n in names_failed(plan)), (name, plan.checks)


def test_a_manifest_that_omits_a_file_or_misses_one_under_p4_replay_fails_two_checks(tmp_path):
    L = good(tmp_path)
    lines = L.source_manifest_path.read_text().splitlines()
    kept = [ln for ln in lines if not ln.endswith("p4_replay/pins.py")]
    L.source_manifest_path.write_text("\n".join(kept) + "\n")
    rewrite_auth(L, source_manifest_sha256=pn.sha256_file(L.source_manifest_path))
    plan = le.verify_entry(**L.entry_kwargs())
    failed = names_failed(plan)
    assert plan.ok is False and any("exact against the tree" in n for n in failed) and any("covers the reviewed set" in n for n in failed)
    assert "unlisted ['p4_replay/pins.py']" in next(d for n, ok, d in plan.checks if "exact against the tree" in n)


def test_a_matrix_that_the_approvals_seed_does_not_generate_is_refused(tmp_path):
    D = s2.prepare_launch(tmp_path / "dry", li.DRY)
    D.run()
    L = s2.prepare_launch(tmp_path, li.MEASURED, dry_summary=D.ctx.root / "series_summary.json")
    other_seed = next(s for s in range(50) if [r.B for r in li.measured_rows(s)] != [r.B for r in li.measured_rows(s2.TEST_SEED)])
    text = re.sub(rb"^run_order_seed=.*$", f"run_order_seed={other_seed}".encode(), L.files["approval"].read_bytes(), flags=re.M)
    L.files["approval"].write_bytes(text)
    rewrite_auth(L, series_approval_sha256=pn.sha256_file(L.files["approval"]))
    plan = le.verify_entry(**L.entry_kwargs())
    assert plan.ok is False and any("staged matrix" in n for n in names_failed(plan))


@pytest.mark.parametrize("what", ["wrong_hash", "missing"])
def test_a_measured_launch_needs_the_named_dry_run_summary(tmp_path, what):
    D = s2.prepare_launch(tmp_path / "dry", li.DRY)
    D.run()
    summary = D.ctx.root / "series_summary.json"
    L = s2.prepare_launch(tmp_path, li.MEASURED, dry_summary=summary)
    if what == "wrong_hash":
        summary2 = tmp_path / "other_summary.json"
        summary2.write_text(summary.read_text() + " ")
        L.dry_summary_path = summary2
    else:
        L.dry_summary_path = None
    plan = le.verify_entry(**L.entry_kwargs())
    assert plan.ok is False and any("dry-run summary" in n for n in names_failed(plan))


def test_a_malformed_authorization_or_a_missing_file_fails_and_does_not_raise(tmp_path):
    L = good(tmp_path)
    L.files["authorization"].write_text("not an authorization\n")
    plan = le.verify_entry(**L.entry_kwargs())
    assert plan.ok is False and names_failed(plan) == ["authorization well formed"] and plan.auth is None
    L2 = good(tmp_path / "b")
    kw = L2.entry_kwargs()
    kw["authorization"] = tmp_path / "nowhere.txt"
    plan2 = le.verify_entry(**kw)
    assert plan2.ok is False and names_failed(plan2) == ["authorization file readable"]
    L3 = good(tmp_path / "c")
    kw3 = L3.entry_kwargs()
    kw3["approval"] = tmp_path / "nowhere.txt"
    assert any("series approval file readable" in n for n in names_failed(le.verify_entry(**kw3)))


def test_a_signed_drift_list_must_be_the_one_the_authorization_names(tmp_path):
    text = "repo_p4_replay/live_entry.py  " + "a" * 64 + "\n"
    L = s2.prepare_launch(tmp_path, li.DRY, s1_route="SIGNED_DRIFT_UPDATE", s1_list_text=text)
    lst = tmp_path / "drift.txt"
    lst.write_bytes(text.encode())                                                           # bytes, not write_text: the hash binds the exact bytes
    assert le.verify_entry(**L.entry_kwargs(), s1_list=lst).ok is True
    lst.write_bytes(text.replace("\n", "\r\n").encode())                                     # the same text with CRLF line ends is NOT what was signed: it fails closed
    crlf = le.verify_entry(**L.entry_kwargs(), s1_list=lst)
    assert crlf.ok is False and any("S1 drift list" in n for n in names_failed(crlf))
    lst.write_bytes((text + "repo_p4_replay/other.py  " + "b" * 64 + "\n").encode())
    plan = le.verify_entry(**L.entry_kwargs(), s1_list=lst)
    assert plan.ok is False and any("S1 drift list" in n for n in names_failed(plan))
    assert any("S1 drift list" in n for n in names_failed(le.verify_entry(**L.entry_kwargs())))


# ----------------------------------------------------------------------------------------------- the enabled branch, ordering only (in memory, one test at a time, no real backend)
class Reached(Exception):
    pass


def enable(monkeypatch):
    monkeypatch.setattr(le, "LIVE_ENTRY_ENABLED", True)


def test_when_enabled_a_failed_check_refuses_before_any_backend_is_built(tmp_path, monkeypatch):
    L = good(tmp_path)
    TAMPERS["approval bytes"][0](L)
    built = []
    enable(monkeypatch)
    monkeypatch.setattr(le, "_construct_live_backend", lambda: built.append(1))
    with pytest.raises(le.LiveEntryRefused, match="series approval"):
        le.launch(**L.entry_kwargs(), clock_dir=L.clock_dir)
    assert built == [] and dr.LIVE_EXECUTION_ENABLED is False


def test_when_enabled_the_backend_is_built_only_after_every_check_passed(tmp_path, monkeypatch):
    L = good(tmp_path)
    order = []
    real_verify = le.verify_entry

    def spy_verify(**kw):
        plan = real_verify(**kw)
        order.append(("verify", plan.ok))
        return plan

    def build():
        order.append("backend")
        raise Reached

    enable(monkeypatch)
    monkeypatch.setattr(le, "verify_entry", spy_verify)
    monkeypatch.setattr(le, "_construct_live_backend", build)
    with pytest.raises(Reached):
        le.launch(**L.entry_kwargs(), clock_dir=L.clock_dir)
    assert order == [("verify", True), "backend"] and dr.LIVE_EXECUTION_ENABLED is False


@pytest.mark.parametrize("bad", ["fake", "subclass", "undeclared"])
def test_when_enabled_a_fake_or_look_alike_backend_is_refused_and_the_flag_stays_off(tmp_path, monkeypatch, bad):
    class Sub(p1b.Backend):
        pass

    L = good(tmp_path)
    enable(monkeypatch)
    monkeypatch.setattr(le, "_construct_live_backend", lambda: {"fake": s2.FakeLaunchBackend(), "subclass": object.__new__(Sub), "undeclared": object()}[bad])
    seen = []
    monkeypatch.setattr(ls, "run_launch", lambda *a, **kw: seen.append(1))
    with pytest.raises(le.LiveEntryRefused, match="exact p1b.Backend"):
        le.launch(**L.entry_kwargs(), clock_dir=L.clock_dir)
    assert seen == [] and dr.LIVE_EXECUTION_ENABLED is False


def test_when_enabled_the_orchestrator_runs_in_live_mode_with_the_flag_on_only_inside_the_context(tmp_path, monkeypatch):
    L = good(tmp_path)
    be = object.__new__(p1b.Backend)
    seen = {}

    def fake_run_launch(session, rows, stage, approval, auth, pins, manifest, **kw):
        seen.update(flag=dr.LIVE_EXECUTION_ENABLED, mode=kw["mode"], be=session.be, rows=rows, root=session.ctx.root.name, snap=session.ctx.win_snapshot)
        return {"status": "STUB"}

    enable(monkeypatch)
    monkeypatch.setattr(le, "_construct_live_backend", lambda: be)
    monkeypatch.setattr(le, "_load_env_doc", lambda ctx: {"chaincode": {"name": "e1asset"}})
    monkeypatch.setattr(ls, "run_launch", fake_run_launch)
    assert dr.LIVE_EXECUTION_ENABLED is False
    out = le.launch(**L.entry_kwargs(), clock_dir=L.clock_dir)
    assert out == {"status": "STUB"} and seen["flag"] is True and seen["mode"] == ls.MODE_LIVE and seen["be"] is be and seen["root"] == L.launch_id
    assert seen["snap"] == L.pins.snapshot.path_win and [r.run_id for r in seen["rows"]] == ["rp90"]
    assert dr.LIVE_EXECUTION_ENABLED is False, "restored after the launch"


def test_the_live_context_holds_concrete_paths_so_the_frozen_gates_can_call_exists_and_mkdir():
    """Edit N9b (2026-11-15). A read-only probe inside WSL showed that gate S11 raised AttributeError on the PurePosixPath context `driver.posix_ctx` returns, which would have failed the
    live pre-flight and consumed the launch id. The live entry builds its context through `_live_ctx`, whose path fields are concrete."""
    pure = dr.posix_ctx("p4r-dry-01")
    assert isinstance(pure.root, PurePosixPath) and not hasattr(pure.root, "exists")        # the offline listing context is unchanged
    with pytest.raises(AttributeError):
        p1b.gate_s11(pure, li.dry_rows())                                                    # the defect: a pure path has no exists()
    snap = r"D:\PLAN_BLOCK18\evidence_archive\ARCHIVE_20261114T113430Z"
    ctx = le._live_ctx("p4r-dry-01", snap)
    for name in ("repo", "e1_home", "root", "net", "env", "caliper_ws", "matrix"):
        value = getattr(ctx, name)
        assert type(value) is type(Path()) and hasattr(value, "exists") and hasattr(value, "mkdir"), name
    assert ctx.root.name == "p4r-dry-01" and ctx.win_snapshot == snap and ctx.py == "python3" and ctx.fabric_bin == pure.fabric_bin
    assert ctx.root.parent.name == "p4_replay_series" and ctx.caliper_ws.name == "caliper-ws-benchmark" and ctx.net.name == "network-e1-recovery.json"
    assert p1b.gate_s11(ctx, li.dry_rows()).status == "PASS"                                 # nothing exists under this launch id here, and the gate no longer raises


def test_the_live_flag_is_restored_after_an_exception_and_after_an_interrupt():
    for exc in (RuntimeError, KeyboardInterrupt):
        with pytest.raises(exc):
            with le._live_execution_enabled():
                assert dr.LIVE_EXECUTION_ENABLED is True
                raise exc
        assert dr.LIVE_EXECUTION_ENABLED is False


# ----------------------------------------------------------------------------------------------- static properties of the package
def test_only_the_live_entry_ever_assigns_the_live_flag_and_driver_keeps_it_false():
    assigners = []
    for p in NON_TEST:
        for node in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if (isinstance(t, ast.Attribute) and t.attr == "LIVE_EXECUTION_ENABLED") or (isinstance(t, ast.Name) and t.id == "LIVE_EXECUTION_ENABLED"):
                        assigners.append((p.name, isinstance(t, ast.Name), getattr(node.value, "value", "<expr>")))
    assert sorted(assigners, key=str) == [("driver.py", True, False), ("live_entry.py", False, "<expr>"), ("live_entry.py", False, True)], assigners
    tree = ast.parse((PKG / "live_entry.py").read_text(encoding="utf-8"))
    live = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "_live_execution_enabled"]
    assert len(live) == 1, "both attribute assignments sit inside the one context manager"
    inside = {id(n) for n in ast.walk(live[0])}
    sites = [n for n in ast.walk(tree) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Attribute) and t.attr == "LIVE_EXECUTION_ENABLED" for t in n.targets)]
    assert len(sites) == 2 and all(id(n) in inside for n in sites)


def test_a_real_backend_is_constructed_in_exactly_one_place():
    sites = []
    for p in NON_TEST:
        for node in ast.walk(ast.parse(p.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "Backend":
                sites.append(p.name)
    assert sites == ["live_entry.py"]


def test_no_phase_2c_module_runs_a_process_or_touches_docker_wsl_or_the_network_at_import_or_by_default():
    banned = {"subprocess", "socket", "urllib", "requests", "http", "asyncio", "multiprocessing"}
    for name in ("clock_evidence", "replay_fidelity", "replay_classify", "gate_r4", "duration_model", "analysis_primary", "pins", "launch_ids", "launch_approval"):
        tree = ast.parse((PKG / f"{name}.py").read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert not (banned & imported), (name, banned & imported)
        calls = {f"{n.func.value.id}.{n.func.attr}" for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)}
        assert not ({"os.system", "os.popen", "os.startfile", "os.spawnl", "os.execv"} & calls), name
