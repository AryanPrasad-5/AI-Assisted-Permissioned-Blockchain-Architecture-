"""A0w clock-only live route: safety and behavior tests (2026-11-22). Everything here is SYNTHETIC.

No process is ever started: ``subprocess.run`` and ``subprocess.Popen`` are replaced by functions that fail the test, and the round is driven by an injected fake runner. The one
test that looks at ``real_process_run`` replaces ``subprocess.run`` with a recorder and inspects the keyword arguments it would have received.
"""

from __future__ import annotations

import ast
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from fabric_perf.controllers import bounded_execution as be
from fabric_perf.controllers import clock_only_live as col
from fabric_perf.controllers import clock_only_readiness as cor
from fabric_perf.controllers import clock_only_round as cor_round
from fabric_perf.controllers import clock_preflight as cp
from fabric_perf.controllers import prelive_integrity as pi
from fabric_perf.controllers import process_runner as pr
from fabric_perf.controllers import wsl_clock_probe as wp
from fabric_perf.controllers import wsl_transport as wt

REPO = Path(__file__).resolve().parents[1]
DISTRO = "Ubuntu-24.04"
NODE, OSV = "TESTHOST", "10.0.99999"
DISTROS = ["docker-desktop", DISTRO]
A0V_MANIFEST_SHA = "b4ae11c95f8267afc691862a5685cc26f8dd98db417187954051343d2817185c"


@pytest.fixture(autouse=True)
def no_process(monkeypatch):
    def refuse(*_a, **_k):
        raise AssertionError("a test tried to start a process")
    monkeypatch.setattr(subprocess, "run", refuse)
    monkeypatch.setattr(subprocess, "Popen", refuse)


class Env:
    pass


@pytest.fixture()
def env(tmp_path):
    e = Env()
    e.repo = tmp_path / "repo"
    for rel in col.SNAPSHOT_FILES:
        (e.repo / rel).parent.mkdir(parents=True, exist_ok=True)
        (e.repo / rel).write_text(f"# synthetic {rel}\n", encoding="utf-8")
    e.exe = tmp_path / "bin" / "wsl.exe"
    e.exe.parent.mkdir()
    e.exe.write_bytes(b"synthetic wsl.exe")
    e.manifest = e.repo / "docs" / "research" / "e1" / "prelive_candidate_manifest_synthetic.json"
    e.manifest.parent.mkdir(parents=True, exist_ok=True)
    write_manifest(e)
    e.out = e.repo / col.EVIDENCE_ROOT / "round_01"
    e.doc = tmp_path / "approval.md"
    e.tmp = tmp_path
    e.record = col.build_record(repo_root=e.repo, distro=DISTRO, wsl_exe=str(e.exe), manifest=e.manifest, out_dir=e.out, node_name=NODE, os_version=OSV, distros=DISTROS)
    e.sealed = seal(e, e.record)
    return e


def write_manifest(e):
    doc = {"schema": pi.MANIFEST_SCHEMA, "status": pi.STATUS_CANDIDATE, "approved": False,
           "entries": [{"path": rel, "sha256": pi.sha256_file(e.repo / rel), "pinned_sha256": None} for rel in col.SNAPSHOT_FILES]}
    e.manifest.write_text(json.dumps(doc), encoding="utf-8")


def seal(e, record):
    e.doc.write_text(f"The owner approves the clock-only record {record['record_sha256']}.\n", encoding="utf-8")
    return col.seal_record(record, e.doc, "synthetic owner")


def run_preflight(e, record=None, **over):
    kw = dict(repo_root=e.repo, manifest_path=e.manifest, node_name=NODE, os_version=OSV, distros=DISTROS)
    kw.update(over)
    return col.preflight(record if record is not None else e.sealed, **kw)


def refused_gate(e, record=None, **over):
    with pytest.raises(col.ClockOnlyRefused) as info:
        run_preflight(e, record, **over)
    return info.value


class FakeClock:
    def __init__(self):
        self.t = 0

    def wall(self):
        self.t += 1_000
        return 1_700_000_000_000_000_000 + self.t

    def mono(self):
        self.t += 1_000
        return 5_000_000_000 + self.t

    def advance(self, ns):
        self.t += ns


def make_runner(clock, *, behavior=None, calls=None):
    """A fake process runner: records argv/timeout; ``behavior(n)`` may return a ProcOutcome, raise, or None for a valid reading."""
    counter = [0]

    def runner(argv, timeout_s):
        counter[0] += 1
        n = counter[0]
        if calls is not None:
            calls.append((list(argv), timeout_s))
        clock.advance(2_000_000)
        outcome = behavior(n) if behavior else None
        if outcome is not None:
            return outcome
        return col.ProcOutcome(0, b"%019d\n" % (1_700_000_000_000_000_000 + clock.t + 500_000), b"", False)
    return runner


def run_round(e, runner, **kw):
    pre = run_preflight(e)
    clock = kw.pop("clock")
    return col.execute(e.sealed, pre, run_process=runner, wall_ns=clock.wall, mono_ns=clock.mono, sleep=lambda s: kw.setdefault("sleeps", []).append(s) or None,
                       now_utc=lambda: datetime(2026, 11, 22, tzinfo=timezone.utc), **{k: v for k, v in kw.items() if k != "sleeps"})


# ---------------------------------------------------------------------------------------------------------- 1-4 allowlist and prohibited programs
def test_only_one_vector_exists_and_it_is_the_clock_command(env):
    vec = col.allowed_vector(DISTRO, timeout_s=10.0, uses=40)
    assert vec.os_argv == ("wsl.exe", "-d", DISTRO, "--exec", "date", "+%s%N") and vec.klass == "read_only_observation" and vec.wsl_cwd is None
    assert col.spawn_argv(str(env.exe), DISTRO) == [str(env.exe), "-d", DISTRO, "--exec", "date", "+%s%N"]


def test_module_has_exactly_one_process_call_and_no_forbidden_program_names():
    src = (REPO / "src/fabric_perf/controllers/clock_only_live.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name) and n.func.value.id == "subprocess"
             and n.func.attr in ("run", "Popen", "call", "check_call", "check_output")]
    assert len(calls) == 1 and calls[0].func.attr == "run"
    assert not any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in ("system", "popen", "spawnv", "startfile") for n in ast.walk(tree))
    assert "shell=True" not in src and "os.environ" not in src and "getenv" not in src
    literals = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and len(n.value) < 40]
    for banned in ("docker", "caliper", "peer", "orderer", "configtxlator", "prometheus", "curl", "bash", "powershell"):
        assert banned not in [s.lower() for s in literals], banned


@pytest.mark.parametrize("category", [wt.OPC_CALIPER, wt.OPC_DISCOVERY, wt.OPC_X13])
def test_other_operation_categories_are_refused(env, category):
    rec = json.loads(json.dumps(env.sealed))
    rec["operation_category"] = category
    assert refused_gate(env, rec).gate == "gate3_scope"


def test_tampered_vector_set_that_names_docker_is_refused_and_nothing_is_started(env):
    rec = json.loads(json.dumps(env.sealed))
    rec["allowed_vectors"][0]["os_argv"] = ["wsl.exe", "-d", DISTRO, "--exec", "docker", "ps"]
    exc = refused_gate(env, rec)
    assert exc.gate == "gate5_authorization" and {"vector_set_mismatch", "record_hash"} & {f["code"] for f in exc.findings}


def test_validate_argv_refuses_shell_and_non_wsl_vectors_used_by_this_route():
    with pytest.raises(pr.ArgvRefused):
        pr.validate_argv(["C:\\Windows\\System32\\wsl.exe", "-d", DISTRO, "--exec", "bash", "-c", "date"])
    with pytest.raises(pr.ArgvRefused):
        pr.validate_argv(["C:\\Windows\\System32\\wsl.exe", "-d", DISTRO, "date"])


# ---------------------------------------------------------------------------------------------------------- 5-12 binding, authorization, integrity
def test_record_verifies_when_everything_matches(env):
    pre = run_preflight(env)
    assert set(pre["gates"].values()) == {"PASS"} and pre["record_sha256"] == env.sealed["record_sha256"]


def test_wrong_host_is_refused(env):
    assert refused_gate(env, node_name="OTHERHOST").gate == "gate2_runtime_identity"
    assert refused_gate(env, os_version="10.0.1").gate == "gate2_runtime_identity"


def test_wrong_or_unregistered_distribution_is_refused(env):
    assert refused_gate(env, distros=["docker-desktop"]).gate == "gate2_runtime_identity"
    rec = json.loads(json.dumps(env.sealed))
    rec["distro"] = "docker-desktop"
    assert refused_gate(env, rec).gate == "gate5_authorization"


def test_changed_wsl_exe_is_refused(env):
    env.exe.write_bytes(b"a different binary")
    exc = refused_gate(env)
    assert exc.gate == "gate5_authorization" and "binding_mismatch" in {f["code"] for f in exc.findings}


def test_wrong_executable_path_is_refused(env):
    other = env.tmp / "bin2" / "wsl.exe"
    other.parent.mkdir()
    other.write_bytes(b"synthetic wsl.exe")
    rec = json.loads(json.dumps(env.sealed))
    rec["executable"] = str(other)
    assert refused_gate(env, rec).gate == "gate5_authorization"


def test_changed_manifest_digest_is_refused(env):
    env.manifest.write_text(env.manifest.read_text(encoding="utf-8") + " ", encoding="utf-8")
    exc = refused_gate(env)
    assert exc.gate == "gate5_authorization" and "candidate_manifest_mismatch" in {f["code"] for f in exc.findings}


def test_changed_source_file_is_refused_at_the_integrity_gate(env):
    (env.repo / col.SNAPSHOT_FILES[0]).write_text("# edited after the manifest\n", encoding="utf-8")
    exc = refused_gate(env)
    assert exc.gate == "gate1_integrity" and "modified" in {f["code"] for f in exc.findings}


def test_missing_manifest_and_unlisted_snapshot_file_are_refused(env):
    assert refused_gate(env, manifest_path=env.tmp / "nope.json").gate == "gate1_integrity"
    doc = json.loads(env.manifest.read_text(encoding="utf-8"))
    doc["entries"] = doc["entries"][1:]
    env.manifest.write_text(json.dumps(doc), encoding="utf-8")
    assert refused_gate(env).gate == "gate1_integrity"


def test_missing_authorization_is_refused(env):
    assert refused_gate(env, record=None if False else {}).gate == "gate5_authorization"
    unsealed = json.loads(json.dumps(env.record))
    exc = refused_gate(env, unsealed)
    assert exc.gate == "gate5_authorization" and "approval_unset" in {f["code"] for f in exc.findings}


def test_changed_record_after_sealing_is_refused(env):
    rec = json.loads(json.dumps(env.sealed))
    rec["activation_reference"] = "something else"
    assert "record_hash" in {f["code"] for f in refused_gate(env, rec).findings}
    rec2 = json.loads(json.dumps(env.sealed))
    rec2["max_capture_bytes"] = 1 << 30
    assert refused_gate(env, rec2).gate == "gate5_authorization"


def test_approval_document_must_pin_the_record_and_stay_unchanged(env):
    env.doc.write_text("approves nothing in particular\n", encoding="utf-8")
    codes = {f["code"] for f in refused_gate(env).findings}
    assert {"approval_record_hash"} <= codes
    with pytest.raises(col.ClockOnlyRefused):
        col.seal_record(env.record, env.doc, "owner")


def test_real_spawn_authorized_false_is_refused(env):
    rec = json.loads(json.dumps(env.record))
    rec["real_spawn_authorized"] = False
    rec["record_sha256"] = be._digest(rec, drop=("record_sha256", "approval"))
    sealed = seal(env, rec)
    assert "real_spawn_not_authorized" in {f["code"] for f in refused_gate(env, sealed).findings}


# ---------------------------------------------------------------------------------------------------------- 13-14 bounds
@pytest.mark.parametrize("key,value", [("samples_per_phase", 21), ("probe_timeout_s", 11.0), ("phase_gap_s", 6.0), ("max_total_runtime_s", 421.0), ("max_process_starts", 41),
                                       ("max_process_starts", 38)])
def test_schedule_beyond_the_caps_is_refused(env, key, value):
    sched = dict(col.SCHEDULE)
    sched[key] = value
    binding = dict(env.record["binding"], schedule=sched)
    assert col.check_scope(binding), key


def test_output_directory_must_be_new_and_under_the_evidence_root(env):
    env.out.mkdir(parents=True)
    assert refused_gate(env).gate == "gate6_output_isolation"
    bad = col.build_record(repo_root=env.repo, distro=DISTRO, wsl_exe=str(env.exe), manifest=env.manifest, out_dir=env.tmp / "elsewhere", node_name=NODE, os_version=OSV, distros=DISTROS)
    assert "output_dir" in {f["code"] for f in col.check_scope(bad["binding"])}


def test_complete_round_starts_exactly_forty_processes_each_bounded_by_the_timeout(env):
    calls, clock = [], FakeClock()
    status = run_round(env, make_runner(clock, calls=calls), clock=clock)
    assert status["status"] == "COMPLETED" and len(calls) == 40 and status["process_starts"] == 40
    assert {t for _, t in calls} == {10.0}
    assert all(a == col.spawn_argv(str(env.exe), DISTRO) for a, _ in calls)


# ---------------------------------------------------------------------------------------------------------- 15-17 retry, timestamps, stdout
def test_failed_commands_are_never_retried_and_three_consecutive_failures_stop_the_round(env):
    calls, clock = [], FakeClock()
    status = run_round(env, make_runner(clock, calls=calls, behavior=lambda n: col.ProcOutcome(1, b"", b"boom", False)), clock=clock)
    assert len(calls) == 3 and status["stop_reason"] == "3_consecutive_failures"
    log = json.loads((env.out / "process_logs" / "process_log.json").read_text(encoding="utf-8"))
    assert log["retried_any"] is False and [p["retried"] for p in log["processes"]] == [False] * 3
    outcome = json.loads((env.out / "run_outcome.json").read_text(encoding="utf-8"))
    assert outcome["class"] != "REAL_CLOCK_EVIDENCE_COMPLETE"


def test_isolated_failures_do_not_add_starts(env):
    calls, clock = [], FakeClock()
    status = run_round(env, make_runner(clock, calls=calls, behavior=lambda n: col.ProcOutcome(None, b"", b"", True) if n in (2, 30) else None), clock=clock)
    assert len(calls) == 40 and status["status"] == "COMPLETED"
    pre = json.loads((env.out / "clock" / "pre_run.json").read_text(encoding="utf-8"))
    assert pre["invalid"][0]["code"] == "remote_command_timeout" and pre["count_collected"] == 19


@pytest.mark.parametrize("stdout,code", [(b"123\n", "remote_unit_unexpected"), (b"%N\n", "remote_reading_malformed"), (b"", "remote_reading_missing"),
                                         (b"1700000000000000000x\n", "remote_reading_malformed"), (b"1700000000\n", "remote_unit_unexpected"),
                                         (b"1700000000000000000\n1\n2\n", "remote_reading_malformed"), (b"1700000000000000000\n1700000000000000001\n", "remote_reading_malformed")])
def test_invalid_stdout_is_recorded_not_repaired(env, stdout, code):
    clock = FakeClock()
    run_round(env, make_runner(clock, behavior=lambda n: col.ProcOutcome(0, stdout, b"", False) if n == 1 else None), clock=clock)
    pre = json.loads((env.out / "clock" / "pre_run.json").read_text(encoding="utf-8"))
    assert pre["invalid"][0]["code"] == code and pre["count_collected"] == 19


def test_non_monotonic_or_backwards_clock_is_invalid(env):
    class Odd(FakeClock):
        def mono(self):
            self.t += 1_000
            return 5_000_000_000                                                         # never advances
    clock = Odd()
    run_round(env, make_runner(clock), clock=clock)
    pre = json.loads((env.out / "clock" / "pre_run.json").read_text(encoding="utf-8"))
    assert pre["count_collected"] == 0 and {i["code"] for i in pre["invalid"]} == {"monotonic_not_advancing"}


# ---------------------------------------------------------------------------------------------------------- 18-20 partial evidence, A0e, tolerance
def test_interrupt_preserves_partial_evidence_and_reraises(env):
    clock = FakeClock()

    def behavior(n):
        if n == 25:
            raise KeyboardInterrupt()
        return None
    with pytest.raises(KeyboardInterrupt):
        run_round(env, make_runner(clock, behavior=behavior), clock=clock)
    post = json.loads((env.out / "clock" / "post_run.json").read_text(encoding="utf-8"))
    assert post["count_collected"] == 4 and (env.out / "clock" / "pre_run.json").is_file()
    status = json.loads((env.out / "round_status.json").read_text(encoding="utf-8"))
    assert status["status"] == "INTERRUPTED" and (env.out / "integrity_manifest.json").is_file() and (env.out / "run_outcome.json").is_file()


def test_stored_samples_are_re_evaluated_identically_by_the_unchanged_a0e_evaluator(env):
    clock = FakeClock()
    run_round(env, make_runner(clock), clock=clock)
    diag = env.sealed["binding"]["diagnostic_evaluator_params"]
    plan = {"request": {"tolerance_ms": diag["tolerance_ms"], "min_valid_samples": diag["min_valid_samples"], "max_wall_monotonic_disagreement_ms": diag["max_wall_monotonic_disagreement_ms"]}}
    result = cor.assess_round_directory(env.out, plan)
    assert result["verdicts_equal"] and result["sample_order_valid"] and result["raw_samples_preserved"]
    verdict = json.loads((env.out / "clock" / "clock_verdict.json").read_text(encoding="utf-8"))
    assert verdict["parameters_label"] == col.DIAGNOSTIC_LABEL and verdict["version"] == cp.CLOCK_PREFLIGHT_VERSION
    analysis = json.loads((env.out / "clock" / "analysis.json").read_text(encoding="utf-8"))
    assert set(analysis["phases"]) == {"pre_run", "post_run"} and "phase_to_phase" in analysis
    # the synthetic reading sits 0.5 ms after the call: the offset interval brackets +0.5 ms plus half the round trip
    assert 0 < analysis["phases"]["pre_run"]["offset_ms"]["median"] < 5


def test_no_tolerance_is_approved_by_this_route(env):
    assert wp.LIVE_CLOCK_SAMPLING_ENABLED is False
    binding = dict(env.record["binding"])
    binding["diagnostic_evaluator_params"] = {k: v for k, v in binding["diagnostic_evaluator_params"].items() if k != "label"}
    assert "diagnostic_params" in {f["code"] for f in col.check_scope(binding)}
    clock = FakeClock()
    run_round(env, make_runner(clock), clock=clock)
    for p in (env.out).rglob("*.json"):
        text = p.read_text(encoding="utf-8")
        assert "tolerance_approved" not in text and "accepted_tolerance" not in text


# ---------------------------------------------------------------------------------------------------------- 21-24 isolation, history, fixture separation
def test_real_process_call_is_structured_and_bounded(monkeypatch):
    seen = {}

    class Done:
        returncode, stdout, stderr = 0, b"1700000000000000000\n", b""

    def recorder(argv, **kwargs):
        seen["argv"], seen["kwargs"] = argv, kwargs
        return Done()
    monkeypatch.setattr(subprocess, "run", recorder)
    out = col.real_process_run(["C:\\Windows\\System32\\wsl.exe", "-d", DISTRO, "--exec", "date", "+%s%N"], 10.0)
    assert out.returncode == 0 and isinstance(seen["argv"], list)
    assert seen["kwargs"]["shell"] is False and seen["kwargs"]["timeout"] == 10.0 and seen["kwargs"]["stdin"] == subprocess.DEVNULL and seen["kwargs"]["capture_output"] is True


def test_real_process_call_reports_a_timeout_instead_of_raising(monkeypatch):
    def recorder(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"], output=b"x", stderr=b"y")
    monkeypatch.setattr(subprocess, "run", recorder)
    out = col.real_process_run(["wsl.exe"], 1.0)
    assert out.timed_out and out.returncode is None and out.stdout == b"x"


def test_execute_needs_a_passing_preflight_for_the_same_record(env):
    clock = FakeClock()
    with pytest.raises(col.ClockOnlyRefused):
        col.execute(env.sealed, {"gates": {}}, run_process=make_runner(clock), wall_ns=clock.wall, mono_ns=clock.mono, sleep=lambda s: None)
    pre = run_preflight(env)
    pre["record_sha256"] = "0" * 64
    with pytest.raises(col.ClockOnlyRefused):
        col.execute(env.sealed, pre, run_process=make_runner(clock), wall_ns=clock.wall, mono_ns=clock.mono, sleep=lambda s: None)
    assert not env.out.exists()


def test_injected_runner_output_is_labelled_simulated_never_real_evidence(env):
    clock = FakeClock()
    run_round(env, make_runner(clock), clock=clock)
    outcome = json.loads((env.out / "run_outcome.json").read_text(encoding="utf-8"))
    assert outcome["class"] == "SIMULATED_NOT_EVIDENCE" and outcome["real_process_runner"] is False
    assert "INJECTED TEST RUNNER" in json.loads((env.out / "clock" / "pre_run.json").read_text(encoding="utf-8"))["evidence_origin"]
    assert col.classify_outcome(real=True, starts=40, planned=40, valid_by_phase={"pre_run": 20, "post_run": 20}, min_valid=10, stop_reason=None, write_errors=0)["class"] == "REAL_CLOCK_EVIDENCE_COMPLETE"
    assert col.classify_outcome(real=True, starts=40, planned=40, valid_by_phase={"pre_run": 20, "post_run": 3}, min_valid=10, stop_reason=None, write_errors=0)["class"] == "REAL_CLOCK_EVIDENCE_PARTIAL"
    assert col.classify_outcome(real=True, starts=0, planned=40, valid_by_phase={}, min_valid=10, stop_reason=None, write_errors=0)["class"] == "NO_REAL_EVIDENCE"


def test_integrity_manifest_lists_every_file_and_hashes_match(env):
    clock = FakeClock()
    run_round(env, make_runner(clock), clock=clock)
    manifest = json.loads((env.out / "integrity_manifest.json").read_text(encoding="utf-8"))["files"]
    on_disk = {str(p.relative_to(env.out)).replace("\\", "/") for p in env.out.rglob("*") if p.is_file()} - {"integrity_manifest.json"}
    assert set(manifest) == on_disk
    assert all(hashlib.sha256((env.out / rel).read_bytes()).hexdigest() == h for rel, h in manifest.items())
    with pytest.raises(FileExistsError):                                                   # exclusive creation: nothing is overwritten
        be._create_new(env.out / "round_status.json", b"x")


def test_the_three_a0v_global_switches_and_the_executors_are_unchanged():
    from fabric_perf.controllers import bounded_execution, process_runner, wsl_clock_probe, wsl_transport
    assert (wsl_transport.REAL_SPAWN_COMPILED_IN, process_runner.REAL_TRANSPORT_ENABLED, wsl_clock_probe.LIVE_CLOCK_SAMPLING_ENABLED, bounded_execution.REAL_LAUNCH_ENABLED) == (False,) * 4
    manifest = json.loads((REPO / "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0v.json").read_text(encoding="utf-8"))
    listed = {e["path"]: e["sha256"] for e in manifest["entries"]}
    for rel in ("wsl_transport", "process_runner", "bounded_execution", "wsl_clock_probe", "clock_only_round", "clock_preflight", "clock_only_readiness", "run_outcome"):
        path = f"src/fabric_perf/controllers/{rel}.py"
        assert pi.sha256_file(REPO / path) == listed[path], path


def test_historical_a0v_manifest_is_byte_identical_and_still_not_approved():
    path = REPO / "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0v.json"
    assert pi.sha256_file(path) == A0V_MANIFEST_SHA
    doc = json.loads(path.read_text(encoding="utf-8"))
    assert doc["status"] == "CANDIDATE_NOT_APPROVED" and doc["approved"] is False


def test_frozen_experimental_artifacts_are_not_in_the_route():
    src = (REPO / "src/fabric_perf/controllers/clock_only_live.py").read_text(encoding="utf-8")
    for token in ("HFBTP", "MMBPD", ".joblib", "D:\\\\Plan_research", "Reseach_Model"):
        assert token not in src


# ---------------------------------------------------------------------------------------------------------- added after the mutation check
@pytest.mark.parametrize("key,value", [("samples_per_phase", 21), ("probe_timeout_s", 11.0), ("phase_gap_s", 6.0), ("max_total_runtime_s", 500.0)])
def test_a_validly_sealed_oversize_schedule_is_refused_by_preflight(env, key, value):
    sched = dict(col.SCHEDULE)
    sched[key] = value
    if key == "samples_per_phase":
        sched["max_process_starts"] = 42
    rec = col.build_record(repo_root=env.repo, distro=DISTRO, wsl_exe=str(env.exe), manifest=env.manifest, out_dir=env.out, node_name=NODE, os_version=OSV, distros=DISTROS, schedule=sched)
    exc = refused_gate(env, seal(env, rec))
    assert exc.gate == "gate3_scope"
    assert not env.out.exists()


def test_a_sealed_record_whose_output_is_under_another_trees_evidence_root_is_refused(env):
    other = env.tmp / "other_tree" / col.EVIDENCE_ROOT / "round_01"
    rec = col.build_record(repo_root=env.repo, distro=DISTRO, wsl_exe=str(env.exe), manifest=env.manifest, out_dir=other, node_name=NODE, os_version=OSV, distros=DISTROS)
    assert not col.check_scope(rec["binding"])                                            # the textual scope check passes ...
    exc = refused_gate(env, seal(env, rec))
    assert exc.gate == "gate6_output_isolation" and exc.findings[0]["code"] == "output_outside_evidence_root"
