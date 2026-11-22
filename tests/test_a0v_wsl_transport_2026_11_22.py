"""A0v Workstream B: the operation-scoped WSL process transport. Process-level behaviour (timeout, interrupt, exit status, capture, retry, lifetime) is exercised with HARMLESS LOCAL Python
commands in FIXTURE mode; the real-mode argument handling is exercised with an injected fake ``Popen`` that starts nothing. ``wsl.exe``, Docker, Caliper and Fabric are never started (the ``guard``
fixture fails any attempt)."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from tests import a0t_support as t
from tests import a0u_support as u
from tests import a0v_support as v
from tests.a0v_support import guard  # noqa: F401  (fixture)

from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import runtime_discovery as rd      # noqa: E402
from fabric_perf.controllers import wsl_transport as wt          # noqa: E402

pytestmark = pytest.mark.usefixtures("guard")


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("a0v_world")
    plan, _req, _root = u.build_plan(tmp)
    record = t.path_record(plan)
    addendum = u.make_addendum(plan, record)
    spec = rd.discovery_spec(plan, rd.scan_container_declarations(v.REPO), distro=v.DISTRO)
    return SimpleNamespace(tmp=tmp, plan=plan, record=record, addendum=addendum, spec=spec)


@pytest.fixture(autouse=True)
def quick_termination(monkeypatch):
    monkeypatch.setattr(wt, "TERMINATE_GRACE_S", 1.0)


def real_transport(monkeypatch, tmp_path, category, allowed, binding, popen):
    """A REAL-mode transport (compiled-in flag set only inside the test, fake Popen injected)."""
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    record = v.real_record(category, allowed, binding, tmp_path)
    permit = wt.issue_real_permit(record, category=category, allowed=allowed, binding=binding, expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    return wt.WslProcessTransport(permit, popen=popen)


# ---- 1. structured argument vectors are preserved token for token
def test_01_real_mode_spawns_the_explicit_executable_with_the_vector_tokens_unchanged_and_no_shell(world, monkeypatch, tmp_path):
    allowed = wt.vectors_for_category(wt.OPC_DISCOVERY, world.spec, distro=v.DISTRO, mount_root=v.MOUNT, container_name="caliper", step_timeout_s=10)
    fake = v.FakePopen()
    transport = real_transport(monkeypatch, tmp_path, wt.OPC_DISCOVERY, allowed, {"run_id": world.plan["run_id"]}, fake)
    d1 = next(a for a in allowed if a.step_id == "D1_list_containers")
    transport.run(v.invocation(d1))
    argv, kwargs = fake.calls[0]
    assert argv == [v.EXE] + list(d1.os_argv[1:]) and isinstance(argv, list)
    assert "{{.ID}} {{.Names}} {{.Status}} {{.Image}}" in argv                       # a token with spaces and braces stays ONE token
    assert kwargs["shell"] is False and kwargs["stdin"] == subprocess.DEVNULL and kwargs["close_fds"] is True
    assert transport.kind == "process" and transport.evidence[0]["spawned_argv"][0] == v.EXE


# ---- 2, 3. explicit distribution; missing distribution
def test_02_the_distribution_is_explicit_in_every_vector_and_in_the_record(world, tmp_path):
    allowed = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)
    assert allowed and all(a.os_argv[:3] == ("wsl.exe", "-d", v.DISTRO) for a in allowed)
    other = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro="Debian", mount_root=v.MOUNT)
    assert all(a.os_argv[2] == "Debian" for a in other) and wt.vectors_digest(other) != wt.vectors_digest(allowed)
    record = v.real_record(wt.OPC_X13, allowed, {"run_id": "r"}, tmp_path, distro="Debian")
    findings = wt.verify_activation_record(record, category=wt.OPC_X13, allowed=allowed, binding={"run_id": "r"}, expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    assert "distro_mismatch" in {f["code"] for f in findings}


@pytest.mark.parametrize("bad", [None, "", "Ub untu", "a" * 65, 7])
def test_03_a_missing_or_malformed_distribution_is_refused(world, bad):
    with pytest.raises(wt.ActivationRefused) as info:
        wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=bad, mount_root=v.MOUNT)
    assert info.value.code == "distro"


def test_03_a_record_without_a_distribution_does_not_verify(world, tmp_path):
    allowed = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)
    record = v.real_record(wt.OPC_X13, allowed, {"run_id": "r"}, tmp_path, distro=None)
    codes = {f["code"] for f in wt.verify_activation_record(record, category=wt.OPC_X13, allowed=allowed, binding={"run_id": "r"}, expected_candidate_manifest_sha256=v.MANIFEST_SHA)}
    assert {"owner_field_unset", "distro"} <= codes


# ---- 4. unapproved executable
@pytest.mark.parametrize("exe", ["wsl.exe", "C:\\Windows\\System32\\cmd.exe", "C:\\Windows\\System32\\wsl.exe.bak", "/usr/bin/wsl.exe", "..\\wsl.exe", "C:\\x\\..\\wsl.exe"])
def test_04_only_an_explicit_absolute_windows_path_to_wsl_exe_is_accepted_in_a_record(world, tmp_path, exe):
    allowed = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)
    record = v.real_record(wt.OPC_X13, allowed, {"run_id": "r"}, tmp_path, executable=exe)
    codes = {f["code"] for f in wt.verify_activation_record(record, category=wt.OPC_X13, allowed=allowed, binding={"run_id": "r"}, expected_candidate_manifest_sha256=v.MANIFEST_SHA)}
    assert "executable" in codes


@pytest.mark.parametrize("name", ["wsl.exe", "docker", "bash", "powershell.exe", "caliper", "C:\\Windows\\System32\\wsl.exe", "wslconfig.exe"])
def test_04_a_fixture_permit_refuses_wsl_shells_and_tools_as_its_executable(name):
    with pytest.raises(wt.ActivationRefused) as info:
        wt.issue_fixture_permit(wt.OPC_CLOCK, [v.local_vector()], executable=name, max_lifetime_s=5, acknowledge_fixture_only=True)
    assert info.value.code == "fixture_executable_forbidden"


def test_04_a_fixture_command_naming_a_forbidden_tool_is_refused_at_start():
    permit = wt.issue_fixture_permit(wt.OPC_CLOCK, [v.local_vector()], executable=sys.executable, max_lifetime_s=5, acknowledge_fixture_only=True)
    transport = wt.WslProcessTransport(permit, command_for=lambda inv: ["docker", "ps"])
    with pytest.raises(pr.RunnerRefused) as info:
        transport.run(v.invocation(permit.allowed[0]))
    assert info.value.code == "fixture_command_forbidden" and transport.evidence == []


def test_04_a_fixture_permit_needs_an_explicit_acknowledgement():
    with pytest.raises(wt.ActivationRefused) as info:
        wt.issue_fixture_permit(wt.OPC_CLOCK, [v.local_vector()], executable=sys.executable, max_lifetime_s=5)
    assert info.value.code == "fixture_not_acknowledged"


def test_04_a_transport_cannot_be_built_without_a_permit_issued_by_this_module():
    with pytest.raises(wt.ActivationRefused) as info:
        wt.WslProcessTransport(object())                       # type: ignore[arg-type]
    assert info.value.code == "permit_required"
    forged = wt.SpawnPermit(wt.MODE_REAL, wt.OPC_CLOCK, v.EXE, v.DISTRO, (), 5.0, 1, 100, None)
    with pytest.raises(wt.ActivationRefused):
        wt.WslProcessTransport(forged)


# ---- 5. shell injection and arbitrary scripts
@pytest.mark.parametrize("tokens, code", [
    (("wsl.exe", "-d", "Ubuntu", "--exec", "date", "+%s%N;rm"), "shell_metacharacter_or_quote"),
    (("wsl.exe", "-d", "Ubuntu", "--exec", "date", "$(id)"), "shell_metacharacter_or_quote"),
    (("wsl.exe", "-d", "Ubuntu", "--exec", "date", "a|b"), "shell_metacharacter_or_quote"),
    (("wsl.exe", "-d", "Ubuntu", "--exec", "bash", "-c", "x"), "shell_executable"),
    (("wsl.exe", "-d", "Ubuntu", "--exec", "python3", "-c", "import os"), "inline_code"),
    (("wsl.exe", "-d", "Ubuntu", "date"), "wsl_without_exec"),
    (("wsl.exe", "-d", "Ubuntu", "--exec", "cat", "../../etc/passwd"), "path_traversal"),
])
def test_05_shell_injection_and_inline_scripts_are_rejected_before_any_table_lookup(tokens, code):
    transport = v.fixture_transport(wt.OPC_CLOCK, [v.local_vector()], lambda inv: "pass")
    inv = v.invocation(v.local_vector(), os_argv=tokens)
    assert transport.problems(inv)[0]["code"] == code
    with pytest.raises(pr.RunnerRefused) as info:
        transport.run(inv)
    assert info.value.code == code and transport.evidence == []


def test_05_a_single_string_is_never_a_command():
    transport = v.fixture_transport(wt.OPC_CLOCK, [v.local_vector()], lambda inv: "pass")
    inv = v.invocation(v.local_vector(), os_argv="wsl.exe -d Ubuntu --exec date")
    assert transport.problems(inv)[0]["code"] == "argv_not_a_vector"


def test_05_a_vector_that_is_well_formed_but_not_listed_is_refused():
    transport = v.fixture_transport(wt.OPC_CLOCK, [v.local_vector()], lambda inv: "pass")
    inv = v.invocation(v.local_vector(), os_argv=("wsl.exe", "-d", "Ubuntu", "--exec", "rm", "-rf", "/tmp/x"))
    assert transport.problems(inv)[0]["code"] == "vector_not_permitted"


# ---- 6. wrong working directory
def test_06_a_wrong_working_directory_is_refused(world):
    allowed = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)
    transport = v.fixture_transport(wt.OPC_X13, allowed, lambda inv: "pass")
    vector = allowed[0]
    assert vector.wsl_cwd and transport.problems(v.invocation(vector)) == []
    assert transport.problems(v.invocation(vector, wsl_cwd="/tmp"))[0]["code"] == "cwd_not_approved"
    assert transport.problems(v.invocation(vector, wsl_cwd=None))[0]["code"] == "cwd_not_approved"


# ---- 7. wrong operation category
def test_07_each_category_lists_only_its_own_vectors_and_the_sets_are_disjoint(world):
    clock_plan = v.clock_plan(world.tmp / "c1")
    sets = {wt.OPC_CLOCK: v.clock_allowed(clock_plan),
            wt.OPC_DISCOVERY: wt.vectors_for_category(wt.OPC_DISCOVERY, world.spec, distro=v.DISTRO, mount_root=v.MOUNT, container_name="caliper", step_timeout_s=10),
            wt.OPC_X13: wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT),
            wt.OPC_CALIPER: wt.vectors_for_category(wt.OPC_CALIPER, world.plan, distro=v.DISTRO, mount_root=v.MOUNT)}
    read_only = (wt.OPC_CLOCK, wt.OPC_DISCOVERY, wt.OPC_X13)
    for first in read_only:                                        # the three read-only categories never share a vector with each other
        for second in read_only:
            if first < second:
                assert not ({a.os_argv for a in sets[first]} & {a.os_argv for a in sets[second]}), (first, second)
    stronger = {a.os_argv for a in sets[wt.OPC_CALIPER] if a.klass in ("transaction_producing", "local_staging")}
    assert stronger and not any(a.os_argv in stronger for c in read_only for a in sets[c])          # no read-only category can start a staging or transaction step
    assert not any(a.klass != "read_only_observation" for c in read_only for a in sets[c])
    assert any(a.klass == "transaction_producing" for a in sets[wt.OPC_CALIPER])
    # the only overlap allowed: the bounded run's own clock gate, which is also the clock-only probe vector
    overlap = {a.step_id for a in sets[wt.OPC_CALIPER] if a.os_argv in {b.os_argv for c in read_only for b in sets[c]}}
    assert overlap <= {"X03_clock_pre", "X11_clock_post"}, overlap


def test_07_a_transport_of_one_category_refuses_a_vector_of_another(world):
    clock = v.fixture_transport(wt.OPC_CLOCK, v.clock_allowed(v.clock_plan(world.tmp / "c2")), lambda inv: "pass")
    x13 = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)[0]
    assert clock.problems(v.invocation(x13))[0]["code"] == "vector_not_permitted"
    with pytest.raises(wt.ActivationRefused) as info:
        wt.vectors_for_category("generic_shell", world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)
    assert info.value.code == "operation_category"
    assert set(wt.CATEGORIES) == {wt.OPC_CLOCK, wt.OPC_DISCOVERY, wt.OPC_X13, wt.OPC_CALIPER}


def test_07_the_wrong_source_document_for_a_category_is_refused(world):
    with pytest.raises(wt.ActivationRefused) as info:
        wt.vectors_for_category(wt.OPC_CLOCK, world.plan, distro=v.DISTRO, mount_root=v.MOUNT)
    assert info.value.code == "source_schema"


# ---- 8. approval record binding is tested in test_a0v_authorization; here the permit consequence
def test_08_a_record_that_does_not_verify_yields_no_permit(world, tmp_path, monkeypatch):
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    allowed = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)
    record = v.real_record(wt.OPC_X13, allowed, {"run_id": "r"}, tmp_path, pin=False)
    with pytest.raises(wt.ActivationRefused) as info:
        wt.issue_real_permit(record, category=wt.OPC_X13, allowed=allowed, binding={"run_id": "r"}, expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    assert info.value.code == "approval_document_does_not_pin_record"


# ---- 9. process lifetime
def test_09_a_timeout_above_the_vector_bound_or_the_permit_lifetime_is_refused():
    vec = v.local_vector(timeout_s=5)
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "pass", lifetime=3)
    assert transport.problems(v.invocation(vec, timeout_s=3)) == []
    assert transport.problems(v.invocation(vec, timeout_s=4))[0]["code"] == "timeout_exceeds_bound"
    assert transport.problems(v.invocation(vec, timeout_s=None))[0]["code"] == "timeout_missing"
    assert transport.problems(v.invocation(vec, timeout_s=True))[0]["code"] == "timeout_missing"
    assert transport.problems(v.invocation(vec, timeout_s=0))[0]["code"] == "timeout_missing"


def test_09_a_background_process_past_the_approved_lifetime_is_ended_by_finish():
    vec = v.local_vector("BG", timeout_s=1.0, mode=pr.LAUNCH_BACKGROUND)
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "import time; time.sleep(30)", lifetime=1.0)
    handle = transport.start(v.invocation(vec, timeout_s=1.0))
    early = transport.finish(handle, 0.2)
    assert early.timed_out is True and early.returncode is None                    # still running inside its lifetime: the caller decides
    time.sleep(1.1)
    late = transport.finish(handle, 5)
    assert late.timed_out is True and "+lifetime" in transport.evidence[-1]["termination"] and transport._children[handle].proc.poll() is not None


# ---- 10. timeout
def test_10_a_timeout_terminates_only_the_started_process_and_keeps_the_evidence():
    vec = v.local_vector(timeout_s=5)
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "import sys, time; sys.stdout.write('partial'); sys.stdout.flush(); time.sleep(30)")
    started = time.monotonic()
    result = transport.run(v.invocation(vec, timeout_s=1.0))
    assert result.timed_out is True and time.monotonic() - started < 15
    row = transport.evidence[0]
    assert row["timed_out"] is True and row["termination"] != "none" and transport._children[1].proc.poll() is not None
    assert result.stdout == b"partial"                                              # partial output is preserved, not discarded


# ---- 11. interrupt
def test_11_an_interrupt_terminates_the_child_records_the_evidence_and_propagates():
    vec = v.local_vector(timeout_s=5)

    class InterruptProxy:
        def __init__(self, proc):
            self._p, self._raised = proc, False

        def __getattr__(self, name):
            return getattr(self._p, name)

        def wait(self, timeout=None):
            if not self._raised:
                self._raised = True
                raise KeyboardInterrupt
            return self._p.wait(timeout)

    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "import time; time.sleep(30)", popen=lambda argv, **kw: InterruptProxy(subprocess.Popen(argv, **kw)))
    with pytest.raises(KeyboardInterrupt):
        transport.run(v.invocation(vec, timeout_s=5))
    row = transport.evidence[0]
    assert row["interrupted"] is True and row["termination"] != "none" and transport._children[1].proc.poll() is not None


# ---- 12, 13. exit status, stdout and stderr
@pytest.mark.parametrize("code", [0, 1, 7, 255])
def test_12_the_exit_status_is_preserved_exactly(code):
    vec = v.local_vector()
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: f"import sys; sys.exit({code})")
    result = transport.run(v.invocation(vec))
    assert result.returncode == code and transport.evidence[0]["returncode"] == code and result.timed_out is False


def test_13_stdout_and_stderr_are_captured_hashed_and_written_without_overwriting(tmp_path):
    vec = v.local_vector()
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "import sys; sys.stdout.buffer.write(b'out\\n'); sys.stderr.buffer.write(b'err\\n'); sys.exit(3)")
    result = transport.run(v.invocation(vec))
    assert result.stdout.replace(b"\r\n", b"\n") == b"out\n" and result.stderr.replace(b"\r\n", b"\n") == b"err\n"
    row = transport.evidence[0]
    assert row["stdout_sha256"] == hashlib.sha256(result.stdout).hexdigest() and row["stderr_sha256"] == hashlib.sha256(result.stderr).hexdigest()
    written = transport.write_evidence(tmp_path / "ev")
    assert written[0] == "evidence.json" and (tmp_path / "ev" / "001.stdout").read_bytes() == result.stdout
    document = json.loads((tmp_path / "ev" / "evidence.json").read_text(encoding="utf-8"))
    assert document["schema"] == wt.EVIDENCE_SCHEMA and document["descendants_in_distribution_terminated"] == "UNVERIFIED" and document["real_process_started"] is False
    with pytest.raises(FileExistsError):
        transport.write_evidence(tmp_path / "ev")


def test_13_output_beyond_the_capture_limit_is_truncated_without_blocking_the_child():
    vec = v.local_vector()
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "import sys; sys.stdout.write('x' * 200000)", cap=10)
    result = transport.run(v.invocation(vec))
    assert len(result.stdout) == 10 and transport.evidence[0]["output_truncated"] is True and result.returncode == 0


# ---- 14, 15. no retry, no second invocation
def test_14_a_failed_process_is_never_retried():
    vec = v.local_vector()
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "import sys; sys.exit(1)")
    first = transport.run(v.invocation(vec))
    assert first.returncode == 1 and len(transport.evidence) == 1 and transport.evidence[0]["retried"] is False
    with pytest.raises(pr.RunnerRefused) as info:
        transport.run(v.invocation(vec))
    assert info.value.code == "no_retry" and len(transport.evidence) == 1


def test_15_the_use_count_is_enforced_by_the_transport_itself_not_only_by_the_runner():
    vec = v.local_vector(uses=2)
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "pass")
    transport.run(v.invocation(vec))
    transport.run(v.invocation(vec))
    with pytest.raises(pr.RunnerRefused) as info:
        transport.run(v.invocation(vec))
    assert info.value.code == "no_retry" and len(transport.evidence) == 2


def test_15_a_second_active_process_is_refused_where_one_is_the_limit():
    a, b = v.local_vector("A", mode=pr.LAUNCH_BACKGROUND), v.local_vector("B", mode=pr.LAUNCH_BACKGROUND)
    transport = v.fixture_transport(wt.OPC_CLOCK, [a, b], lambda inv: "import time; time.sleep(30)", max_active=1)
    handle = transport.start(v.invocation(a))
    with pytest.raises(pr.RunnerRefused) as info:
        transport.start(v.invocation(b))
    assert info.value.code == "active_operation_limit"
    transport.terminate(handle, 1.0)
    assert transport._children[handle].finished is True
    transport.start(v.invocation(b))                              # after the first ended, the slot is free again
    transport.shutdown()


def test_15_only_a_process_the_transport_started_can_be_terminated_and_shutdown_spares_others():
    vec = v.local_vector("BG", mode=pr.LAUNCH_BACKGROUND)
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "import time; time.sleep(30)")
    other = subprocess.Popen(v.py("import time; time.sleep(30)"))          # not started by the transport
    try:
        with pytest.raises(pr.RunnerRefused) as info:
            transport.terminate(12345, 1.0)
        assert info.value.code == "unknown_handle"
        transport.start(v.invocation(vec))
        transport.shutdown()
        assert transport._children[1].proc.poll() is not None and other.poll() is None
    finally:
        other.kill()
        other.wait()


# ---- routing across categories
def test_routing_sends_each_vector_to_the_one_permit_that_lists_it_and_refuses_the_rest(world):
    x13 = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)
    clock_vec = v.clock_allowed(v.clock_plan(world.tmp / "c3"))
    a = v.fixture_transport(wt.OPC_X13, x13, lambda inv: "pass")
    b = v.fixture_transport(wt.OPC_CLOCK, clock_vec, lambda inv: "pass")
    router = wt.RoutingTransport([a, b])
    router.run(v.invocation(x13[0]))
    assert len(a.evidence) == 1 and len(b.evidence) == 0
    with pytest.raises(pr.RunnerRefused) as info:
        router.run(v.invocation(v.local_vector("Z")))
    assert info.value.code == "vector_not_permitted"
    with pytest.raises(wt.ActivationRefused):
        wt.RoutingTransport([a, v.fixture_transport(wt.OPC_X13, x13, lambda inv: "pass")])          # one transport per category
    with pytest.raises(wt.ActivationRefused):
        wt.RoutingTransport([a, wt.WslProcessTransport(wt.SpawnPermit(wt.MODE_FIXTURE, wt.OPC_CLOCK, sys.executable, None, tuple(clock_vec), 5.0, 1, 100, None, {}, _origin="x"))])
