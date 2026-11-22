"""A0v hardening tests, written after a mutation check on the new code showed which defense-in-depth checks no test reached: the compiled-in re-check at start-up, the wsl.exe-only rule of a real
transport, the class check of a category, the lifetime-versus-vector bound, mixed transport kinds, the stored transport evidence hash, the clean-row rule and the COMPLETED reason code. It also covers
three hardening changes: a failed start counts as an attempt, a permit cannot be built by hand, and an incomplete capture is flagged. Offline; nothing real is started."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests import a0t_support as t
from tests import a0u_support as u
from tests import a0v_support as v
from tests.a0v_support import guard  # noqa: F401  (fixture)
from tests.test_a0v_clock_plan_2026_11_22 import _promote_clock, run_round  # noqa: F401
from tests.test_a0v_integrity_outcome_2026_11_22 import promote, rd, reseal, run_sim, wr  # noqa: F401

from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import run_outcome as ro            # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi          # noqa: E402
from fabric_perf.controllers import wsl_transport as wt          # noqa: E402

pytestmark = pytest.mark.usefixtures("guard")
BINDING = {"run_id": "r-test", "plan_sha256": "a" * 64}


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    return v.build_world(tmp_path_factory.mktemp("a0v_hardening_world"))


@pytest.fixture()
def x13(world):
    return wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)


def real_permit(monkeypatch, tmp_path, category, allowed, binding=BINDING, **over):
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    record = v.real_record(category, allowed, binding, tmp_path, **over)
    return wt.issue_real_permit(record, category=category, allowed=allowed, binding=binding, expected_candidate_manifest_sha256=v.MANIFEST_SHA)


def codes(record, category, allowed, binding=BINDING):
    return {f["code"] for f in wt.verify_activation_record(record, category=category, allowed=allowed, binding=binding, expected_candidate_manifest_sha256=v.MANIFEST_SHA)}


# ---- a failed start is an attempt, and is never retried
def test_a_start_that_fails_counts_as_the_attempt_leaves_evidence_and_is_not_retried():
    vec = v.local_vector()

    def refuse(argv, **kwargs):
        raise FileNotFoundError("no such executable")

    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "pass", popen=refuse)
    with pytest.raises(pr.RunnerRefused) as info:
        transport.run(v.invocation(vec))
    assert info.value.code == "spawn_failed"
    row = transport.evidence[0]
    assert row["returncode"] is None and row["spawn_error"].startswith("FileNotFoundError") and row["retried"] is False
    with pytest.raises(pr.RunnerRefused) as again:
        transport.run(v.invocation(vec))
    assert again.value.code == "no_retry" and len(transport.evidence) == 1


# ---- the compiled-in key is re-checked at the moment of start-up, not only at admission
def test_the_start_up_step_itself_refuses_when_real_spawn_is_not_compiled_in(x13, tmp_path, monkeypatch):
    permit = real_permit(monkeypatch, tmp_path, wt.OPC_X13, x13)
    fake = v.FakePopen()
    transport = wt.WslProcessTransport(permit, popen=fake)
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", False)
    with pytest.raises(wt.ActivationRefused) as info:
        transport._spawn(v.invocation(x13[0]), x13[0])                # white-box: bypass admission to reach the second check
    assert info.value.code == "real_spawn_not_compiled_in" and fake.calls == [] and transport._uses == {}


# ---- a real transport starts wsl.exe only
def test_a_real_transport_refuses_a_vector_whose_program_is_not_wsl_exe(monkeypatch):
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    odd = wt.AllowedVector("ODD", ("notwsl.exe", "-d", v.DISTRO, "--exec", "date"), None, 5.0, "read_only_observation", 1)
    permit = wt.SpawnPermit(wt.MODE_REAL, wt.OPC_CLOCK, v.EXE, v.DISTRO, (odd,), 30.0, 1, 1000, None, {}, _origin=wt._ISSUER)       # white-box: built past the issuer
    fake = v.FakePopen()
    transport = wt.WslProcessTransport(permit, popen=fake)
    assert transport.problems(v.invocation(odd))[0]["code"] == "executable_not_wsl"
    with pytest.raises(pr.RunnerRefused):
        transport.run(v.invocation(odd))
    assert fake.calls == []


# ---- a category never lists a vector of a stronger class
def test_a_read_only_category_cannot_list_a_staging_or_transaction_vector_at_issuance(world, x13, tmp_path):
    caliper = wt.vectors_for_category(wt.OPC_CALIPER, world.plan, distro=v.DISTRO, mount_root=v.MOUNT)
    tx = next(a for a in caliper if a.klass == "transaction_producing")
    with pytest.raises(wt.ActivationRefused) as info:
        wt.issue_fixture_permit(wt.OPC_CLOCK, [v.local_vector(), tx], executable=sys.executable, max_lifetime_s=5, acknowledge_fixture_only=True)
    assert info.value.code == "class_not_permitted_in_category"
    mixed = x13 + [tx]
    record = v.real_record(wt.OPC_X13, mixed, BINDING, tmp_path)
    assert codes(record, wt.OPC_X13, mixed) == set()                   # the record verifies against what it binds ...
    with pytest.raises(wt.ActivationRefused) as real:
        wt.issue_real_permit(record, category=wt.OPC_X13, allowed=mixed, binding=BINDING, expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    assert real.value.code == "class_not_permitted_in_category"       # ... but a permit still refuses to list a stronger class (before the compiled-in key is even consulted)


def test_the_admission_check_also_rejects_a_stronger_class_if_a_permit_were_built_by_hand():
    tx = v.local_vector("TX", klass="transaction_producing")
    permit = wt.SpawnPermit(wt.MODE_FIXTURE, wt.OPC_CLOCK, sys.executable, None, (tx,), 30.0, 1, 1000, None, {}, _origin=wt._ISSUER)           # white-box
    transport = wt.WslProcessTransport(permit, command_for=lambda inv: v.py("pass"))
    assert transport.problems(v.invocation(tx))[0]["code"] == "class_not_permitted"


def test_a_permit_cannot_be_built_by_hand_even_with_a_plausible_origin_string(x13):
    for origin in ("real-permit", "fixture-permit", True, 1, object()):
        forged = wt.SpawnPermit(wt.MODE_REAL, wt.OPC_X13, v.EXE, v.DISTRO, tuple(x13), 30.0, 1, 1000, "a" * 64, {}, _origin=origin)
        with pytest.raises(wt.ActivationRefused) as info:
            wt.WslProcessTransport(forged)
        assert info.value.code == "permit_required"


# ---- record lifetime must cover the longest vector bound; mixed kinds
def test_a_record_lifetime_shorter_than_the_longest_vector_bound_is_refused(x13, tmp_path):
    longest = max(a.timeout_s for a in x13)
    short = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path, max_process_lifetime_s=longest - 1)
    assert "lifetime_below_vector_timeout" in codes(short, wt.OPC_X13, x13)
    exact = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path / "exact", max_process_lifetime_s=longest)
    assert "lifetime_below_vector_timeout" not in codes(exact, wt.OPC_X13, x13)


def test_real_and_fixture_transports_are_never_mixed_in_one_router(x13, tmp_path, monkeypatch):
    clock_plan = v.clock_plan(tmp_path / "k")
    real = wt.WslProcessTransport(real_permit(monkeypatch, tmp_path / "r", wt.OPC_X13, x13), popen=v.FakePopen())
    fixture = v.fixture_transport(wt.OPC_CLOCK, v.clock_allowed(clock_plan), lambda inv: "pass", lifetime=60)
    with pytest.raises(wt.ActivationRefused) as info:
        wt.RoutingTransport([real, fixture])
    assert info.value.code == "mixed_transport_kinds"


def test_a_routed_bounded_run_sends_its_own_clock_gate_to_the_narrower_clock_permit(world, tmp_path):
    caliper = wt.vectors_for_category(wt.OPC_CALIPER, world.plan, distro=v.DISTRO, mount_root=v.MOUNT)
    clock = wt.vectors_for_category(wt.OPC_CLOCK, v.clock_plan(tmp_path / "k"), distro=v.DISTRO, mount_root=v.MOUNT)
    gate = next(a for a in caliper if a.step_id == "X03_clock_pre")
    shared = next(c for c in clock if c.os_argv == gate.os_argv)
    wide = v.fixture_transport(wt.OPC_CALIPER, caliper, lambda inv: "pass", lifetime=100000)
    narrow = v.fixture_transport(wt.OPC_CLOCK, [shared], lambda inv: "pass", lifetime=60)
    router = wt.RoutingTransport([wide, narrow])
    router.run(v.invocation(gate))
    assert len(narrow.evidence) == 1 and len(wide.evidence) == 0


# ---- an incomplete capture is flagged
def test_a_descendant_that_keeps_the_pipe_open_marks_the_capture_incomplete():
    vec = v.local_vector(timeout_s=10)
    script = ("import subprocess, sys; subprocess.Popen([sys.executable, '-I', '-c', 'import time; time.sleep(4)']); sys.stdout.write('head'); sys.stdout.flush()")
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: script)
    result = transport.run(v.invocation(vec, timeout_s=10))
    assert result.returncode == 0 and result.stdout == b"head" and transport.evidence[0]["capture_incomplete"] is True


def test_a_clean_child_is_not_marked_incomplete():
    vec = v.local_vector()
    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "print('ok')")
    transport.run(v.invocation(vec))
    assert transport.evidence[0]["capture_incomplete"] is False and "spawn_error" not in transport.evidence[0]


# ---- the success verdict: reason codes and stored transport evidence
@pytest.fixture(scope="module")
def promoted_source(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("a0v_hardening_sim")
    return run_sim(tmp)


def promoted(promoted_source, tmp_path, monkeypatch):
    plan, addendum, out, status = promoted_source
    target = tmp_path / "run"
    shutil.copytree(out, target)
    promote(target, plan, addendum, monkeypatch, tmp_path)
    return plan, addendum, target


def edit_transport(out: Path, mutate, *, reseal_hash: bool):
    path = out / ro.TRANSPORT_DIR / wt.OPC_X13 / "evidence.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    mutate(document)
    if reseal_hash:
        document["document_sha256"] = wt.be._digest(document, drop=("document_sha256",))
    path.write_text(json.dumps(document), encoding="utf-8")


def codes_of(outcome) -> set[str]:
    return {r["code"] for r in outcome["reasons"]}


def test_a_status_other_than_completed_names_its_reason(promoted_source, tmp_path, monkeypatch):
    plan, addendum, out = promoted(promoted_source, tmp_path, monkeypatch)
    path = out / plan["layout"]["host_evidence"]["run_status"]
    document = rd(path)
    document["status"] = "FINAL_CHECK_FAILED"
    wr(path, document)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and outcome["outcome"] == ro.O_PROCESS_FAILED and "status_not_completed" in codes_of(outcome)


def test_recorded_errors_name_their_reason(promoted_source, tmp_path, monkeypatch):
    plan, addendum, out = promoted(promoted_source, tmp_path, monkeypatch)
    path = out / plan["layout"]["host_evidence"]["run_status"]
    document = rd(path)
    document["errors"] = [{"step": "X13_integrity", "reason": "hash failed"}]
    wr(path, document)
    assert "status_has_errors" in codes_of(ro.classify_bounded_run(out, plan, addendum))


def test_transport_evidence_edited_without_its_hash_is_detected(promoted_source, tmp_path, monkeypatch):
    plan, addendum, out = promoted(promoted_source, tmp_path, monkeypatch)
    edit_transport(out, lambda d: d["evidence"][0].update(duration_s=123.0), reseal_hash=False)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and "transport_evidence_hash" in codes_of(outcome)


@pytest.mark.parametrize("field, value", [("returncode", 1), ("timed_out", True), ("interrupted", True), ("output_truncated", True), ("capture_incomplete", True), ("spawn_error", "OSError: x")])
def test_a_digest_process_that_did_not_finish_cleanly_is_never_a_success_even_if_the_stored_output_matches(promoted_source, tmp_path, monkeypatch, field, value):
    plan, addendum, out = promoted(promoted_source, tmp_path, monkeypatch)
    edit_transport(out, lambda d: d["evidence"][0].update({field: value}), reseal_hash=True)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and "transport_row_not_clean" in codes_of(outcome)


def test_two_process_records_for_one_digest_step_are_not_one_run(promoted_source, tmp_path, monkeypatch):
    plan, addendum, out = promoted(promoted_source, tmp_path, monkeypatch)
    edit_transport(out, lambda d: d["evidence"].append(dict(d["evidence"][0])), reseal_hash=True)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and "transport_row_count" in codes_of(outcome)


def test_transport_evidence_for_other_vectors_is_refused(promoted_source, tmp_path, monkeypatch):
    plan, addendum, out = promoted(promoted_source, tmp_path, monkeypatch)
    edit_transport(out, lambda d: d.update(vectors_sha256="e" * 64), reseal_hash=True)
    assert "transport_vectors_mismatch" in codes_of(ro.classify_bounded_run(out, plan, addendum))


# ---- an interrupt while collecting a background process also ends only that child, records the evidence and propagates
def test_an_interrupt_during_finish_terminates_the_child_records_the_evidence_and_propagates():
    vec = v.local_vector("BG", timeout_s=30, mode=pr.LAUNCH_BACKGROUND)

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

    transport = v.fixture_transport(wt.OPC_CLOCK, [vec], lambda inv: "import time; time.sleep(30)", lifetime=60, popen=lambda argv, **kw: InterruptProxy(subprocess.Popen(argv, **kw)))
    handle = transport.start(v.invocation(vec, timeout_s=30))
    with pytest.raises(KeyboardInterrupt):
        transport.finish(handle, 5)
    row = transport.evidence[0]
    assert row["interrupted"] is True and row["termination"] != "none" and transport._children[handle].proc.poll() is not None


# ---- a clock round whose probe processes did not finish cleanly is never a verified measurement
@pytest.fixture(scope="module")
def clock_round(tmp_path_factory):
    return run_round(tmp_path_factory.mktemp("a0v_hardening_clock"))


@pytest.mark.parametrize("field, value", [("capture_incomplete", True), ("spawn_error", "OSError: x"), ("timed_out", True), ("interrupted", True), ("returncode", 3)])
def test_a_clock_probe_row_that_is_not_clean_breaks_the_verified_outcome(clock_round, tmp_path, monkeypatch, field, value):
    plan, transport, status, out = clock_round
    copy = tmp_path / "copy"
    shutil.copytree(out, copy)
    _promote_clock(copy, plan, monkeypatch, tmp_path)
    assert ro.classify_clock_round(copy, plan)["success"] is True                          # the promotion alone is a success, so the one edit below is the cause
    path = copy / ro.TRANSPORT_DIR / wt.OPC_CLOCK / "evidence.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["evidence"][2][field] = value
    document["document_sha256"] = wt.be._digest(document, drop=("document_sha256",))
    path.write_text(json.dumps(document), encoding="utf-8")
    outcome = ro.classify_clock_round(copy, plan)
    assert outcome["success"] is False and "transport_row_not_clean" in {r["code"] for r in outcome["reasons"]}
