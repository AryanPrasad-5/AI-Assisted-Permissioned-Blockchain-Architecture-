"""A0v Workstream E/F: operation-specific authorization. A record for one category cannot authorize another; a synthetic, incomplete, changed or mis-bound record cannot activate the real transport; the
compiled-in key and the three executor gates are independent. Every record here is SYNTHETIC (it LOOKS approved so the verifier can be tested). Nothing is started; real-mode transports receive a fake Popen."""

from __future__ import annotations

import ast
import inspect
import sys

import pytest

from tests import a0t_support as t
from tests import a0u_support as u  # noqa: F401
from tests import a0v_support as v
from tests.a0v_support import guard  # noqa: F401  (fixture)
from tests.test_a0t_clock_only_2026_11_22 import approved as clock_auth

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import clock_only_round as co       # noqa: E402
from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi          # noqa: E402
from fabric_perf.controllers import wsl_transport as wt          # noqa: E402

pytestmark = pytest.mark.usefixtures("guard")
BINDING = {"run_id": "r-test", "plan_sha256": "a" * 64}


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    return v.build_world(tmp_path_factory.mktemp("a0v_auth_world"))


@pytest.fixture()
def x13(world):
    return wt.vectors_for_category(wt.OPC_X13, world.addendum, distro=v.DISTRO, mount_root=v.MOUNT)


@pytest.fixture()
def caliper(world):
    return wt.vectors_for_category(wt.OPC_CALIPER, world.plan, distro=v.DISTRO, mount_root=v.MOUNT)


def codes(record, category, allowed, binding=BINDING, manifest=v.MANIFEST_SHA, **kw):
    return {f["code"] for f in wt.verify_activation_record(record, category=category, allowed=allowed, binding=binding, expected_candidate_manifest_sha256=manifest, **kw)}


def test_a_complete_synthetic_record_verifies_so_the_failures_below_are_each_caused_by_their_one_change(x13, tmp_path):
    assert codes(v.real_record(wt.OPC_X13, x13, BINDING, tmp_path), wt.OPC_X13, x13) == set()


# ---- 16-18. one approval authorizes one category
def test_16_a_clock_approval_cannot_authorize_caliper(world, caliper, tmp_path):
    plan = v.clock_plan(tmp_path / "k")
    clock = v.clock_allowed(plan)
    record = v.real_record(wt.OPC_CLOCK, clock, v.clock_binding(plan), tmp_path)
    assert {"operation_category_mismatch", "vector_set_mismatch", "binding_mismatch"} <= codes(record, wt.OPC_CALIPER, caliper)
    transport = v.fixture_transport(wt.OPC_CLOCK, clock, lambda inv: "pass")
    tx = next(a for a in caliper if a.klass == "transaction_producing")
    assert transport.problems(v.invocation(tx))[0]["code"] == "vector_not_permitted"
    staging = next(a for a in caliper if a.klass == "local_staging")
    assert transport.problems(v.invocation(staging))[0]["code"] == "vector_not_permitted"


def test_17_a_discovery_approval_cannot_authorize_channel_creation(world, tmp_path):
    allowed = wt.vectors_for_category(wt.OPC_DISCOVERY, world.spec, distro=v.DISTRO, mount_root=v.MOUNT, container_name="caliper", step_timeout_s=10)
    flat = [tok.lower() for a in allowed for tok in a.os_argv]
    assert not any(word in flat for word in ("create", "join", "channel", "update", "up", "run", "start", "rm", "cp", "mkdir", "launch", "invoke"))
    transport = v.fixture_transport(wt.OPC_DISCOVERY, allowed, lambda inv: "pass")
    channel_create = ("wsl.exe", "-d", v.DISTRO, "--exec", "peer", "channel", "create", "-c", "mychannel")
    assert transport.problems(v.invocation(allowed[0], os_argv=channel_create))[0]["code"] == "vector_not_permitted"
    assert not any(a.klass != "read_only_observation" for a in allowed)
    record = v.real_record(wt.OPC_DISCOVERY, allowed, BINDING, tmp_path)
    assert "operation_category_mismatch" in codes(record, wt.OPC_CALIPER, allowed)


def test_18_a_hash_approval_cannot_authorize_a_workload(world, x13, caliper, tmp_path):
    transport = v.fixture_transport(wt.OPC_X13, x13, lambda inv: "pass")
    for vector in caliper:
        if vector.klass in ("transaction_producing", "local_staging"):
            assert transport.problems(v.invocation(vector))[0]["code"] == "vector_not_permitted", vector.step_id
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    assert "operation_category_mismatch" in codes(record, wt.OPC_CALIPER, caliper)


def test_a_caliper_permit_needs_its_own_transaction_authorization_and_no_other_category_may_carry_one(world, x13, caliper, tmp_path):
    good = v.real_record(wt.OPC_CALIPER, caliper, BINDING, tmp_path)
    assert codes(good, wt.OPC_CALIPER, caliper) == set()
    for tx in (None, {"authorized": None, "max_transactions": None, "authorization_reference": None}, {"authorized": True, "max_transactions": 0, "authorization_reference": "r"},
               {"authorized": True, "max_transactions": 5, "authorization_reference": ""}, {"authorized": False, "max_transactions": 5, "authorization_reference": "r"}):
        assert "transaction_authorization_missing" in codes(v.real_record(wt.OPC_CALIPER, caliper, BINDING, tmp_path, transaction_authorization=tx), wt.OPC_CALIPER, caliper)
    carrying = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path, transaction_authorization={"authorized": True, "max_transactions": 5, "authorization_reference": "r"})
    assert "transaction_authorization_not_permitted" in codes(carrying, wt.OPC_X13, x13)


# ---- 19. synthetic records
def test_19_a_synthetic_record_cannot_activate_the_real_transport_even_when_everything_else_is_in_place(x13, tmp_path, monkeypatch):
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path, synthetic=True)
    assert "synthetic_record_for_real_transport" in codes(record, wt.OPC_X13, x13)
    with pytest.raises(wt.ActivationRefused) as info:
        wt.issue_real_permit(record, category=wt.OPC_X13, allowed=x13, binding=BINDING, expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    assert info.value.code == "synthetic_record_for_real_transport"
    assert "synthetic_record_for_real_transport" not in codes(record, wt.OPC_X13, x13, allow_synthetic=True)      # only the test-only switch of the verifier can see past it, and issue_real_permit never sets it
    assert "allow_synthetic" not in inspect.signature(wt.issue_real_permit).parameters


# ---- 20. missing owner decision
def test_20_the_unset_template_cannot_activate_anything(x13):
    template = wt.activation_template(wt.OPC_X13, x13, BINDING)
    found = codes(template, wt.OPC_X13, x13)
    assert {"owner_field_unset", "real_spawn_not_authorized", "approval_unset", "distro", "executable", "candidate_manifest_hash"} <= found
    assert all(template[k] is None for k in wt.OWNER_FIELDS) and all(template["approval"][k] is None for k in be.APPROVAL_KEYS)
    with pytest.raises(wt.ActivationRefused):
        wt.issue_real_permit(template, category=wt.OPC_X13, allowed=x13, binding=BINDING, expected_candidate_manifest_sha256=v.MANIFEST_SHA)


@pytest.mark.parametrize("field", ["distro", "executable", "max_process_lifetime_s", "max_active_processes", "max_capture_bytes", "candidate_manifest_sha256", "activation_reference", "real_spawn_authorized"])
def test_20_every_owner_field_is_individually_required(x13, tmp_path, field):
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path, **{field: None})
    assert "owner_field_unset" in codes(record, wt.OPC_X13, x13)


@pytest.mark.parametrize("flag", [False, None, "true", 1, "yes"])
def test_20_real_spawn_authorized_must_be_an_explicit_true(x13, tmp_path, flag):
    assert "real_spawn_not_authorized" in codes(v.real_record(wt.OPC_X13, x13, BINDING, tmp_path, real_spawn_authorized=flag), wt.OPC_X13, x13)


def test_20_the_approval_block_must_be_set_and_approved(x13, tmp_path):
    for override in ({"status": "PROPOSED"}, {"status": None}, {"approved_by": ""}, {"approved_utc": "not a time"}, {"record_path": str(tmp_path / "missing.txt")}):
        found = codes(v.real_record(wt.OPC_X13, x13, BINDING, tmp_path, approval=override), wt.OPC_X13, x13)
        assert found, override


# ---- 21. a changed record
def test_21_changing_the_record_after_approval_invalidates_it(x13, tmp_path):
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    record["max_process_lifetime_s"] = 3600
    assert "record_hash" in codes(record, wt.OPC_X13, x13)


def test_21_a_changed_approval_document_invalidates_it(x13, tmp_path):
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    with open(record["approval"]["record_path"], "a", encoding="utf-8") as handle:
        handle.write("edited after approval\n")
    assert "approval_record_hash" in codes(record, wt.OPC_X13, x13)


def test_21_an_approval_document_that_does_not_pin_this_record_does_not_approve_it(x13, tmp_path):
    assert "approval_document_does_not_pin_record" in codes(v.real_record(wt.OPC_X13, x13, BINDING, tmp_path, pin=False), wt.OPC_X13, x13)


def test_21_a_record_reused_with_other_limits_needs_a_new_pin(x13, tmp_path):
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path / "first")
    other = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path / "second", max_capture_bytes=2 << 20)
    assert other["record_sha256"] != record["record_sha256"]
    other["approval"] = record["approval"]                                   # the old approval document pins the OLD record hash
    assert "approval_document_does_not_pin_record" in codes(other, wt.OPC_X13, x13)


# ---- 22. artifact hash, vector set
def test_22_an_incorrect_candidate_manifest_hash_invalidates_the_record(x13, tmp_path):
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    assert "candidate_manifest_mismatch" in codes(record, wt.OPC_X13, x13, manifest="d" * 64)
    assert "manifest_expectation_missing" in codes(record, wt.OPC_X13, x13, manifest=None)
    bad = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path, manifest_sha="not-a-hash")
    assert "candidate_manifest_hash" in codes(bad, wt.OPC_X13, x13, manifest="not-a-hash")


def test_22_a_record_for_other_vectors_is_refused(world, x13, tmp_path):
    other = wt.vectors_for_category(wt.OPC_X13, world.addendum, distro="Debian", mount_root=v.MOUNT)
    record = v.real_record(wt.OPC_X13, other, BINDING, tmp_path, distro="Debian")
    assert "vector_set_mismatch" in codes(record, wt.OPC_X13, x13)
    tampered = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    tampered["allowed_vectors"][0]["timeout_s"] = 999
    assert {"vector_set_mismatch", "record_hash"} <= codes(tampered, wt.OPC_X13, x13)


# ---- 23. run identity
@pytest.mark.parametrize("other", [{"run_id": "another-run", "plan_sha256": "a" * 64}, {"run_id": "r-test", "plan_sha256": "b" * 64}, {"run_id": "r-test"}, {}])
def test_23_an_incorrect_run_identity_invalidates_the_record(x13, tmp_path, other):
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    assert "binding_mismatch" in codes(record, wt.OPC_X13, x13, binding=other)


def test_23_a_clock_record_binds_the_round_and_plan_digest(tmp_path):
    plan = v.clock_plan(tmp_path / "k")
    clock = v.clock_allowed(plan)
    record = v.real_record(wt.OPC_CLOCK, clock, v.clock_binding(plan), tmp_path)
    assert codes(record, wt.OPC_CLOCK, clock, binding=v.clock_binding(plan)) == set()
    assert "binding_mismatch" in codes(record, wt.OPC_CLOCK, clock, binding={**v.clock_binding(plan), "round_id": "other"})


# ---- the keys: a record alone is not enough
def test_a_fully_valid_record_still_activates_nothing_while_real_spawn_is_not_compiled_in(x13, tmp_path):
    assert wt.REAL_SPAWN_COMPILED_IN is False
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    assert codes(record, wt.OPC_X13, x13) == set()
    with pytest.raises(wt.ActivationRefused) as info:
        wt.issue_real_permit(record, category=wt.OPC_X13, allowed=x13, binding=BINDING, expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    assert info.value.code == "real_spawn_not_compiled_in"


def test_the_compiled_in_key_is_rechecked_at_the_moment_of_use(x13, tmp_path, monkeypatch):
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    fake = v.FakePopen()
    transport = wt.WslProcessTransport(wt.issue_real_permit(record, category=wt.OPC_X13, allowed=x13, binding=BINDING, expected_candidate_manifest_sha256=v.MANIFEST_SHA), popen=fake)
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", False)
    with pytest.raises(pr.RunnerRefused) as info:
        transport.run(v.invocation(x13[0]))
    assert info.value.code == "real_spawn_not_compiled_in" and fake.calls == []


def test_a_real_transport_starts_wsl_exe_only_and_never_rewrites_a_vector(x13, tmp_path, monkeypatch):
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    record = v.real_record(wt.OPC_X13, x13, BINDING, tmp_path)
    permit = wt.issue_real_permit(record, category=wt.OPC_X13, allowed=x13, binding=BINDING, expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    with pytest.raises(wt.ActivationRefused) as info:
        wt.WslProcessTransport(permit, command_for=lambda inv: ["cmd.exe"])
    assert info.value.code == "command_for_on_real_transport"


def test_there_is_no_boolean_environment_variable_or_flag_activation_path():
    for func in (wt.issue_real_permit, wt.WslProcessTransport.__init__):
        names = set(inspect.signature(func).parameters)
        assert not any(word in n for n in names for word in ("enable", "execute", "force", "real", "live", "activate", "on")) or names <= {"self", "permit", "popen", "command_for", "now_utc"}
    tree = ast.parse(inspect.getsource(wt))
    attributes = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id in ("os", "sys")}
    imported = {a.name for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names} | {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    assert not ({"environ", "getenv", "argv"} & attributes) and "argparse" not in imported and "getenv" not in imported


# ---- the three executor gates remain independent keys
def _real_kind_transport(x13_or_clock, category, tmp_path, monkeypatch, binding):
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    record = v.real_record(category, x13_or_clock, binding, tmp_path)
    permit = wt.issue_real_permit(record, category=category, allowed=x13_or_clock, binding=binding, expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    return wt.WslProcessTransport(permit, popen=v.FakePopen())


def test_the_clock_only_executor_refuses_a_real_transport_even_with_a_valid_activation_record(tmp_path, monkeypatch):
    plan = co.build_clock_only_plan(v.clock_request(tmp_path), created_utc=v.CREATED)
    transport = _real_kind_transport(v.clock_allowed(plan), wt.OPC_CLOCK, tmp_path, monkeypatch, v.clock_binding(plan))
    assert transport.kind == "process"
    auth = clock_auth(plan, tmp_path, synthetic=False, transport=pr.TRANSPORT_WSL)
    with pytest.raises(be.LaunchRefused) as info:
        co.execute_clock_only(plan, auth, transport, wall_ns=v.wall_ns, mono_ns=lambda: 0, sleep=lambda s: None, now_utc=v.now_utc, allow_synthetic=False, hash_tree=wi.host_hash_tree)
    assert "disabled in this version" in str(info.value) and not (tmp_path / "clock_out").exists() and transport.evidence == []


def test_the_bounded_executor_refuses_a_process_runner(world, x13, tmp_path, monkeypatch):
    transport = _real_kind_transport(x13, wt.OPC_X13, tmp_path, monkeypatch, BINDING)
    runner = pr.PlanBoundRunner(world.plan, transport, distro=v.DISTRO, mount_root=v.MOUNT, max_process_lifetime_s=60)
    assert runner.kind == "process"
    deps = be.Deps(runner=runner, **t.deps_parts(t.FakeClock()))
    with pytest.raises(be.LaunchRefused) as info:
        be.execute_bounded(world.plan, {}, deps, repo_root=v.REPO, allow_synthetic=True)
    assert "process runner is refused" in str(info.value) and transport.evidence == []


def test_the_adapters_refuse_a_process_transport_before_starting_anything(world, x13, tmp_path, monkeypatch):
    transport = _real_kind_transport(x13, wt.OPC_X13, tmp_path, monkeypatch, BINDING)
    auth = t.a0t_auth(world.plan, world.record, tmp_path, transport=pr.TRANSPORT_WSL, synthetic=False)
    with pytest.raises(be.LaunchRefused):
        pr.execute_through_adapter(world.plan, auth, world.record, transport, t.deps_parts(t.FakeClock()), repo_root=v.REPO, expected_category=pr.OP_SMOKE, allow_synthetic=False)
    with pytest.raises(be.LaunchRefused):
        wi.execute_with_integrity(world.plan, auth, world.record, world.addendum, transport, {k: val for k, val in t.deps_parts(t.FakeClock()).items() if k != "hash_tree"},
                                  repo_root=v.REPO, expected_category=pr.OP_SMOKE, allow_synthetic=False)
    assert transport.evidence == []
    assert be.REAL_LAUNCH_ENABLED is False and pr.REAL_TRANSPORT_ENABLED is False


def test_describe_activation_lists_every_independent_key_and_starts_nothing(x13):
    description = wt.describe_activation(wt.OPC_X13, x13, executable=None)
    assert description["nothing_started"] is True and description["real_spawn_compiled_in"] is False and description["executor_gates_accept_process_transport"] is False
    keys = " | ".join(description["keys_required"])
    assert len(description["keys_required"]) >= 4 and "REAL_SPAWN_COMPILED_IN" in keys and "REAL_TRANSPORT_ENABLED" in keys and description["vectors_sha256"] == wt.vectors_digest(x13)
    clock = " | ".join(wt.describe_activation(wt.OPC_CLOCK, x13)["keys_required"])
    assert "LIVE_CLOCK_SAMPLING_ENABLED" in clock and "execute_clock_only" in clock and "REAL_LAUNCH_ENABLED" in " | ".join(wt.describe_activation(wt.OPC_CALIPER, x13)["keys_required"])
    assert sys.modules[wt.__name__] is wt
