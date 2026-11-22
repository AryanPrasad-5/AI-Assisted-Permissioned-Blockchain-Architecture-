"""A0u Workstreams D and E: container identity and mount discovery (specification and evaluation only) and clock-only readiness. Offline; nothing is started; every observation here is SYNTHETIC."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tests import a0t_support as t
from tests import a0u_support as u
from tests.a0t_support import FakeClock, MockTransport, REPO

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import clock_only_readiness as cr   # noqa: E402
from fabric_perf.controllers import clock_only_round as co       # noqa: E402
from fabric_perf.controllers import clock_preflight as cp        # noqa: E402
from fabric_perf.controllers import path_mapping as pm           # noqa: E402
from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import runtime_discovery as rd      # noqa: E402
from fabric_perf.controllers import wsl_clock_probe as wp        # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi          # noqa: E402

CREATED = "2026-11-22T09:00:00Z"


@pytest.fixture()
def built(tmp_path):
    plan, req, root = u.build_plan(tmp_path)
    return tmp_path, plan, t.path_record(plan)


# ================================================================== D: container identity and mounts
def test_23_the_repository_declares_two_distinct_container_names_and_no_compose_service():
    declarations = rd.scan_container_declarations(REPO)
    summary = rd.summarize_declarations(declarations)
    assert summary["distinct_container_names"] == ["caliper", "caliper-benchmark-001"] and summary["conflict"] is True and summary["compose_services"] == []
    where = summary["names"]
    assert any(w.startswith("scripts/e1/e1_la2_render.py") for w in where["caliper"])
    assert any(w.startswith("scripts/e1/e1_quicklook.py") for w in where["caliper-benchmark-001"]) and any("e1_existing_channel_pilot.py" in w for w in where["caliper-benchmark-001"])
    assert "not aliases" in summary["interpretation"] and "UNRESOLVED" in summary["interpretation"]


def test_23_the_five_identity_concepts_are_kept_apart(built):
    tmp, plan, record = built
    concepts = rd.identity_concepts(plan)
    assert set(concepts) == {"compose_service_name", "container_name", "container_id", "container_workspace_path", "host_mount_source"}
    assert concepts["compose_service_name"]["value"] is None and concepts["container_id"]["plan_value"] is None
    assert concepts["container_name"]["plan_value"] == "caliper" and concepts["container_workspace_path"]["plan_value"] == "/hyperledger/caliper/workspace"
    assert concepts["host_mount_source"]["plan_value"] == "/home/mailf18/e1/caliper-ws-benchmark"


def test_24_conflicting_identities_the_plan_names_caliper_the_benchmark_scripts_name_another(built):
    tmp, plan, record = built
    decls = rd.scan_container_declarations(REPO)
    spec = rd.discovery_spec(plan, decls, distro=t.DISTRO)
    assert spec["status"] == "SPECIFICATION_ONLY_NOT_EXECUTED" and spec["summary"]["conflict"] is True
    inspected = [step["step"] for step in spec["steps"] if step["step"].startswith("D2_inspect_")]
    assert inspected == ["D2_inspect_caliper", "D2_inspect_caliper-benchmark-001"]
    verdict = rd.evaluate_discovery(plan, "caliper-benchmark-001", [], observation_class=rd.OBSERVATION_SYNTHETIC)
    assert "plan_container_mismatch" in {f["code"] for f in verdict["findings"]} and verdict["gate_satisfied"] is False


def inspect_doc(plan, *, name="caliper", source=None, destination=None, rw=True, mount_type="bind", ident="a" * 64):
    return {"Id": ident, "Name": "/" + name, "State": {"Status": "exited"}, "Config": {"User": "node", "Image": "hyperledger/caliper:0.6.0"},
            "Mounts": [{"Type": mount_type, "Source": source or plan["mounts"]["host_workspace"], "Destination": destination or plan["mounts"]["container_workspace"], "RW": rw}]}


def test_25_invalid_mount_observations_are_findings(built):
    tmp, plan, record = built
    ok = rd.evaluate_discovery(plan, "caliper", [inspect_doc(plan)], observation_class=rd.OBSERVATION_SYNTHETIC)
    assert ok["valid"] is True and ok["gate_satisfied"] is False and ok["recorded"][0]["State"] == "exited"
    for kwargs, code in (({"source": "/home/other/ws"}, "workspace_mount_source_mismatch"), ({"mount_type": "volume"}, "workspace_mount_source_mismatch"), ({"rw": False}, "workspace_mount_not_writable"),
                         ({"destination": "/elsewhere"}, "workspace_mount_missing"), ({"ident": "short"}, "container_id_missing")):
        found = rd.evaluate_discovery(plan, "caliper", [inspect_doc(plan, **kwargs)], observation_class=rd.OBSERVATION_SYNTHETIC)
        assert code in {f["code"] for f in found["findings"]}, kwargs
    duplicate = rd.evaluate_discovery(plan, "caliper", [inspect_doc(plan), inspect_doc(plan)], observation_class=rd.OBSERVATION_SYNTHETIC)
    assert "container_not_unique" in {f["code"] for f in duplicate["findings"]}
    assert "authorized_name_unset" in {f["code"] for f in rd.evaluate_discovery(plan, None, [inspect_doc(plan)], observation_class=rd.OBSERVATION_SYNTHETIC)["findings"]}


def test_25_the_path_record_refuses_a_mount_that_does_not_map_the_planned_run_directory(built):
    tmp, plan, record = built
    wrong = pm.ContainerMount(wsl_root="/home/mailf18/e1/other-ws", container_root="/hyperledger/caliper/workspace", container_name="caliper", source="test")
    with pytest.raises(pm.PathMappingError) as info:
        t.path_record(plan, container_mount=wrong)
    assert info.value.code == "mount_root_mismatch"


def test_26_a_missing_workspace_declaration_is_only_a_syntactic_unverified_mapping(built):
    tmp, plan, record = built
    weak = t.path_record(plan, container_mount=None)
    entry = next(e for e in weak["entries"] if e["role"] == "caliper_workspace")
    assert entry["verification_level"] == pm.L_SYNTACTIC and "UNVERIFIED" in entry["note"] and weak["runtime_verified"] is False


def test_27_an_output_path_or_worker_root_outside_the_mounted_workspace_is_refused(built):
    tmp, plan, record = built
    moved = copy.deepcopy(plan)
    moved["mounts"]["run_dir_host"] = "/home/mailf18/elsewhere/runs/" + plan["run_id"]
    with pytest.raises(wi.IntegrityRefused) as info:
        wi.digest_requests(moved, distro=t.DISTRO, mount_root=t.MOUNT_ROOT, record=record, symlink_policy="refuse", limits=u.LIMITS)
    assert info.value.code == "worker_root_outside_workspace"
    escaped = copy.deepcopy(plan)
    escaped["steps"][next(i for i, s in enumerate(escaped["steps"]) if s["step"] == "X06_ledger_follower")]["writes"].append("/home/mailf18/elsewhere/out")
    assert pr.check_writes_isolated(escaped, mount_root=t.MOUNT_ROOT)


def test_28_and_29_every_discovery_vector_is_a_structured_non_shell_vector_and_none_is_executed(built):
    tmp, plan, record = built
    spec = rd.discovery_spec(plan, rd.scan_container_declarations(REPO), distro=t.DISTRO)
    json.dumps(spec)
    for step in spec["steps"]:
        assert pr.validate_argv(step["argv"]) == step["argv"] and step["executed"] is False and step["authorization"] == rd.AUTH_CLASS
        assert step["argv"][0] in ("docker", "wsl.exe") and not any(tok in pr.SHELL_NAMES for tok in step["argv"])
        verbs = step["argv"][1:3] if step["argv"][0] == "docker" else step["argv"][3:5]
        assert verbs[0] in ("ps", "inspect", "exec", "--exec") and not {"run", "start", "stop", "rm", "kill", "create", "build", "pull", "compose"} & set(step["argv"])
    exec_steps = [s for s in spec["steps"] if s["argv"][:2] == ["docker", "exec"]]
    assert exec_steps and all(s["argv"][2] == rd.PLACEHOLDER and s["requires_running_container"] for s in exec_steps)
    assert all(a in ("test",) or a.startswith(("-", "/", rd.PLACEHOLDER, "docker", "exec")) for s in exec_steps for a in s["argv"])
    assert spec["start_decision"].startswith("D5 and D6 need a running container") and "state change" in spec["start_decision"]


def test_29_a_name_that_could_carry_a_shell_command_is_refused_and_a_distribution_is_required(built):
    tmp, plan, record = built
    with pytest.raises(ValueError):
        rd.discovery_spec(plan, [{"file": "x", "line": 1, "kind": "constant", "symbol": "CALIPER", "name": "caliper; rm -rf /"}], distro=t.DISTRO)
    with pytest.raises(ValueError):
        rd.discovery_spec(plan, [], distro=None)
    with pytest.raises(pr.ArgvRefused):
        rd._vector("X", "bad", ["sh", "-c", "docker ps"], needs_running=False, passes_when="", on_failure="")
    with pytest.raises(ValueError):
        rd.evaluate_discovery(plan, "caliper", [], observation_class="REAL")


def test_a_synthetic_observation_never_satisfies_the_gate_and_only_a_real_one_with_no_finding_could(built):
    tmp, plan, record = built
    doc = [inspect_doc(plan)]
    assert rd.evaluate_discovery(plan, "caliper", doc, observation_class=rd.OBSERVATION_SYNTHETIC)["gate_satisfied"] is False
    assert rd.evaluate_discovery(plan, "caliper", doc, observation_class=rd.OBSERVATION_REAL)["gate_satisfied"] is True
    assert rd.evaluate_discovery(plan, "caliper", [inspect_doc(plan, rw=False)], observation_class=rd.OBSERVATION_REAL)["gate_satisfied"] is False


# ================================================================== E: clock-only readiness
def request(tmp: Path, **over):
    req = {"schema": co.REQUEST_SCHEMA, "round_id": "clk-test-001", "distro": "Ubuntu", "samples_per_phase": 5, "min_valid_samples": 3, "tolerance_ms": 50,        # SYNTHETIC test values
           "max_wall_monotonic_disagreement_ms": 5, "remote_resolution_ns": 1000, "probe_timeout_s": 5, "phase_gap_s": 2, "reference_domain": "windows-host:time_ns",
           "remote_domain": "wsl:Ubuntu:date_ns", "max_process_lifetime_s": 30, "output_dir": str(tmp / "clock_out"), "automount_root": "/mnt"}
    req.update(over)
    return req


@pytest.fixture()
def clock_plan(tmp_path):
    return co.build_clock_only_plan(request(tmp_path), created_utc=CREATED)


def synthetic_packet(plan, tmp, **owner):
    packet = cr.approval_packet_template(plan)
    req = plan["request"]
    packet["synthetic"] = True
    packet["owner"].update(approved_host_identity="SYNTHETIC-HOST", approved_wsl_distribution=req["distro"], approved_clock_pair={"reference_domain": req["reference_domain"], "remote_domain": req["remote_domain"]},
                           approved_sampling_commands=[list(plan["probe"]["argv"])], approved_sample_count_per_phase=req["samples_per_phase"], approved_min_valid_samples=req["min_valid_samples"],
                           approved_sampling_phases=list(cr.PHASES), approved_phase_gap_s=req["phase_gap_s"], approved_max_process_duration_s=req["max_process_lifetime_s"],
                           approved_output_paths={"output_dir": req["output_dir"]}, approved_tolerance_ms=req["tolerance_ms"], approved_wall_monotonic_bound_ms=req["max_wall_monotonic_disagreement_ms"],
                           approved_remote_resolution_ns=req["remote_resolution_ns"], approved_probe_timeout_s=req["probe_timeout_s"], approved_automount_root=req["automount_root"],
                           stop_conditions=list(cr.STOP_CONDITIONS), owner_authorization_reference="SYNTHETIC-REF", process_transport=pr.TRANSPORT_FIXTURE,
                           execution_environment=pr.ENV_WINDOWS_ORCHESTRATOR, real_transport_authorized=False)
    packet["owner"].update(owner)
    record = tmp / "clock_packet_approval.txt"
    record.write_text("SYNTHETIC approval document for a test; not an approval\n", encoding="utf-8")
    packet["approval"] = {"status": "APPROVED_BY_OWNER", "approved_by": "synthetic-test-fixture", "approved_utc": "2026-11-22T09:30:00Z", "record_path": str(record),
                          "record_sha256": cr.be.pi.sha256_file(record)}
    return packet


def run_round(plan, tmp, clock=None):
    clock = clock or FakeClock()
    parts = t.deps_parts(clock)
    auth = co.authorization_template(plan)
    record = tmp / "clock_approval.txt"
    record.write_text("SYNTHETIC approval document for a test; not an approval\n", encoding="utf-8")
    auth.update(synthetic=True, process_transport=pr.TRANSPORT_FIXTURE, execution_environment=pr.ENV_WINDOWS_ORCHESTRATOR, real_transport_authorized=False)
    auth["approval"] = {"status": "APPROVED_BY_OWNER", "approved_by": "synthetic-test-fixture", "approved_utc": "2026-11-22T09:30:00Z", "record_path": str(record),
                        "record_sha256": cr.be.pi.sha256_file(record)}
    transport = MockTransport(clock)
    status = co.execute_clock_only(plan, auth, transport, wall_ns=clock.wall, mono_ns=clock.mono, sleep=lambda s: None, now_utc=parts["now_utc"], allow_synthetic=True, hash_tree=parts["hash_tree"])
    return status, transport


def test_30_the_clock_only_command_set_is_transaction_free(clock_plan):
    assert cr.audit_clock_only_plan(clock_plan) == []
    assert [s["argv"] for s in clock_plan["steps"] if s["argv"]] == [["wsl.exe", "-d", "Ubuntu", "--exec", "date", "+%s%N"]] * 2
    for key, value in (("transactions_planned", 1), ("channel_created", True), ("configuration_changed", True), ("model_inference", True), ("real_launch_enabled", True), ("transactions_planned", False)):
        bad = copy.deepcopy(clock_plan)
        bad[key] = value
        assert {p["code"] for p in cr.audit_clock_only_plan(bad)} == {"not_clock_only"}, key


@pytest.mark.parametrize("injected", [["docker", "exec", "caliper", "caliper", "launch"], ["wsl.exe", "-d", "Ubuntu", "--exec", "peer", "channel", "create"],
                                      ["wsl.exe", "-d", "Ubuntu", "--exec", "configtxlator", "proto_decode"], ["wsl.exe", "-d", "Ubuntu", "--exec", "python3", "predict.py"],
                                      ["wsl.exe", "-d", "Ubuntu", "--exec", "date", "+%s%N", "extra"]])
def test_31_no_fabric_configuration_docker_caliper_or_model_vector_can_join_the_round(clock_plan, injected):
    bad = copy.deepcopy(clock_plan)
    bad["steps"].append({"step": "K99", "kind": "gate_clock", "class": "read_only_observation", "argv": injected, "cwd": None, "timeout_s": 5, "writes": [], "note": ""})
    assert "unexpected_vector" in {p["code"] for p in cr.audit_clock_only_plan(bad)}


def test_31_a_mutating_step_class_or_step_kind_is_refused(clock_plan):
    for field, value, code in (("class", "transaction_producing", "step_class"), ("kind", "run_foreground", "unexpected_step_kind")):
        bad = copy.deepcopy(clock_plan)
        bad["steps"][0][field] = value
        assert code in {p["code"] for p in cr.audit_clock_only_plan(bad)}
    assert "configuration_change" in co.PROHIBITED and "channel_creation" in co.PROHIBITED and "transaction_submission" in co.PROHIBITED


def test_32_the_a0e_evaluator_accepts_the_raw_samples_a_clock_only_round_stores(clock_plan, tmp_path):
    status, transport = run_round(clock_plan, tmp_path)
    assert status["status"] == "COMPLETED" and status["transactions_attempted"] == 0 and status["real_process_started"] is False
    assessment = cr.assess_round_directory(Path(clock_plan["request"]["output_dir"]), clock_plan)
    assert assessment["phases_present"] == ["pre_run", "post_run"] and assessment["raw_samples_preserved"] is True and assessment["sample_order_valid"] is True
    assert assessment["verdicts_equal"] is True and assessment["reevaluated_verdict"] == cp.AGREE and assessment["tolerance_ms_used"] == 50
    assert "no default" in assessment["tolerance_source"]
    for rec in (json.loads((Path(clock_plan["request"]["output_dir"]) / "clock" / f"{p}.json").read_text(encoding="utf-8")) for p in cr.PHASES):
        assert wp.check_phase(rec) == [] and [s["seq"] for s in rec["samples"]] == sorted(s["seq"] for s in rec["samples"]) and rec["count_collected"] == 5
    assert [i.plan_argv for i in transport.invocations if i.plan_argv[0] != "wsl.exe"] == []                      # nothing but the probe was ever handed to the transport


def test_32_a_contradictory_clock_gives_a_disagree_verdict_from_the_same_unchanged_evaluator(clock_plan, tmp_path):
    run_round(clock_plan, tmp_path, FakeClock(offset_ns=500 * 1_000_000))
    assessment = cr.assess_round_directory(Path(clock_plan["request"]["output_dir"]), clock_plan)
    assert assessment["reevaluated_verdict"] == cp.DISAGREE and assessment["verdicts_equal"] is True


@pytest.mark.parametrize("key", co.REQUEST_KEYS)
def test_33_missing_clock_parameters_are_rejected_in_the_request_and_unset_in_the_packet(tmp_path, key):
    req = request(tmp_path)
    del req[key]
    with pytest.raises(be.RequestInvalid):
        co.build_clock_only_plan(req, created_utc=CREATED)


def test_33_the_packet_template_has_every_owner_field_unset_and_proposes_no_clock_value(clock_plan):
    packet = cr.approval_packet_template(clock_plan)
    flat = json.dumps(packet["owner"])
    assert all(v is None for k, v in packet["owner"].items() if k not in ("approved_clock_pair", "approved_output_paths"))
    assert all(v is None for v in packet["owner"]["approved_clock_pair"].values()) and packet["owner"]["approved_output_paths"] == {"output_dir": None}
    assert packet["approval"] == {k: None for k in be.APPROVAL_KEYS} and packet["synthetic"] is False and packet["transactions_authorized"] == 0
    assert packet["plan_values_for_review"]["tolerance_ms"] == clock_plan["request"]["tolerance_ms"] and "50" not in flat
    verdict = cr.verify_approval_packet(packet, clock_plan)
    assert verdict["valid"] is False and verdict["real_transport_permitted"] is False and sum(f["code"] == "owner_field_unset" for f in verdict["findings"]) >= 15


def test_34_a_wrong_clock_pair_identity_or_distribution_or_command_is_rejected(clock_plan, tmp_path):
    good = cr.verify_approval_packet(synthetic_packet(clock_plan, tmp_path), clock_plan, allow_synthetic=True)
    assert good["valid"] is True and good["real_transport_permitted"] is False
    for owner, code in (({"approved_clock_pair": {"reference_domain": "windows-host:time_ns", "remote_domain": "wsl:Debian:date_ns"}}, "clock_pair_mismatch"),
                        ({"approved_wsl_distribution": "Debian"}, "owner_value_differs_from_plan"),
                        ({"approved_sampling_commands": [["wsl.exe", "-d", "Ubuntu", "--exec", "date"]]}, "owner_value_differs_from_plan"),
                        ({"approved_output_paths": {"output_dir": "C:\\elsewhere"}}, "output_path_mismatch"),
                        ({"stop_conditions": ["interrupt"]}, "owner_value_differs_from_plan")):
        found = cr.verify_approval_packet(synthetic_packet(clock_plan, tmp_path, **owner), clock_plan, allow_synthetic=True)
        assert code in {f["code"] for f in found["findings"]}, owner


def test_35_no_implicit_tolerance_the_plan_cannot_be_built_without_one_and_a_packet_cannot_approve_another(clock_plan, tmp_path):
    for value in (None, 0, -5, True, "50"):
        with pytest.raises(be.RequestInvalid):
            co.build_clock_only_plan(request(tmp_path, tolerance_ms=value), created_utc=CREATED)
    found = cr.verify_approval_packet(synthetic_packet(clock_plan, tmp_path, approved_tolerance_ms=500), clock_plan, allow_synthetic=True)
    assert "owner_value_differs_from_plan" in {f["code"] for f in found["findings"]}
    import inspect
    for fn in (cr.assess_round_directory, wp.evaluate):
        sig = inspect.signature(fn).parameters
        assert all(p.default is inspect.Parameter.empty for name, p in sig.items() if "tolerance" in name)


def test_36_invalid_sample_ordering_is_detected_in_the_stored_raw_samples(clock_plan, tmp_path):
    run_round(clock_plan, tmp_path)
    out = Path(clock_plan["request"]["output_dir"]) / "clock" / "pre_run.json"
    record = json.loads(out.read_text(encoding="utf-8"))
    record["samples"][1], record["samples"][2] = record["samples"][2], record["samples"][1]
    out.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(wp.ProbeRefused):
        cr.assess_round_directory(Path(clock_plan["request"]["output_dir"]), clock_plan)
    record["samples"][0]["ref_mono_after_ns"] = record["samples"][0]["ref_mono_before_ns"]
    assert {p["code"] for p in wp.check_phase(record)} >= {"invalid_sample_ordering", "sequence_not_increasing"}


def test_37_synthetic_evidence_and_synthetic_packets_cannot_satisfy_a_real_authorization(clock_plan, tmp_path):
    packet = synthetic_packet(clock_plan, tmp_path)
    assert {f["code"] for f in cr.verify_approval_packet(packet, clock_plan, allow_synthetic=False)["findings"]} >= {"fixture_transport_not_operational", "synthetic_packet"}
    real_named = synthetic_packet(clock_plan, tmp_path, process_transport=pr.TRANSPORT_WSL, real_transport_authorized=True)
    found = cr.verify_approval_packet(real_named, clock_plan, allow_synthetic=True)
    assert "synthetic_for_real_transport" in {f["code"] for f in found["findings"]} and found["real_transport_permitted"] is False
    with pytest.raises(pr.RunnerRefused) as info:
        pr.make_transport(pr.TRANSPORT_WSL, synthetic_authorization=True)
    assert info.value.code == "synthetic_authorization_for_real_transport"
    non_synthetic = synthetic_packet(clock_plan, tmp_path, process_transport=pr.TRANSPORT_WSL, real_transport_authorized=True)
    non_synthetic["synthetic"] = False
    still = cr.verify_approval_packet(non_synthetic, clock_plan)
    assert still["real_transport_permitted"] is False and pr.REAL_TRANSPORT_ENABLED is False
    auth = co.authorization_template(clock_plan)
    assert co.verify_authorization(auth, clock_plan)["valid"] is False


def test_a_clock_only_round_never_needs_the_x13_worker_root_because_it_hashes_only_its_windows_output_directory(clock_plan, tmp_path):
    run_round(clock_plan, tmp_path)
    manifest = json.loads((Path(clock_plan["request"]["output_dir"]) / "integrity_manifest.json").read_text(encoding="utf-8"))
    assert manifest["files"] and {"clock/pre_run.json", "clock/post_run.json", "clock/clock_verdict.json"} <= set(manifest["files"]) and wi.audit_integrity_manifest({"files": {"output_dir": manifest["files"], "worker_evidence_root": {"x": "y"}}}) == []
