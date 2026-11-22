"""A0v Workstream F, safety items: protected channels, no real WSL call, no Docker, no Caliper, no Fabric, no model, no actuator, no Git, and every real-operation switch still off. Static checks read
the SOURCE TEXT of the new modules (``ast``), never import-and-run anything risky; the one subprocess (a harmless interpreter) only reports which modules an import pulls in."""

from __future__ import annotations

import ast
import copy
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from tests import a0v_support as v
from tests.a0v_support import guard  # noqa: F401  (fixture)

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import channel_selection as cs      # noqa: E402
from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import wsl_clock_probe as wp        # noqa: E402
from fabric_perf.controllers import wsl_transport as wt          # noqa: E402

REPO = v.REPO
SRC = REPO / "src" / "fabric_perf" / "controllers"
NEW_MODULES = ("wsl_transport.py", "run_outcome.py", "clock_transport_plan.py")
NEW_SCRIPTS = (REPO / "scripts" / "e1" / "e1_a0v_transport.py", REPO / "docs" / "research" / "e1" / "a0v_example_artifacts_2026-11-22" / "generate_a0v_examples.py")
NEW_SOURCES = [SRC / n for n in NEW_MODULES] + list(NEW_SCRIPTS)
FORBIDDEN_IMPORTS = {"socket", "http", "urllib", "requests", "docker", "grpc", "ssl", "ftplib", "smtplib", "paramiko", "joblib", "sklearn", "numpy", "pandas", "torch", "pickle", "xgboost", "lightgbm"}
pytestmark = pytest.mark.usefixtures("guard")


def parse(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"))


def imported(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module.split(".")[0])
    return names


# ---- 38. protected channels
@pytest.mark.parametrize("channel", ["e1-r002", "e1-cfg001", "E1-R002", " e1-cfg001 ", "E1-CFG001\t"])
def test_38_a_protected_reference_channel_is_rejected_at_every_level(channel):
    assert cs.is_protected_reference_channel(channel) is True
    with pytest.raises(cs.ChannelSelectionError):
        cs.select_experiment_channel(channel)


def test_38_the_bounded_request_validator_rejects_a_protected_channel_and_a_record_for_another_channel_does_not_bind(tmp_path_factory):
    world = v.build_world(tmp_path_factory.mktemp("a0v_safety_world"))
    request = copy.deepcopy(world.plan["request"])
    request["channel"] = "e1-r002"
    issues = be.validate_request(request)
    assert issues and any("channel" in (i.get("field", "") + i.get("code", "")) for i in issues)
    allowed = v.wt.vectors_for_category(v.wt.OPC_CALIPER, world.plan, distro=v.DISTRO, mount_root=v.MOUNT)
    binding = {"run_id": world.plan["run_id"], "channel": world.plan["channel"], "plan_sha256": world.plan["plan_sha256"]}
    record = v.real_record(v.wt.OPC_CALIPER, allowed, binding, tmp_path_factory.mktemp("a0v_safety_rec"))
    other = {**binding, "channel": "e1-r002"}
    findings = {f["code"] for f in wt.verify_activation_record(record, category=wt.OPC_CALIPER, allowed=allowed, binding=other, expected_candidate_manifest_sha256=v.MANIFEST_SHA)}
    assert "binding_mismatch" in findings


# ---- 39-42. no real WSL, Docker, Caliper or Fabric
def test_39_every_real_operation_switch_is_still_off():
    assert wt.REAL_SPAWN_COMPILED_IN is False and pr.REAL_TRANSPORT_ENABLED is False and be.REAL_LAUNCH_ENABLED is False and wp.LIVE_CLOCK_SAMPLING_ENABLED is False
    assert be.PERMITTED_RUNNER_KINDS == ("mock", "fixture")


def test_39_only_the_transport_module_may_start_a_process_and_it_never_uses_a_shell():
    for path in NEW_SOURCES:
        tree = parse(path)
        text = path.read_text(encoding="utf-8")
        names = imported(tree)
        assert not (names & FORBIDDEN_IMPORTS), (path.name, names & FORBIDDEN_IMPORTS)
        assert "shell=True" not in text and "os.system" not in text and "os.popen" not in text and "os.exec" not in text and "os.spawn" not in text, path.name
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
        process_calls = [c for c in calls if isinstance(c.func, ast.Attribute) and isinstance(c.func.value, ast.Name) and c.func.value.id == "subprocess" and c.func.attr in ("run", "call", "check_output", "check_call")]
        assert not process_calls, path.name
        if path.name != "wsl_transport.py":
            assert "subprocess" not in names and "threading" not in names, path.name
    transport_tree = parse(SRC / "wsl_transport.py")
    popen_uses = [n for n in ast.walk(transport_tree) if isinstance(n, ast.Attribute) and n.attr == "Popen"]
    assert len(popen_uses) == 1                                                            # the one place a process could be created, behind the permit checks


@pytest.mark.parametrize("path", NEW_SOURCES, ids=lambda p: p.name)
def test_40_41_42_no_new_file_runs_docker_caliper_or_a_fabric_tool_and_none_reads_the_environment(path):
    tree = parse(path)
    text = path.read_text(encoding="utf-8")
    attrs = {(n.value.id, n.attr) for n in ast.walk(tree) if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name)}
    assert ("os", "environ") not in attrs and ("os", "getenv") not in attrs and not any(a == "sys" and b == "argv" and path.name == "wsl_transport.py" for a, b in attrs)
    literals = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    program_calls = [s for s in literals if re.fullmatch(r"(docker|docker\.exe|caliper|peer|configtxlator|orderer|git|git\.exe|curl|ssh)", s)]
    allowed_mentions = {"wsl_transport.py": {"docker", "docker.exe", "caliper", "peer", "configtxlator", "orderer", "curl", "ssh", "git", "git.exe"}, "clock_transport_plan.py": set(), "run_outcome.py": set()}
    assert set(program_calls) <= allowed_mentions.get(path.name, set()), (path.name, program_calls)      # in wsl_transport these appear ONLY in the fixture-executable FORBIDDEN list


def test_40_the_fixture_forbidden_list_is_the_only_place_wsl_transport_names_a_container_or_fabric_program():
    tree = parse(SRC / "wsl_transport.py")
    holders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "FORBIDDEN_FIXTURE_EXECUTABLES" for t in node.targets):
            holders.append(node)
    assert len(holders) == 1
    others = [n for n in ast.walk(tree) if isinstance(n, ast.Constant) and n.value in ("docker", "caliper", "peer", "configtxlator", "orderer")]
    inside = [n for n in ast.walk(holders[0]) if isinstance(n, ast.Constant) and n.value in ("docker", "caliper", "peer", "configtxlator", "orderer")]
    assert len(others) == len(inside)


def test_39_importing_the_new_modules_adds_no_network_container_or_model_library():
    """The pre-existing A0s import chain already loads http.client / ssl / socket / urllib.request (library imports; no connection is made; reported in the A0v report). The new modules must add NOTHING
    beyond it, and no model library may be loaded at all."""
    code = ("import sys, json; sys.path.insert(0, %r); "
            "import fabric_perf.controllers.bounded_execution, fabric_perf.controllers.process_runner, fabric_perf.controllers.wsl_integrity, fabric_perf.controllers.clock_only_round, "
            "fabric_perf.controllers.clock_only_readiness, fabric_perf.controllers.wsl_clock_probe, fabric_perf.controllers.config_inspection, fabric_perf.controllers.path_mapping; "
            "before = set(sys.modules); "
            "import fabric_perf.controllers.wsl_transport, fabric_perf.controllers.run_outcome, fabric_perf.controllers.clock_transport_plan; "
            "added = sorted(m for m in set(sys.modules) - before if not m.startswith('fabric_perf')); "
            "models = sorted(m for m in ('joblib', 'sklearn', 'numpy', 'pandas', 'torch', 'xgboost', 'lightgbm', 'docker', 'requests', 'urllib3', 'grpc') if m in sys.modules); "
            "print(json.dumps({'added': added, 'models_or_clients': models}))" % str(REPO / "src"))
    result = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert report["models_or_clients"] == [], report
    assert not ({"socket", "ssl", "http", "http.client", "urllib", "urllib.request", "email", "docker", "requests"} & set(report["added"])), report


# ---- 43. no model, 44. no actuator
def test_43_44_the_new_modules_import_no_controller_strategy_predictor_or_actuator():
    for path in NEW_SOURCES:
        text = path.read_text(encoding="utf-8")
        tree = parse(path)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                tail = node.module.split(".")
                assert not (set(tail) & {"strategies", "live_integration", "harness", "telemetry", "models", "artifacts", "predictive", "actuator"}), (path.name, node.module)
        assert "joblib" not in text and ".predict(" not in text and "load_model" not in text, path.name
        assert "update_orderer" not in text and "channel update" not in text.lower() or path.name == "wsl_transport.py"
    transport_text = (SRC / "wsl_transport.py").read_text(encoding="utf-8")
    assert "channel update" not in transport_text.lower() and "broadcast" not in transport_text.lower()


# ---- 45. no Git commit or staging
def test_45_no_new_file_invokes_git():
    for path in NEW_SOURCES:
        tree = parse(path)
        literals = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        assert not ({"git", "git.exe", "commit", "add", "push", "stage"} & literals) or path.name == "wsl_transport.py" and not ({"commit", "add", "push", "stage"} & literals), path.name


def test_45_the_guard_itself_refuses_the_forbidden_programs(guard):
    for name in ("wsl.exe", "docker", "git", "bash"):
        with pytest.raises(AssertionError):
            subprocess.Popen([name, "--version"])
    assert guard == []                                                                     # nothing was started


# ---- the CLI cannot execute
def test_the_cli_refuses_execute_and_sample_with_exit_5_and_ignores_every_argument():
    for words in (["execute"], ["sample"], ["execute", "--real", "--yes", "--distro", "Ubuntu"], ["sample", "--force"]):
        result = subprocess.run([sys.executable, "-I", str(REPO / "scripts" / "e1" / "e1_a0v_transport.py")] + words, capture_output=True, text=True, timeout=60)
        assert result.returncode == 5 and "REFUSED" in result.stdout and "was started" in result.stdout


def test_the_cli_clock_plan_of_the_blank_template_writes_an_unresolved_plan_and_never_overwrites(tmp_path):
    out = tmp_path / "plan.json"
    run = lambda: subprocess.run([sys.executable, "-I", str(REPO / "scripts" / "e1" / "e1_a0v_transport.py"), "clock-plan", "--out", str(out)], capture_output=True, text=True, timeout=60)  # noqa: E731
    first, second = run(), run()
    assert first.returncode == 0 and second.returncode == 2 and "FileExistsError" in second.stdout
    assert json.loads(out.read_text(encoding="utf-8"))["status"] == "UNRESOLVED_NOT_APPROVED_NOT_EXECUTED"


def test_no_environment_variable_changes_the_real_switches(monkeypatch):
    for name in ("REAL_SPAWN_COMPILED_IN", "A0V_REAL", "ENABLE_REAL_TRANSPORT", "REAL_TRANSPORT_ENABLED", "REAL_LAUNCH_ENABLED", "WSL_REAL"):
        monkeypatch.setenv(name, "1")
    assert wt.REAL_SPAWN_COMPILED_IN is False and pr.REAL_TRANSPORT_ENABLED is False and be.REAL_LAUNCH_ENABLED is False
