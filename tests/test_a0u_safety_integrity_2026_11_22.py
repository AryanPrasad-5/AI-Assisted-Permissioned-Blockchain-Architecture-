"""A0u safety and integrity: every real operation stays disabled, no A0u library can start a process, nothing is approved, earlier manifests, frozen files and the capacity-bracket design are
byte-identical, the A0u candidate manifest matches the files, and nothing is staged or committed. Static source checks, stored-file hashes and the offline CLI only."""

from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path

import pytest

from tests import a0t_support as t
from tests import a0u_support as u
from tests.a0s_support import REPO
from tests.test_a0s_integrity_2026_11_22 import EARLIER, FORBIDDEN_IMPORTS, FORBIDDEN_MODEL_IMPORTS, PINNED, imports_of

import e1_a0u_candidate_manifest as mk        # noqa: E402
import e1_a0u_integrity as cli                # noqa: E402

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi      # noqa: E402
from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi          # noqa: E402

A0U_LIBS = ["src/fabric_perf/controllers/tree_digest.py", "src/fabric_perf/controllers/wsl_integrity.py", "src/fabric_perf/controllers/runtime_discovery.py",
            "src/fabric_perf/controllers/clock_only_readiness.py"]
A0U_SCRIPTS = ["scripts/e1/e1_wsl_tree_digest.py", "scripts/e1/e1_a0u_integrity.py", "scripts/e1/e1_a0u_candidate_manifest.py"]
A0U_FILES = A0U_LIBS + A0U_SCRIPTS
MANIFEST = REPO / "docs" / "research" / "e1" / mk.SUCCESSOR_NAME
EXAMPLES = REPO / "docs" / "research" / "e1" / "a0u_example_artifacts_2026-11-22"
A0T_EXAMPLES = REPO / "docs" / "research" / "e1" / "a0t_example_artifacts_2026-11-22"
A0S_MANIFEST_SHA = "026c167a948f2b82e0ead45a079cc838ed57d4e698041f762c3223d858a6b167"
A0T_MANIFEST = ("prelive_candidate_manifest_2026-11-22_a0t.json", "529e5d5ed35ef7dd0cb74534db3088c08627d09b674ea2bbda23bd3227a013f4")
FROZEN_A0T_EXAMPLES = {"capacity_bracket_design.json": "ce8e828ec888da5588e3479afe1b98a4d32ef098c490a526d2132ca33c4d6b1c",
                       "capacity_bracket_schedule_ILLUSTRATIVE.json": "2af51c51015e7066017bf19f830c942ee39754a215f56f3fbdade08f8777bf61",
                       "capacity_bracket_decision_record_TEMPLATE_UNSET.json": "02f5cc2443aba06829acf6c1bf9af12157311ab59de3f4b0134429eac5f0f413",
                       "synthetic_smoke_plan.json": "9f59b1a65d3036581b28c262aa8db9e5515f28099396492d3272128183097b86",
                       "clock_only_authorization_TEMPLATE_UNSET.json": "b2754ae2771ced309ecf8a8aec92b0e66cace42b7e67a2d5b5de94a4f4bd2fa2",
                       "smoke_authorization_template_UNSET.json": "c7b435a11a1e006ab53a362de02e32f9da0558947458f3abeba46024ebab6411"}
CAPACITY_BRACKET_PY = "a62776d0cb1e4cda58ef7078573b9e88b02462290c90358005a587c8357b4cc4"
HEAD = "8c67a4678b3901c8d601fb1a33210d7d8c84cf78"


# ---- 38-42: the real transport and every real operation stay disabled; no library can start a process or load a model
def test_38_every_real_operation_flag_is_false_and_assigned_exactly_once():
    for rel, name in (("src/fabric_perf/controllers/process_runner.py", "REAL_TRANSPORT_ENABLED"), ("src/fabric_perf/controllers/bounded_execution.py", "REAL_LAUNCH_ENABLED"),
                      ("src/fabric_perf/controllers/config_inspection.py", "LIVE_INSPECTION_ENABLED"), ("src/fabric_perf/controllers/wsl_clock_probe.py", "LIVE_CLOCK_SAMPLING_ENABLED"),
                      ("src/fabric_perf/controllers/stage3_predictive.py", "REAL_INFERENCE_AUTHORIZED")):
        assert re.findall(rf"^{name}\s*=\s*(\w+)", (REPO / rel).read_text(encoding="utf-8"), flags=re.MULTILINE) == ["False"], (rel, name)
    assert (pr.REAL_TRANSPORT_ENABLED, be.REAL_LAUNCH_ENABLED) == (False, False)
    for rel in A0U_FILES:
        text = (REPO / rel).read_text(encoding="utf-8")
        assert not re.search(r"(REAL_TRANSPORT_ENABLED|REAL_LAUNCH_ENABLED|LIVE_INSPECTION_ENABLED|LIVE_CLOCK_SAMPLING_ENABLED|REAL_INFERENCE_AUTHORIZED)\s*=\s*True", text), rel
        assert "global REAL_" not in text and "setattr(" not in text, rel


def test_38_no_a0u_file_reads_the_environment_except_one_attestation_in_the_wsl_side_script():
    for rel in A0U_FILES:
        text = (REPO / rel).read_text(encoding="utf-8")
        if rel == "scripts/e1/e1_wsl_tree_digest.py":
            assert text.count("os.environ") == 1 and text.count('os.environ.get("WSL_DISTRO_NAME")') == 1 and "getenv" not in text
            continue
        for needle in ("os.environ", "os.getenv", "getenv(", "environ[", "environ.get"):
            assert needle not in text, (rel, needle)


@pytest.mark.parametrize("path", A0U_FILES)
def test_38_to_40_no_a0u_file_imports_a_process_network_or_container_api(path):
    allowed = {"os"} if path == "scripts/e1/e1_wsl_tree_digest.py" else set()
    assert imports_of(path) & FORBIDDEN_IMPORTS == allowed
    text = (REPO / path).read_text(encoding="utf-8")
    for needle in ("Popen(", "subprocess", "os.system", "shell=True", "socket.", "urlopen", "import requests", "requests.get", "requests.post", "requests.Session", "docker.from_env", "paramiko",
                   "os.popen", "ctypes"):                       # (the A0t needle 'requests.' would match this module's local variable 'requests.values()')
        assert needle not in text, (path, needle)
    called = {n.func.id for n in ast.walk(ast.parse(text)) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert not called & {"exec", "eval", "compile", "__import__", "open_process"}


@pytest.mark.parametrize("path", A0U_FILES)
def test_41_and_42_no_a0u_file_imports_a_model_forecaster_or_actuator(path):
    assert imports_of(path) & FORBIDDEN_MODEL_IMPORTS == set()
    names = {n.id for n in ast.walk(ast.parse((REPO / path).read_text(encoding="utf-8"))) if isinstance(n, ast.Name)}
    assert not names & {"joblib", "pickle", "HFBTPArtifactPerformancePredictor", "FlashbotsSeriesForecaster", "PredictiveController", "ReactiveController", "ConfigActuator"}


def test_38_the_digest_tool_never_writes_deletes_or_starts_anything():
    text = (REPO / "scripts/e1/e1_wsl_tree_digest.py").read_text(encoding="utf-8")
    for needle in (".write_text(", ".write_bytes(", "unlink", "rmdir", "rename(", "chmod", "mkdir", "open(\"w", "\"wb\"", "\"xb\"", "shutil"):
        assert needle not in text, needle
    assert text.count("sys.stdout.write(") == 1


def test_38_to_40_the_real_transport_and_the_real_launch_are_refused_by_the_a0u_wiring(tmp_path):
    plan, req, root = u.build_plan(tmp_path)
    record = t.path_record(plan)
    addendum = u.make_addendum(plan, record)
    with pytest.raises(be.LaunchRefused):
        u.run_with_integrity(tmp_path, plan, record, addendum, pr.DisabledWslTransport())
    with pytest.raises(pr.TransportDisabled):
        pr.DisabledWslTransport().run(None)
    assert not Path(plan["layout"]["output_dir"]).exists()


def test_cli_execute_and_sample_are_refused_with_exit_5_before_parsing(capsys, monkeypatch):
    for name in ("A0U_REAL_TRANSPORT", "A0T_REAL_TRANSPORT", "EXECUTE", "A0S_REAL_LAUNCH", "REAL_TRANSPORT_ENABLED", "WSL_DISTRO_NAME"):
        monkeypatch.setenv(name, "1")
    assert (cli.main(["execute"]), cli.main(["execute", "--plan", "x", "--approved"]), cli.main(["sample"])) == (5, 5, 5)
    assert capsys.readouterr().out.count("REFUSED") == 3


# ---- 43: protected channels
@pytest.mark.parametrize("channel", ["e1-r002", "e1-cfg001", "e1-b001", "e1-b002", "e1-b003", "e1-e00a", "e1-r001", "E1-R002", " e1-r002", "e1-cp01-b10-l20", "e1-ql01-b40-l15", "e1-rp01-b10-r1"])
def test_43_protected_historical_and_reserved_channels_are_rejected_in_the_a0u_plan(tmp_path, channel):
    plan, req, root = u.build_plan(tmp_path)
    bad = dict(req)
    bad["channel"] = channel
    with pytest.raises(be.RequestInvalid) as info:
        be.build_execution_plan(bad, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    assert "channel_refused" in {i["code"] for i in info.value.issues}


def test_43_an_authorization_for_a_plan_forged_onto_a_protected_channel_is_refused(tmp_path):
    from tests import a0s_support as s
    plan, req, root = u.build_plan(tmp_path)
    forged = json.loads(json.dumps(plan))
    forged["channel"] = "e1-r002"
    forged["plan_sha256"] = be._digest(forged, drop=("plan_sha256", "created_utc"))
    verdict = be.verify_authorization(s.approved_auth(forged, tmp_path), forged, repo_root=REPO, allow_synthetic=True)
    assert verdict["valid"] is False and "channel_refused" in {f["code"] for f in verdict["findings"]}


# ---- defaults
def test_44_the_safety_relevant_a0u_parameters_have_no_defaults():
    import inspect
    from fabric_perf.controllers import tree_digest as td
    for fn, names in ((wi.build_integrity_addendum, ("distro", "mount_root", "symlink_policy", "limits", "digest_timeout_s", "repo_root")), (td.digest_tree, ("approved_root", "expected_type", "symlink_policy", "max_entries", "max_total_bytes", "max_depth")),
                      (wi.execute_with_integrity, ("expected_category", "allow_synthetic", "repo_root")), (wi.IntegrityBoundRunner.__init__, ("addendum", "distro", "mount_root", "max_process_lifetime_s")),
                      (td.build_evidence, ("observed_distro", "observed_utc", "platform"))):
        params = inspect.signature(fn).parameters
        for name in names:
            assert params[name].default is inspect.Parameter.empty, (fn.__qualname__, name)
    assert inspect.signature(wi.verify_a0u_authorization).parameters["allow_synthetic"].default is False
    assert set(td.REQUEST_KEYS) == {"run_id", "variant", "plan_sha256", "distro", "phase", "operation", "approved_root", "target", "expected_type", "symlink_policy",
                                    "max_entries", "max_total_bytes", "max_depth", "path_record_sha256"}


# ---- the offline CLI
def write_json(path: Path, doc) -> Path:
    path.write_text(json.dumps(doc), encoding="utf-8")
    return path


def test_cli_addendum_verify_template_discovery_and_clock_commands_work_offline_and_never_overwrite(tmp_path, capsys):
    plan, req, root = u.build_plan(tmp_path)
    plan_path, record_path = write_json(tmp_path / "plan.json", plan), write_json(tmp_path / "record.json", t.path_record(plan))
    out = tmp_path / "addendum.json"
    args = ["addendum", "--plan", str(plan_path), "--path-record", str(record_path), "--distro", "Ubuntu", "--mount-root", "/mnt", "--symlink-policy", "refuse", "--max-entries", "200",
            "--max-bytes", "10000000", "--max-depth", "8", "--digest-timeout-s", "30", "--out", str(out)]
    assert cli.main(args) == 0 and out.is_file()
    assert cli.main(args) == 2 and "REFUSED" in capsys.readouterr().out
    capsys.readouterr()
    assert cli.main(["verify-addendum", "--plan", str(plan_path), "--path-record", str(record_path), "--addendum", str(out)]) == 0
    assert json.loads(capsys.readouterr().out)["problems"] == []
    template = tmp_path / "auth.json"
    assert cli.main(["authorization-template", "--plan", str(plan_path), "--path-record", str(record_path), "--addendum", str(out), "--distro", "Ubuntu", "--category", pr.OP_SMOKE, "--out", str(template)]) == 0
    doc = json.loads(template.read_text(encoding="utf-8"))
    assert doc["approval"] == {k: None for k in be.APPROVAL_KEYS} and [k for k in wi.A0U_OWNER_KEYS if doc["a0u"][k] is not None] == []
    capsys.readouterr()
    assert cli.main(["verify-authorization", "--plan", str(plan_path), "--path-record", str(record_path), "--addendum", str(out), "--authorization", str(template), "--category", pr.OP_SMOKE]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is False and payload["real_transport_permitted"] is False and "a0u_field_unset" in {f["code"] for f in payload["a0u"]["findings"]}
    spec = tmp_path / "spec.json"
    assert cli.main(["discovery-spec", "--plan", str(plan_path), "--distro", "Ubuntu", "--out", str(spec)]) == 0
    assert json.loads(spec.read_text(encoding="utf-8"))["status"] == "SPECIFICATION_ONLY_NOT_EXECUTED"
    clock_plan = A0T_EXAMPLES / "synthetic_clock_only_plan.json"
    assert cli.main(["clock-only-audit", "--plan", str(clock_plan)]) == 0
    packet = tmp_path / "packet.json"
    assert cli.main(["clock-only-packet", "--plan", str(clock_plan), "--out", str(packet)]) == 0
    assert cli.main(["clock-only-packet", "--plan", str(clock_plan), "--out", str(packet)]) == 2


def test_cli_reports_invalid_input_without_creating_anything(tmp_path, capsys):
    plan, req, root = u.build_plan(tmp_path)
    plan_path, record_path = write_json(tmp_path / "plan.json", plan), write_json(tmp_path / "record.json", t.path_record(plan))
    out = tmp_path / "bad.json"
    assert cli.main(["addendum", "--plan", str(plan_path), "--path-record", str(record_path), "--distro", "bad name", "--mount-root", "/mnt", "--symlink-policy", "refuse", "--max-entries", "5",
                     "--max-bytes", "5", "--max-depth", "5", "--digest-timeout-s", "5", "--out", str(out)]) == 2 and not out.exists()
    capsys.readouterr()


# ---- approvals stay unset; frozen files, earlier manifests and the capacity-bracket design are byte-identical
def test_every_a0u_template_leaves_every_owner_and_approval_field_unset():
    names = sorted(p.name for p in EXAMPLES.glob("*UNSET*.json"))
    assert names, "the A0u example artifacts have not been generated"
    for name in names:
        doc = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
        assert doc["approval"] == {k: None for k in be.APPROVAL_KEYS} and doc.get("synthetic", False) is False, name
        if "a0u" in doc:
            assert [k for k in wi.A0U_OWNER_KEYS if doc["a0u"][k] is not None] == [] and doc["a0t"]["process_transport"] is None and doc["a0t"]["real_transport_authorized"] is None
        if "owner" in doc:
            owner = doc["owner"]
            assert all(v is None for k, v in owner.items() if k not in ("approved_clock_pair", "approved_output_paths"))
            assert all(v is None for v in owner["approved_clock_pair"].values()) and owner["approved_output_paths"] == {"output_dir": None}


def test_earlier_manifests_including_the_a0s_and_a0t_manifests_and_pinned_artifacts_are_byte_identical():
    for name, want in {**EARLIER, "prelive_candidate_manifest_2026-11-22_a0s.json": A0S_MANIFEST_SHA, A0T_MANIFEST[0]: A0T_MANIFEST[1]}.items():
        assert hashlib.sha256((REPO / "docs" / "research" / "e1" / name).read_bytes()).hexdigest() == want, name
    for rel, want in PINNED.items():
        assert pi.sha256_file(REPO / rel) == want, rel


def test_the_capacity_bracket_design_and_its_builder_are_unchanged():
    for name, want in FROZEN_A0T_EXAMPLES.items():
        assert hashlib.sha256((A0T_EXAMPLES / name).read_bytes()).hexdigest() == want, name
    assert pi.sha256_file(REPO / "src/fabric_perf/controllers/capacity_bracket.py") == CAPACITY_BRACKET_PY
    design = json.loads((A0T_EXAMPLES / "capacity_bracket_design.json").read_text(encoding="utf-8"))
    assert design["approved"] is False and design["configurations"]["static_baseline"]["cell_id"].startswith("b10-t2000ms") and design["configurations"]["alternative"]["cell_id"].startswith("b40-t2000ms")


def test_the_frozen_model_artifacts_equal_their_pins_without_being_loaded():
    pins = json.loads((REPO / "Integration/system_integration/offline_replay_inputs.json").read_text(encoding="utf-8"))["files"]
    for key in ("model_metadata", "throughput_model", "latency_model"):
        assert hashlib.sha256((REPO / pins[key]["path"]).read_bytes()).hexdigest().upper() == pins[key]["sha256"], key


# ---- the A0u candidate manifest
def test_the_a0u_manifest_matches_the_files_stays_unapproved_and_supersedes_a0t():
    assert MANIFEST.is_file(), "the A0u manifest has not been written"
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert pi.verify_candidate_manifest(manifest, REPO) == []
    assert manifest["status"] == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest["approval_record"] is None
    assert all(e["approval_status"] != "APPROVED" for e in manifest["entries"])
    assert [e["path"] for e in manifest["entries"] if e["pinned_sha256"] is not None and e["path"] in mk.A0U_NEW_PATHS] == []
    assert manifest["changed_since_a0t"] == [] and manifest["superseded_entries"] == [] and manifest["removed_since_a0t"] == []
    assert manifest["added_since_a0t"] == mk.A0U_NEW_PATHS and len(manifest["entries"]) == 114 + len(mk.A0U_NEW_PATHS)
    assert manifest["supersedes"]["sha256"] == A0T_MANIFEST[1]
    assert manifest["launch_configuration"]["live_run"] is False and manifest["runtime_dependencies"]["process_or_network_api_imported_by_a0u_files"] is False
    assert manifest["architecture_recommendation"]["N2_STATUS"] == "RECOMMENDED_NOT_APPROVED"
    assert not any(item.startswith(mk.X13_A0T_ITEM) for item in manifest["unresolved_dependencies"])
    assert any("Caliper container" in item for item in manifest["unresolved_dependencies"])


def test_every_a0t_manifest_entry_is_still_at_its_a0t_hash():
    a0t = json.loads((REPO / "docs" / "research" / "e1" / A0T_MANIFEST[0]).read_text(encoding="utf-8"))
    assert [e["path"] for e in a0t["entries"] if pi.sha256_file(REPO / e["path"]) != e["sha256"]] == []


def test_every_file_the_a0u_tests_and_tools_rely_on_is_listed_in_the_manifest():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    paths = {e["path"] for e in manifest["entries"]}
    assert set(A0U_FILES) <= paths and set(be.REQUIRED_TOOL_FILES) <= paths and {wi.DIGEST_TOOL, wi.DIGEST_LIB} <= paths


def test_a_freshly_built_manifest_equals_the_stored_one_apart_from_git():
    stored = json.loads(MANIFEST.read_text(encoding="utf-8"))
    fresh = mk.build(created_utc=stored["created_utc"])
    stored.pop("git", None), fresh.pop("git", None)
    assert fresh == stored


# ---- the stored A0u examples
def test_the_stored_a0u_plan_addendum_and_path_record_verify_against_each_other_and_the_manifest():
    plan = json.loads((EXAMPLES / "synthetic_smoke_plan_a0u.json").read_text(encoding="utf-8"))
    record = json.loads((EXAMPLES / "synthetic_path_mapping_record.json").read_text(encoding="utf-8"))
    addendum = json.loads((EXAMPLES / "integrity_addendum_SYNTHETIC_LIMITS.json").read_text(encoding="utf-8"))
    assert plan["candidate_manifest"]["path"].endswith(mk.SUCCESSOR_NAME) and plan["candidate_manifest"]["sha256"] == pi.sha256_file(MANIFEST)
    assert be._digest(plan, drop=("plan_sha256", "created_utc")) == plan["plan_sha256"] and wi.verify_addendum(addendum, plan, record, repo_root=REPO) == []
    assert addendum["status"] == "PLANNED_NOT_AUTHORIZED_NOT_EXECUTED" and addendum["real_transport_enabled"] is False
    assert [s["step"] for s in addendum["steps"]] == ["I1_worker_root_pre", "I2_worker_root_post", "I3_output_dir_host"]
    assert next(s for s in plan["steps"] if s["step"] == "X13_integrity")["argv"] is None and plan["real_launch_enabled"] is False


def test_the_stored_defect_reproduction_shows_the_empty_worker_manifest():
    doc = json.loads((EXAMPLES / "x13_defect_reproduction.json").read_text(encoding="utf-8"))
    assert doc["a0s_status"] == "COMPLETED" and doc["worker_evidence_root_files"] == 0 and doc["audit"] == ["worker_evidence_root_empty"] and doc["simulated"] is True


def test_the_stored_simulated_verdict_is_labelled_simulated_and_satisfies_no_gate():
    doc = json.loads((EXAMPLES / "simulated_run_integrity_verdict_SIMULATED.json").read_text(encoding="utf-8"))
    assert doc["verdict"] == wi.V_SIM and doc["evidence_class"] == "SIMULATED" and doc["gate_satisfied"] is False


# ---- nothing staged, committed or pushed
def test_46_nothing_is_staged_and_head_is_unchanged():
    assert pi.read_git_head(REPO)["commit"] == HEAD
    index = (REPO / ".git" / "index").read_bytes()
    for rel in A0U_FILES + ["tests/a0u_support.py", "tests/test_a0u_x13_2026_11_22.py"]:
        assert rel.encode("utf-8") not in index, f"{rel} is staged"
