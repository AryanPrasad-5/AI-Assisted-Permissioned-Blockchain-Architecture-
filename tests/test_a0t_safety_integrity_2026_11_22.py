"""A0t safety and integrity: every real operation stays disabled, no library file can start a process, nothing is approved, earlier manifests and frozen files are byte-identical, and the
A0t candidate manifest matches the files. Static source checks and stored-file hashes only."""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import re
from pathlib import Path

import pytest

from tests import a0t_support as t
from tests.a0s_support import REPO
from tests.test_a0s_integrity_2026_11_22 import EARLIER, FORBIDDEN_IMPORTS, FORBIDDEN_MODEL_IMPORTS, PINNED, imports_of

import e1_a0t_candidate_manifest as mk      # noqa: E402

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import capacity_bracket as cb       # noqa: E402
from fabric_perf.controllers import clock_only_round as co       # noqa: E402
from fabric_perf.controllers import path_mapping as pm           # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi      # noqa: E402
from fabric_perf.controllers import process_runner as pr         # noqa: E402

A0T_FILES = ["src/fabric_perf/controllers/path_mapping.py", "src/fabric_perf/controllers/process_runner.py", "src/fabric_perf/controllers/clock_only_round.py",
             "src/fabric_perf/controllers/capacity_bracket.py", "scripts/e1/e1_a0t_process_runner.py", "scripts/e1/e1_a0t_candidate_manifest.py"]
MANIFEST = REPO / "docs" / "research" / "e1" / mk.SUCCESSOR_NAME
EXAMPLES = REPO / "docs" / "research" / "e1" / "a0t_example_artifacts_2026-11-22"
A0S_MANIFEST = "prelive_candidate_manifest_2026-11-22_a0s.json"
A0S_MANIFEST_SHA = "026c167a948f2b82e0ead45a079cc838ed57d4e698041f762c3223d858a6b167"


# ---- 37: every real operation flag is False
def test_37_every_real_operation_flag_is_false_and_assigned_exactly_once():
    for rel, name in (("src/fabric_perf/controllers/process_runner.py", "REAL_TRANSPORT_ENABLED"), ("src/fabric_perf/controllers/bounded_execution.py", "REAL_LAUNCH_ENABLED"),
                      ("src/fabric_perf/controllers/config_inspection.py", "LIVE_INSPECTION_ENABLED"), ("src/fabric_perf/controllers/wsl_clock_probe.py", "LIVE_CLOCK_SAMPLING_ENABLED"),
                      ("src/fabric_perf/controllers/stage3_predictive.py", "REAL_INFERENCE_AUTHORIZED")):
        text = (REPO / rel).read_text(encoding="utf-8")
        assert re.findall(rf"^{name}\s*=\s*(\w+)", text, flags=re.MULTILINE) == ["False"], (rel, name)
    assert (pr.REAL_TRANSPORT_ENABLED, be.REAL_LAUNCH_ENABLED) == (False, False)


def test_37_no_a0t_source_can_turn_a_flag_on_and_none_reads_the_environment():
    for rel in A0T_FILES:
        text = (REPO / rel).read_text(encoding="utf-8")
        assert not re.search(r"(REAL_TRANSPORT_ENABLED|REAL_LAUNCH_ENABLED|LIVE_INSPECTION_ENABLED|LIVE_CLOCK_SAMPLING_ENABLED|REAL_INFERENCE_AUTHORIZED)\s*=\s*True", text), rel
        assert "global REAL_" not in text and "setattr(" not in text, rel
        for needle in ("os.environ", "os.getenv", "getenv(", "environ[", "environ.get"):
            assert needle not in text, (rel, needle)


# ---- 38, 39, 40: Caliper, WSL, Fabric cannot be launched or contacted by any A0t library or CLI file
@pytest.mark.parametrize("path", A0T_FILES)
def test_38_to_40_no_a0t_library_or_cli_file_imports_a_process_network_container_or_environment_api(path):
    assert imports_of(path) & FORBIDDEN_IMPORTS == set()
    text = (REPO / path).read_text(encoding="utf-8")
    for needle in ("Popen(", "subprocess", "os.system", "shell=True", "socket.", "urlopen", "requests.", "docker.from_env", "paramiko", "os.popen", "ctypes"):
        assert needle not in text, (path, needle)
    tree = ast.parse(text)
    called = {n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert not called & {"exec", "eval", "compile", "__import__", "open_process"}


def test_38_to_40_the_only_process_starting_test_code_is_the_fixture_transport_the_bystander_and_one_mklink():
    for path in sorted((REPO / "tests").glob("*a0t*.py")):
        text = path.read_text(encoding="utf-8")
        for match in re.finditer(r"(Popen|subprocess\.run)\(\s*(\[[^\]]*\]|[A-Za-z_]+)", text):
            head = match.group(2)
            assert head.startswith("[sys.executable") or head.startswith('["cmd"') or head == "argv" or head == "code", (path.name, match.group(0))
    support = (REPO / "tests" / "a0t_support.py").read_text(encoding="utf-8")
    assert support.count("subprocess.Popen(") == 1 and "[sys.executable, \"-I\", \"-c\", code]" in support


def test_38_to_40_the_fixture_transport_never_runs_the_invocation_it_is_given():
    support = (REPO / "tests" / "a0t_support.py").read_text(encoding="utf-8")
    body = support[support.index("class FixtureTransport"):support.index("# ---- plans, records")]
    assert "inv.os_argv" not in body and "plan_argv" not in body, "the fixture runs a script chosen by the test, never the vector"


# ---- 41, 42: no actuator, no model, no inference
@pytest.mark.parametrize("path", A0T_FILES)
def test_41_and_42_no_a0t_file_imports_a_model_forecaster_or_actuator(path):
    assert imports_of(path) & FORBIDDEN_MODEL_IMPORTS == set()
    names = {n.id for n in ast.walk(ast.parse((REPO / path).read_text(encoding="utf-8"))) if isinstance(n, ast.Name)}
    assert not names & {"joblib", "pickle", "HFBTPArtifactPerformancePredictor", "FlashbotsSeriesForecaster", "PredictiveController", "ReactiveController", "ConfigActuator"}


# ---- 43: protected channels
@pytest.mark.parametrize("channel", ["e1-r002", "e1-cfg001", "e1-b001", "e1-b002", "e1-b003", "e1-e00a", "e1-r001", "E1-R002", " e1-r002", "e1-cp01-b10-l20", "e1-ql01-b40-l15", "e1-rp01-b10-r1"])
def test_43_protected_historical_and_reserved_channels_are_rejected_everywhere(tmp_path, channel):
    plan, req, root = t.build_a0t_plan(tmp_path)
    bad = dict(req)
    bad["channel"] = channel
    with pytest.raises(be.RequestInvalid) as info:
        be.build_execution_plan(bad, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    assert "channel_refused" in {i["code"] for i in info.value.issues}


def test_43_the_clock_only_round_names_no_channel_and_the_schedule_uses_only_selectable_new_channels():
    plan_text = json.dumps(json.loads((EXAMPLES / "synthetic_clock_only_plan.json").read_text(encoding="utf-8")))
    assert "e1-" not in plan_text and '"channel_created": false' in plan_text
    schedule = json.loads((EXAMPLES / "capacity_bracket_schedule_ILLUSTRATIVE.json").read_text(encoding="utf-8"))
    from fabric_perf.controllers import channel_selection as cs
    for run in schedule["runs"]:
        assert cs.select_experiment_channel(run["channel"]) == run["channel"]


# ---- 44: no implicit defaults
def test_44_the_safety_relevant_parameters_have_no_defaults():
    def required(fn, *names):
        params = inspect.signature(fn).parameters
        for name in names:
            assert name in params and params[name].default is inspect.Parameter.empty, (fn.__qualname__, name)

    required(pm.windows_to_wsl, "mount_root")
    required(pm.wsl_to_windows, "mount_root")
    required(pm.translate_argv_for_wsl, "mount_root")
    required(pm.build_path_record, "mount_root", "container_mount", "repo_declared_wsl", "repo_root_windows")
    required(pr.build_invocation, "launch_mode", "distro", "mount_root")
    required(pr.PlanBoundRunner.__init__, "distro", "mount_root", "max_process_lifetime_s")
    required(pr.describe_plan_invocations, "distro", "mount_root")
    required(pr.verify_a0t_authorization, "expected_category")                  # allow_synthetic defaults to False: the refusing direction, as in bounded_execution.verify_authorization
    assert inspect.signature(pr.verify_a0t_authorization).parameters["allow_synthetic"].default is False
    assert inspect.signature(co.verify_authorization).parameters["allow_synthetic"].default is False
    required(pr.execute_through_adapter, "expected_category", "allow_synthetic")
    required(pr.make_transport, "synthetic_authorization")
    required(co.execute_clock_only, "allow_synthetic", "wall_ns", "mono_ns", "sleep", "now_utc", "hash_tree")
    required(cb.build_schedule, "seed", "rungs_tps", "replicates", "channel_family")
    required(cb.build_design, "measured_pairs")
    for key in co.REQUEST_KEYS:
        assert co.blank_request_template()[key] is None or key == "schema"


def test_44_nothing_is_defaulted_in_the_a0t_authorization_extension():
    ext = json.loads((EXAMPLES / "smoke_authorization_template_UNSET.json").read_text(encoding="utf-8"))["a0t"]
    assert [k for k in pr.A0T_KEYS if ext[k] in (None, "")] == ["process_transport", "execution_environment", "distro", "max_process_lifetime_s", "real_transport_authorized"]


# ---- the CLI
def test_cli_execute_and_sample_are_refused_with_exit_5_before_parsing(capsys, monkeypatch):
    import e1_a0t_process_runner as cli
    for name in ("A0T_REAL_TRANSPORT", "EXECUTE", "A0S_REAL_LAUNCH", "REAL_TRANSPORT_ENABLED"):
        monkeypatch.setenv(name, "1")
    assert (cli.main(["execute"]), cli.main(["execute", "--plan", "x", "--approved"]), cli.main(["sample"])) == (5, 5, 5)
    assert capsys.readouterr().out.count("REFUSED") == 3


def test_cli_dry_commands_work_offline_and_template_verification_fails(capsys):
    import e1_a0t_process_runner as cli
    plan = EXAMPLES / "synthetic_smoke_plan.json"
    record = EXAMPLES / "synthetic_path_mapping_record.json"
    assert cli.main(["inspect-invocations", "--plan", str(plan), "--distro", "Ubuntu", "--mount-root", "/mnt", "--path-record", str(record)]) == 0
    description = json.loads(capsys.readouterr().out)
    assert description["nothing_started"] is True and description["real_transport_enabled"] is False
    assert description == json.loads((EXAMPLES / "synthetic_planned_invocations.json").read_text(encoding="utf-8"))
    assert cli.main(["verify-authorization", "--plan", str(plan), "--path-record", str(record), "--authorization", str(EXAMPLES / "smoke_authorization_template_UNSET.json"),
                     "--category", "bounded_transaction_smoke"]) == 1
    assert json.loads(capsys.readouterr().out)["real_transport_permitted"] is False
    assert cli.main(["clock-only-verify", "--plan", str(EXAMPLES / "synthetic_clock_only_plan.json"), "--authorization", str(EXAMPLES / "clock_only_authorization_TEMPLATE_UNSET.json")]) == 1
    capsys.readouterr()


def test_cli_refuses_to_overwrite_and_reports_invalid_requests(tmp_path, capsys):
    import e1_a0t_process_runner as cli
    target = tmp_path / "template.json"
    args = ["clock-only-template", "--plan", str(EXAMPLES / "synthetic_clock_only_plan.json"), "--out", str(target)]
    assert cli.main(args) == 0
    assert cli.main(args) == 2 and "REFUSED" in capsys.readouterr().out
    bad = tmp_path / "bad_request.json"
    bad.write_text(json.dumps(co.blank_request_template()), encoding="utf-8")
    assert cli.main(["clock-only-plan", "--request", str(bad), "--out-plan", str(tmp_path / "p.json"), "--created-utc", "2026-11-22T09:00:00Z"]) == 2
    assert not (tmp_path / "p.json").exists()


# ---- approvals stay unset
@pytest.mark.parametrize("name", sorted(p.name for p in EXAMPLES.glob("*UNSET*.json")))
def test_every_template_leaves_every_approval_field_unset(name):
    doc = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    if "approval" in doc:                                                  # a request template carries no approval block at all
        assert doc["approval"] == {k: None for k in be.APPROVAL_KEYS}
    else:
        assert "request" in name and doc["schema"] == co.REQUEST_SCHEMA
    assert doc.get("synthetic", False) is False
    for key in ("process_transport", "execution_environment", "real_transport_authorized", "max_offered_rate_tps", "series_authorization_mechanism"):
        assert doc.get(key) is None
    ext = doc.get("a0t")
    if ext:
        assert ext["process_transport"] is None and ext["real_transport_authorized"] is None and ext["execution_environment"] is None


def test_the_design_in_the_examples_is_the_proposed_design_and_validates():
    stored = json.loads((EXAMPLES / "capacity_bracket_design.json").read_text(encoding="utf-8"))
    results = json.loads((REPO / "docs/research/e1/a0s_headroom_verification_2026-11-22/headroom_results.json").read_text(encoding="utf-8"))
    assert stored == json.loads(cb.design_json(cb.build_design(measured_pairs=results["measured_fabric_evidence"]["pairs"])))
    assert cb.validate_design(stored) == [] and stored["approved"] is False
    schedule = json.loads((EXAMPLES / "capacity_bracket_schedule_ILLUSTRATIVE.json").read_text(encoding="utf-8"))
    assert schedule["status"].startswith("ILLUSTRATIVE_NOT_AUTHORIZED") and schedule["run_count"] == 38


def test_the_smoke_cell_rendered_offline_matches_the_proposal_and_a_supported_cell():
    v = json.loads((EXAMPLES / "smoke_cell_verification.json").read_text(encoding="utf-8"))
    assert v["matches_proposal"] is True and v["rendered_planned_transactions"] == 1800 and v["workers"] == 1 and v["cell_id"] == "b10-t2000ms-p10485760-a103809024-o3"
    assert [r["tps"] for r in v["rendered_rounds"]] == [20, 20] and [r["txDuration"] for r in v["rendered_rounds"]] == [30, 60]
    assert "P1b Tier 1" in v["supported_by_measured_cell"] and "not_verified" in v


def test_the_stored_path_record_verifies_and_makes_no_runtime_claim():
    rec = json.loads((EXAMPLES / "synthetic_path_mapping_record.json").read_text(encoding="utf-8"))
    assert pm.verify_path_record(rec) == [] and rec["runtime_verified"] is False and rec["automount_root"] == "/mnt"
    assert {e["runtime_status"] for e in rec["entries"]} == {pm.RUNTIME_STATUS}


# ---- earlier manifests and frozen files
def test_earlier_manifests_including_the_a0s_manifest_and_pinned_artifacts_are_byte_identical():
    for name, want in {**EARLIER, A0S_MANIFEST: A0S_MANIFEST_SHA}.items():
        assert hashlib.sha256((REPO / "docs" / "research" / "e1" / name).read_bytes()).hexdigest() == want, name
    for rel, want in PINNED.items():
        assert pi.sha256_file(REPO / rel) == want, rel


def test_the_frozen_model_artifacts_equal_their_pins_without_being_loaded():
    pins = json.loads((REPO / "Integration/system_integration/offline_replay_inputs.json").read_text(encoding="utf-8"))["files"]
    for key in ("model_metadata", "throughput_model", "latency_model"):
        assert hashlib.sha256((REPO / pins[key]["path"]).read_bytes()).hexdigest().upper() == pins[key]["sha256"], key


# ---- the A0t candidate manifest
def test_the_a0t_manifest_matches_the_files_stays_unapproved_and_supersedes_a0s():
    assert MANIFEST.is_file(), "the A0t manifest has not been written"
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert pi.verify_candidate_manifest(manifest, REPO) == []
    assert manifest["status"] == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest["approval_record"] is None
    assert all(e["approval_status"] != "APPROVED" for e in manifest["entries"])
    assert [e["path"] for e in manifest["entries"] if e["pinned_sha256"] is not None and e["path"] in mk.A0T_NEW_PATHS] == []
    assert manifest["changed_since_a0s"] == [] and manifest["superseded_entries"] == [] and manifest["removed_since_a0s"] == []
    assert manifest["added_since_a0s"] == mk.A0T_NEW_PATHS and len(manifest["entries"]) == 108 + len(mk.A0T_NEW_PATHS)
    assert manifest["supersedes"]["sha256"] == A0S_MANIFEST_SHA
    assert manifest["launch_configuration"]["live_run"] is False and manifest["runtime_dependencies"]["process_or_network_api_imported_by_a0t_files"] is False
    assert manifest["selected_instrumentation_variant"]["status"] == "PROPOSED_NOT_APPROVED"


def test_every_a0s_manifest_entry_is_still_at_its_a0s_hash():
    a0s = json.loads((REPO / "docs" / "research" / "e1" / A0S_MANIFEST).read_text(encoding="utf-8"))
    assert [e["path"] for e in a0s["entries"] if pi.sha256_file(REPO / e["path"]) != e["sha256"]] == []


def test_every_file_the_a0t_tests_and_tools_rely_on_is_listed_in_the_manifest():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    paths = {e["path"] for e in manifest["entries"]}
    assert set(A0T_FILES) <= paths and set(be.REQUIRED_TOOL_FILES) <= paths


def test_a_freshly_built_manifest_equals_the_stored_one_apart_from_git():
    stored = json.loads(MANIFEST.read_text(encoding="utf-8"))
    fresh = mk.build(created_utc=stored["created_utc"])
    stored.pop("git", None), fresh.pop("git", None)
    assert fresh == stored
