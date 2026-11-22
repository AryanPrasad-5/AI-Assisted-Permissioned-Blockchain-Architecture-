"""A0s research-integrity and manifest checks: no process/network API in any A0s file, real launch / inspection / sampling / inference disabled, no actuator, earlier manifests and
frozen artifacts byte-identical, the A0s candidate manifest consistent with the files on disk and still unapproved."""

from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path

import pytest

from tests.a0s_support import REPO

import e1_a0s_candidate_manifest as mk      # noqa: E402

from fabric_perf.controllers import prelive_integrity as pi      # noqa: E402

A0S_FILES = ["src/fabric_perf/controllers/bounded_execution.py", "src/fabric_perf/controllers/config_inspection.py", "src/fabric_perf/controllers/wsl_clock_probe.py",
             "src/fabric_perf/controllers/run_layout.py", "scripts/e1/e1_a0s_bounded_execute.py", "scripts/e1/e1_a0s_config_inspect.py", "scripts/e1/e1_a0s_clock_probe.py",
             "scripts/e1/e1_a0s_candidate_manifest.py"]
FORBIDDEN_IMPORTS = {"subprocess", "socket", "asyncio", "http", "urllib", "requests", "multiprocessing", "ctypes", "os", "shlex", "signal", "threading", "ssl", "ftplib", "smtplib", "telnetlib",
                     "docker", "paramiko", "pty", "pexpect"}
FORBIDDEN_MODEL_IMPORTS = {"joblib", "sklearn", "numpy", "pandas", "scipy", "torch", "pickle", "stage3_predictive", "real_inference_smoke", "adapters", "artifacts", "forecast_boundary",
                           "predictive_pipeline", "reactive_pipeline", "strategies", "harness", "e1_config_actuator", "live_foundation", "live_integration"}
MANIFEST = REPO / "docs" / "research" / "e1" / mk.SUCCESSOR_NAME
EARLIER = {
    "prelive_candidate_manifest_2026-11-21.json": "13640a7f683adcfec96f26f9060b05f29306bb2ebcfeb4c4b90c5fc43fa2b984",
    "prelive_candidate_manifest_2026-11-21_a0g.json": "491c7d0c296515bf24cd2a8d4462be3f34a81926f933e5d7542a45105eb5a5e7",
    "prelive_candidate_manifest_2026-11-21_a0h.json": "d6c451cfae089642d8c5c6be2c80d1fbdef7f718d1d2888c95d86275c1fda689",
    "prelive_candidate_manifest_2026-11-21_a0i.json": "7bc73bc7f648c92e3d515e4e9e98f41df1a552d2c9bd8bad204f78761d2a7c66",
    "prelive_candidate_manifest_2026-11-21_a0j.json": "831cb76ecdbd75318ebc7d4fe9be0d0c3eedfe7873a9a2190fa3446dd736c594",
    "prelive_candidate_manifest_2026-11-21_a0k.json": "d648a7151bf7d2688d2f658594a4d5adae4d08201b4428de5924372688ff756c",
    "prelive_candidate_manifest_2026-11-21_a0l.json": "b256ba9e04e699be412d362c3d167a7c5b58204c442cb3bb17afac5c2b0efcb4",
    "prelive_candidate_manifest_2026-11-21_a0m.json": "64687487752fc097f4d18e5e86593936421b0738dc967d0c23f06b3b3ccdd0fd",
    "prelive_candidate_manifest_2026-11-21_a0o.json": "e152ebae4d5f35cbea36acac3ca8d1854cf1899f1aa3ccac616c2a44e7d8ecf7",
    "prelive_candidate_manifest_2026-11-22_a0p.json": "f9843de8837cac54c677ee3bbf47bcee2dc8c30937e66de616c284d9bf394e92",
    "prelive_candidate_manifest_2026-11-22_a0q.json": "a624b57379b3a8c98dfafcf0b869ac5d76a3d874eaa067ad1824d54ce008aed4",
}
PINNED = {"scripts/e1/e1_la2_render.py": "f77a83e7de106b3c1dac28f93333e8dbaf67ea869836944b906f9bc1aabe3d83",
          "scripts/e1/caliper/e1_per_tx_recorder.js": "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8"}


def imports_of(path: str) -> set[str]:
    tree = ast.parse((REPO / path).read_text(encoding="utf-8"))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            found.add((node.module or "").split(".")[0])
            found |= {a.name for a in node.names}                 # "from . import x" and "from pkg import x"
    return found


@pytest.mark.parametrize("path", A0S_FILES)
def test_01_no_a0s_file_imports_a_process_network_container_or_environment_api(path):
    assert imports_of(path) & FORBIDDEN_IMPORTS == set()
    text = (REPO / path).read_text(encoding="utf-8")
    for needle in ("os.environ", "os.getenv", "os.system", "Popen(", "subprocess.", "socket.", "eval(", "exec(", "__import__("):
        assert needle not in text.replace("`", ""), (path, needle)


@pytest.mark.parametrize("path", A0S_FILES)
def test_02_no_a0s_file_imports_a_model_forecaster_or_actuator(path):
    assert imports_of(path) & FORBIDDEN_MODEL_IMPORTS == set()
    names = {n.id for n in ast.walk(ast.parse((REPO / path).read_text(encoding="utf-8"))) if isinstance(n, ast.Name)}
    assert not any("actuator" in name.lower() for name in names)


def test_03_every_real_operation_flag_is_false_and_assigned_once():
    for rel, name in (("src/fabric_perf/controllers/bounded_execution.py", "REAL_LAUNCH_ENABLED"), ("src/fabric_perf/controllers/config_inspection.py", "LIVE_INSPECTION_ENABLED"),
                      ("src/fabric_perf/controllers/wsl_clock_probe.py", "LIVE_CLOCK_SAMPLING_ENABLED"), ("src/fabric_perf/controllers/stage3_predictive.py", "REAL_INFERENCE_AUTHORIZED")):
        text = (REPO / rel).read_text(encoding="utf-8")
        assignments = re.findall(rf"^{name}\s*=\s*(\w+)", text, flags=re.MULTILINE)
        assert assignments == ["False"], (rel, name, assignments)


def test_04_no_a0s_source_can_turn_a_flag_on_there_is_no_setter_and_no_record_field_does_it():
    for rel in A0S_FILES:
        text = (REPO / rel).read_text(encoding="utf-8")
        assert not re.search(r"(REAL_LAUNCH_ENABLED|LIVE_INSPECTION_ENABLED|LIVE_CLOCK_SAMPLING_ENABLED|REAL_INFERENCE_AUTHORIZED)\s*=\s*True", text), rel
        assert "global REAL_" not in text and "setattr(" not in text, rel


def test_05_the_cli_execute_paths_return_5_before_parsing_anything(capsys):
    import e1_a0s_bounded_execute as a
    import e1_a0s_clock_probe as c
    import e1_a0s_config_inspect as i

    assert (a.main(["execute", "--x"]), i.main(["inspect"]), c.main(["sample"])) == (5, 5, 5)
    assert capsys.readouterr().out.count("REFUSED") == 3


def test_06_the_older_execute_entry_points_still_refuse():
    import e1_a0m_dry_render as dr
    import e1_a0p_launch_variant as lv
    import e1_prelive_validation as pv

    assert pv.main(["execute"]) == 5 and dr.main(["execute"]) == 5 and lv.main(["execute"]) == 5


def test_07_earlier_manifests_and_pinned_artifacts_are_byte_identical():
    for name, want in EARLIER.items():
        assert hashlib.sha256((REPO / "docs" / "research" / "e1" / name).read_bytes()).hexdigest() == want, name
    for rel, want in PINNED.items():
        assert pi.sha256_file(REPO / rel) == want, rel


def test_08_frozen_model_artifacts_equal_their_pins_without_being_loaded():
    pins = json.loads((REPO / "Integration/system_integration/offline_replay_inputs.json").read_text(encoding="utf-8"))["files"]
    for key in ("model_metadata", "throughput_model", "latency_model"):
        assert hashlib.sha256((REPO / pins[key]["path"]).read_bytes()).hexdigest().upper() == pins[key]["sha256"], key


def test_09_the_a0s_candidate_manifest_matches_the_files_stays_unapproved_and_supersedes_a0q():
    assert MANIFEST.is_file(), "the A0s manifest has not been written"
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert pi.verify_candidate_manifest(manifest, REPO) == []
    assert manifest["status"] == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest["approval_record"] is None
    assert all(e["approval_status"] != "APPROVED" for e in manifest["entries"])
    assert [e["path"] for e in manifest["entries"] if e["pinned_sha256"] is not None and e["path"] in mk.A0S_NEW_PATHS] == []
    assert manifest["changed_since_a0q"] == [] and manifest["superseded_entries"] == [] and manifest["removed_since_a0q"] == []
    assert manifest["added_since_a0q"] == mk.A0S_NEW_PATHS and len(manifest["entries"]) == 99 + len(mk.A0S_NEW_PATHS)
    assert manifest["supersedes"]["sha256"] == EARLIER["prelive_candidate_manifest_2026-11-22_a0q.json"]
    assert manifest["selected_instrumentation_variant"]["status"] == "PROPOSED_NOT_APPROVED" and manifest["launch_configuration"]["live_run"] is False
    assert manifest["runtime_dependencies"]["process_or_network_api_imported_by_a0s_files"] is False


def test_10_every_tool_the_execution_plan_uses_is_listed_in_the_manifest():
    from fabric_perf.controllers import bounded_execution as be

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    paths = {e["path"] for e in manifest["entries"]}
    assert set(be.REQUIRED_TOOL_FILES) <= paths and set(be.OPTIONAL_TOOL_FILES["before_after"]) <= paths


def test_11_a_freshly_built_manifest_equals_the_stored_one_apart_from_the_clock(tmp_path):
    stored = json.loads(MANIFEST.read_text(encoding="utf-8"))
    fresh = mk.build(created_utc=stored["created_utc"])
    stored.pop("git", None), fresh.pop("git", None)
    assert fresh == stored


def test_12_the_new_test_files_do_not_start_processes_either():
    for path in sorted((REPO / "tests").glob("test_a0s_*_2026_11_22.py")) + [REPO / "tests" / "a0s_support.py"]:
        imported = imports_of(path.relative_to(REPO).as_posix())
        assert "subprocess" not in imported and "socket" not in imported, path.name
