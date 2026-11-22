"""A0q workstream D (guard items 25-31): the recorder-v3 selection guard is syntax-aware, catches an injected unsafe selector, and the real selectors behave as the guard assumes.

Nothing here starts Caliper, Fabric, Docker, WSL or a network; renders go to temporary directories."""

from __future__ import annotations

import json
import os
import socket
import subprocess
from pathlib import Path

import pytest

from tests import a0m_support as m
from tests import recorder_v3_selection_guard as guard
from tests.a0e_support import DROP
from tests.a0o_support import REPO

import e1_a0m_dry_render as dr          # noqa: E402  (scripts/e1 is on sys.path through a0m_support)
import e1_prelive_validation as pv       # noqa: E402

from fabric_perf.controllers import channel_selection as cs      # noqa: E402
from fabric_perf.controllers import launch_variant as lv         # noqa: E402

V3 = guard.V3_STEM
OLD_ALLOWED = {"scripts/e1/caliper/e1_per_tx_recorder_v3.js", "scripts/e1/e1_a0j_candidate_manifest.py", "src/fabric_perf/controllers/recorder_health_bridge.py",
               "scripts/e1/e1_a0j_recorder_health_demo.py", "scripts/e1/e1_a0m_dry_render.py"}
THE_FIVE = {"Integration/system_integration/a0n_2026-11-21/timing_parameter_register.json", "Integration/system_integration/a0o_2026-11-21/variant_preflight_result.json",
            "Integration/system_integration/a0o_2026-11-21/collision_reproduction_before_change.json", "Integration/system_integration/a0o_2026-11-21/variant_marker_example.json",
            "src/fabric_perf/controllers/launch_variant.py"}


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a process, container or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(os, "system", refuse)


def legacy_text_scan() -> set[str]:
    """The scan A0j test_42 used before A0q: any file whose TEXT contains the name, minus the A0j and A0k evidence directories."""
    found = set()
    for root in guard.SCAN_ROOTS:
        for path in (REPO / root).rglob("*"):
            if not path.is_file() or path.suffix not in guard.SUFFIXES:
                continue
            rel = path.relative_to(REPO).as_posix()
            if "a0j_2026-11-21/" in rel or "a0k_2026-11-21/" in rel or "__pycache__" in rel or "node_modules" in rel:
                continue
            if V3 in path.read_text(encoding="utf-8", errors="replace"):
                found.add(rel)
    return found


def tree_with(tmp: Path, files: dict[str, str]) -> Path:
    for rel, text in files.items():
        target = tmp / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    return tmp


# ----------------------------------------------------------------------------------------------------------------------------- 25 reproduction
def test_25_the_original_failure_is_reproduced_by_the_old_scan_and_resolved_by_the_new_one():
    extra = legacy_text_scan() - OLD_ALLOWED
    assert THE_FIVE <= extra, sorted(THE_FIVE - extra)                      # the old scan still reports the five A0n/A0o files that name v3 without selecting it
    violations, records = guard.scan_tree(REPO)
    assert violations == []
    assert {"Integration/system_integration/a0n_2026-11-21/timing_parameter_register.json", "Integration/system_integration/a0o_2026-11-21/variant_marker_example.json"} <= set(records)
    for rel in records:                                                      # a recorded mention is a JSON evidence document, never a benchmark-shaped selection
        doc = json.loads((REPO / rel).read_text(encoding="utf-8"))
        assert not (isinstance(doc, dict) and ("monitors" in doc or "rounds" in (doc.get("test") or {})))
    # the two Python files that remain registered name v3 only as a tested selector / classifier / inventory / mutation target
    assert set(guard.REGISTERED_PYTHON) == {"scripts/e1/e1_a0m_dry_render.py", "src/fabric_perf/controllers/launch_variant.py", "scripts/e1/e1_a0j_candidate_manifest.py",
                                            "Integration/system_integration/a0j_2026-11-21/mutation_check.py"}


def test_25b_the_two_docstring_only_files_need_no_allow_list_entry():
    for rel in ("src/fabric_perf/controllers/recorder_health_bridge.py", "scripts/e1/e1_a0j_recorder_health_demo.py"):
        assert V3 in (REPO / rel).read_text(encoding="utf-8")
        assert guard.scan_python(rel, (REPO / rel).read_text(encoding="utf-8")) == []


# ----------------------------------------------------------------------------------------------------------------------------- 26 explicit selection
def test_26_an_explicit_versioned_selection_passes_and_generates_the_matching_configuration(tmp_path):
    root, manifest = m.render(tmp_path)
    facts = lv.derive_variant(m.read_json(m.wired(root, manifest) / "benchmark.yaml"))
    assert facts["variant"] == lv.VARIANT_A0M and facts["monitor_module"] == "e1_per_tx_recorder_v3.js"      # what was generated is what was asked for
    assert manifest["status"] == "RENDERED_OFFLINE_NOT_EXECUTED" and manifest["caliper_invoked"] is False
    assert dr.validate_a0m_parameters(m.a0m_params())["selected_recorder"] == dr.SELECTED_RECORDER
    assert guard.scan_tree(REPO)[0] == []


# ----------------------------------------------------------------------------------------------------------------------------- 27 implicit selection fails
@pytest.mark.parametrize("value", [DROP, None, "", "e1_per_tx_recorder_v2", "e1_per_tx_recorder_v1", "latest", "e1_per_tx_recorder_v3.js"])
def test_27_the_selector_refuses_an_implicit_or_unsupported_recorder_and_creates_nothing(tmp_path, value):
    with pytest.raises(dr.A0mParameterError) as info:
        m.render(tmp_path, a0m=m.a0m_params(selected_recorder=value) if value is not DROP else m.a0m_params(selected_recorder=DROP))
    assert any(issue.field == "selected_recorder" for issue in info.value.issues)
    assert not (tmp_path / "out").exists()


def test_27b_no_variant_is_chosen_by_default(tmp_path):
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.derive_variant({"monitors": {"transaction": []}, "test": {"rounds": [{"workload": {"module": "x.js"}}]}})
    assert exc.value.code == "monitor_not_single"
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.qualified_run_id("base", "recorder_v3")
    assert exc.value.code == "variant_unknown"


# ----------------------------------------------------------------------------------------------------------------------------- 28 legacy behaviour
def test_28_legacy_defaults_are_unchanged_and_the_historical_variant_never_names_v3(tmp_path):
    render_text = (REPO / "scripts/e1/e1_la2_render.py").read_text(encoding="utf-8")
    assert V3 not in render_text and "e1_per_tx_recorder_v2" not in render_text
    p1 = m.write(tmp_path / "a0e.json", m.a0e_params())
    pv.prepare(p1, tmp_path / "a0e_out", "p1")
    facts = lv.classify_directory(tmp_path / "a0e_out" / "p1")
    assert facts["variant"] == lv.VARIANT_A0E and facts["facts"]["monitor_module"] == "e1_per_tx_recorder_v2.js"
    assert lv.VARIANTS[lv.VARIANT_A0E]["forbidden_modules"][0] == "e1_per_tx_recorder_v3.js"


# ----------------------------------------------------------------------------------------------------------------------------- 29 injected unsafe selectors
UNSAFE = {
    "python_default_argument": ("scripts/e1/legacy_launch.py", "def launch(recorder='scripts/e1/caliper/e1_per_tx_recorder_v3.js'):\n    return recorder\n", "default_argument"),
    "python_literal_in_launcher": ("scripts/e1/other_launcher.py", "RECORDER = 'e1_per_tx_recorder_v3.js'\n", "python_literal"),
    "python_dynamic_name": ("scripts/e1/dyn.py", "def pick(v):\n    return f'e1_per_tx_recorder_{v}.js'\n", "dynamic_recorder_selection"),
    "python_concat_name": ("scripts/e1/dyn2.py", "def pick(v):\n    return 'e1_per_tx_recorder_' + v\n", "dynamic_recorder_selection"),
    "environment_getenv": ("scripts/e1/env.py", "import os\nR = os.getenv('E1_RECORDER_VERSION', '3')\n", "environment_selection"),
    "environment_environ_get": ("scripts/e1/env2.py", "import os\nR = os.environ.get('CALIPER_RECORDER')\n", "environment_selection"),
    "environment_subscript": ("scripts/e1/env3.py", "import os\nR = os.environ['RECORDER']\n", "environment_selection"),
    "javascript_require": ("fabric-testbed/caliper/wrapper.js", "const r = require('./e1_per_tx_recorder_v3');\n", "text_mention"),
    "yaml_caliper_module": ("scripts/e1/bench.yaml", "monitors:\n  transaction:\n    - module: /x/e1_per_tx_recorder_v3.js\n", "caliper_module_selection"),
    "shell_copy": ("scripts/e1/run.sh", "cp scripts/e1/caliper/e1_per_tx_recorder_v3.js out/\n", "text_mention"),
    "json_benchmark": ("Integration/bench.json", json.dumps({"monitors": {"transaction": [{"module": "/m/e1_per_tx_recorder_v3.js"}]}}), "caliper_module_selection"),
    "json_benchmark_other_key": ("Integration/bench2.json", json.dumps({"test": {"rounds": []}, "note": "uses e1_per_tx_recorder_v3.js"}), "benchmark_config_naming_v3"),
}


@pytest.mark.parametrize("name", sorted(UNSAFE))
def test_29_an_injected_unsafe_selector_is_detected(tmp_path, name):
    rel, text, kind = UNSAFE[name]
    root = tree_with(tmp_path, {rel: text})
    violations, _ = guard.scan_tree(root)
    assert any(f.path == rel and f.kind == kind for f in violations), [(f.path, f.kind) for f in violations]


def test_29b_the_same_unsafe_selector_in_a_copy_of_the_real_tree_fails_the_real_test_logic(tmp_path):
    root = tree_with(tmp_path, {"scripts/e1/e1_a0m_dry_render.py": (REPO / "scripts/e1/e1_a0m_dry_render.py").read_text(encoding="utf-8"),
                                "scripts/e1/sneaky_launcher.py": "def go(recorder='e1_per_tx_recorder_v3.js'):\n    pass\n"})
    violations, _ = guard.scan_tree(root)
    assert [(f.path, f.kind) for f in violations] == [("scripts/e1/sneaky_launcher.py", "default_argument")]          # the registered selector is fine; the new file is not


def test_29c_a_registered_file_cannot_hide_an_unsafe_form(tmp_path):
    root = tree_with(tmp_path, {"scripts/e1/e1_a0m_dry_render.py": "def go(recorder='e1_per_tx_recorder_v3.js'):\n    pass\n"})
    violations, _ = guard.scan_tree(root)
    assert [f.kind for f in violations] == ["default_argument"]            # registration exempts only a plain literal, never a default, a dynamic name or an environment lookup


@pytest.mark.parametrize("rel,text", [
    ("scripts/e1/doc_only.py", '"""Describes e1_per_tx_recorder_v3 in prose."""\n# also e1_per_tx_recorder_v3 in a comment\nX = 1\n'),
    ("scripts/e1/func_doc.py", 'def f():\n    """Uses e1_per_tx_recorder_v3 only in its docstring."""\n    return 1\n'),
    ("fabric-testbed/note.js", "// e1_per_tx_recorder_v3 is described here\n/* and here e1_per_tx_recorder_v3 */\nmodule.exports = {};\n"),
    ("scripts/e1/a.sh", "# runs e1_per_tx_recorder_v3 someday\necho ok\n"),
    ("Integration/record.json", json.dumps({"schema": "evidence/1", "monitor_module": "e1_per_tx_recorder_v3.js", "note": "recorded, not selected"})),
])
def test_29d_descriptive_mentions_are_not_selections(tmp_path, rel, text):
    root = tree_with(tmp_path, {rel: text})
    assert guard.scan_tree(root)[0] == []


# ----------------------------------------------------------------------------------------------------------------------------- 30 protected channels
@pytest.mark.parametrize("name", ["e1-r002", "e1-cfg001", "E1-R002"])
def test_30_protected_channels_stay_rejected_by_the_selectors(tmp_path, name):
    with pytest.raises(cs.ChannelSelectionError):
        m.render(tmp_path, channel=name)
    assert not (tmp_path / "out").exists()


# ----------------------------------------------------------------------------------------------------------------------------- 31 disabled execution routes
def test_31_no_execution_route_exists_for_the_selector_or_the_classifier():
    assert dr.main(["execute"]) == dr.EXIT_EXECUTE_REFUSED == 5
    assert not {n for n in dir(dr) if n.startswith("run_") or n == "execute"}
    assert not any(n in dir(lv) for n in ("execute", "launch", "run"))
    import ast
    for rel in guard.REGISTERED_PYTHON:
        tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | {n.module.split(".")[0] for n in ast.walk(tree)
                                                                                                                     if isinstance(n, ast.ImportFrom) and n.module}
        if rel.endswith("mutation_check.py"):
            continue                                                         # the A0j mutation tool runs pytest by design; it names the file only as a mutation target
        assert not imported & {"socket", "docker", "requests", "urllib", "http"}, (rel, imported)
