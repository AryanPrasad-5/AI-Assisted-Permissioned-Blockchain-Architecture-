"""Static guards for scripts/forensics/independent_check_hfbtp_forensics.py.

Safe without authorization: parses the source only. No data, model or results file is read.
The checker must stay a console-only, standard-library, second code path (root PROJECT_RULES.md,
Computation boundary: "Descriptive HFBTP analysis ... read the CSV, print, write nothing").
"""

import ast
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "forensics" / "independent_check_hfbtp_forensics.py"
ALLOWED_IMPORTS = {"__future__", "csv", "hashlib", "json", "math", "statistics", "sys", "pathlib"}
WRITE_ATTRS = {"write_text", "write_bytes", "to_csv", "to_json", "dump", "mkdir", "unlink", "rename", "replace"}


def _tree():
    return ast.parse(SCRIPT.read_text(encoding="utf-8"))


def test_imports_are_standard_library_only():
    imported = set()
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported <= ALLOWED_IMPORTS, imported - ALLOWED_IMPORTS


def test_does_not_import_the_generator_or_project_code():
    src = SCRIPT.read_text(encoding="utf-8")
    for name in ("hfbtp_allrow_forensics", "cv_common", "cv_identities", "cv_block_cutter", "fabric_perf", "joblib"):
        assert f"import {name}" not in src and f"from {name}" not in src


def test_writes_nothing():
    for node in ast.walk(_tree()):
        if isinstance(node, ast.Attribute):
            assert node.attr not in WRITE_ATTRS, f"write-like call .{node.attr} at line {node.lineno}"
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "open":
            modes = [a.value for a in node.args[1:2] if isinstance(a, ast.Constant)]
            modes += [k.value.value for k in node.keywords if k.arg == "mode" and isinstance(k.value, ast.Constant)]
            assert all(set(m) <= {"r", "b", "t"} for m in modes), f"open() mode {modes} at line {node.lineno}"
        if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "open":
            modes = [a.value for a in node.args[:1] if isinstance(a, ast.Constant)]
            modes += [k.value.value for k in node.keywords if k.arg == "mode" and isinstance(k.value, ast.Constant)]
            assert all(set(m) <= {"r", "b", "t"} for m in modes), f"Path.open mode {modes} at line {node.lineno}"


def test_hash_pins_match_the_dataset_manifest_values():
    src = SCRIPT.read_text(encoding="utf-8")
    assert "ce295d6451787fefcfe2101579238cda4bede8539bc302e6a482aa4e5782bd5e" in src
    assert "e9275c49a2d7dc1f2a4015b6d8b28482" in src
