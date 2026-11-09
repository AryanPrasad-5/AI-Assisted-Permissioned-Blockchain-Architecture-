"""Synthetic tests of the forensic run comparator. The directories are made up; no forensic output is read."""
import ast
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "forensics" / "compare_forensic_runs.py"
sys.path.insert(0, str(SCRIPT.parent))

import compare_forensic_runs as cmp  # noqa: E402


def make_run(root: Path, results=None, manifest=None, csv_text="n,share\n1000,0.937839\n", extra=None):
    root.mkdir(parents=True)
    (root / "results.json").write_text(json.dumps(results or {"P1": {"share": 0.937839, "rows": [1, 2]},
                                                              "verdicts": {"H3": "FALSIFIED"}}), encoding="utf-8")
    (root / "manifest.json").write_text(json.dumps(manifest or {"started": "2026-11-09T02:38:35+05:30",
                                                                "finished": "2026-11-09T02:38:49+05:30",
                                                                "command": ["py", "--out", str(root)],
                                                                "numpy": "2.5.4"}), encoding="utf-8")
    (root / "n_profile.csv").write_bytes(csv_text.encode())
    for name, text in (extra or {}).items():
        (root / name).write_text(text, encoding="utf-8")
    return root


def test_identical_apart_from_run_specific_keys(tmp_path):
    a = make_run(tmp_path / "run_01")
    b = make_run(tmp_path / "run_04_repro", manifest={"started": "2026-11-10T09:00:00+05:30",
                                                      "finished": "2026-11-10T09:00:14+05:30",
                                                      "command": ["py", "--out", "elsewhere"], "numpy": "2.5.4"})
    r = cmp.compare(a, b)
    assert r["identical"]
    man = next(f for f in r["files"] if f["file"] == "manifest.json")
    assert set(man["ignored"]) == {"started", "finished", "command"}  # differences are recorded, not hidden


def test_value_difference_reported_exactly(tmp_path):
    a = make_run(tmp_path / "a")
    b = make_run(tmp_path / "b", results={"P1": {"share": 0.937839000001, "rows": [1, 2]}, "verdicts": {"H3": "FALSIFIED"}})
    r = cmp.compare(a, b)
    res = next(f for f in r["files"] if f["file"] == "results.json")
    assert not r["identical"] and res["differences"][0]["path"] == "/P1/share"
    assert res["max_abs_numeric_difference"] == pytest.approx(1e-12, rel=1e-3)


@pytest.mark.parametrize("other", [
    {"P1": {"share": 0.937839, "rows": [1, 2, 3]}, "verdicts": {"H3": "FALSIFIED"}},
    {"P1": {"share": 0.937839, "rows": [1, 2]}, "verdicts": {"H3": "INCONCLUSIVE"}},
    {"P1": {"share": 0.937839, "rows": [1, 2]}},
    {"P1": {"share": "0.937839", "rows": [1, 2]}, "verdicts": {"H3": "FALSIFIED"}},
    {"P1": {"share": 0.937839, "rows": [1, 2.0]}, "verdicts": {"H3": "FALSIFIED"}},
])
def test_structural_and_type_differences(tmp_path, other):
    r = cmp.compare(make_run(tmp_path / "a"), make_run(tmp_path / "b", results=other))
    assert not r["identical"]


def test_nan_equals_nan(tmp_path):
    a = make_run(tmp_path / "a", results={"x": float("nan")})
    b = make_run(tmp_path / "b", results={"x": float("nan")})
    assert cmp.compare(a, b)["identical"]


def test_csv_byte_difference_and_line(tmp_path):
    a = make_run(tmp_path / "a")
    b = make_run(tmp_path / "b", csv_text="n,share\n1000,0.937840\n")
    f = next(x for x in cmp.compare(a, b)["files"] if x["file"] == "n_profile.csv")
    assert not f["identical"] and f["first_differing_line"] == 2
    c = make_run(tmp_path / "c", csv_text="n,share\r\n1000,0.937839\r\n")  # line endings count
    assert not cmp.compare(a, c)["identical"]


def test_missing_and_extra_files(tmp_path):
    a = make_run(tmp_path / "a", extra={"p5_by_level.csv": "x\n"})
    b = make_run(tmp_path / "b", extra={"new.csv": "y\n"})
    r = cmp.compare(a, b)
    assert r["missing_in_reproduction"] == ["p5_by_level.csv"] and r["extra_in_reproduction"] == ["new.csv"]
    assert not r["identical"]


def test_cli_exit_codes_and_no_overwrite(tmp_path, capsys):
    a, b = make_run(tmp_path / "a"), make_run(tmp_path / "b")
    out = tmp_path / "report.json"
    assert cmp.main(["--original", str(a), "--reproduction", str(b), "--report-out", str(out)]) == 0
    assert "RUNS IDENTICAL" in capsys.readouterr().out and json.loads(out.read_text())["identical"]
    before = out.read_bytes()
    assert cmp.main(["--original", str(a), "--reproduction", str(b), "--report-out", str(out)]) == 2
    assert out.read_bytes() == before
    d = make_run(tmp_path / "d", csv_text="other\n")
    assert cmp.main(["--original", str(a), "--reproduction", str(d)]) == 1
    assert cmp.main(["--original", str(a), "--reproduction", str(tmp_path / "absent")]) == 2


def test_inputs_never_modified(tmp_path):
    a, b = make_run(tmp_path / "a"), make_run(tmp_path / "b", csv_text="z\n")
    snap = {p: p.read_bytes() for p in list(a.rglob("*")) + list(b.rglob("*")) if p.is_file()}
    cmp.compare(a, b)
    assert {p: p.read_bytes() for p in snap} == snap


def test_standard_library_only_and_no_project_imports():
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    mods = {n.module.split(".")[0] if isinstance(n, ast.ImportFrom) else a.name.split(".")[0]
            for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))
            for a in (n.names if isinstance(n, ast.Import) else [n])}
    assert mods <= {"__future__", "argparse", "hashlib", "json", "math", "sys", "pathlib"}, mods
