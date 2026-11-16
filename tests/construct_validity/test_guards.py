"""Boundaries that must stay true however the analysis evolves."""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "HFBTP" / "evaluation"


def imports_of(name):
    tree = ast.parse((ROOT / name).read_text(encoding="utf-8"))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module)
    return found


def test_mmbpd_module_cannot_reach_simulator_or_models():
    assert not imports_of("cv_mmbpd_observed.py") & {"cv_block_cutter", "cv_mechanism", "cv_frozen_models", "cv_e1_predictions", "joblib", "sklearn"}


def test_mechanism_never_loads_mmbpd():
    for name in ("cv_mechanism.py", "cv_block_cutter.py", "cv_e1_predictions.py", "cv_frozen_models.py"):
        assert "load_mmbpd" not in (ROOT / name).read_text(encoding="utf-8")


def test_no_module_fits_or_saves_models():
    for path in ROOT.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert ".fit(" not in text and "joblib.dump" not in text, path


def test_only_runner_writes_files():
    for path in ROOT.glob("cv_*.py"):
        text = path.read_text(encoding="utf-8")
        assert "write_text" not in text and "to_csv" not in text and 'open(path, "w' not in text, path


def test_selection_uses_only_levels_up_to_145():
    source = (ROOT / "cv_mechanism.py").read_text(encoding="utf-8")
    assert "h[h[SET] <= 145].groupby([SET, BLOCK])" in source
