"""Notebook hygiene for the Flashbots notebooks: valid nbformat, every code cell parses, executed, no error outputs."""

import ast
from pathlib import Path

import pytest

FB = Path(__file__).resolve().parents[1]
NOTEBOOKS = sorted(FB.glob("*/0[1-9]_*.ipynb"))
EXPECTED = ["01_flashbots_data_audit.ipynb", "02_flashbots_eda.ipynb", "03_flashbots_preprocessing_time_series.ipynb",
            "04_flashbots_forecasting_benchmark.ipynb", "05_flashbots_evaluation_and_fabric.ipynb"]
nbformat = pytest.importorskip("nbformat")


def _without_magics(source: str) -> str:
    return "\n".join(line for line in source.splitlines() if not line.lstrip().startswith(("%", "!")))


def test_all_five_notebooks_are_present():
    assert sorted(p.name for p in NOTEBOOKS) == sorted(EXPECTED)


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_is_valid_parses_and_was_executed_without_errors(path):
    nb = nbformat.read(path, as_version=4)
    nbformat.validate(nb)
    code = [c for c in nb.cells if c.cell_type == "code"]
    for index, cell in enumerate(code):
        ast.parse(_without_magics(cell.source), filename=f"{path.name}[code cell {index}]")
    assert all(c.execution_count is not None for c in code), "notebook not fully executed"
    assert not any(o.get("output_type") == "error" for c in code for o in c.get("outputs", []))


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_notebooks_do_not_write_files(path):
    """The notebooks only read stored artifacts; no cell may write, save or delete files."""
    nb = nbformat.read(path, as_version=4)
    source = "\n".join(c.source for c in nb.cells if c.cell_type == "code")
    for forbidden in ("to_csv(", "write_text(", "write_bytes(", "savefig(", "save_figure(", "write_json(", "np.save", ".unlink(", "rmtree"):
        assert forbidden not in source, forbidden
