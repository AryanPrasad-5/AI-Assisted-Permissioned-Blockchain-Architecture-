"""Notebook hygiene: every notebook is valid nbformat and every code cell parses."""
import ast
import unittest
from pathlib import Path

NOTEBOOKS = sorted((Path(__file__).resolve().parent.parent / "notebooks").glob("*.ipynb"))


def _without_magics(source: str) -> str:
    return "\n".join(line for line in source.splitlines() if not line.lstrip().startswith(("%", "!")))


class TestNotebooks(unittest.TestCase):
    def test_notebooks_are_present(self):
        self.assertTrue(NOTEBOOKS)

    def test_notebooks_are_valid_and_code_cells_parse(self):
        try:
            import nbformat
        except ImportError:
            self.skipTest("nbformat is not installed")
        for path in NOTEBOOKS:
            with self.subTest(notebook=path.name):
                nb = nbformat.read(path, as_version=4)
                nbformat.validate(nb)
                for index, cell in enumerate(nb.cells):
                    if cell.cell_type == "code":
                        ast.parse(_without_magics(cell.source), filename=f"{path.name}[cell {index}]")


if __name__ == "__main__":
    unittest.main()
