"""Regression test for the runner entry-point defect: `__doc__.splitlines()[0]` raised AttributeError in
scripts whose module docstring is absent (e1_existing_channel_pilot.py:83, e1_metrics_replay.py:52).
Fabric-free: static source checks plus argparse --help; nothing is executed against Docker or Fabric."""
import ast
import importlib
import re
import sys
from pathlib import Path

import pytest

E1 = Path(__file__).resolve().parents[2] / "scripts" / "e1"
sys.path.insert(0, str(E1))

UNGUARDED = re.compile(r"(?<!\()__doc__\.splitlines\(\)")          # `(__doc__ or "...").splitlines()` is allowed


def test_no_unguarded_doc_splitlines_in_scripts_without_docstring():
    offenders = []
    for path in sorted(E1.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        if (ast.get_docstring(ast.parse(text)) or "").strip():
            continue
        if UNGUARDED.search(text):
            offenders.append(path.name)
    assert offenders == [], f"unguarded __doc__.splitlines() in scripts with no module docstring: {offenders}"


@pytest.mark.parametrize("module", ["e1_existing_channel_pilot", "e1_metrics_replay"])
def test_help_exits_zero_without_a_module_docstring(module, capsys):
    mod = importlib.import_module(module)
    assert not (mod.__doc__ or "").strip(), "this test documents the no-docstring case"
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0
    assert "usage:" in capsys.readouterr().out
