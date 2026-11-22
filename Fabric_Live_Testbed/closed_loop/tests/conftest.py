import subprocess
import sys
from pathlib import Path

import pytest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(autouse=True)
def no_processes(monkeypatch):
    """Offline guard: no test may start a process (Docker, WSL, peer, Caliper)."""
    def refuse(*a, **k):
        raise AssertionError(f"offline test tried to start a process: {a[:1]}")
    monkeypatch.setattr(subprocess, "run", refuse)
    monkeypatch.setattr(subprocess, "Popen", refuse)
