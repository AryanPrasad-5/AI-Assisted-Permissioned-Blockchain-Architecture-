"""Read-only lock checker for the forensic generators (LA-1, approval C2). Synthetic lock files only."""
import importlib.metadata as md
import platform
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "forensics"))

import check_forensics_lock as cl  # noqa: E402

REPO = Path(__file__).resolve().parents[2]


def test_pin_lines_skip_comments_and_reject_bad_lines():
    assert cl.pin_lines("# header\n\nnumpy==2.5.4\n  pandas==3.0.6  \n") == ["numpy==2.5.4", "pandas==3.0.6"]
    for bad in ("numpy>=2.5\n", "numpy\n", "# only comments\n", "numpy==1\nNumPy==2\n", "six==1.17.0 ; extra\n"):
        with pytest.raises(ValueError):
            cl.pin_lines(bad)


def test_check_reports_mismatch_and_absence():
    versions = {"numpy": "2.5.4", "pandas": "3.0.5"}

    def fake(name):
        if name not in versions:
            raise md.PackageNotFoundError(name)
        return versions[name]
    rows = cl.check(["numpy==2.5.4", "pandas==3.0.6", "scipy==1.18.1"], installed=fake)
    assert [r["match"] for r in rows] == [True, False, False] and rows[2]["installed"] is None


def test_main_against_the_running_interpreter(tmp_path, capsys):
    have = md.version("pytest")
    lock = tmp_path / "lock.txt"
    lock.write_text(f"# test\npytest=={have}\n", encoding="utf-8")
    assert cl.main([str(lock), "--python", platform.python_version()]) == 0
    assert "ALL MATCH" in capsys.readouterr().out
    lock.write_text("pytest==0.0.1\n", encoding="utf-8")
    assert cl.main([str(lock)]) == 1
    assert cl.main([str(lock), "--python", "2.7.18"]) == 1
    prop = tmp_path / "proposal.txt"
    prop.write_text("pytest==0.0.2\n", encoding="utf-8")
    assert cl.main([str(lock), "--proposal", str(prop)]) == 1
    lock.write_text("pytest>=1\n", encoding="utf-8")
    assert cl.main([str(lock)]) == 2


def test_adopted_lock_pins_exactly_the_five_proposed_packages():
    pins = cl.pin_lines((REPO / "scripts" / "forensics" / "requirements-forensics-run01.lock.txt")
                        .read_text(encoding="utf-8"))
    assert pins == ["numpy==2.5.4", "pandas==3.0.6", "python-dateutil==2.9.0.post0", "six==1.17.0", "tzdata==2026.5"]
    assert b"\r" not in (REPO / "scripts" / "forensics" / "requirements-forensics-run01.lock.txt").read_bytes()
