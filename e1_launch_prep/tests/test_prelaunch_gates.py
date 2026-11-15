"""Offline checks of the read-only pre-flight gate runner. It is never run against the live host here; these tests only prove its guard rails and that it evaluates the registered gates."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from e1_launch_prep import prelaunch_gates as pg
from p4_replay import launch_ids as li

SRC = Path(pg.__file__)


def test_it_refuses_a_measured_launch_id_before_touching_anything(tmp_path):
    out = tmp_path / "report.json"
    with pytest.raises(li.LaunchIdError):
        pg.main(["--launch-id", "p4r-meas-01", "--out", str(out)])
    assert not out.exists()


def test_it_runs_only_the_documented_read_only_gates_and_submits_nothing():
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    gates = sorted(c.args[0].value for c in ast.walk(tree) if isinstance(c, ast.Call) and getattr(c.func, "id", "") == "attempt" and c.args and isinstance(c.args[0], ast.Constant))
    assert gates == ["R4", "S11", "S3", "S4", "S5", "S6", "S8"]
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert not names & {"gate_s7", "fingerprint", "start_keep_awake", "spawn", "run_launch", "launch", "write_text", "write_bytes", "run_launch_preflight"}, names
    assert 'open(args.out, "x"' in SRC.read_text(encoding="utf-8")                          # the only write is the create-exclusive report
