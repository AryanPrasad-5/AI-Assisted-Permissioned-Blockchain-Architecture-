"""Task A0c (2026-11-21): the two command-line switches are opt-in and fail closed.

Only argument handling is exercised: each case ends at ``parser.error`` before any file is read, any model loaded or any
endpoint contacted.
"""

import ast
import importlib.util
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
INTEGRATED = REPO / "Integration" / "system_integration" / "run_integrated_controller.py"
PRODUCTION = REPO / "Fabric_Live_Testbed" / "controllers" / "run_production_shadow_validation.py"


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def integrated():
    return load(INTEGRATED, "a0c_run_integrated_controller")


@pytest.fixture(scope="module")
def production():
    return load(PRODUCTION, "a0c_run_production_shadow_validation")


def required_args(tmp_path):
    return ["--run-id", "r", "--snapshots", str(tmp_path / "missing.jsonl"), "--out-dir", str(tmp_path / "out"),
            "--hfbtp-artifact-dir", str(tmp_path / "no_model"), "--flashbots-series", str(tmp_path / "no_series.csv"),
            "--candidate-batch-sizes", "40,45", "--hfbtp-block-map", "40:40,45:45", "--batch-size", "45"]


@pytest.mark.parametrize("extra, message", [
    (["--stage3-frame-quality"], "needs --max-consecutive-abstentions"),
    (["--stage3-frame-quality", "--max-consecutive-abstentions", "0"], "positive integer"),
    (["--stage3-frame-quality", "--max-consecutive-abstentions", "-2"], "positive integer"),
    (["--max-consecutive-abstentions", "3"], "applies only with --stage3-frame-quality"),
])
def test_a_stage_3_run_without_a_bound_does_not_start_and_nothing_is_touched(integrated, tmp_path, capsys, extra, message):
    with pytest.raises(SystemExit) as caught:
        integrated.main(required_args(tmp_path) + extra)
    assert caught.value.code == 2
    assert message in capsys.readouterr().err
    assert not (tmp_path / "out").exists()                                    # refused before any output or model access


def test_the_stage_3_switch_is_off_by_default_and_has_no_default_bound(integrated):
    tree = ast.parse(INTEGRATED.read_text(encoding="utf-8"))
    options = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "add_argument" and node.args:
            first = node.args[0]
            if isinstance(first, ast.Constant) and first.value in ("--stage3-frame-quality", "--max-consecutive-abstentions"):
                options[first.value] = {k.arg: k.value for k in node.keywords}
    assert isinstance(options["--stage3-frame-quality"]["action"], ast.Constant) and options["--stage3-frame-quality"]["action"].value == "store_true"
    assert "default" not in options["--stage3-frame-quality"]
    assert isinstance(options["--max-consecutive-abstentions"]["default"], ast.Constant)
    assert options["--max-consecutive-abstentions"]["default"].value is None   # no assumed threshold


def test_the_integrated_command_enables_the_contract_only_through_the_checked_path():
    text = INTEGRATED.read_text(encoding="utf-8")
    assert 'invalid_frame_policy="abstain" if frame_quality is not None else "stop"' in text
    assert "frame_quality=frame_quality" in text


def test_the_capture_command_defaults_to_the_unchanged_column_mode_and_refuses_unknown_modes(production, capsys):
    tree = ast.parse(PRODUCTION.read_text(encoding="utf-8"))
    option = next(n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "add_argument"
                  and n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == "--recorder-latency")
    assert {k.arg: getattr(k.value, "value", None) for k in option.keywords}["default"] == "column"
    assert "recorder_latency=args.recorder_latency" in PRODUCTION.read_text(encoding="utf-8")
    with pytest.raises(SystemExit) as caught:
        production.main(["--recorder-latency", "guess"])
    assert caught.value.code == 2 and "invalid choice" in capsys.readouterr().err
    with pytest.raises(SystemExit) as helped:
        production.main(["--help"])
    assert helped.value.code == 0
    assert "derive_from_timestamps" in capsys.readouterr().out
