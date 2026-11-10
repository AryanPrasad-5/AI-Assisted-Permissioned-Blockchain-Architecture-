"""Independent gate cross-check (owner decision A2-R1, 2026-11-10): synthetic probe data, not real clock data."""
import csv
import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts" / "e1"))
sys.path.insert(0, str(HERE))

import e1_clock_gate as gate  # noqa: E402
import e1_clock_gate_crosscheck as xc  # noqa: E402
import e1_clock_rate_probe as cp  # noqa: E402
from test_e1_clock_gate import synth  # noqa: E402


def write_run(tmp_path, samples, refs, *, min_duration_s=1800, drop_row_after_gate=False, tamper=None):
    man = {"user_hz": 100, "parameters": {"period_s": 0.05}, "tool": "synthetic", "errors": [],
           "samples": len(samples),
           "start": {c: int(samples[0][f"{c}_ns"]) for c in cp.CLOCKS},
           "end": {c: int(samples[-1][f"{c}_ns"]) for c in cp.CLOCKS}}
    g = gate.evaluate(samples, refs, man, min_duration_s=min_duration_s)
    g["argv"] = ["--in-dir", str(tmp_path)]
    if tamper:
        tamper(g)
    if drop_row_after_gate:
        samples = samples[:100] + samples[101:]
    for name, rows, cols in (("clock_samples.csv", samples, cp.SAMPLE_COLS), ("ref_reads.csv", refs, cp.REF_COLS)):
        with (tmp_path / name).open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, cols, lineterminator="\n", extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
    (tmp_path / "probe_manifest.json").write_text(json.dumps(man), encoding="utf-8")
    (tmp_path / "clock_gate.json").write_text(json.dumps(g), encoding="utf-8")
    return g


def run_xc(tmp_path):
    rc = xc.main(["--in-dir", str(tmp_path), "--out", str(tmp_path / "xc.json")])
    return rc, json.loads((tmp_path / "xc.json").read_text(encoding="utf-8"))


def test_owner_values_written_independently():
    assert (xc.OWNER_STEP_MS, xc.OWNER_RATE_PPM, xc.OWNER_WINDOW_PPM, xc.OWNER_WINDOW_S, xc.OWNER_MIN_DURATION_S) \
        == (1.0, 100.0, 333.0, 3.0, 1800.0)


def test_crosscheck_does_not_import_gate_or_probe():
    src = (HERE.parents[1] / "scripts" / "e1" / "e1_clock_gate_crosscheck.py").read_text(encoding="utf-8")
    assert "import e1_clock_gate" not in src and "import e1_clock_rate_probe" not in src


@pytest.mark.parametrize("kw, verdict", [
    ({}, "PASS"),
    ({"steps": ((20.0, 1_500_000),)}, "FAIL"),
    ({"rate_ppm": 150}, "FAIL"),
    ({"burst": (20.0, 6.0, 1000)}, "FAIL"),
    ({"burst": (0.0, 2.4, 80_000)}, "FAIL"),      # startup slew like A2: the first window must count
    ({"qpc_ppm": -200}, "FAIL"),
])
def test_agrees_with_gate(tmp_path, kw, verdict):
    g = write_run(tmp_path, *synth(duration_s=1800, **kw))   # the owner minimum is 1800 s
    rc, out = run_xc(tmp_path)
    assert rc == 0, out["failed_checks"]
    assert g["verdict"] == out["independent_result"]["verdict"] == verdict


def test_startup_window_is_row_0_and_counted(tmp_path):
    write_run(tmp_path, *synth(duration_s=1800, burst=(0.0, 2.4, 80_000)))
    _, out = run_xc(tmp_path)
    c = out["independent_result"]["c"]
    assert c["first_window"]["start_row"] == 0 and c["windows_over_threshold"][0]["start_row"] == 0


def test_short_run_is_invalid_under_owner_minimum(tmp_path):
    write_run(tmp_path, *synth(), min_duration_s=1800)
    rc, out = run_xc(tmp_path)
    assert rc == 0 and out["independent_result"]["verdict"] == "INVALID"


def test_weakened_gate_threshold_is_flagged(tmp_path):
    def loosen(g):
        g["criteria"]["c_no_3s_window_over_333ppm"]["threshold_ppm"] = 2000.0
    write_run(tmp_path, *synth(), tamper=loosen)
    rc, out = run_xc(tmp_path)
    assert rc == 5 and "gate threshold (c) = 333 ppm" in out["failed_checks"]


def test_min_duration_override_is_flagged(tmp_path):
    def override(g):
        g["argv"] += ["--min-duration", "60"]
    write_run(tmp_path, *synth(), tamper=override)
    rc, out = run_xc(tmp_path)
    assert rc == 5 and "gate argv has no --min-duration override" in out["failed_checks"]


def test_row_missing_from_evaluation_is_flagged(tmp_path):
    write_run(tmp_path, *synth(), drop_row_after_gate=True)
    rc, out = run_xc(tmp_path)
    assert rc == 5
    assert "sample index contiguous 0..n-1" in out["failed_checks"]


def test_output_never_overwritten(tmp_path):
    write_run(tmp_path, *synth())
    run_xc(tmp_path)
    with pytest.raises(FileExistsError):
        run_xc(tmp_path)
