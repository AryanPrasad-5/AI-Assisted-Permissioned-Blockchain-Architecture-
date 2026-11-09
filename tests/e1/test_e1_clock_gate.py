"""Clock pre-flight gate (owner decision A2, 2026-11-09): synthetic probe data, not real clock data."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_clock_gate as gate  # noqa: E402
import e1_clock_rate_probe as cp  # noqa: E402

S = 1_000_000_000
R0 = 1_794_240_000 * S
MAN = {"user_hz": 100, "parameters": {"period_s": 0.05}, "tool": "synthetic", "errors": []}


def synth(duration_s=60, period_s=0.05, rate_ppm=0.0, steps=(), burst=None, qpc_ppm=0.0, refs_every_s=5):
    """RAW is true time. MONOTONIC runs at (1 + rate_ppm) x RAW, plus an optional burst (start_s, length_s, ppm)
    that is compensated right after (net zero over the run). REALTIME = MONOTONIC + steps (at_s, ns)."""
    samples, mono, step_acc, pending = [], 0, 0, sorted(steps)
    n = int(duration_s / period_s)
    for i in range(n + 1):
        raw = int(i * period_s * S)
        if i:
            t = raw / S
            ppm = rate_ppm
            if burst and burst[0] <= t < burst[0] + burst[1]:
                ppm += burst[2]
            elif burst and burst[0] + burst[1] <= t < burst[0] + 2 * burst[1]:
                ppm -= burst[2]
            mono += int(period_s * S * (1 + ppm / 1e6))
        while pending and raw >= pending[0][0] * S:
            step_acc += pending.pop(0)[1]
        samples.append({"i": i, "real_ns": R0 + mono + step_acc, "mono_ns": mono, "raw_ns": raw, "boot_ns": mono,
                        "adj_rc": 0, "tick_us": 10000, "freq_scaled": int(rate_ppm * 65536), "status": 0x2000,
                        "offset": 0, "constant": 1})
    refs = []
    for k, t in enumerate(range(0, duration_s, refs_every_s)):
        s = samples[int(t / period_s)]
        row = {"k": k, "ref_qpc_ns": int(s["raw_ns"] * (1 + qpc_ppm / 1e6)) + 7 * S, "ref_utc_ns": R0 + s["raw_ns"]}
        for c in cp.CLOCKS:
            row[f"{c}_before_ns"] = s[f"{c}_ns"] - 1_000_000
            row[f"{c}_after_ns"] = s[f"{c}_ns"] + 1_000_000
        refs.append(row)
    return samples, refs


def run(samples, refs, min_duration_s=60):
    return gate.evaluate(samples, refs, MAN, min_duration_s=min_duration_s)


def test_thresholds_are_the_owner_approved_values():
    assert (gate.MAX_STEP_MS, gate.MAX_RATE_PPM, gate.MAX_WINDOW_PPM, gate.WINDOW_S) == (1.0, 100.0, 333.0, 3.0)
    assert gate.DEFAULT_MIN_DURATION_S == 1800.0


def test_clean_clock_passes():
    r = run(*synth())
    assert r["verdict"] == "PASS"
    assert r["criteria"]["a_no_jump_over_1ms"]["max_abs_change_ms"] == 0
    assert abs(r["criteria"]["b_rate_error_le_100ppm"]["mono_vs_raw_ppm"]) < 1


def test_step_over_1ms_fails_a_only():
    r = run(*synth(steps=((20.0, 1_500_000),)))
    assert r["verdict"] == "FAIL"
    c = r["criteria"]
    assert not c["a_no_jump_over_1ms"]["pass"] and c["a_no_jump_over_1ms"]["n_jumps_over_threshold"] == 1
    assert c["b_rate_error_le_100ppm"]["pass"] and c["c_no_3s_window_over_333ppm"]["pass"]


def test_step_of_0_9ms_passes_a():
    r = run(*synth(steps=((20.0, 900_000),)))
    assert r["criteria"]["a_no_jump_over_1ms"]["pass"] and r["verdict"] == "PASS"


def test_negative_step_detected():
    r = run(*synth(steps=((20.0, -2_000_000),)))
    assert not r["criteria"]["a_no_jump_over_1ms"]["pass"]


def test_uniform_rate_150ppm_fails_b_not_c():
    r = run(*synth(rate_ppm=150))
    c = r["criteria"]
    assert not c["b_rate_error_le_100ppm"]["pass"] and c["c_no_3s_window_over_333ppm"]["pass"]
    assert c["b_rate_error_le_100ppm"]["mono_vs_raw_ppm"] == pytest.approx(150, abs=1)


def test_compensated_burst_fails_c_only():
    r = run(*synth(burst=(20.0, 6.0, 1000)))
    c = r["criteria"]
    assert c["b_rate_error_le_100ppm"]["pass"], c["b_rate_error_le_100ppm"]
    assert not c["c_no_3s_window_over_333ppm"]["pass"]
    assert c["c_no_3s_window_over_333ppm"]["max_abs_ppm"] == pytest.approx(1000, rel=0.02)
    assert r["verdict"] == "FAIL"


def test_host_reference_rate_error_fails_b():
    r = run(*synth(qpc_ppm=-200))       # MONOTONIC then runs about +200 ppm against the host QPC
    b = r["criteria"]["b_rate_error_le_100ppm"]
    assert abs(b["mono_vs_raw_ppm"]) < 1 and not b["pass"]
    assert b["mono_vs_host_qpc_ppm"] == pytest.approx(200, abs=2)


def test_short_run_is_invalid_not_pass():
    r = run(*synth(duration_s=60), min_duration_s=1800)
    assert r["verdict"] == "INVALID" and not r["validity"]["duration_s"]["ok"]


def test_missing_reference_is_invalid():
    samples, _ = synth()
    r = run(samples, [])
    assert r["verdict"] == "INVALID" and not r["validity"]["host_reference_reads"]["ok"]
    assert not r["criteria"]["b_rate_error_le_100ppm"]["pass"]


def test_sparse_samples_are_invalid():
    samples, refs = synth()
    r = run(samples[::3], refs)
    assert r["verdict"] == "INVALID" and not r["validity"]["sample_coverage"]["ok"]


def test_cli_writes_new_file_and_refuses_overwrite(tmp_path):
    samples, refs = synth()
    for name, rows, cols in (("clock_samples.csv", samples, cp.SAMPLE_COLS), ("ref_reads.csv", refs, cp.REF_COLS)):
        import csv
        with (tmp_path / name).open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, cols, lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
    (tmp_path / "probe_manifest.json").write_text(json.dumps(MAN), encoding="utf-8")
    assert gate.main(["--in-dir", str(tmp_path), "--min-duration", "60"]) == 0
    out = json.loads((tmp_path / "clock_gate.json").read_text(encoding="utf-8"))
    assert out["verdict"] == "PASS"
    with pytest.raises(FileExistsError):
        gate.main(["--in-dir", str(tmp_path), "--min-duration", "60"])
    assert gate.main(["--in-dir", str(tmp_path), "--out-name", "g2.json"]) == 4    # default 1800 s -> INVALID
