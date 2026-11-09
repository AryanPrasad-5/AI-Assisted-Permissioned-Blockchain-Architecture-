"""Clock pre-flight gate (owner decision A2, 2026-11-09). Read-only: it evaluates the files that
`e1_clock_rate_probe.py collect` wrote and never touches a clock.

Owner-approved acceptance criteria (A2, all three required; thresholds are fixed here, not CLI options):
  (a) no jump above 1 ms: |change of (CLOCK_REALTIME - CLOCK_MONOTONIC)| between consecutive samples
      <= 1.0 ms for every pair of samples
  (b) rate error <= 100 ppm: |MONOTONIC against MONOTONIC_RAW| over the whole run <= 100 ppm AND
      |MONOTONIC against the host QPC reference| <= 100 ppm
  (c) no 3 s window above 333 ppm: every consecutive non-overlapping 3 s window (MONOTONIC_RAW time)
      has |MONOTONIC against MONOTONIC_RAW| <= 333 ppm (1 ms over 3 s)
This gate is an additional pre-flight check. It does not replace or change V7 (2 ms, e1_run_orchestrator)
or the V4 cross-source bound (1 ms).

Data validity (not acceptance criteria; a failure makes the verdict INVALID, never PASS):
  duration >= --min-duration (default 1800 s, the approved 30 min) minus one sample period;
  samples >= 95 % of duration / period; >= 3 successful host reference reads.

Output: <in-dir>/<out-name> (new file, never overwritten). Exit 0 PASS, 3 FAIL, 4 INVALID.
Standard library only.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_clock_rate_probe as probe  # noqa: E402

GATE_VERSION = "e1_clock_gate 0.1 (2026-11-09, owner decision A2)"
MAX_STEP_MS = 1.0          # (a)
MAX_RATE_PPM = 100.0       # (b)
MAX_WINDOW_PPM = 333.0     # (c)
WINDOW_S = 3.0             # (c)
DEFAULT_MIN_DURATION_S = 1800.0
MIN_COVERAGE = 0.95
MIN_REF_READS = 3


def max_realtime_minus_monotonic_change_ms(samples: list[dict]) -> float:
    best = 0
    for a, b in zip(samples, samples[1:]):
        d = (int(b["real_ns"]) - int(b["mono_ns"])) - (int(a["real_ns"]) - int(a["mono_ns"]))
        best = max(best, abs(d))
    return best / 1e6


def max_sample_gap_ms(samples: list[dict]) -> float:
    return max((int(b["raw_ns"]) - int(a["raw_ns"]) for a, b in zip(samples, samples[1:])), default=0) / 1e6


def evaluate(samples: list[dict], refs: list[dict], manifest: dict, *,
             min_duration_s: float = DEFAULT_MIN_DURATION_S) -> dict:
    period_s = float(manifest.get("parameters", {}).get("period_s", 0.05))
    res = probe.analyze(samples, [], refs, user_hz=int(manifest.get("user_hz", 100)), step_ms=MAX_STEP_MS)
    rates, kernel, win = res["rates"], res["kernel_adjustment"], probe.window_rates(samples, WINDOW_S)
    offsets = rates.get("real_minus_host_utc_ms") or []

    duration_s = res["duration_mono_s"]
    expected = duration_s / period_s if period_s > 0 else 0
    validity = {
        "duration_s": {"value": duration_s, "min": min_duration_s - period_s,
                       "ok": duration_s >= min_duration_s - period_s},
        "sample_coverage": {"value": (len(samples) - 1) / expected if expected else 0.0, "min": MIN_COVERAGE,
                            "ok": expected > 0 and (len(samples) - 1) / expected >= MIN_COVERAGE},
        "host_reference_reads": {"value": rates.get("n_reads", 0), "min": MIN_REF_READS,
                                 "ok": rates.get("n_reads", 0) >= MIN_REF_READS},
    }

    max_step = max_realtime_minus_monotonic_change_ms(samples)
    mono_raw = kernel["mono_vs_raw_ppm"]
    mono_qpc = rates.get("mono_vs_host_qpc_ppm")
    criteria = {
        "a_no_jump_over_1ms": {
            "threshold_ms": MAX_STEP_MS, "n_jumps_over_threshold": res["n_steps"],
            "max_abs_change_ms": max_step, "pass": res["n_steps"] == 0 and max_step <= MAX_STEP_MS},
        "b_rate_error_le_100ppm": {
            "threshold_ppm": MAX_RATE_PPM, "mono_vs_raw_ppm": mono_raw, "mono_vs_host_qpc_ppm": mono_qpc,
            "host_qpc_endpoint_slope_bound_ppm": rates.get("endpoint_slope_bound_ppm"),
            "pass": abs(mono_raw) <= MAX_RATE_PPM and mono_qpc is not None and abs(mono_qpc) <= MAX_RATE_PPM},
        "c_no_3s_window_over_333ppm": {
            "threshold_ppm": MAX_WINDOW_PPM, **win,
            "pass": bool(win["windows"]) and win["max_abs_ppm"] <= MAX_WINDOW_PPM},
    }
    if not all(v["ok"] for v in validity.values()):
        verdict = "INVALID"
    elif all(c["pass"] for c in criteria.values()):
        verdict = "PASS"
    else:
        verdict = "FAIL"
    return {"gate": GATE_VERSION, "verdict": verdict, "criteria": criteria, "validity": validity,
            "context": {"samples": len(samples), "max_sample_gap_ms": max_sample_gap_ms(samples),
                        "tick_values": res["tick_values"], "adjtimex_changes": len(res["adjtimex_history"]),
                        "real_vs_host_qpc_ppm": rates.get("real_vs_host_qpc_ppm"),
                        "raw_vs_host_qpc_ppm": rates.get("raw_vs_host_qpc_ppm"),
                        "host_utc_vs_host_qpc_ppm": rates.get("host_utc_vs_host_qpc_ppm"),
                        "real_minus_host_utc_ms_first_last": [offsets[0], offsets[-1]] if offsets else None,
                        "real_minus_host_utc_ms_range": [min(offsets), max(offsets)] if offsets else None,
                        "probe_tool": manifest.get("tool"), "boot_id": manifest.get("boot_id"),
                        "clocksource": manifest.get("clocksource"), "collect_errors": len(manifest.get("errors", []))},
            "note": "V7 (2 ms) and V4 (1 ms) are separate criteria and are not changed by this gate."}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--in-dir", required=True, type=Path)
    ap.add_argument("--min-duration", type=float, default=DEFAULT_MIN_DURATION_S)
    ap.add_argument("--out-name", default="clock_gate.json", help="new file in --in-dir (never overwritten)")
    args = ap.parse_args(argv)
    man = json.loads((args.in_dir / "probe_manifest.json").read_text(encoding="utf-8"))
    res = evaluate(probe._read(args.in_dir / "clock_samples.csv"), probe._read(args.in_dir / "ref_reads.csv"), man,
                   min_duration_s=args.min_duration)
    res["argv"] = sys.argv if argv is None else argv
    out = args.in_dir / args.out_name
    out.open("x", encoding="utf-8").write(json.dumps(res, indent=2) + "\n")
    a, b, c = (res["criteria"][k] for k in ("a_no_jump_over_1ms", "b_rate_error_le_100ppm",
                                            "c_no_3s_window_over_333ppm"))
    print(f"(a) jumps > 1 ms: {a['n_jumps_over_threshold']}, max |change| {a['max_abs_change_ms']:.4f} ms -> "
          f"{'PASS' if a['pass'] else 'FAIL'}")
    q = b["mono_vs_host_qpc_ppm"]
    print(f"(b) MONO vs RAW {b['mono_vs_raw_ppm']:+.2f} ppm, MONO vs host QPC "
          f"{'n/a' if q is None else f'{q:+.2f}'} ppm (endpoint bound {b['host_qpc_endpoint_slope_bound_ppm']}) -> "
          f"{'PASS' if b['pass'] else 'FAIL'}")
    print(f"(c) 3 s windows {c['windows']}, max {c['max_abs_ppm']} ppm, p99 {c['p99_abs_ppm']}, median "
          f"{c['median_abs_ppm']}, share > 333 {c['share_over_333ppm']} -> {'PASS' if c['pass'] else 'FAIL'}")
    for k, v in res["validity"].items():
        print(f"validity {k}: {v['value']} (min {v['min']}) {'ok' if v['ok'] else 'NOT OK'}")
    print(f"VERDICT {res['verdict']} -> {out}")
    return {"PASS": 0, "FAIL": 3, "INVALID": 4}[res["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
