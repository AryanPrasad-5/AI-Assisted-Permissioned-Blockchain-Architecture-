"""Independent cross-check of the clock pre-flight gate (owner decision A2-R1, 2026-11-10). Read-only.

Recomputes the owner-approved A2 criteria from the raw probe files with a separate implementation
(numpy and integer arithmetic; it never imports e1_clock_gate or e1_clock_rate_probe) and compares the
result with the gate's output file. Agreement checks:
  - the gate and probe source files are the unchanged A2 versions (SHA-256);
  - the gate's thresholds are the owner-approved values: (a) 1.0 ms, (b) 100 ppm, (c) 333 ppm over 3 s,
    minimum duration 1800 s (no --min-duration override);
  - every sample row was evaluated: contiguous index 0..n-1, n = gate sample count = manifest sample
    count, first and last rows match the manifest start and end, the first 3 s window starts at row 0;
  - (a), (b), (c), the validity checks and the verdict agree with the gate.
Supplementary diagnostics (reported, never used for any verdict): 3 s windows starting at every sample
(sliding), the trailing part after the last full window, the host reference fit residuals, the error
types recorded by the probe.

Output: --out (new file, never overwritten). Exit 0 = every agreement check holds, 5 = a disagreement.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

import numpy as np

# Owner-approved values (A2 message, 2026-11-09), written here independently of e1_clock_gate.
OWNER_STEP_MS = 1.0
OWNER_RATE_PPM = 100.0
OWNER_WINDOW_PPM = 333.0
OWNER_WINDOW_S = 3.0
OWNER_MIN_DURATION_S = 1800.0
OWNER_MIN_COVERAGE = 0.95
OWNER_MIN_REF_READS = 3
# Source files used for A2 (SHA-256 recorded in the A2 and E0-0 remediation manifests / report).
EXPECTED_SOURCES = {
    "scripts/e1/e1_clock_gate.py": "90455530f5c53206cb04e48ca5e0387b5a86498af0404d63d30de00bd7da2702",
    "scripts/e1/e1_clock_rate_probe.py": "ca11696e84a8cf6df8465974acf9384a4ed25e6a01b32b37eddf30c82755af71",
}
EXPECTED_GATE_VERSION = "e1_clock_gate 0.1 (2026-11-09, owner decision A2)"
CLOCKS = ("real", "mono", "raw", "boot")
PPM_TOL = 0.01          # agreement tolerance for rates (ppm); values are recomputed, not copied
MS_TOL = 1e-6           # agreement tolerance for (a) (ms)
REPO = Path(__file__).resolve().parents[2]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def col(rows: list[dict], name: str) -> np.ndarray:
    return np.array([int(r[name]) for r in rows], dtype=np.int64)


def nonoverlap_windows(raw: np.ndarray, mono: np.ndarray, window_ns: int) -> list[tuple[int, int, float]]:
    """Consecutive non-overlapping windows: start at row 0; a window closes at the first row whose RAW
    time is at least window_ns after its start row; the next window starts at that row."""
    out, start, n = [], 0, len(raw)
    while True:
        end = int(np.searchsorted(raw, raw[start] + window_ns, side="left"))
        if end >= n:
            return out
        out.append((start, end, (int(mono[end] - mono[start]) / int(raw[end] - raw[start]) - 1.0) * 1e6))
        start = end


def evaluate_raw(samples: list[dict], refs: list[dict], manifest: dict) -> dict:
    n = len(samples)
    idx, real, mono, raw = col(samples, "i"), col(samples, "real_ns"), col(samples, "mono_ns"), col(samples, "raw_ns")
    period_s = float(manifest["parameters"]["period_s"])

    # (a) change of REALTIME - MONOTONIC between consecutive rows
    d = np.diff(real - mono)
    a_max_ms = float(np.abs(d).max()) / 1e6 if n > 1 else 0.0
    a_n = int(np.count_nonzero(np.abs(d) > int(OWNER_STEP_MS * 1e6)))
    a_pass = a_n == 0 and a_max_ms <= OWNER_STEP_MS

    # (b) whole-run MONOTONIC against MONOTONIC_RAW (exact integer spans) and against the host QPC
    mono_span, raw_span = int(mono[-1] - mono[0]), int(raw[-1] - raw[0])
    b_raw = (mono_span / raw_span - 1.0) * 1e6
    good = [r for r in refs if str(r.get("ref_qpc_ns", "")).strip()]
    b_qpc = bound = resid_ms = None
    if len(good) >= 2:
        q = np.array([int(r["ref_qpc_ns"]) for r in good], dtype=np.int64)
        mid2 = np.array([int(r["mono_before_ns"]) + int(r["mono_after_ns"]) for r in good], dtype=np.int64)
        x = (q - q[0]).astype(np.float64) / 1e9                 # offsets from the first read, exact in int64
        y = (mid2 - mid2[0]).astype(np.float64) / 2e9
        slope, icpt = (float(v) for v in np.polyfit(x, y, 1))
        b_qpc = (slope - 1.0) * 1e6
        resid_ms = float(np.abs(y - (slope * x + icpt)).max() * 1e3)
        half = max(max((int(r[f"{c}_after_ns"]) - int(r[f"{c}_before_ns"])) / 2e9 for c in CLOCKS) for r in good)
        bound = 2 * half / float(x[-1] - x[0]) * 1e6 if x[-1] > x[0] else None
    b_pass = bool(abs(b_raw) <= OWNER_RATE_PPM and b_qpc is not None and abs(b_qpc) <= OWNER_RATE_PPM)

    # (c) non-overlapping 3 s windows in RAW time, from row 0
    wn = int(OWNER_WINDOW_S * 1e9)
    wins = nonoverlap_windows(raw, mono, wn)
    w_abs = np.array([abs(w[2]) for w in wins])
    c_max = float(w_abs.max()) if len(wins) else None
    c_over = [{"start_row": s, "end_row": e, "t_start_s": int(raw[s] - raw[0]) / 1e9,
               "t_end_s": int(raw[e] - raw[0]) / 1e9, "ppm": p} for s, e, p in wins if abs(p) > OWNER_WINDOW_PPM]
    c_pass = bool(wins) and c_max <= OWNER_WINDOW_PPM

    # supplementary: sliding windows from every row, trailing part
    ends = np.searchsorted(raw, raw + wn, side="left")
    ok = ends < n
    st = np.nonzero(ok)[0]
    sl = (mono[ends[ok]] - mono[st]) / (raw[ends[ok]] - raw[st]) * 1e6 - 1e6 if len(st) else np.array([])
    tail_start = wins[-1][1] if wins else 0
    tail_s = int(raw[-1] - raw[tail_start]) / 1e9
    tail_ppm = (int(mono[-1] - mono[tail_start]) / int(raw[-1] - raw[tail_start]) - 1) * 1e6 if tail_s > 0 else None

    duration_s = mono_span / 1e9
    expected = duration_s / period_s
    validity = {
        "duration_s": {"value": duration_s, "min": OWNER_MIN_DURATION_S - period_s,
                       "ok": duration_s >= OWNER_MIN_DURATION_S - period_s},
        "sample_coverage": {"value": (n - 1) / expected, "min": OWNER_MIN_COVERAGE,
                            "ok": (n - 1) / expected >= OWNER_MIN_COVERAGE},
        "host_reference_reads": {"value": len(good), "min": OWNER_MIN_REF_READS, "ok": len(good) >= OWNER_MIN_REF_READS},
    }
    if not all(v["ok"] for v in validity.values()):
        verdict = "INVALID"
    else:
        verdict = "PASS" if (a_pass and b_pass and c_pass) else "FAIL"
    return {
        "verdict": verdict,
        "a": {"threshold_ms": OWNER_STEP_MS, "n_over": a_n, "max_abs_change_ms": a_max_ms, "pass": a_pass,
              "pairs_evaluated": int(len(d))},
        "b": {"threshold_ppm": OWNER_RATE_PPM, "mono_vs_raw_ppm": b_raw, "mono_vs_host_qpc_ppm": b_qpc,
              "host_qpc_endpoint_slope_bound_ppm": bound, "host_qpc_fit_max_residual_ms": resid_ms,
              "host_reads_used": len(good), "host_reads_total": len(refs),
              "host_reads_nonzero_exit": sum(1 for r in refs if str(r.get("exit_code", "0")).strip() not in ("0", "")),
              "pass": b_pass},
        "c": {"threshold_ppm": OWNER_WINDOW_PPM, "window_s": OWNER_WINDOW_S, "windows": len(wins), "max_abs_ppm": c_max,
              "windows_over_threshold": c_over, "first_window": ({"start_row": wins[0][0], "end_row": wins[0][1],
                                                                   "ppm": wins[0][2]} if wins else None),
              "pass": c_pass},
        "validity": validity,
        "rows": {"n": n, "index_contiguous_from_0": bool(np.array_equal(idx, np.arange(n))),
                 "raw_strictly_increasing": bool(np.all(np.diff(raw) > 0)),
                 "max_row_gap_ms": float(np.diff(raw).max()) / 1e6 if n > 1 else None,
                 "first": {c: int(samples[0][f"{c}_ns"]) for c in CLOCKS},
                 "last": {c: int(samples[-1][f"{c}_ns"]) for c in CLOCKS}},
        "supplementary_not_criteria": {
            "sliding_3s_windows": int(len(sl)), "sliding_max_abs_ppm": float(np.abs(sl).max()) if len(sl) else None,
            "sliding_starts_over_333ppm": int(np.count_nonzero(np.abs(sl) > OWNER_WINDOW_PPM)),
            "sliding_t_start_s_over_333ppm": ([float(int(raw[s] - raw[0]) / 1e9) for s in st[np.abs(sl) > OWNER_WINDOW_PPM][:20]]
                                              if len(sl) else []),
            "trailing_part_s_after_last_full_window": tail_s, "trailing_part_ppm": tail_ppm},
    }


def compare(mine: dict, gate: dict, manifest: dict, gate_argv: list) -> list[dict]:
    g = gate["criteria"]
    ga, gb, gc = g["a_no_jump_over_1ms"], g["b_rate_error_le_100ppm"], g["c_no_3s_window_over_333ppm"]
    period_s = float(manifest["parameters"]["period_s"])
    checks = []

    def chk(name, ok, detail):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    for rel, want in EXPECTED_SOURCES.items():
        got = sha256(REPO / rel)
        chk(f"source unchanged: {rel}", got == want, {"sha256": got, "expected": want})
    chk("gate version string", gate.get("gate") == EXPECTED_GATE_VERSION, gate.get("gate"))
    chk("gate threshold (a) = 1.0 ms", ga["threshold_ms"] == OWNER_STEP_MS, ga["threshold_ms"])
    chk("gate threshold (b) = 100 ppm", gb["threshold_ppm"] == OWNER_RATE_PPM, gb["threshold_ppm"])
    chk("gate threshold (c) = 333 ppm", gc["threshold_ppm"] == OWNER_WINDOW_PPM, gc["threshold_ppm"])
    chk("gate window (c) = 3 s", gc["window_s"] == OWNER_WINDOW_S, gc["window_s"])
    chk("gate minimum duration = 1800 s - one period",
        abs(gate["validity"]["duration_s"]["min"] - (OWNER_MIN_DURATION_S - period_s)) < 1e-9,
        gate["validity"]["duration_s"]["min"])
    chk("gate argv has no --min-duration override", "--min-duration" not in [str(a) for a in gate_argv], gate_argv)

    r = mine["rows"]
    chk("sample index contiguous 0..n-1", r["index_contiguous_from_0"], r["n"])
    chk("gate evaluated every row (gate samples = csv rows)", gate["context"]["samples"] == r["n"],
        {"gate": gate["context"]["samples"], "csv": r["n"]})
    chk("manifest sample count = csv rows", manifest.get("samples") == r["n"],
        {"manifest": manifest.get("samples"), "csv": r["n"]})
    st, en = manifest["start"], manifest["end"]
    d0 = (r["first"]["mono"] - st["mono"]) / 1e9
    d1 = (en["mono"] - r["last"]["mono"]) / 1e9
    chk("first csv row is the run start (0 <= first - manifest start <= one period)", 0 <= d0 <= period_s, d0)
    chk("last csv row is the run end (0 <= manifest end - last <= two periods)", 0 <= d1 <= 2 * period_s, d1)
    chk("first 3 s window starts at row 0", mine["c"]["first_window"] is not None
        and mine["c"]["first_window"]["start_row"] == 0, mine["c"]["first_window"])

    chk("(a) n over 1 ms agrees", ga["n_jumps_over_threshold"] == mine["a"]["n_over"],
        {"gate": ga["n_jumps_over_threshold"], "mine": mine["a"]["n_over"]})
    chk("(a) max |change| agrees", abs(ga["max_abs_change_ms"] - mine["a"]["max_abs_change_ms"]) <= MS_TOL,
        {"gate": ga["max_abs_change_ms"], "mine": mine["a"]["max_abs_change_ms"]})
    chk("(a) pass agrees", ga["pass"] == mine["a"]["pass"], {"gate": ga["pass"], "mine": mine["a"]["pass"]})
    chk("(b) MONO vs RAW agrees", abs(gb["mono_vs_raw_ppm"] - mine["b"]["mono_vs_raw_ppm"]) <= PPM_TOL,
        {"gate": gb["mono_vs_raw_ppm"], "mine": mine["b"]["mono_vs_raw_ppm"]})
    gq, mq = gb["mono_vs_host_qpc_ppm"], mine["b"]["mono_vs_host_qpc_ppm"]
    chk("(b) MONO vs host QPC agrees", (gq is None and mq is None) or (gq is not None and mq is not None
                                                                      and abs(gq - mq) <= PPM_TOL),
        {"gate": gq, "mine": mq})
    chk("(b) pass agrees", gb["pass"] == mine["b"]["pass"], {"gate": gb["pass"], "mine": mine["b"]["pass"]})
    chk("(c) window count agrees", gc["windows"] == mine["c"]["windows"], {"gate": gc["windows"], "mine": mine["c"]["windows"]})
    gm, mm = gc["max_abs_ppm"], mine["c"]["max_abs_ppm"]
    chk("(c) max window agrees", (gm is None and mm is None) or (gm is not None and mm is not None
                                                                and abs(gm - mm) <= PPM_TOL), {"gate": gm, "mine": mm})
    chk("(c) pass agrees", gc["pass"] == mine["c"]["pass"], {"gate": gc["pass"], "mine": mine["c"]["pass"]})
    for k, v in mine["validity"].items():
        chk(f"validity {k} agrees", gate["validity"][k]["ok"] == v["ok"], {"gate": gate["validity"][k], "mine": v})
    chk("verdict agrees", gate["verdict"] == mine["verdict"], {"gate": gate["verdict"], "mine": mine["verdict"]})
    return checks


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--in-dir", required=True, type=Path, help="probe output folder")
    ap.add_argument("--gate-json", type=Path, help="gate output (default <in-dir>/clock_gate.json)")
    ap.add_argument("--out", required=True, type=Path, help="new JSON file (never overwritten)")
    args = ap.parse_args(argv)
    gate_path = args.gate_json or args.in_dir / "clock_gate.json"
    files = {p.name: p for p in (args.in_dir / "clock_samples.csv", args.in_dir / "ref_reads.csv",
                                 args.in_dir / "probe_manifest.json", gate_path)}
    manifest = json.loads(files["probe_manifest.json"].read_text(encoding="utf-8"))
    gate = json.loads(gate_path.read_text(encoding="utf-8"))
    mine = evaluate_raw(read_csv(files["clock_samples.csv"]), read_csv(files["ref_reads.csv"]), manifest)
    checks = compare(mine, gate, manifest, gate.get("argv", []))
    errs = manifest.get("errors", [])
    kinds: dict[str, int] = {}
    for e in errs:
        k = f"{e.split(' ', 1)[0]}: {e.split(': ', 1)[1] if ': ' in e else e}"
        kinds[k] = kinds.get(k, 0) + 1
    res = {"tool": "e1_clock_gate_crosscheck 0.1 (2026-11-10, owner decision A2-R1)",
           "crosscheck_sha256": sha256(Path(__file__).resolve()),
           "python": sys.version, "numpy": np.__version__,
           "inputs_sha256": {k: sha256(p) for k, p in files.items()},
           "all_agreement_checks_ok": all(c["ok"] for c in checks), "n_checks": len(checks),
           "failed_checks": [c["check"] for c in checks if not c["ok"]], "checks": checks,
           "independent_result": mine, "probe_error_kinds": kinds,
           "argv": sys.argv if argv is None else argv}
    args.out.open("x", encoding="utf-8").write(json.dumps(res, indent=2) + "\n")
    print(f"independent verdict {mine['verdict']} | gate verdict {gate['verdict']} | "
          f"agreement checks {sum(c['ok'] for c in checks)}/{len(checks)} ok")
    for c in checks:
        if not c["ok"]:
            print(f"  DISAGREE: {c['check']}: {c['detail']}")
    a, b, c = mine["a"], mine["b"], mine["c"]
    print(f"(a) pairs {a['pairs_evaluated']}, over 1 ms {a['n_over']}, max {a['max_abs_change_ms']:.4f} ms -> "
          f"{'PASS' if a['pass'] else 'FAIL'}")
    print(f"(b) MONO vs RAW {b['mono_vs_raw_ppm']:+.3f} ppm, MONO vs QPC "
          f"{'n/a' if b['mono_vs_host_qpc_ppm'] is None else format(b['mono_vs_host_qpc_ppm'], '+.3f')} ppm "
          f"({b['host_reads_used']} reads) -> {'PASS' if b['pass'] else 'FAIL'}")
    print(f"(c) windows {c['windows']}, max {c['max_abs_ppm']} ppm, over 333: {len(c['windows_over_threshold'])} -> "
          f"{'PASS' if c['pass'] else 'FAIL'}")
    s = mine["supplementary_not_criteria"]
    print(f"supplementary: sliding windows {s['sliding_3s_windows']}, max {s['sliding_max_abs_ppm']} ppm, "
          f"starts over 333 {s['sliding_starts_over_333ppm']}; trailing {s['trailing_part_s_after_last_full_window']:.3f} s "
          f"at {s['trailing_part_ppm']} ppm")
    print(f"-> {args.out}")
    return 0 if res["all_agreement_checks_ok"] else 5


if __name__ == "__main__":
    sys.exit(main())
