"""Clock-rate probe (E0-0 remediation F1, 2026-11-09). Read-only diagnostic: it reads clocks, calls
adjtimex with modes = 0 (read only, no privilege) and reads systemd-timesyncd's last NTP message. It
never sets a clock, a tick, a frequency or a clock source.

collect   (Linux only) for --duration seconds:
  clock_samples.csv  every --period s: CLOCK_REALTIME, CLOCK_MONOTONIC, CLOCK_MONOTONIC_RAW, CLOCK_BOOTTIME
                     (ns) and adjtimex(modes=0): return code, tick (us), freq (2^-16 ppm), status, offset,
                     constant
  ntp_messages.csv   every --ntp-period s: timesyncd's last NTP message (busctl, raw microseconds), one row
                     per new PacketCount; the raw busctl text is kept
  ref_reads.csv      every --ref-period s: one reading of an external reference clock, bracketed by Linux
                     readings just before and just after (the reference reading lies in between). The
                     reference is --ref-python (a Windows python.exe reached through WSL interop) printing
                     time.time_ns() (host UTC) and time.perf_counter_ns() (host QPC)
  probe_manifest.json
analyze   steps: REALTIME - MONOTONIC changing by more than --step-ms between consecutive samples (both
          clocks share one rate, so only a step changes the difference);
          attribution: a step is attributed to a timesyncd exchange when it follows the exchange's
          destination timestamp by at most --lag-s and equals the exchange's NTP offset within --match-ms;
          adjtimex history; kernel adjustment: MONOTONIC against MONOTONIC_RAW, measured and predicted
          from tick and freq; rates of each Linux clock against the reference (least squares on bracket
          midpoints) -> probe_analysis.json

The step and match thresholds are diagnostic settings of this probe, not protocol criteria.
Standard library only (runs under the WSL system python3).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

TOOL_VERSION = "e1_clock_rate_probe 0.1 (2026-11-09)"
CLOCKS = ("real", "mono", "raw", "boot")
SAMPLE_COLS = ["i", "real_ns", "mono_ns", "raw_ns", "boot_ns", "adj_rc", "tick_us", "freq_scaled", "status",
               "offset", "constant"]
NTP_COLS = ["poll_mono_ns", "poll_real_ns", "packet_count", "origin_us", "receive_us", "transmit_us", "dest_us",
            "spike", "jitter_us", "raw"]
REF_COLS = ["k", *[f"{c}_before_ns" for c in CLOCKS], *[f"{c}_after_ns" for c in CLOCKS], "ref_utc_ns", "ref_qpc_ns",
            "exit_code", "stdout", "stderr"]
BUSCTL = ["busctl", "get-property", "org.freedesktop.timesync1", "/org/freedesktop/timesync1",
          "org.freedesktop.timesync1.Manager", "NTPMessage"]
REF_CODE = "import time;print(time.time_ns(), time.perf_counter_ns())"


# ------------------------------------------------------------------ reading (Linux)
def _adjtimex_reader():
    import ctypes
    import ctypes.util

    class Timeval(ctypes.Structure):
        _fields_ = [("tv_sec", ctypes.c_long), ("tv_usec", ctypes.c_long)]

    class Timex(ctypes.Structure):
        _fields_ = [("modes", ctypes.c_uint), ("offset", ctypes.c_long), ("freq", ctypes.c_long),
                    ("maxerror", ctypes.c_long), ("esterror", ctypes.c_long), ("status", ctypes.c_int),
                    ("constant", ctypes.c_long), ("precision", ctypes.c_long), ("tolerance", ctypes.c_long),
                    ("time", Timeval), ("tick", ctypes.c_long), ("ppsfreq", ctypes.c_long),
                    ("jitter", ctypes.c_long), ("shift", ctypes.c_int), ("stabil", ctypes.c_long),
                    ("jitcnt", ctypes.c_long), ("calcnt", ctypes.c_long), ("errcnt", ctypes.c_long),
                    ("stbcnt", ctypes.c_long), ("tai", ctypes.c_int), ("pad", ctypes.c_int * 11)]

    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

    def read() -> dict:
        t = Timex()
        t.modes = 0  # read only
        rc = libc.adjtimex(ctypes.byref(t))
        return {"adj_rc": rc, "tick_us": t.tick, "freq_scaled": t.freq, "status": t.status, "offset": t.offset,
                "constant": t.constant}
    return read


def read_clocks() -> dict:
    g = time.clock_gettime_ns
    return {"real": g(time.CLOCK_REALTIME), "mono": g(time.CLOCK_MONOTONIC), "raw": g(time.CLOCK_MONOTONIC_RAW),
            "boot": g(time.CLOCK_BOOTTIME)}


def parse_busctl_ntp(text: str) -> dict:
    """busctl NTPMessage '(uuuuittayttttbtt) leap ver mode stratum prec rdelay rdisp N b1..bN orig recv trans
    dest spike count jitter' -> dict (microseconds)."""
    tok = text.split()
    if not tok or not tok[0].startswith("("):
        raise ValueError(f"unexpected busctl output {text[:80]!r}")
    v = tok[1:]
    n = int(v[7])
    rest = v[8 + n:]
    if len(rest) != 7:
        raise ValueError(f"unexpected NTPMessage field count in {text[:120]!r}")
    return {"origin_us": int(rest[0]), "receive_us": int(rest[1]), "transmit_us": int(rest[2]),
            "dest_us": int(rest[3]), "spike": rest[4] == "true", "packet_count": int(rest[5]),
            "jitter_us": int(rest[6])}


def collect(out_dir: Path, duration_s: float, period_s: float, ntp_period_s: float, ref_period_s: float,
            ref_python: str | None, argv: list[str]) -> dict:
    if not sys.platform.startswith("linux"):
        raise SystemExit("collect runs on Linux only")
    out_dir.mkdir(parents=True, exist_ok=False)
    adj = _adjtimex_reader()
    stop = threading.Event()
    errors: list[str] = []

    def ntp_loop(fh):
        w = csv.DictWriter(fh, NTP_COLS, lineterminator="\n")
        w.writeheader()
        last = None
        while not stop.is_set():
            pm, pr = time.monotonic_ns(), time.time_ns()
            try:
                txt = subprocess.run(BUSCTL, capture_output=True, text=True, timeout=10).stdout.strip()
                msg = parse_busctl_ntp(txt)
                if msg["packet_count"] != last:
                    last = msg["packet_count"]
                    w.writerow({"poll_mono_ns": pm, "poll_real_ns": pr, **msg, "raw": txt})
                    fh.flush()
            except Exception as exc:  # recorded, never fatal
                errors.append(f"ntp {pr}: {exc!r}")
            stop.wait(ntp_period_s)

    def ref_loop(fh):
        w = csv.DictWriter(fh, REF_COLS, lineterminator="\n")
        w.writeheader()
        k = 0
        while not stop.is_set():
            b = read_clocks()
            try:
                p = subprocess.run([ref_python, "-I", "-c", REF_CODE], capture_output=True, text=True, timeout=20)
                code, so, se = p.returncode, p.stdout.strip(), p.stderr.strip()
            except Exception as exc:
                code, so, se = None, "", repr(exc)
            a = read_clocks()
            row = {"k": k, **{f"{c}_before_ns": b[c] for c in CLOCKS}, **{f"{c}_after_ns": a[c] for c in CLOCKS},
                   "exit_code": code, "stdout": so, "stderr": se[-200:]}
            parts = so.split()
            if code == 0 and len(parts) == 2 and all(x.isdigit() for x in parts):
                row["ref_utc_ns"], row["ref_qpc_ns"] = int(parts[0]), int(parts[1])
            w.writerow(row)
            fh.flush()
            k += 1
            stop.wait(ref_period_s)

    files = {n: (out_dir / n).open("x", newline="", encoding="utf-8")
             for n in ("clock_samples.csv", "ntp_messages.csv", "ref_reads.csv")}
    threads = [threading.Thread(target=ntp_loop, args=(files["ntp_messages.csv"],), daemon=True)]
    if ref_python:
        threads.append(threading.Thread(target=ref_loop, args=(files["ref_reads.csv"],), daemon=True))
    start = read_clocks()
    for t in threads:
        t.start()
    w = csv.DictWriter(files["clock_samples.csv"], SAMPLE_COLS, lineterminator="\n")
    w.writeheader()
    i, t0 = 0, time.monotonic_ns()
    while time.monotonic_ns() - t0 < duration_s * 1e9:
        c = read_clocks()
        w.writerow({"i": i, **{f"{k}_ns": v for k, v in c.items()}, **adj()})
        i += 1
        if i % 200 == 0:
            files["clock_samples.csv"].flush()
        due = t0 + int(i * period_s * 1e9)
        time.sleep(max(0.0, (due - time.monotonic_ns()) / 1e9))
    stop.set()
    for t in threads:
        t.join(timeout=30)
    end = read_clocks()
    for fh in files.values():
        fh.close()
    manifest = {"tool": TOOL_VERSION, "argv": argv, "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                "clocksource": Path("/sys/devices/system/clocksource/clocksource0/current_clocksource").read_text().strip(),
                "user_hz": os.sysconf("SC_CLK_TCK"), "uname": " ".join(os.uname()),
                "parameters": {"duration_s": duration_s, "period_s": period_s, "ntp_period_s": ntp_period_s,
                               "ref_period_s": ref_period_s, "ref_python": ref_python},
                "start": start, "end": end, "samples": i, "errors": errors}
    (out_dir / "probe_manifest.json").open("x", encoding="utf-8").write(json.dumps(manifest, indent=2) + "\n")
    return manifest


# ------------------------------------------------------------------ analysis (pure)
def find_steps(samples: list[dict], step_ns: int) -> list[dict]:
    out = []
    for a, b in zip(samples, samples[1:]):
        d = (int(b["real_ns"]) - int(b["mono_ns"])) - (int(a["real_ns"]) - int(a["mono_ns"]))
        if abs(d) > step_ns:
            out.append({"i": int(b["i"]), "mono_ns": int(b["mono_ns"]), "real_after_ns": int(b["real_ns"]),
                        "real_pre_equiv_ns": int(b["real_ns"]) - d, "step_ns": d})
    return out


def ntp_offset_us(m: dict) -> float:
    return ((int(m["receive_us"]) - int(m["origin_us"])) + (int(m["transmit_us"]) - int(m["dest_us"]))) / 2


def attribute_steps(steps: list[dict], ntp: list[dict], match_ns: int, lag_ns: int) -> list[dict]:
    out = []
    for s in steps:
        best = None
        for m in ntp:
            dest_ns = int(m["dest_us"]) * 1000
            lag = s["real_pre_equiv_ns"] - dest_ns
            diff = s["step_ns"] - ntp_offset_us(m) * 1000
            if -100_000_000 <= lag <= lag_ns and abs(diff) <= match_ns:
                if best is None or abs(diff) < abs(best["step_minus_offset_ns"]):
                    best = {"packet_count": int(m["packet_count"]), "lag_after_dest_ns": lag,
                            "step_minus_offset_ns": diff, "ntp_offset_ms": ntp_offset_us(m) / 1000}
        out.append({**s, "attributed": best is not None, "match": best})
    return out


def fit_slope(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx


def window_rates(samples: list[dict], window_s: float = 3.0) -> dict:
    """MONOTONIC against MONOTONIC_RAW over consecutive non-overlapping windows of window_s (RAW time):
    the rate error an interval of that length can carry. 3 s ~ the longest per-transaction latency at
    tau = 2 s; 333 ppm over 3 s = 1 ms (the protocol's cross-source resolution)."""
    errs, i0 = [], 0
    for i in range(1, len(samples)):
        draw = int(samples[i]["raw_ns"]) - int(samples[i0]["raw_ns"])
        if draw >= window_s * 1e9:
            errs.append((int(samples[i]["mono_ns"]) - int(samples[i0]["mono_ns"])) / draw * 1e6 - 1e6)
            i0 = i
    a = sorted(abs(e) for e in errs)
    return {"window_s": window_s, "windows": len(a), "max_abs_ppm": a[-1] if a else None,
            "p99_abs_ppm": a[min(len(a) - 1, int(0.99 * len(a)))] if a else None,
            "median_abs_ppm": a[len(a) // 2] if a else None,
            "share_over_333ppm": round(sum(1 for x in a if x > 333) / len(a), 6) if a else None}


def adjtimex_history(samples: list[dict]) -> list[dict]:
    hist, cur = [], None
    for s in samples:
        key = (int(s["tick_us"]), int(s["freq_scaled"]), int(s["status"]), int(s["constant"]))
        if key != cur:
            hist.append({"from_i": int(s["i"]), "from_mono_ns": int(s["mono_ns"]), "tick_us": key[0],
                         "freq_ppm": key[1] / 65536, "status": hex(key[2]), "constant": key[3]})
            cur = key
    return hist


def analyze(samples: list[dict], ntp: list[dict], refs: list[dict], *, user_hz: int = 100, step_ms: float = 1.0,
            match_ms: float = 1.0, lag_s: float = 2.0) -> dict:
    steps = attribute_steps(find_steps(samples, int(step_ms * 1e6)), ntp, int(match_ms * 1e6), int(lag_s * 1e9))
    first, last = samples[0], samples[-1]
    span = {c: int(last[f"{c}_ns"]) - int(first[f"{c}_ns"]) for c in CLOCKS}
    hist = adjtimex_history(samples)
    nominal_tick = 1_000_000 / user_hz
    # predicted MONOTONIC/MONOTONIC_RAW from tick and freq, time-weighted over the samples
    pred_num = 0.0
    for a, b in zip(samples, samples[1:]):
        w = int(b["raw_ns"]) - int(a["raw_ns"])
        pred_num += w * (int(a["tick_us"]) / nominal_tick) * (1 + int(a["freq_scaled"]) / 65536 / 1e6)
    kernel = {"mono_over_raw_measured": span["mono"] / span["raw"],
              "mono_over_raw_predicted_from_adjtimex": pred_num / span["raw"],
              "nominal_tick_us": nominal_tick}
    kernel["mono_vs_raw_ppm"] = (kernel["mono_over_raw_measured"] - 1) * 1e6
    good = [r for r in refs if str(r.get("ref_qpc_ns", "")).strip()]
    rates: dict = {"n_reads": len(good)}
    if len(good) >= 3:
        x = [int(r["ref_qpc_ns"]) / 1e9 for r in good]
        half = [max((int(r[f"{c}_after_ns"]) - int(r[f"{c}_before_ns"])) / 2e9 for c in CLOCKS) for r in good]
        xspan = max(x) - min(x)
        for c in CLOCKS:
            y = [(int(r[f"{c}_before_ns"]) + int(r[f"{c}_after_ns"])) / 2e9 for r in good]
            rates[f"{c}_vs_host_qpc_ppm"] = (fit_slope(x, y) - 1) * 1e6
        rates["host_utc_vs_host_qpc_ppm"] = (fit_slope(x, [int(r["ref_utc_ns"]) / 1e9 for r in good]) - 1) * 1e6
        rates["span_s"] = xspan
        rates["max_bracket_half_ms"] = max(half) * 1e3
        rates["endpoint_slope_bound_ppm"] = 2 * max(half) / xspan * 1e6
        rates["real_minus_host_utc_ms"] = [round(((int(r["real_before_ns"]) + int(r["real_after_ns"])) / 2
                                                   - int(r["ref_utc_ns"])) / 1e6, 3) for r in good]
    attributed = sum(1 for s in steps if s["attributed"])
    return {"tool": TOOL_VERSION, "samples": len(samples), "duration_mono_s": span["mono"] / 1e9,
            "steps": steps, "n_steps": len(steps), "n_attributed_to_timesyncd": attributed,
            "n_unattributed": len(steps) - attributed, "step_total_ms": sum(s["step_ns"] for s in steps) / 1e6,
            "ntp_exchanges": len(ntp), "adjtimex_history": hist, "tick_values": sorted({h["tick_us"] for h in hist}),
            "kernel_adjustment": kernel, "rates": rates, "window_rates_3s": window_rates(samples, 3.0),
            "thresholds_note": f"probe settings step {step_ms} ms, match {match_ms} ms, lag {lag_s} s; diagnostic only"}


def _read(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--out-dir", required=True, type=Path)
    c.add_argument("--duration", type=float, required=True)
    c.add_argument("--period", type=float, default=0.05)
    c.add_argument("--ntp-period", type=float, default=5.0)
    c.add_argument("--ref-period", type=float, default=30.0)
    c.add_argument("--ref-python", help="Windows python.exe path for the host reference (omit: no reference)")
    a = sub.add_parser("analyze")
    a.add_argument("--in-dir", required=True, type=Path)
    a.add_argument("--step-ms", type=float, default=1.0)
    a.add_argument("--match-ms", type=float, default=1.0)
    a.add_argument("--lag-s", type=float, default=2.0)
    a.add_argument("--out-name", default="probe_analysis.json", help="new file in --in-dir (never overwritten)")
    args = ap.parse_args(argv)
    cmdline = sys.argv if argv is None else argv
    if args.cmd == "collect":
        m = collect(args.out_dir, args.duration, args.period, args.ntp_period, args.ref_period, args.ref_python, cmdline)
        print(f"COLLECTED {m['samples']} samples, errors {len(m['errors'])} -> {args.out_dir}")
        return 0
    man = json.loads((args.in_dir / "probe_manifest.json").read_text(encoding="utf-8"))
    res = analyze(_read(args.in_dir / "clock_samples.csv"), _read(args.in_dir / "ntp_messages.csv"),
                  _read(args.in_dir / "ref_reads.csv"), user_hz=int(man.get("user_hz", 100)), step_ms=args.step_ms,
                  match_ms=args.match_ms, lag_s=args.lag_s)
    res["input_manifest"] = {k: man[k] for k in ("boot_id", "clocksource", "user_hz", "parameters")}
    out = args.in_dir / args.out_name
    out.open("x", encoding="utf-8").write(json.dumps(res, indent=2) + "\n")
    k, r, w = res["kernel_adjustment"], res["rates"], res["window_rates_3s"]
    print(f"3 s windows: {w['windows']}, |rate error| median {w['median_abs_ppm']:.0f} ppm, p99 {w['p99_abs_ppm']:.0f} "
          f"ppm, max {w['max_abs_ppm']:.0f} ppm, share > 333 ppm {w['share_over_333ppm']}")
    print(f"steps {res['n_steps']} (timesyncd {res['n_attributed_to_timesyncd']}, other {res['n_unattributed']}), "
          f"total {res['step_total_ms']:.1f} ms over {res['duration_mono_s']:.0f} s; tick values {res['tick_values']}")
    print(f"MONOTONIC vs MONOTONIC_RAW {k['mono_vs_raw_ppm']:+.1f} ppm (predicted from adjtimex "
          f"{(k['mono_over_raw_predicted_from_adjtimex'] - 1) * 1e6:+.1f} ppm)")
    for key in sorted(x for x in r if x.endswith("_ppm")):
        print(f"  {key:<32} {r[key]:+.1f}")
    print(f"-> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
