"""Clock handshake (LA-3; spec file 03 section 7; E0 protocol E0-0 "Clock domain"; spec section 8 item 6).

Status: written 2026-11-09, tested with a fake `docker exec` and a fake clock only. Never run against
Docker. Whether `date +%s%N` gives nanoseconds inside the pinned Fabric and Caliper images (busybox
`date` may lack %N) is UNVERIFIED and is detected, not assumed.

handshake  for every container, --rounds times (round robin): host wall t0, `docker exec <c> <probe>`,
           host wall t1. The probe prints the container's wall clock since the epoch; its unit is
           taken from the digit count (19 ns, 16 us, 13 ms, 10 s). With remote resolution r:
               offset      = (remote + r/2) - (t0 + t1)/2
               uncertainty = (t1 - t0)/2 + r/2
           The sample with the smallest uncertainty is the container's estimate. Prints the plan; runs
           only with --execute.
drift      start vs end handshake per container: delta offset +- (u_start + u_end), drift in ppm.
bound      the V4-cross-source bound for a (client, peer) pair, |off_c - off_p| + u_c + u_p, as
           e1_reconcile.py `clock_offset_bound_ms`, and whether it meets the protocol bound (E0-0:
           1 ms). Prints only.

What the handshake can and cannot show: its bound is limited by the docker exec round trip (tens
of ms on Docker Desktop is plausible; NOT MEASURED). It measures the host <-> container offset for
aligning host-side 1 s telemetry. It is NOT the instrument for the E0-0 clock criterion.

E0-0 clock domain ("Caliper container and Fabric containers share one kernel clock, offset <= 1 ms"):
identity   decisive, structural. Every container prints /proc/sys/kernel/random/boot_id, the kernel
           release and its time namespace in one `docker exec`. CLOCK_REALTIME is one clock per
           kernel and is never virtualized: time namespaces offset only CLOCK_MONOTONIC and
           CLOCK_BOOTTIME (time_namespaces(7)). One boot_id therefore means one CLOCK_REALTIME, so
           the offset between containers is 0 by construction; reported stamps carry only their
           own truncation (Date.now and Fabric log times: 1 ms). The orchestration process itself
           is checked the same way (same boot_id when it runs in a WSL2 distro on the same kernel).
log-lag    falsification. The Docker daemon stamps a log line when it reads it, after the Fabric
           process formatted its own (ms-truncated) time, so on one clock
           docker_ns - fabric_ms * 1e6 >= 0 for every line. Any negative value refutes the shared
           clock. Inputs: e1_log_collector's orderer_write.csv / peer_commit.csv.

Outputs (handshake; new --out-dir): clock_samples.csv, clock_anomalies.csv, clock_handshake.json
Outputs (identity; new --out-dir): raw/<container>.stdout|.stderr, clock_identity.json
Outputs (drift): one new JSON file

Checks
  completeness  probe_failed, malformed_output, no_subsecond_support (the probe printed %N or N),
                container_unmeasured, drift_unmeasured
  consistency   remote_clock_step (a container's stamps decreased between rounds), host_clock_step
                (host wall and monotonic elapsed differ by more than 2 ms, or wall went backwards),
                offset_inconsistent (two samples' offset intervals do not overlap),
                implausible_remote_time (|offset| > 1 day: wrong unit or a broken clock),
                drift_exceeds_tolerance
  warnings      coarse_resolution (seconds only)
Standard library only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_telemetry_common as tc  # noqa: E402

TOOL_VERSION = "e1_clock_handshake 0.1 (2026-11-09)"
DEFAULT_PROBE = ["date", "+%s%N"]
COMPLETENESS = {"probe_failed", "malformed_output", "no_subsecond_support", "container_unmeasured", "drift_unmeasured",
                "identity_unreadable"}
CONSISTENCY = {"remote_clock_step", "host_clock_step", "offset_inconsistent", "implausible_remote_time",
               "drift_exceeds_tolerance", "different_kernel_clock"}
WARNINGS = {"coarse_resolution", "time_namespace_differs", "host_outside_clock_domain"}
IDENTITY_PROBE = ["sh", "-c", "cat /proc/sys/kernel/random/boot_id; uname -r; "
                              "readlink /proc/self/ns/time 2>/dev/null || echo none"]
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
SAME, DIFFERENT, UNVERIFIABLE = "SAME_KERNEL_CLOCK", "DIFFERENT_KERNEL_CLOCKS", "UNVERIFIABLE"
HOST_STEP_NS = 2_000_000
DAY_NS = 86_400 * 10**9
RESOLUTION_BY_DIGITS = {19: 1, 16: 1_000, 13: 1_000_000, 10: 1_000_000_000}
_CONTAINER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
SAMPLE_COLS = ["container", "round", "probe", "t0_wall_ns", "t1_wall_ns", "t0_mono_ns", "t1_mono_ns", "rtt_ns",
               "remote_raw", "remote_ns", "resolution_ns", "offset_ns", "uncertainty_ns", "exit_code", "status",
               "error"]


def parse_remote(text: str) -> tuple[int, int]:
    """Probe output -> (remote epoch ns at the start of its resolution step, resolution ns).
    ValueError('no_subsecond_support' | 'malformed_output: ...')."""
    s = text.strip()
    if re.fullmatch(r"\d{10}(%N|N)", s):
        raise ValueError("no_subsecond_support")
    if not re.fullmatch(r"\d+", s) or len(s) not in RESOLUTION_BY_DIGITS:
        raise ValueError(f"malformed_output: {s[:60]!r}")
    res = RESOLUTION_BY_DIGITS[len(s)]
    return int(s) * res, res


def run_handshake(containers: list[str], phase: str, out_dir: Path, runner: tc.Runner, *, rounds: int = 5,
                  probes: dict[str, list[str]] | None = None, wall_ns: Callable[[], int] = time.time_ns,
                  mono_ns: Callable[[], int] = time.monotonic_ns, sleep: Callable[[float], None] = time.sleep,
                  gap_s: float = 0.05, protocol_bound_ms: float = 1.0, argv: list[str] | None = None) -> dict:
    if not containers or len(set(containers)) != len(containers) or not all(_CONTAINER.match(c) for c in containers):
        raise ValueError("need distinct valid container names")
    if rounds < 1 or phase not in ("start", "end", "check"):
        raise ValueError("rounds >= 1 and phase start|end|check")
    probes = probes or {}
    out_dir.mkdir(parents=True, exist_ok=False)
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    rows: list[dict] = []
    start_w, start_m = wall_ns(), mono_ns()
    for r in range(rounds):
        for c in containers:
            probe = probes.get(c, DEFAULT_PROBE)
            t0w, t0m = wall_ns(), mono_ns()
            try:
                code, out, err = runner(["docker", "exec", c] + probe)
            except (OSError, subprocess.SubprocessError) as exc:
                code, out, err = None, b"", repr(exc).encode()
            t1w, t1m = wall_ns(), mono_ns()
            row = {"container": c, "round": r, "probe": " ".join(probe), "t0_wall_ns": t0w, "t1_wall_ns": t1w,
                   "t0_mono_ns": t0m, "t1_mono_ns": t1m, "rtt_ns": t1w - t0w, "exit_code": code,
                   "remote_raw": out.decode("utf-8", "replace").strip()[:80], "status": "failed", "error": ""}
            src = f"{c} round {r}"
            if t1w < t0w or abs((t1w - t0w) - (t1m - t0m)) > HOST_STEP_NS:
                an.add("host_clock_step", c, f"wall {t1w - t0w} ns vs monotonic {t1m - t0m} ns", src)
                row["error"] = "host clock step during the sample"
                rows.append(row)
                continue
            if code != 0:
                row["error"] = err.decode("utf-8", "replace").strip()[-200:]
                an.add("probe_failed", c, f"exit {code}: {row['error']}", src)
                rows.append(row)
                continue
            try:
                remote, res = parse_remote(out.decode("utf-8", "replace"))
            except ValueError as exc:
                kind = str(exc).split(":")[0]
                an.add(kind, c, str(exc), src, row["remote_raw"])
                row["error"] = str(exc)
                rows.append(row)
                continue
            mid2 = t0w + t1w                                   # 2 x host midpoint, kept integral
            offset = (2 * remote + res - mid2) // 2
            unc = (t1w - t0w + res + 1) // 2
            row.update(remote_ns=remote, resolution_ns=res, offset_ns=offset, uncertainty_ns=unc, status="ok")
            if abs(offset) > DAY_NS:
                an.add("implausible_remote_time", c, f"offset {offset / 1e9:.3f} s", src, row["remote_raw"])
                row["status"] = "implausible"
            if res >= 1_000_000_000:
                an.add("coarse_resolution", c, "seconds only: uncertainty >= 0.5 s", src, row["remote_raw"])
            rows.append(row)
            sleep(gap_s)
    end_w, end_m = wall_ns(), mono_ns()
    if abs((end_w - start_w) - (end_m - start_m)) > HOST_STEP_NS:
        an.add("host_clock_step", "", f"over the handshake: wall {end_w - start_w} ns vs monotonic {end_m - start_m} ns")
    per = {}
    for c in containers:
        ok = [x for x in rows if x["container"] == c and x["status"] == "ok"]
        stamps = [x["remote_ns"] for x in ok]
        if any(b < a for a, b in zip(stamps, stamps[1:])):
            an.add("remote_clock_step", c, "remote stamps decreased between rounds")
        for i, a in enumerate(ok):
            for b in ok[i + 1:]:
                if abs(a["offset_ns"] - b["offset_ns"]) > a["uncertainty_ns"] + b["uncertainty_ns"]:
                    an.add("offset_inconsistent", c, f"rounds {a['round']} and {b['round']}: offsets "
                                                     f"{a['offset_ns']} and {b['offset_ns']} ns do not overlap")
        if not ok:
            an.add("container_unmeasured", c, "no usable sample")
            per[c] = {"status": "UNMEASURED", "samples_ok": 0}
            continue
        best = min(ok, key=lambda x: (x["uncertainty_ns"], x["round"]))
        per[c] = {"status": "MEASURED", "samples_ok": len(ok), "samples": rounds,
                  "offset_ns": best["offset_ns"], "uncertainty_ns": best["uncertainty_ns"], "best_round": best["round"],
                  "rtt_ns": best["rtt_ns"], "resolution_ns": best["resolution_ns"],
                  "t_mid_wall_ns": (best["t0_wall_ns"] + best["t1_wall_ns"]) // 2,
                  "offset_range_ns": [min(x["offset_ns"] for x in ok), max(x["offset_ns"] for x in ok)]}
    pairs = []
    measured = [c for c in containers if per[c]["status"] == "MEASURED"]
    for i, a in enumerate(measured):
        for b in measured[i + 1:]:
            pairs.append(pair_bound(per, a, b, protocol_bound_ms))
    outputs = {"clock_samples.csv": tc.write_csv_x(out_dir / "clock_samples.csv", SAMPLE_COLS, rows),
               "clock_anomalies.csv": tc.write_csv_x(out_dir / "clock_anomalies.csv", tc.ANOMALY_COLS, an.rows())}
    result = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "phase": phase,
              "containers": per, "pairs": pairs, "protocol_bound_ms": protocol_bound_ms,
              "parameters": {"rounds": rounds, "gap_s": gap_s, "probes": {c: probes.get(c, DEFAULT_PROBE)
                                                                          for c in containers}},
              "start_utc": tc.utc_iso(start_w), "end_utc": tc.utc_iso(end_w),
              "method_note": "offset = container clock - host clock; uncertainty = half round trip + half "
                             "remote resolution; bounds are limited by the docker exec round trip",
              "status": an.status(), "anomalies": an.counts(), "outputs_sha256": outputs}
    tc.write_json_x(out_dir / "clock_handshake.json", result)
    return result


def pair_bound(per: dict, a: str, b: str, protocol_bound_ms: float = 1.0) -> dict | None:
    """|off_a - off_b| + u_a + u_b in ms: the largest clock difference consistent with the samples."""
    pa, pb = per.get(a), per.get(b)
    if not pa or not pb or pa.get("status") != "MEASURED" or pb.get("status") != "MEASURED":
        return None
    diff = pa["offset_ns"] - pb["offset_ns"]
    unc = pa["uncertainty_ns"] + pb["uncertainty_ns"]
    bound_ms = (abs(diff) + unc) / 1e6
    return {"a": a, "b": b, "diff_ms": diff / 1e6, "uncertainty_ms": unc / 1e6, "bound_ms": bound_ms,
            "same_clock_consistent": abs(diff) <= unc, "within_protocol_bound": bound_ms <= protocol_bound_ms}


def drift(start: dict, end: dict, out_path: Path, *, tolerance_ms: float = 2.0) -> dict:
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    rows = {}
    for c in sorted(set(start["containers"]) | set(end["containers"])):
        s, e = start["containers"].get(c, {}), end["containers"].get(c, {})
        if s.get("status") != "MEASURED" or e.get("status") != "MEASURED":
            an.add("drift_unmeasured", c, f"start {s.get('status')}, end {e.get('status')}")
            rows[c] = {"status": "UNMEASURED"}
            continue
        delta = e["offset_ns"] - s["offset_ns"]
        unc = s["uncertainty_ns"] + e["uncertainty_ns"]
        elapsed = e["t_mid_wall_ns"] - s["t_mid_wall_ns"]
        excess_ms = (abs(delta) - unc) / 1e6
        rows[c] = {"status": "MEASURED", "delta_ns": delta, "uncertainty_ns": unc, "elapsed_ns": elapsed,
                   "drift_ppm": delta / elapsed * 1e6 if elapsed > 0 else None,
                   "excess_beyond_uncertainty_ms": max(0.0, excess_ms)}
        if excess_ms > tolerance_ms:
            an.add("drift_exceeds_tolerance", c, f"|delta| {abs(delta) / 1e6:.3f} ms exceeds uncertainty "
                                                 f"{unc / 1e6:.3f} ms by more than {tolerance_ms} ms")
    result = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "tolerance_ms": tolerance_ms,
              "start_utc": start.get("start_utc"), "end_utc": end.get("end_utc"), "containers": rows,
              "status": an.status(), "anomalies": an.counts(), "anomaly_rows": an.rows()}
    tc.write_json_x(out_path, result)
    return result


# ------------------------------------------------------------------ E0-0 clock domain: identity
def parse_identity(text: str) -> dict:
    """Three lines: boot_id, kernel release, time namespace link (or 'none'). ValueError if malformed."""
    lines = [x.strip() for x in text.strip().splitlines()]
    if len(lines) != 3:
        raise ValueError(f"want 3 lines (boot_id, release, time ns), got {len(lines)}")
    boot, release, tns = lines
    if not _UUID.match(boot):
        raise ValueError(f"boot_id {boot[:60]!r} is not a lowercase UUID")
    if not release or " " in release:
        raise ValueError(f"kernel release {release[:60]!r}")
    return {"boot_id": boot, "kernel_release": release, "time_ns": tns}


def local_identity() -> dict:
    """The orchestration process's own kernel identity (Linux only; Windows has a different clock)."""
    if not sys.platform.startswith("linux"):
        return {"status": "NOT_LINUX", "platform": sys.platform}
    try:
        boot = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        try:
            tns = os.readlink("/proc/self/ns/time")
        except OSError:
            tns = "none"
        return {"status": "OK", "boot_id": boot, "kernel_release": os.uname().release, "time_ns": tns}
    except OSError as exc:
        return {"status": "UNREADABLE", "error": repr(exc)}


def run_identity(containers: list[str], out_dir: Path, runner: tc.Runner, *,
                 local: Callable[[], dict] = local_identity, label: str = "", argv: list[str] | None = None) -> dict:
    """Structural clock-domain check (see the module docstring). Writes clock_identity.json."""
    if not containers or len(set(containers)) != len(containers) or not all(_CONTAINER.match(c) for c in containers):
        raise ValueError("need distinct valid container names")
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "raw").mkdir()
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    members = {}
    for c in containers:
        try:
            code, out, err = runner(["docker", "exec", c] + IDENTITY_PROBE)
        except (OSError, subprocess.SubprocessError) as exc:
            code, out, err = None, b"", repr(exc).encode()
        tc.write_bytes_x(out_dir / "raw" / f"{c}.stdout", out)
        tc.write_bytes_x(out_dir / "raw" / f"{c}.stderr", err)
        if code != 0:
            an.add("identity_unreadable", c, f"exit {code}: {err.decode('utf-8', 'replace').strip()[-200:]}")
            members[c] = {"status": "UNREADABLE"}
            continue
        try:
            members[c] = {"status": "OK", **parse_identity(out.decode("utf-8", "replace"))}
        except ValueError as exc:
            an.add("identity_unreadable", c, str(exc), raw=out.decode("utf-8", "replace")[:200])
            members[c] = {"status": "MALFORMED"}
    ok = {c: m for c, m in members.items() if m["status"] == "OK"}
    boots = sorted({m["boot_id"] for m in ok.values()})
    if len(ok) != len(containers):
        verdict = UNVERIFIABLE
    elif len(boots) == 1:
        verdict = SAME
    else:
        verdict = DIFFERENT
        an.add("different_kernel_clock", ",".join(containers),
               "; ".join(f"{c}={m['boot_id']}" for c, m in ok.items()))
    if len({m["time_ns"] for m in ok.values()}) > 1:
        an.add("time_namespace_differs", "", "CLOCK_REALTIME is not virtualized by time namespaces (time_namespaces(7)); "
                                             "only monotonic stamps differ: never compare them across containers")
    loc = local()
    in_domain = None
    if loc.get("status") == "OK" and len(boots) == 1:
        in_domain = loc["boot_id"] == boots[0]
    if in_domain is not True:
        an.add("host_outside_clock_domain", "local", f"local {loc.get('status')}: host-side timestamps need the "
                                                     "handshake offset (round-trip-limited) for alignment")
    result = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "label": label,
              "probe": IDENTITY_PROBE, "members": members, "local": loc, "boot_ids": boots, "verdict": verdict,
              "host_in_container_clock_domain": in_domain,
              "criterion": {"rule": "E0-0: Caliper and Fabric containers share one kernel clock (offset <= 1 ms)",
                            "status": "DEMONSTRATED" if verdict == SAME else "NOT DEMONSTRATED",
                            "basis": "one boot_id = one kernel = one CLOCK_REALTIME (not namespaced): offset 0 by "
                                     "construction; residual is the 1 ms truncation of each reported stamp"},
              "status": an.status(), "anomalies": an.counts(), "anomaly_rows": an.rows()}
    tc.write_json_x(out_dir / "clock_identity.json", result)
    return result


def log_lag(rows: list[dict]) -> dict:
    """Falsification: docker receive time minus Fabric header time per log event (same clock => >= 0).
    rows: e1_log_collector orderer_write.csv / peer_commit.csv rows (canonical rows with both stamps)."""
    per: dict[str, list[int]] = {}
    for r in rows:
        if str(r.get("canonical")).lower() != "true" or r.get("docker_ns") in ("", None) \
                or r.get("fabric_ms") in ("", None):
            continue
        per.setdefault(r["container"], []).append(int(r["docker_ns"]) - int(r["fabric_ms"]) * 1_000_000)
    out = {}
    for c, lags in sorted(per.items()):
        s = sorted(lags)
        out[c] = {"events": len(s), "min_lag_ms": s[0] / 1e6, "median_lag_ms": s[len(s) // 2] / 1e6,
                  "max_lag_ms": s[-1] / 1e6, "inversions": sum(1 for x in s if x < 0)}
    inv = sum(v["inversions"] for v in out.values())
    return {"status": "NOT EVALUATED" if not out else ("FAIL" if inv else "PASS"), "inversions": inv,
            "containers": out,
            "rule": "docker_ns - fabric_ms*1e6 >= 0 for every event (daemon stamps after the process wrote)",
            "note": "one-sided: bounds how far a container clock can lead the daemon clock; the identity "
                    "check is the decisive test"}


def domain_gate(identities: list[dict], lag: dict | None) -> tuple[str, str]:
    """E0-0 clock-domain gate from identity checks (all must be SAME_KERNEL_CLOCK with one boot_id) and the
    log-lag falsification (no inversion). Returns (PASS | FAIL | NOT EVALUATED, detail)."""
    if not identities:
        return "NOT EVALUATED", "no identity check ran"
    verdicts = [i.get("verdict") for i in identities]
    boots = sorted({b for i in identities for b in i.get("boot_ids", [])})
    if any(v == DIFFERENT for v in verdicts) or len(boots) > 1:
        return "FAIL", f"kernel clocks differ: verdicts {verdicts}, boot_ids {boots}"
    if any(v != SAME for v in verdicts):
        return "FAIL", f"identity not demonstrated: verdicts {verdicts}"
    if lag is not None and lag["status"] == "FAIL":
        return "FAIL", f"log-lag falsification: {lag['inversions']} inversion(s) {lag['containers']}"
    tail = "log-lag not evaluated" if lag is None or lag["status"] == "NOT EVALUATED" else \
        f"log-lag PASS ({sum(v['events'] for v in lag['containers'].values())} events, 0 inversions)"
    return "PASS", f"one kernel clock (boot_id {boots[0]}) at every check; {tail}"


# ------------------------------------------------------------------ CLI
def _probes(values: list[str]) -> dict[str, list[str]]:
    out = {}
    for v in values or []:
        name, sep, cmd = v.partition("=")
        if not sep or not _CONTAINER.match(name) or not cmd.strip():
            raise ValueError(f"--probe wants NAME=COMMAND, got {v!r}")
        out[name] = shlex.split(cmd)
    return out


def main(argv: list[str] | None = None, runner: tc.Runner | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    h = sub.add_parser("handshake")
    h.add_argument("--container", action="append", required=True)
    h.add_argument("--phase", required=True, choices=["start", "end", "check"])
    h.add_argument("--rounds", type=int, default=5)
    h.add_argument("--probe", action="append", default=[])
    h.add_argument("--protocol-bound-ms", type=float, default=1.0)
    h.add_argument("--out-dir", required=True, type=Path)
    h.add_argument("--execute", action="store_true", help="run docker exec (needs Docker authorization)")
    d = sub.add_parser("drift")
    d.add_argument("--start", required=True, type=Path)
    d.add_argument("--end", required=True, type=Path)
    d.add_argument("--tolerance-ms", type=float, default=2.0)
    d.add_argument("--out", required=True, type=Path)
    b = sub.add_parser("bound")
    b.add_argument("--handshake", required=True, type=Path)
    b.add_argument("--client", required=True)
    b.add_argument("--peer", required=True)
    i = sub.add_parser("identity")
    i.add_argument("--container", action="append", required=True)
    i.add_argument("--out-dir", required=True, type=Path)
    i.add_argument("--execute", action="store_true", help="run docker exec (needs Docker authorization)")
    lg = sub.add_parser("log-lag")
    lg.add_argument("--events", action="append", required=True, type=Path,
                    help="e1_log_collector orderer_write.csv / peer_commit.csv (repeatable)")
    args = ap.parse_args(argv)
    cmdline = sys.argv if argv is None else argv
    try:
        if args.cmd == "log-lag":
            rows = [r for p in args.events for r in tc.read_csv(p)]
            r = log_lag(rows)
            print(json.dumps(r, indent=2))
            return {"PASS": 0, "FAIL": 1}.get(r["status"], 2)
        if args.cmd == "identity":
            if args.out_dir.exists():
                print(f"refusing to overwrite {args.out_dir}")
                return 2
            if not args.execute:
                for c in args.container:
                    print("PLAN " + " ".join(["docker", "exec", c] + IDENTITY_PROBE))
                print("dry run: nothing executed or written (add --execute once Docker use is authorized)")
                return 0
            r = run_identity(args.container, args.out_dir, runner or tc.subprocess_runner, argv=cmdline)
            print(f"IDENTITY {r['verdict']} criterion {r['criterion']['status']} "
                  f"host_in_domain={r['host_in_container_clock_domain']} -> {args.out_dir}")
            return 0 if r["verdict"] == SAME else 1
        if args.cmd == "bound":
            hs = json.loads(args.handshake.read_text(encoding="utf-8"))
            pb = pair_bound(hs["containers"], args.client, args.peer, hs.get("protocol_bound_ms", 1.0))
            if pb is None:
                print("BOUND UNAVAILABLE: a container is not MEASURED")
                return 1
            print(json.dumps(pb, indent=2))
            return 0 if pb["within_protocol_bound"] else 1
        if args.cmd == "drift":
            if args.out.exists():
                print(f"refusing to overwrite {args.out}")
                return 2
            r = drift(json.loads(args.start.read_text(encoding="utf-8")), json.loads(args.end.read_text(encoding="utf-8")),
                      args.out, tolerance_ms=args.tolerance_ms)
            print(f"DRIFT {r['status']} -> {args.out}")
            return tc.EXIT[r["status"]]
        if args.out_dir.exists():
            print(f"refusing to overwrite {args.out_dir}")
            return 2
        probes = _probes(args.probe)
        if not args.execute:
            for c in args.container:
                print("PLAN " + " ".join(["docker", "exec", c] + probes.get(c, DEFAULT_PROBE)) + f"   x{args.rounds}")
            print("dry run: nothing executed or written (add --execute once Docker use is authorized)")
            return 0
        r = run_handshake(args.container, args.phase, args.out_dir, runner or tc.subprocess_runner,
                          rounds=args.rounds, probes=probes, protocol_bound_ms=args.protocol_bound_ms, argv=cmdline)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as err:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {err!r}")
        return 2
    for c, v in r["containers"].items():
        if v["status"] == "MEASURED":
            print(f"  {c:<28} offset {v['offset_ns'] / 1e6:+.3f} ms +- {v['uncertainty_ns'] / 1e6:.3f} ms")
        else:
            print(f"  {c:<28} UNMEASURED")
    print(f"CLOCK {r['status']} -> {args.out_dir}")
    return tc.EXIT[r["status"]]


if __name__ == "__main__":
    sys.exit(main())
