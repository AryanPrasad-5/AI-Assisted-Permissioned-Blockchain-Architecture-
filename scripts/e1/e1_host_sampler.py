#!/usr/bin/env python3
"""E1 host sampler (instrumentation B3): CPU, memory pressure, disk and network of the machine under the testbed, once per second.

Why it exists: container CPU alone cannot rule host contention in or out (the WSL VM has 24 CPUs and shares the host with Windows). The P1a rounds had only
before/after host readings. This tool records the host while a run's Caliper phase is in progress, so that a later reading of a run can say whether the host
itself was busy. It is evidence only: nothing it records labels a run (no host threshold exists in the project), but a run whose host record is incomplete is
not accepted as valid (the driver's G-evidence gate).

  sample     --out-dir D [--stop-file F] [--interval 1] [--max-seconds 600] [--proc-root /proc] [--no-windows] [--execute]
             WSL side, every tick, from the VM kernel's own files: /proc/stat (total and per-CPU busy, iowait, steal), /proc/pressure/{cpu,memory,io} (PSI stall
             totals), /proc/loadavg, /proc/meminfo, /proc/diskstats (whole disks), /proc/net/dev (every non-loopback interface, and lo).
             Windows side, from ONE background `typeperf.exe` (WSL interop, one row per second): processor utility and time, processor queue, physical-disk time,
             bytes/s, transfers/s, latency and queue, bytes/s of every network interface (summed), available memory, commit and pages/s.
             Prints the plan; runs only with --execute. Stops at --stop-file, or after --max-seconds (a guard so that it can never outlive its run).
  summarize  --samples wsl_samples.csv --win win_counters.csv --from-ms A --to-ms B
             descriptive per-window figures (what the driver stores in a run verdict)

Time base. Every row carries the WSL realtime clock (the clock the Caliper recorder uses). Windows rows carry typeperf's own sample time converted to UTC with the
whole-quarter-hour offset inferred from the first row (typeperf's header has no time-zone bias here), plus the receipt time and the lag between them: measured on this
machine, a piped typeperf prints each row about one interval after its sample time, so the receipt time is NEVER used as the sample time.

What it cannot see (stated, not hidden): the Docker bridge network lives in the docker-desktop distribution's network namespace, which is not visible from the Ubuntu
distribution (only `lo` and `eth0` are). Traffic between the Fabric containers is therefore measured only by the per-container counters of the resource sampler.

Standard library only; runs inside Ubuntu WSL (the Windows counters need typeperf.exe through interop). Never overwrites a file.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import math
import queue
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_telemetry_common as tc  # noqa: E402

TOOL_VERSION = "e1_host_sampler 0.1 (2026-11-13, instrumentation B3)"

COMPLETENESS = {"no_wsl_samples", "sampling_gap", "proc_unreadable", "win_no_header", "win_no_rows", "win_gap", "win_required_missing"}
CONSISTENCY = {"time_order", "counter_reset"}
WARNINGS = {"psi_unavailable", "diskstats_no_devices", "win_optional_missing", "win_value_missing", "win_malformed_line", "win_error_line",
            "win_exited_early", "win_disabled", "win_clock_skew", "win_write_failed"}

INTERVAL_S = 1.0
MAX_SECONDS = 600.0
GAP_FACTOR = 2.5                  # a gap above this many intervals is a sampling gap
WIN_ROW_GAP_S = 2.5               # typeperf rows are one second apart; a longer hole is a gap
EPOCH = datetime.datetime(1970, 1, 1)

# ---------------------------------------------------------------------------------------------- WSL kernel files (pure parsers)
CPU_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")
DISK_RE = re.compile(r"^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|nvme\d+n\d+|mmcblk\d+)$")        # whole disks only: partitions would double count
WSL_COLS = ["tick", "wall_ns", "mono_ns", "dt_s",
            "cpu_busy_pct", "cpu_user_pct", "cpu_system_pct", "cpu_iowait_pct", "cpu_steal_pct", "cpu_irq_pct", "cpu_max_core_busy_pct", "cpus",
            "load1", "runnable", "mem_available_mb", "swap_used_mb",
            "psi_cpu_some_pct", "psi_cpu_full_pct", "psi_mem_some_pct", "psi_mem_full_pct", "psi_io_some_pct", "psi_io_full_pct",
            "disk_read_mb_s", "disk_write_mb_s", "disk_read_iops", "disk_write_iops", "disk_util_max_pct",
            "net_if_rx_mb_s", "net_if_tx_mb_s", "net_if_rx_pkts_s", "net_if_tx_pkts_s", "net_if_err_drop_s", "net_lo_mb_s"]
PSI_FILES = (("cpu", "psi_cpu"), ("memory", "psi_mem"), ("io", "psi_io"))


def read_text(path: Path) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def parse_stat(text: str | None) -> dict[str, dict[str, int]]:
    """/proc/stat -> {'cpu': {...jiffies...}, 'cpu0': {...}, ...}. Guest time is already inside user, so the total is the first eight fields."""
    out: dict[str, dict[str, int]] = {}
    for line in (text or "").splitlines():
        if not line.startswith("cpu"):
            continue
        parts = line.split()
        try:
            vals = [int(x) for x in parts[1:9]]
        except ValueError:
            continue
        vals += [0] * (len(CPU_FIELDS) - len(vals))
        out[parts[0]] = dict(zip(CPU_FIELDS, vals))
    return out


def cpu_percentages(prev: dict, cur: dict) -> dict | None:
    """Percent of CPU time between two /proc/stat snapshots; {'reset': True} when a counter went backwards; None when there is no elapsed time."""
    a, b = prev.get("cpu"), cur.get("cpu")
    if not a or not b:
        return None
    d = {k: b[k] - a[k] for k in CPU_FIELDS}
    if any(v < 0 for v in d.values()):
        return {"reset": True}
    tot = sum(d.values())
    if tot <= 0:
        return None
    out = {"busy": 100.0 * (tot - d["idle"] - d["iowait"]) / tot, "user": 100.0 * (d["user"] + d["nice"]) / tot,
           "system": 100.0 * d["system"] / tot, "iowait": 100.0 * d["iowait"] / tot, "steal": 100.0 * d["steal"] / tot,
           "irq": 100.0 * (d["irq"] + d["softirq"]) / tot}
    cores = []
    for name, cb in cur.items():
        ca = prev.get(name)
        if name == "cpu" or not ca:
            continue
        dd = {k: cb[k] - ca[k] for k in CPU_FIELDS}
        t = sum(dd.values())
        if t > 0 and all(v >= 0 for v in dd.values()):
            cores.append(100.0 * (t - dd["idle"] - dd["iowait"]) / t)
    out["max_core"] = max(cores) if cores else None
    out["cpus"] = sum(1 for n in cur if n != "cpu")
    return out


def parse_pressure(text: str | None) -> dict[str, int]:
    """/proc/pressure/<x> -> {'some': total_us, 'full': total_us} (cumulative stall microseconds)."""
    out: dict[str, int] = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if len(parts) < 2 or parts[0] not in ("some", "full"):
            continue
        kv = dict(p.split("=", 1) for p in parts[1:] if "=" in p)
        try:
            out[parts[0]] = int(kv["total"])
        except (KeyError, ValueError):
            pass
    return out


def parse_loadavg(text: str | None) -> dict:
    parts = (text or "").split()
    try:
        return {"load1": float(parts[0]), "runnable": int(parts[3].split("/")[0])}
    except (IndexError, ValueError):
        return {}


def parse_meminfo(text: str | None) -> dict:
    kb: dict[str, int] = {}
    for line in (text or "").splitlines():
        m = re.match(r"^(MemAvailable|SwapTotal|SwapFree):\s+(\d+)\s*kB", line)
        if m:
            kb[m.group(1)] = int(m.group(2))
    out: dict = {}
    if "MemAvailable" in kb:
        out["mem_available_mb"] = kb["MemAvailable"] / 1024.0
    if "SwapTotal" in kb and "SwapFree" in kb:
        out["swap_used_mb"] = (kb["SwapTotal"] - kb["SwapFree"]) / 1024.0
    return out


def parse_diskstats(text: str | None) -> dict[str, dict[str, int]]:
    """/proc/diskstats -> whole disks only. Sectors are 512 bytes; io_ticks_ms is the time the device was busy."""
    out: dict[str, dict[str, int]] = {}
    for line in (text or "").splitlines():
        p = line.split()
        if len(p) < 14 or not DISK_RE.match(p[2]):
            continue
        try:
            v = [int(x) for x in p[3:14]]
        except ValueError:
            continue
        out[p[2]] = {"reads": v[0], "sectors_read": v[2], "writes": v[4], "sectors_written": v[6], "io_ticks_ms": v[9]}
    return out


def parse_net_dev(text: str | None) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for line in (text or "").splitlines():
        if ":" not in line:
            continue
        name, rest = line.split(":", 1)
        f = rest.split()
        if len(f) < 16:
            continue
        try:
            v = [int(x) for x in f[:16]]
        except ValueError:
            continue
        out[name.strip()] = {"rx_bytes": v[0], "rx_packets": v[1], "rx_errs": v[2], "rx_drop": v[3],
                             "tx_bytes": v[8], "tx_packets": v[9], "tx_errs": v[10], "tx_drop": v[11]}
    return out


def snapshot(proc_root: Path, wall_ns: int, mono_ns: int) -> dict:
    root = Path(proc_root)
    return {"wall_ns": wall_ns, "mono_ns": mono_ns, "stat_text": read_text(root / "stat"),
            "stat": parse_stat(read_text(root / "stat")), "load": parse_loadavg(read_text(root / "loadavg")),
            "mem": parse_meminfo(read_text(root / "meminfo")), "disks": parse_diskstats(read_text(root / "diskstats")),
            "net": parse_net_dev(read_text(root / "net" / "dev")),
            "psi": {key: parse_pressure(read_text(root / "pressure" / name)) for name, key in PSI_FILES}}


def _delta(a: int, b: int, resets: list[str], what: str) -> int | None:
    if b < a:
        resets.append(what)
        return None
    return b - a


def rates(prev: dict | None, cur: dict, tick: int) -> tuple[dict, list[tuple[str, str]]]:
    """One output row for `cur`; per-second figures use the real elapsed monotonic time since `prev`. Returns (row, [(anomaly kind, detail)])."""
    row: dict = {"tick": tick, "wall_ns": cur["wall_ns"], "mono_ns": cur["mono_ns"]}
    notes: list[tuple[str, str]] = []
    row.update(load1=cur["load"].get("load1"), runnable=cur["load"].get("runnable"),
               mem_available_mb=cur["mem"].get("mem_available_mb"), swap_used_mb=cur["mem"].get("swap_used_mb"))
    if prev is None:
        return row, notes
    dt = (cur["mono_ns"] - prev["mono_ns"]) / 1e9
    row["dt_s"] = round(dt, 6)
    if dt <= 0:
        notes.append(("time_order", f"monotonic clock did not advance ({dt})"))
        return row, notes
    resets: list[str] = []
    cp = cpu_percentages(prev["stat"], cur["stat"])
    if cp and cp.get("reset"):
        resets.append("/proc/stat")
    elif cp:
        row.update(cpu_busy_pct=cp["busy"], cpu_user_pct=cp["user"], cpu_system_pct=cp["system"], cpu_iowait_pct=cp["iowait"],
                   cpu_steal_pct=cp["steal"], cpu_irq_pct=cp["irq"], cpu_max_core_busy_pct=cp["max_core"], cpus=cp["cpus"])
    for key in ("psi_cpu", "psi_mem", "psi_io"):
        for kind in ("some", "full"):
            a, b = prev["psi"][key].get(kind), cur["psi"][key].get(kind)
            if a is None or b is None:
                continue
            d = _delta(a, b, resets, f"pressure {key} {kind}")
            if d is not None:
                row[f"{key}_{kind}_pct"] = min(100.0, 100.0 * d / (dt * 1e6))
    common = sorted(set(prev["disks"]) & set(cur["disks"]))
    if common:
        sums = {"sectors_read": 0, "sectors_written": 0, "reads": 0, "writes": 0}
        util = []
        ok = True
        for dev in common:
            for k in sums:
                d = _delta(prev["disks"][dev][k], cur["disks"][dev][k], resets, f"diskstats {dev} {k}")
                if d is None:
                    ok = False
                else:
                    sums[k] += d
            d = _delta(prev["disks"][dev]["io_ticks_ms"], cur["disks"][dev]["io_ticks_ms"], resets, f"diskstats {dev} io_ticks")
            if d is not None:
                util.append(min(100.0, 100.0 * d / (dt * 1000.0)))
        if ok:
            row.update(disk_read_mb_s=sums["sectors_read"] * 512 / 1e6 / dt, disk_write_mb_s=sums["sectors_written"] * 512 / 1e6 / dt,
                       disk_read_iops=sums["reads"] / dt, disk_write_iops=sums["writes"] / dt)
        if util:
            row["disk_util_max_pct"] = max(util)
    elif tick == 1 and not cur["disks"] and not prev["disks"]:
        notes.append(("diskstats_no_devices", "no whole-disk device in /proc/diskstats"))
    nif = sorted((set(prev["net"]) & set(cur["net"])) - {"lo"})
    if nif:
        agg = {"rx_bytes": 0, "tx_bytes": 0, "rx_packets": 0, "tx_packets": 0, "errdrop": 0}
        ok = True
        for n in nif:
            for src, dst in (("rx_bytes", "rx_bytes"), ("tx_bytes", "tx_bytes"), ("rx_packets", "rx_packets"), ("tx_packets", "tx_packets")):
                d = _delta(prev["net"][n][src], cur["net"][n][src], resets, f"net {n} {src}")
                if d is None:
                    ok = False
                else:
                    agg[dst] += d
            for src in ("rx_errs", "rx_drop", "tx_errs", "tx_drop"):
                d = _delta(prev["net"][n][src], cur["net"][n][src], resets, f"net {n} {src}")
                if d is not None:
                    agg["errdrop"] += d
        if ok:
            row.update(net_if_rx_mb_s=agg["rx_bytes"] / 1e6 / dt, net_if_tx_mb_s=agg["tx_bytes"] / 1e6 / dt,
                       net_if_rx_pkts_s=agg["rx_packets"] / dt, net_if_tx_pkts_s=agg["tx_packets"] / dt, net_if_err_drop_s=agg["errdrop"] / dt)
    if "lo" in prev["net"] and "lo" in cur["net"]:
        d = _delta(prev["net"]["lo"]["rx_bytes"], cur["net"]["lo"]["rx_bytes"], resets, "net lo rx_bytes")
        if d is not None:
            row["net_lo_mb_s"] = d / 1e6 / dt
    for what in resets:
        notes.append(("counter_reset", what))
    return row, notes


# ---------------------------------------------------------------------------------------------- Windows counters (typeperf)
WIN_COUNTER_PATHS = [
    r"\Processor Information(_Total)\% Processor Utility",
    r"\Processor(_Total)\% Processor Time",              # not the 'Processor Information' form: typeperf drops a second counter of the same object and instance
    r"\System\Processor Queue Length",
    r"\PhysicalDisk(_Total)\% Disk Time",
    r"\PhysicalDisk(_Total)\Disk Bytes/sec",
    r"\PhysicalDisk(_Total)\Disk Transfers/sec",
    r"\PhysicalDisk(_Total)\Avg. Disk sec/Transfer",
    r"\PhysicalDisk(_Total)\Current Disk Queue Length",
    r"\Network Interface(*)\Bytes Total/sec",
    r"\Memory\Available MBytes",
    r"\Memory\% Committed Bytes In Use",
    r"\Memory\Pages/sec",
]
WIN_SPECS = (       # (column, regex on the counter path without the \\HOST prefix, required for a usable record)
    ("cpu_utility_pct", r"^\\Processor Information\(_Total\)\\% Processor Utility$", True),
    ("cpu_time_pct", r"^\\Processor\(_Total\)\\% Processor Time$", False),
    ("proc_queue", r"^\\System\\Processor Queue Length$", False),
    ("disk_time_pct", r"^\\PhysicalDisk\(_Total\)\\% Disk Time$", True),
    ("disk_bytes_s", r"^\\PhysicalDisk\(_Total\)\\Disk Bytes/sec$", True),
    ("disk_iops", r"^\\PhysicalDisk\(_Total\)\\Disk Transfers/sec$", False),
    ("disk_sec_per_transfer", r"^\\PhysicalDisk\(_Total\)\\Avg\. Disk sec/Transfer$", False),
    ("disk_queue", r"^\\PhysicalDisk\(_Total\)\\Current Disk Queue Length$", False),
    ("net_bytes_s", r"^\\Network Interface\(.*\)\\Bytes Total/sec$", True),
    ("mem_available_mb", r"^\\Memory\\Available MBytes$", True),
    ("commit_pct", r"^\\Memory\\% Committed Bytes In Use$", False),
    ("pages_s", r"^\\Memory\\Pages/sec$", False),
)
WIN_COLS = ["row", "t_wall_ns", "recv_wall_ns", "recv_mono_ns", "lag_ms", "t_local"] + [c for c, _, _ in WIN_SPECS] + ["net_instances"]
_HOST_PREFIX = re.compile(r"^\\\\[^\\]+")
_TS_LOCAL = "%m/%d/%Y %H:%M:%S.%f"


def typeperf_argv(max_seconds: float, interval_s: float = INTERVAL_S) -> list[str]:
    """-sc bounds the process so that it ends by itself even if the interop launcher is killed without taking the Windows process with it."""
    return ["typeperf.exe", *WIN_COUNTER_PATHS, "-si", str(int(interval_s)), "-sc", str(int(math.ceil(max_seconds / interval_s)) + 2)]


def parse_win_header(line: str) -> tuple[dict[str, list[int]], list[str], list[str]]:
    """typeperf header -> ({column: [cell indexes]}, missing required columns, missing optional columns). The header is the ground truth: typeperf drops counters silently."""
    cells = next(csv.reader([line]))
    mapping: dict[str, list[int]] = {c: [] for c, _, _ in WIN_SPECS}
    for i, cell in enumerate(cells[1:], start=1):
        path = _HOST_PREFIX.sub("", cell)
        for col, rx, _ in WIN_SPECS:
            if re.match(rx, path):
                mapping[col].append(i)
                break
    missing_req = [c for c, _, req in WIN_SPECS if req and not mapping[c]]
    missing_opt = [c for c, _, req in WIN_SPECS if not req and not mapping[c]]
    return mapping, missing_req, missing_opt


def parse_win_row(line: str, mapping: dict[str, list[int]]) -> dict | None:
    """One typeperf data line -> {'t_local': text, 'local_epoch': float, <column>: float|None, 'net_instances': n}; None when it is not a data row."""
    try:
        cells = next(csv.reader([line]))
        local = datetime.datetime.strptime(cells[0], _TS_LOCAL)
    except (StopIteration, IndexError, ValueError, csv.Error):
        return None
    out: dict = {"t_local": cells[0], "local_epoch": (local - EPOCH).total_seconds()}
    for col, idx in mapping.items():
        vals = []
        for i in idx:
            try:
                vals.append(float(cells[i]))
            except (IndexError, ValueError):
                pass
        if col == "net_bytes_s":
            out[col] = sum(vals) if vals else None
            out["net_instances"] = len(vals)
        else:
            out[col] = vals[0] if vals else None
    return out


def infer_offset_min(local_epoch: float, recv_wall_ns: int) -> int:
    """Whole-quarter-hour UTC offset of typeperf's local timestamps: robust to a receipt delay of up to seven minutes."""
    return int(round((local_epoch - recv_wall_ns / 1e9) / 900.0)) * 15


class TypeperfSource:
    """The real Windows source: one typeperf.exe, stdout lines stamped on receipt by a reader thread, drained by the sampling loop."""

    def __init__(self, argv: list[str], *, wall_ns: Callable[[], int] = time.time_ns, mono_ns: Callable[[], int] = time.monotonic_ns,
                 popen=subprocess.Popen):
        self.argv, self.wall_ns, self.mono_ns = argv, wall_ns, mono_ns
        self.q: queue.Queue = queue.Queue()
        self.eof, self.stderr_tail = False, b""
        self.proc = popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
        self._t_out = threading.Thread(target=self._pump_out, daemon=True)
        self._t_err = threading.Thread(target=self._pump_err, daemon=True)
        self._t_out.start()
        self._t_err.start()

    def _pump_out(self) -> None:
        try:
            for raw in iter(self.proc.stdout.readline, b""):
                self.q.put((self.wall_ns(), self.mono_ns(), raw))
        finally:
            self.q.put(None)

    def _pump_err(self) -> None:
        try:
            self.stderr_tail = (self.proc.stderr.read() or b"")[-2000:]
        except (OSError, ValueError):
            pass

    def drain(self) -> list[tuple[int, int, bytes]]:
        out = []
        while True:
            try:
                item = self.q.get_nowait()
            except queue.Empty:
                return out
            if item is None:
                self.eof = True
                continue
            out.append(item)

    @property
    def returncode(self) -> int | None:
        return self.proc.poll()

    def stop(self) -> None:
        if self.proc.poll() is None:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:                                                              # noqa: BLE001 - best effort, -sc ends it anyway
                try:
                    self.proc.kill()
                except Exception:                                                          # noqa: BLE001
                    pass
        self._t_out.join(timeout=2)
        self._t_err.join(timeout=2)


# ---------------------------------------------------------------------------------------------- the sampling loop
def _open_csv(path: Path, cols: list[str]):
    fh = path.open("x", newline="", encoding="utf-8")
    w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n", extrasaction="raise")
    w.writeheader()
    fh.flush()
    return fh, w


def run_sample(out_dir: Path, *, interval_s: float = INTERVAL_S, max_seconds: float = MAX_SECONDS, stop: Callable[[], bool] | None = None,
               proc_root: Path = Path("/proc"), win=None, wall_ns: Callable[[], int] = time.time_ns, mono_ns: Callable[[], int] = time.monotonic_ns,
               sleep: Callable[[float], None] = time.sleep, argv: list[str] | None = None) -> dict:
    """Sample until `stop()` is true or max_seconds have passed. `win` is a source with drain()/stop()/eof/returncode (None = Windows side off)."""
    if interval_s <= 0 or max_seconds <= 0:
        raise ValueError("interval and max-seconds must be positive")
    out_dir.mkdir(parents=True, exist_ok=False)
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    if win is None:
        an.add("win_disabled", "", "the Windows side was not started (--no-windows)")
    wsl_fh, wsl_w = _open_csv(out_dir / "wsl_samples.csv", WSL_COLS)
    win_fh, win_w = _open_csv(out_dir / "win_counters.csv", WIN_COLS)
    raw_fh = (out_dir / "win_raw.tsv").open("x", encoding="utf-8", newline="\n")
    state = {"mapping": None, "offset_min": None, "offset_source": None, "win_rows": 0, "lags_ms": [], "last_t": None, "header": None,
             "missing_required": [], "missing_optional": []}
    start_mono, start_wall = mono_ns(), wall_ns()
    prev, ticks, wsl_rows, stop_reason = None, 0, 0, "error"
    last_win_t: float | None = None
    psi_seen, stat_flagged, early_flagged = False, False, False

    def handle_win_lines(items) -> None:
        nonlocal last_win_t
        for recv_wall, recv_mono, raw in items:
            text = raw.decode("utf-8", "replace").rstrip("\r\n")
            raw_fh.write(f"{recv_wall}\t{text}\n")
            if not text.strip():
                continue
            if text.startswith('"(PDH'):
                if state["mapping"] is None:
                    state["header"] = text
                    state["mapping"], req, opt = parse_win_header(text)
                    state["missing_required"], state["missing_optional"] = req, opt
                    for c in req:
                        an.add("win_required_missing", c, "required counter absent from the typeperf header")
                    for c in opt:
                        an.add("win_optional_missing", c, "optional counter absent from the typeperf header")
                continue
            if state["mapping"] is None or not text.startswith('"'):
                if text.lower().startswith("error"):
                    an.add("win_error_line", "", text[:200])
                continue
            parsed = parse_win_row(text, state["mapping"])
            if parsed is None:
                an.add("win_malformed_line", "", text[:200])
                continue
            if state["offset_min"] is None:
                state["offset_min"] = infer_offset_min(parsed["local_epoch"], recv_wall)
                state["offset_source"] = "inferred from the first row (rounded to a quarter hour)"
            t_wall = int(round((parsed["local_epoch"] - state["offset_min"] * 60.0) * 1e9))
            if last_win_t is not None and t_wall <= last_win_t:
                an.add("time_order", "windows", f"row time {t_wall} not after {last_win_t}")
            if last_win_t is not None and (t_wall - last_win_t) / 1e9 > WIN_ROW_GAP_S:
                an.add("win_gap", "", f"{(t_wall - last_win_t) / 1e9:.2f} s between typeperf rows")
            last_win_t = t_wall if last_win_t is None else max(last_win_t, t_wall)
            lag_ms = (recv_wall - t_wall) / 1e6
            state["lags_ms"].append(lag_ms)
            row = {"row": state["win_rows"], "t_wall_ns": t_wall, "recv_wall_ns": recv_wall, "recv_mono_ns": recv_mono,
                   "lag_ms": round(lag_ms, 1), "t_local": parsed["t_local"], "net_instances": parsed.get("net_instances")}
            for c, _, _ in WIN_SPECS:
                row[c] = parsed.get(c)
                if row[c] is None and state["mapping"][c]:           # a column the header lacks is already reported once, from the header
                    an.add("win_value_missing", c, f"row {state['win_rows']} has no value")
            win_w.writerow({k: ("" if v is None else v) for k, v in row.items()})
            win_fh.flush()
            state["win_rows"] += 1

    try:
        k = 0
        while True:
            now = mono_ns()
            snap = snapshot(proc_root, wall_ns(), now)
            if not snap["stat"] and not stat_flagged:
                stat_flagged = True
                an.add("proc_unreadable", "stat", f"{Path(proc_root) / 'stat'} could not be read or parsed")
            if ticks == 0 and not any(snap["psi"][key] for key in ("psi_cpu", "psi_mem", "psi_io")):
                an.add("psi_unavailable", "", "no /proc/pressure file could be parsed (kernel without PSI)")
            psi_seen = psi_seen or any(snap["psi"][key] for key in ("psi_cpu", "psi_mem", "psi_io"))
            if prev is not None and (snap["mono_ns"] - prev["mono_ns"]) / 1e9 > GAP_FACTOR * interval_s:
                an.add("sampling_gap", "", f"{(snap['mono_ns'] - prev['mono_ns']) / 1e9:.2f} s since the previous tick")
            row, notes = rates(prev, snap, ticks)
            for kind, detail in notes:
                an.add(kind, "", detail)
            wsl_w.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in WSL_COLS})
            wsl_fh.flush()
            wsl_rows += 1
            prev, ticks = snap, ticks + 1
            if win is not None:
                handle_win_lines(win.drain())
                if win.eof and state["win_rows"] == 0 and not early_flagged:
                    early_flagged = True
                    an.add("win_exited_early", "", f"typeperf ended with no data rows (exit {win.returncode})")
            if stop is not None and stop():
                stop_reason = "stop_file"
                break
            if (mono_ns() - start_mono) / 1e9 >= max_seconds:
                stop_reason = "max_seconds"
                break
            k += 1
            due = start_mono + int(k * interval_s * 1e9)
            while True:
                left = (due - mono_ns()) / 1e9
                if left <= 0:
                    break
                sleep(min(left, 0.2))
                if stop is not None and stop():
                    break
            if (mono_ns() - due) / 1e9 > interval_s:                  # fell behind: skip the missed ticks, never burst
                k = int((mono_ns() - start_mono) / (interval_s * 1e9))
    finally:
        if win is not None:
            try:
                win.stop()
                handle_win_lines(win.drain())
            except Exception as exc:                                                       # noqa: BLE001
                an.add("win_error_line", "", f"stopping the Windows source failed: {exc!r}")
        for fh in (wsl_fh, win_fh, raw_fh):
            fh.close()
    end_wall, end_mono = wall_ns(), mono_ns()
    if wsl_rows == 0:
        an.add("no_wsl_samples", "", "no sampling tick completed")
    if win is not None:
        if state["mapping"] is None:
            an.add("win_no_header", "", "typeperf produced no header line" + (f"; stderr: {win.stderr_tail.decode('utf-8', 'replace')[:200]}" if getattr(win, "stderr_tail", b"") else ""))
        if state["win_rows"] == 0:
            an.add("win_no_rows", "", "typeperf produced no data row")
        if state["lags_ms"]:
            med = sorted(state["lags_ms"])[len(state["lags_ms"]) // 2]
            if med < -2000 or med > 5000:
                an.add("win_clock_skew", "", f"median lag between a row's sample time and its receipt is {med:.0f} ms (expected about 1000 ms)")
    outputs = {n: tc.sha256_file(out_dir / n) for n in ("wsl_samples.csv", "win_counters.csv", "win_raw.tsv")}
    outputs["host_anomalies.csv"] = tc.write_csv_x(out_dir / "host_anomalies.csv", tc.ANOMALY_COLS, an.rows())
    lags = sorted(state["lags_ms"])
    manifest = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv,
                "parameters": {"interval_s": interval_s, "max_seconds": max_seconds, "proc_root": str(proc_root),
                               "windows": win is not None, "windows_argv": getattr(win, "argv", None)},
                "stop_reason": stop_reason, "start_wall_ns": start_wall, "end_wall_ns": end_wall, "start_mono_ns": start_mono, "end_mono_ns": end_mono,
                "ticks": ticks, "wsl_rows": wsl_rows, "win_rows": state["win_rows"], "psi_available": psi_seen,
                "windows_header": state["header"], "windows_missing_required": state["missing_required"],
                "windows_missing_optional": state["missing_optional"], "windows_utc_offset_min": state["offset_min"],
                "windows_offset_source": state["offset_source"],
                "windows_lag_ms": {"n": len(lags), "median": lags[len(lags) // 2] if lags else None, "min": lags[0] if lags else None,
                                   "max": lags[-1] if lags else None},
                "windows_exit_code": getattr(win, "returncode", None) if win is not None else None,
                "status": an.status(), "anomalies": an.counts(), "outputs_sha256": outputs}
    tc.write_json_x(out_dir / "host_manifest.json", manifest)
    return manifest


# ---------------------------------------------------------------------------------------------- window evidence (pure; the driver imports these)
def _f(x) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def stream_completeness(times_ms: list[float], lo_ms: float, hi_ms: float, interval_s: float = 1.0) -> dict:
    """How well a 1 Hz stream covers [lo_ms, hi_ms]: sample count against the expected count, and the longest hole including the two edges."""
    expected = int(math.floor((hi_ms - lo_ms) / 1000.0 / interval_s))
    ts = sorted(t for t in times_ms if lo_ms <= t <= hi_ms)
    if not ts:
        return {"count": 0, "expected": expected, "ratio": 0.0, "max_gap_s": (hi_ms - lo_ms) / 1000.0, "lead_s": None, "tail_s": None}
    gaps = [(b - a) / 1000.0 for a, b in zip(ts, ts[1:])]
    lead, tail = (ts[0] - lo_ms) / 1000.0, (hi_ms - ts[-1]) / 1000.0
    return {"count": len(ts), "expected": expected, "ratio": (len(ts) / expected) if expected else 0.0,
            "max_gap_s": max(gaps + [lead, tail]), "lead_s": round(lead, 3), "tail_s": round(tail, 3)}


def _stats(vals: list[float]) -> dict:
    if not vals:
        return {"n": 0}
    s = sorted(vals)
    return {"n": len(s), "mean": sum(s) / len(s), "p95": s[max(0, math.ceil(0.95 * len(s)) - 1)], "min": s[0], "max": s[-1]}


def summarize_window(wsl_rows: list[dict], win_rows: list[dict], lo_ms: float, hi_ms: float) -> dict:
    """Descriptive statistics of the host record inside [lo_ms, hi_ms]. Nothing here is compared with a threshold."""
    def col(rows, key, time_key, scale=1.0):
        out = []
        for r in rows:
            t, v = _f(r.get(time_key)), _f(r.get(key))
            if t is not None and v is not None and lo_ms <= t / 1e6 <= hi_ms:
                out.append(v * scale)
        return out
    wsl = {c: _stats(col(wsl_rows, c, "wall_ns")) for c in WSL_COLS if c not in ("tick", "wall_ns", "mono_ns", "dt_s", "cpus")}
    win = {c: _stats(col(win_rows, c, "t_wall_ns")) for c, _, _ in WIN_SPECS if c not in ("disk_bytes_s", "net_bytes_s")}
    win["disk_mb_s"] = _stats(col(win_rows, "disk_bytes_s", "t_wall_ns", 1e-6))
    win["net_mb_s"] = _stats(col(win_rows, "net_bytes_s", "t_wall_ns", 1e-6))
    return {"window_ms": [lo_ms, hi_ms], "wsl": wsl, "windows": win}


def read_rows(path: Path) -> list[dict]:
    with Path(path).open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------------------------- CLI
def main(argv: list[str] | None = None, win_factory: Callable[[list[str]], object] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").strip().splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--out-dir", required=True, type=Path)
    s.add_argument("--interval", type=float, default=INTERVAL_S)
    s.add_argument("--max-seconds", type=float, default=MAX_SECONDS)
    s.add_argument("--stop-file", type=Path)
    s.add_argument("--proc-root", type=Path, default=Path("/proc"))
    s.add_argument("--no-windows", action="store_true", help="skip the typeperf side (the record then lacks the Windows counters and the driver gate fails)")
    s.add_argument("--execute", action="store_true", help="read the kernel files and start typeperf.exe (without it: print the plan, write nothing)")
    m = sub.add_parser("summarize")
    m.add_argument("--samples", required=True, type=Path)
    m.add_argument("--win", required=True, type=Path)
    m.add_argument("--from-ms", required=True, type=float)
    m.add_argument("--to-ms", required=True, type=float)
    args = ap.parse_args(argv)
    cmdline = sys.argv if argv is None else argv
    try:
        if args.cmd == "summarize":
            print(json.dumps(summarize_window(read_rows(args.samples), read_rows(args.win), args.from_ms, args.to_ms), indent=2))
            return 0
        if args.out_dir.exists():
            print(f"refusing to overwrite {args.out_dir}")
            return 2
        if args.interval < 1 or int(args.interval) != args.interval:
            print("the interval must be a whole number of seconds >= 1 (typeperf -si takes whole seconds)")
            return 2
        tp = typeperf_argv(args.max_seconds, args.interval)
        if not args.execute:
            for f in ("stat", "loadavg", "meminfo", "diskstats", "net/dev", "pressure/cpu", "pressure/memory", "pressure/io"):
                print(f"PLAN read {args.proc_root / f} every {args.interval:g} s")
            print("PLAN " + (" ".join(tp) if not args.no_windows else "no Windows side (--no-windows)"))
            print(f"PLAN write {args.out_dir}/{{wsl_samples.csv,win_counters.csv,win_raw.tsv,host_anomalies.csv,host_manifest.json}} (never overwrites)")
            print("dry run: nothing executed or written (add --execute)")
            return 0
        stop = (lambda: args.stop_file.exists()) if args.stop_file else None
        win = None if args.no_windows else (win_factory or TypeperfSource)(tp)
        man = run_sample(args.out_dir, interval_s=args.interval, max_seconds=args.max_seconds, stop=stop, proc_root=args.proc_root, win=win, argv=cmdline)
    except (OSError, ValueError) as err:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {err!r}")
        return 2
    for sev in ("error", "warning"):
        for k, n in sorted(man["anomalies"][sev].items()):
            print(f"  {sev:<7} {k:<24} {n}")
    print(f"HOST {man['status']} ticks={man['ticks']} win_rows={man['win_rows']} stop={man['stop_reason']} -> {args.out_dir}")
    return tc.EXIT[man["status"]]


if __name__ == "__main__":
    sys.exit(main())
