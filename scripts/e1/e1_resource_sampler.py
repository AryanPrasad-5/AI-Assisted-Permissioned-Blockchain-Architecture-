"""Resource sampler (LA-3; spec file 03 section 7; E1 v0.2 protocol section 12.7; gate V6 inputs).

Status: 0.1 written 2026-11-09 and run once in E0-0 check 8 (--source cli: `docker stats --no-stream`
took about 2 s per call, so a 1 s cadence was impossible: 5 interval_overrun warnings). 0.2 (E0-0
remediation F3b, 2026-11-09) adds --source api, the default: one Docker Engine API stats stream per
container. Measured on this machine (engine 29.9.3, API 1.56): one JSON document per container every
1.003 s, with daemon timestamps. Its own overhead is measured in E0-4 (sampler ON/OFF), not here.

sample --source api (default)
          GET /containers/<name>/stats?stream=1 on the Docker socket (--docker-sock), one connection
          and thread per listed container, for --duration s, until --stop-file exists, or until every
          container has --max-ticks samples. The daemon publishes every 1 s (fixed in dockerd), so
          --interval must be 1.0. Every received line is kept byte-exact in raw/stats_<container>.jsonl
          and stamped on receipt with the host wall and monotonic clocks. A failed or ended stream is
          recorded and reopened until the stop (the gap is then detected). `docker inspect` runs once at
          the start and once at the end, as in --source cli.
sample --source cli
          every --interval s: `docker stats --no-stream --no-trunc --format "{{json .}}"` (all running
          containers; the --container list is kept, others are counted and ignored), plus
          `docker inspect <containers>` once at the start and once at the end (restart count,
          StartedAt, OOMKilled, status). Prints the plan; runs only with --execute.
typeperf  parse a Windows `typeperf <counters> -si 1 -f CSV -o FILE` log (host CPU, % processor
          performance as a throttling proxy, available memory) into a long table with UTC times.
          The UTC offset comes from the PDH header "(<zone>)(<bias>)" (UTC = local + bias minutes)
          or --utc-offset-min. WSL VM memory is NOT sampled by this tool (documented gap).

Outputs (sample --source api; new --out-dir)
  raw/stats_<container>.jsonl, raw/inspect_start.json, raw/inspect_end.json   exactly as received
  resource_receipts.csv  one row per received line: container, raw line number, stream attempt, host wall
                         and monotonic receipt times, bytes, SHA-256
  resource_samples.csv   one row per parsed sample (STREAM_SAMPLE_COLS): daemon read/preread times,
                         receipt times, receipt delay, receipt gap, exact daemon counters (CPU ns, memory,
                         network, block I/O bytes, PIDs) and CPU % computed as the docker CLI does
                         ((delta container CPU / delta system CPU) x online CPUs x 100)
  resource_anomalies.csv, resource_manifest.json
Outputs (sample --source cli; new --out-dir)
  raw/tick_<k:06d>.stdout|.stderr, raw/inspect_start.json, raw/inspect_end.json   exactly as received
  resource_ticks.csv     one row per tick: host wall/monotonic times, exit code, lines, SHA-256
  resource_samples.csv   one row per (tick, container): parsed CPU %, memory used/limit (bytes),
                         memory %, net rx/tx, block read/write (bytes), PIDs, plus the raw strings
  resource_anomalies.csv, resource_manifest.json (per-container coverage and peaks, snapshots)
Outputs (typeperf): host_counters.csv, host_counters_anomalies.csv, host_counters_manifest.json

Precision: docker stats prints rounded human units (e.g. "105.3MiB", "1.2MB"); bytes are those
values times the unit, so they carry the CLI's rounding. Binary units (KiB, MiB, GiB) and decimal
units (kB, MB, GB) are kept apart.

Checks
  completeness  stats_failed, malformed_line, container_missing (a listed container absent from a
                successful tick; api: no sample at all), value_unavailable ("--"; api: a value absent
                from a document, CPU % excepted on the first document of a stream), inspect_failed,
                no_successful_tick, value_missing (typeperf blank), timezone_unknown (typeperf),
                stream_failed (api: the stream could not be opened, or ended or failed before the stop),
                sample_gap (api: the daemon interval between consecutive samples, i.e. their daemon read
                times' difference minus the wall-clock step seen between their receipts, exceeds
                --gap-factor x interval; or more than that between the start or stop and the first or
                last receipt on the monotonic clock: at least one 1 s sample is missing)
  consistency   duplicate_container (same name twice in one tick), container_replaced (ID changed
                between ticks), container_restarted (StartedAt changed or RestartCount rose between
                the start and end inspections), oom_killed, container_not_running (end inspection),
                host_clock_step, duplicate_timestamp and timestamp_order (typeperf; api: a daemon read
                time repeated or decreased), container_mismatch (api: document for another container)
  warnings      interval_overrun, sample_delayed (api: received more than --max-delay s after the daemon
                read it; the daemon read time is the sample time, so the value stays usable)
Daemon read times are CLOCK_REALTIME and include any clock step (E0-0 remediation F1), so the gap test
removes the step measured between the two receipts (wall minus monotonic, both read by the receiving
thread); a late delivery is sample_delayed, not a gap. host_clock_step uses the same receipt stamps with
the 2 ms rule of 0.1.
Standard library only.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import http.client
import io
import json
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Callable, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_telemetry_common as tc  # noqa: E402

TOOL_VERSION = "e1_resource_sampler 0.2 (2026-11-09, E0-0 remediation F3b)"
STATS_ARGV = ["docker", "stats", "--no-stream", "--no-trunc", "--format", "{{json .}}"]
DOCKER_SOCK = "/var/run/docker.sock"
API_STATS_PATH = "/containers/{name}/stats?stream=1"
DAEMON_INTERVAL_S = 1.0   # dockerd's stats collector period; measured 1.003 s on this machine (F3b)
COMPLETENESS = {"stats_failed", "malformed_line", "container_missing", "value_unavailable", "inspect_failed",
                "no_successful_tick", "value_missing", "timezone_unknown", "stream_failed", "sample_gap"}
CONSISTENCY = {"duplicate_container", "container_replaced", "container_restarted", "oom_killed",
               "container_not_running", "host_clock_step", "duplicate_timestamp", "timestamp_order",
               "container_mismatch"}
WARNINGS = {"interval_overrun", "sample_delayed"}
HOST_STEP_NS = 2_000_000
UNITS = {"B": 1, "kB": 10**3, "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12, "PB": 10**15,
         "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40, "PiB": 2**50}
_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?|\.\d+)\s*([A-Za-z]+)\s*$")
_PCT = re.compile(r"^\s*(\d+(?:\.\d+)?)%\s*$")
_CONTAINER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
STATS_FIELDS = ("Name", "ID", "CPUPerc", "MemUsage", "MemPerc", "NetIO", "BlockIO", "PIDs")
TICK_COLS = ["tick", "t0_wall_ns", "t1_wall_ns", "t0_utc", "t_mid_wall_ns", "t0_mono_ns", "t1_mono_ns", "duration_ms",
             "exit_code", "lines", "stdout_sha256", "stderr_sha256", "kept_rows", "ignored_rows", "error"]
SAMPLE_COLS = ["tick", "t_mid_wall_ns", "container", "id", "cpu_percent", "mem_used_bytes", "mem_limit_bytes",
               "mem_percent", "net_rx_bytes", "net_tx_bytes", "blk_read_bytes", "blk_write_bytes", "pids",
               "raw_cpu", "raw_mem", "raw_net", "raw_block", "line_no"]
HOST_COLS = ["row", "t_local", "t_utc_ms", "counter", "value", "value_raw"]


# ------------------------------------------------------------------ value parsing
def size_bytes(text: str) -> float | None:
    """'105.3MiB' -> bytes; '--' -> None; ValueError otherwise."""
    if text.strip() == "--":
        return None
    m = _SIZE.match(text)
    if not m or m.group(2) not in UNITS:
        raise ValueError(f"size {text!r}")
    return float(m.group(1)) * UNITS[m.group(2)]


def size_pair(text: str) -> tuple[float | None, float | None]:
    if text.strip() == "--":
        return None, None
    parts = text.split(" / ")
    if len(parts) != 2:
        raise ValueError(f"pair {text!r}")
    return size_bytes(parts[0]), size_bytes(parts[1])


def percent(text: str) -> float | None:
    if text.strip() == "--":
        return None
    m = _PCT.match(text)
    if not m:
        raise ValueError(f"percent {text!r}")
    return float(m.group(1))


def pids(text: str) -> int | None:
    if text.strip() == "--":
        return None
    if not text.strip().isdigit():
        raise ValueError(f"PIDs {text!r}")
    return int(text)


def parse_stats_line(line: str) -> dict:
    """One `docker stats --format '{{json .}}'` line -> parsed row. ValueError when malformed."""
    try:
        d = json.loads(line)
    except json.JSONDecodeError as err:
        raise ValueError(f"not JSON: {err}") from err
    if not isinstance(d, dict) or any(not isinstance(d.get(f), str) for f in STATS_FIELDS):
        raise ValueError(f"missing or non-string fields; want {list(STATS_FIELDS)}")
    used, limit = size_pair(d["MemUsage"])
    rx, txb = size_pair(d["NetIO"])
    rd, wr = size_pair(d["BlockIO"])
    row = {"container": d["Name"], "id": d["ID"], "cpu_percent": percent(d["CPUPerc"]), "mem_used_bytes": used,
           "mem_limit_bytes": limit, "mem_percent": percent(d["MemPerc"]), "net_rx_bytes": rx, "net_tx_bytes": txb,
           "blk_read_bytes": rd, "blk_write_bytes": wr, "pids": pids(d["PIDs"]), "raw_cpu": d["CPUPerc"],
           "raw_mem": d["MemUsage"], "raw_net": d["NetIO"], "raw_block": d["BlockIO"]}
    row["unavailable"] = [k for k in ("cpu_percent", "mem_used_bytes", "mem_limit_bytes", "mem_percent", "net_rx_bytes",
                                      "net_tx_bytes", "blk_read_bytes", "blk_write_bytes", "pids") if row[k] is None]
    return row


def parse_inspect(stdout: bytes) -> dict[str, dict]:
    """`docker inspect` JSON array -> {name: {id, status, running, oom_killed, started_at, restart_count}}."""
    docs = json.loads(stdout.decode("utf-8"))
    if not isinstance(docs, list):
        raise ValueError("docker inspect output is not a JSON array")
    out = {}
    for d in docs:
        st = d["State"]
        out[d["Name"].lstrip("/")] = {"id": d["Id"], "status": st["Status"], "running": st["Running"],
                                      "oom_killed": st["OOMKilled"], "started_at": st["StartedAt"],
                                      "restart_count": d["RestartCount"]}
    return out


# ------------------------------------------------------------------ docker inspect (both sources)
def _call(runner: tc.Runner, cmd: list[str]):
    try:
        return runner(cmd)
    except (OSError, subprocess.SubprocessError) as exc:
        return None, b"", repr(exc).encode()


def inspect_snapshot(runner: tc.Runner, containers: list[str], out_dir: Path, an: tc.Anomalies,
                     label: str) -> dict | None:
    code, out, err = _call(runner, ["docker", "inspect"] + containers)
    tc.write_bytes_x(out_dir / "raw" / f"inspect_{label}.json", out)
    tc.write_bytes_x(out_dir / "raw" / f"inspect_{label}.stderr", err)
    try:
        snap = parse_inspect(out) if code == 0 else None
    except (ValueError, KeyError, TypeError, UnicodeDecodeError) as exc:
        an.add("inspect_failed", label, f"unparseable: {exc!r}")
        return None
    if snap is None:
        an.add("inspect_failed", label, f"exit {code}: {err.decode('utf-8', 'replace').strip()[-200:]}")
        return None
    for c in sorted(set(containers) - set(snap)):
        an.add("inspect_failed", c, f"{label}: container not in docker inspect output")
    return snap


def compare_snapshots(start: dict | None, end: dict | None, containers: list[str], an: tc.Anomalies) -> None:
    if start is None or end is None:
        return
    for c in containers:
        a, b = start.get(c), end.get(c)
        if a is None or b is None:
            continue
        if a["id"] != b["id"]:
            an.add("container_replaced", c, f"inspect id {a['id'][:12]} -> {b['id'][:12]}")
        if a["started_at"] != b["started_at"] or b["restart_count"] > a["restart_count"]:
            an.add("container_restarted", c, f"StartedAt {a['started_at']} -> {b['started_at']}; "
                                             f"RestartCount {a['restart_count']} -> {b['restart_count']}")
        if b["oom_killed"]:
            an.add("oom_killed", c, "State.OOMKilled is true at the end")
        if not b["running"]:
            an.add("container_not_running", c, f"status {b['status']!r} at the end")


# ------------------------------------------------------------------ sampling (--source cli)
def run_sample(containers: list[str], out_dir: Path, runner: tc.Runner, *, wall_ns: Callable[[], int] = time.time_ns,
               mono_ns: Callable[[], int] = time.monotonic_ns, sleep: Callable[[float], None] = time.sleep,
               interval_s: float = 1.0, duration_s: float | None = None, max_ticks: int | None = None,
               stop: Callable[[], bool] | None = None, argv: list[str] | None = None) -> dict:
    if not containers or len(set(containers)) != len(containers) or not all(_CONTAINER.match(c) for c in containers):
        raise ValueError("need distinct valid container names")
    if duration_s is None and max_ticks is None and stop is None:
        raise ValueError("need --duration, --max-ticks or --stop-file")
    if interval_s <= 0:
        raise ValueError("interval must be > 0")
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "raw").mkdir()
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    want = set(containers)

    def call(cmd):
        return _call(runner, cmd)

    def inspect(label: str) -> dict | None:
        return inspect_snapshot(runner, containers, out_dir, an, label)

    start = inspect("start")
    ticks, samples = [], []
    ids: dict[str, str] = {}
    present = {c: 0 for c in containers}
    ignored_names: set[str] = set()
    ok_ticks, missed, k, done = 0, 0, 0, 0
    start_w, start_m = wall_ns(), mono_ns()
    while True:
        if (max_ticks is not None and done >= max_ticks) or (stop is not None and stop()) \
                or (duration_s is not None and mono_ns() - start_m >= duration_s * 1e9):
            break
        t0w, t0m = wall_ns(), mono_ns()
        code, out, err = call(STATS_ARGV)
        t1w, t1m = wall_ns(), mono_ns()
        mid = (t0w + t1w) // 2
        src = f"tick {k}"
        tc.write_bytes_x(out_dir / "raw" / f"tick_{k:06d}.stdout", out)
        tc.write_bytes_x(out_dir / "raw" / f"tick_{k:06d}.stderr", err)
        lines = out.decode("utf-8", "replace").splitlines()
        row = {"tick": k, "t0_wall_ns": t0w, "t1_wall_ns": t1w, "t0_utc": tc.utc_iso(t0w), "t_mid_wall_ns": mid,
               "t0_mono_ns": t0m, "t1_mono_ns": t1m, "duration_ms": round((t1m - t0m) / 1e6, 3), "exit_code": code,
               "lines": len(lines), "stdout_sha256": tc.sha256_bytes(out), "stderr_sha256": tc.sha256_bytes(err),
               "kept_rows": 0, "ignored_rows": 0, "error": ""}
        if abs((t1w - t0w) - (t1m - t0m)) > HOST_STEP_NS:
            an.add("host_clock_step", "", f"wall {t1w - t0w} ns vs monotonic {t1m - t0m} ns", src)
        if code != 0:
            row["error"] = err.decode("utf-8", "replace").strip()[-200:]
            an.add("stats_failed", "", f"exit {code}: {row['error']}", src)
        else:
            ok_ticks += 1
            seen: dict[str, int] = {}
            for no, line in enumerate(lines, start=1):
                if not line.strip():
                    continue
                try:
                    p = parse_stats_line(line)
                except ValueError as err_:
                    an.add("malformed_line", "", str(err_), f"raw/tick_{k:06d}.stdout:{no}", line)
                    continue
                name = p["container"]
                if name not in want:
                    row["ignored_rows"] += 1
                    ignored_names.add(name)
                    continue
                if name in seen:
                    an.add("duplicate_container", name, f"also on line {seen[name]}", f"raw/tick_{k:06d}.stdout:{no}",
                           line)
                    continue
                seen[name] = no
                if name in ids and ids[name] != p["id"]:
                    an.add("container_replaced", name, f"id {ids[name][:12]} -> {p['id'][:12]}", src)
                ids[name] = p["id"]
                for field in p.pop("unavailable"):
                    an.add("value_unavailable", name, f"{field} is '--'", f"raw/tick_{k:06d}.stdout:{no}", line)
                samples.append({"tick": k, "t_mid_wall_ns": mid, "line_no": no, **p})
                present[name] += 1
                row["kept_rows"] += 1
            for c in sorted(want - set(seen)):
                an.add("container_missing", c, "not in docker stats output", src)
        ticks.append(row)
        done += 1
        due = start_m + int((k + 1) * interval_s * 1e9)
        now = mono_ns()
        if now > due:
            behind = int((now - due) // (interval_s * 1e9)) + 1
            an.add("interval_overrun", "", f"tick {k} ended {(now - due) / 1e6:.3f} ms late; {behind - 1} slot(s) "
                                           "skipped, not back-filled", src)
            missed += behind - 1
            k += behind
        else:
            sleep((due - now) / 1e9)
            k += 1
    end_w, end_m = wall_ns(), mono_ns()
    if abs((end_w - start_w) - (end_m - start_m)) > HOST_STEP_NS:
        an.add("host_clock_step", "", f"over the sampling: wall {end_w - start_w} ns vs monotonic {end_m - start_m} ns")
    if ok_ticks == 0:
        an.add("no_successful_tick", "", f"{done} tick(s), none succeeded")
    end = inspect("end")
    compare_snapshots(start, end, containers, an)
    summary = {}
    for c in containers:
        rows = [s for s in samples if s["container"] == c]
        cpu = [s["cpu_percent"] for s in rows if s["cpu_percent"] is not None]
        mem = [s["mem_used_bytes"] for s in rows if s["mem_used_bytes"] is not None]
        summary[c] = {"ticks_present": present[c], "ticks_successful": ok_ticks,
                      "coverage": round(present[c] / ok_ticks, 6) if ok_ticks else 0.0,
                      "cpu_percent_max": max(cpu) if cpu else None,
                      "cpu_percent_mean": round(sum(cpu) / len(cpu), 6) if cpu else None,
                      "mem_used_bytes_max": max(mem) if mem else None,
                      "mem_limit_bytes": next((s["mem_limit_bytes"] for s in rows if s["mem_limit_bytes"]), None)}
    outputs = {"resource_ticks.csv": tc.write_csv_x(out_dir / "resource_ticks.csv", TICK_COLS, ticks),
               "resource_samples.csv": tc.write_csv_x(out_dir / "resource_samples.csv", SAMPLE_COLS, samples),
               "resource_anomalies.csv": tc.write_csv_x(out_dir / "resource_anomalies.csv", tc.ANOMALY_COLS,
                                                        an.rows())}
    manifest = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "containers": containers,
                "stats_argv": STATS_ARGV, "parameters": {"interval_s": interval_s, "duration_s": duration_s,
                                                         "max_ticks": max_ticks},
                "start_utc": tc.utc_iso(start_w), "end_utc": tc.utc_iso(end_w), "ticks": done,
                "successful_ticks": ok_ticks, "missed_tick_slots": missed,
                "ignored_containers": sorted(ignored_names),
                "ignored_note": "rows of containers not in the list are counted per tick and kept in raw/",
                "snapshots": {"start": start, "end": end}, "per_container": summary,
                "precision_note": "docker stats values are the CLI's rounded human units converted to bytes",
                "status": an.status(), "anomalies": an.counts(), "outputs_sha256": outputs}
    tc.write_json_x(out_dir / "resource_manifest.json", manifest)
    return manifest


# ------------------------------------------------------------------ sampling (--source api, default)
_RFC3339 = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$")
STREAM_SAMPLE_COLS = ["container", "raw_line", "attempt", "id", "read_utc", "read_ns", "preread_utc", "recv_wall_ns",
                      "recv_mono_ns", "delay_ms", "recv_gap_ms", "read_gap_ms", "daemon_gap_ms", "cpu_percent",
                      "cpu_total_ns",
                      "system_cpu_ns", "online_cpus", "mem_used_bytes", "mem_usage_bytes", "mem_inactive_file_bytes",
                      "mem_limit_bytes", "mem_percent", "net_rx_bytes", "net_tx_bytes", "blk_read_bytes",
                      "blk_write_bytes", "pids"]
RECEIPT_COLS = ["container", "raw_line", "attempt", "recv_wall_ns", "recv_mono_ns", "bytes", "sha256"]
_STREAM_VALUES = ("mem_used_bytes", "mem_limit_bytes", "mem_percent", "net_rx_bytes", "net_tx_bytes",
                  "blk_read_bytes", "blk_write_bytes", "pids")
# opener(container) -> (iterable of raw lines, close function)
StreamOpener = Callable[[str], "tuple[Iterable[bytes], Callable[[], None]]"]


def rfc3339_ns(text: str) -> int | None:
    """Docker RFC 3339 UTC time with up to 9 fraction digits -> ns since the epoch; the zero time -> None."""
    m = _RFC3339.match(text or "")
    if not m:
        raise ValueError(f"timestamp {text!r}")
    if m.group(1).startswith("0001-01-01"):
        return None
    dt = datetime.datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp()) * 1_000_000_000 + int((m.group(2) or "0").ljust(9, "0"))


def parse_api_stats(doc) -> dict:
    """One Engine API stats document -> exact counters. ValueError when a required field is missing."""
    if not isinstance(doc, dict):
        raise ValueError("not a JSON object")
    try:
        read_ns, preread_ns = rfc3339_ns(doc["read"]), rfc3339_ns(doc["preread"])
        cpu, pre, mem = doc["cpu_stats"], doc.get("precpu_stats") or {}, doc["memory_stats"]
        total = cpu["cpu_usage"]["total_usage"]
        name, cid = doc["name"].lstrip("/"), doc["id"]
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"missing or malformed field: {exc!r}") from exc
    if read_ns is None:
        raise ValueError("read is the zero time")
    sys_now = cpu.get("system_cpu_usage")
    online = cpu.get("online_cpus") or len(cpu["cpu_usage"].get("percpu_usage") or []) or None
    pre_total, pre_sys = (pre.get("cpu_usage") or {}).get("total_usage"), pre.get("system_cpu_usage")
    cpu_pct = None
    if None not in (sys_now, pre_total, pre_sys, online) and sys_now > pre_sys:
        cpu_pct = (total - pre_total) / (sys_now - pre_sys) * online * 100.0   # docker CLI's formula
    usage, limit, st = mem.get("usage"), mem.get("limit"), mem.get("stats") or {}
    inactive = st.get("inactive_file", st.get("total_inactive_file"))
    used = usage - inactive if usage is not None and inactive is not None and inactive < usage else usage
    nets = doc.get("networks")
    blk = (doc.get("blkio_stats") or {}).get("io_service_bytes_recursive")

    def blk_sum(op):
        return None if blk is None else sum(e.get("value", 0) for e in blk if str(e.get("op", "")).lower() == op)
    return {"container": name, "id": cid, "read_ns": read_ns, "preread_ns": preread_ns, "cpu_percent": cpu_pct,
            "cpu_total_ns": total, "system_cpu_ns": sys_now, "online_cpus": online, "mem_used_bytes": used,
            "mem_usage_bytes": usage, "mem_inactive_file_bytes": inactive, "mem_limit_bytes": limit,
            "mem_percent": used / limit * 100.0 if used is not None and limit else None,
            "net_rx_bytes": sum(v.get("rx_bytes", 0) for v in nets.values()) if nets else None,
            "net_tx_bytes": sum(v.get("tx_bytes", 0) for v in nets.values()) if nets else None,
            "blk_read_bytes": blk_sum("read"), "blk_write_bytes": blk_sum("write"),
            "pids": (doc.get("pids_stats") or {}).get("current")}


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, sock_path: str, timeout: float):
        super().__init__("localhost", timeout=timeout)
        self.sock_path = sock_path

    def connect(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect(self.sock_path)
        self.sock = s


def docker_api_opener(sock_path: str = DOCKER_SOCK, timeout_s: float = 10.0) -> StreamOpener:
    """Real opener: one Engine API stats stream on the Docker socket (read only)."""
    def open_stream(container: str):
        conn = _UnixHTTPConnection(sock_path, timeout_s)
        conn.request("GET", API_STATS_PATH.format(name=urllib.parse.quote(container, safe="")))
        resp = conn.getresponse()
        if resp.status != 200:
            body = resp.read(500)
            conn.close()
            raise OSError(f"HTTP {resp.status}: {body.decode('utf-8', 'replace').strip()}")

        def lines():
            while True:
                line = resp.readline()
                if not line:
                    return
                yield line

        def close():
            try:
                if conn.sock is not None:
                    conn.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()
        return lines(), close
    return open_stream


def collect_streams(containers: list[str], opener: StreamOpener, raw_dir: Path, stop: threading.Event, *,
                    wall_ns: Callable[[], int] = time.time_ns, mono_ns: Callable[[], int] = time.monotonic_ns,
                    retry_s: float = 1.0) -> tuple[list[threading.Thread], list[dict], dict, threading.Lock]:
    """Starts one reader thread per container. Each received line is written byte-exact to
    raw/stats_<container>.jsonl and recorded with its receipt stamps; a failed or ended stream is recorded
    as an error event and reopened until `stop` is set. Returns (threads, records, closers, lock)."""
    records: list[dict] = []
    closers: dict[str, Callable[[], None]] = {}
    lock = threading.Lock()

    def worker(c: str):
        with (raw_dir / f"stats_{c}.jsonl").open("xb") as raw:
            n_line, attempt = 0, 0
            while not stop.is_set():
                attempt += 1
                try:
                    it, close = opener(c)
                except Exception as exc:  # recorded as stream_failed; retried until the stop
                    with lock:
                        records.append({"container": c, "event": "error", "attempt": attempt, "wall_ns": wall_ns(),
                                        "mono_ns": mono_ns(), "detail": f"open failed: {exc!r}"})
                    stop.wait(retry_s)
                    continue
                with lock:
                    closers[c] = close
                detail = "stream ended"
                try:
                    for line in it:
                        w, m = wall_ns(), mono_ns()
                        body = line.rstrip(b"\r\n")
                        raw.write(body + b"\n")
                        raw.flush()
                        n_line += 1
                        with lock:
                            records.append({"container": c, "event": "line", "attempt": attempt, "wall_ns": w,
                                            "mono_ns": m, "line": body, "raw_line": n_line})
                        if stop.is_set():
                            break
                except Exception as exc:
                    detail = f"read failed: {exc!r}"
                finally:
                    close()
                if not stop.is_set():
                    with lock:
                        records.append({"container": c, "event": "error", "attempt": attempt, "wall_ns": wall_ns(),
                                        "mono_ns": mono_ns(), "detail": detail})
                    stop.wait(retry_s)

    threads = [threading.Thread(target=worker, args=(c,), name=f"stats-{c}", daemon=True) for c in containers]
    for t in threads:
        t.start()
    return threads, records, closers, lock


def analyze_stream(records: list[dict], containers: list[str], an: tc.Anomalies, *, start_mono_ns: int,
                   stop_mono_ns: int, interval_s: float = DAEMON_INTERVAL_S, gap_factor: float = 1.5,
                   max_delay_s: float = 0.5) -> tuple[list[dict], dict]:
    """Pure: parsed samples and per-container coverage from receipt records; anomalies go to `an`."""
    gap_ns, delay_ns = int(gap_factor * interval_s * 1e9), int(max_delay_s * 1e9)
    window_s = (stop_mono_ns - start_mono_ns) / 1e9
    samples, per = [], {}
    for c in containers:
        recs = [r for r in records if r["container"] == c]
        for e in (r for r in recs if r["event"] == "error"):
            an.add("stream_failed", c, e["detail"], f"attempt {e['attempt']}")
        rows: list[dict] = []
        cid, seen, prev = None, set(), None
        for r in (r for r in recs if r["event"] == "line"):
            src = f"raw/stats_{c}.jsonl:{r['raw_line']}"
            text = r["line"].decode("utf-8", "replace")
            try:
                p = parse_api_stats(json.loads(text))
            except (ValueError, json.JSONDecodeError) as exc:
                an.add("malformed_line", c, str(exc)[:200], src, text[:300])
                continue
            if p["container"] != c:
                an.add("container_mismatch", c, f"document for {p['container']!r}", src)
                continue
            if p["read_ns"] in seen:
                an.add("duplicate_timestamp", c, f"daemon read {tc.utc_iso(p['read_ns'])} repeated", src)
                continue
            seen.add(p["read_ns"])
            if cid is not None and p["id"] != cid:
                an.add("container_replaced", c, f"id {cid[:12]} -> {p['id'][:12]}", src)
            cid = p["id"]
            new_stream = prev is None or prev["attempt"] != r["attempt"]
            row = {"container": c, "raw_line": r["raw_line"], "attempt": r["attempt"], "id": p["id"],
                   "read_utc": tc.utc_iso(p["read_ns"]), "read_ns": p["read_ns"],
                   "preread_utc": tc.utc_iso(p["preread_ns"]), "recv_wall_ns": r["wall_ns"],
                   "recv_mono_ns": r["mono_ns"], "delay_ms": round((r["wall_ns"] - p["read_ns"]) / 1e6, 3),
                   **{k: p[k] for k in ("cpu_percent", "cpu_total_ns", "system_cpu_ns", "online_cpus",
                                        "mem_used_bytes", "mem_usage_bytes", "mem_inactive_file_bytes",
                                        "mem_limit_bytes", "mem_percent", "net_rx_bytes", "net_tx_bytes",
                                        "blk_read_bytes", "blk_write_bytes", "pids")}}
            if r["wall_ns"] - p["read_ns"] > delay_ns:
                an.add("sample_delayed", c, f"received {row['delay_ms']} ms after the daemon read", src)
            if prev is not None:
                gap = r["mono_ns"] - prev["recv_mono_ns"]
                step = (r["wall_ns"] - prev["recv_wall_ns"]) - gap          # wall-clock step between receipts
                # the daemon's interval on the monotonic scale: daemon read times (CLOCK_REALTIME) minus the step;
                # a late delivery changes the receipt gap but not this
                dgap = (p["read_ns"] - prev["read_ns"]) - step
                row["recv_gap_ms"] = round(gap / 1e6, 3)
                row["read_gap_ms"] = round((p["read_ns"] - prev["read_ns"]) / 1e6, 3)
                row["daemon_gap_ms"] = round(dgap / 1e6, 3)
                if p["read_ns"] < prev["read_ns"]:
                    an.add("timestamp_order", c, f"daemon read {row['read_utc']} before {prev['read_utc']}", src)
                if dgap > gap_ns:
                    an.add("sample_gap", c, f"{dgap / 1e6:.1f} ms between daemon reads (step-corrected; > "
                                            f"{gap_factor} x {interval_s} s): about "
                                            f"{max(1, round(dgap / (interval_s * 1e9)) - 1)} sample(s) missing", src)
                if abs(step) > HOST_STEP_NS:
                    an.add("host_clock_step", c, f"wall advanced {step / 1e6:+.3f} ms more than monotonic between "
                                                 "receipts", src)
            missing = [k for k in _STREAM_VALUES if p[k] is None]
            if p["cpu_percent"] is None and not new_stream:
                missing.insert(0, "cpu_percent")
            for k in missing:
                an.add("value_unavailable", c, f"{k} absent", src)
            rows.append(row)
            prev = row
        if not rows:
            an.add("container_missing", c, "no stats sample received")
        else:
            head, tail = rows[0]["recv_mono_ns"] - start_mono_ns, stop_mono_ns - rows[-1]["recv_mono_ns"]
            if head > gap_ns:
                an.add("sample_gap", c, f"first sample {head / 1e6:.1f} ms after the start")
            if tail > gap_ns:
                an.add("sample_gap", c, f"last sample {tail / 1e6:.1f} ms before the stop")
        gaps = [x["recv_gap_ms"] for x in rows if x.get("recv_gap_ms") is not None]
        delays = sorted(x["delay_ms"] for x in rows)
        cpu = [x["cpu_percent"] for x in rows if x["cpu_percent"] is not None]
        mem = [x["mem_used_bytes"] for x in rows if x["mem_used_bytes"] is not None]
        expected = int(window_s // interval_s)
        per[c] = {"samples": len(rows), "expected_samples": expected,
                  "coverage": round(len(rows) / expected, 6) if expected else None,
                  "recv_gap_ms_max": max(gaps) if gaps else None,
                  "recv_gap_ms_mean": round(sum(gaps) / len(gaps), 3) if gaps else None,
                  "delay_ms_median": delays[len(delays) // 2] if delays else None,
                  "delay_ms_max": delays[-1] if delays else None,
                  "cpu_percent_max": max(cpu) if cpu else None,
                  "cpu_percent_mean": round(sum(cpu) / len(cpu), 6) if cpu else None,
                  "mem_used_bytes_max": max(mem) if mem else None,
                  "mem_limit_bytes": next((x["mem_limit_bytes"] for x in rows if x["mem_limit_bytes"]), None),
                  "streams_opened": len({x["attempt"] for x in rows})}
        samples += rows
    return samples, per


def run_stream(containers: list[str], out_dir: Path, runner: tc.Runner, *, opener: StreamOpener | None = None,
               wall_ns: Callable[[], int] = time.time_ns, mono_ns: Callable[[], int] = time.monotonic_ns,
               poll_s: float = 0.05, interval_s: float = DAEMON_INTERVAL_S, duration_s: float | None = None,
               max_ticks: int | None = None, stop: Callable[[], bool] | None = None, gap_factor: float = 1.5,
               max_delay_s: float = 0.5, docker_sock: str = DOCKER_SOCK, argv: list[str] | None = None) -> dict:
    if not containers or len(set(containers)) != len(containers) or not all(_CONTAINER.match(c) for c in containers):
        raise ValueError("need distinct valid container names")
    if duration_s is None and max_ticks is None and stop is None:
        raise ValueError("need --duration, --max-ticks or --stop-file")
    if interval_s != DAEMON_INTERVAL_S:
        raise ValueError(f"--source api: the Docker daemon publishes stats every {DAEMON_INTERVAL_S} s; "
                         f"--interval must be {DAEMON_INTERVAL_S}")
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "raw").mkdir()
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    snap_start = inspect_snapshot(runner, containers, out_dir, an, "start")
    ev = threading.Event()
    start_w, start_m = wall_ns(), mono_ns()
    threads, records, closers, lock = collect_streams(containers, opener or docker_api_opener(docker_sock),
                                                      out_dir / "raw", ev, wall_ns=wall_ns, mono_ns=mono_ns)
    while True:
        if (stop is not None and stop()) or (duration_s is not None and mono_ns() - start_m >= duration_s * 1e9):
            break
        if max_ticks is not None:
            with lock:
                counts = {c: 0 for c in containers}
                for r in records:
                    if r["event"] == "line":
                        counts[r["container"]] += 1
            if min(counts.values()) >= max_ticks:
                break
        time.sleep(poll_s)
    stop_w, stop_m = wall_ns(), mono_ns()
    ev.set()
    with lock:
        for close in closers.values():
            close()
    for t in threads:
        t.join(timeout=15)
    hung = [t.name for t in threads if t.is_alive()]
    with lock:
        recs = [dict(r) for r in records if r["mono_ns"] <= stop_m]   # lines that arrived after the stop are not samples
    if abs((stop_w - start_w) - (stop_m - start_m)) > HOST_STEP_NS:
        an.add("host_clock_step", "", f"over the sampling: wall {stop_w - start_w} ns vs monotonic {stop_m - start_m} ns")
    samples, per = analyze_stream(recs, containers, an, start_mono_ns=start_m, stop_mono_ns=stop_m,
                                  interval_s=interval_s, gap_factor=gap_factor, max_delay_s=max_delay_s)
    snap_end = inspect_snapshot(runner, containers, out_dir, an, "end")
    compare_snapshots(snap_start, snap_end, containers, an)
    receipts = [{"container": r["container"], "raw_line": r["raw_line"], "attempt": r["attempt"],
                 "recv_wall_ns": r["wall_ns"], "recv_mono_ns": r["mono_ns"], "bytes": len(r["line"]),
                 "sha256": tc.sha256_bytes(r["line"])} for r in recs if r["event"] == "line"]
    outputs = {"resource_receipts.csv": tc.write_csv_x(out_dir / "resource_receipts.csv", RECEIPT_COLS, receipts),
               "resource_samples.csv": tc.write_csv_x(out_dir / "resource_samples.csv", STREAM_SAMPLE_COLS, samples),
               "resource_anomalies.csv": tc.write_csv_x(out_dir / "resource_anomalies.csv", tc.ANOMALY_COLS,
                                                        an.rows())}
    for c in containers:
        outputs[f"raw/stats_{c}.jsonl"] = tc.sha256_file(out_dir / "raw" / f"stats_{c}.jsonl")
    manifest = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "source": "docker_engine_api",
                "api_path": API_STATS_PATH, "docker_sock": docker_sock, "containers": containers,
                "parameters": {"interval_s": interval_s, "duration_s": duration_s, "max_ticks": max_ticks,
                               "gap_factor": gap_factor, "max_delay_s": max_delay_s},
                "start_utc": tc.utc_iso(start_w), "end_utc": tc.utc_iso(stop_w),
                "window_s": round((stop_m - start_m) / 1e9, 6), "lines_received": len(receipts),
                "lines_after_stop_dropped": sum(1 for r in records if r["mono_ns"] > stop_m),
                "reader_threads_not_stopped": hung, "snapshots": {"start": snap_start, "end": snap_end},
                "per_container": per,
                "precision_note": "exact daemon counters (ns, bytes); CPU % = delta container CPU / delta system CPU "
                                  "x online CPUs x 100 (docker CLI formula); memory used = usage - inactive_file",
                "timestamp_note": "read = daemon collection time (CLOCK_REALTIME of the shared kernel); recv = "
                                  "receiving thread's wall and monotonic clocks; gaps use the monotonic receipts",
                "status": an.status(), "anomalies": an.counts(), "outputs_sha256": outputs}
    tc.write_json_x(out_dir / "resource_manifest.json", manifest)
    return manifest


# ------------------------------------------------------------------ typeperf
_TP_TIME = "%m/%d/%Y %H:%M:%S.%f"
_TP_BIAS = re.compile(r"\((-?\d+)\)\s*$")


def parse_typeperf(text: str, out_dir: Path, *, utc_offset_min: int | None = None, source: str = "",
                   argv: list[str] | None = None) -> dict:
    """Parse a typeperf CSV log. UTC = local + bias minutes (PDH header), or local - utc_offset_min."""
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    rows_in = list(csv.reader(io.StringIO(text)))
    rows_in = [r for r in rows_in if r and any(c.strip() for c in r)]
    if not rows_in:
        raise ValueError("typeperf file has no rows")
    header = rows_in[0]
    counters = header[1:]
    m = _TP_BIAS.search(header[0])
    if utc_offset_min is not None:
        bias, bias_source = -utc_offset_min, "--utc-offset-min"
    elif m:
        bias, bias_source = int(m.group(1)), "PDH header"
    else:
        bias, bias_source = None, "unknown"
        an.add("timezone_unknown", "", "no (bias) in the PDH header and no --utc-offset-min", f"{source}:1", header[0])
    out, prev_ms, seen_ms = [], None, set()
    for no, r in enumerate(rows_in[1:], start=2):
        if len(r) != len(header):
            an.add("malformed_line", "", f"{len(r)} fields, header has {len(header)}", f"{source}:{no}", ",".join(r))
            continue
        try:
            local = datetime.datetime.strptime(r[0], _TP_TIME)
        except ValueError:
            an.add("malformed_line", "", f"timestamp {r[0]!r}", f"{source}:{no}", ",".join(r))
            continue
        t_ms = None
        if bias is not None:
            utc = (local + datetime.timedelta(minutes=bias)).replace(tzinfo=datetime.timezone.utc)
            t_ms = int(utc.timestamp() * 1000)
            if t_ms in seen_ms:
                an.add("duplicate_timestamp", "", r[0], f"{source}:{no}")
            elif prev_ms is not None and t_ms < prev_ms:
                an.add("timestamp_order", "", f"{r[0]} earlier than the previous row", f"{source}:{no}")
            seen_ms.add(t_ms)
            prev_ms = t_ms if prev_ms is None else max(prev_ms, t_ms)
        for name, raw in zip(counters, r[1:]):
            val = None
            if raw.strip():
                try:
                    val = float(raw)
                except ValueError:
                    an.add("malformed_line", name, f"value {raw!r}", f"{source}:{no}")
                    continue
            else:
                an.add("value_missing", name, "blank value", f"{source}:{no}")
            out.append({"row": no, "t_local": r[0], "t_utc_ms": t_ms, "counter": name, "value": val, "value_raw": raw})
    out_dir.mkdir(parents=True, exist_ok=False)
    outputs = {"host_counters.csv": tc.write_csv_x(out_dir / "host_counters.csv", HOST_COLS, out),
               "host_counters_anomalies.csv": tc.write_csv_x(out_dir / "host_counters_anomalies.csv",
                                                             tc.ANOMALY_COLS, an.rows())}
    manifest = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "source": source,
                "source_sha256": tc.sha256_bytes(text.encode("utf-8")), "counters": counters,
                "utc_bias_minutes": bias, "utc_bias_source": bias_source, "rows": len(rows_in) - 1,
                "status": an.status(), "anomalies": an.counts(), "outputs_sha256": outputs}
    tc.write_json_x(out_dir / "host_counters_manifest.json", manifest)
    return manifest


# ------------------------------------------------------------------ CLI
def main(argv: list[str] | None = None, runner: tc.Runner | None = None, opener: StreamOpener | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--container", action="append", required=True)
    s.add_argument("--out-dir", required=True, type=Path)
    s.add_argument("--interval", type=float, default=1.0)
    s.add_argument("--duration", type=float)
    s.add_argument("--max-ticks", type=int)
    s.add_argument("--stop-file", type=Path)
    s.add_argument("--source", choices=["api", "cli"], default="api",
                   help="api: Docker Engine API stats streams, 1 s daemon cadence (default, 0.2); cli: docker stats "
                        "--no-stream per tick (0.1; about 2 s per call on this machine)")
    s.add_argument("--docker-sock", default=DOCKER_SOCK)
    s.add_argument("--gap-factor", type=float, default=1.5, help="api: sample_gap above this many intervals")
    s.add_argument("--max-delay", type=float, default=0.5, help="api: sample_delayed above this many seconds")
    s.add_argument("--execute", action="store_true", help="run docker stats/inspect (needs Docker authorization)")
    t = sub.add_parser("typeperf")
    t.add_argument("--file", required=True, type=Path)
    t.add_argument("--utc-offset-min", type=int)
    t.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args(argv)
    if args.out_dir.exists():
        print(f"refusing to overwrite {args.out_dir}")
        return 2
    cmdline = sys.argv if argv is None else argv
    try:
        if args.cmd == "typeperf":
            raw = args.file.read_bytes()
            text = raw.decode("utf-8-sig") if not raw.startswith(b"\xff\xfe") else raw.decode("utf-16")
            m = parse_typeperf(text, args.out_dir, utc_offset_min=args.utc_offset_min, source=str(args.file),
                               argv=cmdline)
            print(f"HOST COUNTERS {m['status']} rows={m['rows']} -> {args.out_dir}")
            return tc.EXIT[m["status"]]
        if not args.execute:
            print("PLAN " + " ".join(["docker", "inspect"] + args.container) + "   (start and end)")
            if args.source == "api":
                for c in args.container:
                    print(f"PLAN GET unix:{args.docker_sock} {API_STATS_PATH.format(name=c)}   (one stream, "
                          f"daemon cadence {DAEMON_INTERVAL_S} s)")
            else:
                print("PLAN " + " ".join(STATS_ARGV) + f"   every {args.interval} s")
            print("dry run: nothing executed or written (add --execute once Docker use is authorized)")
            return 0
        stop = (lambda: args.stop_file.exists()) if args.stop_file else None
        if args.source == "api":
            m = run_stream(args.container, args.out_dir, runner or tc.subprocess_runner, opener=opener,
                           interval_s=args.interval, duration_s=args.duration, max_ticks=args.max_ticks, stop=stop,
                           gap_factor=args.gap_factor, max_delay_s=args.max_delay, docker_sock=args.docker_sock,
                           argv=cmdline)
        else:
            m = run_sample(args.container, args.out_dir, runner or tc.subprocess_runner, interval_s=args.interval,
                           duration_s=args.duration, max_ticks=args.max_ticks, stop=stop, argv=cmdline)
    except (OSError, ValueError, UnicodeDecodeError) as err:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {err!r}")
        return 2
    for sev in ("error", "warning"):
        for k, n in sorted(m["anomalies"][sev].items()):
            print(f"  {sev:<7} {k:<24} {n}")
    count = m["ticks"] if "ticks" in m else f"{m['lines_received']} lines"
    print(f"RESOURCES {m['status']} source={args.source} ticks={count} -> {args.out_dir}")
    return tc.EXIT[m["status"]]


if __name__ == "__main__":
    sys.exit(main())
