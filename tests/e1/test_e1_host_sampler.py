"""Fabric-free tests for scripts/e1/e1_host_sampler.py (instrumentation B3). Synthetic only: nothing here is an E1 result.

A fake /proc tree and a scripted typeperf source stand in for the host; the clocks are fake. The parsers, the rate arithmetic, the Windows-header handling
(built from the real typeperf header and row format measured on the testbed machine on 2026-11-13), the loop, the manifest and the window-evidence functions
are the real code. No Docker, Fabric, Caliper, WSL or Windows process is started, and no test sleeps on a real clock for more than a fraction of a second.
"""
import ast
import csv
import datetime
import json
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "e1"))

import e1_host_sampler as hs  # noqa: E402
import e1_telemetry_common as tc  # noqa: E402

UTC = datetime.timezone.utc

# ---- real lines from the testbed's WSL VM (2026-11-13), used verbatim by the parser tests
REAL_DISKSTATS = """   7       0 loop0 54 0 2216 8 0 0 0 0 0 52 8 0 0 0 0 0 0
   8       0 sda 1911 504 221314 519 0 0 0 0 0 400 519 0 0 0 0 0 0
   8       1 sda1 1000 100 100000 100 0 0 0 0 0 100 100 0 0 0 0 0 0
   8      32 sdc 6429 1717 68840 1526 11324 149115 1283504 11876 0 4420 13404 0 0 0 0 1 1
   8      48 sdd 1059454 22603 232456230 225795 56983 66470 22268776 182555 0 202388 411725 642 73 1109160 84 7489 3290
 259       0 nvme0n1 5 0 40 1 6 0 48 2 0 3 3 0 0 0 0 0 0
 259       1 nvme0n1p1 5 0 40 1 6 0 48 2 0 3 3 0 0 0 0 0 0
   1       0 ram0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
"""
REAL_NET_DEV = """Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 3768677415 1520475    0    0    0     0          0         0 3768677415 1520475    0    0    0     0       0          0
  eth0:  450135    1977    1    2    0     0          0      1935     7147      79    3    4    0     0       0          0
"""
REAL_PRESSURE_CPU = "some avg10=0.00 avg60=0.00 avg300=0.00 total=83213363\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
NET_INSTANCES = ["Realtek PCIe GbE Family Controller", "Intel[R] Wi-Fi 7 BE200 320MHz", "Intel[R] Wi-Fi 7 BE200 320MHz _2",
                 "Intel[R] Wi-Fi 7 BE200 320MHz _3", "Intel[R] Wi-Fi 7 BE200 320MHz _4", "Intel[R] Wi-Fi 7 BE200 320MHz _5"]
HOST = r"\\LAPTOP-PGCEOISJ"


def win_header_cells(drop=()):
    paths = [r"\Processor Information(_Total)\% Processor Utility", r"\Processor(_Total)\% Processor Time", r"\System\Processor Queue Length",
             r"\PhysicalDisk(_Total)\% Disk Time", r"\PhysicalDisk(_Total)\Disk Bytes/sec", r"\PhysicalDisk(_Total)\Disk Transfers/sec",
             r"\PhysicalDisk(_Total)\Avg. Disk sec/Transfer", r"\PhysicalDisk(_Total)\Current Disk Queue Length"] + \
            [rf"\Network Interface({n})\Bytes Total/sec" for n in NET_INSTANCES] + \
            [r"\Memory\Available MBytes", r"\Memory\% Committed Bytes In Use", r"\Memory\Pages/sec"]
    return [p for p in paths if not any(d in p for d in drop)]


def win_header_line(drop=()):
    return ",".join(f'"{c}"' for c in ["(PDH-CSV 4.0)"] + [HOST + p for p in win_header_cells(drop)]) + "\r\n"


def win_row_line(local_text, values):
    return ",".join(f'"{c}"' for c in [local_text] + ["" if v is None else f"{v:.6f}" for v in values]) + "\r\n"


def win_values(n_cols, base=1.0):
    return [base + i for i in range(n_cols)]


# ====================================================================================== parsers
def test_parse_stat_cpu_percentages_and_per_core_maximum():
    a = hs.parse_stat("cpu  100 0 50 800 50 0 0 0 0 0\ncpu0 60 0 20 400 20 0 0 0 0 0\ncpu1 40 0 30 400 30 0 0 0 0 0\nintr 1\n")
    b = hs.parse_stat("cpu  300 0 150 1000 70 0 0 0 0 0\ncpu0 160 0 70 400 20 0 0 0 0 0\ncpu1 140 0 80 600 50 0 0 0 0 0\n")
    assert set(a) == {"cpu", "cpu0", "cpu1"}
    p = hs.cpu_percentages(a, b)
    # aggregate delta: user 200, system 100, idle 200, iowait 20 -> total 520, busy 300
    assert p["busy"] == pytest.approx(100 * 300 / 520) and p["iowait"] == pytest.approx(100 * 20 / 520) and p["user"] == pytest.approx(100 * 200 / 520)
    assert p["max_core"] == pytest.approx(100.0) and p["cpus"] == 2          # cpu0: all 200 jiffies busy
    assert hs.cpu_percentages(a, a) is None                                  # no elapsed time
    assert hs.cpu_percentages(b, a) == {"reset": True}                       # counters going backwards


def test_parse_pressure_loadavg_meminfo():
    assert hs.parse_pressure(REAL_PRESSURE_CPU) == {"some": 83213363, "full": 0}
    assert hs.parse_pressure(None) == {} and hs.parse_pressure("garbage") == {}
    assert hs.parse_loadavg("0.04 0.03 0.07 1/844 50610") == {"load1": 0.04, "runnable": 1} and hs.parse_loadavg("") == {}
    m = hs.parse_meminfo("MemTotal: 7925916 kB\nMemAvailable:    6026192 kB\nSwapTotal: 2048 kB\nSwapFree: 1024 kB\n")
    assert m["mem_available_mb"] == pytest.approx(6026192 / 1024) and m["swap_used_mb"] == pytest.approx(1.0)


def test_parse_diskstats_keeps_whole_disks_only():
    d = hs.parse_diskstats(REAL_DISKSTATS)
    assert sorted(d) == ["nvme0n1", "sda", "sdc", "sdd"]                     # no loop, ram, or partition (sda1, nvme0n1p1)
    assert d["sdd"] == {"reads": 1059454, "sectors_read": 232456230, "writes": 56983, "sectors_written": 22268776, "io_ticks_ms": 202388}
    assert hs.parse_diskstats("short line\n") == {} and hs.parse_diskstats(None) == {}


def test_parse_net_dev_real_lines():
    n = hs.parse_net_dev(REAL_NET_DEV)
    assert sorted(n) == ["eth0", "lo"] and n["eth0"]["rx_bytes"] == 450135 and n["eth0"]["tx_bytes"] == 7147
    assert n["eth0"]["rx_errs"] == 1 and n["eth0"]["rx_drop"] == 2 and n["eth0"]["tx_errs"] == 3 and n["eth0"]["tx_drop"] == 4
    assert n["lo"]["rx_bytes"] == 3768677415


# ====================================================================================== rates
def snap_of(t_s, *, jiffies=0, psi=0, sectors=0, ticks=0, rx=0, lo=0, errs=0, wall0=1_700_000_000_000_000_000):
    stat = (f"cpu  {jiffies} 0 0 {jiffies} 0 0 0 0 0 0\ncpu0 {jiffies} 0 0 0 0 0 0 0 0 0\ncpu1 0 0 0 {jiffies} 0 0 0 0 0 0\n")
    return {"wall_ns": wall0 + int(t_s * 1e9), "mono_ns": int(t_s * 1e9), "stat": hs.parse_stat(stat), "load": {"load1": 0.5, "runnable": 2},
            "mem": {"mem_available_mb": 5000.0, "swap_used_mb": 0.0},
            "disks": {"sda": {"reads": 10 * int(t_s), "sectors_read": sectors, "writes": 20 * int(t_s), "sectors_written": 2 * sectors,
                              "io_ticks_ms": ticks}},
            "net": {"lo": {"rx_bytes": lo, "rx_packets": 0, "rx_errs": 0, "rx_drop": 0, "tx_bytes": lo, "tx_packets": 0, "tx_errs": 0, "tx_drop": 0},
                    "eth0": {"rx_bytes": rx, "rx_packets": 3 * int(t_s), "rx_errs": errs, "rx_drop": 0, "tx_bytes": rx // 2, "tx_packets": int(t_s),
                             "tx_errs": 0, "tx_drop": 0}},
            "psi": {"psi_cpu": {"some": psi, "full": 0}, "psi_mem": {"some": 0, "full": 0}, "psi_io": {"some": psi // 2}}}


def test_rates_use_the_real_elapsed_monotonic_time():
    a = snap_of(10.0)
    b = snap_of(12.0, jiffies=100, psi=500_000, sectors=4_000_000, ticks=1000, rx=4_000_000, lo=8_000_000, errs=6)       # 2.0 s elapsed
    row, notes = hs.rates(a, b, 1)
    assert notes == [] and row["dt_s"] == 2.0 and row["tick"] == 1
    assert row["cpu_busy_pct"] == pytest.approx(50.0) and row["cpu_max_core_busy_pct"] == pytest.approx(100.0)
    assert row["psi_cpu_some_pct"] == pytest.approx(100 * 500_000 / 2e6) and row["psi_io_some_pct"] == pytest.approx(100 * 250_000 / 2e6)
    assert row["psi_mem_some_pct"] == 0
    assert row["disk_read_mb_s"] == pytest.approx(4_000_000 * 512 / 1e6 / 2) and row["disk_write_mb_s"] == pytest.approx(2 * 4_000_000 * 512 / 1e6 / 2)
    assert row["disk_read_iops"] == pytest.approx(10.0) and row["disk_write_iops"] == pytest.approx(20.0)
    assert row["disk_util_max_pct"] == pytest.approx(100 * 1000 / 2000)
    assert row["net_if_rx_mb_s"] == pytest.approx(2.0) and row["net_if_tx_mb_s"] == pytest.approx(1.0) and row["net_if_err_drop_s"] == pytest.approx(3.0)
    assert row["net_lo_mb_s"] == pytest.approx(4.0)                          # loopback is reported apart and never added to the interface totals
    assert row["net_if_rx_pkts_s"] == pytest.approx(3.0) and row["net_if_tx_pkts_s"] == pytest.approx(1.0)


def test_first_sample_has_no_rates_and_a_non_advancing_clock_is_flagged():
    row, notes = hs.rates(None, snap_of(1.0), 0)
    assert notes == [] and "cpu_busy_pct" not in row and row["mem_available_mb"] == 5000.0 and row["load1"] == 0.5
    row2, notes2 = hs.rates(snap_of(5.0), snap_of(5.0), 1)
    assert [k for k, _ in notes2] == ["time_order"] and "cpu_busy_pct" not in row2


def test_a_counter_that_goes_backwards_is_reported_and_leaves_that_figure_blank():
    a = snap_of(1.0, jiffies=100, sectors=1000, rx=5000)
    b = snap_of(2.0, jiffies=200, sectors=500, rx=9000)                      # sectors_read went backwards
    row, notes = hs.rates(a, b, 1)
    assert ("counter_reset", "diskstats sda sectors_read") in notes
    assert "disk_read_mb_s" not in row and row["net_if_rx_mb_s"] == pytest.approx(4000 / 1e6 / 1.0) and "cpu_busy_pct" in row


def test_a_host_without_whole_disks_is_noted_once_on_the_first_rate():
    a, b = snap_of(1.0), snap_of(2.0)
    a["disks"], b["disks"] = {}, {}
    _, notes1 = hs.rates(a, b, 1)
    _, notes2 = hs.rates(b, snap_of(3.0) | {"disks": {}}, 2)
    assert [k for k, _ in notes1] == ["diskstats_no_devices"] and notes2 == []


# ====================================================================================== Windows side
def test_real_header_maps_every_counter_and_sums_the_network_instances():
    mapping, req, opt = hs.parse_win_header(win_header_line().rstrip("\r\n"))
    assert req == [] and opt == [] and len(mapping["net_bytes_s"]) == 6 and mapping["cpu_utility_pct"] == [1] and mapping["mem_available_mb"] == [15]
    row = hs.parse_win_row(win_row_line("11/13/2026 05:08:08.913", win_values(17)).rstrip("\r\n"), mapping)
    assert row["t_local"] == "11/13/2026 05:08:08.913" and row["cpu_utility_pct"] == 1.0 and row["mem_available_mb"] == 15.0
    assert row["net_bytes_s"] == sum(range(9, 15)) and row["net_instances"] == 6      # cells 9..14 hold the six interface values 9..14
    assert row["local_epoch"] == (datetime.datetime(2026, 11, 13, 5, 8, 8, 913000) - hs.EPOCH).total_seconds()


def test_a_dropped_counter_is_visible_in_the_header_not_assumed():
    mapping, req, opt = hs.parse_win_header(win_header_line(drop=("% Processor Time",)).rstrip("\r\n"))
    assert req == [] and opt == ["cpu_time_pct"]                              # optional: the record stays usable
    mapping, req, opt = hs.parse_win_header(win_header_line(drop=("Processor Utility", "Bytes Total/sec")).rstrip("\r\n"))
    assert req == ["cpu_utility_pct", "net_bytes_s"]                          # required: the record would not be accepted


def test_non_data_lines_are_not_rows():
    mapping, _, _ = hs.parse_win_header(win_header_line().rstrip("\r\n"))
    for text in ("Exiting, please wait...", "The command completed successfully.", "", '"not a time","1"'):
        assert hs.parse_win_row(text, mapping) is None


def test_a_blank_cell_is_a_missing_value_not_a_zero():
    mapping, _, _ = hs.parse_win_header(win_header_line().rstrip("\r\n"))
    vals = win_values(17)
    vals[14] = None                                                            # Available MBytes
    row = hs.parse_win_row(win_row_line("11/13/2026 05:08:08.913", vals).rstrip("\r\n"), mapping)
    assert row["mem_available_mb"] is None and row["cpu_utility_pct"] == 1.0


@pytest.mark.parametrize("offset_min", [330, 0, -300, -210, 540, 345])
def test_offset_inference_survives_the_receipt_lag(offset_min):
    utc = datetime.datetime(2026, 11, 12, 23, 38, 8, 913000)
    local_epoch = (utc + datetime.timedelta(minutes=offset_min) - hs.EPOCH).total_seconds()
    recv = int((utc - hs.EPOCH).total_seconds() * 1e9) + 1_200_000_000           # received 1.2 s after its sample time, as measured
    assert hs.infer_offset_min(local_epoch, recv) == offset_min


def test_typeperf_command_line_is_bounded_and_complete():
    a = hs.typeperf_argv(600.0)
    assert a[0] == "typeperf.exe" and a[a.index("-si") + 1] == "1" and a[a.index("-sc") + 1] == "602"
    assert a[1:13] == hs.WIN_COUNTER_PATHS and len(hs.WIN_COUNTER_PATHS) == 12
    assert any("Network Interface(*)" in p for p in hs.WIN_COUNTER_PATHS)
    # the quirk found on 2026-11-13: two counters of the same object and instance, so only one Processor Information counter is asked for
    assert sum(1 for p in hs.WIN_COUNTER_PATHS if p.startswith(r"\Processor Information(_Total)")) == 1


FIRST_AT = 2.0
ROW0_UTC_NS = int((datetime.datetime(2026, 11, 12, 23, 38, 8, 913000, tzinfo=UTC) - datetime.datetime(1970, 1, 1, tzinfo=UTC)).total_seconds() * 1e9)
WALL0 = ROW0_UTC_NS - int(FIRST_AT * 1e9)         # the fake wall clock reads ROW0_UTC at monotonic time FIRST_AT: typeperf row 0 is sampled then (local 05:08:08.913, UTC+5:30)


class Clock:
    """Fake monotonic and wall clocks; sleep() advances them and lets the fake /proc move on."""

    def __init__(self, wall0, on_sleep=None):
        self.t, self.wall0, self.on_sleep = 0.0, wall0, on_sleep

    def mono_ns(self):
        return int(self.t * 1e9)

    def wall_ns(self):
        return self.wall(self.t)

    def wall(self, t):
        return self.wall0 + int(t * 1e9)

    def sleep(self, s):
        self.t += s
        if self.on_sleep:
            self.on_sleep(self.t)


class FakeWin:
    """Scripted typeperf: a header, then one row per 1.013 s, each received `lag_s` after its sample time (what a piped typeperf does)."""

    def __init__(self, clock, *, lag_s=1.2, header=None, local0=datetime.datetime(2026, 11, 13, 5, 8, 8, 913000), hole=None, values=None):
        self.clock, self.lag, self.argv, self.stderr_tail, self.rc = clock, lag_s, ["typeperf.exe"], b"", None
        self.header = win_header_line() if header is None else header
        self.local0, self.hole, self.values, self.stopped, self.eof = local0, hole, values, False, False
        self.header_sent, self.k = False, 0

    def drain(self):
        out = []
        if not self.header_sent and self.clock.t >= FIRST_AT - 1.0 + self.lag:
            self.header_sent = True
            if self.header:
                out.append((self.clock.wall(FIRST_AT - 1.0 + self.lag), 0, self.header.encode()))
        while not self.stopped and (FIRST_AT + self.k * 1.013 + self.lag) <= self.clock.t:
            t_sample = FIRST_AT + self.k * 1.013
            if not (self.hole and self.hole[0] <= t_sample < self.hole[1]):
                local = self.local0 + datetime.timedelta(seconds=self.k * 1.013)
                vals = self.values(self.k) if self.values else win_values(17, base=self.k)
                text = local.strftime("%m/%d/%Y %H:%M:%S.") + f"{local.microsecond // 1000:03d}"
                out.append((self.clock.wall(t_sample + self.lag), 0, win_row_line(text, vals).encode()))
            self.k += 1
        return out

    def stop(self):
        self.stopped, self.rc, self.eof = True, 1, True

    @property
    def returncode(self):
        return self.rc


class ProcTree:
    """A /proc that moves with the fake clock: 2 CPUs (cpu0 always busy, cpu1 idle), a disk, eth0 and lo, PSI."""

    def __init__(self, root, *, missing=()):
        self.root, self.missing = Path(root), set(missing)
        (self.root / "net").mkdir(parents=True)
        (self.root / "pressure").mkdir()
        self.write(0.0)

    def write(self, t):
        s = int(round(t))
        files = {"stat": f"cpu  {100 * s} 0 0 {100 * s} 0 0 0 0 0 0\ncpu0 {100 * s} 0 0 0 0 0 0 0 0 0\ncpu1 0 0 0 {100 * s} 0 0 0 0 0 0\n",
                 "loadavg": "0.50 0.40 0.30 2/800 1234\n", "meminfo": "MemAvailable: 5242880 kB\nSwapTotal: 0 kB\nSwapFree: 0 kB\n",
                 "diskstats": f"   8 0 sda {10 * s} 0 {2000 * s} 0 {20 * s} 0 {4000 * s} 0 0 {100 * s} 0 0 0 0 0 0\n",
                 "net/dev": REAL_NET_DEV.replace("450135", str(450135 + 1_000_000 * s)).replace("3768677415", str(3768677415 + 500_000 * s)),
                 "pressure/cpu": f"some avg10=0.00 avg60=0.00 avg300=0.00 total={10_000 * s}\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
                 "pressure/memory": "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
                 "pressure/io": f"some avg10=0.00 avg60=0.00 avg300=0.00 total={5_000 * s}\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"}
        for name, text in files.items():
            if name not in self.missing:
                (self.root / name).write_text(text, encoding="utf-8")


def sample(tmp_path, *, win=True, seconds=10, missing=(), max_seconds=600.0, hole=None, header=None, jump=None, values=None, lag_s=1.2):
    tree = ProcTree(tmp_path / "proc", missing=missing)
    holder: dict = {}
    jumped = {"done": False}

    def on_sleep(t):
        if jump and holder["clock"].t >= jump[0] and not jumped["done"]:
            holder["clock"].t += jump[1]                                    # the sampler process is starved for a while
            jumped["done"] = True
        tree.write(holder["clock"].t)
    clock = Clock(WALL0, on_sleep)
    holder["clock"] = clock
    w = FakeWin(clock, hole=hole, header=header, values=values, lag_s=lag_s) if win else None
    out = tmp_path / "host"
    man = hs.run_sample(out, interval_s=1.0, max_seconds=max_seconds, stop=lambda: clock.t >= seconds, proc_root=tree.root, win=w,
                        wall_ns=clock.wall_ns, mono_ns=clock.mono_ns, sleep=clock.sleep, argv=["test"])
    return man, out, clock


def rows(path):
    with Path(path).open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ====================================================================================== the loop
def test_a_clean_sampling_session_is_complete_and_the_files_are_consistent(tmp_path):
    man, out, clock = sample(tmp_path, seconds=12)
    assert man["status"] == "COMPLETE", man["anomalies"]
    assert man["stop_reason"] == "stop_file" and man["ticks"] == man["wsl_rows"] >= 12 and man["psi_available"] is True
    wsl = rows(out / "wsl_samples.csv")
    assert list(wsl[0]) == hs.WSL_COLS and wsl[0]["cpu_busy_pct"] == "" and wsl[1]["cpu_busy_pct"] != ""
    assert float(wsl[5]["cpu_busy_pct"]) == pytest.approx(50.0) and float(wsl[5]["cpu_max_core_busy_pct"]) == pytest.approx(100.0)
    assert float(wsl[5]["disk_read_mb_s"]) == pytest.approx(2000 * 512 / 1e6) and float(wsl[5]["net_if_rx_mb_s"]) == pytest.approx(1.0)
    assert float(wsl[5]["psi_cpu_some_pct"]) == pytest.approx(1.0) and float(wsl[5]["dt_s"]) == pytest.approx(1.0)
    win = rows(out / "win_counters.csv")
    assert list(win[0]) == hs.WIN_COLS and len(win) >= 8 and man["win_rows"] == len(win)
    assert man["windows_utc_offset_min"] == 330 and man["windows_offset_source"].startswith("inferred")
    assert float(win[0]["lag_ms"]) == pytest.approx(1200.0, abs=5) and int(win[0]["net_instances"]) == 6
    t = [int(r["t_wall_ns"]) for r in win]
    assert t == sorted(t) and all(1.0 < (b - a) / 1e9 < 1.03 for a, b in zip(t, t[1:]))      # sample times follow typeperf's own clock
    assert (out / "win_raw.tsv").read_text(encoding="utf-8").count("\n") >= len(win) + 1     # every received line is kept, header included
    for name, sha in man["outputs_sha256"].items():                                           # the manifest hashes the files as written
        assert tc.sha256_file(out / name) == sha, name
    assert json.loads((out / "host_manifest.json").read_text(encoding="utf-8"))["tool"] == hs.TOOL_VERSION


def test_windows_sample_times_not_receipt_times_decide_the_window(tmp_path):
    man, out, _ = sample(tmp_path, seconds=20)
    win = rows(out / "win_counters.csv")
    assert int(win[0]["t_wall_ns"]) == pytest.approx(ROW0_UTC_NS, abs=2_000_000)                # the row's own sample time, on the WSL clock
    assert int(win[0]["recv_wall_ns"]) - int(win[0]["t_wall_ns"]) == pytest.approx(1_200_000_000, abs=5_000_000)   # receipts are kept apart, as a diagnostic


def test_never_overwrites_an_existing_output_directory(tmp_path):
    sample(tmp_path, seconds=3)
    with pytest.raises(FileExistsError):
        hs.run_sample(tmp_path / "host", proc_root=tmp_path / "proc", win=None)


def test_max_seconds_is_a_hard_guard_even_without_a_stop_file(tmp_path):
    tree = ProcTree(tmp_path / "proc")
    clock = Clock(WALL0, lambda t: tree.write(t))
    man = hs.run_sample(tmp_path / "h", max_seconds=5.0, stop=None, proc_root=tree.root, win=None, wall_ns=clock.wall_ns, mono_ns=clock.mono_ns, sleep=clock.sleep)
    assert man["stop_reason"] == "max_seconds" and 5 <= man["ticks"] <= 7


def test_no_windows_flag_is_a_warning_but_the_record_has_no_windows_rows(tmp_path):
    man, out, _ = sample(tmp_path, win=False, seconds=4)
    assert man["status"] == "COMPLETE_WITH_WARNINGS" and man["anomalies"]["warning"]["win_disabled"] == 1 and man["win_rows"] == 0
    assert rows(out / "win_counters.csv") == [] and man["parameters"]["windows"] is False


def test_a_missing_proc_stat_makes_the_session_incomplete_once_not_per_tick(tmp_path):
    man, out, _ = sample(tmp_path, win=False, seconds=6, missing=("stat",))
    assert man["status"] == "INCOMPLETE" and man["anomalies"]["error"]["proc_unreadable"] == 1
    assert all(r["cpu_busy_pct"] == "" for r in rows(out / "wsl_samples.csv"))


def test_a_kernel_without_psi_is_only_a_warning(tmp_path):
    man, out, _ = sample(tmp_path, win=False, seconds=4, missing=("pressure/cpu", "pressure/memory", "pressure/io"))
    assert man["status"] == "COMPLETE_WITH_WARNINGS" and man["psi_available"] is False and man["anomalies"]["warning"]["psi_unavailable"] == 1
    assert rows(out / "wsl_samples.csv")[2]["psi_cpu_some_pct"] == ""


def test_typeperf_rows_without_a_header_are_not_data(tmp_path):
    man, out, _ = sample(tmp_path, seconds=6, header="")                                      # the scripted source sends rows but no header
    assert man["status"] == "INCOMPLETE" and man["win_rows"] == 0
    assert man["anomalies"]["error"]["win_no_header"] == 1 and man["anomalies"]["error"]["win_no_rows"] == 1


def test_a_required_windows_counter_missing_from_the_header_is_incomplete_an_optional_one_is_not(tmp_path):
    man, out, _ = sample(tmp_path, seconds=6, header=win_header_line(drop=("% Processor Time",)))
    assert man["status"] == "COMPLETE_WITH_WARNINGS" and man["windows_missing_optional"] == ["cpu_time_pct"] and man["windows_missing_required"] == []
    assert "win_value_missing" not in man["anomalies"]["warning"]                             # absent from the header: reported once, not once per row
    second = tmp_path / "second"
    second.mkdir()
    man2, _, _ = sample(second, seconds=6, header=win_header_line(drop=("Disk Bytes/sec",)))
    assert man2["status"] == "INCOMPLETE" and man2["windows_missing_required"] == ["disk_bytes_s"]


def test_a_hole_in_the_typeperf_rows_and_a_stalled_sampling_tick_are_gaps(tmp_path):
    man, out, _ = sample(tmp_path, seconds=20, hole=(8.0, 12.0))
    assert man["status"] == "INCOMPLETE" and man["anomalies"]["error"]["win_gap"] >= 1
    d = tmp_path / "b"
    d.mkdir()
    man2, out2, _ = sample(d, win=False, seconds=15, jump=(5.0, 6.0))
    assert man2["status"] == "INCOMPLETE" and man2["anomalies"]["error"]["sampling_gap"] == 1
    t = [int(r["mono_ns"]) for r in rows(out2 / "wsl_samples.csv")]
    assert max(b - a for a, b in zip(t, t[1:])) / 1e9 > 5.0 and len(t) < 16                    # the missed ticks are skipped, never burst


def test_a_blank_windows_value_in_a_row_is_a_warning_and_stays_blank_in_the_file(tmp_path):
    def vals(k):
        v = win_values(17, base=k)
        if k == 3:
            v[14] = None                                                                       # Available MBytes missing in row 3 only
        return v
    man, out, _ = sample(tmp_path, seconds=10, values=vals)
    assert man["status"] == "COMPLETE_WITH_WARNINGS" and man["anomalies"]["warning"]["win_value_missing"] == 1
    w = rows(out / "win_counters.csv")
    assert w[3]["mem_available_mb"] == "" and w[2]["mem_available_mb"] != "" and w[4]["mem_available_mb"] != ""


def test_a_large_lag_between_sample_time_and_receipt_is_flagged_as_clock_skew(tmp_path):
    man, out, _ = sample(tmp_path, seconds=25, lag_s=9.0)                                      # rows arrive 9 s after their sample time
    assert man["anomalies"]["warning"]["win_clock_skew"] == 1 and man["windows_lag_ms"]["median"] == pytest.approx(9000, abs=10)
    assert man["windows_utc_offset_min"] == 330                                                # the offset inference is not fooled by the lag


def test_invalid_parameters_are_refused(tmp_path):
    with pytest.raises(ValueError):
        hs.run_sample(tmp_path / "x", interval_s=0)
    with pytest.raises(ValueError):
        hs.run_sample(tmp_path / "y", max_seconds=-1)


# ====================================================================================== the real Windows source (fake process)
class FakePopen:
    class _Pipe:
        def __init__(self, data):
            import io
            self.b = io.BytesIO(data)

        def readline(self):
            return self.b.readline()

        def read(self):
            return self.b.read()

    def __init__(self, argv, stdout=None, stderr=None, stdin=None):
        self.argv, self.rc, self.terminated = argv, None, False
        self.stdout = self._Pipe(b"\r\n" + win_header_line().encode() + win_row_line("11/13/2026 05:08:08.913", win_values(17)).encode())
        self.stderr = self._Pipe(b"warn")

    def poll(self):
        return self.rc

    def terminate(self):
        self.terminated, self.rc = True, 1

    def kill(self):
        self.rc = -9

    def wait(self, timeout=None):
        return self.rc


def test_typeperf_source_stamps_every_line_on_receipt_and_reports_eof():
    stamps = iter(range(1, 100))
    src = hs.TypeperfSource(["typeperf.exe", "x"], wall_ns=lambda: 1_000 * next(stamps), mono_ns=lambda: 7, popen=FakePopen)
    got = []
    for _ in range(300):
        got += src.drain()
        if src.eof:
            break
        time.sleep(0.01)
    src.stop()
    assert src.eof and len(got) == 3                                                           # a blank line, the header, one data row
    assert [g[0] for g in got] == [1000, 2000, 3000] and all(g[1] == 7 for g in got)           # stamped in the order received
    assert got[1][2].startswith(b'"(PDH') and got[0][2] == b"\r\n"
    assert src.stderr_tail == b"warn" and src.proc.argv == ["typeperf.exe", "x"] and src.proc.terminated is True


# ====================================================================================== window evidence
def test_stream_completeness_counts_expected_samples_and_the_longest_hole_including_the_edges():
    lo, hi = 1_000_000.0, 1_120_500.0                                                         # a 120.5 s window
    full = [lo + 1000.0 * k + 400.0 for k in range(120)]                                       # first at +0.4 s, last at +119.4 s
    c = hs.stream_completeness(full, lo, hi)
    assert c["expected"] == 120 and c["count"] == 120 and c["ratio"] == 1.0
    assert c["lead_s"] == pytest.approx(0.4) and c["tail_s"] == pytest.approx(1.1) and c["max_gap_s"] == pytest.approx(1.1)
    holed = [t for t in full if not (30_000 + lo <= t < 36_000 + lo)]                         # a 6-s hole
    h = hs.stream_completeness(holed, lo, hi)
    assert h["count"] == 114 and h["max_gap_s"] == pytest.approx(7.0, abs=0.05) and h["ratio"] == pytest.approx(0.95)
    late = hs.stream_completeness([t for t in full if t > lo + 10_000], lo, hi)                # starts 10 s late: the lead edge is a hole
    assert late["max_gap_s"] > 10.0
    none = hs.stream_completeness([lo - 5, hi + 5], lo, hi)
    assert none["count"] == 0 and none["ratio"] == 0.0 and none["max_gap_s"] == pytest.approx(120.5) and none["lead_s"] is None


def test_summarize_window_uses_only_rows_inside_the_window_and_converts_units():
    t0 = 1_700_000_000_000                                                                     # epoch ms
    wsl = [{"wall_ns": str((t0 + 1000 * k) * 1_000_000), "cpu_busy_pct": str(10.0 * k), "psi_cpu_some_pct": ""} for k in range(11)]
    win = [{"t_wall_ns": str((t0 + 1000 * k) * 1_000_000), "disk_bytes_s": str(5e6 * k), "net_bytes_s": str(2e6), "cpu_utility_pct": str(k),
            "mem_available_mb": str(3000 - k)} for k in range(11)]
    s = hs.summarize_window(wsl, win, t0 + 2000, t0 + 6000)                                    # rows k = 2..6
    assert s["wsl"]["cpu_busy_pct"] == {"n": 5, "mean": 40.0, "p95": 60.0, "min": 20.0, "max": 60.0}
    assert s["wsl"]["psi_cpu_some_pct"] == {"n": 0}                                            # blank cells are not zeros
    assert s["windows"]["disk_mb_s"]["mean"] == pytest.approx(20.0) and s["windows"]["disk_mb_s"]["max"] == pytest.approx(30.0)
    assert s["windows"]["net_mb_s"]["mean"] == pytest.approx(2.0) and s["windows"]["mem_available_mb"]["min"] == 2994
    assert "disk_bytes_s" not in s["windows"] and s["window_ms"] == [t0 + 2000, t0 + 6000]
    assert hs.summarize_window([], [], 0.0, 1.0)["wsl"]["cpu_busy_pct"] == {"n": 0}


def test_summarize_window_on_a_real_session(tmp_path):
    man, out, _ = sample(tmp_path, seconds=30)
    lo_ms, hi_ms = WALL0 / 1e6 + 10_000, WALL0 / 1e6 + 20_000
    s = hs.summarize_window(rows(out / "wsl_samples.csv"), rows(out / "win_counters.csv"), lo_ms, hi_ms)
    assert 9 <= s["wsl"]["cpu_busy_pct"]["n"] <= 11 and s["wsl"]["cpu_busy_pct"]["mean"] == pytest.approx(50.0)
    assert s["wsl"]["net_if_rx_mb_s"]["mean"] == pytest.approx(1.0) and 8 <= s["windows"]["net_mb_s"]["n"] <= 10


# ====================================================================================== command line
def test_dry_run_prints_the_plan_and_creates_nothing(tmp_path, capsys):
    out = tmp_path / "never"
    rc = hs.main(["sample", "--out-dir", str(out), "--max-seconds", "60"])
    text = capsys.readouterr().out
    assert rc == 0 and not out.exists() and "dry run: nothing executed or written" in text
    assert "PLAN typeperf.exe" in text and "-sc 62" in text and "stat" in text


def test_cli_refuses_an_existing_directory_and_a_fractional_interval(tmp_path, capsys):
    (tmp_path / "exists").mkdir()
    assert hs.main(["sample", "--out-dir", str(tmp_path / "exists"), "--execute"]) == 2
    assert "refusing to overwrite" in capsys.readouterr().out
    assert hs.main(["sample", "--out-dir", str(tmp_path / "new"), "--interval", "0.5", "--execute"]) == 2
    assert not (tmp_path / "new").exists()


def test_cli_execute_runs_one_tick_when_the_stop_file_already_exists(tmp_path, capsys):
    tree = ProcTree(tmp_path / "proc")
    stop = tmp_path / "stop"
    stop.write_text("stop\n", encoding="utf-8")
    rc = hs.main(["sample", "--out-dir", str(tmp_path / "o"), "--proc-root", str(tree.root), "--stop-file", str(stop), "--execute", "--no-windows"])
    out = capsys.readouterr().out
    man = json.loads((tmp_path / "o" / "host_manifest.json").read_text(encoding="utf-8"))
    assert rc == 0 and man["ticks"] == 1 and man["stop_reason"] == "stop_file" and "HOST COMPLETE_WITH_WARNINGS" in out


def test_cli_execute_uses_the_windows_factory_it_is_given(tmp_path):
    tree = ProcTree(tmp_path / "proc")
    stop = tmp_path / "stop"
    stop.write_text("stop\n", encoding="utf-8")
    seen = {}

    class Quiet:
        argv, stderr_tail, eof, returncode = None, b"", False, None

        def drain(self):
            return []

        def stop(self):
            seen["stopped"] = True

    def factory(argv):
        seen["argv"] = argv
        return Quiet()
    rc = hs.main(["sample", "--out-dir", str(tmp_path / "o"), "--proc-root", str(tree.root), "--stop-file", str(stop), "--execute"], win_factory=factory)
    assert seen["argv"][0] == "typeperf.exe" and seen["stopped"] is True and rc == 1            # no header, no rows: the record is incomplete


def test_summarize_subcommand_prints_json(tmp_path, capsys):
    man, out, _ = sample(tmp_path, seconds=15)
    rc = hs.main(["summarize", "--samples", str(out / "wsl_samples.csv"), "--win", str(out / "win_counters.csv"),
                  "--from-ms", str(WALL0 / 1e6 + 5_000), "--to-ms", str(WALL0 / 1e6 + 12_000)])
    d = json.loads(capsys.readouterr().out)
    assert rc == 0 and d["wsl"]["cpu_busy_pct"]["n"] >= 6 and d["windows"]["cpu_utility_pct"]["n"] >= 3


def test_the_tool_touches_no_container_runtime():
    """The only program the tool may start is typeperf.exe: nothing in its code (docstring aside) names a container runtime or a Fabric tool."""
    tree = ast.parse(Path(hs.__file__).read_text(encoding="utf-8"))
    body = tree.body[1:] if isinstance(tree.body[0], ast.Expr) and isinstance(tree.body[0].value, ast.Constant) else tree.body
    code = "\n".join(ast.unparse(n) for n in body).lower()
    for forbidden in ("docker", "caliper", "osnadmin", "configtx", "'peer'", '"peer"', "kubectl", "wsl.exe", "powershell"):
        assert forbidden not in code, forbidden
    assert "typeperf.exe" in code
