"""Resource sampler: fake docker stats/inspect output and synthetic typeperf files. Not Docker output."""
import datetime
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_resource_sampler as rs  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

NAMES = ["orderer1.example.com", "peer0.org1.example.com"]


class FakeDocker:
    """stats_by_tick[k] = list of lines or (exit, stderr); inspect[label] = list of docs or (exit, stderr)."""

    def __init__(self, clock, stats=None, inspect=None, latency_ms=150):
        self.clock, self.stats, self.inspect, self.latency = clock, dict(stats or {}), dict(inspect or {}), latency_ms
        self.n_stats, self.n_inspect = 0, 0

    def default_lines(self):
        return [sf.stats_line(n, cid=f"{i}" * 64) for i, n in enumerate(NAMES, start=1)] + \
            [sf.stats_line("payment_pg", cid="9" * 64)]

    def __call__(self, argv):
        self.clock.step(int(self.latency * 1e6))
        if argv[:2] == ["docker", "stats"]:
            k = self.n_stats
            self.n_stats += 1
            v = self.stats.get(k, self.default_lines())
            if isinstance(v, tuple):
                return v[0], b"", v[1]
            return 0, ("\n".join(v) + "\n").encode(), b""
        if argv[:2] == ["docker", "inspect"]:
            label = "start" if self.n_inspect == 0 else "end"
            self.n_inspect += 1
            v = self.inspect.get(label, [sf.inspect_doc(n, cid=f"{i}" * 64) for i, n in enumerate(NAMES, start=1)])
            if isinstance(v, tuple):
                return v[0], b"", v[1]
            return 0, json.dumps(v).encode(), b""
        return 127, b"", b"unexpected"


def sample(tmp_path, docker, clock, name="s", **kw):
    kw.setdefault("max_ticks", 4)
    return rs.run_sample(NAMES, tmp_path / name, docker, wall_ns=clock.wall_ns, mono_ns=clock.mono_ns,
                         sleep=clock.sleep, **kw)


def kinds(m):
    return {**m["anomalies"]["error"], **m["anomalies"]["warning"]}


# ------------------------------------------------------------------ parsing
def test_units_binary_and_decimal_are_kept_apart():
    assert rs.size_bytes("105.3MiB") == pytest.approx(105.3 * 2**20)
    assert rs.size_bytes("1.2MB") == pytest.approx(1.2e6) and rs.size_bytes("845kB") == 845_000
    assert rs.size_bytes("0B") == 0 and rs.size_bytes("--") is None
    assert rs.size_pair("1.2MB / 845kB") == (pytest.approx(1.2e6), 845_000)
    assert rs.percent("12.50%") == 12.5 and rs.percent("--") is None and rs.pids("21") == 21
    for bad in ("12 parsecs", "1.2", "MiB", "1,2MB"):
        with pytest.raises(ValueError):
            rs.size_bytes(bad)
    with pytest.raises(ValueError):
        rs.size_pair("1MB")


def test_stats_line_parsing_and_rejection():
    row = rs.parse_stats_line(sf.stats_line("peer0", mem="7.637GiB / 7.637GiB", cpu="250.00%"))
    assert row["cpu_percent"] == 250.0 and row["mem_used_bytes"] == row["mem_limit_bytes"]
    assert rs.parse_stats_line(sf.stats_line("p", cpu="--", net="--"))["unavailable"] == \
        ["cpu_percent", "net_rx_bytes", "net_tx_bytes"]
    for bad in ("{not json", json.dumps({"Name": "x"}), json.dumps([1, 2]),
                sf.stats_line("p", mem="12 parsecs / 1GiB")):
        with pytest.raises(ValueError):
            rs.parse_stats_line(bad)


# ------------------------------------------------------------------ sampling
def test_clean_sampling_keeps_listed_containers_and_counts_others(tmp_path):
    clock = sf.Clock()
    m = sample(tmp_path, FakeDocker(clock), clock)
    assert m["status"] == "COMPLETE" and m["successful_ticks"] == 4 and m["missed_tick_slots"] == 0
    assert m["ignored_containers"] == ["payment_pg"]
    assert m["per_container"]["peer0.org1.example.com"]["coverage"] == 1.0
    rows = rs.tc.read_csv(tmp_path / "s" / "resource_samples.csv")
    assert len(rows) == 8 and {r["container"] for r in rows} == set(NAMES)
    ticks = rs.tc.read_csv(tmp_path / "s" / "resource_ticks.csv")
    assert [int(t["ignored_rows"]) for t in ticks] == [1] * 4
    t0 = [int(t["t0_mono_ns"]) for t in ticks]
    assert [b - a for a, b in zip(t0, t0[1:])] == [1_000_000_000] * 3
    assert b"payment_pg" in (tmp_path / "s" / "raw" / "tick_000000.stdout").read_bytes()  # raw kept whole


def test_missing_malformed_unavailable_and_failed_ticks(tmp_path):
    clock = sf.Clock()
    d = FakeDocker(clock)
    good = d.default_lines()
    stats = {1: good[:1],                                     # peer missing
             2: (1, b"Cannot connect to the Docker daemon"),  # whole tick failed
             3: [good[0], "{garbage", sf.stats_line(NAMES[1], cid="2" * 64, cpu="--")]}
    d.stats = stats
    m = sample(tmp_path, d, clock)
    k = kinds(m)
    assert m["status"] == "INCOMPLETE"
    assert k["container_missing"] == 1 and k["stats_failed"] == 1 and k["malformed_line"] == 1 \
        and k["value_unavailable"] == 1
    assert m["per_container"]["peer0.org1.example.com"]["ticks_present"] == 2
    bad = [a for a in rs.tc.read_csv(tmp_path / "s" / "resource_anomalies.csv") if a["kind"] == "malformed_line"]
    assert bad[0]["raw"] == "{garbage" and bad[0]["source"] == "raw/tick_000003.stdout:2"


def test_duplicate_and_replaced_containers_are_inconsistent(tmp_path):
    clock = sf.Clock()
    d = FakeDocker(clock)
    good = d.default_lines()
    d.stats = {1: good + [good[0]], 2: [sf.stats_line(NAMES[0], cid="7" * 64), good[1]]}
    m = sample(tmp_path, d, clock)
    assert m["status"] == "INCONSISTENT" and kinds(m)["duplicate_container"] == 1 and \
        kinds(m)["container_replaced"] >= 1


@pytest.mark.parametrize("end_doc,kind", [
    (sf.inspect_doc(NAMES[0], cid="1" * 64, started="2023-11-14T22:30:00Z", restarts=1), "container_restarted"),
    (sf.inspect_doc(NAMES[0], cid="1" * 64, oom=True), "oom_killed"),
    (sf.inspect_doc(NAMES[0], cid="1" * 64, status="exited"), "container_not_running")])
def test_v6_restart_oom_and_exit_detected(tmp_path, end_doc, kind):
    clock = sf.Clock()
    d = FakeDocker(clock, inspect={"end": [end_doc, sf.inspect_doc(NAMES[1], cid="2" * 64)]})
    m = sample(tmp_path, d, clock)
    assert m["status"] == "INCONSISTENT" and kinds(m) == {kind: 1}


def test_inspect_failure_and_no_successful_tick(tmp_path):
    clock = sf.Clock()
    d = FakeDocker(clock, inspect={"start": (1, b"Error: No such object")},
                   stats={i: (1, b"daemon down") for i in range(4)})
    m = sample(tmp_path, d, clock)
    assert m["status"] == "INCOMPLETE" and kinds(m)["inspect_failed"] == 1 and kinds(m)["no_successful_tick"] == 1


def test_overrun_and_host_clock_step(tmp_path):
    clock = sf.Clock()
    # 2.5 s per tick: ticks start at slots 0 and 2 (2.5 s); the second ends at 5.0 s, so slots 1, 3 and 4 pass unsampled
    m = sample(tmp_path, FakeDocker(clock, latency_ms=2500), clock, max_ticks=2)
    assert kinds(m)["interval_overrun"] == 2 and m["missed_tick_slots"] == 3 and m["status"] == "COMPLETE_WITH_WARNINGS"

    class Jumpy(FakeDocker):
        def __call__(self, argv):
            out = super().__call__(argv)
            self.clock.jump(-3_000_000)
            return out
    clock2 = sf.Clock()
    m2 = sample(tmp_path, Jumpy(clock2), clock2, name="s2", max_ticks=2)
    assert m2["status"] == "INCONSISTENT" and kinds(m2)["host_clock_step"] >= 2


# ------------------------------------------------------------------ typeperf
TP = ('"(PDH-CSV 4.0) (India Standard Time)(-330)","\\\\HOST\\Processor(_Total)\\% Processor Time",'
      '"\\\\HOST\\Memory\\Available MBytes"\n'
      '"11/09/2026 17:40:01.123","12.500000","2500.000000"\n'
      '"11/09/2026 17:40:02.124","14.000000","2490.000000"\n')


def test_typeperf_converts_with_the_pdh_bias(tmp_path):
    m = rs.parse_typeperf(TP, tmp_path / "t", source="tp.csv")
    assert m["status"] == "COMPLETE" and m["utc_bias_minutes"] == -330 and m["utc_bias_source"] == "PDH header"
    rows = rs.tc.read_csv(tmp_path / "t" / "host_counters.csv")
    assert len(rows) == 4
    # 17:40:01.123 IST = 12:10:01.123 UTC
    want = datetime.datetime(2026, 11, 9, 12, 10, 1, 123000, tzinfo=datetime.timezone.utc)
    assert int(rows[0]["t_utc_ms"]) == int(want.timestamp() * 1000)
    override = rs.parse_typeperf(TP, tmp_path / "t2", utc_offset_min=330)
    assert override["utc_bias_minutes"] == -330


def test_typeperf_problems(tmp_path):
    no_bias = TP.replace("(India Standard Time)(-330)", "")
    assert rs.parse_typeperf(no_bias, tmp_path / "a")["status"] == "INCOMPLETE"
    blank = TP + '"11/09/2026 17:40:03.120"," ","2480.0"\n'
    assert rs.parse_typeperf(blank, tmp_path / "b")["anomalies"]["error"] == {"value_missing": 1}
    dup = TP + '"11/09/2026 17:40:02.124","1.0","2.0"\n'
    assert rs.parse_typeperf(dup, tmp_path / "c")["status"] == "INCONSISTENT"
    back = TP + '"11/09/2026 17:39:59.000","1.0","2.0"\n'
    assert rs.parse_typeperf(back, tmp_path / "d")["anomalies"]["error"] == {"timestamp_order": 1}
    short = TP + '"11/09/2026 17:40:04.000","1.0"\n"not a date","1","2"\n'
    assert rs.parse_typeperf(short, tmp_path / "e")["anomalies"]["error"] == {"malformed_line": 2}


def test_cli_guards(tmp_path, capsys):
    base = ["sample", "--container", NAMES[0]]
    assert rs.main(base + ["--out-dir", str(tmp_path / "dry"), "--max-ticks", "1"]) == 0
    assert "dry run" in capsys.readouterr().out and not (tmp_path / "dry").exists()
    assert rs.main(base + ["--out-dir", str(tmp_path / "x"), "--execute"]) == 2            # no stop condition
    assert rs.main(["sample", "--container", "bad name", "--out-dir", str(tmp_path / "y"), "--max-ticks", "1",
                    "--execute"]) == 2
    clock = sf.Clock()
    # 0.2 (E0-0 remediation F3b): the default source is now the Engine API stream; this test covers the tick path
    code = rs.main(base + ["--container", NAMES[1], "--out-dir", str(tmp_path / "ok"), "--max-ticks", "1",
                           "--interval", "0.001", "--source", "cli", "--execute"], runner=FakeDocker(clock))
    assert code == 0
    (tmp_path / "tp.csv").write_bytes(TP.encode("utf-16"))
    assert rs.main(["typeperf", "--file", str(tmp_path / "tp.csv"), "--out-dir", str(tmp_path / "tp")]) == 0
