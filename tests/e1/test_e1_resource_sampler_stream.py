"""Resource sampler 0.2 --source api (E0-0 remediation F3b, 2026-11-09). Synthetic Engine API documents
with the field layout seen on this machine (engine 29.9.3, API 1.56: read/preread RFC 3339 ns, cpu_stats
with system_cpu_usage and online_cpus, memory_stats.stats.inactive_file, networks.eth0, blkio op
read/write). Not Docker output."""
import datetime
import json
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_resource_sampler as rs  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

A, B = "orderer.example.com", "peer0.org1.example.com"
T0, M0 = 1_794_241_669_000_000_000, 50_000_000_000
S = 1_000_000_000


def iso(ns: int | None) -> str:
    if ns is None:
        return "0001-01-01T00:00:00Z"
    d = datetime.datetime.fromtimestamp(ns // S, tz=datetime.timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns % S:09d}Z"


def doc(name, read_ns, pre_ns, k, cid="a" * 64, nets=True):
    def cpu(j):
        return {"cpu_usage": {"total_usage": 10**9 + j * 50_000_000}, "system_cpu_usage": 10**12 + j * 24 * S,
                "online_cpus": 24}
    d = {"name": "/" + name, "id": cid, "read": iso(read_ns), "preread": iso(pre_ns), "cpu_stats": cpu(k),
         "precpu_stats": cpu(k - 1) if pre_ns else {"cpu_usage": {"total_usage": 0}},
         "memory_stats": {"usage": 60_000_000, "limit": 8_000_000_000, "stats": {"inactive_file": 10_000_000}},
         "blkio_stats": {"io_service_bytes_recursive": [{"major": 8, "minor": 0, "op": "read", "value": 4096},
                                                        {"major": 8, "minor": 0, "op": "write", "value": 8192}]},
         "pids_stats": {"current": 17}}
    if nets:
        d["networks"] = {"eth0": {"rx_bytes": 1000 + k, "tx_bytes": 2000 + k}}
    return d


def stream(name, n=10, *, skip=(), wall_shift=None, delay_ms=None, attempt=1, start_k=0, cid="a" * 64, nets_off=()):
    """Records for one container: sample k read at T0 + k s (+ wall shifts), received 2 ms later (monotonic
    M0 + k s + 2 ms). wall_shift[k] = ns added to the wall clock from sample k on (a clock step)."""
    out, prev_read, shift, line_no = [], None, 0, 0
    for k in range(start_k, start_k + n):
        shift += (wall_shift or {}).get(k, 0)
        read = T0 + k * S + shift
        if k in skip:
            prev_read = None if False else prev_read  # daemon read but never received: chain kept by the daemon
            continue
        extra = (delay_ms or {}).get(k, 0) * 1_000_000
        line_no += 1
        d = doc(name, read, prev_read, k, cid=cid, nets=k not in nets_off)
        out.append({"container": name, "event": "line", "attempt": attempt, "wall_ns": read + 2_000_000 + extra,
                    "mono_ns": M0 + k * S + 2_000_000 + extra, "line": json.dumps(d).encode(), "raw_line": line_no})
        prev_read = read
    return out


def analyze(records, containers=(A, B), n=10, **kw):
    an = rs.tc.Anomalies(rs.COMPLETENESS, rs.CONSISTENCY, rs.WARNINGS)
    samples, per = rs.analyze_stream(records, list(containers), an, start_mono_ns=M0, stop_mono_ns=M0 + n * S, **kw)
    return an, samples, per


def kinds(an):
    c = an.counts()
    return {**c["error"], **c["warning"]}


# ------------------------------------------------------------------ parsing
def test_rfc3339_and_document_parsing():
    assert rs.rfc3339_ns("2026-11-09T16:27:49.2014376Z") % S == 201_437_600
    assert rs.rfc3339_ns("2026-11-09T16:27:49Z") % S == 0 and rs.rfc3339_ns("0001-01-01T00:00:00Z") is None
    for bad in ("2026-11-09 16:27:49Z", "2026-11-09T16:27:49.1234567890Z", "2026-11-09T16:27:49+05:30"):
        with pytest.raises(ValueError):
            rs.rfc3339_ns(bad)
    p = rs.parse_api_stats(doc(A, T0 + S, T0, 1))
    assert p["cpu_percent"] == pytest.approx(5.0)            # 0.05 s CPU / 24 s system x 24 CPUs x 100
    assert p["mem_used_bytes"] == 50_000_000 and p["blk_read_bytes"] == 4096 and p["blk_write_bytes"] == 8192
    assert rs.parse_api_stats(doc(A, T0, None, 0))["cpu_percent"] is None   # first document of a stream
    for broken in ({"read": "x"}, [], {**doc(A, T0, None, 0), "read": "0001-01-01T00:00:00Z"}):
        with pytest.raises(ValueError):
            rs.parse_api_stats(broken)


# ------------------------------------------------------------------ positive
def test_clean_streams_are_complete_with_full_coverage():
    an, samples, per = analyze(stream(A) + stream(B))
    assert an.status() == "COMPLETE"
    assert per[A]["samples"] == 10 and per[A]["coverage"] == 1.0 and per[A]["recv_gap_ms_max"] == 1000.0
    assert samples[1]["cpu_percent"] == pytest.approx(5.0) and samples[0]["cpu_percent"] is None
    assert samples[0]["delay_ms"] == 2.0


def test_clock_step_is_flagged_but_is_not_a_gap():
    """F1 shape: the wall clock (and the daemon's read times) step +690 ms; monotonic receipts stay 1 s apart."""
    an, samples, _ = analyze(stream(A, wall_shift={5: 690_000_000}), containers=(A,))
    assert kinds(an) == {"host_clock_step": 1} and an.status() == "INCONSISTENT"
    assert samples[5]["read_gap_ms"] == 1690.0 and samples[5]["recv_gap_ms"] == 1000.0
    assert samples[5]["daemon_gap_ms"] == 1000.0


def test_reconnected_stream_first_document_needs_no_cpu():
    recs = stream(A, n=5) + stream(A, n=5, start_k=5, attempt=2)
    recs[5]["line"] = json.dumps(doc(A, T0 + 5 * S, None, 5)).encode()  # new stream: zero preread
    an, samples, per = analyze(recs, containers=(A,))
    assert an.status() == "COMPLETE" and per[A]["streams_opened"] == 2 and samples[5]["cpu_percent"] is None


# ------------------------------------------------------------------ negative
def test_dropped_sample_is_a_gap():
    an, _, per = analyze(stream(A, skip={5}) + stream(B))
    assert an.status() == "INCOMPLETE" and kinds(an) == {"sample_gap": 1} and per[A]["samples"] == 9
    assert "about 1 sample(s) missing" in an.of("sample_gap")[0].detail


def test_late_start_and_early_end_are_gaps():
    an, _, _ = analyze(stream(A, skip={0, 1, 2}) + stream(B, skip={8, 9}))
    assert [a.detail.split()[0:2] for a in an.of("sample_gap")] == [["first", "sample"], ["last", "sample"]]


def test_delayed_sample_is_a_warning_only():
    """Received 800 ms late: the receipt gap is 1.8 s, but the daemon read it on time, so nothing is missing."""
    an, samples, _ = analyze(stream(A, delay_ms={4: 800}), containers=(A,))
    assert kinds(an) == {"sample_delayed": 1} and an.status() == "COMPLETE_WITH_WARNINGS"
    assert samples[4]["recv_gap_ms"] == 1800.0 and samples[4]["daemon_gap_ms"] == 1000.0


def test_duplicate_and_out_of_order_daemon_times():
    recs = stream(A)
    recs[3]["line"] = recs[2]["line"]
    an, _, _ = analyze(recs, containers=(A,))
    assert kinds(an)["duplicate_timestamp"] == 1
    recs2 = stream(A)
    recs2[4]["line"] = json.dumps(doc(A, T0 + 1 * S + 1, T0 + 3 * S, 4)).encode()
    an2, _, _ = analyze(recs2, containers=(A,))
    # one backward read time: out of order, 3 s older than its own receipt (delayed), and the following
    # interval (k 4 -> 5) spans 4 s on the daemon scale (gap)
    assert kinds(an2) == {"timestamp_order": 1, "sample_delayed": 1, "sample_gap": 1}
    assert an2.status() == "INCOMPLETE"


def test_malformed_mismatched_replaced_and_missing_values():
    recs = stream(A, nets_off={6}) + stream(B)
    recs[2]["line"] = b"{not json"
    recs[3]["line"] = json.dumps(doc(B, T0 + 3 * S, T0 + 2 * S, 3)).encode()
    recs[7]["line"] = json.dumps(doc(A, T0 + 7 * S, T0 + 6 * S, 7, cid="b" * 64)).encode()
    an, _, _ = analyze(recs)
    k = kinds(an)
    assert k["malformed_line"] == 1 and k["container_mismatch"] == 1
    assert k["container_replaced"] == 2                                        # a -> b at k = 7, b -> a at k = 8
    assert k["value_unavailable"] == 2 and an.status() == "INCOMPLETE"         # net rx and tx absent at k = 6
    bad = an.of("malformed_line")[0]
    assert bad.source == f"raw/stats_{A}.jsonl:3" and bad.raw == "{not json"


def test_stream_failure_and_silent_container():
    recs = stream(A) + [{"container": B, "event": "error", "attempt": 1, "wall_ns": T0, "mono_ns": M0,
                         "detail": "open failed: OSError('HTTP 404: No such container')"}]
    an, _, per = analyze(recs)
    assert kinds(an) == {"stream_failed": 1, "container_missing": 1} and per[B]["samples"] == 0


# ------------------------------------------------------------------ threaded collector and CLI (fake opener)
class FakeOpener:
    """Yields n documents per open, then blocks until closed. fail = containers whose open raises."""

    def __init__(self, n=3, fail=()):
        self.n, self.fail, self.opened = n, set(fail), []

    def __call__(self, c):
        if c in self.fail:
            raise OSError(f"HTTP 404: No such container: {c}")
        closed = threading.Event()
        self.opened.append(c)

        def lines():
            prev = None
            for k in range(self.n):
                now = time.time_ns()
                yield json.dumps(doc(c, now, prev, k)).encode() + b"\n"
                prev = now
            closed.wait(10)
        return lines(), closed.set


def fake_runner(argv):
    if argv[:2] == ["docker", "inspect"]:
        return 0, json.dumps([sf.inspect_doc(n, cid=f"{i}" * 64) for i, n in enumerate(argv[2:], 1)]).encode(), b""
    return 127, b"", b"unexpected"


def test_run_stream_end_to_end_keeps_raw_bytes(tmp_path):
    op = FakeOpener(n=3)
    m = rs.run_stream([A, B], tmp_path / "r", fake_runner, opener=op, max_ticks=3)
    assert m["status"] == "COMPLETE" and m["lines_received"] == 6 and m["reader_threads_not_stopped"] == []
    raw = (tmp_path / "r" / "raw" / f"stats_{A}.jsonl").read_bytes().splitlines()
    rec = rs.tc.read_csv(tmp_path / "r" / "resource_receipts.csv")
    assert len(raw) == 3 and len(rec) == 6
    assert {r["sha256"] for r in rec if r["container"] == A} == {rs.tc.sha256_bytes(x) for x in raw}
    assert m["snapshots"]["start"][A]["running"] is True and m["source"] == "docker_engine_api"


def test_run_stream_open_failure_and_guards(tmp_path):
    m = rs.run_stream([A, B], tmp_path / "f", fake_runner, opener=FakeOpener(n=2, fail={B}), duration_s=0.3)
    assert m["status"] == "INCOMPLETE"
    assert m["anomalies"]["error"]["container_missing"] == 1 and m["anomalies"]["error"]["stream_failed"] >= 1
    with pytest.raises(ValueError):
        rs.run_stream([A], tmp_path / "g", fake_runner, opener=FakeOpener(), interval_s=2.0, max_ticks=1)
    with pytest.raises(ValueError):
        rs.run_stream([A], tmp_path / "h", fake_runner, opener=FakeOpener())       # no stop condition


def test_cli_default_source_is_api(tmp_path, capsys):
    assert rs.main(["sample", "--container", A, "--out-dir", str(tmp_path / "dry"), "--max-ticks", "1"]) == 0
    out = capsys.readouterr().out
    assert f"PLAN GET unix:/var/run/docker.sock /containers/{A}/stats?stream=1" in out and "dry run" in out
    code = rs.main(["sample", "--container", A, "--out-dir", str(tmp_path / "ok"), "--max-ticks", "2", "--execute"],
                   runner=fake_runner, opener=FakeOpener(n=2))
    assert code == 0
    assert rs.main(["sample", "--container", A, "--out-dir", str(tmp_path / "bad"), "--max-ticks", "1",
                    "--interval", "0.5", "--execute"], runner=fake_runner, opener=FakeOpener()) == 2
