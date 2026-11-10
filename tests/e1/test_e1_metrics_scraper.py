"""Metrics scraper: synthetic Prometheus text and a fake HTTP fetcher with a fake clock. Not Fabric output."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_metrics_scraper as ms  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

TARGETS = [{"name": "orderer1", "role": "orderer", "url": "http://o1:9443/metrics"},
           {"name": "peer0org1", "role": "peer", "url": "http://p1:9444/metrics"}]


class FakeFetch:
    """Serves synthetic metrics; k advances once per orderer scrape. overrides[(url, call)] = (status, body) or an
    exception instance; latency_ms is added to the fake clock per call."""

    def __init__(self, clock, overrides=None, latency_ms=3, leader_changes=0, reset_at=None):
        self.clock, self.overrides, self.latency = clock, dict(overrides or {}), latency_ms
        self.calls = {}
        self.leader_changes, self.reset_at = leader_changes, reset_at

    def __call__(self, url, timeout):
        n = self.calls.get(url, 0)
        self.calls[url] = n + 1
        self.clock.step(int(self.latency * 1e6))
        ov = self.overrides.get((url, n))
        if isinstance(ov, Exception):
            raise ov
        if ov is not None:
            return ov
        k = n if self.reset_at is None or n < self.reset_at else n - self.reset_at
        if "o1" in url:
            return 200, sf.orderer_metrics(k, leader_changes=self.leader_changes if n > 0 else 0).encode()
        return 200, sf.peer_metrics(k).encode()


def scrape(tmp_path, phase, fetch, clock, name=None, **kw):
    return ms.run_scrape(TARGETS, phase, tmp_path / (name or phase), fetch=fetch, wall_ns=clock.wall_ns,
                         mono_ns=clock.mono_ns, sleep=clock.sleep, **kw)


def kinds(m):
    return {**m["anomalies"]["error"], **m["anomalies"]["warning"]}


# ------------------------------------------------------------------ parser
def test_parser_reads_types_labels_escapes_and_special_values():
    text = ('# HELP h synthetic\n# TYPE h histogram\nh_bucket{le="+Inf",channel="c"} 3\nh_sum{channel="c"} 0.5\n'
            'h_count{channel="c"} 3\n# TYPE g gauge\ng{a="x\\"y",b="1\\\\2",c="l\\nm"} NaN\nu -Inf\nv 1e-3 1700\n')
    samples, types, problems = ms.parse_exposition(text)
    assert types == {"h": "histogram", "g": "gauge"}
    by = {s["metric"]: s for s in samples}
    assert by["h_bucket"]["family"] == "h" and by["h_bucket"]["type"] == "histogram"
    assert by["h_bucket"]["labels"] == (("channel", "c"), ("le", "+Inf"))
    assert by["g"]["labels"] == (("a", 'x"y'), ("b", "1\\2"), ("c", "l\nm"))
    assert ms.labels_text(by["g"]["labels"]) == 'a="x\\"y",b="1\\\\2",c="l\\nm"'
    assert by["u"]["value"] == float("-inf") and by["u"]["type"] == "untyped"
    assert [p[0] for p in problems] == ["exposition_timestamp"]


@pytest.mark.parametrize("line", ['m{a="1" 2', 'm{a=1} 2', "m 1,5", "m", "m 1 x", '9m 1', 'm{a="1",a="2"} 1',
                                  'm{a="\\q"} 1', "m 1_000"])
def test_parser_rejects_malformed_lines(line):
    samples, _, problems = ms.parse_exposition(line + "\n")
    assert samples == [] and [p[0] for p in problems] == ["malformed_line"]


def test_parser_flags_duplicates_and_type_conflicts():
    samples, _, problems = ms.parse_exposition("# TYPE m counter\n# TYPE m gauge\nm 1\nm 2\n")
    assert [s["duplicate"] for s in samples] == [False, True]
    assert sorted(p[0] for p in problems) == ["duplicate_series", "type_conflict"]


# ------------------------------------------------------------------ scrape phases
def test_before_during_after_and_summary(tmp_path):
    clock = sf.Clock()
    f = FakeFetch(clock)
    mb = scrape(tmp_path, "before", f, clock)
    md = scrape(tmp_path, "during", f, clock, max_rounds=5, interval_s=1.0)
    ma = scrape(tmp_path, "after", f, clock)
    assert mb["status"] == md["status"] == ma["status"] == "COMPLETE"
    assert md["rounds"] == 5 and md["missed_round_slots"] == 0
    rows = ms.tc.read_csv(tmp_path / "during" / "scrapes.csv")
    t0 = [int(r["t0_mono_ns"]) for r in rows if r["target"] == "orderer1"]
    assert [b - a for a, b in zip(t0, t0[1:])] == [1_000_000_000] * 4          # paced on the monotonic clock
    assert all((tmp_path / "during" / r["raw_file"]).exists() for r in rows)
    s = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "sum", during=tmp_path / "during",
                     channel=sf.CHANNEL)
    assert s["status"] == "COMPLETE"
    o = s["derived"]["orderer1"]
    assert o["leader_changes_delta"] == 0 and o["fill_duration_count_delta"] == 6
    assert o["fill_duration_mean_s"] == pytest.approx(0.05) and o["is_leader_after"] == 1.0


def test_fresh_channel_absent_before_counts_as_zero_only_when_declared(tmp_path):
    clock = sf.Clock()
    empty = (200, b"# TYPE go_goroutines gauge\ngo_goroutines 7\n")
    f = FakeFetch(clock, overrides={("http://o1:9443/metrics", 0): empty, ("http://p1:9444/metrics", 0): empty})
    mb = scrape(tmp_path, "before", f, clock)
    assert mb["status"] == "COMPLETE_WITH_WARNINGS" and "expected_metric_absent_early" in kinds(mb)
    scrape(tmp_path, "after", f, clock)
    s1 = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s1", channel=sf.CHANNEL)
    assert s1["status"] == "INCOMPLETE" and "delta_unavailable" in kinds(s1)
    s2 = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s2", channel=sf.CHANNEL, fresh_channel=True)
    assert s2["status"] == "COMPLETE_WITH_WARNINGS"     # carries the before-phase warning forward
    rows = ms.tc.read_csv(tmp_path / "s2" / "metric_deltas.csv")
    assert {r["before_source"] for r in rows} == {"assumed_zero"} and s2["derived"]["orderer1"]["leader_changes_delta"] == 0


# ------------------------------------------------------------------ negative tests
def test_connection_errors_http_errors_and_malformed_bodies(tmp_path):
    clock = sf.Clock()
    f = FakeFetch(clock, overrides={("http://o1:9443/metrics", 1): ConnectionRefusedError("refused"),
                                    ("http://p1:9444/metrics", 2): (503, b"busy"),
                                    ("http://o1:9443/metrics", 3): (200, b"\xff\xfe"),
                                    ("http://p1:9444/metrics", 3): (200, b"ledger_blockchain_height{ 3\n")})
    m = scrape(tmp_path, "during", f, clock, max_rounds=4)
    k = kinds(m)
    assert m["status"] == "INCOMPLETE"
    assert k["scrape_failed"] == 1 and k["http_status"] == 1 and k["decode_error"] == 1 and k["malformed_line"] == 1
    rows = ms.tc.read_csv(tmp_path / "during" / "scrapes.csv")
    assert len(rows) == 8                                     # every attempt has a row, failed ones included
    assert (tmp_path / "during" / "raw" / "during_000002_peer0org1.prom").read_bytes() == b"busy"
    bad = [a for a in ms.tc.read_csv(tmp_path / "during" / "metrics_anomalies.csv") if a["kind"] == "malformed_line"]
    assert bad[0]["raw"] == "ledger_blockchain_height{ 3" and bad[0]["source"].endswith(".prom:1")


def test_target_never_reachable(tmp_path):
    clock = sf.Clock()
    f = FakeFetch(clock, overrides={("http://o1:9443/metrics", 0): TimeoutError("timed out")})
    m = scrape(tmp_path, "after", f, clock)
    assert m["status"] == "INCOMPLETE" and kinds(m)["no_successful_scrape"] == 1


def test_duplicate_series_and_counter_reset_are_inconsistent(tmp_path):
    clock = sf.Clock()
    dup = sf.orderer_metrics(3) + 'consensus_etcdraft_leader_changes{channel="e1-r001"} 0\n'
    f = FakeFetch(clock, overrides={("http://o1:9443/metrics", 0): (200, dup.encode())})
    m = scrape(tmp_path, "before", f, clock)
    assert m["status"] == "INCONSISTENT" and kinds(m) == {"duplicate_series": 1}
    rows = ms.tc.read_csv(tmp_path / "before" / "samples.csv")
    assert sum(r["duplicate"] == "true" for r in rows) == 1    # kept and labelled, not dropped
    clock2 = sf.Clock()
    m2 = scrape(tmp_path, "during", FakeFetch(clock2, reset_at=3), clock2, name="d2", max_rounds=5)
    assert m2["status"] == "INCONSISTENT" and kinds(m2)["counter_reset"] >= 1


def test_overrun_is_counted_never_backfilled_and_clock_step_detected(tmp_path):
    clock = sf.Clock()
    # each round takes 2.6 s (2 targets x 1.3 s): rounds start at slots 0, 2 and 5; slots 1, 3, 4 and 6 pass unscraped
    m = scrape(tmp_path, "during", FakeFetch(clock, latency_ms=1300), clock, max_rounds=3)
    assert kinds(m)["interval_overrun"] == 3 and m["missed_round_slots"] == 4 and m["rounds"] == 3
    assert [int(r["round"]) for r in ms.tc.read_csv(tmp_path / "during" / "scrapes.csv")][::2] == [0, 2, 5]
    assert m["status"] == "COMPLETE_WITH_WARNINGS"

    class Jumpy(FakeFetch):
        def __call__(self, url, timeout):
            out = super().__call__(url, timeout)
            self.clock.jump(5_000_000)  # wall clock steps 5 ms forward, monotonic does not
            return out
    clock2 = sf.Clock()
    m2 = scrape(tmp_path, "after", Jumpy(clock2), clock2, name="a2")
    assert m2["status"] == "INCONSISTENT" and kinds(m2)["host_clock_step"] >= 2


def test_leader_change_reaches_the_summary(tmp_path):
    clock = sf.Clock()
    f = FakeFetch(clock, leader_changes=1)
    scrape(tmp_path, "before", f, clock)
    scrape(tmp_path, "after", f, clock)
    s = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s", channel=sf.CHANNEL)
    assert s["derived"]["orderer1"]["leader_changes_delta"] == 1


def test_family_filter_keeps_raw_bodies_complete(tmp_path):
    clock = sf.Clock()
    m = scrape(tmp_path, "after", FakeFetch(clock), clock, family_filter="^consensus_")
    assert m["samples_filtered_out"] > 0
    rows = ms.tc.read_csv(tmp_path / "after" / "samples.csv")
    assert rows and all(r["family"].startswith("consensus_") for r in rows)
    raw = (tmp_path / "after" / "raw" / "after_000000_orderer1.prom").read_text()
    assert "blockcutter_block_fill_duration_sum" in raw


def test_cli_guards(tmp_path, capsys):
    t = ["--target", "orderer1:orderer=http://o1:9443/metrics"]
    assert ms.main(["scrape", *t, "--phase", "after", "--out-dir", str(tmp_path / "dry")]) == 0
    assert "dry run" in capsys.readouterr().out and not (tmp_path / "dry").exists()
    assert ms.main(["scrape", "--target", "bad", "--phase", "after", "--out-dir", str(tmp_path / "x"),
                    "--execute"]) == 2
    assert ms.main(["scrape", *t, *t, "--phase", "after", "--out-dir", str(tmp_path / "y"), "--execute"]) == 2
    assert ms.main(["scrape", *t, "--phase", "during", "--out-dir", str(tmp_path / "z"), "--execute"],
                   fetch=lambda u, s: (200, b"")) == 2       # during without a stop condition
    (tmp_path / "exists").mkdir()
    assert ms.main(["scrape", *t, "--phase", "after", "--out-dir", str(tmp_path / "exists"), "--execute"]) == 2
    code = ms.main(["scrape", *t, "--phase", "after", "--out-dir", str(tmp_path / "ok"), "--execute"],
                   fetch=lambda u, s: (200, sf.orderer_metrics(2).encode()))
    assert code == 0 and json.loads((tmp_path / "ok" / "metrics_manifest.json").read_text())["status"] == "COMPLETE"
