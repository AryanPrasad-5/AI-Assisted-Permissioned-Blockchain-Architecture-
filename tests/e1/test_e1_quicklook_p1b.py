"""Fabric-free tests for scripts/e1/e1_quicklook_p1b.py (the P1b TIER 1 capacity-characterization driver, derived from the frozen P1a driver).
Synthetic only: nothing here is an E1 result.

A fake world stands in at the process, HTTP and Windows-host boundary (Backend). The engine, the gates, the verdict logic and
the dry-run text are the real code. No Docker, Fabric, Caliper or network access. The engine tests were inherited from the P1a driver's
suite and adapted to the Tier 1 matrix (10 rows, 15-40 tx/s); the tests at the end are new: corrected classification, ramp chains, approval cap, the
owner's six bounds-only conditions (B2), the host sampler (B3), the per-run Caliper report (B4), evidence completeness, the erratum interlock and derivation.
"""
import csv
import dataclasses
import hashlib
import io
import json
import shutil
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_channel_run as cr  # noqa: E402
import e1_host_sampler as hs  # noqa: E402
import e1_la2_render as lr  # noqa: E402
import e1_p1b_matrix as mx  # noqa: E402
import e1_quicklook as v1  # noqa: E402                  (the frozen P1a driver: imported only to compare and to prove it is untouched)
import e1_quicklook_p1b as ql  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

PKG = "e1asset_1.0:" + "ab" * 32
RATES = list(mx.RATES)
REAL_BLOCKERS = ql.BLOCKERS_OPEN          # as shipped, captured at import time (the world fixture empties it for the engine tests)
DEFAULT_REPORT = "<html><body>the P1a report (default path)</body></html>"


def matrix_bytes() -> bytes:
    return mx.matrix_bytes()


def erratum_text(sha: str, by: str = "Test Owner", on: str = "2026-11-13") -> str:
    """A stand-in for the erratum document: only section 11 matters to the driver."""
    return ("# Erratum E1-QL-ERR-001 (test stand-in)\n\nSome prose that mentions adopted_by= in the middle of a sentence is not a signature line.\n\n"
            f"## 11. Owner sign-off\n```\nerratum_id=E1-QL-ERR-001\nderived_csv_sha256={sha}\nadopted_by={by}\nadopted_on={on}\n```\n")


@pytest.fixture
def world(tmp_path, monkeypatch):
    e1 = tmp_path / "e1"
    erratum = tmp_path / "p1a_corrected_classification.csv"
    erratum.write_text("run_id,corrected_class\nql17,HEALTHY\nql23,HEALTHY\nql01,HEALTHY\n", encoding="utf-8", newline="\n")
    erratum_sha = hashlib.sha256(erratum.read_bytes()).hexdigest()
    erratum_doc = tmp_path / "ERRATUM.md"
    erratum_doc.write_text(erratum_text(erratum_sha), encoding="utf-8", newline="\n")
    monkeypatch.setattr(ql, "ERRATUM_CSV_SHA256", erratum_sha)
    monkeypatch.setattr(ql, "BLOCKERS_OPEN", ())              # the engine tests run the engine; the blocker guard has its own test below
    ctx = ql.Ctx(repo=REPO, e1_home=e1, root=e1 / "p1b", net=tmp_path / "network.json", env=tmp_path / "env.json",
                 caliper_ws=e1 / "caliper-ws", py="python3", win_repo="C:\\repo", win_snapshot="D:\\snap",
                 win_python="C:\\python.exe", ref_python="/x/python.exe", matrix=tmp_path / "run_matrix.csv", fabric_bin="/fab/bin",
                 erratum_csv=erratum, erratum_doc=erratum_doc)
    env_doc = {"test_network": "/tn", "fabric_bin": "/fab/bin", "fabric_cfg": "/fab/cfg", "repo": str(REPO),
               "caliper_workspace": str(ctx.caliper_ws), "chaincode": {"name": "e1asset", "version": "1.0", "sequence": 1},
               "protocol": {"event_strategy": "msp_any", "invoke_timeout_s": 60, "binding": "fabric:2.5"}}
    net_doc = lr.network(env_doc, PKG)
    ctx.net.write_text(json.dumps(net_doc), encoding="utf-8")
    ctx.env.write_text(json.dumps(env_doc), encoding="utf-8")
    ctx.matrix.write_bytes(matrix_bytes())
    e1.mkdir()
    ctx.caliper_ws.mkdir()
    (ctx.caliper_ws / "report.html").write_text(DEFAULT_REPORT, encoding="utf-8")        # the workspace's default report, as P1a left it
    rows = ql.load_matrix(ctx.matrix)
    return SimpleNamespace(ctx=ctx, env_doc=env_doc, net_doc=net_doc, rows=rows, tmp=tmp_path, erratum_sha=erratum_sha)


# ====================================================================================== matrix
def test_embedded_matrix_is_byte_identical_to_the_frozen_one():
    assert hashlib.sha256(matrix_bytes()).hexdigest() == ql.MATRIX_SHA256 == mx.matrix_sha256()


def test_load_matrix_ok(world):
    rows = world.rows
    assert [r.run_id for r in rows] == [f"cp{i:02d}" for i in range(1, 11)] and ql.ROW_COUNT == 10
    assert rows[0].channel == "e1-cp01-b40-l15" and rows[0].B == 40 and rows[0].rate == 15 and rows[0].kind == "ramp"
    assert rows[9].channel == "e1-cp10-b10-l40" and rows[9].kind == "ramp"
    assert len({r.channel for r in rows}) == 10 and not ({r.channel for r in rows} & ql.EXISTING_CHANNELS)
    assert rows[0].anchor() == ("ql17", 40, 10) and rows[1].anchor() == ("ql23", 10, 10)
    assert rows[0].prev_cell() is None and rows[2].prev_cell() == (40, 15) and rows[2].anchor() is None and rows[3].prev_cell() == (10, 15)
    assert max(r.rate for r in rows) == ql.TIER1_MAX_RATE_TPS == 40 and ql.ALLOWED_RATES == (15, 20, 25, 30, 40)


def test_the_26_row_tier_2_design_cannot_be_loaded_by_this_driver(tmp_path):
    """Tier 2 is structurally absent: not by hash, not by row count, not by rate grid."""
    p = tmp_path / "full.csv"
    p.write_bytes(mx.matrix_bytes(200))
    with pytest.raises(ql.MatrixError, match="hash"):
        ql.load_matrix(p)                                                                        # the pinned hash is the Tier 1 matrix's
    sha = hashlib.sha256(p.read_bytes()).hexdigest()
    with pytest.raises(ql.MatrixError, match="26 rows, expected 10"):
        ql.load_matrix(p, expected_sha=sha)                                                      # someone who also swaps the hash still fails the row count
    with pytest.raises(ql.MatrixError, match="outside the frozen grid"):
        ql.load_matrix(p, expected_sha=sha, require_count=None)                                  # and, with the count waived, the rate grid (60-200 tx/s)


def test_load_matrix_rejects_other_bytes(tmp_path):
    p = tmp_path / "m.csv"
    p.write_bytes(matrix_bytes() + b"\n")
    with pytest.raises(ql.MatrixError, match="hash"):
        ql.load_matrix(p)
    with pytest.raises(ql.MatrixError, match="not found"):
        ql.load_matrix(tmp_path / "nope.csv")


def _mutated(tmp_path, old: str, new: str, name="m.csv") -> Path:
    text = matrix_bytes().decode().replace(old, new, 1)
    assert text != matrix_bytes().decode()
    p = tmp_path / name
    p.write_text(text, encoding="utf-8", newline="\n")
    return p


@pytest.mark.parametrize("old,new,msg", [
    ("e1-cp05-b40-l25", "e1-r002", "encode|existing"),
    ("cp05,e1-cp05-b40-l25,ramp,40", "cp05,e1-cp05-b40-l25,ramp,50", "grid"),
    ("e1-cp06-b10-l25", "e1-cp06-b10-l26", "encode"),
    ("cp03,e1-cp03-b40-l20", "cp04,e1-cp03-b40-l20", "order"),
    ("seq,run_id", "sequence,run_id", "header"),
    ("cp04,e1-cp04-b10-l20,ramp,10,2s", "cp04,e1-cp04-b10-l20,ramp,10,3s", "common parameters"),
    ("cp04,e1-cp04-b10-l20,ramp", "cp04,e1-cp04-b10-l20,sweep", "kind|ramp rows only"),
    ("P1a run ql17 (B=40", "P1a run ql99 (B=40", "must continue from P1a run ql17"),
])
def test_validate_rejects_bad_rows(tmp_path, old, new, msg):
    p = _mutated(tmp_path, old, new)
    sha = hashlib.sha256(p.read_bytes()).hexdigest()
    with pytest.raises(ql.MatrixError, match=msg):
        ql.load_matrix(p, expected_sha=sha)


def test_ramp_precondition_must_name_the_previous_rung_of_the_same_setting(tmp_path):
    p = _mutated(tmp_path, "at B=40, rate=15 was HEALTHY", "at B=40, rate=25 was HEALTHY")
    with pytest.raises(ql.MatrixError, match="precondition"):
        ql.load_matrix(p, expected_sha=hashlib.sha256(p.read_bytes()).hexdigest())
    q = _mutated(tmp_path, "at B=40, rate=15 was HEALTHY", "at B=10, rate=15 was HEALTHY", name="m2.csv")      # the other setting's rung
    with pytest.raises(ql.MatrixError, match="precondition"):
        ql.load_matrix(q, expected_sha=hashlib.sha256(q.read_bytes()).hexdigest())


def test_design_findings_are_empty_for_a_ramp_only_matrix(world):
    assert ql.design_findings(world.rows) == []


# ====================================================================================== command builders
def test_channel_and_render_argv(world):
    c, r = world.ctx, world.rows[0]
    a = ql.argv_channel(r, c)
    assert a[:3] == ["python3", "-B", c.tool("e1_channel_run.py")] and a[-1] == "--execute"
    assert a[a.index("--channel") + 1] == "e1-cp01-b40-l15" and a[a.index("--max-message-count") + 1] == "40"
    assert a[a.index("--batch-timeout") + 1] == "2s" and a[a.index("--preferred-max-bytes") + 1] == "10485760"
    assert a[a.index("--run-dir") + 1] == str(c.root / "cp01" / "channel")
    b = ql.argv_render(r, c)
    assert b[3] == "run" and b[b.index("--duration-s") + 1] == "120" and b[b.index("--warmup-s") + 1] == "30"
    assert b[b.index("--rate") + 1] == "15" and b[b.index("--workload") + 1] == "create" and "--n" not in b


def test_scrape_resources_summarize_fetch_analysis_argv(world):
    c, r = world.ctx, world.rows[2]                    # cp03: B=40, rate 20
    s = ql.argv_scrape(r, c, "during", c.root / "cp03" / "stop_metrics")
    assert s[s.index("--phase") + 1] == "during" and "--stop-file" in s and s.count("--target") == 5 and s[-len(ql.target_args()) - 1] == "--execute"
    rs = ql.argv_resources(r, c, ql.watch_containers(["dev-peer-x"]), c.root / "cp03" / "stop_resources")
    assert rs.count("--container") == 7 and rs[rs.index("--source") + 1] == "api"
    z = ql.argv_summarize(r, c)
    assert "--fresh-channel" in z and "--expect-traffic" in z and z[z.index("--channel") + 1] == r.channel
    f = ql.argv_fetch(r, c)
    assert f[f.index("--first") + 1] == "0" and f[f.index("--max-message-count") + 1] == "40"
    an = ql.argv_analysis(r, c, 2400)
    assert an[an.index("--expected-n") + 1] == "2400" and an[an.index("--rate-tps") + 1] == "20" and "--other-round-csv" in an
    assert an[an.index("--recorder-base") + 1].endswith("cp03_w0_r1_m-cp03")


def test_caliper_plan_validates_with_the_orchestrator_plan(world):
    import e1_run_orchestrator as ro
    plan = ql.caliper_plan(world.rows[0], world.ctx, world.env_doc)
    p = ro.Plan(plan)
    assert plan["caliper"]["argv"][:5] == ["docker", "exec", "-w", lr.CAL_WS, "caliper"]       # the renderer names the stopped E0-0 container
    fixed = ql.caliper_argv(plan)
    assert fixed[:5] == ["docker", "exec", "-w", lr.CAL_WS, ql.CALIPER] and fixed[5:-2] == plan["caliper"]["argv"][5:]
    assert fixed[-2:] == ["--caliper-report-path", "runs/cp01/report.html"]                       # B4: always appended, nothing else changes
    broken = json.loads(json.dumps(plan))
    broken["caliper"]["argv"][4] = "someone-else"
    with pytest.raises(ValueError, match="unexpected Caliper argv"):
        ql.caliper_argv(broken)
    assert p.request.channel == "e1-cp01-b40-l15" and p.raw["cell"]["mode"] == "txDuration" and p.raw["cell"]["duration_s"] == 120
    assert Path(plan["caliper"]["recorder_dir"]) == world.ctx.recorder_dir("cp01")


# ====================================================================================== B4: the per-run Caliper report path
def test_every_row_gets_its_own_report_path_inside_its_own_run_folder(world):
    paths = []
    for r in world.rows:
        argv = ql.caliper_argv(ql.caliper_plan(r, world.ctx, world.env_doc))
        assert argv.count("--caliper-report-path") == 1 and argv[argv.index("--caliper-report-path") + 1] == f"runs/{r.run_id}/report.html"
        assert argv[argv.index("--caliper-benchconfig") + 1] == f"runs/{r.run_id}/benchmark.yaml"        # the same folder the benchmark and the recorder use
        paths.append(argv[argv.index("--caliper-report-path") + 1])
    assert len(set(paths)) == len(world.rows) == 10 and ql.report_rel("cp07") == "runs/cp07/report.html"
    assert not any(p in ("report.html", "./report.html") or p.startswith("/") for p in paths)           # never the workspace default, never absolute


def test_a_plan_that_already_sets_a_report_path_is_refused(world):
    plan = ql.caliper_plan(world.rows[0], world.ctx, world.env_doc)
    plan["caliper"]["argv"] = plan["caliper"]["argv"] + ["--caliper-report-path", "report.html"]
    with pytest.raises(ValueError, match="already sets a Caliper report path"):
        ql.caliper_argv(plan)


def test_the_report_flag_is_the_one_the_pinned_caliper_source_defines():
    """caliper-core v0.6.0 common/config/Config.js:41 defines Report.Path as 'caliper-report-path'; the renderer's own flags use the same dashed-key convention
    (Config.js:143 'caliper-flow-only-test'). A typo here would be ignored by Caliper (command-line arguments are open-ended settings), so it is pinned by value."""
    plan_flags = [a for a in lr.run_plan({"caliper_workspace": "/w", "protocol": {"event_strategy": "x", "invoke_timeout_s": 1, "binding": "fabric:2.5"}},
                                        "/n", "cp01", "e1-cp01", {"B": 10}, 30, True)["caliper"]["argv"] if a.startswith("--caliper-")]
    assert "--caliper-flow-only-test" in plan_flags and all(f == f.lower() and "_" not in f for f in plan_flags)
    argv = ql.caliper_argv(ql.caliper_plan(ql.Row(1, "cp01", "e1-cp01-b40-l15", "ramp", 40, "2s", 10485760, 15, 1, 30, 120, ""), ql.Ctx.default(),
                                           {"caliper_workspace": "/w", "protocol": {"event_strategy": "x", "invoke_timeout_s": 1, "binding": "fabric:2.5"}}))
    assert "--caliper-report-path" in argv                                                                  # the literal, not a derived spelling


def test_expected_regime_is_arithmetic_only(world):
    by = {r.run_id: ql.expected_regime(r) for r in world.rows}
    assert by["cp02"].startswith("count-cut") and "W = 0.600 s" in by["cp02"] and "p95 about 0.625 s" in by["cp02"]            # B=10 < 15*2; W = 9/15
    assert by["cp01"].startswith("timer-cut") and "W = 2.000 s" in by["cp01"] and "p95 about 1.940 s" in by["cp01"]            # B=40 > 15*2; W = tau
    assert by["cp03"].startswith("boundary") and "W = 1.950 s" in by["cp03"] and "p95 about 1.926 s" in by["cp03"]             # B=40 == 20*2; W = 39/20
    assert by["cp05"].startswith("count-cut") and "W = 1.560 s" in by["cp05"]            # B=40 < 25*2; W = 39/25
    assert all("hypothesis above 20 tx/s" in v for v in by.values())
    assert ql.expected_submits(world.rows[0]) == {"measured": 1800, "warmup": 450}


# ====================================================================================== parsers
def test_parse_typeperf_skips_header_footer_and_missing_values():
    text = ('"(PDH-CSV 4.0)","\\\\H\\Memory\\Available MBytes","\\\\H\\Memory\\% Committed Bytes In Use","\\\\H\\Memory\\Pages/sec"\n'
            '"11/12/2026 17:05:01.234"," 4569.000000","59.000000"," "\n'
            '"11/12/2026 17:05:06.234","4570.000000","59.100000","12.500000"\n'
            '"11/12/2026 17:05:11.234","4571.000000","59.200000","80.000000"\n'
            'Exiting, please wait...  The command completed successfully.\n')
    d = ql.parse_typeperf(text)
    assert d["available_mb"] == [4570.0, 4571.0] and d["pages_sec"] == [12.5, 80.0] and d["skipped"] == 1
    assert ql.parse_typeperf("garbage") is None


def test_parse_getinfo_prom_and_inspect():
    assert ql.parse_getinfo('Blockchain info: {"height":9,"currentBlockHash":"aa","previousBlockHash":"bb"}')["height"] == 9
    assert ql.parse_getinfo("nothing") is None
    prom = 'consensus_etcdraft_active_nodes{channel="e1-r002"} 3\nconsensus_etcdraft_active_nodes{channel="e1-b001"} 2\n'
    assert ql.prom_value(prom, "consensus_etcdraft_active_nodes", "e1-r002") == 3.0
    assert ql.prom_value(prom, "consensus_etcdraft_active_nodes", "e1-zzz") is None
    docs = [sf.inspect_doc("a", cid="1" * 64), sf.inspect_doc("b", cid="2" * 64, restarts=2)]
    m = ql.parse_inspect(json.dumps(docs))
    assert m["a"]["running"] and m["b"]["restart_count"] == 2 and ql.parse_inspect("not json") == {}


def test_container_events_detects_restart_replace_exit_oom():
    base = ql.parse_inspect(json.dumps([sf.inspect_doc(n, cid=hashlib.sha256(n.encode()).hexdigest()) for n in "abcde"]))
    same = ql.parse_inspect(json.dumps([sf.inspect_doc(n, cid=hashlib.sha256(n.encode()).hexdigest()) for n in "abcde"]))
    assert ql.container_events(same, base) == []
    now = ql.parse_inspect(json.dumps([
        sf.inspect_doc("a", cid=hashlib.sha256(b"a").hexdigest(), started="2026-11-12T13:00:00Z"),
        sf.inspect_doc("b", cid="f" * 64),
        sf.inspect_doc("c", cid=hashlib.sha256(b"c").hexdigest(), status="exited"),
        sf.inspect_doc("d", cid=hashlib.sha256(b"d").hexdigest(), oom=True),
        sf.inspect_doc("e", cid=hashlib.sha256(b"e").hexdigest(), restarts=1)]))
    ev = " | ".join(ql.container_events(now, base))
    for word in ("a: restarted", "b: container replaced", "c: not running", "d: OOM", "e: restarted"):
        assert word in ev


# ====================================================================================== gates
GOOD = {"available_mb": [4500.0] * 12, "commit_pct": [59.0] * 12, "pages_sec": [20.0] * 12}


def test_memory_gate_thresholds():
    assert ql.eval_memory(GOOD, 6000).status == "PASS"
    assert ql.eval_memory({**GOOD, "available_mb": [4500.0] * 11 + [1499.0]}, 6000).status == "FAIL"
    assert ql.eval_memory({**GOOD, "commit_pct": [59.0] * 11 + [85.1]}, 6000).status == "FAIL"
    assert ql.eval_memory({**GOOD, "pages_sec": [500.0] * 12}, 6000).status == "FAIL"
    assert ql.eval_memory(GOOD, 2999).status == "FAIL"
    assert ql.eval_memory(None, 6000).status == "FAIL"
    spikes = {**GOOD, "pages_sec": [20.0] * 10 + [3000.0, 1600.0]}          # a median rule tolerates two spikes, and reports them
    g = ql.eval_memory(spikes, 6000)
    assert g.status == "PASS" and g.evidence["pages_sec_samples_over_100"] == 2 and g.evidence["pages_sec_max"] == 3000.0


def test_disk_gate():
    assert ql.eval_disk(15.0).status == "PASS" and ql.eval_disk(14.9).status == "FAIL"


def _creates(rate, n, late=lambda k: 0.0):
    return [1_000_000.0 + k * 1000.0 / rate + late(k) for k in range(n)]


def test_v2_pacing():
    ok = ql.v2_pacing(_creates(10, 1200), 1200, 10, 120, 3.0)
    assert ok["pass"] and ok["lateness_p99_ms"] == pytest.approx(0.0, abs=1e-6)
    assert not ql.v2_pacing(_creates(10, 1200), 1200, 10, 120, 25.0)["pass"]                       # event loop
    assert not ql.v2_pacing(_creates(10, 1200), 1100, 10, 120, 3.0)["pass"]                        # ratio 0.917
    drift = ql.v2_pacing(_creates(10, 1200, late=lambda k: 0.02 * k), 1200, 10, 120, 3.0)           # client falls behind
    assert not drift["pass"] and drift["lateness_p99_ms"] > 5
    assert ql.v2_pacing(_creates(10, 1200), 1200 * 1.03, 10, 120, 3.0)["ratio_A_over_lambda"] == pytest.approx(1.03)


def test_interior_rate_and_classification():
    meas = [{"tx_id": f"t{k}", "create_ms": 1000.0 * k / 10.0, "final_ms": 1000.0 * k / 10.0 + 1500, "status": "success"} for k in range(1200)]
    valid = {m["tx_id"] for m in meas}
    v = ql.interior_valid_rate(meas, valid, 120)
    assert v["rate"] == pytest.approx(10.0, rel=0.01) and v["window_s"] == 100.0
    assert ql.classify(10.0, v, 30, 0.0, 3.0, 25.0, True) == []
    assert ql.classify(10.0, {"rate": 8.0}, 30, 0.0, 3.0, 25.0, True) == ["SATURATED"]
    assert ql.classify(10.0, v, 250, 0.0, 3.0, 25.0, True) == ["SATURATED"]
    assert ql.classify(10.0, v, 30, 0.02, 3.0, 25.0, True) == ["SATURATED"]
    assert ql.classify(10.0, v, 30, 0.0, 12.0, 25.0, True) == ["CLIENT_LIMITED"]
    assert ql.classify(10.0, v, 30, 0.0, 3.0, 80.0, True) == ["CLIENT_LIMITED"]
    assert ql.classify(10.0, v, 30, 0.0, 3.0, 25.0, False) == ["CLIENT_LIMITED"]
    half = {m["tx_id"] for m in meas[::2]}
    assert ql.interior_valid_rate(meas, half, 120)["rate"] == pytest.approx(5.0, rel=0.02)          # ledger-INVALID transactions do not count


# ====================================================================================== the fake world
class FakeProc:
    def __init__(self, be, duration=None, stop_file=None, code=0, on_done=None):
        self.be, self.done_at, self.stop_file, self.code, self.on_done = be, (be.t + duration) if duration is not None else None, stop_file, code, on_done
        self.returncode, self.killed = None, False

    def poll(self):
        if self.returncode is not None:
            return self.returncode
        if (self.stop_file is not None and Path(self.stop_file).exists()) or (self.done_at is not None and self.be.t >= self.done_at):
            self.returncode = self.code
            if self.on_done:
                self.on_done()
        return self.returncode

    def kill(self):
        self.killed = True
        if self.returncode is None:
            self.returncode = -9

    terminate = kill

    def wait(self, timeout=None):
        return self.returncode


EPOCH12 = ql.utc_to_ns("2026-11-12T12:00:00.000Z")           # FakeBackend.now_utc() is 12:00:00 plus the simulated seconds
PROBE_COLS = "i,real_ns,mono_ns,raw_ns,boot_ns,adj_rc,tick_us,freq_scaled,status,offset,constant"


class FakeProbeProc(FakeProc):
    """The pinned clock probe as the driver meets it: 50 ms samples until it is stopped, no manifest when stopped early (the real
    tool writes probe_manifest.json only when its --duration ends), and a torn last line, as after a SIGTERM in the middle of a write."""

    def __init__(self, be, out_dir, *, exit_code=None, steps=(), stop_sampling_after=None, dies_after=None):
        super().__init__(be)
        self.out_dir, self.t0, self.steps, self.limit = Path(out_dir), be.t, list(steps), stop_sampling_after
        self.die_at = None if dies_after is None else be.t + dies_after       # a probe that is killed from outside, mid-series
        if exit_code is not None:                               # a probe that died: its samples end after stop_sampling_after seconds (default: at once)
            self.returncode = exit_code
            self._write(be.t + (stop_sampling_after or 0.0))

    def _write(self, t_end):
        if self.limit is not None:
            t_end = min(t_end, self.t0 + self.limit)
        rows, k = [PROBE_COLS], 0
        while self.t0 + k * 0.05 <= t_end:
            off = sum(ns for at, ns in self.steps if k * 0.05 >= at)
            real = EPOCH12 + int((self.t0 + k * 0.05) * 1e9) + off
            mono = 5_000_000_000_000 + int(k * 0.05 * 1e9)
            rows.append(f"{k},{real},{mono},{mono},{mono},0,10000,0,0,0,0")
            k += 1
        text = "\n".join(rows) + "\n" + f"{k},{EPOCH12}"                        # the torn tail
        (self.out_dir / "clock_samples.csv").write_text(text, encoding="utf-8")

    def poll(self):
        if self.returncode is None and self.die_at is not None and self.be.t >= self.die_at:
            self.returncode = 1
            self._write(self.die_at)
        return super().poll()

    def terminate(self):
        if self.poll() is None:
            self._write(self.be.t)
            self.returncode, self.killed = -15, True

    kill = terminate


def _arg(a, flag):
    return a[a.index(flag) + 1]


class FakeBackend:
    """Everything the engine touches: processes, HTTP, Windows host. Deterministic simulated time."""

    def __init__(self, world, **opt):
        self.w, self.ctx, self.opt = world, world.ctx, opt
        self.t, self.calls, self.spawns, self.killed, self.runs, self.channels, self.pb_channel = 100.0, [], [], [], {}, {}, {}
        names = ql.watch_containers(["dev-peer0.org1.example.com-e1asset_1.0-aa", "dev-peer0.org2.example.com-e1asset_1.0-aa"])
        self.containers = {n: {"id": hashlib.sha256(n.encode()).hexdigest(), "started": "2026-11-12T11:22:25Z", "restarts": 0,
                               "status": "running", "oom": False} for n in names}
        self.health_off: dict[int, float] = {}                   # ops port -> time from which /healthz fails

    # ------------- clocks
    def now_utc(self):
        s = int(self.t)
        return f"2026-11-12T12:{s // 60 % 60:02d}:{s % 60:02d}.000Z"

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.t += s

    # ------------- host
    def host_memory(self, samples, interval_s):
        return {"available_mb": [self.opt.get("win_avail", 4500.0)] * samples, "commit_pct": [59.0] * samples, "pages_sec": [20.0] * samples}

    def host_power(self, helper_pid=None):
        return {"line": "Online", "battery": 79, "helper_alive": True if helper_pid else None}

    def disk_free_gb(self, path):
        return self.opt.get("disk", 50.0)

    def wsl_mem_available_mb(self):
        return self.opt.get("wsl", 6000.0)

    def boot_id(self):
        return "boot-1"

    def http_status(self, url, timeout=2.0):
        port = int(url.split(":")[2].split("/")[0])
        return None if port in self.health_off and self.t >= self.health_off[port] else 200

    def http_text(self, url, timeout=5.0):
        return 'consensus_etcdraft_active_nodes{channel="e1-r002"} 3\nconsensus_etcdraft_cluster_size{channel="e1-r002"} 3\n'

    # ------------- processes
    def _inspect(self, names):
        out = []
        for n in names:
            c = self.containers.get(n)
            if c is None:
                continue
            d = sf.inspect_doc(n, cid=c["id"], started=c["started"], restarts=c["restarts"], oom=c["oom"], status=c["status"])
            d["Config"] = {"Image": "hyperledger/caliper:0.6.0" if n == ql.CALIPER else "hyperledger/fabric-peer:2.5.16"}
            out.append(d)
        return out

    def run(self, argv, env=None, timeout=None):
        self.calls.append(list(argv))
        self.t += 0.2
        code, out = 0, b""
        a = list(argv)
        tool = Path(a[2]).name if len(a) > 2 and a[1] == "-B" else None
        if a[0] == "docker" and a[1] == "inspect":
            docs = self._inspect(a[2:])
            code, out = (0, json.dumps(docs).encode()) if docs else (1, b"[]")
        elif a[0] == "docker" and a[1] == "kill":
            self.killed.append(a[2])
        elif a[0] == "docker" and a[1] == "ps":
            out = "\n".join(n for n in self.containers if n.startswith("dev-peer")).encode()
        elif tool == "e1_channel_run.py":
            rd, ch = Path(_arg(a, "--run-dir")), _arg(a, "--channel")
            rd.mkdir(parents=True)
            ok = not self.opt.get("fail_channel")
            self.channels[ch] = {"max_message_count": int(_arg(a, "--max-message-count")), "batch_timeout_ms": 2000,
                                 "preferred_max_bytes": int(_arg(a, "--preferred-max-bytes"))}
            (rd / "channel_run_record.json").write_text(json.dumps({
                "status": "VERIFIED" if ok else "CONFIG_MISMATCH",
                "checks": [{"check": "max_message_count", "pass": ok}, {"check": "batch_timeout", "pass": True}]}), encoding="utf-8")
            code = 0 if ok else 1
        elif tool == "e1_la2_render.py":
            run_id, ch = _arg(a, "--run-id"), _arg(a, "--channel")
            cell = {"B": int(_arg(a, "--B")), "tau": "2s", "P": int(_arg(a, "--P")), "n": 0, "mode": "txDuration",
                    "rate_tps": float(_arg(a, "--rate")), "duration_s": int(_arg(a, "--duration-s"))}
            plan = lr.run_plan(self.w.env_doc, str(self.ctx.net), run_id, ch, cell, int(_arg(a, "--warmup-s")), True)
            Path(_arg(a, "--out-plan")).write_text(json.dumps(plan), encoding="utf-8")
            (self.ctx.caliper_ws / "runs" / run_id).mkdir(parents=True)
            self.runs[run_id] = {"rate": float(_arg(a, "--rate")), "B": cell["B"], "warmup": int(_arg(a, "--warmup-s")),
                                 "measured": int(_arg(a, "--duration-s")), "channel": ch}
        elif tool == "e1_metrics_scraper.py":
            od = Path(_arg(a, "--out-dir"))
            od.mkdir(parents=True)
            if a[3] == "summarize":
                n = 1 if self.opt.get("leader_change") else 0
                (od / "metrics_summary.json").write_text(json.dumps({
                    "status": "COMPLETE", "traffic": {}, "anomalies": {},
                    "derived": {f"orderer{i}": {"role": "orderer", "leader_changes_delta": n if i == 1 else 0, "fill_duration_mean_s": 0.9}
                                for i in (1, 2, 3)}}), encoding="utf-8")
        elif tool == "e1_block_fetch.py":
            self._ledger(Path(_arg(a, "--out-dir")), _arg(a, "--channel"))
        elif tool == "e1_benchmark_analyze.py":
            self._analysis(Path(_arg(a, "--out-dir")), _arg(a, "--run-id"))
        elif a[0].endswith("peer") and a[1:3] == ["channel", "getinfo"]:
            out = b'Blockchain info: {"height":7,"currentBlockHash":"aGFzaA==","previousBlockHash":"cHJldg=="}\n'
        elif a[0].endswith("peer") and a[1:3] == ["channel", "fetch"]:
            ch = a[a.index("-c") + 1]
            Path(a[4]).write_bytes(f"cfg-{ch}-{self.opt.get('cfg_rev', 0)}".encode() + (b"x" if self.opt.get("config_changes") and "config_after" in a[4] else b""))
            self.pb_channel[a[4]] = ch
        elif a[0].endswith("configtxlator"):
            ch = self.pb_channel[_arg(a, "--input")]
            Path(_arg(a, "--output")).write_text(json.dumps({"n": 0, "cfg": self.channels.get(ch, {})}), encoding="utf-8")
        elif a[0].endswith("peer") and a[1:3] == ["lifecycle", "chaincode"]:
            out = f"Installed chaincodes on peer:\nPackage ID: {PKG}, Label: e1asset_1.0\n".encode()
        elif a[0].endswith("osnadmin"):
            names = [{"name": n} for n in self.opt.get("listed", ["e1-b001", "e1-r002"])]
            out = ("Status: 200\n" + json.dumps({"systemChannel": None, "channels": names})).encode()
        elif a[0].endswith("python.exe"):
            out = self.opt.get("s1_text", "").encode()
        return SimpleNamespace(code=code, stdout=out, stderr=b"", started_utc=self.now_utc(), ended_utc=self.now_utc(),
                               duration_s=0.2, timed_out=False)

    def spawn(self, argv, env=None, stdout_path=None, stderr_path=None):
        self.calls.append(list(argv))
        self.spawns.append(list(argv))
        for p in (stdout_path, stderr_path):
            if p is not None:
                Path(p).open("xb").close()
        a = list(argv)
        tool = Path(a[2]).name if len(a) > 2 and a[1] == "-B" else None
        if tool == "e1_metrics_scraper.py":
            od = Path(_arg(a, "--out-dir"))
            od.mkdir(parents=True)
            run_id = od.parent.parent.name                                   # <run folder>/metrics/during
            return FakeProc(self, stop_file=_arg(a, "--stop-file"), on_done=lambda: self._scrapes(od, run_id))
        if tool == "e1_resource_sampler.py":
            od = Path(_arg(a, "--out-dir"))
            od.mkdir(parents=True)
            run_id = od.parent.name
            return FakeProc(self, stop_file=_arg(a, "--stop-file"), on_done=lambda: self._resources(od, run_id))
        if tool == "e1_host_sampler.py":
            od = Path(_arg(a, "--out-dir"))
            od.mkdir(parents=True)
            run_id = od.parent.name
            assert a[a.index("--max-seconds") + 1] == str(ql.EVID.host_max_seconds) and "--execute" in a and "--stop-file" in a
            return FakeProc(self, stop_file=_arg(a, "--stop-file"), on_done=lambda: self._host(od, run_id))
        if a[:2] == ["docker", "exec"]:
            assert a[4] == ql.CALIPER, "the engine must exec into caliper-benchmark-001, not the stopped E0-0 'caliper' container"
            run_id = a[a.index("--caliper-benchconfig") + 1].split("/")[1]
            flagged = a[a.index("--caliper-report-path") + 1] if "--caliper-report-path" in a else None
            return FakeProc(self, duration=self.opt.get("caliper_s", 190.0), code=self.opt.get("caliper_code", 0),
                            on_done=lambda: (self._recorder(run_id), self._caliper_report(run_id, flagged, stdout_path)))
        if tool == "e1_clock_rate_probe.py":
            od = Path(_arg(a, "--out-dir"))
            od.mkdir(parents=True)
            self.probe = FakeProbeProc(self, od, exit_code=self.opt.get("probe_exit"), steps=self.opt.get("probe_steps", ()),
                                       stop_sampling_after=self.opt.get("probe_stops_after"), dies_after=self.opt.get("probe_dies_after"))
            return self.probe
        raise AssertionError(f"unexpected spawn {a}")

    def start_keep_awake(self, seconds, pid_file_win):
        self.keep_awake_seconds = seconds
        self.keep_awake = FakeProc(self)
        return self.keep_awake

    # ------------- files the tools would write
    def _recorder(self, run_id):
        info = self.runs[run_id]
        rate, rec = info["rate"], self.ctx.recorder_dir(run_id)
        rec.mkdir(parents=True, exist_ok=True)
        for rnd, label, dur in ((0, f"wu-{run_id}", info["warmup"]), (1, f"m-{run_id}", info["measured"])):
            base = rec / f"{run_id}_w0_r{rnd}_{label}"
            n, t0 = int(rate * dur), 1_700_000_000_000 + rnd * 60_000
            lat = self.opt.get("latency_ms", 1500.0)
            tx = ["tx_id,seq_finish,create_ms,final_ms,status,verified,err,obs_hr_ns,worker,round,label"]
            sub = ["seq_submit,t_submit_evt_ms,count"]
            for k in range(n):
                c = t0 + k * 1000.0 / rate
                tx.append(f"{run_id}r{rnd}k{k},{k},{c:.3f},{c + lat:.3f},success,true,,0,0,{rnd},{label}")
                sub.append(f"{k},{c:.3f},1")
            Path(str(base) + ".csv").write_text("\n".join(tx) + "\n", encoding="utf-8")
            Path(str(base) + ".submits.csv").write_text("\n".join(sub) + "\n", encoding="utf-8")
            Path(str(base) + ".manifest.json").write_text(json.dumps({
                "run_id": run_id, "counts": {"submitted": n, "finished": n, "pending_at_deactivate": 0, "rows": n, "success": n, "failed": 0,
                                             "other_status": 0, "duplicate_ids": 0, "submit_events": n, "observer_errors": 0},
                "clock": {"wall0_ms": t0, "wall1_ms": t0 + dur * 1000 + 3000, "wall_elapsed_ms": dur * 1000 + 3000,
                          "wall_minus_monotonic_ms": self.opt.get("wall_mono", 0.3)},
                "event_loop_delay": {"p50_ms": 1.0, "p99_ms": self.opt.get("el_p99", 3.0), "max_ms": 6.0, "samples": 100}}), encoding="utf-8")

    def _run_of_dir(self, d):
        return next(r for r in self.runs if r in Path(d).parts)

    def _ledger(self, od, channel):
        run_id = self._run_of_dir(od)
        (od / "ledger").mkdir(parents=True)
        rows = ["tx_id,header_type,validation_code", "lifecycle1,3,0"]
        bad = self.opt.get("invalid_every", 0)
        base = self.ctx.recorder_dir(run_id) / f"{run_id}_w0_r1_m-{run_id}.csv"
        for i, r in enumerate(list(csv.DictReader(base.open(encoding="utf-8")))):
            rows.append(f"{r['tx_id']},3,{11 if bad and i % bad == 0 else 0}")
        (od / "ledger" / "ledger_tx.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")
        (od / "ledger" / "blocks.csv").write_text("number,tx_count\n0,1\n", encoding="utf-8")
        (od / "block_fetch_manifest.json").write_text(json.dumps({"status": "COMPLETE", "ledger_tables": {"written": True}}), encoding="utf-8")

    def _analysis(self, od, run_id):
        od.mkdir(parents=True)
        info = self.runs[run_id]
        base = self.ctx.recorder_dir(run_id) / f"{run_id}_w0_r1_m-{run_id}.csv"
        recs = list(csv.DictReader(base.open(encoding="utf-8")))
        n = len(recs)
        ledger = {r["tx_id"]: r["validation_code"] for r in csv.DictReader((self.ctx.rd(run_id) / "ledger/ledger/ledger_tx.csv").open(encoding="utf-8"))}
        valid = sum(1 for r in recs if ledger.get(r["tx_id"]) == "0")
        with (od / "transaction_measurements.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["tx_id", "seq_finish", "create_ms", "final_ms", "latency_ms", "status", "verified", "error"])
            for r in recs:
                w.writerow([r["tx_id"], r["seq_finish"], r["create_ms"], r["final_ms"], float(r["final_ms"]) - float(r["create_ms"]), "success", "true", ""])
        (od / "summary.json").write_text(json.dumps({
            "run_id": run_id, "counts": {"submitted": n, "finished": n, "success": n, "failed": 0, "valid_workload_ledger_transactions": valid},
            "metrics": {"latency_ms_p95_nearest_rank": 1500.0, "latency_ms_mean": 1500.0, "max_inflight_backlog": self.opt.get("backlog", 30)},
            "reconciliation": {"verdict": self.opt.get("reconcile", "VALID")}, "analysis_verdict": "VALID"}), encoding="utf-8")

    def _window(self, run_id):
        """The measured window the fake recorder reports (wall0_ms, wall1_ms)."""
        t0 = 1_700_000_000_000 + 60_000
        return t0, t0 + self.runs[run_id]["measured"] * 1000 + 3000

    def _seconds(self, run_id, hole=None, lead=-3, tail=3):
        """One timestamp (ms) per second from `lead` s before the window to `tail` s after it; `hole` = (from_s, to_s) after the window start is left out."""
        lo, hi = self._window(run_id)
        out, k = [], lead
        while lo + 1000 * k <= hi + 1000 * tail:
            if not (hole and hole[0] <= k < hole[1]):
                out.append(lo + 1000 * k + 400)
            k += 1
        return out

    def _resources(self, od, run_id):
        names = list(self.containers)
        gap = self.opt.get("resource_gap")                                  # (container, from_s, to_s)
        with (od / "resource_samples.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["container", "read_ns", "cpu_percent"])
            for n in names:
                hole = (gap[1], gap[2]) if gap and gap[0] == n else None
                for t in self._seconds(run_id, hole=hole):
                    w.writerow([n, int(t * 1e6), self.opt.get("cpu", 25.0) if n == ql.CALIPER else 3.0])
        (od / "resource_manifest.json").write_text(json.dumps({"status": "COMPLETE", "containers": names, "anomalies": {"error": {}, "warning": {}},
                                                                "snapshots": {"start": {}, "end": {}}}), encoding="utf-8")

    def _scrapes(self, od, run_id):
        targets = [("orderer1", "orderer"), ("orderer2", "orderer"), ("orderer3", "orderer"), ("peer0org1", "peer"), ("peer0org2", "peer")]
        gap = self.opt.get("scrape_gap")                                    # (target, from_s, to_s)
        with (od / "scrapes.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["round", "phase", "target", "role", "t_mid_wall_ns", "http_status"])
            for name, role in targets:
                hole = (gap[1], gap[2]) if gap and gap[0] == name else None
                for i, t in enumerate(self._seconds(run_id, hole=hole)):
                    w.writerow([i, "during", name, role, int(t * 1e6), 200])
        (od / "metrics_manifest.json").write_text(json.dumps({"phase": "during", "targets": [{"name": n, "role": r} for n, r in targets], "status": "COMPLETE"}),
                                                  encoding="utf-8")

    def _host(self, od, run_id):
        lo, hi = self._window(run_id)
        hole = self.opt.get("host_hole")                                    # (from_s, to_s) in the WSL stream
        with (od / "wsl_samples.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=hs.WSL_COLS, lineterminator="\n")
            w.writeheader()
            for i, t in enumerate(self._seconds(run_id, hole=hole)):
                w.writerow({"tick": i, "wall_ns": int(t * 1e6), "mono_ns": i * 10**9, "cpu_busy_pct": 12.5, "cpu_max_core_busy_pct": 40.0, "net_if_rx_mb_s": 0.01})
        with (od / "win_counters.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=hs.WIN_COLS, lineterminator="\n")
            w.writeheader()
            if not self.opt.get("host_no_windows_rows"):
                for i, t in enumerate(self._seconds(run_id)):
                    w.writerow({"row": i, "t_wall_ns": int(t * 1e6), "cpu_utility_pct": 6.0, "disk_time_pct": 1.0, "disk_bytes_s": 2_000_000, "net_bytes_s": 5_000,
                                "mem_available_mb": 4500})
        (od / "host_manifest.json").write_text(json.dumps({"status": "COMPLETE", "windows_missing_required": self.opt.get("host_missing_required", []),
                                                           "parameters": {"windows": not self.opt.get("host_windows_off")}}), encoding="utf-8")

    def _caliper_report(self, run_id, flagged, stdout_path):
        """What Caliper's report builder does: honour --caliper-report-path (the real behaviour), or, to test the gate, ignore it or write nothing."""
        mode = self.opt.get("report_mode", "per_run")
        ws = self.ctx.caliper_ws
        target = {"per_run": flagged, "ignored": None, "none": "-"}[mode]
        if target == "-":
            return
        rel = target or "report.html"
        path = ws / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"<html><body>report for {run_id}</body></html>", encoding="utf-8")
        line = f"2026.11.13-10:00:00.000 info  [caliper] [report-builder] \tGenerated report with path {lr.CAL_WS}/{rel}\n"
        if stdout_path is not None:
            Path(stdout_path).write_text("2026.11.13-09:59:00.000 info  [caliper] started\n" + line, encoding="utf-8")


@pytest.fixture
def patched(monkeypatch):
    class Cfg:
        def __init__(self, d):
            self.d = d

        def as_dict(self):
            return dict(self.d)
    monkeypatch.setattr(ql.ca, "config_from_block", lambda block: (Cfg(block["cfg"]), {}))
    monkeypatch.setattr(ql.ca, "block_number", lambda block: block["n"])
    monkeypatch.setattr(ql.ca, "block_data_hash", lambda block: "datahash")


def make_session(world, patched, **opt):
    be = FakeBackend(world, **opt)
    s = ql.Session(world.ctx, be)
    s.baseline = ql.parse_inspect(json.dumps(be._inspect(list(be.containers))))
    s.chaincode = [n for n in be.containers if n.startswith("dev-peer")]
    s.helper_pid = 4242
    world.ctx.root.mkdir(parents=True, exist_ok=True)
    s.log_path = world.ctx.root / "series_events.log"
    return s, be


# ====================================================================================== engine (fake world)
def labels_of(rd):
    return [json.loads(x)["label"] for x in (rd / "commands.jsonl").read_text(encoding="utf-8").splitlines()]


def test_execute_run_golden_path_is_valid_and_ordered(world, patched):
    s, be = make_session(world, patched)
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "VALID" and v["labels"] == [] and v["failed_gates"] == [] and v["not_evaluated_gates"] == [], v
    rd = world.ctx.rd("cp01")
    seq = labels_of(rd)
    order = ["channel", "render", "metrics_before", "metrics_during", "resources", "host", "getinfo_before", "config_before_fetch",
             "config_before_decode", "caliper", "caliper_end", "getinfo_after", "config_after_fetch", "config_after_decode", "stop_metrics",
             "stop_resources", "stop_host", "metrics_after", "metrics_summary", "block_fetch", "analysis"]
    assert [x for x in seq if x in order] == order
    for f in ("run_verdict.json", "facts.json", "config_check.json", "plan.json", "analysis/summary.json", "ledger/block_fetch_manifest.json",
              "host/host_manifest.json", "host/wsl_samples.csv", "host/win_counters.csv"):
        assert (rd / f).is_file(), f
    facts = json.loads((rd / "facts.json").read_text(encoding="utf-8"))
    assert facts["stop_background"] == {"metrics": 0, "resources": 0, "host": 0}                       # all three samplers were stopped by file and exited cleanly
    assert facts["report"]["default_sha256_before"] == facts["report"]["default_sha256_after"] == hashlib.sha256(DEFAULT_REPORT.encode()).hexdigest()
    assert Path(facts["report"]["per_run_path"]).parts[-3:] == ("runs", "cp01", "report.html") and facts["report"]["per_run_sha256"]
    assert (world.ctx.caliper_ws / "runs" / "cp01" / "report.html").is_file() and (world.ctx.caliper_ws / "report.html").read_text(encoding="utf-8") == DEFAULT_REPORT
    assert {"G-postrun", "G-evidence", "G-report"} <= {g["id"] for g in v["gates"]} and all(g["status"] == "PASS" for g in v["gates"] if g["id"] in ("G-postrun", "G-evidence", "G-report"))
    assert v["metrics"]["host"]["wsl"]["cpu_busy_pct"]["mean"] == pytest.approx(12.5) and v["metrics"]["host"]["windows"]["disk_mb_s"]["mean"] == pytest.approx(2.0)
    assert len(v["metrics"]["evidence"]) == 8 + 5 + 2 and all(e["ratio"] >= 0.95 for e in v["metrics"]["evidence"].values())     # 8 containers, 5 scrape targets, 2 host streams
    assert json.loads((rd / "config_check.json").read_text())["unchanged"] is True
    assert be.killed == []
    # the channel step ran before anything else touched the network
    first_tool = next(c for c in be.calls if len(c) > 2 and c[1] == "-B")
    assert first_tool[2].endswith("e1_channel_run.py")
    # the 8 watched containers: 3 orderers, 2 peers, caliper-benchmark-001 and the 2 peer-launched chaincode containers
    res_call = next(c for c in be.spawns if c[2].endswith("e1_resource_sampler.py"))
    assert res_call.count("--container") == 8 and ql.CALIPER in res_call and "caliper" not in res_call
    cal_call = next(c for c in be.spawns if c[:2] == ["docker", "exec"])
    assert cal_call[4] == ql.CALIPER


def test_run_folder_must_not_exist(world, patched):
    s, _ = make_session(world, patched)
    world.ctx.rd("cp01").mkdir(parents=True)
    with pytest.raises(FileExistsError):
        ql.execute_run(s, world.rows[0])


def test_channel_failure_is_invalid_infra_and_nothing_else_runs(world, patched):
    s, be = make_session(world, patched, fail_channel=True)
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "INVALID_INFRA" and v["step_failure"]["label"] == "channel"
    assert not any(len(c) > 2 and c[1] == "-B" and c[2].endswith(("e1_la2_render.py", "e1_metrics_scraper.py")) for c in be.calls)
    assert be.spawns == [] and be.killed == []


def test_watchdog_aborts_on_two_consecutive_health_failures(world, patched):
    s, be = make_session(world, patched)
    be.health_off = {9443: be.t + 40}                    # orderer1 stops answering shortly after the run starts
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "ABORTED_INFRA"
    assert be.killed == [ql.CALIPER]
    facts = json.loads((world.ctx.rd("cp01") / "facts.json").read_text())
    assert "health failed 2 consecutive polls" in facts["aborted"] and facts["caliper"]["exit_code"] == -9
    assert "stop_metrics" in labels_of(world.ctx.rd("cp01"))              # the background samplers were still stopped cleanly


def test_watchdog_aborts_on_container_restart_at_the_first_poll(world, patched):
    s, be = make_session(world, patched)
    real_sleep = be.sleep

    def sleeper(x):
        real_sleep(x)
        if be.t > 130:                                    # a peer restarts mid-run
            be.containers["peer0.org2.example.com"].update(started="2026-11-12T12:30:00Z", restarts=1)
    be.sleep = sleeper
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "ABORTED_INFRA" and be.killed == [ql.CALIPER]
    assert "peer0.org2.example.com: restarted" in json.loads((world.ctx.rd("cp01") / "facts.json").read_text())["aborted"]


def test_caliper_hard_timeout_aborts(world, patched):
    s, be = make_session(world, patched, caliper_s=10_000.0)
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "ABORTED_INFRA" and be.killed == [ql.CALIPER]
    assert json.loads((world.ctx.rd("cp01") / "facts.json").read_text())["caliper"]["timed_out"] is True


def test_caliper_nonzero_exit_is_invalid_infra(world, patched):
    s, be = make_session(world, patched, caliper_code=1)
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "INVALID_INFRA" and v["step_failure"]["label"] == "caliper" and be.killed == []


def test_config_change_during_the_run_is_invalid_infra(world, patched):
    s, be = make_session(world, patched, config_changes=True)
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "INVALID_INFRA" and "G-config-stable" in v["failed_gates"]


# ----- offline verdict variants: edit the golden run's files, re-evaluate
def golden(world, patched, **opt):
    s, be = make_session(world, patched, **opt)
    row = world.rows[0]
    v = ql.execute_run(s, row)
    return s, be, row, v, world.ctx.rd("cp01")


def reeval(world, rd, row, mutate=None):
    cp = world.tmp / f"copy_{time.time_ns()}"
    shutil.copytree(rd, cp)
    if mutate:
        mutate(cp)
    return ql.evaluate_run(cp, row, world.ctx.recorder_dir(row.run_id), json.loads((rd / "facts.json").read_text()))


def test_verdict_leader_change_is_invalid_infra(world, patched):
    _, _, row, v, rd = golden(world, patched)
    def m(cp):
        p = cp / "metrics/summary/metrics_summary.json"
        d = json.loads(p.read_text())
        d["derived"]["orderer2"]["leader_changes_delta"] = 1
        p.write_text(json.dumps(d))
    out = reeval(world, rd, row, m)
    assert out["verdict"] == "INVALID_INFRA" and out["failed_gates"] == ["V6-leader"]


def test_verdict_container_anomaly_and_clock_step_are_invalid_infra(world, patched):
    _, _, row, v, rd = golden(world, patched)
    def m(cp):
        p = cp / "resources/resource_manifest.json"
        d = json.loads(p.read_text())
        d["anomalies"]["error"] = {"container_restarted": 1}
        p.write_text(json.dumps(d))
    assert reeval(world, rd, row, m)["failed_gates"] == ["V6-containers"]
    mb = world.ctx.recorder_dir("cp01") / "cp01_w0_r1_m-cp01.manifest.json"          # now a wall-clock step inside the measured round
    d = json.loads(mb.read_text())
    d["clock"]["wall_minus_monotonic_ms"] = 2.5
    mb.write_text(json.dumps(d))
    out = ql.evaluate_run(rd, row, world.ctx.recorder_dir("cp01"), json.loads((rd / "facts.json").read_text()))
    assert out["verdict"] == "INVALID_INFRA" and "V7-clock" in out["failed_gates"]


def test_verdict_event_loop_above_the_v2_bound_is_invalid_schedule_but_not_a_client_label(world, patched):
    """The V2 validity gate (p99 <= 20 ms) still decides INVALID_SCHEDULE; the corrected rule does not read the probe as client saturation."""
    _, _, row, v, rd = golden(world, patched, el_p99=25.0)
    assert v["verdict"] == "INVALID_SCHEDULE" and v["failed_gates"] == ["V2-pacing"]
    assert v["labels"] == [] and "CLIENT_LIMITED" in v["labels_frozen_rule"] and v["class_v2"] == "HEALTHY"
    assert v["flags_v2"]["el_v2_gate_ok"] is False and v["flags_v2"]["v2_gate_pass"] is False
    assert not ql.ccv2.ramp_may_continue(v["verdict"], v["class_v2"])


def test_verdict_event_loop_above_10_ms_no_longer_labels_a_run(world, patched):
    """The P1a defect: an event-loop p99 of 12 ms (any value above the 10 ms resolution floor) labelled every run CLIENT_LIMITED."""
    _, _, row, v, rd = golden(world, patched, el_p99=12.0)
    assert v["verdict"] == "VALID" and v["labels"] == [] and v["class_v2"] == "HEALTHY"
    assert v["labels_frozen_rule"] == ["CLIENT_LIMITED"]                 # the frozen rule is still computed and kept for comparison


def test_verdict_caliper_cpu_at_the_line_is_client_limited(world, patched):
    _, _, row, v, rd = golden(world, patched, cpu=85.0)
    assert v["verdict"] == "VALID" and v["labels"] == ["CLIENT_LIMITED"] and v["class_v2"] == "CLIENT_LIMITED"
    assert "CLIENT_LIMITED" in v["labels_frozen_rule"] and v["flags_v2"]["client_cpu_ok"] is False


def test_verdict_actual_pacing_lateness_is_client_limited_and_invalid_schedule(world, patched):
    _, _, row, v, rd = golden(world, patched)
    assert v["verdict"] == "VALID" and v["labels"] == []

    def late(cp):                                           # every 5th transaction is created 40 ms late: p99 lateness far above 5 ms
        path = cp / "analysis" / "transaction_measurements.csv"
        rows = list(csv.DictReader(path.open(encoding="utf-8")))
        for i, r in enumerate(rows):
            if i % 5 == 4:
                r["create_ms"] = f"{float(r['create_ms']) + 40.0:.3f}"
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    out = reeval(world, rd, row, late)
    assert out["verdict"] == "INVALID_SCHEDULE" and out["failed_gates"] == ["V2-pacing"]
    assert out["labels"] == ["CLIENT_LIMITED"] and out["class_v2"] == "CLIENT_LIMITED" and out["flags_v2"]["lateness_ok"] is False


def test_verdict_saturation_label_allows_failures(world, patched):
    _, _, row, v, rd = golden(world, patched, invalid_every=3, backlog=500)
    assert v["labels"] == ["SATURATED"] and v["verdict"] == "VALID"          # failures are reported, not hidden
    assert v["metrics"]["failure_rate"] == pytest.approx(1 / 3, abs=0.01)


def test_verdict_failures_with_sustained_throughput_are_a_plant_fault(world, patched):
    # 2% of transactions are INVALID in the ledger, yet the interior VALID rate stays >= 0.95 of offered and the backlog is small:
    # that is not overload. The plan's SATURATED label still follows its own definition (failure rate > 1%), but the Failures gate
    # must not be vacuous (the plan's wording would exempt every run above 1%).
    _, _, row, v, rd = golden(world, patched, invalid_every=50, backlog=30)
    assert v["verdict"] == "INVALID_DATA" and v["failed_gates"] == ["G-failures"] and v["labels"] == ["SATURATED"]
    assert v["metrics"]["interior_valid_rate_tps"] / row.rate >= 0.95


def test_verdict_reconciliation_failure_is_invalid_data(world, patched):
    _, _, row, v, rd = golden(world, patched, reconcile="INVALID")
    assert v["verdict"] == "INVALID_DATA" and v["failed_gates"] == ["G-reconcile"]


def test_verdict_missing_files_are_not_evaluated_not_passed(world, patched):
    _, _, row, v, rd = golden(world, patched)
    def m(cp):
        (cp / "analysis" / "summary.json").unlink()
        shutil.rmtree(cp / "resources")
    out = reeval(world, rd, row, m)
    assert out["verdict"] == "INVALID_DATA" and {"G-reconcile", "G-telemetry", "V6-containers"} <= set(out["not_evaluated_gates"])


# ====================================================================================== series rules
def stub_series(monkeypatch, world, verdicts, prechecks=None, anchors=None, max_rate=None):
    s = ql.Session(world.ctx, SimpleNamespace(now_utc=lambda: "t"), max_rate=max_rate,
                   anchor_classes=anchors if anchors is not None else {"ql17": "HEALTHY", "ql23": "HEALTHY"})
    s.log = lambda m: None
    ran = []

    def fake_exec(sess, row):
        ran.append(row.run_id)
        v = verdicts(row) if callable(verdicts) else verdicts.get(row.run_id, {"verdict": "VALID", "labels": []})
        v = {"run_id": row.run_id, "channel": row.channel, "failed_gates": [], **v}
        v.setdefault("class_v2", "+".join(v.get("labels", [])) or "HEALTHY")             # what evaluate_run would have derived from the labels
        return v
    monkeypatch.setattr(ql, "execute_run", fake_exec)
    monkeypatch.setattr(ql, "precheck", prechecks or (lambda sess, row: [("ok", True, "")]))
    return s, ran


def chain(world, B):
    return [r.run_id for r in world.rows if r.B == B]


def test_series_runs_everything_when_all_valid(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {})
    res = ql.run_series(s, world.rows)
    assert ran == [r.run_id for r in world.rows] and len(res) == 10
    assert chain(world, 40) == ["cp01", "cp03", "cp05", "cp08", "cp09"] and chain(world, 10) == ["cp02", "cp04", "cp06", "cp07", "cp10"]


def test_ramp_stops_climbing_after_saturation_and_skips_the_rest_of_that_chain_only(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {"cp05": {"verdict": "VALID", "labels": ["SATURATED"]}})          # B=40 at 25 tx/s
    res = ql.run_series(s, world.rows)
    skipped = [v["run_id"] for v in res if v["verdict"] == "SKIPPED_PRECONDITION"]
    assert skipped == ["cp08", "cp09"] and all(r in ran for r in chain(world, 10))                              # the B=10 ramp is independent
    assert (world.ctx.root / "skipped" / "cp08.json").is_file() and "cp05" in ran and "cp08" not in ran
    assert "cp05 is VALID / SATURATED" in next(v for v in res if v["run_id"] == "cp08")["reason"]


def test_ramp_stops_on_client_limited_and_on_invalid_schedule_and_on_missing_evidence(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {"cp04": {"verdict": "VALID", "labels": ["CLIENT_LIMITED"]}})       # B=10 at 20 tx/s
    res = ql.run_series(s, world.rows)
    assert [v["run_id"] for v in res if v["verdict"] == "SKIPPED_PRECONDITION"] == [r for r in chain(world, 10) if r > "cp04"]
    shutil.rmtree(world.ctx.root / "skipped")
    s, ran = stub_series(monkeypatch, world, {"cp03": {"verdict": "INVALID_SCHEDULE", "labels": [], "class_v2": "HEALTHY"}})   # B=40 at 20 tx/s
    res = ql.run_series(s, world.rows)
    assert [v["run_id"] for v in res if v["verdict"] == "SKIPPED_PRECONDITION"] == [r for r in chain(world, 40) if r > "cp03"]
    shutil.rmtree(world.ctx.root / "skipped")
    s, ran = stub_series(monkeypatch, world, {"cp04": {"verdict": "VALID", "labels": [], "class_v2": "UNDETERMINED"}})
    res = ql.run_series(s, world.rows)
    assert [v["run_id"] for v in res if v["verdict"] == "SKIPPED_PRECONDITION"] == [r for r in chain(world, 10) if r > "cp04"]


def test_first_rung_runs_only_if_its_p1a_anchor_is_healthy_in_the_erratum_table(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {}, anchors={"ql17": "CLIENT_LIMITED", "ql23": "HEALTHY"})
    res = ql.run_series(s, world.rows)
    assert "cp01" not in ran and "cp02" in ran and set(chain(world, 40)).isdisjoint(ran) and set(chain(world, 10)) <= set(ran)
    assert "anchor ql17 is CLIENT_LIMITED in the erratum table" in next(v for v in res if v["run_id"] == "cp01")["reason"]
    shutil.rmtree(world.ctx.root / "skipped")
    s, ran = stub_series(monkeypatch, world, {}, anchors={"ql23": "HEALTHY"})                  # no entry for ql17
    res = ql.run_series(s, world.rows)
    assert "cp01" not in ran and "no entry in the pinned erratum table" in next(v for v in res if v["run_id"] == "cp01")["reason"]


def test_the_approval_cap_skips_every_rung_above_it(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {}, max_rate=25)
    res = ql.run_series(s, world.rows)
    assert ran == [r.run_id for r in world.rows if r.rate <= 25] and len(ran) == 6                # a lower cap than Tier 1's own bound is honoured
    skipped = [v for v in res if v["verdict"] == "SKIPPED_PRECONDITION"]
    assert len(skipped) == 4 and all("approved cap max_rate_tps=25" in v["reason"] for v in skipped)
    shutil.rmtree(world.ctx.root / "skipped")
    s, ran = stub_series(monkeypatch, world, {}, max_rate=40)
    res = ql.run_series(s, world.rows)
    assert len(ran) == 10 and not [v for v in res if v["verdict"] == "SKIPPED_PRECONDITION"]        # the Tier 1 cap lets all ten skipped P1a rungs run


@pytest.mark.parametrize("bad", ["INVALID_INFRA", "ABORTED_INFRA", "DRIVER_ERROR"])
def test_series_halts_on_infrastructure_verdicts(world, monkeypatch, bad):
    s, ran = stub_series(monkeypatch, world, {"cp03": {"verdict": bad, "labels": []}})
    with pytest.raises(ql.HaltSeries, match="cp03"):
        ql.run_series(s, world.rows)
    assert ran == ["cp01", "cp02", "cp03"]


def test_series_halts_after_two_consecutive_invalid_runs(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {"cp04": {"verdict": "INVALID_DATA", "labels": []}, "cp05": {"verdict": "INVALID_SCHEDULE", "labels": []}})
    with pytest.raises(ql.HaltSeries, match="consecutive"):
        ql.run_series(s, world.rows)
    assert ran[-1] == "cp05"
    s2, ran2 = stub_series(monkeypatch, world, {"cp04": {"verdict": "INVALID_DATA", "labels": []}})
    ql.run_series(s2, world.rows)                           # a single INVALID_DATA does not halt; only its own ramp stops climbing
    assert len(ran2) == 10 - len([r for r in chain(world, 10) if r > "cp04"]) == 7


def test_series_halts_when_the_first_run_is_not_valid(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {"cp01": {"verdict": "INVALID_DATA", "labels": []}})
    with pytest.raises(ql.HaltSeries, match="first run"):
        ql.run_series(s, world.rows)
    assert ran == ["cp01"]


def test_series_halts_on_failed_precheck_before_starting_the_run(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {}, prechecks=lambda sess, row: [("five /healthz == 200", row.seq != 2, "")])
    with pytest.raises(ql.HaltSeries, match="pre-run safety check failed"):
        ql.run_series(s, world.rows)
    assert ran == ["cp01"]


def test_series_driver_exception_becomes_a_halt(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {})

    def boom(sess, row):
        raise RuntimeError("boom")
    monkeypatch.setattr(ql, "execute_run", boom)
    with pytest.raises(ql.HaltSeries, match="driver error"):
        ql.run_series(s, world.rows)
    assert s.verdicts["cp01"]["verdict"] == "DRIVER_ERROR"


def test_only_and_from_seq_filters(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {})
    ql.run_series(s, world.rows, only={"cp01", "cp02"})
    assert ran == ["cp01", "cp02"]
    s2, ran2 = stub_series(monkeypatch, world, {})
    res2 = ql.run_series(s2, world.rows, from_seq=9)
    assert ran2 == [] and [v["verdict"] for v in res2] == ["SKIPPED_PRECONDITION"] * 2        # ramp rungs never run without their predecessor: no resume mode


def test_precheck_requires_the_keep_awake_helper_and_the_clock_probe_to_be_alive(world, patched):
    """A4: found when the pre-flight helper of 11:26Z was gone by 14:07Z without any sleep event: a dead helper must halt the series."""
    s, be = make_session(world, patched)
    s.probe = FakeProc(be)                                               # running
    assert all(ok for _, ok, _ in ql.precheck(s, world.rows[0]))
    alive = be.host_power
    be.host_power = lambda pid=None: {"line": "Online", "battery": 79, "helper_alive": False}
    assert {n for n, ok, _ in ql.precheck(s, world.rows[0]) if not ok} == {"keep-awake helper alive (pid 4242)"}
    be.host_power = alive
    s.probe.kill()
    assert {n for n, ok, _ in ql.precheck(s, world.rows[0]) if not ok} == {"continuous clock probe running"}
    s.probe, s.helper_pid = None, None                                   # no helper was ever recorded: fails, no probe handle: not checked
    assert {n for n, ok, _ in ql.precheck(s, world.rows[0]) if not ok} == {"keep-awake helper alive (pid None)"}


def test_precheck_reports_each_failure(world, patched):
    s, be = make_session(world, patched)
    assert all(ok for _, ok, _ in ql.precheck(s, world.rows[0]))
    be.health_off = {9444: 0.0}
    be.containers["orderer.example.com"]["restarts"] = 1
    be.opt["wsl"] = 500.0
    bad = {n for n, ok, _ in ql.precheck(s, world.rows[0]) if not ok}
    assert {"five /healthz == 200", "no container restarted, replaced or exited since session start"} <= bad and any("WSL" in n for n in bad)
    world.ctx.rd("cp01").mkdir(parents=True)
    assert "run output folders absent" in {n for n, ok, _ in ql.precheck(s, world.rows[0]) if not ok}


# ====================================================================================== preflight gates
S1_OK = ("SOURCE_ADDED repo_scripts/e1/e1_client_classification_v2.py\nSOURCE_ADDED   repo_scripts/e1/e1_p1b_matrix.py\nSOURCE_ADDED repo_scripts/e1/e1_quicklook_p1b.py\n"
         "SOURCE_ADDED repo_scripts/e1/e1_host_sampler.py\n"
         "SOURCE_ADDED repo_tests/e1/test_e1_client_classification_v2.py\nSOURCE_ADDED repo_tests/e1/test_e1_p1b_matrix.py\nSOURCE_ADDED repo_tests/e1/test_e1_quicklook_p1b.py\n"
         "SOURCE_ADDED repo_tests/e1/test_e1_host_sampler.py\n"
         "verify: checked=40000 mismatched=0 missing=0 unlisted=0 source_drift=8\n")


def test_gate_s1_accepts_only_the_authorised_drift(world, tmp_path):
    be = FakeBackend(world)
    ev = tmp_path / "s1.txt"
    ev.write_text(S1_OK, encoding="utf-8")
    assert ql.gate_s1(world.ctx, be, ev).status == "PASS"
    assert "repo_scripts/e1/e1_host_sampler.py" in ql.S1_ALLOWED_DRIFT and "repo_tests/e1/test_e1_host_sampler.py" in ql.S1_ALLOWED_DRIFT and len(ql.S1_ALLOWED_DRIFT) == 8
    ev_cache = tmp_path / "s1_cache.txt"       # the documented hazard: a bytecode or cache file in a source group is drift, and the gate must refuse it
    ev_cache.write_text(S1_OK.replace("source_drift=8", "source_drift=9") + "SOURCE_ADDED repo_scripts/e1/__pycache__/e1_host_sampler.cpython-312.pyc\n", encoding="utf-8")
    gc = ql.gate_s1(world.ctx, be, ev_cache)
    assert gc.status == "FAIL" and "__pycache__" in gc.detail
    ev2 = tmp_path / "s1b.txt"
    ev2.write_text(S1_OK.replace("source_drift=8", "source_drift=9") + "SOURCE_CHANGED repo_src/fabric_perf/x.py\n", encoding="utf-8")
    g = ql.gate_s1(world.ctx, be, ev2)
    assert g.status == "FAIL" and "repo_src/fabric_perf/x.py" in g.detail
    ev3 = tmp_path / "s1c.txt"
    ev3.write_text(S1_OK.replace("mismatched=0", "mismatched=2"), encoding="utf-8")
    assert ql.gate_s1(world.ctx, be, ev3).status == "FAIL"
    ev4 = tmp_path / "s1d.txt"
    ev4.write_text("nothing useful", encoding="utf-8")
    assert ql.gate_s1(world.ctx, be, ev4).status == "FAIL"


def test_gate_table_s1_line_states_the_real_drift_count():
    # found by the Tier 1 dry-run listing: the S1 line said '6 owner-approved P1b files' after the host sampler raised the list to 8
    line = dict((g[0], g[2]) for g in ql.GATE_TABLE)["S1"]
    assert f"drift limited to the {len(ql.S1_ALLOWED_DRIFT)} owner-approved P1b files" in line
    assert "the 6 owner-approved" not in line


def test_gate_s1_executes_the_windows_verifier_when_no_evidence_is_given(world):
    be = FakeBackend(world, s1_text=S1_OK)
    assert ql.gate_s1(world.ctx, be).status == "PASS"
    cmd = be.calls[-1]
    assert cmd[0] == "C:\\python.exe" and cmd[2:4] == ["verify", "--snapshot"] and cmd[-1] == "--check-sources"


def test_default_windows_interpreter_is_exec_able_from_wsl_but_its_arguments_are_windows_paths():
    """Regression (found by the first real pre-flight): argv[0] 'C:\\...\\python.exe' cannot be exec'd from WSL (FileNotFoundError)."""
    c = ql.Ctx.default()
    assert c.win_python.startswith("/mnt/c/") and c.win_python.endswith("/python.exe") and "\\" not in c.win_python
    assert c.win_repo[1:3] == ":\\" and c.win_snapshot[1:3] == ":\\"


def test_gate_s1_failure_carries_a_diagnostic_not_just_no_summary(world):
    be = FakeBackend(world, s1_text="Traceback (most recent call last):\nOSError: nope")
    g = ql.gate_s1(world.ctx, be)
    assert g.status == "FAIL" and "no verify summary line" in g.detail and "exit 0" in g.detail and "OSError: nope" in g.detail
    unrunnable = FakeBackend(world)
    unrunnable.run = lambda argv, env=None, timeout=None: SimpleNamespace(code=None, stdout=b"", stderr=b"FileNotFoundError(2, 'No such file')", started_utc="", ended_utc="", duration_s=0.0, timed_out=False)
    g2 = ql.gate_s1(world.ctx, unrunnable)
    assert g2.status == "FAIL" and "exit None" in g2.detail and "FileNotFoundError" in g2.detail


def test_gate_s2_needs_ac_and_a_live_helper(world):
    be = FakeBackend(world)
    assert ql.gate_s2(be, 4242).status == "PASS" and ql.gate_s2(be, None).status == "FAIL"
    be.host_power = lambda pid=None: {"line": "Offline", "battery": 50, "helper_alive": True}
    assert ql.gate_s2(be, 4242).status == "FAIL"


def test_gate_s5_s6_s8(world):
    be = FakeBackend(world)
    g, base, cc = ql.gate_s5(world.ctx, be)
    assert g.status == "PASS" and len(cc) == 2 and ql.CALIPER in base
    be.health_off = {9445: 0.0}
    assert ql.gate_s5(world.ctx, be)[0].status == "FAIL"
    net = cr.Network(world.net_doc)
    assert ql.gate_s6(world.ctx, FakeBackend(world), net).status == "PASS"
    be2 = FakeBackend(world)
    be2.http_text = lambda url, timeout=5.0: 'consensus_etcdraft_active_nodes{channel="e1-r002"} 2\nconsensus_etcdraft_cluster_size{channel="e1-r002"} 3\n'
    assert ql.gate_s6(world.ctx, be2, net).status == "FAIL"
    assert ql.gate_s8(world.ctx, FakeBackend(world), net, world.rows).status == "PASS"
    clash = ql.gate_s8(world.ctx, FakeBackend(world, listed=["e1-r002", "e1-cp05-b40-l25"]), net, world.rows)
    assert clash.status == "FAIL" and "e1-cp05-b40-l25" in clash.detail


def _probe_dir(tmp_path, *, verdict="PASS", boot="boot-1", dur=1800.0, age_h=0.1):
    d = tmp_path / "probe"
    d.mkdir()
    (d / "probe_manifest.json").write_text(json.dumps({"boot_id": boot, "parameters": {"duration_s": dur},
                                                       "end": {"real": time.time_ns() - int(age_h * 3.6e12)}}), encoding="utf-8")
    (d / "clock_gate.json").write_text(json.dumps({"verdict": verdict, "criteria": {}, "validity": {}}), encoding="utf-8")
    return d


def test_gate_s9_variants(world, tmp_path):
    be = FakeBackend(world)
    assert ql.gate_s9(world.ctx, be, _probe_dir(tmp_path)).status == "PASS"
    assert ql.gate_s9(world.ctx, be, None).status == "FAIL"
    for i, kw in enumerate(({"verdict": "FAIL"}, {"boot": "boot-0"}, {"dur": 900.0}, {"age_h": 30.0})):
        sub = tmp_path / f"v{i}"
        sub.mkdir()
        assert ql.gate_s9(world.ctx, be, _probe_dir(sub, **kw)).status == "FAIL", kw


def test_gate_s10_s11(world):
    net = cr.Network(world.net_doc)
    out = world.tmp / "pf"
    out.mkdir()
    g = ql.gate_s10(world.ctx, net, out)
    assert g.status == "PASS" and (out / "tool_hashes.json").is_file(), g.detail
    hashes = json.loads((out / "tool_hashes.json").read_text())
    assert "scripts/e1/e1_quicklook_p1b.py" in hashes and "scripts/e1/e1_client_classification_v2.py" in hashes and "scripts/e1/e1_p1b_matrix.py" in hashes
    assert "scripts/e1/e1_host_sampler.py" in hashes and hashes["scripts/e1/e1_host_sampler.py"] == hashlib.sha256((REPO / "scripts/e1/e1_host_sampler.py").read_bytes()).hexdigest()
    assert hashes["p1a_corrected_classification.csv"] == world.erratum_sha
    assert hashes["erratum_E1-QL-ERR-001_signed_document"] == hashlib.sha256(world.ctx.erratum_doc.read_bytes()).hexdigest()      # recorded, not pinned
    gone = world.ctx.erratum_doc.with_name("gone.md")
    import dataclasses
    assert "erratum_E1-QL-ERR-001_signed_document" in ql.gate_s10(dataclasses.replace(world.ctx, erratum_doc=gone), net, world.tmp / "pf4").detail     # a missing document fails S10
    world.ctx.erratum_csv.write_bytes(world.ctx.erratum_csv.read_bytes() + b"ql99,HEALTHY\n")       # the pinned erratum table changed
    bad = ql.gate_s10(world.ctx, net, world.tmp / "pf3")
    assert bad.status == "FAIL" and "erratum table hash ok: False" in bad.detail
    world.ctx.erratum_csv.write_bytes(world.ctx.erratum_csv.read_bytes()[:-len(b"ql99,HEALTHY\n")])
    world.ctx.matrix.write_bytes(matrix_bytes() + b"\n")
    assert ql.gate_s10(world.ctx, net, world.tmp / "pf2").status == "FAIL"
    assert ql.gate_s11(world.ctx, world.rows).status == "PASS"
    (world.ctx.caliper_ws / "runs" / "cp07").mkdir(parents=True)
    assert ql.gate_s11(world.ctx, world.rows).status == "FAIL"


def test_fingerprint_and_compare(world, patched):
    be = FakeBackend(world)
    net = cr.Network(world.net_doc)
    a = ql.fingerprint(world.ctx, be, net, world.tmp / "fp")
    assert set(a) == set(ql.FINGERPRINT_CHANNELS) and a["e1-r002"]["height"] == 7 and a["e1-r002"]["config_block_sha256"]
    assert ql.compare_fingerprints(a, a) == []
    b = json.loads(json.dumps(a))
    b["e1-cfg001"]["height"] = 8
    assert ql.compare_fingerprints(a, b) == ["e1-cfg001.height: 7 -> 8"]


def test_full_preflight_with_the_fake_world_passes_and_writes_a_report(world, tmp_path):
    be = FakeBackend(world, s1_text=S1_OK)
    probe = _probe_dir(tmp_path)
    results, report = ql.run_preflight(world.ctx, be, world.rows, tmp_path / "preflight", clock_dir=probe, helper_pid=4242)
    assert report["all_pass"] and [g.id for g in results] == ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11"], \
        [(g.id, g.status, g.detail) for g in results if g.status != "PASS"]
    assert (tmp_path / "preflight" / "preflight_report.json").is_file() and report["context"]["chaincode"]
    with pytest.raises(FileExistsError):
        ql.run_preflight(world.ctx, be, world.rows, tmp_path / "preflight", clock_dir=probe, helper_pid=4242)


def test_preflight_gate_exceptions_become_failures_not_crashes(world, tmp_path):
    be = FakeBackend(world)
    be.host_memory = lambda s, i: (_ for _ in ()).throw(RuntimeError("typeperf missing"))
    results, report = ql.run_preflight(world.ctx, be, world.rows, tmp_path / "pf", clock_dir=None, helper_pid=None, gates={"S3", "S9"})
    assert not report["all_pass"] and {g.id: g.status for g in results} == {"S3": "FAIL", "S9": "FAIL"}


# ====================================================================================== interlocks and dry-run
def _approval(world, tmp_path, name="ok.txt", cap=40, erratum=True, matrix=True, owner=True, tier=True, watchdog=True):
    lines = (["approved_by=owner"] if owner else []) + ([f"matrix_sha256={ql.MATRIX_SHA256}"] if matrix else []) + \
            ([f"erratum_csv_sha256={world.erratum_sha}"] if erratum else []) + ([f"max_rate_tps={cap}"] if cap is not None else []) + \
            (["tier=1"] if tier else []) + ([f"watchdog_mode={ql.WATCHDOG_MODE}"] if watchdog else [])
    p = tmp_path / name
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


def test_run_is_refused_without_an_owner_approval_file(world, tmp_path):
    with pytest.raises(SystemExit, match="approval"):
        ql.check_approval(None)
    with pytest.raises(SystemExit, match="approval"):
        ql.check_approval(tmp_path / "missing.txt")
    with pytest.raises(SystemExit, match="matrix_sha256"):
        ql.check_approval(_approval(world, tmp_path, "a.txt", matrix=False))
    with pytest.raises(SystemExit, match="approved_by=owner"):
        ql.check_approval(_approval(world, tmp_path, "b.txt", owner=False))
    with pytest.raises(SystemExit, match="erratum_csv_sha256"):
        ql.check_approval(_approval(world, tmp_path, "c.txt", erratum=False))
    with pytest.raises(SystemExit, match="max_rate_tps"):
        ql.check_approval(_approval(world, tmp_path, "d.txt", cap=None))
    with pytest.raises(SystemExit, match="max_rate_tps"):
        ql.check_approval(_approval(world, tmp_path, "e.txt", cap=45))            # not a rung of the matrix
    good = _approval(world, tmp_path, "g.txt", cap=40)
    text = ql.check_approval(good)
    assert "approved_by=owner" in text and ql.approved_max_rate(text) == 40


def test_the_approval_must_name_tier_one_and_the_bounds_only_watchdog_mode(world, tmp_path):
    with pytest.raises(SystemExit, match="tier=1"):
        ql.check_approval(_approval(world, tmp_path, "t.txt", tier=False))
    with pytest.raises(SystemExit, match=f"watchdog_mode={ql.WATCHDOG_MODE}"):
        ql.check_approval(_approval(world, tmp_path, "w.txt", watchdog=False))
    bad = tmp_path / "w2.txt"                                                       # a different mode name is not the owner's conditional acceptance
    bad.write_text(_approval(world, tmp_path, "w3.txt").read_text().replace(ql.WATCHDOG_MODE, "bounds_only_tier2"), encoding="utf-8")
    with pytest.raises(SystemExit, match="watchdog_mode"):
        ql.check_approval(bad)
    assert ql.WATCHDOG_MODE == "bounds_only_tier1"
    inline = tmp_path / "w4.txt"                                                    # a mention inside another line is not a line of its own
    inline.write_text(_approval(world, tmp_path, "w5.txt", tier=False).read_text() + "note: tier=1 would be wrong here\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="tier=1"):
        ql.check_approval(inline)


@pytest.mark.parametrize("cap", [45, 50, 60, 100, 200, 1000])
def test_a_cap_above_tier_one_is_refused_with_the_tier_two_reason(world, tmp_path, cap):
    with pytest.raises(SystemExit) as e:
        ql.check_approval(_approval(world, tmp_path, f"cap{cap}.txt", cap=cap))
    msg = str(e.value)
    if cap > ql.TIER1_MAX_RATE_TPS:
        assert "above the Tier 1 bound of 40 tx/s" in msg and "stronger overload monitoring" in msg
    for ok in (15, 20, 25, 30, 40):
        assert ql.approved_max_rate(f"max_rate_tps={ok}\n") == ok


def test_anchor_table_must_match_the_pinned_hash(world, tmp_path):
    assert ql.load_anchor_classes(world.ctx.erratum_csv) == {"ql17": "HEALTHY", "ql23": "HEALTHY", "ql01": "HEALTHY"}
    other = tmp_path / "other.csv"
    other.write_text("run_id,corrected_class\nql17,HEALTHY\n", encoding="utf-8")
    with pytest.raises(ql.MatrixError, match="pinned"):
        ql.load_anchor_classes(other)
    with pytest.raises(ql.MatrixError, match="not found"):
        ql.load_anchor_classes(tmp_path / "nope.csv")


def test_run_without_execute_does_nothing(world, tmp_path, capsys):
    good = _approval(world, tmp_path)
    be = FakeBackend(world)
    args = SimpleNamespace(execute=False, approval_file=good, out_dir=tmp_path / "o", clock_dir=None, only=None, from_seq=1, s1_evidence=None)
    assert ql.cmd_run(args, world.ctx, be) == 2
    assert be.calls == [] and not world.ctx.root.exists() and not (tmp_path / "o").exists()


def test_run_is_refused_while_instrumentation_blockers_are_open(world, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(ql, "BLOCKERS_OPEN", ("B2 live backlog watchdog (test)",))
    be = FakeBackend(world)
    args = SimpleNamespace(execute=True, approval_file=_approval(world, tmp_path), out_dir=tmp_path / "o", clock_dir=None, only=None, from_seq=1, s1_evidence=None)
    assert ql.cmd_run(args, world.ctx, be) == 2
    assert "open instrumentation blockers" in capsys.readouterr().out
    assert be.calls == [] and not world.ctx.root.exists() and not (tmp_path / "o").exists() and be.spawns == []


def test_the_real_driver_has_cleared_b3_and_b4_and_names_what_tier_two_still_lacks():
    """B3 (host sampler) and B4 (per-run report path) are implemented in v0.2, so the packaged driver has no open blocker for Tier 1 (the monkeypatch in the
    world fixture is test-only). B2 was accepted for Tier 1 only: what a higher rate still needs is recorded, not forgotten."""
    assert REAL_BLOCKERS == () and ql.BLOCKERS_OPEN == ()
    assert len(ql.TIER2_BLOCKERS) == 2 and "stronger overload monitoring" in ql.TIER2_BLOCKERS[0] and "conditional on Tier 1 results" in ql.TIER2_BLOCKERS[1]


# ====================================================================================== the erratum interlock (the owner's adoption is mechanical, not assumed)
UNSIGNED_SECTION_11 = ("## 11. Owner sign-off (not signed)\n```\nerratum_id=E1-QL-ERR-001\nderived_csv_sha256={sha}\n"
                       "adopted_by=<the owner writes this line to adopt the erratum>\nadopted_on=<date>\n```\n")


def test_erratum_adoption_reads_section_11_as_lines_and_refuses_placeholders(world):
    sha = world.erratum_sha
    assert ql.erratum_adoption(erratum_text(sha))["signed"] is True
    unsigned = UNSIGNED_SECTION_11.format(sha=sha)
    a = ql.erratum_adoption(unsigned)
    assert a["signed"] is False and a["problems"] == ["adopted_by is not filled in", "adopted_on is not filled in"]
    half = ql.erratum_adoption(unsigned.replace("<date>", "2026-11-13"))
    assert half["signed"] is False and half["problems"] == ["adopted_by is not filled in"]
    angle = ql.erratum_adoption(erratum_text(sha, by="<Aryan Prasad>"))                              # a placeholder that was only partly replaced is still a placeholder
    assert angle["signed"] is False
    assert ql.erratum_adoption(erratum_text(sha).replace(sha, "0" * 64))["problems"] == ["derived_csv_sha256 is not the pinned derived-table hash"]
    assert ql.erratum_adoption(erratum_text(sha).replace("E1-QL-ERR-001\nderived", "E1-QL-ERR-002\nderived"))["problems"][0].startswith("erratum_id is 'E1-QL-ERR-002'")
    assert ql.erratum_adoption("")["signed"] is False and len(ql.erratum_adoption("")["problems"]) == 4
    # prose that merely mentions the key inside a sentence is not a signature line (the stand-in document contains such a sentence)
    assert "adopted_by=" in erratum_text(sha).split("## 11")[0] and ql.erratum_adoption(unsigned)["adopted_by"].startswith("<")


def test_check_erratum_adopted_refuses_a_missing_or_unsigned_document(world, tmp_path):
    assert ql.check_erratum_adopted(world.ctx.erratum_doc)["adopted_by"] == "Test Owner"
    with pytest.raises(SystemExit, match="was not found"):
        ql.check_erratum_adopted(tmp_path / "absent.md")
    unsigned = tmp_path / "unsigned.md"
    unsigned.write_text(UNSIGNED_SECTION_11.format(sha=world.erratum_sha), encoding="utf-8")
    with pytest.raises(SystemExit, match="section 11 of the erratum document is not signed"):
        ql.check_erratum_adopted(unsigned)


def test_run_is_refused_before_anything_starts_when_the_erratum_is_unsigned(world, tmp_path, monkeypatch):
    monkeypatch.setattr(ql, "is_linux", lambda: True)
    world.ctx.erratum_doc.write_text(UNSIGNED_SECTION_11.format(sha=world.erratum_sha), encoding="utf-8")
    be = FakeBackend(world, s1_text=S1_OK)
    args = SimpleNamespace(execute=True, approval_file=_approval(world, tmp_path), out_dir=tmp_path / "o", clock_dir=None, only=None, from_seq=1, s1_evidence=None)
    with pytest.raises(SystemExit, match="not signed"):
        ql.cmd_run(args, world.ctx, be)
    assert be.calls == [] and be.spawns == [] and not world.ctx.root.exists() and not (tmp_path / "o").exists()          # no helper, no gate, no folder


def test_dry_run_lists_all_10_tier_1_runs_deterministically_and_writes_nothing(world, monkeypatch):
    net = cr.Network(world.net_doc)
    before = sorted(p.name for p in world.tmp.rglob("*"))
    t1 = ql.build_dry_run(world.rows, world.ctx, net, world.env_doc, ql.design_findings(world.rows), None)
    t2 = ql.build_dry_run(world.rows, world.ctx, net, world.env_doc, ql.design_findings(world.rows), None)
    assert sorted(p.name for p in world.tmp.rglob("*")) == before
    strip = lambda t: "\n".join(x for x in t.splitlines() if not x.startswith("generated_utc"))
    assert strip(t1) == strip(t2)
    for r in world.rows:
        assert f"### {r.run_id}  channel {r.channel}" in t1
    for needle in ("DRY RUN", "ABORT", "docker kill caliper-benchmark-001",
                   "e1_channel_run.py", "e1_block_fetch.py", "--fresh-channel", "timeout 300 docker exec -w /hyperledger/caliper/workspace caliper-benchmark-001",
                   "Operationalisations", "S9", "Failures gate", "Caliper container",
                   "Continuous clock probe (A4)", "clock_series_check.json", "no manifest",
                   "erratum E1-QL-ERR-001", "HEALTHY", "UNDETERMINED", "labels_frozen_rule", "max_rate_tps", "P1a anchor run", "predicted batch wait W",
                   "post-P1a snapshot intact", "none of the 10 names present", "rung steps of at most 1.5x",
                   # v0.2: the owner's conditional acceptance, B3, B4, the new gates, the interlocks
                   "## I. Instrumentation, blockers and interlocks", "BLOCKERS_OPEN: 0 open", "B3 host sampler: IMPLEMENTED", "B4 per-run Caliper report path: IMPLEMENTED",
                   "B2 live backlog watchdog: NOT implemented; accepted for Tier 1 only", "Erratum interlock", "watchdog_mode=bounds_only_tier1",
                   "04c host sampler (B3;", "e1_host_sampler.py", "--max-seconds 420", "stop_host", "--caliper-report-path runs/cp01/report.html",
                   "$CALWS/runs/cp01/report.html", "G-postrun", "G-evidence", "G-report", "plan 7.8 (E)", "Tier 2 is not part of this series and is not authorised",
                   "NO live backlog watchdog exists", "B2.1 ", "B2.6 ", "docker-desktop distribution's network namespace is not visible"):
        assert needle in t1, needle
    assert "None" not in t1 and t1.count("### cp") == 10 and "### ql" not in t1
    assert "CLIENT_LIMITED = event-loop p99 > 10 ms" not in t1                                  # the withdrawn clause is not described as a label rule
    assert "docker exec -w /hyperledger/caliper/workspace caliper caliper" not in t1          # never the stopped E0-0 container
    assert "OPEN BLOCKER" not in t1 and "OPEN B2" not in t1                                    # nothing in the listing still calls B2/B3/B4 open blockers
    # Tier 2 is not in the executable listing: no run above 40 tx/s, no 26-run language
    assert not any(f"rate={n} tx/s" in t1 for n in (60, 80, 100, 120, 140, 160, 180, 200)) and "26 runs" not in t1 and "none of the 26" not in t1


def test_dry_run_with_a_blocker_still_open_says_so(world, monkeypatch):
    monkeypatch.setattr(ql, "BLOCKERS_OPEN", ("B9 something (test)",))
    t = ql.build_dry_run(world.rows, world.ctx, cr.Network(world.net_doc), None, [], None, only={"cp01"})
    assert "BLOCKERS_OPEN: 1 open: B9 something (test)" in t


def test_dry_run_can_list_a_subset(world):
    net = cr.Network(world.net_doc)
    t = ql.build_dry_run(world.rows, world.ctx, net, None, [], None, only={"cp04"})
    assert "### cp04" in t and "### cp05" not in t and "<docker exec argv from the rendered plan.json" in t


# ====================================================================================== B2: the owner's six conditions for bounds-only Tier 1, each pinned
OWNER_CONDITIONS = ("Offered load and run duration strictly bounded.", "Five-second infrastructure health polling remains active.",
                    "Existing timeout, container restart, exit and OOM aborts remain active.",
                    "Post-run backlog, transaction validity and ledger reconciliation are mandatory.",
                    "Any infrastructure fault halts the entire series.", "Tier 2 remains unauthorized without stronger overload monitoring.")


def test_the_six_conditions_in_the_code_are_the_owners_six_sentences():
    assert [c.rstrip(".").lower() for c in OWNER_CONDITIONS] == [c.lower() for c, _ in ql.B2_BOUNDS_CONDITIONS]
    assert all(how.strip() for _, how in ql.B2_BOUNDS_CONDITIONS)


def test_condition_1_load_and_duration_are_bounded_by_the_pinned_matrix_the_cap_and_the_hard_timeout(world, tmp_path):
    assert max(r.rate for r in world.rows) == 40 == ql.TIER1_MAX_RATE_TPS and ql.ALLOWED_RATES == (15, 20, 25, 30, 40) and ql.ROW_COUNT == 10
    assert {(r.warmup_s, r.measured_s, r.tau, r.P) for r in world.rows} == {(30, 120, "2s", 10485760)} and (ql.WARMUP_S, ql.MEASURED_S) == (30, 120)
    assert ql.THR.caliper_timeout_s == 300.0
    with pytest.raises(SystemExit, match="above the Tier 1 bound"):
        ql.check_approval(_approval(world, tmp_path, "c60.txt", cap=60))


def test_conditions_2_and_3_the_health_polling_and_abort_code_is_byte_identical_to_the_frozen_driver_that_ran_p1a():
    """'Existing ... remain active': the code that polls every 5 s and aborts on timeout, restart, exit and OOM is not a copy, it is the same text."""
    assert ql.THR.health_poll_s == 5.0 and ql.THR.health_consecutive_fail == 2 and ql.THR.caliper_timeout_s == 300.0
    a, b = _top_level_definitions(v1.__file__), _top_level_definitions(ql.__file__)
    for name in ("run_caliper", "container_events", "inspect_all", "health", "stop_background", "precheck", "Thresholds", "THR", "OPS_PORT", "CALIPER", "HaltSeries",
                 "parse_inspect", "StepFailed", "do_getinfo", "do_fetch_config"):
        assert a[name] == b[name], name
    assert dataclasses.asdict(ql.THR) == dataclasses.asdict(v1.THR)


@pytest.mark.parametrize("what", ["restart", "oom", "exit", "replaced"])
def test_condition_3_a_restart_oom_exit_or_replacement_mid_run_aborts_and_kills_caliper(world, patched, what):
    s, be = make_session(world, patched)
    real_sleep = be.sleep

    def sleeper(x):
        real_sleep(x)
        if be.t > 130:
            c = be.containers["peer0.org2.example.com"]
            {"restart": lambda: c.update(started="2026-11-12T12:30:00Z", restarts=1), "oom": lambda: c.update(oom=True),
             "exit": lambda: c.update(status="exited"), "replaced": lambda: c.update(id="f" * 64)}[what]()
    be.sleep = sleeper
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "ABORTED_INFRA" and be.killed == [ql.CALIPER]
    abort = json.loads((world.ctx.rd("cp01") / "facts.json").read_text())["aborted"]
    assert abort.startswith("container event") and "peer0.org2.example.com" in abort
    assert {"restart": "restarted", "oom": "OOM killed", "exit": "not running", "replaced": "container replaced"}[what] in abort


def test_condition_4_post_run_backlog_transaction_validity_and_failure_rate_are_mandatory(world, patched):
    _, _, row, v, rd = golden(world, patched)
    assert next(g for g in v["gates"] if g["id"] == "G-postrun")["status"] == "PASS"

    def no_backlog(cp):
        p = cp / "analysis/summary.json"
        d = json.loads(p.read_text())
        del d["metrics"]["max_inflight_backlog"]
        p.write_text(json.dumps(d))
    out = reeval(world, rd, row, no_backlog)
    assert out["verdict"] == "INVALID_DATA" and "G-postrun" in out["failed_gates"] and out["class_v2"] == "UNDETERMINED"       # a missing backlog is never HEALTHY

    def no_ledger_codes(cp):
        (cp / "ledger/ledger/ledger_tx.csv").write_text("tx_id,header_type,validation_code\n", encoding="utf-8")
    out2 = reeval(world, rd, row, no_ledger_codes)
    assert out2["verdict"] == "INVALID_DATA" and "G-postrun" in out2["failed_gates"]

    def no_summary(cp):
        (cp / "analysis/summary.json").unlink()
    out3 = reeval(world, rd, row, no_summary)
    assert out3["verdict"] == "INVALID_DATA" and {"G-postrun", "G-reconcile"} <= set(out3["not_evaluated_gates"])                   # reconciliation too: not evaluated = not valid


def test_condition_5_every_infrastructure_verdict_ends_the_whole_series():
    """The behaviour is tested by test_series_halts_on_infrastructure_verdicts (all three verdicts) and the first-run and two-consecutive-INVALID rules; here the
    vocabulary the rule rests on is pinned to the one the frozen driver used."""
    assert ql.HALT_VERDICTS == v1.HALT_VERDICTS == ("ABORTED_INFRA", "INVALID_INFRA", "DRIVER_ERROR")
    assert ql.INFRA_GATES == v1.INFRA_GATES and set(ql.INFRA_GATES).isdisjoint(ql.DATA_GATES)           # a data-evidence shortfall is never confused with an infrastructure fault


def test_condition_6_tier_two_is_absent_from_every_path_a_run_can_take(world, tmp_path):
    for cap in (60, 200):
        with pytest.raises(SystemExit, match="Tier 2"):
            ql.check_approval(_approval(world, tmp_path, f"x{cap}.txt", cap=cap))
    assert all(r.rate <= 40 for r in ql.load_matrix(world.ctx.matrix))
    with pytest.raises(ql.MatrixError):
        ql.load_matrix(_write(tmp_path / "t2.csv", mx.matrix_bytes(200)))
    assert any("Tier 2" in b for b in ql.TIER2_BLOCKERS)


def _write(path, data):
    path.write_bytes(data)
    return path


# ====================================================================================== B3: the host sampler in the run
def test_the_host_sampler_command_line(world):
    a = ql.argv_host(world.rows[0], world.ctx, world.ctx.rd("cp01") / "stop_host")
    assert a[:4] == ["python3", "-B", world.ctx.tool("e1_host_sampler.py"), "sample"] and a[-1] == "--execute"
    assert a[a.index("--out-dir") + 1] == str(world.ctx.rd("cp01") / "host") and a[a.index("--stop-file") + 1] == str(world.ctx.rd("cp01") / "stop_host")
    assert a[a.index("--interval") + 1] == "1" and a[a.index("--max-seconds") + 1] == "420" == str(ql.EVID.host_max_seconds)
    assert ql.EVID.host_max_seconds > ql.THR.caliper_timeout_s + 60            # the guard outlasts the hard Caliper timeout, so it never cuts a run's window short


def test_a_host_sampler_that_dies_at_once_ends_the_run_as_infra_like_the_other_samplers(world, patched):
    s, be = make_session(world, patched)
    real_spawn = be.spawn

    def spawn(argv, env=None, stdout_path=None, stderr_path=None):
        proc = real_spawn(argv, env, stdout_path, stderr_path)
        if len(argv) > 2 and str(argv[2]).endswith("e1_host_sampler.py"):
            proc.returncode = 1                                                  # typeperf could not start, the sampler gave up
        return proc
    be.spawn = spawn
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "INVALID_INFRA" and v["step_failure"]["label"] == "host" and "exited early" in v["step_failure"]["detail"]
    assert not any(c[:2] == ["docker", "exec"] for c in be.spawns) and be.killed == []             # Caliper never started


# ====================================================================================== plan 7.8 (E): evidence completeness
@pytest.mark.parametrize("opt,stream", [
    ({"resource_gap": ("peer0.org1.example.com", 20, 40)}, "resources:peer0.org1.example.com"),
    ({"resource_gap": (ql.CALIPER, 60, 62)}, None),                              # a 2-sample hole is inside the tolerance: the gate must still PASS
    ({"scrape_gap": ("orderer2", 50, 70)}, "scrapes:orderer2"),
    ({"host_hole": (30, 60)}, "host:wsl"),
    ({"host_no_windows_rows": True}, "host:windows"),
])
def test_evidence_gate_fails_on_a_hole_or_a_missing_stream_and_passes_a_tiny_one(world, patched, opt, stream):
    _, _, row, v, rd = golden(world, patched, **opt)
    g = next(x for x in v["gates"] if x["id"] == "G-evidence")
    if stream is None:
        assert g["status"] == "PASS" and v["verdict"] == "VALID", g["detail"]
        return
    assert g["status"] == "FAIL" and v["verdict"] == "INVALID_DATA" and "G-evidence" in v["failed_gates"] and stream in g["detail"]
    assert v["metrics"]["evidence"][stream]["ratio"] < 0.95 or v["metrics"]["evidence"][stream]["max_gap_s"] > 5.0


def test_evidence_gate_fails_when_the_host_record_lacks_a_required_windows_counter_or_the_windows_side_was_off(world, patched):
    _, _, row, v, rd = golden(world, patched, host_missing_required=["disk_bytes_s"])
    g = next(x for x in v["gates"] if x["id"] == "G-evidence")
    assert g["status"] == "FAIL" and "required counters" in g["detail"] and "disk_bytes_s" in g["detail"] and v["verdict"] == "INVALID_DATA"
    out = reeval(world, rd, row, lambda cp: (cp / "host/host_manifest.json").write_text(json.dumps({"status": "COMPLETE", "windows_missing_required": [],
                                                                                                     "parameters": {"windows": False}})))
    assert next(x for x in out["gates"] if x["id"] == "G-evidence")["detail"].endswith("['windows side disabled']")


def test_evidence_gate_is_not_evaluated_when_a_source_is_missing_which_is_invalid_not_a_pass(world, patched):
    _, _, row, v, rd = golden(world, patched)
    for victim in ("host/host_manifest.json", "host/wsl_samples.csv", "resources/resource_manifest.json", "metrics/during/scrapes.csv"):
        out = reeval(world, rd, row, lambda cp, victim=victim: (cp / victim).unlink())
        assert "G-evidence" in out["not_evaluated_gates"] and out["verdict"] == "INVALID_DATA", victim


def test_evidence_completeness_boundaries_ratio_gap_and_edges():
    """The numbers of plan 7.8 (E): >= 95 % of the expected 1 Hz samples and no hole above 5 s, edges included."""
    lo, hi = 1_000_000.0, 1_120_000.0                                                       # 120 s: expected 120
    def check(times):
        return hs.stream_completeness(times, lo, hi)
    every = [lo + 1000.0 * k + 500.0 for k in range(120)]
    assert check(every)["ratio"] == 1.0
    scattered = [t for i, t in enumerate(every) if i % 20 != 7]                              # 6 scattered misses: 114 / 120 = 0.95 exactly, holes of 2 s
    c = check(scattered)
    assert c["count"] == 114 and c["ratio"] == pytest.approx(0.95) and c["max_gap_s"] == pytest.approx(2.0)
    assert c["ratio"] >= ql.EVID.min_ratio and c["max_gap_s"] <= ql.EVID.max_gap_s                 # the boundary passes (>=), as written in the plan
    seven = [t for i, t in enumerate(every) if i % 17 != 3]                                   # 7 misses: 113 / 120 < 0.95
    assert check(seven)["ratio"] < ql.EVID.min_ratio
    contiguous = [t for i, t in enumerate(every) if not (50 <= i < 54)]                       # a 5-s hole (4 missing): gap 5.0 s: allowed
    assert check(contiguous)["max_gap_s"] == pytest.approx(5.0) and check(contiguous)["ratio"] >= 0.95
    wider = [t for i, t in enumerate(every) if not (50 <= i < 55)]                            # 5 missing: gap 6.0 s: refused although the ratio is 0.958
    assert check(wider)["max_gap_s"] == pytest.approx(6.0) and check(wider)["ratio"] >= 0.95 and check(wider)["max_gap_s"] > ql.EVID.max_gap_s
    late_start = [t for t in every if t > lo + 6000]                                          # the stream begins 6 s into the window: the edge is a hole
    assert check(late_start)["max_gap_s"] > ql.EVID.max_gap_s
    assert ql.EVID.min_ratio == 0.95 and ql.EVID.max_gap_s == 5.0


def test_thresholds_stay_inside_the_margin_the_p1a_back_test_showed():
    """Measured on the 18 archived P1a runs before adoption (script and output in D:\\PLAN_BLOCK18\\p1b_tier1_prep_2026-11-13\\evidence_backtest): resource sampler
    >= 98.4 % per container, Prometheus scrapes >= 100 % per target, longest hole 1.05 s. The numbers are the plan's own (section 7.8 (E)), not tuned to the data;
    this guard only stops them from being tightened past what real runs achieved without a new look."""
    assert ql.EVID.min_ratio <= 0.984 and ql.EVID.max_gap_s >= 1.05


# ====================================================================================== B4: the per-run report
@pytest.mark.parametrize("mode", ["ignored", "none"])
def test_report_gate_fails_if_caliper_ignores_the_flag_or_writes_nothing(world, patched, mode):
    _, _, row, v, rd = golden(world, patched, report_mode=mode)
    g = next(x for x in v["gates"] if x["id"] == "G-report")
    checks = g["evidence"]["checks"]
    assert g["status"] == "FAIL" and v["verdict"] == "INVALID_DATA" and "G-report" in v["failed_gates"]
    assert checks["per_run_report_exists_and_is_html"] is False and checks["caliper_log_names_the_per_run_path"] is False
    assert checks["default_report_unchanged"] is (mode == "none")                              # 'ignored' overwrote the workspace report: the exact P1a defect


def test_report_gate_checks_each_of_its_three_conditions_separately(world, patched):
    _, _, row, v, rd = golden(world, patched)
    assert next(x for x in v["gates"] if x["id"] == "G-report")["status"] == "PASS"
    out = reeval(world, rd, row, lambda cp: [p.write_text("no report line here\n", encoding="utf-8") for p in (cp / "cmd").glob("*_caliper.stdout")])
    g = next(x for x in out["gates"] if x["id"] == "G-report")
    assert g["status"] == "FAIL" and g["evidence"]["checks"] == {"per_run_report_exists_and_is_html": True, "caliper_log_names_the_per_run_path": False,
                                                                 "default_report_unchanged": True}
    (world.ctx.caliper_ws / "runs" / "cp01" / "report.html").write_text("plain text, not html", encoding="utf-8")
    g2 = next(x for x in reeval(world, rd, row)["gates"] if x["id"] == "G-report")
    assert g2["evidence"]["checks"]["per_run_report_exists_and_is_html"] is False
    (world.ctx.caliper_ws / "runs" / "cp01" / "report.html").write_text("<html>ok</html>", encoding="utf-8")
    facts = json.loads((rd / "facts.json").read_text())
    facts["report"]["default_sha256_after"] = "0" * 64                                           # the default report was touched during the launch
    cp = world.tmp / "copy_facts"
    shutil.copytree(rd, cp)
    out3 = ql.evaluate_run(cp, row, world.ctx.recorder_dir("cp01"), facts)
    assert next(x for x in out3["gates"] if x["id"] == "G-report")["evidence"]["checks"]["default_report_unchanged"] is False


def test_the_series_halts_after_one_run_if_the_report_flag_turns_out_to_be_ignored(world, patched):
    """The residual risk of B4 (the flag was confirmed against the source and real logs, not against the container's own CLI): it costs one run, not ten."""
    s, be = make_session(world, patched, report_mode="ignored")
    s.max_rate, s.anchor_classes = 40, {"ql17": "HEALTHY", "ql23": "HEALTHY"}
    with pytest.raises(ql.HaltSeries, match="first run cp01 is INVALID_DATA"):
        ql.run_series(s, world.rows)
    assert len([c for c in be.spawns if c[:2] == ["docker", "exec"]]) == 1 and s.verdicts["cp01"]["failed_gates"] == ["G-report"]
    assert (world.ctx.caliper_ws / "report.html").read_text(encoding="utf-8") != DEFAULT_REPORT          # the evidence of the defect is kept, not hidden


def test_a_per_run_report_never_overwrites_another_runs_or_the_default_one(world, patched):
    s, be = make_session(world, patched)
    for row in world.rows[:2]:
        assert ql.execute_run(s, row)["verdict"] == "VALID"
    reports = {r.run_id: (world.ctx.caliper_ws / "runs" / r.run_id / "report.html").read_text(encoding="utf-8") for r in world.rows[:2]}
    assert reports["cp01"] != reports["cp02"] and "cp01" in reports["cp01"] and "cp02" in reports["cp02"]
    assert (world.ctx.caliper_ws / "report.html").read_text(encoding="utf-8") == DEFAULT_REPORT


def test_backend_run_captures_output_exit_code_and_timeout():
    be = ql.Backend()
    r = be.run([sys.executable, "-c", "import sys; print('hi'); sys.stderr.write('e'); sys.exit(3)"])
    assert r.code == 3 and r.stdout.strip() == b"hi" and r.stderr == b"e" and not r.timed_out
    r = be.run([sys.executable, "-c", "import time; time.sleep(30)"], timeout=0.5)
    assert r.code is None and r.timed_out
    assert be.run(["definitely-not-a-command-xyz"]).code is None


# ====================================================================================== continuous clock record (A4)
def _samples(seconds, *, steps=(), holes=(), period=0.05):
    """(i, real_ns, mono_ns) rows every 50 ms; steps = [(t_s, ns)]; holes = [(t0_s, t1_s)] without rows (time still passes)."""
    out, k = [], 0
    while k * period <= seconds:
        t = k * period
        if not any(a <= t < b for a, b in holes):
            out.append({"i": k, "real_ns": EPOCH12 + int(t * 1e9) + sum(ns for at, ns in steps if t >= at), "mono_ns": 5_000_000_000_000 + int(t * 1e9)})
        k += 1
    return out


def _win(run_id, a_s, b_s):
    return {"run_id": run_id, "start_ns": EPOCH12 + int(a_s * 1e9), "end_ns": EPOCH12 + int(b_s * 1e9)}


def _facts(world, run_id, started, finished):
    d = world.ctx.rd(run_id)
    d.mkdir(parents=True, exist_ok=True)
    (d / "facts.json").write_text(json.dumps({"started_utc": started, "finished_utc": finished}), encoding="utf-8")


def test_utc_ns_conversions():
    assert ql.utc_to_ns("2026-11-12T12:01:40.000Z") == EPOCH12 + 100 * 10**9
    assert ql.utc_to_ns("1970-01-01T00:00:01.5Z") == 1_500_000_000
    assert ql.ns_to_utc(EPOCH12 + 100_250_000_000) == "2026-11-12T12:01:40.250Z"


def test_clock_check_clean_series_passes_and_reports_coverage():
    r = ql.clock_series_check(_samples(60), [_win("cp01", 10, 40)])
    assert r["verdict"] == "PASS" and r["steps_total"] == 0 and r["max_abs_offset_change_ms"] == 0.0 and r["run_windows"] == 1
    w = r["windows"][0]
    assert w["covered"] and w["samples"] > 500 and w["lead_ms"] <= 50 and w["tail_ms"] <= 50 and w["max_gap_ms"] <= 51


def test_clock_check_flags_steps_inside_run_windows_only_above_the_threshold():
    pos = ql.clock_series_check(_samples(60, steps=[(20, 2_000_000)]), [_win("cp01", 10, 40)])
    assert pos["verdict"] == "STEP_IN_RUN_WINDOW" and pos["steps_total"] == 1
    assert pos["steps_in_run_windows"][0]["step_ns"] == 2_000_000 and pos["steps_in_run_windows"][0]["runs"] == ["cp01"]
    neg = ql.clock_series_check(_samples(60, steps=[(20, -1_500_000)]), [_win("cp01", 10, 40)])
    assert neg["verdict"] == "STEP_IN_RUN_WINDOW" and neg["steps_in_run_windows"][0]["step_ns"] == -1_500_000
    between_runs = ql.clock_series_check(_samples(60, steps=[(50, 2_000_000)]), [_win("cp01", 10, 40)])
    assert between_runs["verdict"] == "PASS" and between_runs["steps_total"] == 1 and between_runs["steps_in_run_windows"] == []
    assert len(between_runs["steps_outside_run_windows"]) == 1
    small = ql.clock_series_check(_samples(60, steps=[(20, 900_000)]), [_win("cp01", 10, 40)])
    assert small["verdict"] == "PASS" and small["steps_total"] == 0 and small["max_abs_offset_change_ms"] == pytest.approx(0.9)
    on_the_edge = ql.clock_series_check(_samples(60, steps=[(10, 2_000_000)]), [_win("cp01", 10, 40)])
    assert on_the_edge["verdict"] == "STEP_IN_RUN_WINDOW"


def test_clock_check_flags_a_sampling_hole_and_a_probe_that_stopped_early():
    hole = ql.clock_series_check(_samples(60, holes=[(20, 25)]), [_win("cp01", 10, 40)])
    assert hole["verdict"] == "COVERAGE_GAP" and not hole["windows"][0]["covered"] and hole["windows"][0]["max_gap_ms"] == pytest.approx(5050, abs=5)
    dead = ql.clock_series_check(_samples(30), [_win("cp01", 10, 40)])
    assert dead["verdict"] == "COVERAGE_GAP" and dead["windows"][0]["tail_ms"] == pytest.approx(10000, abs=60)
    never = ql.clock_series_check(_samples(5), [_win("cp01", 100, 110)])
    assert never["verdict"] == "COVERAGE_GAP" and never["windows"][0]["samples"] == 0 and never["windows"][0]["lead_ms"] is None


def test_clock_check_degenerate_inputs():
    assert ql.clock_series_check([], [_win("cp01", 1, 2)])["verdict"] == "NO_DATA"
    assert ql.clock_series_check(_samples(0), [_win("cp01", 1, 2)])["verdict"] == "NO_DATA"
    assert ql.clock_series_check(_samples(5), [])["verdict"] == "NO_RUN_WINDOWS"


def test_read_clock_samples_drops_a_torn_last_line_and_rejects_a_foreign_file(tmp_path):
    p = tmp_path / "clock_samples.csv"
    p.write_text(PROBE_COLS + "\n0,100,50,50,50,0,10000,0,0,0,0\n1,200,150,150,150,0,10000,0,0,0,0\n2,30", encoding="utf-8")
    rows, torn = ql.read_clock_samples(p)
    assert rows == [{"i": 0, "real_ns": 100, "mono_ns": 50}, {"i": 1, "real_ns": 200, "mono_ns": 150}] and torn == 1
    other = tmp_path / "other.csv"
    other.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="header"):
        ql.read_clock_samples(other)
    empty = tmp_path / "empty.csv"
    empty.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="header"):
        ql.read_clock_samples(empty)


def test_run_windows_come_from_facts_json_and_skip_runs_without_one(world):
    _facts(world, "cp02", "2026-11-12T12:10:00.000Z", "2026-11-12T12:13:30.000Z")
    _facts(world, "cp01", "2026-11-12T12:02:00.000Z", "2026-11-12T12:05:30.500Z")
    world.ctx.rd("cp03").mkdir(parents=True)
    w = ql.run_windows(world.ctx, ["cp01", "cp02", "cp03", "cp04"])
    assert [x["run_id"] for x in w] == ["cp01", "cp02"] and w[0]["end_ns"] - w[0]["start_ns"] == 210_500_000_000


def test_finish_clock_series_waits_the_flush_margin_stops_the_probe_and_writes_the_check(world, patched):
    s, be = make_session(world, patched)
    od = world.ctx.root / "clock_series"
    od.mkdir()
    probe = FakeProbeProc(be, od)
    t0 = be.t
    be.t += 200.0                                                    # the series ran for 200 simulated seconds
    _facts(world, "cp01", ql.ns_to_utc(EPOCH12 + int((t0 + 5) * 1e9)), ql.ns_to_utc(EPOCH12 + int((t0 + 150) * 1e9)))
    out = ql.finish_clock_series(s, probe, ["cp01", "cp02"])
    assert out["verdict"] == "PASS" and probe.killed and out["probe_returncode"] == -15 and out["torn_rows_dropped"] == 1
    assert be.t == pytest.approx(t0 + 200.0 + ql.THR.clock_flush_margin_s)       # it slept the margin before stopping the probe
    assert out["samples"] > 4000 and out["run_windows"] == 1 and "absent by design" in out["note"]
    assert not (od / "probe_manifest.json").exists()
    assert json.loads((world.ctx.root / "clock_series_check.json").read_text())["verdict"] == "PASS"


def test_finish_clock_series_with_a_probe_that_died_early_reports_a_gap_without_waiting(world, patched):
    s, be = make_session(world, patched)
    od = world.ctx.root / "clock_series"
    od.mkdir()
    probe = FakeProbeProc(be, od, exit_code=1, stop_sampling_after=20.0)
    t0 = be.t
    be.t += 200.0
    _facts(world, "cp01", ql.ns_to_utc(EPOCH12 + int((t0 + 5) * 1e9)), ql.ns_to_utc(EPOCH12 + int((t0 + 150) * 1e9)))
    out = ql.finish_clock_series(s, probe, ["cp01"])
    assert out["probe_exit_before_stop"] == 1 and out["verdict"] == "COVERAGE_GAP" and be.t == pytest.approx(t0 + 200.0)


def test_finish_clock_series_never_raises_and_records_a_missing_sample_file(world, patched):
    s, be = make_session(world, patched)
    probe = FakeProc(be)                                             # a probe that wrote nothing
    out = ql.finish_clock_series(s, probe, ["cp01"])
    assert out["verdict"] == "CHECK_ERROR" and "FileNotFoundError" in out["error"] and probe.killed
    assert json.loads((world.ctx.root / "clock_series_check.json").read_text())["verdict"] == "CHECK_ERROR"


# ---- the run command end to end in the fake world (the first test of cmd_run beyond its refusals)
@pytest.fixture
def series_args(world, patched, monkeypatch, tmp_path):
    monkeypatch.setattr(ql, "is_linux", lambda: True)
    approval = _approval(world, tmp_path, "approval.txt", cap=40)
    (tmp_path / "pf_keep_awake.pid").write_text("4242\n", encoding="utf-8")
    return SimpleNamespace(execute=True, approval_file=approval, out_dir=tmp_path / "pf", clock_dir=_probe_dir(tmp_path), only="cp01", s1_evidence=None)


def _summary(world):
    return json.loads((world.ctx.root / "series_summary.json").read_text())


def test_cmd_run_end_to_end_in_the_fake_world(world, series_args, capsys):
    be = FakeBackend(world, s1_text=S1_OK)
    rc = ql.cmd_run(series_args, world.ctx, be)
    out = capsys.readouterr().out
    assert rc == 0, out
    summary, root = _summary(world), world.ctx.root
    assert [(r["run_id"], r["verdict"]) for r in summary["runs"]] == [("cp01", "VALID")] and summary["halted"] is None
    assert summary["fingerprint_differences"] == [] and summary["clock_series_check"]["verdict"] == "PASS"
    check = json.loads((root / "clock_series_check.json").read_text())
    assert check["run_windows"] == 1 and check["torn_rows_dropped"] == 1 and check["steps_total"] == 0 and check["windows"][0]["covered"]
    for f in ("session_manifest.json", "fingerprint_compare.json", "series_events.log", "cp01/run_verdict.json", "clock_series/clock_samples.csv"):
        assert (root / f).is_file(), f
    assert not world.ctx.rd("cp02").exists()                                   # --only cp01
    assert be.probe.killed and be.keep_awake.killed and ["taskkill.exe", "/PID", "4242", "/F"] in be.calls
    assert "SERIES COMPLETE" in out and "clock series check: PASS" in out
    assert (series_args.out_dir / "preflight_report.json").is_file() and be.keep_awake_seconds == 10 * 540 + 1200
    sm = json.loads((root / "session_manifest.json").read_text(encoding="utf-8"))                  # the owner's acceptance is part of the record of what ran
    assert sm["tier"] == 1 and sm["watchdog_mode"] == "bounds_only_tier1" and len(sm["b2_bounds_conditions"]) == 6 and len(sm["tier2_blockers"]) == 2
    assert sm["evidence_thresholds"] == {"min_ratio": 0.95, "max_gap_s": 5.0, "host_max_seconds": 420} and sm["erratum_adoption"]["signed"] is True
    assert "watchdog_mode=bounds_only_tier1" in sm["approval_text"] and "tier=1" in sm["approval_text"] and "scripts/e1/e1_host_sampler.py" in sm["tool_hashes"]
    assert (root / "cp01" / "host" / "host_manifest.json").is_file() and (world.ctx.caliper_ws / "runs" / "cp01" / "report.html").is_file()


def test_cmd_run_starts_no_run_when_the_continuous_probe_exits_at_once(world, series_args):
    be = FakeBackend(world, s1_text=S1_OK, probe_exit=1)
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 4 and "continuous clock probe exited at once" in summary["halted"] and summary["runs"] == []
    assert not world.ctx.rd("cp01").exists() and not any(c[:2] == ["docker", "exec"] for c in be.spawns)
    assert be.keep_awake.killed and ["taskkill.exe", "/PID", "4242", "/F"] in be.calls


def test_cmd_run_halts_at_the_next_run_when_the_keep_awake_helper_died(world, series_args):
    be = FakeBackend(world, s1_text=S1_OK)
    alive = be.host_power
    be.host_power = lambda pid=None: {**alive(pid), "helper_alive": False} if (world.ctx.rd("cp01") / "run_verdict.json").exists() else alive(pid)
    series_args.only = "cp01,cp02"
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 4 and "keep-awake helper alive" in summary["halted"] and [r["run_id"] for r in summary["runs"]] == ["cp01"]
    assert not world.ctx.rd("cp02").exists() and be.probe.killed


def test_cmd_run_halts_at_the_next_run_when_the_clock_probe_died_and_reports_the_gap(world, series_args):
    be = FakeBackend(world, s1_text=S1_OK, probe_dies_after=100.0)
    series_args.only = "cp01,cp02"
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 4 and "continuous clock probe running" in summary["halted"] and [r["run_id"] for r in summary["runs"]] == ["cp01"]
    check = json.loads((world.ctx.root / "clock_series_check.json").read_text())
    assert check["probe_exit_before_stop"] == 1 and check["verdict"] == "COVERAGE_GAP" and not check["windows"][0]["covered"]
    assert not world.ctx.rd("cp02").exists()


def test_cmd_run_reports_a_clock_step_inside_a_run_window_but_leaves_the_run_verdict_alone(world, series_args):
    be = FakeBackend(world, s1_text=S1_OK, probe_steps=[(100.0, 2_000_000)])
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 5 and summary["clock_series_check"]["verdict"] == "STEP_IN_RUN_WINDOW"
    assert summary["clock_series_check"]["steps_in_run_windows"][0]["runs"] == ["cp01"]
    assert [(r["run_id"], r["verdict"]) for r in summary["runs"]] == [("cp01", "VALID")]       # A4: the evidence is kept, the existing gates decide


def test_cmd_run_stops_the_probe_and_the_helper_even_if_the_end_of_series_fingerprint_fails(world, series_args, monkeypatch):
    real, calls = ql.fingerprint, {"n": 0}

    def flaky(ctx, be_, net, out_dir):
        calls["n"] += 1
        if calls["n"] >= 2:                                          # the first call belongs to gate S7
            raise RuntimeError("peer unreachable")
        return real(ctx, be_, net, out_dir)
    monkeypatch.setattr(ql, "fingerprint", flaky)
    be = FakeBackend(world, s1_text=S1_OK)
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 4 and summary["fingerprint_differences"][0].startswith("end-of-series fingerprint failed") and "peer unreachable" in summary["fingerprint_differences"][0]
    assert be.probe.killed and be.keep_awake.killed and summary["clock_series_check"]["verdict"] == "PASS"


# ====================================================================================== derivation from the frozen driver
FROZEN_P1A_DRIVER_SHA256 = "38b8ec29b5fd666bcc313d8c917945555643585113819481546fd8ecefb39ad6"     # recorded in the P1a series' session_manifest.json
# Reviewed on 2026-11-13. v0.1 (the 26-row plan) changed 29 definitions and added 7; v0.2 (Tier 1, B3, B4, the owner's bounds-only conditions) changes six more
# (DATA_GATES, caliper_argv, execute_run, expected_outputs, probe_tools, run_listing) and adds thirteen. run_caliper, container_events, inspect_all, health,
# stop_background and precheck are NOT in this list: they are the frozen text, which is what 'the existing aborts remain active' rests on.
CHANGED_DEFINITIONS = sorted([
    "ALLOWED_B", "ALLOWED_RATES", "CHANNEL_RE", "Ctx", "DATA_GATES", "GATE_TABLE", "INVOKED_TOOLS", "MATRIX_SHA256", "PRECONDITION_RE", "ROW_COUNT", "Row",
    "S1_ALLOWED_DRIFT", "Session", "TOOL_VERSION", "build_dry_run", "caliper_argv", "check_approval", "cmd_run", "evaluate_run", "execute_run", "expected_outputs",
    "expected_regime", "gate_s1", "gate_s10", "gate_s11", "gate_s8", "main", "probe_tools", "ramp_blocked", "run_listing", "run_preflight", "run_series",
    "static_findings", "tool_hashes", "validate_rows"])
ADDED_DEFINITIONS = sorted(["ANCHOR_RE", "ANCHOR_RUNS", "B2_BOUNDS_CONDITIONS", "BLOCKERS_OPEN", "ERRATUM_CSV_SHA256", "ERRATUM_ID", "EVID", "EvidenceThresholds",
                            "TIER1_MAX_RATE_TPS", "TIER2_BLOCKERS", "WATCHDOG_MODE", "approved_max_rate", "argv_host", "check_erratum_adopted", "erratum_adoption",
                            "evidence_completeness", "load_anchor_classes", "report_rel",
                            "import:import e1_client_classification_v2 as ccv2", "import:import e1_host_sampler as hs"])


def _top_level_definitions(path):
    import ast
    text = Path(path).read_text(encoding="utf-8")
    out = {}
    for node in ast.parse(text).body:
        seg = ast.get_source_segment(text, node)
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            out[node.name] = seg
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    out[tgt.id] = seg
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            out[node.target.id] = seg
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            out["import:" + (seg or "").strip()] = seg
    return out


def test_the_frozen_p1a_driver_is_untouched():
    assert hashlib.sha256(Path(v1.__file__).read_bytes()).hexdigest() == FROZEN_P1A_DRIVER_SHA256


def test_the_derivation_changed_exactly_the_reviewed_definitions():
    """Every top-level definition that is not listed here is byte-identical to the frozen driver (command builders, Backend, the S2-S7 and S9 gates,
    execute_run, the watchdog, the clock-series check, the fingerprint code). A change to any other definition fails this test until it is reviewed."""
    a, b = _top_level_definitions(v1.__file__), _top_level_definitions(ql.__file__)
    assert sorted(k for k in a if k in b and a[k] != b[k]) == CHANGED_DEFINITIONS
    assert sorted(k for k in b if k not in a) == ADDED_DEFINITIONS
    assert [k for k in a if k not in b] == []
    assert len([k for k in a if k in b and a[k] == b[k]]) == 118          # 153 frozen definitions: 35 changed, 118 byte-identical


def test_the_frozen_rule_is_still_computed_unchanged_for_comparison():
    assert ql.classify.__code__.co_code == v1.classify.__code__.co_code
    from dataclasses import asdict
    assert asdict(ql.THR) == asdict(v1.THR) and ql.THR.client_event_loop_p99_ms == 10.0
    assert ql.v2_pacing.__code__.co_code == v1.v2_pacing.__code__.co_code and ql.interior_valid_rate.__code__.co_code == v1.interior_valid_rate.__code__.co_code