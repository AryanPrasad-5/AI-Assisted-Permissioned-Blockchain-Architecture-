"""Fabric-free tests for scripts/e1/e1_quicklook.py (the P1a quick-look driver). Synthetic only: nothing here is an E1 result.

A fake world stands in at the process, HTTP and Windows-host boundary (Backend). The engine, the gates, the verdict logic and
the dry-run text are the real code. No Docker, Fabric, Caliper or network access.
"""
import csv
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
import e1_la2_render as lr  # noqa: E402
import e1_quicklook as ql  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

PKG = "e1asset_1.0:" + "ab" * 32
SWEEP = [(20, 10, 1), (10, 10, 1), (20, 10, 2), (40, 20, 2), (80, 10, 2), (10, 20, 1), (20, 20, 2), (40, 20, 1),
         (10, 10, 2), (40, 10, 2), (80, 20, 1), (80, 20, 2), (10, 20, 2), (40, 10, 1), (80, 10, 1), (20, 20, 1)]
RATES = [10, 15, 20, 25, 30, 40]


def matrix_bytes() -> bytes:
    rows = [(i, f"ql{i:02d}", f"e1-ql{i:02d}-b{B}-l{r}", "sweep", B, "2s", 10485760, r, rep, 30, 120, "")
            for i, (B, r, rep) in enumerate(SWEEP, 1)]
    seq = 17
    for B in (40, 10):
        for k, r in enumerate(RATES):
            pre = "" if k == 0 else f"run only if the ramp run at B={B}, rate={RATES[k - 1]} was not SATURATED or CLIENT_LIMITED"
            rows.append((seq, f"ql{seq:02d}", f"e1-ql{seq:02d}-b{B}-l{r}", "ramp", B, "2s", 10485760, r, 1, 30, 120, pre))
            seq += 1
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(ql.MATRIX_HEADER.split(","))
    w.writerows(rows)
    return buf.getvalue().encode()


@pytest.fixture
def world(tmp_path):
    e1 = tmp_path / "e1"
    ctx = ql.Ctx(repo=REPO, e1_home=e1, root=e1 / "p1a", net=tmp_path / "network.json", env=tmp_path / "env.json",
                 caliper_ws=e1 / "caliper-ws", py="python3", win_repo="C:\\repo", win_snapshot="D:\\snap",
                 win_python="C:\\python.exe", ref_python="/x/python.exe", matrix=tmp_path / "run_matrix.csv", fabric_bin="/fab/bin")
    env_doc = {"test_network": "/tn", "fabric_bin": "/fab/bin", "fabric_cfg": "/fab/cfg", "repo": str(REPO),
               "caliper_workspace": str(ctx.caliper_ws), "chaincode": {"name": "e1asset", "version": "1.0", "sequence": 1},
               "protocol": {"event_strategy": "msp_any", "invoke_timeout_s": 60, "binding": "fabric:2.5"}}
    net_doc = lr.network(env_doc, PKG)
    ctx.net.write_text(json.dumps(net_doc), encoding="utf-8")
    ctx.env.write_text(json.dumps(env_doc), encoding="utf-8")
    ctx.matrix.write_bytes(matrix_bytes())
    e1.mkdir()
    rows = ql.load_matrix(ctx.matrix)
    return SimpleNamespace(ctx=ctx, env_doc=env_doc, net_doc=net_doc, rows=rows, tmp=tmp_path)


# ====================================================================================== matrix
def test_embedded_matrix_is_byte_identical_to_the_frozen_one():
    assert hashlib.sha256(matrix_bytes()).hexdigest() == ql.MATRIX_SHA256


def test_load_matrix_ok(world):
    rows = world.rows
    assert [r.run_id for r in rows] == [f"ql{i:02d}" for i in range(1, 29)]
    assert rows[0].channel == "e1-ql01-b20-l10" and rows[0].B == 20 and rows[0].rate == 10
    assert rows[27].channel == "e1-ql28-b10-l40" and rows[27].kind == "ramp"
    assert len({r.channel for r in rows}) == 28 and not ({r.channel for r in rows} & ql.EXISTING_CHANNELS)
    assert rows[17].prev_cell() == (40, 10) and rows[16].prev_cell() is None


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
    ("e1-ql05-b80-l10", "e1-r002", "encode|existing"),
    ("ql05,e1-ql05-b80-l10,sweep,80", "ql05,e1-ql05-b80-l10,sweep,90", "grid"),
    ("e1-ql06-b10-l20", "e1-ql06-b10-l21", "encode"),
    ("ql03,e1-ql03-b20-l10", "ql04,e1-ql03-b20-l10", "order"),
    ("seq,run_id", "sequence,run_id", "header"),
])
def test_validate_rejects_bad_rows(tmp_path, old, new, msg):
    p = _mutated(tmp_path, old, new)
    sha = hashlib.sha256(p.read_bytes()).hexdigest()
    with pytest.raises(ql.MatrixError, match=msg):
        ql.load_matrix(p, expected_sha=sha)


def test_ramp_precondition_must_name_the_previous_rung(tmp_path):
    p = _mutated(tmp_path, "at B=40, rate=10 was", "at B=40, rate=15 was")
    with pytest.raises(ql.MatrixError, match="precondition"):
        ql.load_matrix(p, expected_sha=hashlib.sha256(p.read_bytes()).hexdigest())


def test_design_findings_flag_the_consecutive_repeat(world):
    f = ql.design_findings(world.rows)
    assert len(f) == 1 and "ql11" in f[0] and "ql12" in f[0] and "B=80" in f[0] and "frozen" in f[0]


# ====================================================================================== command builders
def test_channel_and_render_argv(world):
    c, r = world.ctx, world.rows[0]
    a = ql.argv_channel(r, c)
    assert a[:3] == ["python3", "-B", c.tool("e1_channel_run.py")] and a[-1] == "--execute"
    assert a[a.index("--channel") + 1] == "e1-ql01-b20-l10" and a[a.index("--max-message-count") + 1] == "20"
    assert a[a.index("--batch-timeout") + 1] == "2s" and a[a.index("--preferred-max-bytes") + 1] == "10485760"
    assert a[a.index("--run-dir") + 1] == str(c.root / "ql01" / "channel")
    b = ql.argv_render(r, c)
    assert b[3] == "run" and b[b.index("--duration-s") + 1] == "120" and b[b.index("--warmup-s") + 1] == "30"
    assert b[b.index("--rate") + 1] == "10" and b[b.index("--workload") + 1] == "create" and "--n" not in b


def test_scrape_resources_summarize_fetch_analysis_argv(world):
    c, r = world.ctx, world.rows[3]                    # ql04: B=40, rate 20
    s = ql.argv_scrape(r, c, "during", c.root / "ql04" / "stop_metrics")
    assert s[s.index("--phase") + 1] == "during" and "--stop-file" in s and s.count("--target") == 5 and s[-len(ql.target_args()) - 1] == "--execute"
    rs = ql.argv_resources(r, c, ql.watch_containers(["dev-peer-x"]), c.root / "ql04" / "stop_resources")
    assert rs.count("--container") == 7 and rs[rs.index("--source") + 1] == "api"
    z = ql.argv_summarize(r, c)
    assert "--fresh-channel" in z and "--expect-traffic" in z and z[z.index("--channel") + 1] == r.channel
    f = ql.argv_fetch(r, c)
    assert f[f.index("--first") + 1] == "0" and f[f.index("--max-message-count") + 1] == "40"
    an = ql.argv_analysis(r, c, 2400)
    assert an[an.index("--expected-n") + 1] == "2400" and an[an.index("--rate-tps") + 1] == "20" and "--other-round-csv" in an
    assert an[an.index("--recorder-base") + 1].endswith("ql04_w0_r1_m-ql04")


def test_caliper_plan_validates_with_the_orchestrator_plan(world):
    import e1_run_orchestrator as ro
    plan = ql.caliper_plan(world.rows[0], world.ctx, world.env_doc)
    p = ro.Plan(plan)
    assert plan["caliper"]["argv"][:5] == ["docker", "exec", "-w", lr.CAL_WS, "caliper"]       # the renderer names the stopped E0-0 container
    fixed = ql.caliper_argv(plan)
    assert fixed[:5] == ["docker", "exec", "-w", lr.CAL_WS, ql.CALIPER] and fixed[5:] == plan["caliper"]["argv"][5:]
    broken = json.loads(json.dumps(plan))
    broken["caliper"]["argv"][4] = "someone-else"
    with pytest.raises(ValueError, match="unexpected Caliper argv"):
        ql.caliper_argv(broken)
    assert p.request.channel == "e1-ql01-b20-l10" and p.raw["cell"]["mode"] == "txDuration" and p.raw["cell"]["duration_s"] == 120
    assert Path(plan["caliper"]["recorder_dir"]) == world.ctx.recorder_dir("ql01")


def test_expected_regime_is_arithmetic_only(world):
    by = {r.run_id: ql.expected_regime(r) for r in world.rows}
    assert by["ql02"].startswith("count-cut")            # B=10 < 10*2
    assert by["ql04"].startswith("boundary")             # B=40 == 20*2
    assert by["ql01"].startswith("boundary")             # B=20 == 10*2
    assert by["ql05"].startswith("timer-cut")            # B=80 > 10*2
    assert ql.expected_submits(world.rows[0]) == {"measured": 1200, "warmup": 300}


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
            return FakeProc(self, stop_file=_arg(a, "--stop-file"))
        if tool == "e1_resource_sampler.py":
            od = Path(_arg(a, "--out-dir"))
            od.mkdir(parents=True)
            run_id = od.parent.name
            return FakeProc(self, stop_file=_arg(a, "--stop-file"), on_done=lambda: self._resources(od, run_id))
        if a[:2] == ["docker", "exec"]:
            assert a[4] == ql.CALIPER, "the engine must exec into caliper-benchmark-001, not the stopped E0-0 'caliper' container"
            run_id = a[a.index("--caliper-benchconfig") + 1].split("/")[1]
            return FakeProc(self, duration=self.opt.get("caliper_s", 190.0), code=self.opt.get("caliper_code", 0),
                            on_done=lambda: self._recorder(run_id))
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

    def _resources(self, od, run_id):
        t0 = 1_700_000_000_000 + 60_000
        with (od / "resource_samples.csv").open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["container", "read_ns", "cpu_percent"])
            for k in range(20):
                w.writerow([ql.CALIPER, int((t0 + 1000 * k + 500) * 1e6), self.opt.get("cpu", 25.0)])
        (od / "resource_manifest.json").write_text(json.dumps({"status": "COMPLETE", "anomalies": {"error": {}, "warning": {}},
                                                                "snapshots": {"start": {}, "end": {}}}), encoding="utf-8")


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
    rd = world.ctx.rd("ql01")
    seq = labels_of(rd)
    order = ["channel", "render", "metrics_before", "metrics_during", "resources", "getinfo_before", "config_before_fetch",
             "config_before_decode", "caliper", "caliper_end", "getinfo_after", "config_after_fetch", "config_after_decode", "stop_metrics",
             "stop_resources", "metrics_after", "metrics_summary", "block_fetch", "analysis"]
    assert [x for x in seq if x in order] == order
    for f in ("run_verdict.json", "facts.json", "config_check.json", "plan.json", "analysis/summary.json", "ledger/block_fetch_manifest.json"):
        assert (rd / f).is_file(), f
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
    world.ctx.rd("ql01").mkdir(parents=True)
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
    facts = json.loads((world.ctx.rd("ql01") / "facts.json").read_text())
    assert "health failed 2 consecutive polls" in facts["aborted"] and facts["caliper"]["exit_code"] == -9
    assert "stop_metrics" in labels_of(world.ctx.rd("ql01"))              # the background samplers were still stopped cleanly


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
    assert "peer0.org2.example.com: restarted" in json.loads((world.ctx.rd("ql01") / "facts.json").read_text())["aborted"]


def test_caliper_hard_timeout_aborts(world, patched):
    s, be = make_session(world, patched, caliper_s=10_000.0)
    v = ql.execute_run(s, world.rows[0])
    assert v["verdict"] == "ABORTED_INFRA" and be.killed == [ql.CALIPER]
    assert json.loads((world.ctx.rd("ql01") / "facts.json").read_text())["caliper"]["timed_out"] is True


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
    return s, be, row, v, world.ctx.rd("ql01")


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
    mb = world.ctx.recorder_dir("ql01") / "ql01_w0_r1_m-ql01.manifest.json"          # now a wall-clock step inside the measured round
    d = json.loads(mb.read_text())
    d["clock"]["wall_minus_monotonic_ms"] = 2.5
    mb.write_text(json.dumps(d))
    out = ql.evaluate_run(rd, row, world.ctx.recorder_dir("ql01"), json.loads((rd / "facts.json").read_text()))
    assert out["verdict"] == "INVALID_INFRA" and "V7-clock" in out["failed_gates"]


def test_verdict_pacing_failure_is_invalid_schedule_and_client_limited(world, patched):
    _, _, row, v, rd = golden(world, patched, el_p99=25.0)
    assert v["verdict"] == "INVALID_SCHEDULE" and "CLIENT_LIMITED" in v["labels"] and v["failed_gates"] == ["V2-pacing"]


def test_verdict_client_limited_label_without_invalidity(world, patched):
    _, _, row, v, rd = golden(world, patched, el_p99=12.0)          # > 10 ms label threshold, <= 20 ms V2 bound
    assert v["verdict"] == "VALID" and v["labels"] == ["CLIENT_LIMITED"]


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
def stub_series(monkeypatch, world, verdicts, prechecks=None):
    s = ql.Session(world.ctx, SimpleNamespace(now_utc=lambda: "t"))
    s.log = lambda m: None
    ran = []
    def fake_exec(sess, row):
        ran.append(row.run_id)
        v = verdicts(row) if callable(verdicts) else verdicts.get(row.run_id, {"verdict": "VALID", "labels": []})
        v = {"run_id": row.run_id, "channel": row.channel, "failed_gates": [], **v}
        return v
    monkeypatch.setattr(ql, "execute_run", fake_exec)
    monkeypatch.setattr(ql, "precheck", prechecks or (lambda sess, row: [("ok", True, "")]))
    return s, ran


def test_series_runs_everything_when_all_valid(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {})
    res = ql.run_series(s, world.rows)
    assert ran == [r.run_id for r in world.rows] and len(res) == 28


def test_ramp_stops_climbing_after_saturation_and_skips_the_rest_of_that_chain(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {"ql19": {"verdict": "VALID", "labels": ["SATURATED"]}})
    res = ql.run_series(s, world.rows)
    skipped = [v["run_id"] for v in res if v["verdict"] == "SKIPPED_PRECONDITION"]
    assert skipped == ["ql20", "ql21", "ql22"] and "ql23" in ran and "ql24" in ran and "ql22" not in ran
    assert (world.ctx.root / "skipped" / "ql20.json").is_file()


def test_ramp_stops_on_client_limited_and_on_invalid_schedule(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {"ql17": {"verdict": "INVALID_SCHEDULE", "labels": ["CLIENT_LIMITED"]}})
    res = ql.run_series(s, world.rows)
    assert [v["run_id"] for v in res if v["verdict"] == "SKIPPED_PRECONDITION"] == ["ql18", "ql19", "ql20", "ql21", "ql22"]


@pytest.mark.parametrize("bad", ["INVALID_INFRA", "ABORTED_INFRA", "DRIVER_ERROR"])
def test_series_halts_on_infrastructure_verdicts(world, monkeypatch, bad):
    s, ran = stub_series(monkeypatch, world, {"ql03": {"verdict": bad, "labels": []}})
    with pytest.raises(ql.HaltSeries, match="ql03"):
        ql.run_series(s, world.rows)
    assert ran == ["ql01", "ql02", "ql03"]


def test_series_halts_after_two_consecutive_invalid_runs(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {"ql04": {"verdict": "INVALID_DATA", "labels": []}, "ql05": {"verdict": "INVALID_SCHEDULE", "labels": []}})
    with pytest.raises(ql.HaltSeries, match="consecutive"):
        ql.run_series(s, world.rows)
    assert ran[-1] == "ql05"
    s2, ran2 = stub_series(monkeypatch, world, {"ql04": {"verdict": "INVALID_DATA", "labels": []}})
    ql.run_series(s2, world.rows)                           # a single INVALID_DATA followed by VALID runs does not halt
    assert len(ran2) == 28


def test_series_halts_when_the_first_run_is_not_valid(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {"ql01": {"verdict": "INVALID_DATA", "labels": []}})
    with pytest.raises(ql.HaltSeries, match="first run"):
        ql.run_series(s, world.rows)
    assert ran == ["ql01"]


def test_series_halts_on_failed_precheck_before_starting_the_run(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {}, prechecks=lambda sess, row: [("five /healthz == 200", row.seq != 2, "")])
    with pytest.raises(ql.HaltSeries, match="pre-run safety check failed"):
        ql.run_series(s, world.rows)
    assert ran == ["ql01"]


def test_series_driver_exception_becomes_a_halt(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {})
    def boom(sess, row):
        raise RuntimeError("boom")
    monkeypatch.setattr(ql, "execute_run", boom)
    with pytest.raises(ql.HaltSeries, match="driver error"):
        ql.run_series(s, world.rows)
    assert s.verdicts["ql01"]["verdict"] == "DRIVER_ERROR"


def test_only_and_from_seq_filters(world, monkeypatch):
    s, ran = stub_series(monkeypatch, world, {})
    ql.run_series(s, world.rows, only={"ql05", "ql06"})
    assert ran == ["ql05", "ql06"]
    s2, ran2 = stub_series(monkeypatch, world, {})
    res2 = ql.run_series(s2, world.rows, from_seq=27)
    assert ran2 == [] and [v["verdict"] for v in res2] == ["SKIPPED_PRECONDITION"] * 2        # ramp rungs never run without their predecessor


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
    world.ctx.rd("ql01").mkdir(parents=True)
    assert "run output folders absent" in {n for n, ok, _ in ql.precheck(s, world.rows[0]) if not ok}


# ====================================================================================== preflight gates
S1_OK = ("SOURCE_CHANGED repo_scripts/e1/e1_existing_channel_pilot.py\nSOURCE_CHANGED repo_scripts/e1/e1_metrics_replay.py\n"
         "SOURCE_ADDED   repo_tests/e1/test_cli_help_smoke.py\nSOURCE_ADDED repo_scripts/e1/e1_quicklook.py\nSOURCE_ADDED repo_tests/e1/test_e1_quicklook.py\n"
         "verify: checked=13991 mismatched=0 missing=0 unlisted=0 source_drift=5\n")


def test_gate_s1_accepts_only_the_authorised_drift(world, tmp_path):
    be = FakeBackend(world)
    ev = tmp_path / "s1.txt"
    ev.write_text(S1_OK, encoding="utf-8")
    assert ql.gate_s1(world.ctx, be, ev).status == "PASS"
    ev2 = tmp_path / "s1b.txt"
    ev2.write_text(S1_OK.replace("source_drift=5", "source_drift=6") + "SOURCE_CHANGED repo_src/fabric_perf/x.py\n", encoding="utf-8")
    g = ql.gate_s1(world.ctx, be, ev2)
    assert g.status == "FAIL" and "repo_src/fabric_perf/x.py" in g.detail
    ev3 = tmp_path / "s1c.txt"
    ev3.write_text(S1_OK.replace("mismatched=0", "mismatched=2"), encoding="utf-8")
    assert ql.gate_s1(world.ctx, be, ev3).status == "FAIL"
    ev4 = tmp_path / "s1d.txt"
    ev4.write_text("nothing useful", encoding="utf-8")
    assert ql.gate_s1(world.ctx, be, ev4).status == "FAIL"


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
    clash = ql.gate_s8(world.ctx, FakeBackend(world, listed=["e1-r002", "e1-ql05-b80-l10"]), net, world.rows)
    assert clash.status == "FAIL" and "e1-ql05-b80-l10" in clash.detail


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
    world.ctx.matrix.write_bytes(matrix_bytes() + b"\n")
    assert ql.gate_s10(world.ctx, net, world.tmp / "pf2").status == "FAIL"
    assert ql.gate_s11(world.ctx, world.rows).status == "PASS"
    (world.ctx.caliper_ws / "runs" / "ql07").mkdir(parents=True)
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
def test_run_is_refused_without_an_owner_approval_file(world, tmp_path):
    with pytest.raises(SystemExit, match="approval"):
        ql.check_approval(None)
    with pytest.raises(SystemExit, match="approval"):
        ql.check_approval(tmp_path / "missing.txt")
    bad = tmp_path / "a.txt"
    bad.write_text("approved_by=owner\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="matrix_sha256"):
        ql.check_approval(bad)
    good = tmp_path / "b.txt"
    good.write_text(f"approved_by=owner\nmatrix_sha256={ql.MATRIX_SHA256}\n", encoding="utf-8")
    assert "approved_by=owner" in ql.check_approval(good)


def test_run_without_execute_does_nothing(world, tmp_path, capsys):
    good = tmp_path / "ok.txt"
    good.write_text(f"approved_by=owner\nmatrix_sha256={ql.MATRIX_SHA256}\n", encoding="utf-8")
    be = FakeBackend(world)
    args = SimpleNamespace(execute=False, approval_file=good, out_dir=tmp_path / "o", clock_dir=None, only=None, from_seq=1, s1_evidence=None)
    assert ql.cmd_run(args, world.ctx, be) == 2
    assert be.calls == [] and not world.ctx.root.exists() and not (tmp_path / "o").exists()


def test_dry_run_lists_all_28_runs_deterministically_and_writes_nothing(world):
    net = cr.Network(world.net_doc)
    before = sorted(p.name for p in world.tmp.rglob("*"))
    t1 = ql.build_dry_run(world.rows, world.ctx, net, world.env_doc, ql.design_findings(world.rows), None)
    t2 = ql.build_dry_run(world.rows, world.ctx, net, world.env_doc, ql.design_findings(world.rows), None)
    assert sorted(p.name for p in world.tmp.rglob("*")) == before
    strip = lambda t: "\n".join(x for x in t.splitlines() if not x.startswith("generated_utc"))
    assert strip(t1) == strip(t2)
    for r in world.rows:
        assert f"### {r.run_id}  channel {r.channel}" in t1
    for needle in ("DRY RUN", "consecutive repeat of cell B=80, rate=20", "ABORT", "docker kill caliper-benchmark-001",
                   "e1_channel_run.py", "e1_block_fetch.py", "--fresh-channel", "timeout 300 docker exec -w /hyperledger/caliper/workspace caliper-benchmark-001",
                   "Operationalisations", "S9", "Failures gate", "Caliper container",
                   "Continuous clock probe (A4)", "clock_series_check.json", "no manifest"):
        assert needle in t1, needle
    assert "None" not in t1 and t1.count("### ql") == 28
    assert "docker exec -w /hyperledger/caliper/workspace caliper caliper" not in t1          # never the stopped E0-0 container


def test_dry_run_can_list_a_subset(world):
    net = cr.Network(world.net_doc)
    t = ql.build_dry_run(world.rows, world.ctx, net, None, [], None, only={"ql04"})
    assert "### ql04" in t and "### ql05" not in t and "<docker exec argv from the rendered plan.json" in t


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
    r = ql.clock_series_check(_samples(60), [_win("ql01", 10, 40)])
    assert r["verdict"] == "PASS" and r["steps_total"] == 0 and r["max_abs_offset_change_ms"] == 0.0 and r["run_windows"] == 1
    w = r["windows"][0]
    assert w["covered"] and w["samples"] > 500 and w["lead_ms"] <= 50 and w["tail_ms"] <= 50 and w["max_gap_ms"] <= 51


def test_clock_check_flags_steps_inside_run_windows_only_above_the_threshold():
    pos = ql.clock_series_check(_samples(60, steps=[(20, 2_000_000)]), [_win("ql01", 10, 40)])
    assert pos["verdict"] == "STEP_IN_RUN_WINDOW" and pos["steps_total"] == 1
    assert pos["steps_in_run_windows"][0]["step_ns"] == 2_000_000 and pos["steps_in_run_windows"][0]["runs"] == ["ql01"]
    neg = ql.clock_series_check(_samples(60, steps=[(20, -1_500_000)]), [_win("ql01", 10, 40)])
    assert neg["verdict"] == "STEP_IN_RUN_WINDOW" and neg["steps_in_run_windows"][0]["step_ns"] == -1_500_000
    between_runs = ql.clock_series_check(_samples(60, steps=[(50, 2_000_000)]), [_win("ql01", 10, 40)])
    assert between_runs["verdict"] == "PASS" and between_runs["steps_total"] == 1 and between_runs["steps_in_run_windows"] == []
    assert len(between_runs["steps_outside_run_windows"]) == 1
    small = ql.clock_series_check(_samples(60, steps=[(20, 900_000)]), [_win("ql01", 10, 40)])
    assert small["verdict"] == "PASS" and small["steps_total"] == 0 and small["max_abs_offset_change_ms"] == pytest.approx(0.9)
    on_the_edge = ql.clock_series_check(_samples(60, steps=[(10, 2_000_000)]), [_win("ql01", 10, 40)])
    assert on_the_edge["verdict"] == "STEP_IN_RUN_WINDOW"


def test_clock_check_flags_a_sampling_hole_and_a_probe_that_stopped_early():
    hole = ql.clock_series_check(_samples(60, holes=[(20, 25)]), [_win("ql01", 10, 40)])
    assert hole["verdict"] == "COVERAGE_GAP" and not hole["windows"][0]["covered"] and hole["windows"][0]["max_gap_ms"] == pytest.approx(5050, abs=5)
    dead = ql.clock_series_check(_samples(30), [_win("ql01", 10, 40)])
    assert dead["verdict"] == "COVERAGE_GAP" and dead["windows"][0]["tail_ms"] == pytest.approx(10000, abs=60)
    never = ql.clock_series_check(_samples(5), [_win("ql01", 100, 110)])
    assert never["verdict"] == "COVERAGE_GAP" and never["windows"][0]["samples"] == 0 and never["windows"][0]["lead_ms"] is None


def test_clock_check_degenerate_inputs():
    assert ql.clock_series_check([], [_win("ql01", 1, 2)])["verdict"] == "NO_DATA"
    assert ql.clock_series_check(_samples(0), [_win("ql01", 1, 2)])["verdict"] == "NO_DATA"
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
    _facts(world, "ql02", "2026-11-12T12:10:00.000Z", "2026-11-12T12:13:30.000Z")
    _facts(world, "ql01", "2026-11-12T12:02:00.000Z", "2026-11-12T12:05:30.500Z")
    world.ctx.rd("ql03").mkdir(parents=True)
    w = ql.run_windows(world.ctx, ["ql01", "ql02", "ql03", "ql04"])
    assert [x["run_id"] for x in w] == ["ql01", "ql02"] and w[0]["end_ns"] - w[0]["start_ns"] == 210_500_000_000


def test_finish_clock_series_waits_the_flush_margin_stops_the_probe_and_writes_the_check(world, patched):
    s, be = make_session(world, patched)
    od = world.ctx.root / "clock_series"
    od.mkdir()
    probe = FakeProbeProc(be, od)
    t0 = be.t
    be.t += 200.0                                                    # the series ran for 200 simulated seconds
    _facts(world, "ql01", ql.ns_to_utc(EPOCH12 + int((t0 + 5) * 1e9)), ql.ns_to_utc(EPOCH12 + int((t0 + 150) * 1e9)))
    out = ql.finish_clock_series(s, probe, ["ql01", "ql02"])
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
    _facts(world, "ql01", ql.ns_to_utc(EPOCH12 + int((t0 + 5) * 1e9)), ql.ns_to_utc(EPOCH12 + int((t0 + 150) * 1e9)))
    out = ql.finish_clock_series(s, probe, ["ql01"])
    assert out["probe_exit_before_stop"] == 1 and out["verdict"] == "COVERAGE_GAP" and be.t == pytest.approx(t0 + 200.0)


def test_finish_clock_series_never_raises_and_records_a_missing_sample_file(world, patched):
    s, be = make_session(world, patched)
    probe = FakeProc(be)                                             # a probe that wrote nothing
    out = ql.finish_clock_series(s, probe, ["ql01"])
    assert out["verdict"] == "CHECK_ERROR" and "FileNotFoundError" in out["error"] and probe.killed
    assert json.loads((world.ctx.root / "clock_series_check.json").read_text())["verdict"] == "CHECK_ERROR"


# ---- the run command end to end in the fake world (the first test of cmd_run beyond its refusals)
@pytest.fixture
def series_args(world, patched, monkeypatch, tmp_path):
    monkeypatch.setattr(ql, "is_linux", lambda: True)
    approval = tmp_path / "approval.txt"
    approval.write_text(f"approved_by=owner\nmatrix_sha256={ql.MATRIX_SHA256}\n", encoding="utf-8")
    (tmp_path / "pf_keep_awake.pid").write_text("4242\n", encoding="utf-8")
    return SimpleNamespace(execute=True, approval_file=approval, out_dir=tmp_path / "pf", clock_dir=_probe_dir(tmp_path), only="ql01", s1_evidence=None)


def _summary(world):
    return json.loads((world.ctx.root / "series_summary.json").read_text())


def test_cmd_run_end_to_end_in_the_fake_world(world, series_args, capsys):
    be = FakeBackend(world, s1_text=S1_OK)
    rc = ql.cmd_run(series_args, world.ctx, be)
    out = capsys.readouterr().out
    assert rc == 0, out
    summary, root = _summary(world), world.ctx.root
    assert [(r["run_id"], r["verdict"]) for r in summary["runs"]] == [("ql01", "VALID")] and summary["halted"] is None
    assert summary["fingerprint_differences"] == [] and summary["clock_series_check"]["verdict"] == "PASS"
    check = json.loads((root / "clock_series_check.json").read_text())
    assert check["run_windows"] == 1 and check["torn_rows_dropped"] == 1 and check["steps_total"] == 0 and check["windows"][0]["covered"]
    for f in ("session_manifest.json", "fingerprint_compare.json", "series_events.log", "ql01/run_verdict.json", "clock_series/clock_samples.csv"):
        assert (root / f).is_file(), f
    assert not world.ctx.rd("ql02").exists()                                   # --only ql01
    assert be.probe.killed and be.keep_awake.killed and ["taskkill.exe", "/PID", "4242", "/F"] in be.calls
    assert "SERIES COMPLETE" in out and "clock series check: PASS" in out
    assert (series_args.out_dir / "preflight_report.json").is_file() and be.keep_awake_seconds == 28 * 540 + 1200


def test_cmd_run_starts_no_run_when_the_continuous_probe_exits_at_once(world, series_args):
    be = FakeBackend(world, s1_text=S1_OK, probe_exit=1)
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 4 and "continuous clock probe exited at once" in summary["halted"] and summary["runs"] == []
    assert not world.ctx.rd("ql01").exists() and not any(c[:2] == ["docker", "exec"] for c in be.spawns)
    assert be.keep_awake.killed and ["taskkill.exe", "/PID", "4242", "/F"] in be.calls


def test_cmd_run_halts_at_the_next_run_when_the_keep_awake_helper_died(world, series_args):
    be = FakeBackend(world, s1_text=S1_OK)
    alive = be.host_power
    be.host_power = lambda pid=None: {**alive(pid), "helper_alive": False} if (world.ctx.rd("ql01") / "run_verdict.json").exists() else alive(pid)
    series_args.only = "ql01,ql02"
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 4 and "keep-awake helper alive" in summary["halted"] and [r["run_id"] for r in summary["runs"]] == ["ql01"]
    assert not world.ctx.rd("ql02").exists() and be.probe.killed


def test_cmd_run_halts_at_the_next_run_when_the_clock_probe_died_and_reports_the_gap(world, series_args):
    be = FakeBackend(world, s1_text=S1_OK, probe_dies_after=100.0)
    series_args.only = "ql01,ql02"
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 4 and "continuous clock probe running" in summary["halted"] and [r["run_id"] for r in summary["runs"]] == ["ql01"]
    check = json.loads((world.ctx.root / "clock_series_check.json").read_text())
    assert check["probe_exit_before_stop"] == 1 and check["verdict"] == "COVERAGE_GAP" and not check["windows"][0]["covered"]
    assert not world.ctx.rd("ql02").exists()


def test_cmd_run_reports_a_clock_step_inside_a_run_window_but_leaves_the_run_verdict_alone(world, series_args):
    be = FakeBackend(world, s1_text=S1_OK, probe_steps=[(100.0, 2_000_000)])
    rc = ql.cmd_run(series_args, world.ctx, be)
    summary = _summary(world)
    assert rc == 5 and summary["clock_series_check"]["verdict"] == "STEP_IN_RUN_WINDOW"
    assert summary["clock_series_check"]["steps_in_run_windows"][0]["runs"] == ["ql01"]
    assert [(r["run_id"], r["verdict"]) for r in summary["runs"]] == [("ql01", "VALID")]       # A4: the evidence is kept, the existing gates decide


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
