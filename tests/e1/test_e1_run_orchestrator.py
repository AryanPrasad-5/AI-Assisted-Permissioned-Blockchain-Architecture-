"""Run orchestrator end to end against one fake world. Synthetic only; not an E0 or E1 result.

Every real tool runs (clock handshake, metrics scraper, resource sampler, block fetch, log collector,
reconciler) and the real recorder writes the per-transaction files through tests/e1/js/fake_caliper.js.
Fakes stand in only at the process/HTTP boundary (docker, peer CLI, /metrics) and for the channel step,
which e1_channel_run.py's own 101 tests cover. Synthetic timestamps are chosen to satisfy the checks; they
are not a model of Fabric timing. Skipped if Node.js is not on PATH.
"""
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_clock_handshake as ch  # noqa: E402
import e1_metrics_scraper as ms  # noqa: E402
import e1_resource_sampler as rs  # noqa: E402
import e1_run_orchestrator as ro  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="Node.js not on PATH: recorder integration NOT VERIFIED")
ORD, PEERS, CAL = ["orderer1.example.com"], ["peer0.org1.example.com", "peer0.org2.example.com"], "caliper"
N, B, P = 200, 10, 524_288


class World:
    def __init__(self, tmp: Path, **opt):
        self.tmp, self.opt, self.clock = tmp, opt, sf.Clock()
        self.recorder_dir = (tmp / "recorder").resolve()
        self.records, self.peer, self.calls = None, None, []
        self.n_inspect, self.fetch_calls = 0, {}

    # -------- docker, Caliper
    def runner(self, argv):
        self.calls.append(argv)
        self.clock.step(200_000)
        if argv[:2] == ["docker", "exec"] and argv[3:] == ch.IDENTITY_PROBE:
            boot = "0" * 8 + sf.BOOT_ID[8:] if self.opt.get("boot_other") == argv[2] else sf.BOOT_ID
            return 0, sf.identity_output(boot), b""
        if argv[:2] == ["docker", "exec"]:
            rtt = self.opt.get("exec_rtt_ns", 400_000)
            self.clock.step(rtt // 2)
            stamp = self.clock.wall
            self.clock.step(rtt - rtt // 2)
            return 0, f"{stamp}\n".encode(), b""
        if argv[:2] == ["docker", "stats"]:
            return 0, ("\n".join(sf.stats_line(c, cid=hashlib.sha256(c.encode()).hexdigest())
                                 for c in ORD + PEERS + [CAL]) + "\n").encode(), b""
        if argv[:2] == ["docker", "inspect"]:
            self.n_inspect += 1
            restarted = self.opt.get("restart") and self.n_inspect > 1
            docs = [sf.inspect_doc(c, cid=hashlib.sha256(c.encode()).hexdigest(),
                                   started="2023-11-14T23:00:00Z" if restarted and c == PEERS[1] else
                                   "2023-11-14T22:00:00Z") for c in argv[2:]]
            return 0, json.dumps(docs).encode(), b""
        if argv[:2] == ["docker", "logs"]:
            if self.opt.get("no_logs") or self.opt.get("no_logs_for") == argv[-1]:
                return 0, b"", b""
            return 0, b"", ("\n".join(self.log_lines(argv[-1])) + "\n").encode()
        if argv[0] == "fake-caliper":
            return self.caliper()
        return 127, b"", f"unexpected {argv}".encode()

    def caliper(self):
        out = subprocess.run([NODE, str(REPO / "tests" / "e1" / "js" / "fake_caliper.js"), str(self.recorder_dir),
                              "r001", str(N), "0"], capture_output=True, text=True, check=True)
        self.clock.step(25 * 10**9)
        base = self.recorder_dir / "r001_w0_r1_m-r001.csv"
        if self.opt.get("dup_record"):
            rows = base.read_text(encoding="utf-8").splitlines()
            base.write_text("\n".join(rows + [rows[8]]) + "\n", encoding="utf-8")
        with base.open(newline="", encoding="utf-8") as fh:
            self.records = list(csv.DictReader(fh))
        text = out.stdout + ("2023-11-14 Unexpected error: connector failed\n" if self.opt.get("unexpected") else "")
        return self.opt.get("caliper_exit", 0), text.encode(), b""

    def blocks(self):
        ordered = sorted({r["tx_id"]: r for r in self.records}.values(), key=lambda r: float(r["create_ms"]))
        return [r["tx_id"] for r in ordered], {r["tx_id"]: int(float(r["create_ms"])) for r in ordered}

    def log_lines(self, container):
        ids, create = self.blocks()
        lc = self.opt.get("lifecycle_blocks", 0)
        lines = []
        for k in range(1, 1 + (len(ids) + B - 1) // B):
            grp = ids[(k - 1) * B:k * B]
            c0 = create[grp[0]]
            lag = -300_000 if self.opt.get("log_inversion") == container else None
            if container in ORD:
                lines.append(sf.orderer_log_line(k + lc, c0 + 380, **({"lag_ns": lag} if lag else {})))
            else:
                lines.append(sf.peer_log_line(k + lc, c0 + 400, len(grp), **({"lag_ns": lag} if lag else {})))
        return lines

    # -------- peer CLI
    def net_runner(self, argv, env):
        lc = self.opt.get("lifecycle_blocks", 0)
        if self.records is None:      # before Caliper: the channel holds genesis + lifecycle blocks only
            assert argv[1:3] == ["channel", "getinfo"], argv
            if self.opt.get("getinfo_fail"):
                return 1, b"", b"Error: channel not found\n"
            return 0, ('Blockchain info: {"height":%d,"currentBlockHash":"x"}\n' % (1 + lc)).encode(), b""
        if self.peer is None:
            ids, _ = self.blocks()
            groups = sf.ledger_groups(ids, B)
            groups[1:1] = [[{"tx_id": f"lifecycle{i}", "body_len": 900}] for i in range(lc)]
            self.peer = sf.FakePeer(sf.chain(groups), fail={k + lc: v for k, v in (self.opt.get("fetch_fail") or {}).items()})
        return self.peer(argv, env)

    # -------- /metrics
    def fetch(self, url, timeout):
        n = self.fetch_calls.get(url, 0)
        self.fetch_calls[url] = n + 1
        self.clock.step(2_000_000)
        if "orderer" in url:
            return 200, sf.orderer_metrics(n, leader_changes=self.opt.get("leader_changes", 0) if n else 0).encode()
        return 200, sf.peer_metrics(n).encode()

    # -------- channel step and background processes
    def channel_setup(self, req, net, d):
        d.mkdir(parents=True)
        rec = {"status": self.opt.get("channel_status", "VERIFIED"), "request": req.as_record(), "detail": ""}
        (d / "channel_run_record.json").write_text(json.dumps(rec), encoding="utf-8")
        self.clock.step(3 * 10**9)
        return rec

    def start_bg(self, kind, out_dir, params):
        return kind, out_dir, params

    def stop_bg(self, h):
        kind, out_dir, params = h
        if kind == "metrics":
            ms.run_scrape(ms.parse_targets(params["targets"]), "during", out_dir, fetch=self.fetch,
                          wall_ns=self.clock.wall_ns, mono_ns=self.clock.mono_ns, sleep=self.clock.sleep,
                          max_rounds=3, family_filter=params["family_filter"])
        else:
            rs.run_sample(params["containers"], out_dir, self.runner, wall_ns=self.clock.wall_ns,
                          mono_ns=self.clock.mono_ns, sleep=self.clock.sleep, max_ticks=3)
        return {"exit_code": 0}

    def backend(self):
        local = self.opt.get("local", {"status": "OK", "boot_id": sf.BOOT_ID, "kernel_release": "6.6",
                                       "time_ns": "time:[4026531834]"})
        return ro.Backend(runner=self.runner, net_runner=self.net_runner, fetch=self.fetch,
                          channel_setup=self.channel_setup, start_background=self.start_bg,
                          stop_background=self.stop_bg, wall_ns=self.clock.wall_ns, mono_ns=self.clock.mono_ns,
                          sleep=self.clock.sleep, local_identity=lambda: local)


def plan(tmp: Path, **over) -> dict:
    p = {"run_id": "r001", "channel": sf.CHANNEL,
         "cell": {"B": B, "tau": "2s", "P": P, "n": N, "mode": "txNumber", "rate_tps": 10},
         "network": sf.network(tmp),
         "containers": {"orderers": ORD, "peers": PEERS, "caliper": CAL},
         "metrics_targets": ["orderer1:orderer=http://orderer1.example.com:9443/metrics",
                             "peer0org1:peer=http://peer0.org1.example.com:9444/metrics",
                             "peer0org2:peer=http://peer0.org2.example.com:9445/metrics"],
         "caliper": {"argv": ["fake-caliper"], "recorder_dir": str((tmp / "recorder").resolve()), "worker": 0,
                     "measured_round": 1, "measured_label": "m-r001", "warmup_label": None},
         "reconcile_peer": PEERS[0],
         "expect": {"event_strategy": "msp_any", "invoke_timeout_s": 60, "binding": "fabric:2.5",
                    "expect_all_success": True, "expect_block_pattern": True},
         "clock": {"rounds": 3, "protocol_bound_ms": 1.0, "drift_tolerance_ms": 2.0},
         "sampling": {"interval_s": 1.0, "metrics_family_filter": "^(blockcutter|broadcast|consensus|ledger)_"},
         "log_margin_s": 5}
    p.update(over)
    return p


def go(tmp_path, **opt):
    w = World(tmp_path, **opt)
    m = ro.run(ro.Plan(plan(tmp_path)), tmp_path / "run", w.backend())
    g = json.loads((tmp_path / "run" / "gates.json").read_text(encoding="utf-8"))
    return w, m, g, {x["gate"]: x for x in g["gates"]}


# ------------------------------------------------------------------ end to end
@needs_node
def test_clean_run_passes_every_orchestrator_gate(tmp_path):
    w, m, g, by = go(tmp_path)
    failing = {k: (v["status"], v["detail"]) for k, v in by.items() if v["status"] == "FAIL"}
    assert g["orchestrator_verdict"] == "PASS", failing
    for k in ("SETUP-channel", "SETUP-caliper-exit", "V1-worker-log", "V1-counts", "V1-duplicates", "V3-success",
              "V3-ledger-count", "V4-order", "V4-cross-source", "V7-config", "E0-1-blocks", "E0-1-causes",
              "V3-no-failures", "V7-clock", "TEL-ledger", "TEL-logs", "TEL-metrics", "TEL-resources", "TEL-clock",
              "CLOCK-domain", "V6-leader-changes", "V6-containers"):
        assert by[k]["status"] == "PASS", (k, by[k])
    assert {k for k, v in by.items() if v["evaluated_by"] == "analysis"} == {"V2-pacing", "V5-count-cut-margin",
                                                                              "V6-host-memory"}
    assert g["outcome_metrics_computed"] is False
    # manifest: written last, hashes every artifact, records symbols and event semantics (LA-3(c))
    run = tmp_path / "run"
    assert m["gates"]["sha256"] == hashlib.sha256((run / "gates.json").read_bytes()).hexdigest()
    for a in m["artifacts"]:
        assert hashlib.sha256((run / a["path"]).read_bytes()).hexdigest() == a["sha256"]
    paths = {a["path"] for a in m["artifacts"]}
    assert {"ledger/ledger/ledger_tx.csv", "logs/parsed/peer_commit_reconcile.csv", "reconcile/gate.json",
            "metrics/summary/metric_deltas.csv", "resources/resource_samples.csv", "clock/drift.json"} <= paths
    assert m["symbols"]["P"] == {"field": "Orderer.BatchSize.PreferredMaxBytes", "unit": "bytes"}
    assert m["cell"]["B"] == B and m["cell"]["P"] == P and m["cell"]["tau"] == "2s"
    cfg = m["recorder"]["config_in_force"]
    assert cfg["event_strategy"] == "msp_any" and cfg["invoke_timeout_s"] == 60 and cfg["binding"] == "fabric:2.5"
    assert [s["step"] for s in m["steps"]][:4] == ["clock_start", "channel", "setup_height", "metrics_before"]
    assert by["RUN-complete"]["status"] == "PASS"
    assert all(s["status"] not in ("ERROR", "SKIPPED") for s in m["steps"])
    exp = json.loads((run / "reconcile" / "expect.json").read_text())
    assert exp["clock_offset_bound_ms"] == 1.0 and "demonstrated by identity" in exp["clock_offset_bound_source"]
    assert [i["verdict"] for i in m["clock_identity"]] == ["SAME_KERNEL_CLOCK"] * 2
    assert m["clock_identity"][0]["host_in_container_clock_domain"] is True
    assert m["log_lag"]["status"] == "PASS" and m["log_lag"]["inversions"] == 0


@needs_node
def test_lifecycle_blocks_of_the_channel_setup_are_outside_the_round(tmp_path):
    # real Fabric (E0-0, e1-e00a2): approve x2 + commit occupy blocks 1-3; the setup height is 4
    w, m, g, by = go(tmp_path, lifecycle_blocks=3)
    assert g["orchestrator_verdict"] == "PASS", {k: v["detail"] for k, v in by.items() if v["status"] == "FAIL"}
    assert m["ledger_range"]["first_block"] == 4 and by["V3-foreign"]["status"] == "PASS"
    ledger = [r["tx_id"] for r in ro.tc.read_csv(tmp_path / "run" / "ledger" / "ledger" / "ledger_tx.csv")]
    assert not any(t.startswith(("lifecycle", "cfg")) for t in ledger) and len(ledger) == N


@needs_node
def test_unknown_setup_height_aborts_before_caliper(tmp_path):
    w, m, g, by = go(tmp_path, getinfo_fail=True)
    # nothing measured must never PASS: the abort fails RUN-complete even though no other gate could FAIL
    assert g["orchestrator_verdict"] == "FAIL" and w.records is None
    assert by["RUN-complete"]["status"] == "FAIL" and "setup_height=FAILED" in by["RUN-complete"]["detail"]
    assert {s["step"]: s["status"] for s in m["steps"]}["setup_height"] == "FAILED"


@needs_node
def test_slow_exec_no_longer_decides_the_clock_gate_and_never_widens_v4(tmp_path):
    # a 40 ms docker-exec round trip: the handshake bound is ~40 ms (recorded, informational); the clock domain is
    # still demonstrated by identity, and the V4-cross-source bound stays at the protocol's 1 ms
    w, m, g, by = go(tmp_path, exec_rtt_ns=40_000_000)
    assert by["CLOCK-domain"]["status"] == "PASS" and g["orchestrator_verdict"] == "PASS"
    assert m["clock_bound"]["handshake_bound_ms"] > 40 and m["clock_bound"]["v4_bound_ms"] == 1.0
    # the bound the reconciler actually used
    exp = json.loads((tmp_path / "run" / "reconcile" / "expect.json").read_text(encoding="utf-8"))
    assert exp["clock_offset_bound_ms"] == 1.0
    v4 = json.loads((tmp_path / "run" / "reconcile" / "gate.json").read_text(encoding="utf-8"))["checks"]
    assert "offset_bound_ms=1.0" in next(c["detail"] for c in v4 if c["check_id"] == "V4-cross-source")


@needs_node
def test_windows_host_process_is_flagged_but_containers_decide(tmp_path):
    w, m, g, by = go(tmp_path, local={"status": "NOT_LINUX", "platform": "win32"})
    assert by["CLOCK-domain"]["status"] == "PASS"
    assert m["clock_identity"][0]["host_in_container_clock_domain"] is None


@needs_node
def test_channel_mismatch_aborts_before_caliper(tmp_path):
    w, m, g, by = go(tmp_path, channel_status="CONFIG_MISMATCH")
    assert g["orchestrator_verdict"] == "FAIL" and by["SETUP-channel"]["status"] == "FAIL"
    assert not any(c[0] == "fake-caliper" for c in w.calls) and w.records is None
    status = {s["step"]: s["status"] for s in m["steps"]}
    assert status["caliper"] == "SKIPPED" and status["block_fetch"] == "SKIPPED" and status["clock_end"] != "SKIPPED"
    assert (tmp_path / "run" / "run_manifest.json").exists()


@needs_node
@pytest.mark.parametrize("opt,gate", [
    ({"caliper_exit": 1}, "SETUP-caliper-exit"),
    ({"unexpected": True}, "V1-worker-log"),
    ({"dup_record": True}, "V1-duplicates"),
    ({"leader_changes": 1}, "V6-leader-changes"),
    ({"restart": True}, "V6-containers"),
    ({"boot_other": PEERS[1]}, "CLOCK-domain"),     # a container on another kernel: two clocks
    ({"log_inversion": ORD[0]}, "CLOCK-domain"),    # docker stamped a line 0.3 ms before Fabric wrote it
    ({"no_logs": True}, "TEL-logs"),               # both streams empty: stderr is still parsed -> input_empty
    ({"no_logs_for": PEERS[1]}, "TEL-logs"),       # one silent container must not drop out of the check
])
def test_each_injected_fault_fails_its_gate(tmp_path, opt, gate):
    w, m, g, by = go(tmp_path, **opt)
    assert g["orchestrator_verdict"] == "FAIL" and by[gate]["status"] == "FAIL", by[gate]


@needs_node
def test_missing_block_stops_reconciliation_and_fails_the_ledger_gate(tmp_path):
    w, m, g, by = go(tmp_path, fetch_fail={5: -1})
    assert by["TEL-ledger"]["status"] == "FAIL" and by["RECONCILE"]["status"] == "NOT EVALUATED"
    assert g["orchestrator_verdict"] == "FAIL"
    assert not (tmp_path / "run" / "reconcile").exists()
    assert {s["step"]: s["status"] for s in m["steps"]}["reconcile"] == "NOT_RUN"


# ------------------------------------------------------------------ plan validation and CLI (no Node needed)
@pytest.mark.parametrize("change,msg", [
    (lambda p: p["cell"].update(max_message_count=10), "cell keys"),
    (lambda p: p["cell"].pop("P"), "cell keys"),
    (lambda p: p["cell"].update(P=45, A=40), "PreferredMaxBytes"),
    (lambda p: p["cell"].update(P="512 KB"), "PreferredMaxBytes"),
    (lambda p: p.update(reconcile_peer="peer9"), "reconcile_peer"),
    (lambda p: p["caliper"].update(recorder_dir="relative/dir"), "absolute"),
    (lambda p: p.update(extra=1), "unknown"),
    (lambda p: p["containers"].update(caliper=PEERS[0]), "distinct"),
])
def test_plan_validation(tmp_path, change, msg):
    p = plan(tmp_path)
    change(p)
    with pytest.raises(ValueError, match=msg):
        ro.Plan(p)


def test_cli_dry_run_refusals(tmp_path, capsys):
    pf = tmp_path / "plan.json"
    pf.write_text(json.dumps(plan(tmp_path)), encoding="utf-8")
    assert ro.main(["--plan", str(pf), "--run-dir", str(tmp_path / "dry")]) == 0
    out = capsys.readouterr().out
    assert "dry run" in out and "P=524288 bytes" in out and not (tmp_path / "dry").exists()
    (tmp_path / "exists").mkdir()
    assert ro.main(["--plan", str(pf), "--run-dir", str(tmp_path / "exists"), "--execute"]) == 2
    # execution only inside the Ubuntu WSL distro, and only with POSIX paths
    assert ro.main(["--plan", str(pf), "--run-dir", str(tmp_path / "w"), "--execute"], platform_name="win32") == 2
    assert "only inside the Ubuntu WSL distro" in capsys.readouterr().out
    assert ro.main(["--plan", str(pf), "--run-dir", str(tmp_path / "w"), "--execute"], platform_name="linux") == 2
    assert "Windows-style paths" in capsys.readouterr().out and not (tmp_path / "w").exists()
    assert ro.tc.windows_style_paths({"a": ["/home/u/x", "C:\\x"], "b": {"c": "D:/runs"}, "d": 3}) == \
        ["/a/1=C:\\x", "/b/c=D:/runs"]
    bad = plan(tmp_path)
    bad["cell"]["B"] = 0
    pf.write_text(json.dumps(bad), encoding="utf-8")
    assert ro.main(["--plan", str(pf), "--run-dir", str(tmp_path / "x")]) == 2
    with pytest.raises(ro.PlanError):
        ro.run(ro.Plan(plan(tmp_path)), tmp_path / "exists", World(tmp_path).backend())
