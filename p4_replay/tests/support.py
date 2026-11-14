"""Shared test support for the Phase 2 orchestration tests: a recording FAKE backend with a small world model, and builders for a staged series and for the
evidence a run leaves behind. Nothing here starts a process or a network connection (conftest's guard enforces it). All seeds, hashes and times are TEST VALUES.

The fake is a duck-typed backend, deliberately NOT a p1b.Backend, so the live-execution guard lets it through; the guard itself is tested with a real p1b.Backend.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from p4_replay import REPO_ROOT, driver as dr, fidelity as fd, offsets as of, series_approval as sa, snapshot as sn
from p4_replay.tests.conftest import ENV, NETWORK_PATH, PKG, START_S, synthetic_day

p1b = dr.p1b_module()
NETWORK_DOC = dr.rd.lr.network(ENV, PKG)
BASE_NS = p1b.utc_to_ns("2026-11-15T10:00:00.000Z")
TEST_SEED = 7                      # a throw-away TEST seed: the real run-order seed is the owner's
PREREG_SHA = "ab" * 32
KEEP_PID = 4242


class FakeProc:
    def __init__(self, on_terminate=None):
        self.returncode = None
        self.on_terminate = on_terminate

    def poll(self):
        return self.returncode

    def terminate(self):
        self.returncode = -15
        if self.on_terminate:
            self.on_terminate()

    kill = terminate

    def wait(self, timeout=None):
        return self.returncode


@dataclass
class World:
    """Everything the fake reports about the host and Fabric. Mutate a field to break exactly one gate."""
    win_available_mb: float = 6000.0
    win_commit_pct: float = 60.0
    wsl_available_mb: float = 8000.0
    disk_gb: float = 120.0
    power_line: str = "Online"
    health: dict = field(default_factory=dict)                    # ops port -> status (default 200)
    not_running: set = field(default_factory=set)
    restart_after_calls: int | None = None                        # a container restarts once this many `docker inspect` calls were made
    raft_active: dict = field(default_factory=dict)               # orderer name -> active nodes (default 3)
    existing_channels: tuple = ("e1-r002", "e1-cfg001")
    processes: list = field(default_factory=list)                 # extra (pid, ppid, args) rows of `ps`
    caliper_top: list = field(default_factory=list)
    other_containers: list = field(default_factory=list)
    s1_text: str = "verify: checked=100 mismatched=0 missing=0 unlisted=0 source_drift=0\n"
    fingerprint_changes_after: int | None = None                  # reference-channel height changes after this many getinfo calls
    clock_step_in_run: set = field(default_factory=set)           # run ids during whose Caliper step the REALTIME clock steps by 500 ms
    clock_gap_in_run: set = field(default_factory=set)            # run ids during which the probe stops sampling for 10 s
    config_drift_runs: set = field(default_factory=set)           # run ids whose configuration differs after the workload
    caliper_abort_runs: set = field(default_factory=set)
    faults: dict = field(default_factory=dict)                    # run id -> Faults
    keep_dies_at_start: bool = False
    keep_dies_in_run: set = field(default_factory=set)            # run ids during whose Caliper step the keep-awake helper dies
    probe_dies_at_start: bool = False
    probe_dies_in_run: set = field(default_factory=set)           # run ids during whose Caliper step the continuous clock probe process exits
    fail_commands: set = field(default_factory=set)               # (run id, tool file name): that command exits 1


@dataclass
class Faults:
    extra_lag_ms: object = None            # callable (round_index, seq, intended_offset_ms) -> extra lag ms
    drop_last_dispatch: bool = False
    fail_submission_seq: int | None = None
    invalid_seq: int | None = None
    cpu_base: float = 15.0
    cpu_burst: float | None = None
    wall_minus_mono: float = 0.2
    drop_recorder_row: bool = False


class FakeSeriesBackend:
    OFFLINE_FAKE = True                 # the marker the orchestration requires while the live path is closed

    def __init__(self, world: World | None = None):
        self.w = world or World()
        self.t = 0.0
        self.calls: list[list[str]] = []
        self.spawns: list[list[str]] = []
        self.sleeps: list[float] = []
        self.probe_file: Path | None = None
        self.probe_next = 0
        self.offset_ns = 0
        self.gap_until: float = -1.0
        self.keep_alive = False
        self.keep_killed = False
        self.inspect_calls = 0
        self.getinfo_calls = 0
        self.taskkills: list[list[str]] = []
        self.ledger_for_channel: dict[str, Path] = {}
        self.proc_keep: FakeProc | None = None
        self.proc_probe: FakeProc | None = None

    # ---- time and the continuous clock probe
    def _advance(self, dt: float) -> None:
        self.t += dt
        if self.probe_file is None:
            return
        lines = []
        while self.probe_next <= self.t:
            if not (self.probe_next < self.gap_until):
                lines.append(f"{self.probe_next},{BASE_NS + int(self.probe_next * 1e9) + self.offset_ns},{10**15 + int(self.probe_next * 1e9)}\n")
            self.probe_next += 1
        if lines:
            with self.probe_file.open("a", encoding="utf-8", newline="") as fh:
                fh.writelines(lines)

    def now_utc(self) -> str:
        self._advance(1.0)
        return p1b.ns_to_utc(BASE_NS + int(self.t * 1e9))

    def monotonic(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self._advance(s)

    def inject_clock_step(self, ms: float = 500.0) -> None:
        self.offset_ns += int(ms * 1e6)

    def inject_probe_gap(self, seconds: float = 10.0) -> None:
        self.gap_until = self.t + seconds

    # ---- host and Fabric facts
    def boot_id(self) -> str:
        return "boot-1"

    def host_memory(self, samples, interval_s):
        n = max(1, samples)
        return {"available_mb": [self.w.win_available_mb] * n, "commit_pct": [self.w.win_commit_pct] * n, "pages_sec": [5.0] * n, "rows": n, "skipped": 0}

    def host_power(self, helper_pid=None):
        alive = bool(helper_pid) and helper_pid == KEEP_PID and self.keep_alive and not self.keep_killed
        return {"line": self.w.power_line, "battery": 100, "helper_alive": alive if helper_pid else None}

    def disk_free_gb(self, path):
        return self.w.disk_gb

    def wsl_mem_available_mb(self):
        return self.w.wsl_available_mb

    def http_status(self, url, timeout=2.0):
        port = int(re.search(r":(\d+)/", url).group(1))
        return self.w.health.get(port, 200)

    def http_text(self, url, timeout=5.0):
        name = next((o["name"] for o in NETWORK_DOC["orderers"] if o["ops_url"] == url), None)
        a = self.w.raft_active.get(name, 3)
        return "".join(f'consensus_etcdraft_active_nodes{{channel="{ch}"}} {a}\nconsensus_etcdraft_cluster_size{{channel="{ch}"}} 3\n' for ch in self.w.existing_channels)

    # ---- commands
    def _inspect(self, names):
        self.inspect_calls += 1
        out = []
        for n in names:
            image = "hyperledger/caliper:0.6.0" if n == p1b.CALIPER else ("dev-peer-cc:1.0" if n.startswith("dev-peer") else "hyperledger/fabric-x:2.5.16")
            restarted = self.w.restart_after_calls is not None and self.inspect_calls > self.w.restart_after_calls and n == "orderer2.example.com"
            running = n not in self.w.not_running
            out.append({"Name": "/" + n, "Id": "id-" + n, "RestartCount": 1 if restarted else 0, "Config": {"Image": image},
                        "State": {"Status": "running" if running else "exited", "Running": running, "OOMKilled": False, "StartedAt": "2026-11-15T09:00:00Z"}})
        return json.dumps(out).encode()

    def run(self, argv, env=None, timeout=None):
        self.calls.append([str(a) for a in argv])
        a = [str(x) for x in argv]
        a0 = Path(a[0]).name
        ok = lambda out=b"": p1b.CmdResult(code=0, stdout=out, stderr=b"")                # noqa: E731
        joined = " ".join(a)
        if any(rid in joined and tool in joined for rid, tool in self.w.fail_commands):
            return p1b.CmdResult(code=1, stdout=b"", stderr=b"injected failure")
        if a0 == "docker":
            if a[1] == "inspect":
                return ok(self._inspect(a[2:]))
            if a[1] == "ps" and "--filter" in a:
                return ok(b"dev-peer0.org1.example.com-e1asset_1.0-abc\n")
            if a[1] == "ps":
                return ok(("\n".join([p1b.CALIPER, "orderer.example.com"] + self.w.other_containers) + "\n").encode())
            if a[1] == "top":
                return ok(("PID COMMAND\n" + "\n".join(self.w.caliper_top) + "\n").encode())
            return ok()
        if a0 == "ps":
            rows = ["  PID  PPID COMMAND", "  100     1 init", f"  200   100 bash", f"  300   200 python3 -m p4_replay.series run"] + [f"{p} {q} {c}" for p, q, c in self.w.processes]
            return ok(("\n".join(rows) + "\n").encode())
        if a0 == "peer" and "lifecycle" in a:
            return ok(f"Installed chaincodes on peer:\nPackage ID: {PKG}, Label: e1asset_1.0\n".encode())
        if a0 == "peer" and a[1:3] == ["channel", "getinfo"]:
            self.getinfo_calls += 1
            height = 5 + (1 if (self.w.fingerprint_changes_after is not None and self.getinfo_calls > self.w.fingerprint_changes_after and a[-1] == "e1-r002") else 0)
            return ok(('Blockchain info: {"height":%d,"currentBlockHash":"hh%d","previousBlockHash":"pp"}' % (height, height)).encode())
        if a0 == "peer" and a[1:3] == ["channel", "fetch"]:
            Path(a[4]).write_bytes(b"config-block-" + a[-1].encode())
            return ok()
        if a0 == "osnadmin":
            return ok(b"Status: 200\n" + json.dumps({"channels": [{"name": n} for n in self.w.existing_channels]}).encode())
        if a0 == "wslpath":
            return ok(a[-1].encode())
        if a0 == "taskkill.exe":
            self.taskkills.append(a)
            self.keep_killed = True
            return ok()
        if len(a) > 1 and a[1].endswith("archive_evidence.py"):
            return ok(self.w.s1_text.encode())
        if "--run-dir" in a:                                                              # channel creation
            d = Path(a[a.index("--run-dir") + 1])
            d.mkdir(parents=True, exist_ok=True)
            (d / "channel_run_record.json").write_text(json.dumps({"status": "VERIFIED"}), encoding="utf-8")
        if any(x.endswith("e1_block_fetch.py") for x in a):
            ch = a[a.index("--channel") + 1]
            out = Path(a[a.index("--out-dir") + 1]) / "ledger"
            out.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self.ledger_for_channel[ch], out / "ledger_tx.csv")
        if a[0] == "cp" and a[1] == "-r":
            shutil.copytree(a[2], a[3], dirs_exist_ok=True)
        elif a[0] == "cp":
            shutil.copyfile(a[1], a[2])
        return ok()

    def spawn(self, argv, env=None, stdout_path=None, stderr_path=None):
        a = [str(x) for x in argv]
        self.spawns.append(a)
        joined = " ".join(a)
        if a[0] == "powershell.exe":
            pid_file = Path(env["QL_PID_FILE"])
            if self.w.keep_dies_at_start:
                proc = FakeProc()
                proc.returncode = 1
                return proc
            pid_file.write_text(str(KEEP_PID), encoding="utf-8")
            self.keep_alive = True
            self.proc_keep = FakeProc(on_terminate=lambda: setattr(self, "keep_alive", False))
            return self.proc_keep
        if "e1_clock_rate_probe.py" in joined:
            out = Path(a[a.index("--out-dir") + 1])
            out.mkdir(parents=True, exist_ok=True)
            self.probe_file = out / "clock_samples.csv"
            self.probe_file.write_text("i,real_ns,mono_ns\n", encoding="utf-8")
            self.probe_next = int(self.t) + 1
            self.proc_probe = FakeProc()
            if self.w.probe_dies_at_start:
                self.proc_probe.returncode = 1
            return self.proc_probe
        if "e1_resource_sampler.py" in joined:
            out = Path(a[a.index("--out-dir") + 1])
            out.mkdir(parents=True, exist_ok=True)
            (out / "resource_samples.csv").write_text("container,cpu_percent,read_ns\n", encoding="utf-8")      # the rows arrive with the run's evidence
        return FakeProc()


# ----------------------------------------------------------------------------------------------- a staged series on a synthetic trace
def make_processed(tmp_path: Path) -> Path:
    day = synthetic_day()
    root = tmp_path / "processed"
    (root / "windows").mkdir(parents=True)
    manifest = {"days": {}}
    for date in ("2026-09-21", "2026-10-12"):
        np.save(root / f"arrivals_{date}_ms.npy", day)
        manifest["days"][date] = {"array": f"arrivals_{date}_ms.npy", "array_sha256": of.sha256_hex((root / f"arrivals_{date}_ms.npy").read_bytes()).upper()}
    (root / "flashbots_ms_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    for wid, date in (("F_82800", "2026-10-12"), ("D_33000", "2026-09-21")):
        w = of.build_window(day, wid, date, START_S, 600, 60)
        rec = {"day": date, "start_s": START_S, "length_s": 600, "pre_roll_s": 60, "selected_tile": {"n_tx": int(w.measured_ms.size)},
               "schedule_sha256": {"A": of.sha256_hex(of.schedule_bytes(w.measured_ms, b"\r\n")).upper()}}
        (root / "windows" / wid).mkdir()
        (root / "windows" / wid / "window_record.json").write_text(json.dumps(rec), encoding="utf-8")
    return root


@dataclass
class Staged:
    stage_dir: Path
    rows: list
    manifest: dict
    sums_sha256: str
    seed: int


def build_stage(tmp_path: Path, seed: int = TEST_SEED) -> Staged:
    rows = dr.build_matrix(seed)
    stage = tmp_path / "stage"
    (tmp_path / "network.json").write_text(json.dumps(NETWORK_DOC), encoding="utf-8")      # the plan records this path and the executor re-reads it, so it must exist here
    manifest = dr.stage_series(rows, dict(ENV), str(tmp_path / "network.json"), make_processed(tmp_path), stage, network_doc=NETWORK_DOC)
    return Staged(stage, rows, manifest, hashlib.sha256((stage / "SHA256SUMS.txt").read_bytes()).hexdigest(), seed)


def approval_for(staged: Staged, *, phase="DRY_RUN_THEN_SERIES", status="APPROVED", mutate=None) -> sa.SeriesApproval:
    text = sa.example_approval_text(phase=phase, thresholds_status=status, prereg_sha256=PREREG_SHA, stage_sums_sha256=staged.sums_sha256, seed=staged.seed)
    if mutate:
        for key, value in mutate.items():
            text = re.sub(rf"^{key}=.*$", f"{key}={value}", text, flags=re.M)
    return sa.check_series_approval(text, prereg_sha256=PREREG_SHA, stage_sums_sha256=staged.sums_sha256)


def write_clock_dir(tmp_path: Path, *, verdict: str = "PASS") -> Path:
    import time
    d = tmp_path / "clock_dir"
    d.mkdir(parents=True, exist_ok=True)
    (d / "probe_manifest.json").write_text(json.dumps({"parameters": {"duration_s": 1800}, "end": {"real": time.time_ns()}, "boot_id": "boot-1"}), encoding="utf-8")
    (d / "clock_gate.json").write_text(json.dumps({"verdict": verdict, "criteria": {}, "validity": {}}), encoding="utf-8")
    return d


def make_ctx(tmp_path: Path, name: str = "S"):
    (tmp_path / "network.json").write_text(json.dumps(NETWORK_DOC), encoding="utf-8")
    (tmp_path / "env.json").write_text(json.dumps(ENV), encoding="utf-8")
    base = tmp_path / "series_root"
    return p1b.Ctx(repo=REPO_ROOT, e1_home=tmp_path, root=base / name, net=tmp_path / "network.json", env=tmp_path / "env.json", caliper_ws=tmp_path / "ws", py="python3",
                   win_repo="C:/repo", win_snapshot=sn.SNAPSHOT_PATH_WIN, win_python="py.exe", ref_python="py.exe", matrix=tmp_path / "m.csv", fabric_bin="/x")


# ----------------------------------------------------------------------------------------------- the evidence a run leaves behind
def _rows(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def emit_evidence(be: FakeSeriesBackend, ctx, row, staged: Staged, faults: Faults | None = None) -> None:
    """Pre-create what a real run would leave: dispatch logs, recorder files and manifests, the ledger list, and the Caliper CPU samples. Perfect timing unless a fault says not."""
    f = faults or Faults()
    ws_run, rec_dir = ctx.caliper_ws / "runs" / row.run_id, ctx.recorder_dir(row.run_id)
    run_stage = staged.stage_dir / "runs" / row.run_id
    ledger = [{"tx_id": "cfg", "validation_code": "0", "header_type": "1"}]
    cpu: list[tuple[float, float]] = []
    for idx, name in enumerate(("pre_roll", "measured")):
        offs = [int(x) for x in (run_stage / f"schedule_{name}.txt").read_text().split("\n")]
        t0 = 1_700_000_000_000 + row.seq * 10_000_000 + idx * 100_000
        disp = []
        for i, o in enumerate(offs):
            extra = int(f.extra_lag_ms(idx, i, o)) if f.extra_lag_ms else 0
            disp.append([i, o, t0 + o, t0 + o + 7 + extra, 7 + extra])
        if f.drop_last_dispatch and name == "measured":
            disp.pop()
        _rows(ws_run / f"dispatch_{name}.csv", fd.DISPATCH_HEADER, disp)
        _rows(ws_run / f"dispatch_{name}.csv.overflow.csv", ["requested_seq", "observed_ms"], [[len(offs), t0]])
        recs = []
        for i, o in enumerate(offs):
            status = "failed" if (name == "measured" and f.fail_submission_seq == i) else "success"
            recs.append([f"{row.run_id}t{idx}{i:05d}", i, t0 + o + 7, t0 + o + 1507, status, "true" if status == "success" else "false", "", 0, 0, idx, ""])
        if f.drop_recorder_row and name == "measured":
            recs.pop()
        label = f"wu-{row.run_id}" if idx == 0 else f"m-{row.run_id}"
        _rows(rec_dir / f"{row.run_id}_w0_r{idx}_{label}.csv", ["tx_id", "seq_finish", "create_ms", "final_ms", "status", "verified", "err", "obs_hr_ns", "worker", "round", "label"], recs)
        for i, r in enumerate(recs):
            code = "11" if (name == "measured" and f.invalid_seq == i) else "0"
            ledger.append({"tx_id": r[0], "validation_code": code, "header_type": "3"})
        (rec_dir / f"{row.run_id}_w0_r{idx}_{label}.manifest.json").write_text(json.dumps({"clock": {"wall_minus_monotonic_ms": f.wall_minus_mono, "wall0_ms": t0, "wall1_ms": t0 + 600_000},
                                                                                            "event_loop_delay": {"p99_ms": 11.4}, "counts": {}}), encoding="utf-8")
        if name == "measured":
            _rows(rec_dir / f"{row.run_id}_w0_r1_m-{row.run_id}.submits.csv", ["seq_submit", "t_submit_evt_ms", "count"], [[0, t0, len(offs)]])
            burst_secs = np.where(np.bincount(np.array(offs) // 1000, minlength=600) >= 40)[0]
            for sec in range(0, 606):
                wall = t0 + sec * 1000
                in_burst = any(b <= sec < b + 3 for b in burst_secs)
                cpu.append((wall + 500, (f.cpu_burst if (in_burst and f.cpu_burst is not None) else f.cpu_base)))
    ledger_path = ctx.e1_home / f"ledger_{row.run_id}.csv"
    _rows(ledger_path, ["tx_id", "block", "index", "header_type", "validation_code", "env_bytes", "channel_id", "ch_ts_ms"],
          [[x["tx_id"], 0, 0, x["header_type"], x["validation_code"], 0, row.channel, 0] for x in ledger])
    be.ledger_for_channel[row.channel] = ledger_path
    res = ctx.rd(row.run_id) / "resources" / "resource_samples.csv"
    res.parent.mkdir(parents=True, exist_ok=True)
    lines = ["container,cpu_percent,read_ns"] + [f"{p1b.CALIPER},{pct},{int(ms * 1e6)}" for ms, pct in cpu]
    res.write_text("\n".join(lines) + "\n", encoding="utf-8")


def helpers_for(be: FakeSeriesBackend, w: World, ctx, staged: "Staged"):
    """Executor helpers (Phase 1 style): the parts that need Fabric tools are injected; everything else runs for real against the fake."""
    def row_b(channel: str) -> int:
        return int(re.search(r"-b(\d+)-", channel).group(1))

    def caliper(s, rec, plan_raw, env):
        run_id = plan_raw["run_id"]
        emit_evidence(be, ctx, next(r for r in staged.rows if r.run_id == run_id), staged, w.faults.get(run_id))      # what the run leaves behind, created while it runs
        be.sleep(60.0)
        if run_id in w.clock_step_in_run:
            be.inject_clock_step(500.0)
            be.sleep(5.0)
        if run_id in w.keep_dies_in_run:
            be.keep_alive = False
        if run_id in w.probe_dies_in_run:
            be.proc_probe.returncode = 1
        if run_id in w.clock_gap_in_run:
            be.inject_probe_gap(10.0)
            be.sleep(12.0)
        if run_id in w.caliper_abort_runs:
            return {"exit_code": None, "timed_out": False, "aborted": "container event: restart", "duration_s": 5.0}
        return {"exit_code": 0, "timed_out": False, "aborted": None, "duration_s": 60.0}

    def fetch_config(rec, net, channel, penv, label, rd_):
        drift = label == "config_after" and any(r in str(rd_) for r in w.config_drift_runs)
        return {"active_config": {"max_message_count": row_b(channel), "batch_timeout_ms": 2000, "preferred_max_bytes": 10_485_760},
                "block_sha256": "x" if not drift else "y", "block_number": 0, "block_data_hash": "h"}

    return {"host_snapshot": lambda be_, pid=None: {"stub": True}, "load_network": lambda ctx: object(), "peer_env": lambda ctx, net: {}, "tool_env": lambda ctx: {},
            "getinfo": lambda rec, net, ch, penv, label: {"height": 12}, "fetch_config": fetch_config,
            "stop_background": lambda be_, rec, handles, timeout_s=120.0: {n: 0 for n, _, _ in handles}, "run_caliper": caliper}


def prepare_series(tmp_path: Path, world: World | None = None, *, phase="DRY_RUN_THEN_SERIES", status="APPROVED", seed: int = TEST_SEED, approval_mutate=None):
    """Everything a series test needs: stage, approval, ctx, fake backend with evidence for every run, helpers, clock dir."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    w = world or World()
    staged = build_stage(tmp_path, seed)
    be = FakeSeriesBackend(w)
    ctx = make_ctx(tmp_path)
    approval = approval_for(staged, phase=phase, status=status, mutate=approval_mutate)
    s = p1b.Session(ctx=ctx, be=be, thr=dr.replay_thresholds())
    return {"w": w, "be": be, "ctx": ctx, "staged": staged, "approval": approval, "session": s, "helpers": helpers_for(be, w, ctx, staged), "clock_dir": write_clock_dir(tmp_path),
            "preflight_dir": tmp_path / "preflight", "env_doc": dict(ENV)}


def run_series(world_parts: dict, **kw) -> dict:
    from p4_replay import series as se
    p = world_parts
    args = dict(own_pid=300, clock_dir=p["clock_dir"], env_doc=p["env_doc"], preflight_dir=p["preflight_dir"], helpers=p["helpers"], node=False,
                series_root_prefix=str(p["ctx"].root.parent).replace("\\", "/"))
    args.update(kw)
    return se.run_replay_series(p["session"], p["staged"].rows, p["staged"].stage_dir, p["approval"], **args)
