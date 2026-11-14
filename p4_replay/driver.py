"""Derived replay driver. Phase 1: everything here is OFFLINE except execute_replay_run, which is LIVE_UNTESTED and unreachable.

Derived from scripts/e1/e1_quicklook_p1b.py (the frozen, hash-pinned P1b driver). That file is IMPORTED after a hash check and never
edited. What is reused as-is: Session, RunRecorder, Backend (type only), the argv builders, the sampler/Caliper helpers, the halt types.
What is new: the replay matrix, offline staging (render + SHA256SUMS), the step list, the replay fidelity verdict, a prefix-capable S1
evaluator, and the live-execution interlock.

Design decision (OWNER_DECISION_REQUIRED to keep): rendering needs numpy, pandas and PyYAML, which WSL's python3 lacks. So every run is
RENDERED OFFLINE ON WINDOWS into a staging tree with SHA256SUMS, and the live driver only copies and hash-verifies it. That also lets the
owner review and pin the exact schedules before anything live happens.

SAFETY: LIVE_EXECUTION_ENABLED is False. execute_replay_run refuses a real p1b.Backend. `run` refuses with or without an approval.
Flipping the constant is a reviewed code change made only under a separate owner authorisation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from . import PACKAGE_VERSION, REPO_ROOT, ensure_paths
from . import fidelity as fd
from . import offsets as of
from . import prereg as pr
from . import render as rd

ensure_paths()
sys.dont_write_bytecode = True

LIVE_EXECUTION_ENABLED = False
P1B_DRIVER_SHA256 = "00575656d47ca25d624737b9dd816d9d6e889136e2fdf9193b3356f4948488a0"   # frozen, per p1b_source_bundle_2026-11-13/SHA256SUMS.txt
TOOL_VERSION = f"p4_replay.driver {PACKAGE_VERSION}"
MATRIX_HEADER = ["seq", "run_id", "channel", "kind", "window", "schedule", "B", "tau", "P", "replicate"]
CHANNEL_RE = re.compile(r"^e1-rp(\d\d)-b(\d+)-r(\d+)$")
TAU, P_BYTES = "2s", 10_485_760
MAIN_WINDOW, DRY_RUN_WINDOW = "F_82800", "D_33000"
SERIES_ROOT_WSL = "/home/mailf18/p4_replay_series"      # deliberately OUTSIDE /home/mailf18/e1 (snapshot group wsl_e1)


# P1b's run-length assumptions were set for 30 s warm-up + 120 s measured runs. A replay run is 60 s + 600 s of offered load plus round turnover
# and drain, so P1b's defaults would abort every replay: Thresholds.caliper_timeout_s = 300 s (hard abort in run_caliper) and
# EvidenceThresholds.host_max_seconds = 420 (the host sampler's self-stop, baked into p1b.argv_host through a module global).
# The values below are PROPOSED, have no measured basis, and are OWNER_DECISION_REQUIRED; they only have to exceed the load duration.
CALIPER_TIMEOUT_S_PROPOSED = 1200.0
HOST_MAX_SECONDS_PROPOSED = 1500
MIN_CALIPER_TIMEOUT_S = float(of.PRE_ROLL_S + of.MEASURED_S + 120)     # load duration plus a floor for turnover; below this a replay cannot finish


class LiveExecutionRefused(SystemExit):
    """The live path is closed in this build."""


class MatrixError(ValueError):
    pass


_P1B = None


def p1b_module():
    """Import the frozen P1b driver after proving it is byte-identical to the pinned file. Import has no side effects."""
    global _P1B
    if _P1B is None:
        path = REPO_ROOT / "scripts" / "e1" / "e1_quicklook_p1b.py"
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != P1B_DRIVER_SHA256:
            raise RuntimeError(f"the P1b driver is not the frozen file: {digest} != {P1B_DRIVER_SHA256}")
        import e1_quicklook_p1b as module  # noqa: E402
        _P1B = module
    return _P1B


# ----------------------------------------------------------------------------------------------- matrix
@dataclass(frozen=True)
class ReplayRow:
    seq: int
    run_id: str
    channel: str
    kind: str          # dry_run | measured
    window: str
    schedule: str
    B: int
    tau: str
    P: int
    replicate: int

    def csv_row(self) -> list:
        return [self.seq, self.run_id, self.channel, self.kind, self.window, self.schedule, self.B, self.tau, self.P, self.replicate]


def _row(seq: int, kind: str, window: str, B: int, replicate: int) -> ReplayRow:
    run_id = f"rp{seq:02d}"
    return ReplayRow(seq, run_id, f"e1-{run_id}-b{B}-r{replicate}", kind, window, "A", B, TAU, P_BYTES, replicate)


def build_matrix(run_order_seed: int, max_consecutive_same: int = 2) -> list[ReplayRow]:
    """1 dry run first, then the 6 measured runs in the seeded order. The seed is the OWNER's; tests use a throw-away value."""
    rows = [_row(0, "dry_run", DRY_RUN_WINDOW, 10, 0)]
    for item in pr.run_order(run_order_seed, max_consecutive_same):
        rows.append(_row(item["position"], "measured", MAIN_WINDOW, item["B"], item["replicate"]))
    validate_matrix(rows)
    return rows


def validate_matrix(rows: list[ReplayRow]) -> None:
    p1b = p1b_module()
    if [r.seq for r in rows] != list(range(len(rows))):
        raise MatrixError("seq must be 0..n-1 in order")
    if len({r.run_id for r in rows}) != len(rows) or len({r.channel for r in rows}) != len(rows):
        raise MatrixError("run ids and channels must be unique")
    for r in rows:
        if not CHANNEL_RE.match(r.channel) or r.channel in p1b.EXISTING_CHANNELS:
            raise MatrixError(f"channel {r.channel!r} is not a fresh replay channel")
        if r.B not in (10, 40) or r.tau != TAU or r.P != P_BYTES or r.schedule != "A":
            raise MatrixError(f"{r.run_id}: only B in (10, 40), tau 2s, P {P_BYTES}, schedule A are in scope")
    measured = [r for r in rows if r.kind == "measured"]
    if rows[0].kind != "dry_run" or len([r for r in rows if r.kind == "dry_run"]) != 1 or len(measured) != 6:
        raise MatrixError("expected one dry run first and six measured runs")
    if any(r.window != MAIN_WINDOW for r in measured) or rows[0].window != DRY_RUN_WINDOW:
        raise MatrixError("measured runs use F_82800 and the dry run D_33000")
    for b in (10, 40):
        if sorted(r.replicate for r in measured if r.B == b) != [1, 2, 3]:
            raise MatrixError(f"B={b} needs replicates 1, 2, 3")


def matrix_csv(rows: list[ReplayRow]) -> bytes:
    lines = [",".join(MATRIX_HEADER)] + [",".join(str(v) for v in r.csv_row()) for r in rows]
    return ("\n".join(lines) + "\n").encode("ascii")


# ----------------------------------------------------------------------------------------------- context and steps (pure data)
def posix_ctx(series_name: str, *, snapshot: str = "<S1 route not chosen>"):
    """A p1b.Ctx made of PurePosixPath, so argv text keeps forward slashes on any host. Nothing is touched."""
    p1b = p1b_module()
    repo, e1 = PurePosixPath("/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-"), PurePosixPath("/home/mailf18/e1")
    return p1b.Ctx(repo=repo, e1_home=e1, root=PurePosixPath(SERIES_ROOT_WSL) / series_name,
                   net=repo / "fabric-testbed/network/network-e1-recovery.json", env=repo / "fabric-testbed/network/env-e1-caliper-benchmark.json",
                   caliper_ws=e1 / "caliper-ws-benchmark", py="python3", win_repo=r"C:\OJT_SEM3\AI-Assisted-Permissioned-Blockchain-Architecture-",
                   win_snapshot=snapshot, win_python=str(repo / ".venv/Scripts/python.exe"), ref_python=str(repo / ".venv/Scripts/python.exe"),
                   matrix=PurePosixPath("/mnt/d/PLAN_BLOCK18/flashbots_replay_phase1_2026-11-14/replay_matrix.csv"),
                   fabric_bin="/home/mailf18/fabric/fabric-samples/bin")


@dataclass(frozen=True)
class P1bRowShim:
    """What the P1b argv builders read from a Row, filled from a ReplayRow. `rate` is metadata only for the analysis tool."""
    run_id: str
    channel: str
    B: int
    tau: str
    P: int
    rate: float
    measured_s: int
    warmup_s: int


def shim(row: ReplayRow, mean_rate: float, measured_s: int = of.MEASURED_S, pre_roll_s: int = of.PRE_ROLL_S) -> P1bRowShim:
    return P1bRowShim(row.run_id, row.channel, row.B, row.tau, row.P, round(mean_rate, 6), measured_s, pre_roll_s)


def replay_thresholds():
    """P1b's Thresholds with ONLY the run-length limits replaced (PROPOSED). Every other gate number is P1b's, unchanged."""
    import dataclasses
    p1b = p1b_module()
    return dataclasses.replace(p1b.THR, caliper_timeout_s=CALIPER_TIMEOUT_S_PROPOSED)


def sampler_argvs(sh: P1bRowShim, ctx, containers: list[str], run_dir, host_max_seconds: int = HOST_MAX_SECONDS_PROPOSED) -> dict:
    """The three background samplers. p1b.argv_host bakes in --max-seconds 420 (a module global); that value is replaced here."""
    p1b = p1b_module()
    stops = {"metrics": run_dir / "stop_metrics", "resources": run_dir / "stop_resources", "host": run_dir / "stop_host"}
    host = list(p1b.argv_host(sh, ctx, stops["host"]))
    host[host.index("--max-seconds") + 1] = str(host_max_seconds)
    return {"metrics": p1b.argv_scrape(sh, ctx, "during", stops["metrics"]), "resources": p1b.argv_resources(sh, ctx, containers, stops["resources"]),
            "host": host, "stops": stops}


def run_steps(row: ReplayRow, ctx, stage_dir: PurePosixPath, mean_rate: float, chaincode: list | None = None) -> list[dict]:
    """The ordered live steps of one run as DATA. Nothing is executed. `stage_dir` is the staged tree as seen from WSL (/mnt/d/...)."""
    p1b = p1b_module()
    s = shim(row, mean_rate)
    run_dir, ws_run = ctx.rd(row.run_id), ctx.caliper_ws / "runs" / row.run_id
    staged_run, staged_plan = stage_dir / "runs" / row.run_id, stage_dir / "plans" / f"{row.run_id}.plan.json"
    return [
        {"label": "channel", "kind": "run", "argv": p1b.argv_channel(s, ctx), "category": "infra", "note": "fresh channel with B, tau, P; must report VERIFIED"},
        {"label": "stage_copy", "kind": "run", "argv": ["cp", "-r", str(staged_run), str(ws_run)], "category": "infra", "note": "workspace run folder from the staged render"},
        {"label": "stage_plan", "kind": "run", "argv": ["cp", str(staged_plan), str(run_dir / "plan.json")], "category": "infra", "note": "plan.json beside the run's records"},
        {"label": "stage_verify", "kind": "run", "argv": ["sh", "-c", f"cd {ws_run} && sha256sum -c {stage_dir / 'runs' / row.run_id}.SHA256SUMS"], "category": "infra",
         "note": "every copied file equals the pinned render"},
        {"label": "metrics_before", "kind": "run", "argv": p1b.argv_scrape(s, ctx, "before"), "category": "infra"},
        {"label": "samplers", "kind": "background", "category": "infra", "note": "metrics_during, resources, host (P1b argv builders; host --max-seconds raised)",
         "argvs": sampler_argvs(s, ctx, p1b.watch_containers(chaincode), run_dir)},
        {"label": "getinfo_before", "kind": "getinfo", "category": "infra"},
        {"label": "config_before", "kind": "fetch_config", "category": "infra"},
        {"label": "caliper", "kind": "caliper", "category": "abort", "note": "docker exec caliper launch manager with the per-run report path; pre-roll round then measured round"},
        {"label": "getinfo_after", "kind": "getinfo", "category": "infra"},
        {"label": "config_after", "kind": "fetch_config", "category": "infra"},
        {"label": "stop_samplers", "kind": "stop_background", "category": "infra"},
        {"label": "metrics_after", "kind": "run", "argv": p1b.argv_scrape(s, ctx, "after"), "category": "infra"},
        {"label": "metrics_summary", "kind": "run", "argv": p1b.argv_summarize(s, ctx), "category": "data"},
        {"label": "block_fetch", "kind": "run", "argv": p1b.argv_fetch(s, ctx), "category": "data"},
        {"label": "analysis", "kind": "analysis", "category": "data", "note": "e1_benchmark_analyze on the measured round; the pre-roll csv is the other round"},
        {"label": "fidelity", "kind": "fidelity", "category": "data", "note": "p4_replay.fidelity.evaluate over dispatch logs, recorder csv, ledger csv"},
    ]


# ----------------------------------------------------------------------------------------------- staging (offline, Windows)
def sha256_text_lines(root: Path, exclude: set[str] = frozenset()) -> str:
    lines = []
    for p in sorted(root.rglob("*")):
        rel = p.relative_to(root).as_posix()
        if p.is_file() and rel not in exclude:
            lines.append(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {rel}")
    return "\n".join(lines) + "\n"


def stage_series(rows: list[ReplayRow], env: dict, network_path: str, processed_dir: Path, stage_root: Path, *, archive_manifest: Path | None = None,
                 argv: list[str] | None = None, network_doc: dict | None = None) -> dict:
    """Render every run offline into a NEW staging tree: runs/<id>/..., plans/<id>.plan.json, per-run SHA256SUMS, series_manifest.json.
    Starts nothing. Refuses an existing stage_root."""
    validate_matrix(rows)
    if Path(stage_root).exists():
        raise FileExistsError(stage_root)
    stage_root = Path(stage_root)
    (stage_root / "plans").mkdir(parents=True)
    (stage_root / "runs").mkdir()
    identifiers_by_day: dict[str, dict] = {}
    windows: dict[str, tuple] = {}
    series: dict = {"tool": TOOL_VERSION, "argv": argv, "status": "STAGED_OFFLINE_NOT_EXECUTED", "runs": {}, "windows": {}}
    for row in rows:
        if row.window not in windows:
            windows[row.window] = of.load_registered_window(processed_dir, row.window)
        window, lineage = windows[row.window]
        identifiers, day_start_ms = None, None
        if archive_manifest is not None:
            from fabric_perf.replay.trace import load_manifest
            import calendar
            y, m_, d = (int(x) for x in window.day.split("-"))
            day_start_ms = calendar.timegm((y, m_, d, 0, 0, 0)) * 1000
            if window.day not in identifiers_by_day:
                entry = load_manifest(archive_manifest)[window.day]
                a, c = int(window.pre_src_index[0]), int(window.measured_src_index[-1]) + 1
                identifiers_by_day[window.day] = of.load_archive_identifiers(entry["source_path"], entry, a, c)
            identifiers = identifiers_by_day[window.day]
        result = rd.render_replay_run(env, network_path, row.run_id, row.channel, row.B, row.tau, row.P, window, stage_root / "plans" / f"{row.run_id}.plan.json",
                                      ws_root=stage_root, identifiers=identifiers, day_start_ms=day_start_ms, lineage=lineage, argv=argv,
                                      network_doc=network_doc)
        run_dir = stage_root / "runs" / row.run_id
        (stage_root / "runs" / f"{row.run_id}.SHA256SUMS").write_text(sha256_text_lines(run_dir), encoding="ascii", newline="\n")
        series["runs"][row.run_id] = {"row": row.csv_row(), "render_record_sha256": of.sha256_hex((run_dir / "render_record.json").read_bytes()),
                                      "schedules": result["record"]["schedules"], "tx_total": result["record"]["schedules"]["pre_roll"]["n_tx"] + result["record"]["schedules"]["measured"]["n_tx"]}
        series["windows"][row.window] = lineage
    (stage_root / "replay_matrix.csv").write_bytes(matrix_csv(rows))
    (stage_root / "series_manifest.json").write_text(json.dumps(series, indent=2), encoding="utf-8", newline="\n")
    (stage_root / "SHA256SUMS.txt").write_text(sha256_text_lines(stage_root, {"SHA256SUMS.txt"}), encoding="ascii", newline="\n")
    return series


def verify_stage(stage_root: Path) -> dict:
    """Re-hash a staged tree against its SHA256SUMS and re-check each run's render record. Read-only."""
    root = Path(stage_root)
    bad, checked, listed = [], 0, set()
    for line in (root / "SHA256SUMS.txt").read_text(encoding="ascii").splitlines():
        digest, _, rel = line.partition("  ")
        listed.add(rel)
        target = root / rel
        checked += 1
        if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            bad.append(rel)
    unlisted = [p.relative_to(root).as_posix() for p in root.rglob("*")
                if p.is_file() and p.name != "SHA256SUMS.txt" and p.relative_to(root).as_posix() not in listed]
    return {"checked": checked, "mismatched_or_missing": bad, "unlisted": unlisted, "ok": not bad and not unlisted}


# ----------------------------------------------------------------------------------------------- S1 (source drift) evaluator
S1_SUMMARY = re.compile(r"checked=(\d+)\s+mismatched=(\d+)\s+missing=(\d+)\s+unlisted=(\d+)\s+source_drift=(\d+)")


def evaluate_s1(verify_text: str, *, allowed_exact: frozenset = frozenset(), allowed_prefixes: tuple = ()) -> dict:
    """Evaluate `archive_evidence.py verify --check-sources` output. Differs from the P1b gate in two ways: it accepts allowed PREFIXES (a signed
    drift update can name a directory), and only SOURCE_ADDED may ever be allowed: SOURCE_CHANGED and SOURCE_REMOVED always fail."""
    m = S1_SUMMARY.search(verify_text)
    if not m:
        return {"verdict": "FAIL", "reason": "no verify summary line", "unexpected": []}
    checked, mism, miss, unl, drift_n = (int(x) for x in m.groups())
    events = [tuple(line.split(None, 1)) for line in verify_text.splitlines() if line.startswith("SOURCE_") and len(line.split(None, 1)) == 2]
    unexpected = [f"{k} {p.strip()}" for k, p in events
                  if not (k == "SOURCE_ADDED" and (p.strip() in allowed_exact or any(p.strip().startswith(x) for x in allowed_prefixes)))]
    ok = mism == miss == unl == 0 and checked > 0 and not unexpected and len(events) == drift_n
    return {"verdict": "PASS" if ok else "FAIL", "checked": checked, "mismatched": mism, "missing": miss, "unlisted": unl, "source_drift": drift_n,
            "events_parsed": len(events), "unexpected_count": len(unexpected), "unexpected": unexpected[:50]}


# ----------------------------------------------------------------------------------------------- offline fidelity from files
def evaluate_run_dir(run_id: str, stage_run_dir: Path, *, dispatch_dir: Path | None, recorder_dir: Path | None, ledger_csv: Path | None,
                     clock_gate_pass: bool | None, thresholds: fd.FidelityThresholds | None = None) -> dict:
    """Offline evaluation of one run's files against its staged render. Missing files fail their checks; nothing is guessed."""
    tx_rows = of.read_tx_map(stage_run_dir / "tx_map.csv")
    schedules = {}
    for name in ("pre_roll", "measured"):
        text = (stage_run_dir / f"schedule_{name}.txt").read_text(encoding="ascii")
        schedules[name] = [int(x) for x in text.split("\n")]
    run = fd.run_input_from_files(run_id, tx_rows, schedules, dispatch_dir=dispatch_dir, recorder_dir=recorder_dir, ledger_csv=ledger_csv,
                                  clock_gate_pass=clock_gate_pass)
    return fd.evaluate(run, thresholds)


# ----------------------------------------------------------------------------------------------- live path (LIVE_UNTESTED, unreachable)
def _guard_live(be) -> None:
    p1b = p1b_module()
    if isinstance(be, p1b.Backend) and not LIVE_EXECUTION_ENABLED:
        raise LiveExecutionRefused("refused: live execution is not enabled in this build (Phase 1 is offline only; a separate owner authorisation is required)")


def execute_replay_run(s, row: ReplayRow, stage_dir: PurePosixPath, mean_rate: float, *, helpers: dict | None = None, clock_gate_pass: bool | None = None) -> dict:
    """One replay run, strictly in step order, never retried. Derived from p1b.execute_run. LIVE_UNTESTED: only the ordering and halting
    logic is exercised offline, against a recording fake backend with injected helpers. Refuses a real p1b.Backend in this build."""
    p1b = p1b_module()
    _guard_live(s.be)
    if s.thr.caliper_timeout_s < MIN_CALIPER_TIMEOUT_S:
        raise ValueError(f"caliper_timeout_s {s.thr.caliper_timeout_s} cannot fit a {of.PRE_ROLL_S + of.MEASURED_S} s replay: use driver.replay_thresholds(), not P1b's THR")
    h = {"getinfo": p1b.do_getinfo, "fetch_config": p1b.do_fetch_config, "stop_background": p1b.stop_background, "run_caliper": p1b.run_caliper,
         "host_snapshot": p1b.host_snapshot, "load_network": p1b.load_network, "peer_env": p1b.peer_env, "tool_env": p1b.tool_env}
    h.update(helpers or {})
    ctx, be, thr = s.ctx, s.be, s.thr
    run_dir = ctx.rd(row.run_id)
    run_dir.mkdir(parents=False, exist_ok=False)
    rec = p1b.RunRecorder(run_dir, be)
    env, net = h["tool_env"](ctx), h["load_network"](ctx)
    penv = h["peer_env"](ctx, net)
    facts: dict = {"run_id": row.run_id, "channel": row.channel, "started_utc": be.now_utc(), "host": {}, "caliper": {}, "step_failure": None, "aborted": None, "steps_done": []}
    handles: list = []
    steps = {st["label"]: st for st in run_steps(row, ctx, stage_dir, mean_rate, s.chaincode)}
    sh = shim(row, mean_rate)
    try:
        facts["host"]["pre"] = h["host_snapshot"](be, s.helper_pid)
        for label in ("channel", "stage_copy", "stage_plan", "stage_verify", "metrics_before"):
            r = rec.run(label, steps[label]["argv"], env, thr.channel_timeout_s if label == "channel" else thr.tool_timeout_s)
            facts["steps_done"].append(label)
            if r.code != 0:
                raise p1b.StepFailed(label, f"exit {r.code}", steps[label]["category"])
            if label == "channel":
                crec = p1b.load_json(run_dir / "channel" / "channel_run_record.json")
                if not crec or crec.get("status") != "VERIFIED":
                    raise p1b.StepFailed("channel", f"status {(crec or {}).get('status')}")
        sa = steps["samplers"]["argvs"]
        handles = [("metrics", rec.spawn("metrics_during", sa["metrics"], env), sa["stops"]["metrics"]),
                   ("resources", rec.spawn("resources", sa["resources"], env), sa["stops"]["resources"]),
                   ("host", rec.spawn("host", sa["host"], env), sa["stops"]["host"])]
        facts["steps_done"].append("samplers")
        be.sleep(3.0)
        for name, proc, _ in handles:
            if proc.poll() is not None:
                raise p1b.StepFailed(name, f"background sampler exited early with {proc.poll()}")
        h["getinfo"](rec, net, row.channel, penv, "getinfo_before")
        cfg_b = h["fetch_config"](rec, net, row.channel, penv, "config_before", run_dir)
        facts["steps_done"] += ["getinfo_before", "config_before"]
        plan = p1b.ro.Plan(json.loads((run_dir / "plan.json").read_text(encoding="utf-8")))
        cal = h["run_caliper"](s, rec, plan.raw, env)
        facts["caliper"], facts["aborted"] = cal, cal["aborted"]
        facts["steps_done"].append("caliper")
        if cal["aborted"]:
            raise p1b.StepFailed("caliper", cal["aborted"], "abort")
        if cal["exit_code"] != 0:
            raise p1b.StepFailed("caliper", f"exit {cal['exit_code']}")
        info_a = h["getinfo"](rec, net, row.channel, penv, "getinfo_after")
        cfg_a = h["fetch_config"](rec, net, row.channel, penv, "config_after", run_dir)
        want = {"max_message_count": row.B, "batch_timeout_ms": p1b.cr.tau_to_ms(row.tau), "preferred_max_bytes": row.P}
        same = cfg_a["block_sha256"] == cfg_b["block_sha256"] and cfg_a["active_config"] == cfg_b["active_config"]
        requested = all(cfg_b["active_config"].get(k) == v for k, v in want.items())
        p1b.write_json_new(run_dir / "config_check.json", {"unchanged": bool(same and requested), "requested": want})
        facts["steps_done"] += ["getinfo_after", "config_after"]
        facts["stop_background"] = h["stop_background"](be, rec, handles)
        handles = []
        facts["steps_done"].append("stop_samplers")
        height = int(info_a.get("height", 0) or 0)
        for label in ("metrics_after", "metrics_summary", "block_fetch"):
            # block_fetch scales with channel height, as in P1b (a B=10 replay cuts many blocks)
            timeout = max(thr.fetch_timeout_base_s, 120 + thr.fetch_timeout_per_block_s * height) if label == "block_fetch" else thr.tool_timeout_s
            r = rec.run(label, steps[label]["argv"], env, timeout)
            facts["steps_done"].append(label)
            if r.code != 0:
                raise p1b.StepFailed(label, f"exit {r.code}", steps[label]["category"])
        sub = p1b.recorder_base(sh, ctx, "measured")
        sub = sub.with_name(sub.name + ".submits.csv")
        expected_n = sum(int(x["count"]) for x in p1b._read_csv(sub))
        r = rec.run("analysis", p1b.argv_analysis(sh, ctx, expected_n), env, thr.tool_timeout_s)
        facts["steps_done"].append("analysis")
        if r.code != 0:
            raise p1b.StepFailed("analysis", f"exit {r.code}", "data")
    except p1b.StepFailed as exc:
        facts["step_failure"] = {"label": exc.label, "detail": exc.detail, "category": exc.category}
        s.log(f"{row.run_id}: step failed: {exc}")
    finally:
        if handles:
            try:
                facts["stop_background"] = h["stop_background"](be, rec, handles)
            except Exception as exc:                                                       # noqa: BLE001
                facts["stop_background"] = f"error {exc!r}"
        facts["host"]["post"] = h["host_snapshot"](be, s.helper_pid)
        facts["finished_utc"] = be.now_utc()
        rec.close()
        p1b.write_json_new(run_dir / "facts.json", facts)
    verdict: dict = {"run_id": row.run_id, "channel": row.channel, "kind": row.kind, "steps_done": facts["steps_done"]}
    sf = facts["step_failure"]
    if sf:
        verdict.update(verdict="ABORTED_INFRA" if sf["category"] == "abort" else ("INVALID_INFRA" if sf["category"] == "infra" else "INVALID_DATA"), step_failure=sf)
    else:
        ev = evaluate_run_dir(row.run_id, Path(str(stage_dir / "runs" / row.run_id)), dispatch_dir=Path(str(ctx.caliper_ws / "runs" / row.run_id)),
                              recorder_dir=Path(str(ctx.recorder_dir(row.run_id))), ledger_csv=Path(str(run_dir / "ledger/ledger/ledger_tx.csv")),
                              clock_gate_pass=clock_gate_pass)
        p1b.write_json_new(run_dir / "fidelity.json", ev)
        verdict.update(verdict="REPLAY_RECORDED", fidelity=ev["verdict"], failed_checks=ev["failed_checks"])
    p1b.write_json_new(run_dir / "run_verdict.json", verdict)
    return verdict


# ----------------------------------------------------------------------------------------------- CLI
def _listing(rows: list[ReplayRow], mean_rates: dict[str, float]) -> str:
    ctx = posix_ctx("<series>")
    out = [f"{TOOL_VERSION}", "LISTING ONLY: nothing below is executed. LIVE_UNTESTED.", ""]
    for row in rows:
        out.append(f"== {row.run_id} {row.kind} window={row.window} B={row.B} channel={row.channel}")
        for st in run_steps(row, ctx, PurePosixPath("/mnt/d/<stage_root>"), mean_rates.get(row.window, 0.0)):
            argv = " ".join(st["argv"]) if "argv" in st else f"[{st['kind']}] {st.get('note', '')}"
            out.append(f"   {st['label']:<16} {argv}")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="p4_replay.driver", description="Derived replay driver. Phase 1: offline only; the live path is closed.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("matrix", help="print the replay matrix for a run-order seed (offline)")
    m.add_argument("--run-order-seed", type=int, required=True)
    st = sub.add_parser("stage", help="render every run offline into a NEW staging tree with SHA256SUMS (starts nothing)")
    st.add_argument("--run-order-seed", type=int, required=True)
    st.add_argument("--env", type=Path, required=True)
    st.add_argument("--network", required=True, help="network.json path as the live run sees it (a WSL path); recorded in each plan")
    st.add_argument("--network-file", type=Path, help="the same network.json readable here, for offline plan validation")
    st.add_argument("--processed-dir", type=Path, default=REPO_ROOT / "data" / "processed" / "flashbots_ms")
    st.add_argument("--stage-root", type=Path, required=True)
    st.add_argument("--archive-manifest", type=Path, help="load source hashes from the manifest-verified archives (read-only)")
    ls = sub.add_parser("listing", help="print the ordered live steps per run as text; executes nothing")
    ls.add_argument("--run-order-seed", type=int, required=True)
    vs = sub.add_parser("verify-stage", help="re-hash a staging tree (read-only)")
    vs.add_argument("--stage-root", type=Path, required=True)
    s1 = sub.add_parser("s1", help="evaluate saved `archive_evidence.py verify --check-sources` output (offline)")
    s1.add_argument("--verify-output", type=Path, required=True)
    s1.add_argument("--allow-exact", type=Path, help="file with one allowed SOURCE_ADDED path per line")
    s1.add_argument("--allow-prefix", action="append", default=[])
    sub.add_parser("run", help="REFUSED in this build")
    args = ap.parse_args(argv)
    try:
        if args.cmd == "run":
            raise LiveExecutionRefused("refused: the live path is closed in Phase 1. It needs the owner's approval file, a resolved S1 route, "
                                       "approved thresholds, and a separate authorisation that also reviews and enables the live code.")
        if args.cmd == "matrix":
            sys.stdout.write(matrix_csv(build_matrix(args.run_order_seed)).decode("ascii"))
            return 0
        if args.cmd == "listing":
            print(_listing(build_matrix(args.run_order_seed), {}))
            return 0
        if args.cmd == "stage":
            from . import render as _r
            env = _r.lr.load_env(args.env)
            net_doc = json.loads(args.network_file.read_text(encoding="utf-8")) if args.network_file else None
            series = stage_series(build_matrix(args.run_order_seed), env, args.network, args.processed_dir, args.stage_root,
                                  archive_manifest=args.archive_manifest, argv=sys.argv if argv is None else argv, network_doc=net_doc)
            print(f"staged {len(series['runs'])} runs -> {args.stage_root} (rendered only; nothing executed)")
            return 0
        if args.cmd == "verify-stage":
            result = verify_stage(args.stage_root)
            print(json.dumps(result, indent=2))
            return 0 if result["ok"] else 2
        if args.cmd == "s1":
            exact = frozenset(x.strip() for x in args.allow_exact.read_text(encoding="utf-8").splitlines() if x.strip()) if args.allow_exact else frozenset()
            result = evaluate_s1(args.verify_output.read_text(encoding="utf-8", errors="replace"), allowed_exact=exact, allowed_prefixes=tuple(args.allow_prefix))
            print(json.dumps(result, indent=2))
            return 0 if result["verdict"] == "PASS" else 2
    except (OSError, ValueError, KeyError, RuntimeError, of.ScheduleError) as err:
        print(f"INVALID: {err!r}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
