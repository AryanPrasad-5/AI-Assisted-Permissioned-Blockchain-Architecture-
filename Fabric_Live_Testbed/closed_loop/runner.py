"""Closed-loop live runner (WSL, stdlib only). One run = one arm on one fresh channel. Refuses without --execute and the
owner's authorization file. Nothing is retried; a failed step ends the run with a recorded verdict.

Steps: verify stage + arm + authorization -> preflight (containers, memory, disk) -> e1_channel_run.py (fresh channel, B=10)
-> actuator inspect (initial config, independent fetch+decode) -> copy workspace -> Caliper (docker exec) with the control loop
running beside it -> actuator inspect (final config) -> e1_block_fetch.py (whole ledger) -> analyze.

Control loop: when round d's recorder manifest appears, read it (telemetry), ask the arm's controller for segment d, and if the
decision differs from the configuration in force, call e1_config_actuator.py apply --execute with the owner's authorization.
The controller is told about a new configuration only after the actuator reports success with independent verification.
If an actuation fails, actuation is disabled for the rest of the run (decisions are still recorded) and the run is
INVALID_ACTUATION.

  python3 -B -m Fabric_Live_Testbed.closed_loop.runner --stage <stage> --run-id cl01 --authorization <owner file> \
      --out-root /home/mailf18/e1/closed-loop-20261123 [--execute]
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import PACKAGE_VERSION, REPO_ROOT, ensure_paths
from .telemetry import TelemetryError, manifest_ready, read_round, round_base

ensure_paths()

from fabric_perf.controllers import channel_selection as cs  # noqa: E402
from fabric_perf.controllers.base import FabricConfig, confirmed_config  # noqa: E402

from .authorization import AuthorizationError, ExperimentAuthorization  # noqa: E402
from .policies import build_policy, config_with_b  # noqa: E402

ORDERERS = ("orderer.example.com", "orderer2.example.com", "orderer3.example.com")
PEERS = ("peer0.org1.example.com", "peer0.org2.example.com")
CALIPER = "caliper-benchmark-001"
DEFAULTS = {"network": REPO_ROOT / "fabric-testbed/network/network-e1-recovery.json",
            "caliper_ws": Path("/home/mailf18/e1/caliper-ws-benchmark"),
            "orderer_admin_msp": Path("/home/mailf18/fabric/fabric-samples/test-network/organizations/ordererOrganizations/"
                                      "example.com/users/Admin@example.com/msp"),
            "fabric_bin": "/home/mailf18/fabric/fabric-samples/bin"}
MIN_WSL_AVAIL_MB, MIN_DISK_FREE_GB = 1500, 3.0


class RunRefused(RuntimeError):
    pass


class StepFailed(RuntimeError):
    def __init__(self, step: str, detail: str):
        super().__init__(f"{step}: {detail}")
        self.step, self.detail = step, detail


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def sha256_file(p: Path) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def write_json_x(p: Path, doc) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("x", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True, default=str)
        fh.write("\n")


# ----------------------------------------------------------------------------------------------------- the world
@dataclass
class Cmd:
    code: int | None
    stdout: bytes
    stderr: bytes
    duration_s: float


class World:
    """The only place that touches processes, clocks and /proc. Tests replace it."""

    def __init__(self, env: dict):
        self.env = env

    def run(self, argv: list[str], timeout_s: float, env: dict | None = None) -> Cmd:
        t0 = time.monotonic()
        try:
            p = subprocess.run(argv, capture_output=True, timeout=timeout_s, check=False, env=env or self.env)
            return Cmd(p.returncode, p.stdout, p.stderr, time.monotonic() - t0)
        except subprocess.TimeoutExpired as exc:
            return Cmd(None, exc.stdout or b"", (exc.stderr or b"") + b"\nTIMEOUT", time.monotonic() - t0)
        except OSError as exc:
            return Cmd(None, b"", repr(exc).encode(), time.monotonic() - t0)

    def spawn(self, argv: list[str], stdout_path: Path, stderr_path: Path):
        return subprocess.Popen(argv, stdout=stdout_path.open("xb"), stderr=stderr_path.open("xb"), env=self.env)

    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, s: float) -> None:
        time.sleep(s)

    def mem_available_mb(self) -> float:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 1024
        raise RuntimeError("MemAvailable missing")

    def disk_free_gb(self, path: Path) -> float:
        return shutil.disk_usage(path).free / 1e9


# ----------------------------------------------------------------------------------------------------- run context
@dataclass
class Ctx:
    stage: Path
    run_id: str
    authorization: Path
    out_root: Path
    network: Path
    caliper_ws: Path
    orderer_admin_msp: Path
    python: str = sys.executable or "python3"
    poll_s: float = 0.2
    health_every_s: float = 5.0
    actuator_command_timeout_s: float = 180.0
    actuator_commit_timeout_s: float = 60.0

    @property
    def rd(self) -> Path:
        return self.out_root / self.run_id

    @property
    def run_ws(self) -> Path:
        return self.caliper_ws / "runs" / self.run_id

    @property
    def recorder_dir(self) -> Path:
        return self.run_ws / "recorder"

    def tool(self, name: str) -> str:
        return str(REPO_ROOT / "scripts" / "e1" / name)


class Recorder:
    def __init__(self, rd: Path, world: World):
        self.rd, self.world, self.n = rd, world, 0
        (rd / "commands").mkdir(parents=True, exist_ok=False)
        self.events = (rd / "events.jsonl").open("x", encoding="utf-8")

    def event(self, kind: str, **fields) -> None:
        self.events.write(json.dumps({"utc": utc_now(), "kind": kind, **fields}, sort_keys=True, default=str) + "\n")
        self.events.flush()

    def run(self, label: str, argv: list[str], timeout_s: float, env: dict | None = None) -> Cmd:
        self.n += 1
        stem = self.rd / "commands" / f"{self.n:03d}_{label}"
        r = self.world.run(argv, timeout_s, env)
        Path(f"{stem}.stdout").write_bytes(r.stdout)
        Path(f"{stem}.stderr").write_bytes(r.stderr)
        self.event("command", label=label, argv=argv, exit_code=r.code, duration_s=round(r.duration_s, 3), stem=stem.name)
        return r

    def close(self) -> None:
        self.events.close()


def actuator_argv(ctx: Ctx, channel: str, command: list[str], out_dir: Path) -> list[str]:
    return [ctx.python, "-B", ctx.tool("e1_config_actuator.py"), "--network", str(ctx.network), "--channel", channel,
            "--orderer-admin-msp", str(ctx.orderer_admin_msp), "--out-dir", str(out_dir),
            "--command-timeout-s", str(ctx.actuator_command_timeout_s), "--commit-timeout-s", str(ctx.actuator_commit_timeout_s),
            "--cooldown-s", "20", "--min-b", "10", "--max-b", "40", *command]


def parse_inspect(stdout: bytes) -> FabricConfig:
    doc = json.loads(stdout.decode("utf-8"))
    return confirmed_config(doc["active_config"])


# ----------------------------------------------------------------------------------------------------- control loop
@dataclass
class LoopState:
    current: FabricConfig
    next_round: int = 0
    actuation_enabled: bool = True
    decisions: list = field(default_factory=list)
    transitions: list = field(default_factory=list)
    observations: list = field(default_factory=list)
    telemetry_errors: list = field(default_factory=list)


class ControlLoop:
    """Round-by-round decisions. `actuate(target_b, decision)` returns (success, record dict)."""

    def __init__(self, ctx: Ctx, arm: dict, policy, state: LoopState, world: World, rec: Recorder, actuate,
                 max_broadcasts: int | None = None):
        self.ctx, self.arm, self.policy, self.s, self.world, self.rec, self.actuate = ctx, arm, policy, state, world, rec, actuate
        self.rounds = arm["rounds"]
        self.n_decisions = len(arm["decision_times_utc"])
        self.max_broadcasts = max_broadcasts      # owner limit on update attempts per run (None = no limit, tests only)
        self.failure: str | None = None           # set when an actuation fails or the limit is reached: the run stops

    def done(self) -> bool:
        return self.s.next_round >= len(self.rounds)

    def step(self) -> bool:
        """Process the next finished round if its manifest exists. Returns True if a round was consumed."""
        if self.done():
            return False
        r = self.rounds[self.s.next_round]
        base = round_base(self.ctx.recorder_dir, self.ctx.run_id, r["index"], r["label"])
        if not manifest_ready(base):
            return False
        seen_ms = self.world.now_ms()
        obs = None
        try:
            obs, _ = read_round(base, r["index"], r["label"])
            self.s.observations.append({**obs.as_dict(), "seen_ms": seen_ms})
        except TelemetryError as exc:
            self.s.telemetry_errors.append({"round": r["index"], "error": str(exc)})
            self.rec.event("telemetry_error", round=r["index"], error=str(exc))
        d = r["index"]
        if d < self.n_decisions:
            age = None if obs is None else max(0.0, (seen_ms - obs.wall1_ms) / 1000)
            t_dec = self.world.now_ms()
            decision = self.policy.decide(d, obs, self.s.current, age)
            decision.update(decided_ms=t_dec, telemetry_round=r["index"], telemetry_age_s=age,
                            applies_to_round=d + 1, scheduled_utc=self.arm["decision_times_utc"][d],
                            applied=False, actuation=None)
            if decision["changed"]:
                if not self.s.actuation_enabled:
                    decision["actuation"] = "SKIPPED_ACTUATION_DISABLED"
                elif self.max_broadcasts is not None and len(self.s.transitions) >= self.max_broadcasts:
                    decision["actuation"] = "REFUSED_BROADCAST_LIMIT"
                    self.s.actuation_enabled = False
                    self.failure = f"decision {d} needs update {len(self.s.transitions) + 1}; max_broadcasts_per_run={self.max_broadcasts}"
                else:
                    # the decision is on disk before the actuator is called (a crash mid-update leaves this record)
                    self.rec.event("decision_before_actuation", **{k: v for k, v in decision.items() if k != "controller_metadata"})
                    ok, record = self.actuate(decision["target_B"], decision)
                    decision["actuation"] = record
                    self.s.transitions.append({"decision_index": d, "from_B": self.s.current.max_message_count,
                                               "to_B": decision["target_B"], "success": ok, **record})
                    if ok:
                        self.s.current = config_with_b(self.s.current, decision["target_B"])
                        self.policy.applied(self.s.current)
                        decision["applied"] = True
                    else:
                        self.s.actuation_enabled = False
                        self.failure = f"actuation for decision {d} (B={decision['target_B']}) failed: {record.get('error')}"
            self.s.decisions.append(decision)
            self.rec.event("decision", **{k: v for k, v in decision.items() if k != "controller_metadata"})
        self.s.next_round += 1
        return True


def make_actuate(ctx: Ctx, channel: str, rec: Recorder, world: World):
    def actuate(target_b: int, decision: dict) -> tuple[bool, dict]:
        out_dir = ctx.rd / "actuator"
        t0 = world.now_ms()
        r = rec.run(f"actuate_d{decision['decision_index']:02d}_B{target_b}",
                    actuator_argv(ctx, channel, ["--broadcast-authorization", str(ctx.authorization), "apply", "--target-b",
                                                 str(target_b), "--approved-target", str(target_b), "--execute"], out_dir),
                    timeout_s=ctx.actuator_command_timeout_s * 6 + ctx.actuator_commit_timeout_s + 60)
        t1 = world.now_ms()
        info = {"requested_ms": t0, "returned_ms": t1, "actuator_exit": r.code, "duration_ms": t1 - t0}
        try:
            urec = json.loads(r.stdout.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            urec = None
        if r.code != 0 or not isinstance(urec, dict):
            info["error"] = (r.stderr.decode("utf-8", "replace")[-600:]) or "no update record"
            return False, info
        ok = urec.get("success") is True and bool((urec.get("verification_checks") or {}).get("independent_verification"))
        info.update({k: urec.get(k) for k in ("attempt_id", "before_block_number", "after_block_number", "after_config",
                                               "submission_start_timestamp_utc", "submission_completion_timestamp_utc",
                                               "config_block_commit_timestamp_utc", "independent_verification_timestamp_utc",
                                               "commit_latency_ms", "total_adaptation_latency_ms", "artifacts_dir", "error")})
        if ok and int((urec.get("after_config") or {}).get("max_message_count", -1)) != target_b:
            ok, info["error"] = False, "update record after_config differs from the target"
        return ok, info
    return actuate


# ----------------------------------------------------------------------------------------------------- the run
def load_arm(ctx: Ctx) -> tuple[dict, str]:
    from .stage_verify import verify_stage_tree
    stage_sha = verify_stage_tree(ctx.stage)
    arm = json.loads((ctx.stage / ctx.run_id / "arm.json").read_text(encoding="utf-8"))
    if arm["run_id"] != ctx.run_id:
        raise RunRefused("arm.json names another run")
    return arm, stage_sha


def health(rec: Recorder, baseline: dict | None = None) -> dict:
    r = rec.world.run(["docker", "inspect", "--format", "{{.Name}} {{.State.Running}} {{.RestartCount}} {{.State.StartedAt}}",
                       *ORDERERS, *PEERS, CALIPER], 30)
    state = {}
    for line in r.stdout.decode("utf-8", "replace").splitlines():
        parts = line.strip().lstrip("/").split()
        if len(parts) == 4:
            state[parts[0]] = {"running": parts[1] == "true", "restarts": parts[2], "started": parts[3]}
    problems = [n for n in (*ORDERERS, *PEERS, CALIPER) if not state.get(n, {}).get("running")]
    if baseline:
        problems += [n for n, v in baseline.items() if state.get(n, {}).get("started") != v["started"]]
    return {"exit": r.code, "state": state, "problems": problems}


def execute(ctx: Ctx, world: World, *, now: dt.datetime | None = None) -> dict:
    arm, stage_sha = load_arm(ctx)
    try:
        auth = ExperimentAuthorization.load(ctx.authorization, now=now)
        auth.require_run(ctx.run_id, stage_sha, now=now)
    except AuthorizationError as exc:
        raise RunRefused(f"authorization: {exc}") from exc
    channel = arm["channel"]
    try:
        cs.select_experiment_channel(channel, purpose="a closed-loop run", option="arm.json channel")
    except cs.ChannelSelectionError as exc:
        raise RunRefused(str(exc)) from exc
    if arm["arm"] != "static" and channel not in auth.broadcast.channels:
        raise RunRefused(f"the authorization does not allow broadcasts on {channel}; a {arm['arm']} run may need one")
    if ctx.rd.exists() or ctx.run_ws.exists():
        raise RunRefused("run directory or Caliper run workspace already exists (runs are never repeated)")
    ctx.rd.mkdir(parents=True)
    rec = Recorder(ctx.rd, world)
    result = {"tool": PACKAGE_VERSION, "run_id": ctx.run_id, "channel": channel, "arm": arm["arm_label"], "stage_sha256": stage_sha,
              "authorization_sha256": auth.sha256, "started_utc": utc_now(), "verdict": None, "step_failure": None}
    caliper = None
    try:
        # preflight
        h0 = health(rec)
        mem, disk = world.mem_available_mb(), world.disk_free_gb(ctx.out_root)
        result["preflight"] = {"health": h0, "wsl_mem_available_mb": mem, "disk_free_gb": disk}
        if h0["problems"]:
            raise StepFailed("preflight", f"containers not running: {h0['problems']}")
        if mem < MIN_WSL_AVAIL_MB or disk < MIN_DISK_FREE_GB:
            raise StepFailed("preflight", f"memory {mem:.0f} MB or disk {disk:.1f} GB below floor")
        # fresh channel at the initial configuration
        r = rec.run("channel", [ctx.python, "-B", ctx.tool("e1_channel_run.py"), "--network", str(ctx.network), "--channel", channel,
                                "--run-id", ctx.run_id, "--max-message-count", str(arm["initial_B"]), "--batch-timeout",
                                arm["batch_timeout"], "--preferred-max-bytes", str(arm["preferred_max_bytes"]), "--run-dir",
                                str(ctx.rd / "channel"), "--execute"], 900)
        crec = json.loads((ctx.rd / "channel" / "channel_run_record.json").read_text()) if (ctx.rd / "channel" / "channel_run_record.json").exists() else {}
        if r.code != 0 or crec.get("status") != "VERIFIED":
            raise StepFailed("channel", f"exit {r.code}, status {crec.get('status')}")
        r = rec.run("inspect_initial", actuator_argv(ctx, channel, ["inspect"], ctx.rd / "actuator"), 300)
        if r.code != 0:
            raise StepFailed("inspect_initial", f"exit {r.code}")
        initial = parse_inspect(r.stdout)
        if initial.max_message_count != arm["initial_B"]:
            raise StepFailed("inspect_initial", f"channel has B={initial.max_message_count}, expected {arm['initial_B']}")
        result["initial_config"] = initial.as_dict()
        # workspace
        shutil.copytree(ctx.stage / ctx.run_id / "workspace", ctx.run_ws)
        policy = build_policy(arm, initial)
        state = LoopState(current=initial)
        loop = ControlLoop(ctx, arm, policy, state, world, rec, make_actuate(ctx, channel, rec, world),
                           max_broadcasts=auth.max_broadcasts_per_run)
        # Caliper + control loop
        h1 = health(rec)
        baseline = {n: v for n, v in h1["state"].items()}
        rec.event("caliper_start", argv=arm["caliper_argv"])
        t0 = world.monotonic()
        caliper = world.spawn(arm["caliper_argv"], ctx.rd / "caliper.stdout", ctx.rd / "caliper.stderr")
        next_health, aborted = t0 + ctx.health_every_s, None
        while True:
            consumed = loop.step()
            if loop.failure:                     # a failed or refused update stops the workload (no further rounds)
                aborted = loop.failure
                break
            code = caliper.poll()
            if code is not None:
                break
            now_m = world.monotonic()
            if now_m - t0 > arm["caliper_hard_timeout_s"]:
                aborted = f"hard timeout {arm['caliper_hard_timeout_s']} s"
                break
            if now_m >= next_health:
                next_health = now_m + ctx.health_every_s
                h = health(rec, baseline)
                if h["problems"]:
                    aborted = f"container problem: {h['problems']}"
                    break
            if not consumed:
                world.sleep(ctx.poll_s)
        if aborted:
            caliper.kill()
            k = rec.run("abort_pkill_caliper_in_container", ["docker", "exec", CALIPER, "pkill", "-f", "caliper launch"], 60)
            if k.code != 0:      # P1b's proven stop (stops the container; it must be started again before the next run)
                rec.run("abort_docker_kill_caliper", ["docker", "kill", CALIPER], 60)
            result["control"] = {"decisions": state.decisions, "transitions": state.transitions, "observations": state.observations,
                                 "telemetry_errors": state.telemetry_errors, "rounds_consumed": state.next_round,
                                 "final_controller_config": state.current.as_dict()}
            if loop.failure:     # read-only record of the channel after a failed update (evidence only; nothing is retried)
                rec.run("inspect_after_failed_actuation", actuator_argv(ctx, channel, ["inspect"], ctx.rd / "actuator"), 300)
                raise StepFailed("actuation", aborted)
            raise StepFailed("caliper", aborted)
        result["caliper"] = {"exit_code": code, "duration_s": round(world.monotonic() - t0, 3)}
        for _ in range(50):                      # rounds that finished just before Caliper exited
            if not loop.step():
                break
        result["control"] = {"decisions": state.decisions, "transitions": state.transitions, "observations": state.observations,
                             "telemetry_errors": state.telemetry_errors, "rounds_consumed": state.next_round,
                             "final_controller_config": state.current.as_dict()}
        if loop.failure:                         # an update that failed while the last rounds were drained
            raise StepFailed("actuation", loop.failure)
        if code != 0:
            raise StepFailed("caliper", f"exit {code}")
        if not loop.done():
            raise StepFailed("recorder", f"only {state.next_round} of {len(arm['rounds'])} round manifests appeared")
        r = rec.run("inspect_final", actuator_argv(ctx, channel, ["inspect"], ctx.rd / "actuator"), 300)
        if r.code != 0:
            raise StepFailed("inspect_final", f"exit {r.code}")
        final = parse_inspect(r.stdout)
        result["final_config"] = final.as_dict()
        r = rec.run("block_fetch", [ctx.python, "-B", ctx.tool("e1_block_fetch.py"), "fetch", "--network", str(ctx.network),
                                    "--channel", channel, "--first", "0", "--max-message-count", str(arm["initial_B"]),
                                    "--preferred-max-bytes", str(arm["preferred_max_bytes"]), "--out-dir", str(ctx.rd / "ledger"),
                                    "--execute"], 3600)
        if r.code != 0:
            raise StepFailed("block_fetch", f"exit {r.code}")
        from .analyze import analyze_run, decode_config_blocks, lifecycle_tx_ids
        cfg_blocks = decode_config_blocks(ctx.rd / "ledger", channel, lambda argv: rec.run("decode_config_block", argv, 120),
                                          configtxlator=str(Path(DEFAULTS["fabric_bin"]) / "configtxlator"))
        write_json_x(ctx.rd / "config_blocks.json", cfg_blocks)
        analysis = analyze_run(arm, ctx.run_ws, ctx.rd / "ledger", cfg_blocks, result["control"],
                               setup_tx=lifecycle_tx_ids(ctx.rd / "channel"))
        write_json_x(ctx.rd / "analysis.json", analysis)
        result["analysis_verdict"] = analysis["verdict"]
        if final.max_message_count != state.current.max_message_count:
            raise StepFailed("final_config", f"channel ends at B={final.max_message_count}, controller believes {state.current.max_message_count}")
        result["verdict"] = "INVALID_ACTUATION" if any(not t["success"] for t in state.transitions) else analysis["verdict"]
    except StepFailed as exc:
        result["step_failure"] = {"step": exc.step, "detail": exc.detail}
        result["verdict"] = result["verdict"] or "FAILED_" + exc.step.upper()
        rec.event("step_failed", step=exc.step, detail=exc.detail)
    finally:
        if caliper is not None and caliper.poll() is None:
            caliper.kill()
        result["finished_utc"] = utc_now()
        rec.close()
        write_json_x(ctx.rd / "run_result.json", result)
    return result


def listing(ctx: Ctx, arm: dict) -> list[str]:
    ch = arm["channel"]
    return [f"preflight: docker inspect {' '.join((*ORDERERS, *PEERS, CALIPER))}; MemAvailable >= {MIN_WSL_AVAIL_MB} MB; disk >= {MIN_DISK_FREE_GB} GB",
            f"channel: e1_channel_run.py --channel {ch} --max-message-count {arm['initial_B']} --batch-timeout {arm['batch_timeout']} --execute",
            f"inspect: e1_config_actuator.py --channel {ch} inspect",
            f"copy {ctx.stage / ctx.run_id / 'workspace'} -> {ctx.run_ws}",
            "caliper: " + " ".join(arm["caliper_argv"]),
            f"control loop: {len(arm['decision_times_utc'])} decisions ({arm['arm_label']}); a change calls "
            f"e1_config_actuator.py apply --target-b <B> --execute --broadcast-authorization {ctx.authorization}",
            f"inspect (final); e1_block_fetch.py fetch --channel {ch} --first 0; decode config blocks; analyze"]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Closed-loop live run (refuses without --execute and the owner's authorization).")
    ap.add_argument("--stage", required=True, type=Path)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--authorization", required=True, type=Path)
    ap.add_argument("--out-root", required=True, type=Path)
    ap.add_argument("--network", type=Path, default=DEFAULTS["network"])
    ap.add_argument("--caliper-ws", type=Path, default=DEFAULTS["caliper_ws"])
    ap.add_argument("--orderer-admin-msp", type=Path, default=DEFAULTS["orderer_admin_msp"])
    ap.add_argument("--execute", action="store_true")
    a = ap.parse_args(argv)
    ctx = Ctx(a.stage, a.run_id, a.authorization, a.out_root, a.network, a.caliper_ws, a.orderer_admin_msp)
    try:
        arm, stage_sha = load_arm(ctx)
        auth = ExperimentAuthorization.load(ctx.authorization)
        auth.require_run(ctx.run_id, stage_sha)
    except (RunRefused, AuthorizationError, OSError, ValueError) as exc:
        print(f"REFUSED: {exc}")
        return 2
    if not a.execute:
        print("DRY LISTING (nothing executed):")
        for line in listing(ctx, arm):
            print("  " + line)
        return 0
    if sys.platform != "linux":
        print("REFUSED: the live runner runs inside WSL")
        return 2
    env = {**os.environ, "PATH": DEFAULTS["fabric_bin"] + ":" + os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"}
    try:
        result = execute(ctx, World(env))
    except RunRefused as exc:
        print(f"REFUSED: {exc}")
        return 2
    print(json.dumps({k: result.get(k) for k in ("run_id", "arm", "verdict", "step_failure")}, indent=2))
    return 0 if result["verdict"] in ("VALID", "VALID_WITH_FLAGS") else 3


if __name__ == "__main__":
    sys.exit(main())
