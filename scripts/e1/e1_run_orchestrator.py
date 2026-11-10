"""Run orchestrator (LA-3(a)/(c); E1 v0.2 protocol section 9 per-run steps; section 13 gate file).

Status: written 2026-11-09, tested end to end against fake Docker/peer/HTTP endpoints, a fake channel
step and the real recorder driven by tests/e1/js/fake_caliper.js. Never run against Fabric or
Caliper. --execute needs the owner's Fabric/Caliper/Docker execution approval.

One run, in protocol order (every step is recorded; a failed step never stops the record):
   1 clock handshake (start)            e1_clock_handshake      clock/start/
   2 channel per run (B, tau, P)        e1_channel_run          channel/        abort if not VERIFIED
   3 metrics scrape (before)            e1_metrics_scraper      metrics/before/
   4 start metrics (during) + resource sampler, separate processes   metrics/during/, resources/
   5 Caliper (plan argv: warm-up round, settle, measured round; recorder writes the per-tx files)
   6 stop the background samplers; metrics scrape (after); summary   metrics/after/, metrics/summary/
   7 block fetch 0..height-1            e1_block_fetch          ledger/
   8 log capture + parse                e1_log_collector        logs/raw/, logs/parsed/
   9 clock handshake (end) + drift      e1_clock_handshake      clock/end/, clock/drift.json
  10 reconciliation                     e1_reconcile            reconcile/expect.json, reconcile/gate.json
  11 gates.json (before any outcome metric; this tool computes none), then run_manifest.json last

Symbols (one table: e1_telemetry_common.SYMBOLS): B = MaxMessageCount (count), tau = BatchTimeout
(Go duration), P = PreferredMaxBytes (bytes), A = AbsoluteMaxBytes (bytes). The plan's cell must
use exactly these keys.

Gate file: every gate is PASS, FAIL or NOT EVALUATED, with its rule and source. Gates the protocol
assigns to the analysis step (V2 pacing, V5 count-cut margin, V6 host memory) are listed as NOT
EVALUATED with evaluated_by = analysis. orchestrator_verdict = FAIL if any orchestrator gate FAILS,
else PASS; run validity also needs the analysis gates (protocol section 13.4).

Usage
  python scripts/e1/e1_run_orchestrator.py --plan run_plan.json --run-dir runs/r001 [--execute]
Dry run (default): validates the plan and prints the steps; writes nothing.
Standard library only.
"""
from __future__ import annotations

import argparse
import json
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_block_fetch as bf  # noqa: E402
import e1_channel_run as cr  # noqa: E402
import e1_clock_handshake as ch  # noqa: E402
import e1_log_collector as lc  # noqa: E402
import e1_metrics_scraper as ms  # noqa: E402
import e1_reconcile as rc  # noqa: E402
import e1_resource_sampler as rs  # noqa: E402
import e1_telemetry_common as tc  # noqa: E402

TOOL_VERSION = "e1_run_orchestrator 0.1 (2026-11-09)"
HERE = Path(__file__).resolve().parent
TOOLS = ["e1_run_orchestrator.py", "e1_telemetry_common.py", "e1_channel_run.py", "e1_channel_config.py",
         "e1_clock_handshake.py", "e1_metrics_scraper.py", "e1_resource_sampler.py", "e1_block_fetch.py",
         "e1_ledger_blocks.py", "e1_log_collector.py", "e1_reconcile.py", "caliper/e1_per_tx_recorder.js"]
V7_MAX_MS = 2.0              # protocol section 13 V7: |delta(Date.now) - delta(hrtime)| <= 2 ms over the round
CELL_KEYS = {"B", "tau", "P", "A", "n", "mode", "rate_tps", "duration_s"}
PLAN_KEYS = {"run_id", "channel", "cell", "network", "containers", "metrics_targets", "caliper", "reconcile_peer",
             "expect", "clock", "sampling", "log_margin_s"}
EXPECT_KEYS = {"event_strategy", "invoke_timeout_s", "binding", "expect_all_success", "expect_block_pattern"}
PASS, FAIL, NE = rc.PASS, rc.FAIL, rc.NOT_EVALUATED


class PlanError(ValueError):
    pass


# ------------------------------------------------------------------ plan
@dataclass
class Plan:
    raw: dict
    request: cr.Request = field(init=False)
    network: cr.Network = field(init=False)

    def __post_init__(self) -> None:
        r = self.raw
        missing, extra = PLAN_KEYS - set(r), set(r) - PLAN_KEYS
        if missing or extra:
            raise PlanError(f"plan keys: missing {sorted(missing)}, unknown {sorted(extra)}")
        cell = r["cell"]
        if set(cell) - CELL_KEYS or not {"B", "tau", "P", "n"} <= set(cell):
            raise PlanError(f"cell keys must be among {sorted(CELL_KEYS)} and include B, tau, P, n; got {sorted(cell)}")
        if set(r["expect"]) - EXPECT_KEYS:
            raise PlanError(f"expect keys must be among {sorted(EXPECT_KEYS)}")
        mode = cell.get("mode", "txNumber")
        if mode == "txDuration":
            if not isinstance(cell.get("duration_s"), int) or isinstance(cell["duration_s"], bool) \
                    or cell["duration_s"] < 1:
                raise PlanError("txDuration cells require a positive integer duration_s")
            if cell["n"] != 0:
                raise PlanError("txDuration cells must use n=0; the measured count is observed after the run")
        elif mode == "txNumber":
            if not isinstance(cell["n"], int) or isinstance(cell["n"], bool) or cell["n"] < 1:
                raise PlanError("cell n must be a positive integer")
        else:
            raise PlanError(f"unsupported cell mode {mode!r}")
        try:
            self.request = cr.Request(r["channel"], r["run_id"], cell["B"], cell["tau"], cell["P"], cell.get("A"))
            net_raw = r["network"] if isinstance(r["network"], dict) else \
                json.loads(Path(r["network"]).read_text(encoding="utf-8"))
            self.network = cr.Network(net_raw)
        except cr.RequestError as err:
            raise PlanError(str(err)) from err
        if self.network.raw.get("bootstrap"):
            raise PlanError("network description is a bootstrap one (placeholder package id): render it with the "
                            "installed package id first")
        c = r["containers"]
        names = list(c.get("orderers", [])) + list(c.get("peers", [])) + [c.get("caliper", "")]
        if not c.get("orderers") or not c.get("peers") or not all(lc._CONTAINER.match(n or "") for n in names) \
                or len(set(names)) != len(names):
            raise PlanError("containers need orderers, peers and caliper with distinct valid names")
        if r["reconcile_peer"] not in c["peers"]:
            raise PlanError("reconcile_peer must be one of containers.peers")
        ms.parse_targets(r["metrics_targets"])
        cal = r["caliper"]
        for k in ("argv", "recorder_dir", "measured_label"):
            if not cal.get(k):
                raise PlanError(f"caliper.{k} is required")
        for k in ("recorder_dir", "recorder_dir_in_container"):
            v = cal.get(k)
            if v is not None and not (Path(v).is_absolute() or str(v).startswith("/")):
                raise PlanError(f"caliper.{k} must be absolute (the recorder requires it)")
        if cal.get("container") not in (None, c["caliper"]):
            raise PlanError("caliper.container must be containers.caliper (the clock checks exec into it)")

    @property
    def containers(self) -> list[str]:
        c = self.raw["containers"]
        return list(c["orderers"]) + list(c["peers"]) + [c["caliper"]]

    def recorder_base(self, round_key: str, label_key: str) -> Path | None:
        cal = self.raw["caliper"]
        label = cal.get(label_key)
        if not label:
            return None
        rnd = cal.get(round_key, 1 if round_key == "measured_round" else 0)
        return Path(cal["recorder_dir"]) / f"{self.raw['run_id']}_w{cal.get('worker', 0)}_r{rnd}_{label}"


# ------------------------------------------------------------------ backend (the only place that touches the world)
@dataclass
class Backend:
    runner: tc.Runner                                   # docker exec/stats/inspect/logs and the Caliper argv
    net_runner: Callable[[list[str], dict], tuple]      # peer CLI with the org environment
    fetch: ms.Fetch                                     # HTTP GET of /metrics
    channel_setup: Callable[[cr.Request, cr.Network, Path], dict]
    start_background: Callable[[str, Path, dict], object]
    stop_background: Callable[[object], dict]
    wall_ns: Callable[[], int] = time.time_ns
    mono_ns: Callable[[], int] = time.monotonic_ns
    sleep: Callable[[float], None] = time.sleep
    local_identity: Callable[[], dict] = ch.local_identity


class SubprocessBackground:
    """Runs the during-phase metrics scraper and the resource sampler as separate processes, stopped by a
    stop file (protocol: the sampler is a separate process)."""

    def __init__(self, control_dir: Path):
        self.control = control_dir

    def start(self, kind: str, out_dir: Path, params: dict) -> dict:
        self.control.mkdir(parents=True, exist_ok=True)
        stop = self.control / f"stop_{kind}"
        if kind == "metrics":
            argv = [sys.executable, "-B", str(HERE / "e1_metrics_scraper.py"), "scrape", "--phase", "during",
                    "--out-dir", str(out_dir), "--interval", str(params["interval_s"]), "--stop-file", str(stop),
                    "--execute"] + [a for t in params["targets"] for a in ("--target", t)]
            if params.get("family_filter"):
                argv += ["--family-filter", params["family_filter"]]
        else:
            argv = [sys.executable, "-B", str(HERE / "e1_resource_sampler.py"), "sample", "--out-dir", str(out_dir),
                    "--interval", str(params["interval_s"]), "--stop-file", str(stop), "--execute"] + \
                [a for c in params["containers"] for a in ("--container", c)]
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return {"kind": kind, "proc": proc, "stop": stop, "argv": argv}

    def stop(self, h: dict) -> dict:
        h["stop"].write_text("stop\n", encoding="utf-8")
        try:
            out, err = h["proc"].communicate(timeout=120)
        except subprocess.TimeoutExpired:
            h["proc"].kill()
            out, err = h["proc"].communicate()
        (self.control / f"{h['kind']}.stdout").write_bytes(out)
        (self.control / f"{h['kind']}.stderr").write_bytes(err)
        return {"argv": h["argv"], "exit_code": h["proc"].returncode}


def real_backend(run_dir: Path) -> Backend:
    bg = SubprocessBackground(run_dir / "control")
    return Backend(runner=tc.subprocess_runner, net_runner=bf._net_subprocess_runner, fetch=ms.http_fetch,
                   channel_setup=lambda req, net, d: cr.run(req, net, d, cr.subprocess_runner),
                   start_background=bg.start, stop_background=bg.stop)


# ------------------------------------------------------------------ helpers
def _git_base(repo: Path) -> str:
    try:
        head = (repo / ".git" / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref: "):
            ref = head[5:]
            p = repo / ".git" / ref
            if p.exists():
                return p.read_text(encoding="utf-8").strip()
            for line in (repo / ".git" / "packed-refs").read_text(encoding="utf-8").splitlines():
                if line.endswith(" " + ref):
                    return line.split()[0]
        return head
    except OSError:
        return "unknown"


def _status_of(path: Path, key: str = "status") -> str | None:
    try:
        return json.loads(path.read_text(encoding="utf-8")).get(key)
    except (OSError, json.JSONDecodeError):
        return None


def _gate(gates: list, gate: str, status: str, rule: str, detail: str = "", source: str = "",
          evaluated_by: str = "orchestrator") -> None:
    gates.append({"gate": gate, "status": status, "rule": rule, "detail": detail, "source": source,
                  "evaluated_by": evaluated_by})


# ------------------------------------------------------------------ run
def run(plan: Plan, run_dir: Path, be: Backend, *, argv: list[str] | None = None) -> dict:
    """Execute one run. Writes gates.json and run_manifest.json in every case except a refused run dir."""
    if run_dir.exists():
        raise PlanError(f"run dir {run_dir} exists; refusing to reuse or overwrite")
    r, req, net = plan.raw, plan.request, plan.network
    run_dir.mkdir(parents=True)
    plan_bytes = json.dumps(r, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    tc.write_bytes_x(run_dir / "run_plan.json", plan_bytes)
    steps: list[dict] = []
    st: dict = {"abort": None, "error": None}
    clk = r["clock"]
    smp = r["sampling"]
    interval = float(smp.get("interval_s", 1.0))

    def step(name: str, fn: Callable[[], dict | None], *, always: bool = False) -> dict | None:
        rec = {"step": name, "start_utc": tc.utc_iso(be.wall_ns())}
        m0 = be.mono_ns()
        if st["abort"] and not always:
            rec.update(status="SKIPPED", detail=f"skipped: {st['abort']}", duration_s=0.0)
            steps.append(rec)
            return None
        try:
            out = fn() or {}
            rec.update(status=out.get("status", "DONE"), detail=out.get("detail", ""))
        except Exception as exc:  # recorded; re-raised after the gate file and manifest are written
            rec.update(status="ERROR", detail=repr(exc))
            st["error"] = st["error"] or exc
            out = None
        rec["end_utc"] = tc.utc_iso(be.wall_ns())
        rec["duration_s"] = round((be.mono_ns() - m0) / 1e9, 6)
        steps.append(rec)
        return out

    res: dict = {}
    t_run0 = be.wall_ns()

    def clock(phase):
        def go():
            # identity first: it decides the E0-0 clock domain; the handshake only aligns host-side telemetry
            ident = ch.run_identity(plan.containers, run_dir / "clock" / f"identity_{phase}", be.runner,
                                    local=be.local_identity, label=phase, argv=argv)
            res.setdefault("clock_identity", []).append(ident)
            h = ch.run_handshake(plan.containers, phase, run_dir / "clock" / phase, be.runner,
                                 rounds=int(clk.get("rounds", 5)), probes=clk.get("probes"), wall_ns=be.wall_ns,
                                 mono_ns=be.mono_ns, sleep=be.sleep, protocol_bound_ms=float(clk.get(
                                     "protocol_bound_ms", 1.0)), argv=argv)
            res[f"clock_{phase}"] = h
            return {"status": h["status"], "detail": f"identity {ident['verdict']}"}
        return go

    step("clock_start", clock("start"), always=True)

    def channel():
        rec = be.channel_setup(req, net, run_dir / "channel")
        res["channel"] = rec
        if rec.get("status") != cr.VERIFIED:
            st["abort"] = f"channel status {rec.get('status')}"
        return {"status": rec.get("status"), "detail": rec.get("detail", "")}

    step("channel", channel)

    def setup_height():
        # Channel-per-run commits the chaincode lifecycle transactions (approve, commit) in the first blocks.
        # The round's ledger is read from the height reached after setup (protocol section 9 step 6: from the
        # measured round's first block; warm-up blocks after it are declared via other_round_tx_ids).
        code, out, err = be.net_runner(bf.getinfo_argv(net, r["channel"]), net.peer_env(net.orgs()[0]))
        info = bf.parse_getinfo((out + b"\n" + err).decode("utf-8", "replace")) if code == 0 else None
        if info is None:
            st["abort"] = "channel height after setup unknown"
            return {"status": "FAILED", "detail": f"getinfo exit {code}"}
        res["setup_height"] = int(info["height"])
        return {"status": "OK", "detail": f"height {res['setup_height']} after setup (blocks 0..{res['setup_height'] - 1})"}

    step("setup_height", setup_height)
    targets = ms.parse_targets(r["metrics_targets"])

    def scrape(phase):
        def go():
            m = ms.run_scrape(targets, phase, run_dir / "metrics" / phase, fetch=be.fetch, wall_ns=be.wall_ns,
                              mono_ns=be.mono_ns, sleep=be.sleep, family_filter=smp.get("metrics_family_filter"),
                              argv=argv)
            return {"status": m["status"]}
        return go

    step("metrics_before", scrape("before"))
    handles = {}

    def start_bg():
        handles["metrics"] = be.start_background("metrics", run_dir / "metrics" / "during",
                                                 {"interval_s": interval, "targets": r["metrics_targets"],
                                                  "family_filter": smp.get("metrics_family_filter")})
        handles["resources"] = be.start_background("resources", run_dir / "resources",
                                                   {"interval_s": interval, "containers": plan.containers})
        return {"status": "STARTED"}

    step("start_background", start_bg)

    def caliper():
        argv_c = list(r["caliper"]["argv"])
        t0 = be.wall_ns()
        try:
            code, out, err = be.runner(argv_c)
        except (OSError, subprocess.SubprocessError) as exc:
            code, out, err = None, b"", repr(exc).encode()
        t1 = be.wall_ns()
        (run_dir / "caliper").mkdir()
        tc.write_bytes_x(run_dir / "caliper" / "caliper.stdout.log", out)
        tc.write_bytes_x(run_dir / "caliper" / "caliper.stderr.log", err)
        res["caliper"] = {"argv": argv_c, "exit_code": code, "start_ns": t0, "end_ns": t1,
                          "unexpected_error_lines": sum(1 for line in (out + b"\n" + err).decode("utf-8", "replace")
                                                        .splitlines() if "Unexpected error" in line)}
        return {"status": "EXIT_0" if code == 0 else f"EXIT_{code}"}

    step("caliper", caliper)

    def stop_bg():
        out = {}
        for k, h in handles.items():
            out[k] = be.stop_background(h)
        res["background"] = out
        return {"status": "STOPPED", "detail": json.dumps({k: v.get("exit_code") for k, v in out.items()})}

    step("stop_background", stop_bg, always=bool(handles))
    step("metrics_after", scrape("after"))

    def summarize():
        d = run_dir / "metrics"
        # expect_traffic (E0-0 remediation F3a): every orchestrated run submits N >= 1 transactions on its own
        # channel, so a receiving orderer and a block-cutting orderer must be identifiable from the deltas.
        s = ms.summarize(d / "before", d / "after", d / "summary", during=d / "during" if (d / "during").exists()
                         else None, channel=r["channel"], fresh_channel=True, expect_traffic=True, argv=argv)
        res["metrics_summary"] = s
        return {"status": s["status"]}

    step("metrics_summary", summarize)

    def blocks():
        m = bf.run_fetch(net, r["channel"], res["setup_height"], None, run_dir / "ledger", req.max_message_count,
                         req.preferred_max_bytes, be.net_runner, sleep=be.sleep, wall_ns=be.wall_ns, argv=argv)
        res["ledger"] = m
        return {"status": m["status"]}

    step("block_fetch", blocks)

    def logs():
        margin = int(float(r.get("log_margin_s", 5)) * 1e9)
        since = tc.utc_iso(t_run0 - margin)
        until = tc.utc_iso(res.get("caliper", {}).get("end_ns", be.wall_ns()) + margin)
        names = list(r["containers"]["orderers"]) + list(r["containers"]["peers"])
        cap = lc.run_capture(names, since, until, run_dir / "logs" / "raw", be.runner)
        # Fabric logs to stderr (UNVERIFIED until E0-0). Every non-empty stream is parsed; if both are empty,
        # stderr is passed anyway so that the log collector reports input_empty (never silently skipped).
        inputs, used = [], {}
        for role, group in (("orderer", r["containers"]["orderers"]), ("peer", r["containers"]["peers"])):
            for c in group:
                files = {s: run_dir / "logs" / "raw" / f"{c}.{s}.log" for s in ("stderr", "stdout")}
                chosen = [s for s, p in files.items() if p.exists() and p.stat().st_size > 0] or ["stderr"]
                used[c] = chosen
                inputs += [lc.LogInput(c, role, files[s]) for s in chosen]
        blocks_csv = None
        if res.get("ledger", {}).get("status") in tc.USABLE:
            blocks_csv = [b for b in tc.read_csv(run_dir / "ledger" / "ledger" / "blocks.csv")
                          if b["header_type"] == "ENDORSER_TRANSACTION"]
        m = lc.run_parse(inputs, r["channel"], run_dir / "logs" / "parsed", blocks_csv=blocks_csv,
                         reconcile_peer=r["reconcile_peer"], argv=argv)
        res["logs"] = {"capture": cap["status"], "parse": m["status"], "streams_parsed": used,
                       "expected_blocks": "ledger endorser blocks" if blocks_csv is not None else "observed range"}
        return {"status": m["status"] if cap["status"] == "OK" else f"CAPTURE_{cap['status']}"}

    step("logs", logs)
    step("clock_end", clock("end"), always=True)

    def drift():
        if "clock_start" not in res or "clock_end" not in res:
            return {"status": "NOT_RUN", "detail": "a handshake is missing"}
        d = ch.drift(res["clock_start"], res["clock_end"], run_dir / "clock" / "drift.json",
                     tolerance_ms=float(clk.get("drift_tolerance_ms", 2.0)))
        res["drift"] = d
        return {"status": d["status"]}

    step("clock_drift", drift, always=True)

    def reconcile():
        led = res.get("ledger", {})
        if led.get("status") not in tc.USABLE:
            return {"status": "NOT_RUN", "detail": f"ledger status {led.get('status')}"}
        base = plan.recorder_base("measured_round", "measured_label")
        records = rc.read_csv(base.with_name(base.name + ".csv"))
        sub = base.with_name(base.name + ".submits.csv")
        submits = rc.read_csv(sub) if sub.exists() else None
        manifest = json.loads(base.with_name(base.name + ".manifest.json").read_text(encoding="utf-8"))
        res["recorder_manifest"] = manifest
        others: set[str] = set()
        wbase = plan.recorder_base("warmup_round", "warmup_label")
        if wbase is not None:
            others = {x["tx_id"] for x in rc.read_csv(wbase.with_name(wbase.name + ".csv")) if x["tx_id"]}
        bounds = [ch.pair_bound(res[k]["containers"], r["containers"]["caliper"], r["reconcile_peer"],
                                float(clk.get("protocol_bound_ms", 1.0))) for k in ("clock_start", "clock_end")
                  if k in res]
        hs_bound = max((b["bound_ms"] for b in bounds if b), default=None)
        proto = float(clk.get("protocol_bound_ms", 1.0))
        same = ch.domain_gate(res.get("clock_identity", []), None)[0] == PASS
        # The V4 bound is the protocol's 1 ms (stamp truncation) and is never widened to the round-trip bound.
        source = ("protocol bound; one kernel clock demonstrated by identity (offset 0; 1 ms covers truncation)"
                  if same else "protocol bound; one kernel clock NOT demonstrated (gate CLOCK-domain fails)")
        res["clock_bound"] = {"handshake_pairs": bounds, "handshake_bound_ms": hs_bound, "v4_bound_ms": proto,
                              "v4_bound_source": source,
                              "handshake_note": "host<->container round-trip bound; aligns host telemetry only"}
        exp_raw = {"n": r["cell"]["n"], "max_message_count": req.max_message_count,
                   "preferred_max_bytes": req.preferred_max_bytes, "clock_offset_bound_ms": proto,
                   **{k: v for k, v in r["expect"].items()}}
        (run_dir / "reconcile").mkdir()
        tc.write_json_x(run_dir / "reconcile" / "expect.json",
                        {**exp_raw, "other_round_tx_ids_count": len(others), "clock_offset_bound_source": source})
        exp = rc.Expectations(other_round_tx_ids=others, **exp_raw)
        peer = None
        pc = run_dir / "logs" / "parsed" / "peer_commit_reconcile.csv"
        if pc.exists():
            peer = rc.read_csv(pc)
        ledger = rc.read_csv(run_dir / "ledger" / "ledger" / "ledger_tx.csv")
        blocks_rows = rc.read_csv(run_dir / "ledger" / "ledger" / "blocks.csv")
        checks = rc.reconcile(records, submits, manifest, ledger, exp, peer, blocks_rows)
        verdict = rc.verdict(checks)
        tc.write_json_x(run_dir / "reconcile" / "gate.json", {"verdict": verdict,
                                                              "checks": [c.__dict__ for c in checks]})
        res["reconcile"] = {"verdict": verdict, "checks": checks, "records": records}
        return {"status": verdict}

    step("reconcile", reconcile)
    res["abort"] = st["abort"]
    res["step_problems"] = [f"{s['step']}={s['status']}" for s in steps if s["status"] in ("ERROR", "FAILED", "NOT_RUN")]
    gates = build_gates(plan, run_dir, res)
    verdict = FAIL if any(g["status"] == FAIL and g["evaluated_by"] == "orchestrator" for g in gates) else PASS
    gate_doc = {"run_id": r["run_id"], "orchestrator_verdict": verdict, "gates": gates,
                "not_evaluated": [g["gate"] for g in gates if g["status"] == NE],
                "outcome_metrics_computed": False,
                "note": "gates computed before any outcome metric (protocol section 13); run validity also needs "
                        "the analysis gates (evaluated_by = analysis)"}
    tc.write_json_x(run_dir / "gates.json", gate_doc)
    manifest = build_manifest(plan, run_dir, res, steps, gate_doc, argv)
    tc.write_json_x(run_dir / "run_manifest.json", manifest)
    if st["error"] is not None:
        raise st["error"]
    return manifest


# ------------------------------------------------------------------ gates
def build_gates(plan: Plan, run_dir: Path, res: dict) -> list[dict]:
    g: list[dict] = []
    # A run that measured nothing must never PASS by having only NOT EVALUATED gates (E0-0, 2026-11-09).
    missing = [k for k in ("caliper", "reconcile") if res.get(k) is None]
    problems = ([f"aborted: {res['abort']}"] if res.get("abort") else []) + res.get("step_problems", []) + \
        [f"{k} not run" for k in missing]
    _gate(g, "RUN-complete", FAIL if problems else PASS,
          "every step ran without error, Caliper ran and the reconciliation ran", "; ".join(problems) or "complete")
    chan = res.get("channel")
    _gate(g, "SETUP-channel", PASS if chan and chan.get("status") == cr.VERIFIED else FAIL,
          "channel status VERIFIED: the configuration in force equals the cell (protocol 9 step 1; V4)",
          f"status {chan.get('status') if chan else 'not run'}", "channel/channel_run_record.json")
    cal = res.get("caliper")
    if cal is None:
        _gate(g, "SETUP-caliper-exit", NE, "Caliper exited 0", "Caliper not run")
        _gate(g, "V1-worker-log", NE, "no 'Unexpected error' line in the worker output (protocol 13 V1)", "not run")
    else:
        _gate(g, "SETUP-caliper-exit", PASS if cal["exit_code"] == 0 else FAIL, "Caliper exited 0",
              f"exit {cal['exit_code']}", "caliper/")
        _gate(g, "V1-worker-log", PASS if cal["unexpected_error_lines"] == 0 else FAIL,
              "no 'Unexpected error' line in the worker output (protocol 13 V1)",
              f"{cal['unexpected_error_lines']} line(s)", "caliper/")
    rec = res.get("reconcile")
    if rec is None:
        _gate(g, "RECONCILE", NE, "e1_reconcile checks V1/V3/V4/V7-config/E0-1", "reconciliation not run "
              "(ledger unusable or an earlier step failed)")
    else:
        for c in rec["checks"]:
            _gate(g, c.check_id, c.status, "e1_reconcile.py (spec file 03 section 8)", c.detail, "reconcile/gate.json")
        failed = sum(1 for x in rec["records"] if x["status"] != "success")
        if plan.raw["expect"].get("expect_all_success", True):
            _gate(g, "V3-no-failures", PASS if failed == 0 else FAIL, "S_fail = 0 in fixed-count arms (protocol 13 V3)",
                  f"failed records {failed}", "recorder CSV")
    rm = res.get("recorder_manifest")
    wm = (rm or {}).get("clock", {}).get("wall_minus_monotonic_ms")
    if wm is None:
        _gate(g, "V7-clock", NE, f"|wall - monotonic| <= {V7_MAX_MS} ms over the round (protocol 13 V7)",
              "recorder manifest clock not available")
    else:
        _gate(g, "V7-clock", PASS if abs(float(wm)) <= V7_MAX_MS else FAIL,
              f"|wall - monotonic| <= {V7_MAX_MS} ms over the round (protocol 13 V7)", f"{wm} ms", "recorder manifest")
    tele = {"TEL-ledger": (res.get("ledger") or {}).get("status"),
            "TEL-logs": (res.get("logs") or {}).get("parse") if (res.get("logs") or {}).get("capture") == "OK"
            else None,
            "TEL-metrics": (res.get("metrics_summary") or {}).get("status"),
            "TEL-resources": _status_of(run_dir / "resources" / "resource_manifest.json"),
            "TEL-clock": tc.worst([x for x in ((res.get("clock_start") or {}).get("status"),
                                               (res.get("clock_end") or {}).get("status"),
                                               (res.get("drift") or {}).get("status")) if x])
            if all(k in res for k in ("clock_start", "clock_end", "drift")) else None}
    for name, status in tele.items():
        _gate(g, name, NE if status is None else (PASS if status in tc.USABLE else FAIL),
              "tool status COMPLETE or COMPLETE_WITH_WARNINGS", f"status {status}")
    rows = []
    for name in ("orderer_write.csv", "peer_commit.csv"):
        p = run_dir / "logs" / "parsed" / name
        if p.exists():
            rows += tc.read_csv(p)
    lag = ch.log_lag(rows) if rows else None
    res["log_lag"] = lag
    status, detail = ch.domain_gate(res.get("clock_identity", []), lag)
    _gate(g, "CLOCK-domain", status if status != "NOT EVALUATED" else NE,
          "Caliper and Fabric containers share one kernel clock, offset <= 1 ms (E0-0): identity (one boot_id) at "
          "start and end, and log-lag falsification (docker_ns >= fabric time for every event)", detail, "clock/")
    ms_sum = res.get("metrics_summary")
    orderers = plan.raw["containers"]["orderers"]
    if ms_sum is None:
        _gate(g, "V6-leader-changes", NE, "no Raft leader change during the run (protocol 13 V6)", "no metrics summary")
    else:
        deltas = {t: d.get("leader_changes_delta") for t, d in ms_sum["derived"].items() if d.get("role") == "orderer"}
        if not deltas or any(v is None for v in deltas.values()):
            _gate(g, "V6-leader-changes", NE, "no Raft leader change during the run (protocol 13 V6)",
                  f"leader_changes_delta unavailable: {deltas}")
        else:
            _gate(g, "V6-leader-changes", PASS if all(v == 0 for v in deltas.values()) else FAIL,
                  "no Raft leader change during the run (protocol 13 V6)", f"{deltas}; orderers {orderers}",
                  "metrics/summary/")
    rman = run_dir / "resources" / "resource_manifest.json"
    if not rman.exists():
        _gate(g, "V6-containers", NE, "no container restart, replacement, OOM kill or exit (protocol 13 V6)",
              "no resource sampler manifest")
    else:
        rm_doc = json.loads(rman.read_text(encoding="utf-8"))
        bad = {k: v for k, v in rm_doc["anomalies"]["error"].items()
               if k in ("container_restarted", "container_replaced", "oom_killed", "container_not_running")}
        snaps = rm_doc.get("snapshots", {})
        if snaps.get("start") is None or snaps.get("end") is None:
            _gate(g, "V6-containers", FAIL if bad else NE,
                  "no container restart, replacement, OOM kill or exit (protocol 13 V6)",
                  f"inspect snapshot missing; anomalies {bad}")
        else:
            _gate(g, "V6-containers", PASS if not bad else FAIL,
                  "no container restart, replacement, OOM kill or exit (protocol 13 V6)", f"{bad}", "resources/")
    fill = {t: d.get("fill_duration_mean_s") for t, d in (ms_sum or {}).get("derived", {}).items()
            if d.get("role") == "orderer"}
    _gate(g, "V2-pacing", NE, "|A/lambda - 1| <= 0.02; p99 lateness <= 5 ms; event-loop p99 <= 20 ms (protocol 13 V2)",
          f"event_loop_delay {((rm or {}).get('event_loop_delay'))}", "recorder", evaluated_by="analysis")
    _gate(g, "V5-count-cut-margin", NE, "run mean fill duration of count-cut blocks <= 0.5 tau (protocol 13 V5)",
          f"pooled fill_duration_mean_s (all cut causes) {fill}; tau {plan.request.tau_ms} ms", "metrics/summary/",
          evaluated_by="analysis")
    _gate(g, "V6-host-memory", NE, "host memory headroom >= 1 GB; no throttling flag (protocol 13 V6)",
          "host counters are not sampled by the orchestrator (e1_resource_sampler typeperf parses a separate log)",
          evaluated_by="analysis")
    return g


# ------------------------------------------------------------------ manifest
def build_manifest(plan: Plan, run_dir: Path, res: dict, steps: list[dict], gate_doc: dict,
                   argv: list[str] | None) -> dict:
    repo = HERE.parents[1]
    artifacts = []
    for p in sorted(run_dir.rglob("*")):
        if p.is_file() and p.name != "run_manifest.json":
            data = p.read_bytes()
            artifacts.append({"path": p.relative_to(run_dir).as_posix(), "bytes": len(data),
                              "sha256": tc.sha256_bytes(data)})
    rm = res.get("recorder_manifest") or {}
    cal = res.get("caliper") or {}
    req = plan.request
    return {
        "tool": TOOL_VERSION, "argv": argv, "run_id": plan.raw["run_id"], "channel": plan.raw["channel"],
        "cell": {"B": req.max_message_count, "tau": cr.canonical_tau(req.tau_ms), "tau_ms": req.tau_ms,
                 "P": req.preferred_max_bytes, "A": req.absolute_max_bytes, "n": plan.raw["cell"]["n"],
                 "mode": plan.raw["cell"].get("mode"), "rate_tps": plan.raw["cell"].get("rate_tps")},
        "symbols": {k: {"field": v[0], "unit": v[1]} for k, v in tc.SYMBOLS.items()},
        "plan_sha256": tc.sha256_file(run_dir / "run_plan.json"),
        "tools": {t: tc.sha256_file(HERE / t) for t in TOOLS},
        "environment": {"python": sys.version, "platform": platform.platform(), "git_base_head": _git_base(repo),
                        "git_note": "base commit only; the tools may be uncommitted (tools sha256 above)"},
        "steps": steps,
        "caliper": {k: cal.get(k) for k in ("argv", "exit_code", "unexpected_error_lines")} |
                   {"start_utc": tc.utc_iso(cal.get("start_ns")), "end_utc": tc.utc_iso(cal.get("end_ns"))},
        "recorder": {"config_in_force": rm.get("config_in_force"), "versions": rm.get("versions"),
                     "clock": rm.get("clock"), "event_loop_delay": rm.get("event_loop_delay"),
                     "counts": rm.get("counts"), "files": rm.get("files")},
        "event_semantics_and_timeout_note": "LA-3(c): taken from the recorder manifest (config_in_force); "
                                            "V7-config compares them with the plan's expect values",
        "clock_bound": res.get("clock_bound"),
        "ledger_range": {"first_block": res.get("setup_height"),
                         "note": "blocks 0..setup_height-1 hold the channel genesis and the chaincode lifecycle "
                                 "transactions of the channel-per-run setup; they are outside the round"},
        "clock_identity": [{k: i.get(k) for k in ("label", "verdict", "boot_ids", "host_in_container_clock_domain")}
                           for i in res.get("clock_identity", [])],
        "log_lag": res.get("log_lag"),
        "statuses": {"channel": (res.get("channel") or {}).get("status"),
                     "ledger": (res.get("ledger") or {}).get("status"), "logs": res.get("logs"),
                     "metrics_summary": (res.get("metrics_summary") or {}).get("status"),
                     "resources": _status_of(run_dir / "resources" / "resource_manifest.json"),
                     "clock_start": (res.get("clock_start") or {}).get("status"),
                     "clock_end": (res.get("clock_end") or {}).get("status"),
                     "drift": (res.get("drift") or {}).get("status"),
                     "reconcile": (res.get("reconcile") or {}).get("verdict"),
                     "background": res.get("background")},
        "gates": {"file": "gates.json", "sha256": tc.sha256_file(run_dir / "gates.json"),
                  "orchestrator_verdict": gate_doc["orchestrator_verdict"]},
        "artifacts": artifacts,
    }


# ------------------------------------------------------------------ CLI
def main(argv: list[str] | None = None, backend: Backend | None = None, platform_name: str = sys.platform) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--plan", required=True, type=Path)
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--execute", action="store_true", help="run the steps (needs Fabric/Caliper/Docker authorization)")
    args = ap.parse_args(argv)
    try:
        plan = Plan(json.loads(args.plan.read_text(encoding="utf-8")))
    except (OSError, ValueError, KeyError, TypeError) as err:
        print(f"INVALID PLAN: {err}")
        return 2
    if args.run_dir.exists():
        print(f"refusing to overwrite {args.run_dir}")
        return 2
    if args.execute:
        # Execution architecture (2026-11-09): every tool, the Fabric CLIs and the docker CLI run inside the
        # Ubuntu 24.04 WSL2 distro, which shares the container kernel clock. Windows paths cannot reach them.
        if not platform_name.startswith("linux"):
            print(f"REFUSED: --execute runs only inside the Ubuntu WSL distro (platform here: {platform_name})")
            return 2
        bad = tc.windows_style_paths({"plan": plan.raw, "network": plan.network.raw})
        if bad:
            print(f"REFUSED: Windows-style paths in the plan or network description (use wslpath -u): {bad[:5]}")
            return 2
    if not args.execute:
        req = plan.request
        print(f"cell: B={req.max_message_count} tau={cr.canonical_tau(req.tau_ms)} P={req.preferred_max_bytes} bytes "
              f"n={plan.raw['cell']['n']} channel={req.channel}")
        for s in ("clock_start", "channel", "metrics_before", "start_background", "caliper", "stop_background",
                  "metrics_after", "metrics_summary", "block_fetch", "logs", "clock_end", "clock_drift", "reconcile",
                  "gates.json", "run_manifest.json"):
            print("PLAN", s)
        print("PLAN caliper argv: " + " ".join(plan.raw["caliper"]["argv"]))
        print("dry run: nothing executed or written (add --execute once Fabric/Caliper/Docker use is authorized)")
        return 0
    m = run(plan, args.run_dir, backend or real_backend(args.run_dir), argv=sys.argv if argv is None else argv)
    for s in m["steps"]:
        print(f"  {s['step']:<18} {s['status']:<24} {s.get('detail', '')[:80]}")
    print(f"RUN {m['gates']['orchestrator_verdict']} -> {args.run_dir}")
    return 0 if m["gates"]["orchestrator_verdict"] == PASS else 1


if __name__ == "__main__":
    sys.exit(main())
