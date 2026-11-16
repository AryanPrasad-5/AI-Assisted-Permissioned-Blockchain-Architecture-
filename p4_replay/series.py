"""Series-level orchestration of the replay (Phase 2). OFFLINE_TESTED against a fake backend only; every live step is LIVE_UNTESTED and unreachable.

What it adds on top of the Phase 1 per-run executor (`driver.execute_replay_run`, reused, not edited):
  refusal while any owner decision is open; refusal of a real backend before any call (so no helper, probe or file is started);
  process-scoped keep-awake and the continuous clock probe, both cleaned up on success and on failure; the replay preflight (S1-S11 adapted, R1-R5);
  a per-run precheck; the seeded run order; between-run validation (clock record over the run window, channel and configuration verification);
  replay-specific classification; stop-on-failure under the owner-approved stop conditions; final reconciliation; a series summary.

The executor's two hard-wired values are overridden for the duration of the series only (executor_overrides, restored in a `finally`): the host-sampler guard (the approved
one, not the Phase 1 constant) and the fidelity thresholds (the approved ones, so fidelity.json is not left holding the Phase 1 placeholders).

Nothing here changes a frozen file. `driver.LIVE_EXECUTION_ENABLED` stays False: with it False a real p1b.Backend, and any backend that does not declare OFFLINE_FAKE, is refused at the top of run_replay_series.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import json
import math
import sys
import traceback
import hashlib
from pathlib import Path

from . import PACKAGE_VERSION, ensure_paths
from . import caliper_check as cc
from . import classify as cl
from . import driver as dr
from . import fidelity as fd
from . import guard
from . import keepawake as ka
from . import offsets as of
from . import preflight as pf
from . import series_approval as sa
from . import snapshot as sn

ensure_paths()
sys.dont_write_bytecode = True

TOOL_VERSION = f"p4_replay.series {PACKAGE_VERSION}"
LIVE_STATUS = "LIVE_UNTESTED"


# ----------------------------------------------------------------------------------------------- executor overrides (contained, restored)
@contextlib.contextmanager
def executor_overrides(host_max_seconds: float, fidelity_thresholds: fd.FidelityThresholds):
    """For the duration of the series only: the executor's step list gets the approved host-sampler guard, and its fidelity evaluation the approved thresholds.
    The two module attributes are restored in a `finally`, whatever happens."""
    orig_steps, orig_eval = dr.run_steps, dr.evaluate_run_dir

    def steps(row, ctx, stage_dir, mean_rate, chaincode=None):
        out = orig_steps(row, ctx, stage_dir, mean_rate, chaincode)
        for st in out:
            if st["label"] == "samplers":
                host = st["argvs"]["host"]
                host[host.index("--max-seconds") + 1] = str(int(host_max_seconds))
        return out

    def evaluate(run_id, stage_run_dir, **kw):
        return orig_eval(run_id, stage_run_dir, **{**kw, "thresholds": fidelity_thresholds})

    dr.run_steps, dr.evaluate_run_dir = steps, evaluate
    try:
        yield
    finally:
        dr.run_steps, dr.evaluate_run_dir = orig_steps, orig_eval


# ----------------------------------------------------------------------------------------------- per-run precheck
def replay_precheck(s, row, stage_dir: Path, probe) -> list[tuple[str, bool, str]]:
    """The P1b per-run checks without the P1b matrix pin (which would fail every replay), plus the staged files of this run. Run before EVERY run."""
    p1b = dr.p1b_module()
    ctx, be, thr = s.ctx, s.be, s.thr
    out: list[tuple[str, bool, str]] = []
    in_scope = row.B in (10, 40) and row.tau == dr.TAU and row.P == dr.P_BYTES and row.schedule == "A" and dr.CHANNEL_RE.match(row.channel) is not None
    out.append(("run is inside the approved scope (B 10 or 40, tau 2s, P 10 MiB, schedule A, fresh channel name)", in_scope, f"B={row.B} {row.channel}"))
    sums = Path(str(stage_dir)) / "runs" / f"{row.run_id}.SHA256SUMS"
    bad, n = [], 0
    if sums.is_file():
        for line in sums.read_text(encoding="ascii").splitlines():
            digest, _, rel = line.partition("  ")
            n += 1
            target = Path(str(stage_dir)) / "runs" / row.run_id / rel
            if not target.is_file() or p1b.sha256_file(target) != digest:
                bad.append(rel)
    out.append(("staged files of this run equal their pinned hashes", bool(n) and not bad, f"{n} files, mismatched or missing {bad[:5]}"))
    h = p1b.health(be)
    out.append(("five /healthz == 200", all(v == 200 for v in h.values()), json.dumps(h)))
    ins = p1b.inspect_all(be, list(s.baseline))
    ev = p1b.container_events(ins, s.baseline) if ins else ["docker inspect failed"]
    out.append(("no container restarted, replaced or exited since session start", not ev, "; ".join(ev) or "unchanged"))
    free = be.disk_free_gb("/mnt/c")
    out.append((f"C: free >= {thr.disk_free_gb_hard:.0f} GB", free >= thr.disk_free_gb_hard, f"{free:.1f} GB"))
    wsl = be.wsl_mem_available_mb()
    out.append((f"WSL MemAvailable >= {thr.wsl_avail_mb_run:.0f} MB", wsl >= thr.wsl_avail_mb_run, f"{wsl:.0f} MB"))
    mem = be.host_memory(2, 1)
    wa = mem["available_mb"][-1] if mem else None
    out.append((f"Windows available memory >= {thr.win_avail_mb_run:.0f} MB", wa is not None and wa >= thr.win_avail_mb_run, f"{wa} MB"))
    pw = be.host_power(s.helper_pid)
    out.append(("AC power online", bool(pw) and pw.get("line") == "Online", json.dumps(pw)))
    out.append((f"keep-awake helper alive (pid {s.helper_pid})", bool(s.helper_pid) and bool(pw) and pw.get("helper_alive") is True, json.dumps(pw)))
    out.append(("continuous clock probe running", probe is not None and probe.poll() is None, f"exit {None if probe is None else probe.poll()}"))
    absent = not ctx.rd(row.run_id).exists() and not (ctx.caliper_ws / "runs" / row.run_id).exists()
    out.append(("run output folders absent", absent, row.run_id))
    return out


# ----------------------------------------------------------------------------------------------- between-run validation
def between_run_clock(s, row) -> dict:
    """The continuous clock record against THIS run's window. Waits the probe's flush margin first. Never raises: a failure is a CHECK_ERROR verdict, which is not PASS."""
    p1b = dr.p1b_module()
    ctx, be, thr = s.ctx, s.be, s.thr
    be.sleep(thr.clock_flush_margin_s)
    try:
        samples, torn = p1b.read_clock_samples(ctx.root / "clock_series" / "clock_samples.csv")
        check = p1b.clock_series_check(samples, p1b.run_windows(ctx, [row.run_id]), step_ms=thr.clock_step_ms, max_gap_ms=thr.clock_gap_max_ms)
        check["torn_rows_dropped"] = torn
    except Exception as exc:                                                              # noqa: BLE001
        check = {"verdict": "CHECK_ERROR", "error": repr(exc)}
    return check


def run_validation(ctx, row) -> dict:
    """Configuration verification AFTER the run, from the executor's own records: the channel was created VERIFIED with the requested configuration, and the active
    configuration was identical before and after the workload and equals the request. A failure makes the run INVALID_INFRA."""
    p1b = dr.p1b_module()
    rd = ctx.rd(row.run_id)
    crec = p1b.load_json(rd / "channel" / "channel_run_record.json") or {}
    cfg = p1b.load_json(rd / "config_check.json") or {}
    facts = p1b.load_json(rd / "facts.json") or {}
    checks = {"channel_VERIFIED": crec.get("status") == "VERIFIED", "config_unchanged_and_as_requested": cfg.get("unchanged") is True,
              "requested_equals_row": cfg.get("requested") == {"max_message_count": row.B, "batch_timeout_ms": 2000, "preferred_max_bytes": row.P},
              "no_step_failure": facts.get("step_failure") is None, "caliper_exit_0_no_abort": (facts.get("caliper") or {}).get("exit_code") == 0 and not facts.get("aborted")}
    return {"ok": all(checks.values()), "checks": checks}


# ----------------------------------------------------------------------------------------------- the stop policy (pure)
def decide_after_run(*, verdict: str, cls: str | None, is_dry_run: bool, consecutive_invalid: int, approval: sa.SeriesApproval, invalid_stop: int = 2) -> tuple[bool, str, int]:
    """(halt, reason, new consecutive-INVALID count) under STOP_CONDITIONS SC3-SC7. Pure."""
    p1b = dr.p1b_module()
    if verdict in p1b.HALT_VERDICTS:
        return True, f"SC4 {verdict}", 0
    if verdict.startswith("INVALID"):
        n = consecutive_invalid + 1
        if is_dry_run:
            return True, f"SC3 the dry run is {verdict}", n
        return (n >= invalid_stop), (f"SC5 {n} consecutive INVALID runs" if n >= invalid_stop else f"{verdict}: not yet at the limit ({n})"), n
    if verdict != "REPLAY_RECORDED" or cls is None:
        return True, f"SC4 unexpected verdict {verdict!r} with class {cls!r}", 0
    healthy = cls in (cl.HEALTHY, cl.HEALTHY_PROVISIONAL)
    if is_dry_run:
        return (not healthy), ("dry run healthy: measured runs may start" if healthy else f"SC3 the dry run is {cls}"), 0
    if healthy:
        return False, "healthy", 0
    if cls in cl.CONTINUABLE and approval.continue_after_limited:
        return False, f"SC7 {cls} but the owner pre-approved continuing", 0
    return True, (f"SC7 {cls}: halt (the owner did not pre-approve continuing)" if cls in cl.CONTINUABLE else f"SC6 {cls}"), 0


# ----------------------------------------------------------------------------------------------- reconciliation and a descriptive p95
def recorder_csv(recorder_dir: Path, run_id: str, name: str) -> Path:
    idx, label = (0, f"wu-{run_id}") if name == "pre_roll" else (1, f"m-{run_id}")
    return Path(recorder_dir) / f"{run_id}_w0_r{idx}_{label}.csv"


def run_p95_ms(recorder_rows: list[dict]) -> float | None:
    """Nearest-rank 95th percentile of (final - create) over the measured round's transactions. DESCRIPTIVE: not an LP test, not the analysis of record."""
    try:
        lat = sorted(float(r["final_ms"]) - float(r["create_ms"]) for r in recorder_rows if r.get("status") == "success")
    except (KeyError, ValueError, TypeError):
        return None
    return lat[max(0, math.ceil(0.95 * len(lat)) - 1)] if lat else None


def final_reconciliation(ctx, rows_run, stage_manifest: dict) -> dict:
    """Series-level accounting from the files: per run, scheduled == recorder == ledger endorser transactions (tx_id level); across the series no tx_id appears twice."""
    per_run: dict = {}
    seen: dict[str, str] = {}
    cross = 0
    for row in rows_run:
        rd = ctx.rd(row.run_id)
        ledger_path = rd / "ledger" / "ledger" / "ledger_tx.csv"
        entry: dict = {"scheduled": None, "recorder": None, "ledger_endorser": None, "ok": False}
        try:
            sched = stage_manifest["runs"][row.run_id]["tx_total"]
            ids: list[str] = []
            for name in ("pre_roll", "measured"):
                ids += [r["tx_id"] for r in fd.read_recorder_csv(recorder_csv(ctx.recorder_dir(row.run_id), row.run_id, name))]
            led = [r["tx_id"] for r in fd.read_ledger_tx_csv(ledger_path) if str(r["header_type"]) == fd.ENDORSER_HEADER_TYPE]
            for t in ids:
                if t in seen and seen[t] != row.run_id:
                    cross += 1
                seen.setdefault(t, row.run_id)
            entry.update(scheduled=sched, recorder=len(ids), ledger_endorser=len(led), ok=(sched == len(ids) == len(led) and sorted(ids) == sorted(led) and len(set(ids)) == len(ids)))
        except (OSError, KeyError, fd.EvidenceError) as exc:
            entry["error"] = repr(exc)
        per_run[row.run_id] = entry
    ok = bool(per_run) and all(e["ok"] for e in per_run.values()) and cross == 0
    return {"ok": ok, "per_run": per_run, "cross_run_repeated_tx_id": cross, "level": "tx_id (key-level reconciliation is NOT AVAILABLE)",
            "totals": {k: sum((e[k] or 0) for e in per_run.values()) for k in ("scheduled", "recorder", "ledger_endorser")}}


# ----------------------------------------------------------------------------------------------- the series
def probe_argv(ctx, series_s: int) -> list[str]:
    return [ctx.py, "-B", ctx.tool("e1_clock_rate_probe.py"), "collect", "--out-dir", str(ctx.root / "clock_series"), "--duration", str(series_s), "--ref-python", ctx.ref_python]


def evaluate_recorded_run(s, row, stage_dir, approval: sa.SeriesApproval, v: dict) -> dict:
    """Judge a run the executor recorded: configuration verification, the clock record over its window, the replay classification (written as classification.json) and the
    descriptive p95. Returns the fields to merge into the series entry. May raise: the caller turns any exception into DRIVER_ERROR."""
    p1b = dr.p1b_module()
    ctx = s.ctx
    validation = run_validation(ctx, row)
    clock = between_run_clock(s, row)
    cls = cl.classify_run_dir(row.run_id, Path(str(stage_dir)) / "runs" / row.run_id, dispatch_dir=Path(str(ctx.caliper_ws / "runs" / row.run_id)),
                              recorder_dir=Path(str(ctx.recorder_dir(row.run_id))), ledger_csv=Path(str(ctx.rd(row.run_id) / "ledger/ledger/ledger_tx.csv")),
                              resources_csv=Path(str(ctx.rd(row.run_id) / "resources/resource_samples.csv")), clock_gate_pass=True,
                              clock_series_verdict=clock.get("verdict"), thr=approval.thresholds)
    p1b.write_json_new(ctx.rd(row.run_id) / "classification.json", {**cls, "validation": validation, "clock_series": clock})
    try:
        measured_rows = fd.read_recorder_csv(recorder_csv(ctx.recorder_dir(row.run_id), row.run_id, "measured"))
    except fd.EvidenceError:
        measured_rows = []
    out = {"class": cls["class"], "labels": cls["labels"], "subtypes": cls["subtypes"], "validation_ok": validation["ok"], "clock_series_verdict": clock.get("verdict"),
           "fidelity_phase1_verdict": v.get("fidelity"), "p95_ms_descriptive": run_p95_ms(measured_rows)}
    if not validation["ok"]:
        out.update(verdict="INVALID_INFRA", verdict_executor=v["verdict"])
    return out


def run_replay_series(s, rows, stage_dir, approval: sa.SeriesApproval | None, *, own_pid: int, clock_dir: Path | None, env_doc: dict | None, preflight_dir: Path,
                      helpers: dict | None = None, s1_evidence_text: str | None = None, s1_allowed_text: str | bytes | None = None, snapshot_path: Path | None = None,
                      node: bool = True, series_root_prefix: str = dr.SERIES_ROOT_WSL) -> dict:
    """The whole series. Returns the summary dict (also written as series_summary.json once a series root exists). Never retries, never resumes."""
    p1b = dr.p1b_module()
    approval = sa.require_resolved(approval)
    guard.require_offline_backend(s.be)                     # a real or undeclared backend is refused HERE, before any helper, probe, folder or call
    ctx, be = s.ctx, s.be
    sn.assert_ctx_snapshot(ctx)
    s1_allowed = sn.bind_allowance(s1_allowed_text, route=approval.raw["s1_route"], evidence_sha256=approval.raw["s1_evidence_sha256"])      # before anything starts
    s.thr = dataclasses.replace(p1b.THR, caliper_timeout_s=approval.caliper_timeout_s)
    rows_run = [rows[0]] if approval.phase == "DRY_RUN_ONLY" else list(rows)
    series_s = cc.keep_awake_seconds(len(rows_run), approval.caliper_timeout_s)
    pid_file = Path(preflight_dir).parent / f"{Path(preflight_dir).name}_keep_awake.pid"
    summary: dict = {"tool": TOOL_VERSION, "live_status": LIVE_STATUS, "phase": approval.phase, "snapshot": sn.SNAPSHOT_ID, "status": None, "halted": None, "runs": []}
    keep = ka.KeepAwake(be, series_s, pid_file, p1b._wslpath_win(be, pid_file))
    try:
        with keep:
            return _run_inside_keep_awake(s, rows, rows_run, stage_dir, approval, keep, summary, own_pid=own_pid, clock_dir=clock_dir, env_doc=env_doc,
                                          preflight_dir=preflight_dir, helpers=helpers, s1_evidence_text=s1_evidence_text, s1_allowed=s1_allowed,
                                          snapshot_path=snapshot_path, node=node, series_s=series_s, series_root_prefix=series_root_prefix)
    finally:
        summary["keep_awake_cleanup"] = keep.cleanup_log
        if ctx.root.exists():
            p1b.write_json_new(ctx.root / "keep_awake_cleanup.json", {"pid": keep.pid, "log": keep.cleanup_log, "cleaned": keep.cleaned})


def _run_inside_keep_awake(s, rows, rows_run, stage_dir, approval, keep, summary, *, own_pid, clock_dir, env_doc, preflight_dir, helpers, s1_evidence_text, s1_allowed,
                           snapshot_path, node, series_s, series_root_prefix) -> dict:
    p1b = dr.p1b_module()
    ctx, be = s.ctx, s.be
    s.helper_pid = keep.pid
    results, report = pf.run_replay_preflight(ctx, be, rows, preflight_dir, approval=approval, stage_dir=stage_dir, clock_dir=clock_dir, helper_pid=keep.pid, env_doc=env_doc,
                                              own_pid=own_pid, s1_evidence_text=s1_evidence_text, s1_allowed=s1_allowed, snapshot_path=snapshot_path, node=node,
                                              series_root_prefix=series_root_prefix)
    if not report["all_pass"]:
        summary.update(status="PREFLIGHT_FAILED", failed_gates=[g.id for g in results if g.status != "PASS"], started_runs=[])
        return summary
    ctx.root.parent.mkdir(parents=True, exist_ok=True)
    ctx.root.mkdir(parents=False, exist_ok=False)
    s.log_path = ctx.root / "series_events.log"
    s.baseline, s.chaincode = report["context"]["baseline"], report["context"]["chaincode"]
    stage_manifest = json.loads((Path(str(stage_dir)) / "series_manifest.json").read_text(encoding="utf-8"))
    s.log(f"series start ({approval.phase}); approval by owner; snapshot {sn.SNAPSHOT_ID}; thresholds {approval.thresholds.status}")
    p1b.write_json_new(ctx.root / "session_manifest.json", {
        "tool": TOOL_VERSION, "live_status": LIVE_STATUS, "started_utc": be.now_utc(), "boot_id": be.boot_id(), "snapshot": sn.SNAPSHOT_ID, "phase": approval.phase,
        "approval": approval.raw, "thresholds": approval.thresholds.as_dict(), "run_ids": [r.run_id for r in rows_run], "keep_awake_pid": keep.pid, "keep_awake_seconds": series_s,
        "keep_awake_mechanism": "PowerCreateRequest helper (keepawake.KEEP_AWAKE_REQUEST_PS), LIVE_UNTESTED", "caliper_timeout_s": approval.caliper_timeout_s,
        "host_max_seconds": approval.host_max_seconds, "stop_conditions": list(sa.STOP_CONDITIONS), "decision_register": sa.decision_register(approval),
        "preflight_report": str(Path(preflight_dir) / "preflight_report.json")})
    probe = be.spawn(probe_argv(ctx, series_s), p1b.tool_env(ctx), ctx.root / "clock_series.stdout", ctx.root / "clock_series.stderr")
    s.probe = probe
    halted, consecutive = None, 0
    try:
        be.sleep(3.0)
        if probe.poll() is not None:
            raise p1b.HaltSeries(f"the continuous clock probe exited at once (exit {probe.poll()}): no run was started")
        with executor_overrides(approval.host_max_seconds, approval.thresholds.fidelity):
            for row in rows_run:
                is_dry = row.kind == "dry_run"
                checks = replay_precheck(s, row, stage_dir, probe)
                for name, ok, detail in checks:
                    s.log(f"{row.run_id}: precheck {'PASS' if ok else 'FAIL'} {name}: {detail}")
                if not all(ok for _, ok, _ in checks):
                    raise p1b.HaltSeries(f"{row.run_id}: pre-run check failed: " + "; ".join(n for n, ok, _ in checks if not ok))
                s.log(f"{row.run_id}: START {row.channel} B={row.B} window={row.window} ({'dry run' if is_dry else 'measured'}, seq {row.seq})")
                mean_rate = stage_manifest["runs"][row.run_id]["schedules"]["measured"]["n_tx"] / float(of.MEASURED_S)
                try:
                    v = dr.execute_replay_run(s, row, stage_dir, mean_rate, helpers=helpers, clock_gate_pass=True)
                except Exception as exc:                                                  # noqa: BLE001  (LiveExecutionRefused is a SystemExit and passes through)
                    v = {"run_id": row.run_id, "channel": row.channel, "verdict": "DRIVER_ERROR", "error": repr(exc), "traceback": traceback.format_exc()}
                entry = {"run_id": row.run_id, "kind": row.kind, "window": row.window, "B": row.B, "replicate": row.replicate, "verdict": v["verdict"], "class": None}
                if v["verdict"] == "REPLAY_RECORDED":
                    try:
                        entry.update(evaluate_recorded_run(s, row, stage_dir, approval, v))
                    except Exception as exc:                                                  # noqa: BLE001  fail closed: a crash while judging a run is a DRIVER_ERROR, never a pass
                        entry.update(verdict="DRIVER_ERROR", verdict_executor=v["verdict"], error=repr(exc), traceback=traceback.format_exc())
                else:
                    entry["step_failure"] = v.get("step_failure")
                    if v.get("error"):
                        entry["error"], entry["traceback"] = v["error"], v.get("traceback")      # a DRIVER_ERROR must never lose its cause
                halt, reason, consecutive = decide_after_run(verdict=entry["verdict"], cls=entry["class"], is_dry_run=is_dry, consecutive_invalid=consecutive, approval=approval,
                                                             invalid_stop=s.thr.consecutive_invalid_stop)
                entry["decision"] = reason
                s.verdicts[row.run_id] = entry
                summary["runs"].append(entry)
                if ctx.rd(row.run_id).exists():
                    p1b.write_json_new(ctx.rd(row.run_id) / "series_verdict.json", entry)
                s.log(f"{row.run_id}: END verdict {entry['verdict']} class {entry['class']}: {reason}")
                if halt:
                    raise p1b.HaltSeries(f"{row.run_id}: {reason}")
    except p1b.HaltSeries as exc:
        halted = str(exc)
        s.log(f"SERIES HALTED: {halted}")
    finally:
        diffs: list = []
        try:
            fp_post = p1b.fingerprint(ctx, be, p1b.load_network(ctx), ctx.root / "fingerprint_post")
            diffs = p1b.compare_fingerprints(report["context"]["fingerprint"] or {}, fp_post)
            p1b.write_json_new(ctx.root / "fingerprint_compare.json", {"post": fp_post, "differences": diffs})
        except Exception as exc:                                                      # noqa: BLE001 - the probe and the helper must still be stopped
            diffs = [f"end-of-series fingerprint failed: {exc!r}"]
        clock_final = p1b.finish_clock_series(s, probe, [r.run_id for r in rows_run])
        recon = final_reconciliation(ctx, [r for r in rows_run if r.run_id in s.verdicts and s.verdicts[r.run_id]["verdict"] == "REPLAY_RECORDED"], stage_manifest)
        limited = [e["run_id"] for e in summary["runs"] if e.get("class") in cl.CONTINUABLE]
        invalid = [e["run_id"] for e in summary["runs"] if str(e["verdict"]).startswith("INVALID")]
        status = ("HALTED" if halted else "FAILED_INTEGRITY" if (diffs or not recon["ok"]) else "CLOCK_RECORD_NOT_PASS" if clock_final.get("verdict") != "PASS"
                  else "COMPLETE_WITH_INVALID_RUNS" if invalid else "COMPLETE_WITH_LIMITED_RUNS" if limited else "COMPLETE")
        summary.update(status=status, halted=halted, fingerprint_differences=diffs, reconciliation=recon, limited_runs=limited, invalid_runs=invalid,
                       clock_series_check={k: clock_final.get(k) for k in ("verdict", "samples", "steps_total", "steps_in_run_windows", "error")},
                       claims={"live_scheduling_fidelity_validated": False, "law_predictions_evaluated": False,
                               "note": "descriptive only: n = 3 per configuration supports no significance, controller, capacity or HFBTP-transfer claim"})
        p1b.write_json_new(ctx.root / "series_summary.json", summary)
    return summary


# ----------------------------------------------------------------------------------------------- CLI (offline only)
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="p4_replay.series", description="Series orchestration for the replay. Offline commands only; `run` is refused in this build.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    rd_ = sub.add_parser("readiness", help="list the owner decisions and whether an approval file resolves them (offline, read-only)")
    rd_.add_argument("--prereg", type=Path, help="the preregistration file the approval names (its SHA-256 is checked)")
    rd_.add_argument("--approval", type=Path)
    rd_.add_argument("--stage-root", type=Path)
    si = sub.add_parser("snapshot-identity", help="read-only identity check of the pinned S1 snapshot and the p4_replay drift against it")
    si.add_argument("--snapshot", type=Path)
    sub.add_parser("run", help="REFUSED in this build")
    args = ap.parse_args(argv)
    try:
        if args.cmd == "run":
            raise dr.LiveExecutionRefused("refused: the live path is closed (driver.LIVE_EXECUTION_ENABLED is False). It needs the owner's approval file, a resolved S1 route and "
                                          "a separate authorisation that reviews and enables the live code.")
        if args.cmd == "snapshot-identity":
            ident = sn.verify_identity(args.snapshot)
            drift = sn.added_since_snapshot(args.snapshot)
            print(json.dumps({"identity_ok": ident["ok"], "checks": ident["checks"], "p4_replay_added": sorted(drift["added"]), "changed": drift["changed"], "removed": drift["removed"]}, indent=2))
            return 0 if ident["ok"] and not drift["changed"] and not drift["removed"] else 2
        approval = None
        if args.approval is not None:
            prereg_sha = hashlib.sha256(args.prereg.read_bytes()).hexdigest() if args.prereg else ""
            stage_sha = hashlib.sha256((args.stage_root / "SHA256SUMS.txt").read_bytes()).hexdigest() if args.stage_root else ""
            approval = sa.check_series_approval(args.approval.read_text(encoding="utf-8"), prereg_sha256=prereg_sha, stage_sums_sha256=stage_sha)
        reg = sa.decision_register(approval)
        print(json.dumps(reg, indent=2))
        unresolved = [d["id"] for d in reg if d["status"] == "OWNER_DECISION_REQUIRED" and d["id"].startswith("D")]
        print("UNRESOLVED:" if unresolved else "RESOLVED", unresolved)
        return 3 if unresolved else 0
    except SystemExit:
        raise
    except (OSError, ValueError, KeyError) as exc:
        print(f"INVALID: {exc!r}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
