"""Orchestration of ONE launch (a dry run or the measured series), Phase 2C. OFFLINE_TESTED against a fake backend only; every live step is LIVE_UNTESTED and unreachable.

A derived orchestrator, not an edit of `series.py` (Phase 2, untouched). It reuses what is sound by import (`series.replay_precheck`, `run_validation`, `decide_after_run`,
`final_reconciliation`, `executor_overrides`, `probe_argv`, `recorder_csv`, `run_p95_ms`; Phase 1 `driver.execute_replay_run`; the P1b gates) and replaces what the Phase 2C owner
decisions say is wrong or missing:

  Package A  one launch kind per call, disjoint identifiers (launch_ids), pins and the source manifest from outside the tree (pins), the entry authorization binding (launch_approval),
             a backend that must match the mode (OFFLINE_REHEARSAL: a declared fake only; LIVE: the exact p1b.Backend only), gate R6 (dry-run evidence), S1 against the pinned snapshot.
  Package B  keep-awake duration from the documented budget and an identity-checked cleanup (duration_model, keepawake_v2); evidence inputs for the primary analysis.
  Package C  median-referenced classification (replay_classify), clock evidence from the continuous probe record (clock_evidence), gate R4 by classification (gate_r4).

Gates: the sixteen Phase 2 gates, S10 and R1-R3 re-based on the external pins, plus R6. A gate that raises FAILS; there is no parameter that skips or subsets them; a launch cannot start
unless exactly GATE_ORDER_V2 all PASS. No gate threshold of P1b (S3's 1,500 MB / 85 % / 100 pages, "do not waive") is touched.

Stop policy (hash-bound in the approval, STOP_CONDITIONS_V2): fail closed; no retry, resume or replacement; failed-run evidence stays where it is; halt on a failed pre-flight, a clock,
network, configuration, integrity or reconciliation failure (each maps to a verdict or class that `decide_after_run` halts on; the table is tested). Limits that cannot be removed with
the current telemetry (no live backlog watchdog, no resource-exhaustion guard beyond the pre-run floors) are written into every summary under `limits`.
"""

from __future__ import annotations

import dataclasses
import json
import shutil
import traceback
from pathlib import Path, PurePosixPath

from . import PACKAGE_VERSION
from . import analysis_primary as an
from . import caliper_check as cc
from . import clock_evidence as ce
from . import driver as dr
from . import duration_model as dm
from . import fidelity as fd
from . import gate_r4 as g4
from . import guard
from . import keepawake_v2 as k2
from . import launch_approval as la
from . import launch_ids as li
from . import offsets as of
from . import pins as pn
from . import preflight as pf
from . import replay_classify as rc
from . import series as se
from . import snapshot as sn

TOOL_VERSION = f"p4_replay.launch_series {PACKAGE_VERSION}"
LIVE_STATUS = "LIVE_UNTESTED"
MODE_OFFLINE, MODE_LIVE = "OFFLINE_REHEARSAL", "LIVE"
GATE_ORDER_V2 = ("S3", "S2", "S4", "S5", "S6", "R1", "R2", "R3", "S7", "S8", "S9", "R4", "S10", "S11", "R5", "R6", "S1")
LIMITS = [
    "No live backlog watchdog: a run that saturates Fabric is seen only afterwards, through its drain time and in-flight figures (fabric_overload_indicators).",
    "Resource exhaustion (Windows or WSL memory, disk) is guarded by the pre-run floors only; nothing watches it during a run.",
    "A halted launch is never resumed and a failed measured run is never replaced: its identifiers, channels and folders are consumed.",
    "Live scheduling fidelity, the effect of the keep-awake helper and the real idle process list of the Caliper container are LIVE_UNTESTED.",
    "Key-level ledger reconciliation is not available; reconciliation is at tx_id level.",
]


# ----------------------------------------------------------------------------------------------- backend and mode
def require_backend_for_mode(be, mode: str) -> None:
    """OFFLINE_REHEARSAL accepts only a backend that declares OFFLINE_FAKE and is not a p1b.Backend. LIVE accepts only the EXACT p1b.Backend class (no subclass, no look-alike, no
    fake) and only while driver.LIVE_EXECUTION_ENABLED is True. Anything undeclared is refused before any call."""
    p1b = dr.p1b_module()
    fake = getattr(be, guard.MARKER, False) is True
    if mode == MODE_OFFLINE:
        if not fake or isinstance(be, p1b.Backend):
            raise dr.LiveExecutionRefused(f"refused: OFFLINE_REHEARSAL accepts only a declared offline fake that is not a p1b.Backend (type {type(be).__name__})")
    elif mode == MODE_LIVE:
        if fake:
            raise dr.LiveExecutionRefused("refused: a fake backend is never accepted in LIVE mode")
        if type(be) is not p1b.Backend:
            raise dr.LiveExecutionRefused(f"refused: LIVE mode accepts only the exact p1b.Backend class, not {type(be).__name__}")
        if not dr.LIVE_EXECUTION_ENABLED:
            raise dr.LiveExecutionRefused("refused: driver.LIVE_EXECUTION_ENABLED is False")
    else:
        raise ValueError(f"unknown mode {mode!r}")


# ----------------------------------------------------------------------------------------------- the gates that differ from Phase 2
def _res(gid: str, name: str, ok: bool, rule: str, detail: str, evidence: dict | None = None):
    return dr.p1b_module().GateResult(gid, name, "PASS" if ok else "FAIL", rule, detail, evidence or {})


def gate_r1_pins(pin: pn.SnapshotPin, snapshot_path: Path | None = None):
    rule = ("the snapshot folder is the pinned one: SHA256SUMS.txt and manifest hashes, git head, group count and required groups, finish time, manifest result PASS with 0 problem files "
            "(read-only; the per-file re-hash is gate S1)")
    ident = pn.verify_snapshot_identity(pin, snapshot_path)
    failed = sorted(k for k, v in ident["checks"].items() if not v)
    return _res("R1", "snapshot identity (external pin)", ident["ok"], rule, f"identity matches {pin.id}" if ident["ok"] else f"failed: {failed}", {"identity": ident})


def gate_r2_launch(ctx, rows, kind: str, launch_id: str, approval: la.LaunchApproval, pin: pn.SnapshotPin, env_doc, net_raw, stage_manifest, series_root_prefix: str):
    rule = ("launch rows are exactly the namespace of the launch kind; the Ctx snapshot is the pinned one; the series root is <prefix>/<launch_id>, new and outside the wsl_e1 group; "
            "chaincode as pinned; the approved Caliper timeout and host guard fit the staged schedules")
    problems: list[str] = []
    try:
        li.validate_launch_rows(kind, list(rows))
    except li.LaunchIdError as exc:
        problems.append(f"rows: {exc}")
    if approval.kind != kind:
        problems.append(f"approval is for {approval.kind}, launch is {kind}")
    if ctx.win_snapshot != pin.path_win:
        problems.append(f"Ctx.win_snapshot {ctx.win_snapshot!r} != pinned {pin.path_win!r}")
    root = str(PurePosixPath(str(ctx.root).replace("\\", "/")))
    if not root.startswith(series_root_prefix.rstrip("/") + "/") or PurePosixPath(root).name != launch_id:
        problems.append(f"series root {root} is not {series_root_prefix}/{launch_id}")
    if root == pf.WSL_E1_GROUP or root.startswith(pf.WSL_E1_GROUP + "/"):
        problems.append("series root is inside the wsl_e1 snapshot group")
    cc_name = None
    if env_doc is not None and net_raw is not None:
        import re as _re
        cc_name, net_cc = env_doc.get("chaincode", {}).get("name"), (net_raw.get("chaincode") or {})
        if cc_name != "e1asset" or net_cc.get("name") != cc_name or not _re.fullmatch(r"e1asset_[0-9.]+:[0-9a-f]{64}", str(net_cc.get("package_id", ""))):
            problems.append(f"chaincode mismatch: env {cc_name!r}, network {net_cc.get('name')!r}")
    else:
        problems.append("env and network descriptions were not provided")
    last_pre = last_meas = None
    if stage_manifest:
        pre = {r["schedules"]["pre_roll"]["last_offset_ms"] for r in stage_manifest["runs"].values()}
        meas = {r["schedules"]["measured"]["last_offset_ms"] for r in stage_manifest["runs"].values()}
        last_pre, last_meas = max(pre), max(meas)
        budget = cc.run_length_budget(last_pre, last_meas, approval.caliper_timeout_s, approval.host_max_seconds, n_runs=len(rows))
        if not budget["ok"]:
            problems.append(f"run-length budget: {budget['checks']}")
        if stage_manifest.get("launch_kind") != kind:
            problems.append(f"staged tree is for launch_kind {stage_manifest.get('launch_kind')!r}, not {kind}")
    else:
        problems.append("no staged series manifest")
    return _res("R2", "required configuration values", not problems, rule, "; ".join(problems) or "all required configuration values are as pinned",
                {"chaincode": cc_name, "last_pre_roll_offset_ms": last_pre, "last_measured_offset_ms": last_meas})


def gate_r3_launch(stage_dir: Path, auth: la.EntryAuthorization, rows, approval: la.LaunchApproval, *, node: bool = True):
    rule = ("the staged tree re-hashes to its SHA256SUMS, that file's hash equals the authorized stage_sums_sha256, the staged matrix equals this launch's matrix, every run folder passes the "
            "offline Caliper checks, and every window rebuilt to the P1-P5 schedule hash")
    stage = Path(stage_dir)
    problems: list[str] = []
    ver = dr.verify_stage(stage) if (stage / "SHA256SUMS.txt").is_file() else {"ok": False, "mismatched_or_missing": ["SHA256SUMS.txt"], "unlisted": []}
    if not ver["ok"]:
        problems.append(f"stage verify: mismatched/missing {ver['mismatched_or_missing'][:5]}, unlisted {ver['unlisted'][:5]}")
    sums = pf.sha256_file(stage / "SHA256SUMS.txt")
    if sums != auth.stage_sums_sha256:
        problems.append("SHA256SUMS.txt hash differs from the authorized stage_sums_sha256")
    staged_matrix = (stage / "replay_matrix.csv").read_bytes() if (stage / "replay_matrix.csv").is_file() else b""
    if staged_matrix != dr.matrix_csv(list(rows)):
        problems.append("replay_matrix.csv differs from this launch's matrix")
    manifest = json.loads((stage / "series_manifest.json").read_text(encoding="utf-8")) if (stage / "series_manifest.json").is_file() else {}
    if manifest.get("status") != "STAGED_OFFLINE_NOT_EXECUTED":
        problems.append(f"series_manifest status {manifest.get('status')!r}")
    for wid, lineage in (manifest.get("windows") or {}).items():
        if not lineage.get("equal_to_p1_p5_schedule"):
            problems.append(f"window {wid} is not equal to the P1-P5 schedule")
    per_run = {}
    for row in rows:
        run_dir = stage / "runs" / row.run_id
        if not run_dir.is_dir():
            problems.append(f"{row.run_id}: staged run folder missing")
            continue
        res = cc.validate_rendered_run(run_dir, caliper_timeout_s=approval.caliper_timeout_s, host_max_seconds=approval.host_max_seconds, use_node=node)
        per_run[row.run_id] = {"ok": res["ok"], "failed": res["failed"]}
        if not res["ok"]:
            problems.append(f"{row.run_id}: offline Caliper checks failed {res['failed']}")
    return _res("R3", "replay schedule integrity", not problems, rule, "; ".join(problems) or f"{len(rows)} staged runs verified; caliper_status {cc.CALIPER_STATUS}",
                {"stage_sums_sha256": sums, "per_run": per_run, "caliper_status": cc.CALIPER_STATUS})


def gate_s10_launch(ctx, out_dir: Path, source_manifest: dict[str, str]):
    rule = ("every reviewed source file equals the hash in the externally named source manifest, no file under p4_replay is unlisted, the manifest covers the whole reviewed set, "
            "and the eight frozen P1b files equal their pinned hashes")
    p1b = dr.p1b_module()
    repo = Path(ctx.repo)
    problems: list[str] = []
    ver = pn.verify_source_manifest(repo, source_manifest)
    if not ver["ok"]:
        problems.append(f"source manifest: mismatched {ver['mismatched'][:5]}, missing {ver['missing'][:5]}, unlisted {ver['unlisted'][:5]}")
    uncovered = sorted(set(pn.reviewed_source_paths(repo)) - set(source_manifest))
    if uncovered:
        problems.append(f"the manifest does not cover the reviewed set: {uncovered[:5]}")
    frozen = {p: pf.sha256_file(repo / p) for p in pf.FROZEN_P1B_SHA256}
    bad = sorted(p for p, h in frozen.items() if h != pf.FROZEN_P1B_SHA256[p])
    if bad:
        problems.append(f"frozen P1b files changed or missing: {bad}")
    p1b.write_json_new(Path(out_dir) / "tool_hashes.json", {"source_manifest_checked": ver, "frozen_p1b": frozen, "tool": TOOL_VERSION})
    return _res("S10", "tool and source pinning (external manifest)", not problems, rule, "; ".join(problems) or f"{len(source_manifest)} files equal the manifest; {len(frozen)} frozen P1b files unchanged", {"checked": ver["checked"]})


def _parse_utc(text: str):
    from datetime import datetime, timezone
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def gate_r6_dry_run_evidence(kind: str, auth: la.EntryAuthorization, pin: pn.SnapshotPin, dry_summary_path: Path | None, ctx):
    rule = ("a dry-run launch names no earlier dry run; a measured launch needs the dry-run launch's recorded evidence: series_summary.json equal to the authorized hash, launch_kind DRY_RUN, "
            "status COMPLETE, not halted, run rp90 recorded and classified HEALTHY, the keep-awake cleanup recorded, a different series root, and a baseline snapshot that finished AFTER the dry run")
    if kind == li.DRY:
        ok = dry_summary_path is None and auth.dry_run_summary_sha256 is None
        return _res("R6", "dry-run evidence", ok, rule, "no earlier dry run is named" if ok else "a dry-run launch must not name an earlier dry run")
    problems: list[str] = []
    doc: dict = {}
    if dry_summary_path is None or not Path(dry_summary_path).is_file():
        return _res("R6", "dry-run evidence", False, rule, "the dry-run launch's series_summary.json was not provided")
    data = Path(dry_summary_path).read_bytes()
    if pn.sha256_bytes(data) != auth.dry_run_summary_sha256:
        problems.append("the dry-run summary does not hash to the authorized dry_run_summary_sha256")
    try:
        doc = json.loads(data.decode("utf-8"))
    except ValueError:
        problems.append("the dry-run summary is not JSON")
    runs = doc.get("runs") or []
    if doc.get("launch_kind") != li.DRY:
        problems.append(f"launch_kind {doc.get('launch_kind')!r} is not DRY_RUN")
    if doc.get("status") != "COMPLETE" or doc.get("halted"):
        problems.append(f"dry-run status {doc.get('status')!r}, halted {doc.get('halted')!r}")
    if len(runs) != 1 or runs[0].get("run_id") != li.DRY_RUN_ID or runs[0].get("verdict") != "REPLAY_RECORDED" or runs[0].get("class") not in rc.HEALTHY_CLASSES:
        problems.append(f"the dry run was not recorded and classified healthy: {[(r.get('run_id'), r.get('verdict'), r.get('class')) for r in runs]}")
    if not (doc.get("keep_awake_cleanup") or {}).get("ok"):
        problems.append("the dry run's keep-awake cleanup was not recorded as successful")
    if doc.get("series_root") and doc["series_root"] == str(ctx.root):
        problems.append("the dry-run series root equals this launch's root")
    try:
        if not _parse_utc(pin.manifest_finished_utc) > _parse_utc(doc["finished_utc"]):
            problems.append(f"the baseline snapshot finished {pin.manifest_finished_utc}, not after the dry run ({doc.get('finished_utc')}): it does not contain the dry run's residue")
    except (KeyError, ValueError, TypeError):
        problems.append("the dry-run summary has no readable finished_utc")
    return _res("R6", "dry-run evidence", not problems, rule, "; ".join(problems) or "dry run rp90 recorded healthy; the baseline snapshot postdates it", {"dry_class": runs[0].get("class") if runs else None})


def gate_s1_pinned(ctx, be, pin: pn.SnapshotPin, *, allowed: dict[str, str] | None, repo_root: Path, snapshot_path: Path | None = None):
    """S1 against the pinned snapshot. Always executed (never reads an earlier result); the command is built from the pin."""
    rule = (f"{pin.id}: identity matches the pin, and archive_evidence.py verify --check-sources reports every archived hash intact with source drift limited to owner-signed additions "
            f"whose SHA-256 still matches ({len(allowed or {})} allowed)")
    ident = pn.verify_snapshot_identity(pin, snapshot_path)
    if not ident["ok"]:
        return _res("S1", "source snapshot intact (external pin)", False, rule, f"snapshot identity check failed: {sorted(k for k, v in ident['checks'].items() if not v)}", {"identity": ident})
    argv = [ctx.win_python, ctx.win_repo + r"\scripts\research_audit\archive_evidence.py", "verify", "--snapshot", pin.path_win, "--check-sources"]
    r = be.run(argv, None, 3600)
    text = (r.stdout + b"\n" + r.stderr).decode("utf-8", "replace")
    result = sn.evaluate_s1_replay(text, allowed=allowed, repo_root=repo_root)
    detail = (f"executed, exit {r.code}; checked {result.get('checked')}, mismatched {result.get('mismatched')}, missing {result.get('missing')}, unlisted {result.get('unlisted')}, "
              f"source drift {result.get('source_drift')}; unexpected {result.get('unexpected_count')}; allowed with changed hash {result.get('allowed_hash_mismatch')}")
    return _res("S1", "source snapshot intact (external pin)", result["verdict"] == "PASS", rule, detail, {"result": result, "argv_snapshot": pin.path_win})


def run_launch_preflight(ctx, be, rows, out_dir: Path, *, kind: str, launch_id: str, approval: la.LaunchApproval, auth: la.EntryAuthorization, pin: pn.SnapshotPin,
                         source_manifest: dict, stage_dir: Path, clock_dir: Path | None, helper_pid: int | None, env_doc: dict | None, own_pid: int, s1_allowed: dict | None,
                         dry_summary_path: Path | None, snapshot_path: Path | None, node: bool, series_root_prefix: str, mode: str) -> tuple[list, dict]:
    """All gates, in GATE_ORDER_V2, cheap first and the long S1 last. A gate that raises fails. Writes preflight_report.json (new file). There is no way to run a subset."""
    p1b = dr.p1b_module()
    require_backend_for_mode(be, mode)
    thr = p1b.THR
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    results: list = []
    info: dict = {"baseline": {}, "chaincode": [], "fingerprint": None}
    net = p1b.load_network(ctx)
    manifest = json.loads((Path(stage_dir) / "series_manifest.json").read_text(encoding="utf-8")) if (Path(stage_dir) / "series_manifest.json").is_file() else None

    def attempt(gid: str, name: str, fn) -> None:
        try:
            res = fn()
        except Exception as exc:                                                        # noqa: BLE001 - a gate that cannot run has failed
            res = p1b.GateResult(gid, name, "FAIL", "gate raised", repr(exc))
        results.append(res)
        print(f"{res.id:<4}{res.status:<5} {res.name}: {res.detail[:200]}", flush=True)

    def s5():
        res, base, cc_names = p1b.gate_s5(ctx, be)
        info["baseline"], info["chaincode"] = base, cc_names
        return res

    def s7():
        res, fp = p1b.gate_s7(ctx, be, net, out_dir)
        info["fingerprint"] = fp
        return res

    runners = {
        "S3": ("host memory headroom", lambda: p1b.eval_memory(be.host_memory(thr.mem_window_samples, thr.mem_window_interval_s), be.wsl_mem_available_mb(), thr)),
        "S2": ("host power and sleep", lambda: p1b.gate_s2(be, helper_pid)),
        "S4": ("disk", lambda: p1b.eval_disk(be.disk_free_gb("/mnt/c"), thr)),
        "S5": ("containers and health", s5),
        "S6": ("Raft and chaincode state", lambda: p1b.gate_s6(ctx, be, net)),
        "R1": ("snapshot identity (external pin)", lambda: gate_r1_pins(pin, snapshot_path)),
        "R2": ("required configuration values", lambda: gate_r2_launch(ctx, rows, kind, launch_id, approval, pin, env_doc, net.raw, manifest, series_root_prefix)),
        "R3": ("replay schedule integrity", lambda: gate_r3_launch(stage_dir, auth, rows, approval, node=node)),
        "S7": ("existing-channel fingerprint", s7),
        "S8": ("no name collision", lambda: p1b.gate_s8(ctx, be, net, list(rows))),
        "S9": ("clock pre-flight", lambda: p1b.gate_s9(ctx, be, clock_dir, thr)),
        "R4": ("no conflicting experiment process", lambda: g4.gate_r4(be, own_pid)),
        "S10": ("tool and source pinning (external manifest)", lambda: gate_s10_launch(ctx, out_dir, source_manifest)),
        "S11": ("output roots absent", lambda: p1b.gate_s11(ctx, list(rows))),
        "R5": ("no reuse of existing evidence", lambda: pf.gate_r5_no_evidence_reuse(ctx, stage_dir, out_dir)),
        "R6": ("dry-run evidence", lambda: gate_r6_dry_run_evidence(kind, auth, pin, dry_summary_path, ctx)),
        "S1": ("source snapshot intact (external pin)", lambda: gate_s1_pinned(ctx, be, pin, allowed=s1_allowed, repo_root=Path(ctx.repo), snapshot_path=snapshot_path)),
    }
    for gid in GATE_ORDER_V2:
        attempt(gid, *runners[gid])
    results.sort(key=lambda g: GATE_ORDER_V2.index(g.id))
    all_pass = bool(results) and all(g.status == "PASS" for g in results) and sorted(g.id for g in results) == sorted(GATE_ORDER_V2)
    report = {"tool": TOOL_VERSION, "created_utc": be.now_utc(), "snapshot": pin.id, "boot_id": be.boot_id(), "mode": mode, "launch_kind": kind, "launch_id": launch_id,
              "gates": [g.as_dict() for g in results], "all_pass": all_pass, "evaluated": [g.id for g in results], "required": list(GATE_ORDER_V2), "context": info,
              "thresholds_p1b_unchanged": thr.__dict__}
    p1b.write_json_new(out_dir / "preflight_report.json", report)
    return results, report


# ----------------------------------------------------------------------------------------------- binding and per-run evaluation
def bind_s1_allowance(auth: la.EntryAuthorization, text: str | bytes | None) -> dict[str, str]:
    """The drift list supplied must be exactly what the authorization names, and only under the route that allows one. Raises before anything starts."""
    if auth.s1_route == "SNAPSHOT_EXACT":
        if text is not None:
            raise sn.SnapshotError("a drift list was supplied but the authorization's s1_route is SNAPSHOT_EXACT")
        return {}
    if text is None:
        raise sn.SnapshotError("the authorization's s1_route is SIGNED_DRIFT_UPDATE but no drift list was supplied")
    return sn.bind_allowance(text, route="SIGNED_DRIFT_UPDATE", evidence_sha256=auth.s1_evidence_sha256 or "")


def check_bindings(s, rows, approval: la.LaunchApproval, auth: la.EntryAuthorization, pins: pn.Pins, stage_dir: Path, *, mode: str) -> None:
    """Everything that must hold before a backend call. Raises on the first defect."""
    require_backend_for_mode(s.be, mode)
    kind = auth.kind
    if approval.kind != kind:
        raise la.ApprovalRefusedV2(f"refused: the approval is for {approval.kind}, the authorization for {kind}")
    if approval.sha256 != auth.series_approval_sha256:
        raise la.ApprovalRefusedV2("refused: the series approval does not hash to the authorization's series_approval_sha256")
    if pins.sha256 != auth.pins_sha256:
        raise la.ApprovalRefusedV2("refused: the pins file does not hash to the authorization's pins_sha256")
    if kind == li.MEASURED and approval.thresholds.status != "APPROVED":
        raise la.ApprovalRefusedV2("refused: a measured launch needs APPROVED thresholds")
    li.validate_launch_rows(kind, list(rows))
    li.check_launch_id(auth.launch_id, kind)
    if PurePosixPath(str(s.ctx.root).replace("\\", "/")).name != auth.launch_id:
        raise la.ApprovalRefusedV2(f"refused: the series root {s.ctx.root} is not named {auth.launch_id}")
    if s.ctx.win_snapshot != pins.snapshot.path_win:
        raise sn.SnapshotError(f"Ctx.win_snapshot {s.ctx.win_snapshot!r} != pinned {pins.snapshot.path_win!r}")
    if not (Path(stage_dir) / "series_manifest.json").is_file():
        raise FileNotFoundError("no staged series manifest")


def pre_run_clock(s, probe, s9_pass: bool, boot_start: str) -> dict:
    """Real evidence before a run (clock_evidence.pre_run_clock_ok). Waits at most the probe's flush margin for its first samples."""
    p1b = dr.p1b_module()
    be, ctx, thr = s.be, s.ctx, s.thr
    path = ctx.root / "clock_series" / "clock_samples.csv"
    waited, samples = 0.0, []
    while True:
        try:
            samples, _ = ce.read_samples(path)
        except (OSError, ValueError):
            samples = []
        if len(samples) >= 2 or waited >= thr.clock_flush_margin_s + 5.0:
            break
        be.sleep(1.0)
        waited += 1.0
    return ce.pre_run_clock_ok(samples, now_ns=p1b.utc_to_ns(be.now_utc()), preflight_clock_gate_pass=s9_pass, probe_alive=probe is not None and probe.poll() is None,
                               series_boot_id=boot_start, current_boot_id=be.boot_id())


def evaluate_recorded_run(s, row, stage_dir, approval: la.LaunchApproval, v: dict, probe, boot_start: str, s9_pass: bool) -> dict:
    """Judge a run the executor recorded: configuration verification, clock evidence over its window from the continuous record, the 2C classification (written as classification.json),
    the descriptive p95. May raise: the caller turns any exception into DRIVER_ERROR (fail closed)."""
    p1b = dr.p1b_module()
    ctx, be, thr = s.ctx, s.be, s.thr
    validation = se.run_validation(ctx, row)
    be.sleep(thr.clock_flush_margin_s)
    windows = p1b.run_windows(ctx, [row.run_id])
    clock = ce.evaluate_from_files(ctx.root / "clock_series" / "clock_samples.csv", windows[0] if windows else None, step_ms=thr.clock_step_ms, max_gap_ms=thr.clock_gap_max_ms,
                                   series_boot_id=boot_start, current_boot_id=be.boot_id(), preflight_clock_gate_pass=s9_pass, probe_alive=probe.poll() is None)
    cls = rc.classify_replay_run_dir(row.run_id, Path(str(stage_dir)) / "runs" / row.run_id, dispatch_dir=Path(str(ctx.caliper_ws / "runs" / row.run_id)),
                                     recorder_dir=Path(str(ctx.recorder_dir(row.run_id))), ledger_csv=Path(str(ctx.rd(row.run_id) / "ledger/ledger/ledger_tx.csv")),
                                     resources_csv=Path(str(ctx.rd(row.run_id) / "resources/resource_samples.csv")), clock_evidence=clock, thr=approval.thresholds)
    p1b.write_json_new(ctx.rd(row.run_id) / "classification.json", {**cls, "validation": validation, "clock_evidence_full": clock})
    try:
        measured_rows = fd.read_recorder_csv(se.recorder_csv(ctx.recorder_dir(row.run_id), row.run_id, "measured"))
    except fd.EvidenceError:
        measured_rows = []
    out = {"class": cls["class"], "labels": cls["labels"], "subtypes": cls["subtypes"], "validation_ok": validation["ok"], "clock_evidence_verdict": clock.get("verdict"),
           "fidelity_phase1_verdict": v.get("fidelity"), "p95_ms_descriptive": se.run_p95_ms(measured_rows), "drain_deadline_exceeded": cls["fabric_overload_indicators"].get("drain_deadline_exceeded")}
    if not validation["ok"]:
        out.update(verdict="INVALID_INFRA", verdict_executor=v["verdict"])
    return out


def between_run_integrity(s, baseline_fingerprint: dict | None, row) -> list[str]:
    """SC10: fingerprint the reference channels (height, current block hash, config-block hash) after a run and compare with the pre-flight fingerprint. A failure to fingerprint is a
    difference (fail closed). Evidence is written to a new file per run."""
    p1b = dr.p1b_module()
    ctx, be = s.ctx, s.be
    try:
        fp = p1b.fingerprint(ctx, be, p1b.load_network(ctx), ctx.root / f"fingerprint_after_{row.run_id}")
        diffs = p1b.compare_fingerprints(baseline_fingerprint or {}, fp)
        p1b.write_json_new(ctx.root / f"fingerprint_compare_{row.run_id}.json", {"after": fp, "differences": diffs})
    except Exception as exc:                                                              # noqa: BLE001
        diffs = [f"between-run fingerprint failed: {exc!r}"]
    return diffs


def write_analysis_inputs(ctx, rows_run, summary: dict, kind: str, launch_id: str) -> str | None:
    """analysis_inputs.json for analysis_primary: per run, its class and a COPY of the measured-round recorder csv placed in the series root with its SHA-256 (the original stays in the
    Caliper workspace). Written for a measured launch only. Never overwrites."""
    if kind != li.MEASURED:
        return None
    p1b = dr.p1b_module()
    by_id = {e["run_id"]: e for e in summary["runs"]}
    runs = []
    for row in rows_run:
        entry = by_id.get(row.run_id)
        src = se.recorder_csv(ctx.recorder_dir(row.run_id), row.run_id, "measured")
        copy_path, digest = None, None
        if entry is not None and entry.get("verdict") == "REPLAY_RECORDED" and Path(src).is_file() and ctx.rd(row.run_id).exists():
            copy_path = ctx.rd(row.run_id) / "recorder_measured_round.csv"
            with open(src, "rb") as fin, open(copy_path, "xb") as fout:
                shutil.copyfileobj(fin, fout)
            digest = pn.sha256_file(copy_path)
        runs.append({"run_id": row.run_id, "B": row.B, "replicate": row.replicate, "position": row.seq, "class": (entry or {}).get("class") or "NOT_CLASSIFIED",
                     "recorder_csv": str(copy_path) if copy_path else None, "recorder_sha256": digest, "original_recorder_csv": str(src)})
    p1b.write_json_new(ctx.root / "analysis_inputs.json", {"schema": an.SCHEMA, "launch_id": launch_id, "window": dr.MAIN_WINDOW, "runs": runs})
    return str(ctx.root / "analysis_inputs.json")


# ----------------------------------------------------------------------------------------------- the launch
def run_launch(s, rows, stage_dir, approval: la.LaunchApproval, auth: la.EntryAuthorization, pins: pn.Pins, source_manifest: dict[str, str], *, mode: str, own_pid: int,
               clock_dir: Path | None, env_doc: dict | None, preflight_dir: Path, helpers: dict | None = None, s1_allowed_text: str | bytes | None = None,
               dry_summary_path: Path | None = None, snapshot_path: Path | None = None, node: bool = True, series_root_prefix: str = dr.SERIES_ROOT_WSL,
               artifact_hashes: dict | None = None) -> dict:
    """One launch. Returns the summary dict (also written as series_summary.json once a series root exists). Never retries, never resumes, never replaces a run."""
    p1b = dr.p1b_module()
    check_bindings(s, rows, approval, auth, pins, stage_dir, mode=mode)                 # raises BEFORE anything starts: backend mode, hashes, namespace, snapshot
    s1_allowed = bind_s1_allowance(auth, s1_allowed_text)
    ctx, be, kind = s.ctx, s.be, auth.kind
    s.thr = dataclasses.replace(p1b.THR, caliper_timeout_s=approval.caliper_timeout_s)
    stage_manifest = json.loads((Path(str(stage_dir)) / "series_manifest.json").read_text(encoding="utf-8"))
    budget = dm.build_budget(kind, [(r.B, stage_manifest["runs"][r.run_id]["tx_total"]) for r in rows], caliper_timeout_s=approval.caliper_timeout_s, gate_ids=GATE_ORDER_V2, thr=s.thr)
    pid_file = Path(preflight_dir).parent / f"{Path(preflight_dir).name}_keep_awake.pid"
    summary: dict = {"tool": TOOL_VERSION, "live_status": LIVE_STATUS, "mode": mode, "launch_kind": kind, "launch_id": auth.launch_id, "snapshot": pins.snapshot.id, "status": "NOT_COMPLETED",
                     "halted": None, "runs": [], "series_root": str(ctx.root), "keep_awake_budget_s": budget.total_s, "limits": LIMITS}
    keep = k2.KeepAwakeV2(be, budget.total_s, pid_file, p1b._wslpath_win(be, pid_file), backend_check=lambda b: require_backend_for_mode(b, mode))
    owns = {"root": False, "preflight": False}          # a launch writes only into folders IT created: a pre-existing root or pre-flight folder belongs to an earlier launch
    try:
        keep.__enter__()
        _core(s, rows, stage_dir, approval, auth, pins, source_manifest, keep, budget, summary, stage_manifest, s1_allowed, owns, mode=mode, own_pid=own_pid, clock_dir=clock_dir,
              env_doc=env_doc, preflight_dir=preflight_dir, helpers=helpers, dry_summary_path=dry_summary_path, snapshot_path=snapshot_path, node=node,
              series_root_prefix=series_root_prefix, artifact_hashes=artifact_hashes or {})
    finally:
        record = keep.cleanup()
        summary["keep_awake_cleanup"] = record
        summary["finished_utc"] = be.now_utc()
        if summary.get("status") == "COMPLETE" and not record["ok"]:
            summary["status"] = "COMPLETE_KEEP_AWAKE_CLEANUP_FAILED"
        if owns["root"]:
            p1b.write_json_new(ctx.root / "keep_awake_cleanup.json", record)
            p1b.write_json_new(ctx.root / "series_summary.json", summary)
        elif owns["preflight"]:                          # a launch that never created its root leaves its record in its own new pre-flight folder
            p1b.write_json_new(Path(preflight_dir) / "keep_awake_cleanup.json", record)
            p1b.write_json_new(Path(preflight_dir) / "launch_summary_not_started.json", summary)
    return summary


def _core(s, rows, stage_dir, approval, auth, pins, source_manifest, keep, budget, summary, stage_manifest, s1_allowed, owns, *, mode, own_pid, clock_dir, env_doc, preflight_dir, helpers,
          dry_summary_path, snapshot_path, node, series_root_prefix, artifact_hashes) -> None:
    p1b = dr.p1b_module()
    ctx, be, kind = s.ctx, s.be, auth.kind
    s.helper_pid = keep.pid
    if Path(preflight_dir).exists():
        raise FileExistsError(f"the pre-flight folder {preflight_dir} already exists: it belongs to an earlier launch and is never reused")
    owns["preflight"] = True
    results, report = run_launch_preflight(ctx, be, rows, preflight_dir, kind=kind, launch_id=auth.launch_id, approval=approval, auth=auth, pin=pins.snapshot,
                                           source_manifest=source_manifest, stage_dir=stage_dir, clock_dir=clock_dir, helper_pid=keep.pid, env_doc=env_doc, own_pid=own_pid,
                                           s1_allowed=s1_allowed, dry_summary_path=dry_summary_path, snapshot_path=snapshot_path, node=node, series_root_prefix=series_root_prefix, mode=mode)
    if not report["all_pass"]:
        summary.update(status="PREFLIGHT_FAILED", failed_gates=[g.id for g in results if g.status != "PASS"], started_runs=[])
        return
    s9_pass = any(g.id == "S9" and g.status == "PASS" for g in results)
    ctx.root.parent.mkdir(parents=True, exist_ok=True)
    ctx.root.mkdir(parents=False, exist_ok=False)
    owns["root"] = True
    s.log_path = ctx.root / "series_events.log"
    s.baseline, s.chaincode = report["context"]["baseline"], report["context"]["chaincode"]
    boot_start = be.boot_id()
    s.log(f"launch start ({kind} {auth.launch_id}); mode {mode}; snapshot {pins.snapshot.id}; thresholds {approval.thresholds.status}; keep-awake budget {budget.total_s} s")
    p1b.write_json_new(ctx.root / "launch_manifest.json", {
        "tool": TOOL_VERSION, "live_status": LIVE_STATUS, "mode": mode, "started_utc": be.now_utc(), "boot_id": boot_start, "launch_kind": kind, "launch_id": auth.launch_id,
        "snapshot": pins.snapshot.id, "run_ids": [r.run_id for r in rows], "channels": [r.channel for r in rows], "series_approval": approval.raw, "series_approval_sha256": approval.sha256,
        "authorization": auth.raw, "pins_sha256": pins.sha256, "artifact_hashes": artifact_hashes, "thresholds": approval.thresholds.as_dict(), "caliper_timeout_s": approval.caliper_timeout_s,
        "host_max_seconds": approval.host_max_seconds, "keep_awake_pid": keep.pid, "keep_awake_nonce": keep.nonce, "keep_awake_budget": {"total_s": budget.total_s, "preflight_s": budget.preflight_s,
        "probe_s": budget.probe_s, "components": budget.table()}, "stop_conditions": list(la.STOP_CONDITIONS_V2), "stop_conditions_sha256": la.stop_conditions_sha256_v2(),
        "preflight_report": str(Path(preflight_dir) / "preflight_report.json"), "limits": LIMITS})
    probe = be.spawn(se.probe_argv(ctx, budget.probe_s), p1b.tool_env(ctx), ctx.root / "clock_series.stdout", ctx.root / "clock_series.stderr")
    s.probe = probe
    halted, consecutive = None, 0
    try:
        be.sleep(3.0)
        if probe.poll() is not None:
            raise p1b.HaltSeries(f"the continuous clock probe exited at once (exit {probe.poll()}): no run was started")
        with se.executor_overrides(approval.host_max_seconds, approval.thresholds.fidelity_thresholds()):
            for row in rows:
                is_dry = row.kind == "dry_run"
                checks = se.replay_precheck(s, row, stage_dir, probe)
                for name, ok, detail in checks:
                    s.log(f"{row.run_id}: precheck {'PASS' if ok else 'FAIL'} {name}: {detail}")
                if not all(ok for _, ok, _ in checks):
                    raise p1b.HaltSeries(f"{row.run_id}: pre-run check failed: " + "; ".join(n for n, ok, _ in checks if not ok))
                pre = pre_run_clock(s, probe, s9_pass, boot_start)
                s.log(f"{row.run_id}: pre-run clock evidence {'PASS' if pre['ok'] else 'FAIL'}: {pre['reasons'] or 'fresh and complete'}")
                if not pre["ok"]:
                    raise p1b.HaltSeries(f"{row.run_id}: pre-run clock evidence failed: {pre['reasons']}")
                s.log(f"{row.run_id}: START {row.channel} B={row.B} window={row.window} ({'dry run' if is_dry else 'measured'}, seq {row.seq})")
                mean_rate = stage_manifest["runs"][row.run_id]["schedules"]["measured"]["n_tx"] / float(of.MEASURED_S)
                try:
                    v = dr.execute_replay_run(s, row, stage_dir, mean_rate, helpers=helpers, clock_gate_pass=pre["ok"])
                except Exception as exc:                                                  # noqa: BLE001  (LiveExecutionRefused is a SystemExit and passes through)
                    v = {"run_id": row.run_id, "channel": row.channel, "verdict": "DRIVER_ERROR", "error": repr(exc), "traceback": traceback.format_exc()}
                entry = {"run_id": row.run_id, "kind": row.kind, "window": row.window, "B": row.B, "replicate": row.replicate, "verdict": v["verdict"], "class": None}
                if v["verdict"] == "REPLAY_RECORDED":
                    try:
                        entry.update(evaluate_recorded_run(s, row, stage_dir, approval, v, probe, boot_start, s9_pass))
                    except Exception as exc:                                                  # noqa: BLE001  fail closed
                        entry.update(verdict="DRIVER_ERROR", verdict_executor=v["verdict"], error=repr(exc), traceback=traceback.format_exc())
                else:
                    entry["step_failure"] = v.get("step_failure")
                    if v.get("error"):
                        entry["error"], entry["traceback"] = v["error"], v.get("traceback")
                halt, reason, consecutive = se.decide_after_run(verdict=entry["verdict"], cls=entry["class"], is_dry_run=is_dry, consecutive_invalid=consecutive, approval=approval,
                                                                invalid_stop=s.thr.consecutive_invalid_stop)
                if ctx.rd(row.run_id).exists():                                        # a run that never created its folder has nothing to compare; the end-of-launch check still runs
                    integrity = between_run_integrity(s, report["context"]["fingerprint"], row)
                    if integrity:
                        entry["integrity_differences"] = integrity
                        halt, reason = True, f"SC10 reference-channel fingerprint changed after {row.run_id}: {integrity[:2]}" + ("" if not halt else f" (also: {reason})")
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
        s.log(f"LAUNCH HALTED: {halted}")
    except Exception as exc:                                                              # noqa: BLE001  an unexpected error halts: fail closed, evidence stays
        halted = f"UNEXPECTED {exc!r}"
        summary["unexpected_traceback"] = traceback.format_exc()
        s.log(f"LAUNCH HALTED: {halted}")
    except BaseException as exc:                                                          # noqa: BLE001  interrupted: finalise, then let it go on
        halted = f"INTERRUPTED {type(exc).__name__}"
        raise
    finally:
        diffs: list = []
        try:
            fp_post = p1b.fingerprint(ctx, be, p1b.load_network(ctx), ctx.root / "fingerprint_post")
            diffs = p1b.compare_fingerprints(report["context"]["fingerprint"] or {}, fp_post)
            p1b.write_json_new(ctx.root / "fingerprint_compare.json", {"post": fp_post, "differences": diffs})
        except Exception as exc:                                                      # noqa: BLE001 - the probe and the helper must still be stopped
            diffs = [f"end-of-series fingerprint failed: {exc!r}"]
        clock_final = p1b.finish_clock_series(s, probe, [r.run_id for r in rows])
        recorded = [r for r in rows if r.run_id in s.verdicts and s.verdicts[r.run_id]["verdict"] == "REPLAY_RECORDED"]
        recon = se.final_reconciliation(ctx, recorded, stage_manifest)
        limited = [e["run_id"] for e in summary["runs"] if e.get("class") in rc.CONTINUABLE]
        invalid = [e["run_id"] for e in summary["runs"] if str(e["verdict"]).startswith("INVALID")]
        status = ("HALTED" if halted else "FAILED_INTEGRITY" if (diffs or not recon["ok"]) else "CLOCK_RECORD_NOT_PASS" if clock_final.get("verdict") != "PASS"
                  else "COMPLETE_WITH_INVALID_RUNS" if invalid else "COMPLETE_WITH_LIMITED_RUNS" if limited else "COMPLETE")
        summary.update(status=status, halted=halted, fingerprint_differences=diffs, reconciliation=recon, limited_runs=limited, invalid_runs=invalid,
                       clock_series_check={k: clock_final.get(k) for k in ("verdict", "samples", "steps_total", "steps_in_run_windows", "error")},
                       claims={"live_scheduling_fidelity_validated": False, "law_predictions_evaluated": False,
                               "note": "descriptive only: n = 3 per configuration supports no significance, controller, capacity or HFBTP-transfer claim"})
        try:
            summary["analysis_inputs"] = write_analysis_inputs(ctx, rows, summary, kind, auth.launch_id)
        except Exception as exc:                                                          # noqa: BLE001 - evidence is already on disk; record why the manifest is missing
            summary["analysis_inputs"] = f"NOT_WRITTEN: {exc!r}"
