"""Pre-flight gates for the replay series (Phase 2). Every mandatory gate must PASS or nothing is started. No gate is weakened.

Reused BY IMPORT from the frozen P1b driver (unchanged, hash-checked by driver.p1b_module): S2 power and keep-awake, S3 memory (the owner's 1,500 MB / 85 % / 100 pages
thresholds, "do not waive"), S4 disk, S5 containers and health, S6 Raft and chaincode state, S7 reference-channel fingerprint, S8 channel-name collision, S9 clock probe,
S11 output roots absent. Their numbers are P1b's, untouched.
Replaced because they pin the P1b matrix or tool list: S1 (snapshot, strict, third snapshot), S10 (tool pinning).
New: R1 snapshot identity, R2 required configuration values, R3 replay schedule integrity, R4 no conflicting experiment process, R5 no reuse of existing evidence.

A real p1b.Backend, and any backend that does not declare OFFLINE_FAKE, is refused while LIVE_EXECUTION_ENABLED is False (guard.require_offline_backend). Gate functions that read only files run offline.
The launch-day S1 gate must be re-run independently before any live experiment; this module runs it again as its last gate and never reads an earlier result.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path, PurePosixPath

from . import PACKAGE_VERSION, REPO_ROOT, ensure_paths
from . import caliper_check as cc
from . import driver as dr
from . import guard
from . import snapshot as sn

ensure_paths()

# The eight frozen P1b files, measured 2026-11-14 and equal to D:/PLAN_BLOCK18/p1b_source_bundle_2026-11-13/SHA256SUMS.txt.
FROZEN_P1B_SHA256 = {
    "scripts/e1/e1_quicklook_p1b.py": "00575656d47ca25d624737b9dd816d9d6e889136e2fdf9193b3356f4948488a0",
    "scripts/e1/e1_client_classification_v2.py": "ceea2019eb3665065c10df6072dfca3b49c1cc8ffdbdc01d09bbde4982edc83e",
    "scripts/e1/e1_p1b_matrix.py": "e78634adb9ec311e2735eb1f9c1d5e166ec7b2466d9e70f0f185f5740b9b34e8",
    "scripts/e1/e1_host_sampler.py": "92cdcbe1a3892b605e0783d033c6a5ece627309cf3fe8f03432f02d0e15ae143",
    "tests/e1/test_e1_quicklook_p1b.py": "383314f96b96448c77e565dfcd8eed5e310e993af102f644de1c8fe0fa1fd6c7",
    "tests/e1/test_e1_client_classification_v2.py": "c6b0427500fab9973e5bb6f9db58bd85e456eefd97eeca82ba5d6457474f21aa",
    "tests/e1/test_e1_p1b_matrix.py": "87f79ae5615aab828134d1a69824becc60ecfc2de8fc777d9ab46833a268daa1",
    "tests/e1/test_e1_host_sampler.py": "ae512276b679ce92c2258f2a18cf45380e25a1a835b5ad26e99779676ae7b627",
}
GATE_ORDER = ("S3", "S2", "S4", "S5", "S6", "R1", "R2", "R3", "S7", "S8", "S9", "R4", "S10", "S11", "R5", "S1")
CONFLICT_PATTERNS = ("caliper", "e1_quicklook", "e1_channel_run", "e1_clock_rate_probe", "e1_host_sampler", "e1_resource_sampler", "e1_metrics_scraper",
                     "p4_replay.series", "p4_replay.driver", "e1_trace_replay")
EXISTING_EVIDENCE_PREFIXES = ("/home/mailf18/e1/p1b-tier1-20261113", "/home/mailf18/e1/p1b-quicklook", "/mnt/d/PLAN_BLOCK18/evidence_archive")
WSL_E1_GROUP = "/home/mailf18/e1"


def _p1b():
    return dr.p1b_module()


def _fail(gid: str, name: str, rule: str, detail: str, evidence: dict | None = None):
    return _p1b().GateResult(gid, name, "FAIL", rule, detail, evidence or {})


def _res(gid: str, name: str, ok: bool, rule: str, detail: str, evidence: dict | None = None):
    return _p1b().GateResult(gid, name, "PASS" if ok else "FAIL", rule, detail, evidence or {})


def sha256_file(path: Path) -> str | None:
    p = Path(path)
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None


# ----------------------------------------------------------------------------------------------- R1 snapshot identity
def gate_r1_snapshot_identity(snapshot_path: Path | None = None):
    rule = (f"the snapshot folder is {sn.SNAPSHOT_ID}: SHA256SUMS.txt = {sn.SNAPSHOT_SUMS_SHA256[:16]}..., manifest hash, git head, 26 groups incl. repo_p4_replay and wsl_e1, "
            "manifest result PASS with 0 problem files (read-only; the per-file re-hash is gate S1)")
    ident = sn.verify_identity(snapshot_path)
    failed = sorted(k for k, v in ident["checks"].items() if not v)
    return _res("R1", "snapshot identity", ident["ok"], rule, "identity matches the pinned snapshot" if ident["ok"] else f"failed: {failed}", {"identity": ident})


# ----------------------------------------------------------------------------------------------- R2 required configuration values
def gate_r2_required_config(ctx, rows, approval, env_doc: dict | None, net_raw: dict | None, stage_manifest: dict | None, series_root_prefix: str = dr.SERIES_ROOT_WSL):
    rule = ("matrix valid (one dry run then six measured, B in {10, 40}, tau 2s, P 10,485,760, schedule A); Ctx snapshot, series root and chaincode as pinned; "
            "the approved Caliper timeout and host guard fit the staged schedules")
    problems: list[str] = []
    try:
        dr.validate_matrix(list(rows))
    except dr.MatrixError as exc:
        problems.append(f"matrix: {exc}")
    try:
        sn.assert_ctx_snapshot(ctx)
    except sn.SnapshotError as exc:
        problems.append(str(exc))
    root = str(PurePosixPath(str(ctx.root).replace("\\", "/")))
    if not root.startswith(series_root_prefix.rstrip("/") + "/"):
        problems.append(f"series root {root} is not under {series_root_prefix}/")
    if root == WSL_E1_GROUP or root.startswith(WSL_E1_GROUP + "/"):
        problems.append("series root is inside the wsl_e1 snapshot group")
    cc_name = None
    if env_doc is not None and net_raw is not None:
        cc_name, net_cc = env_doc.get("chaincode", {}).get("name"), (net_raw.get("chaincode") or {})
        if cc_name != "e1asset" or net_cc.get("name") != cc_name or not re.fullmatch(r"e1asset_[0-9.]+:[0-9a-f]{64}", str(net_cc.get("package_id", ""))):
            problems.append(f"chaincode mismatch: env {cc_name!r}, network {net_cc.get('name')!r}, package {str(net_cc.get('package_id'))[:30]!r}")
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
    else:
        problems.append("no staged series manifest")
    return _res("R2", "required configuration values", not problems, rule, "; ".join(problems) or "all required configuration values are as pinned",
                {"chaincode": cc_name, "last_pre_roll_offset_ms": last_pre, "last_measured_offset_ms": last_meas})


# ----------------------------------------------------------------------------------------------- R3 replay schedule integrity
def gate_r3_schedule_integrity(stage_dir: Path, approval, rows, *, node: bool = True):
    rule = ("the staged tree re-hashes to its SHA256SUMS, that file's hash equals the approved stage_sums_sha256, the staged matrix equals the seeded matrix, every run folder passes "
            "the offline Caliper checks (counts, ms offsets, pre-roll separation, ties, bursts, timeout fit), and every window rebuilt to the P1-P5 schedule hash")
    stage = Path(stage_dir)
    problems: list[str] = []
    ver = dr.verify_stage(stage) if (stage / "SHA256SUMS.txt").is_file() else {"ok": False, "mismatched_or_missing": ["SHA256SUMS.txt"], "unlisted": []}
    if not ver["ok"]:
        problems.append(f"stage verify: mismatched/missing {ver['mismatched_or_missing'][:5]}, unlisted {ver['unlisted'][:5]}")
    sums = sha256_file(stage / "SHA256SUMS.txt")
    if sums != approval.stage_sums_sha256:
        problems.append("SHA256SUMS.txt hash differs from the approved stage_sums_sha256")
    expected_matrix = dr.matrix_csv(dr.build_matrix(approval.run_order_seed))
    staged_matrix = (stage / "replay_matrix.csv").read_bytes() if (stage / "replay_matrix.csv").is_file() else b""
    if staged_matrix != expected_matrix:
        problems.append("replay_matrix.csv differs from the matrix the approved seed generates")
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


# ----------------------------------------------------------------------------------------------- R4 no conflicting experiment process
def conflicting_processes(ps_text: str, own_pid: int) -> list[str]:
    """Lines of `ps -eo pid,ppid,args` that match an experiment tool, minus this process, its ancestors and the ps command itself."""
    rows = []
    for line in ps_text.splitlines():
        m = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)$", line)
        if m:
            rows.append((int(m.group(1)), int(m.group(2)), m.group(3)))
    parent = {pid: ppid for pid, ppid, _ in rows}
    ancestors, cur = {own_pid}, own_pid
    while cur in parent and parent[cur] not in ancestors and parent[cur] != 0:
        cur = parent[cur]
        ancestors.add(cur)
    hits = []
    for pid, _, args in rows:
        if pid in ancestors or re.match(r"ps\s", args):
            continue
        if any(p in args for p in CONFLICT_PATTERNS):
            hits.append(f"{pid} {args[:160]}")
    return hits


def gate_r4_no_conflicting_process(be, own_pid: int):
    rule = ("no process of an experiment tool (Caliper, the P1b driver, channel, clock probe, samplers, a replay series) other than this one, and no Caliper client process "
            "inside the Caliper container, and no second Caliper container")
    p1b = _p1b()
    r = be.run(["ps", "-eo", "pid,ppid,args"], None, 30)
    if r.code != 0:
        return _fail("R4", "no conflicting experiment process", rule, f"ps failed (exit {r.code}): the absence of a conflict cannot be shown")
    hits = conflicting_processes(r.stdout.decode("utf-8", "replace"), own_pid)
    top = be.run(["docker", "top", p1b.CALIPER, "-eo", "pid,args"], None, 30)
    if top.code != 0:
        return _fail("R4", "no conflicting experiment process", rule, f"docker top {p1b.CALIPER} failed (exit {top.code})")
    inside = [ln.strip()[:160] for ln in top.stdout.decode("utf-8", "replace").splitlines()[1:] if "caliper" in ln.lower() and "docker top" not in ln]
    ps_c = be.run(["docker", "ps", "--format", "{{.Names}}"], None, 30)
    others = sorted(n for n in ps_c.stdout.decode("utf-8", "replace").split() if n.startswith("caliper") and n != p1b.CALIPER) if ps_c.code == 0 else ["docker ps failed"]
    problems = [f"host process: {h}" for h in hits] + [f"inside {p1b.CALIPER}: {i}" for i in inside] + [f"container {o}" for o in others]
    return _res("R4", "no conflicting experiment process", not problems, rule, "; ".join(problems) or "no conflicting process found", {"host": hits, "inside": inside, "other_containers": others})


# ----------------------------------------------------------------------------------------------- S10 (replay) tool pinning
def gate_s10_replay(ctx, net, out_dir: Path, snapshot_path: Path | None = None):
    p1b = _p1b()
    rule = ("the frozen P1b files equal their pinned hashes, the P1b driver equals the hash the derived driver pins, every Phase 1 p4_replay file equals what the snapshot recorded "
            "(additions are listed, a changed file fails), and every tool and workload module is hashed")
    problems: list[str] = []
    frozen = {p: sha256_file(Path(ctx.repo) / p) for p in FROZEN_P1B_SHA256}
    bad = sorted(p for p, h in frozen.items() if h != FROZEN_P1B_SHA256[p])
    if bad:
        problems.append(f"frozen P1b files changed or missing: {bad}")
    if frozen["scripts/e1/e1_quicklook_p1b.py"] != dr.P1B_DRIVER_SHA256:
        problems.append("the P1b driver is not the hash the derived driver pins")
    recorded = sn.recorded_group_files(snapshot_path or sn.default_snapshot_path())
    current = sn.snapshot_group_files(Path(ctx.repo))
    changed = sorted(k for k, v in current.items() if k in recorded and recorded[k] != v)
    removed = sorted(k for k in recorded if k not in current)
    added = sorted(k for k in current if k not in recorded)
    if changed or removed:
        problems.append(f"Phase 1 files changed {changed} or removed {removed} since the snapshot")
    hashes = {f"scripts/e1/{n}": sha256_file(Path(ctx.scripts) / n) for n in p1b.INVOKED_TOOLS}
    hashes.update({p: sha256_file(Path(ctx.repo) / p) for p in p1b.PINNED_OTHER + ("fabric-testbed/caliper/workloads/e1_trace_replay.js",)})
    hashes.update({"env": sha256_file(Path(ctx.env)), "network": sha256_file(Path(ctx.net))})
    missing = sorted(k for k, v in hashes.items() if v is None)
    if missing:
        problems.append(f"missing files: {missing}")
    p1b.write_json_new(Path(out_dir) / "tool_hashes.json", {"hashes": hashes, "frozen_p1b": frozen, "p4_replay_added_since_snapshot": {k: current[k] for k in added},
                                                              "tool": f"p4_replay.preflight {PACKAGE_VERSION}"})
    return _res("S10", "tool and plan pinning (replay)", not problems, rule,
                "; ".join(problems) or f"{len(hashes)} files hashed; {len(frozen)} frozen P1b files and {len(recorded)} Phase 1 files unchanged; {len(added)} p4_replay files added since the snapshot",
                {"added_since_snapshot": added})


# ----------------------------------------------------------------------------------------------- R5 no reuse of existing evidence
def gate_r5_no_evidence_reuse(ctx, stage_dir: Path, out_dir: Path):
    rule = ("the series root and the preflight folder are new: absent, outside every existing evidence folder and the wsl_e1 snapshot group, and no run folder of this "
            "series already exists in the Caliper workspace")
    problems: list[str] = []
    root = str(PurePosixPath(str(ctx.root).replace("\\", "/")))
    for prefix in EXISTING_EVIDENCE_PREFIXES + (WSL_E1_GROUP,):
        if root == prefix or root.startswith(prefix + "/") or prefix.startswith(root + "/"):
            problems.append(f"series root {root} overlaps {prefix}")
    if Path(str(ctx.root)).exists():
        problems.append(f"series root {root} already exists")
    out = str(PurePosixPath(str(out_dir).replace("\\", "/")))                  # the preflight folder itself is created create-exclusive just before the gates run
    for prefix in EXISTING_EVIDENCE_PREFIXES + (WSL_E1_GROUP,):
        if out == prefix or out.startswith(prefix + "/"):
            problems.append(f"preflight folder {out} lies inside {prefix}")
    stage = str(PurePosixPath(str(stage_dir).replace("\\", "/")))
    if stage == root or stage.startswith(root + "/") or root.startswith(stage + "/"):
        problems.append("the staged tree and the series root overlap")
    return _res("R5", "no reuse of existing evidence", not problems, rule, "; ".join(problems) or f"{root} is new and overlaps no evidence folder")


# ----------------------------------------------------------------------------------------------- the preflight run
def run_replay_preflight(ctx, be, rows, out_dir: Path, *, approval, stage_dir: Path, clock_dir: Path | None, helper_pid: int | None, env_doc: dict | None,
                         own_pid: int, s1_evidence_text: str | None = None, s1_allowed: dict | None = None, gates: set[str] | None = None,
                         snapshot_path: Path | None = None, node: bool = True, series_root_prefix: str = dr.SERIES_ROOT_WSL) -> tuple[list, dict]:
    """Run the gates in GATE_ORDER (cheap first, the 18-minute S1 last). A gate that raises FAILS. Writes preflight_report.json (new file) and returns it."""
    p1b = _p1b()
    guard.require_offline_backend(be)
    thr = p1b.THR
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)
    results: list = []
    info: dict = {"baseline": {}, "chaincode": [], "fingerprint": None}
    net = p1b.load_network(ctx)
    manifest = json.loads((Path(stage_dir) / "series_manifest.json").read_text(encoding="utf-8")) if (Path(stage_dir) / "series_manifest.json").is_file() else None

    def want(g: str) -> bool:
        return gates is None or g in gates

    def attempt(gid: str, name: str, fn) -> None:
        if not want(gid):
            return
        try:
            res = fn()
        except Exception as exc:                                                        # noqa: BLE001 - a gate that cannot run has failed
            res = _fail(gid, name, "gate raised", repr(exc))
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
        "R1": ("snapshot identity", lambda: gate_r1_snapshot_identity(snapshot_path)),
        "R2": ("required configuration values", lambda: gate_r2_required_config(ctx, rows, approval, env_doc, net.raw, manifest, series_root_prefix)),
        "R3": ("replay schedule integrity", lambda: gate_r3_schedule_integrity(stage_dir, approval, rows, node=node)),
        "S7": ("existing-channel fingerprint", s7),
        "S8": ("no name collision", lambda: p1b.gate_s8(ctx, be, net, list(rows))),
        "S9": ("clock pre-flight", lambda: p1b.gate_s9(ctx, be, clock_dir, thr)),
        "R4": ("no conflicting experiment process", lambda: gate_r4_no_conflicting_process(be, own_pid)),
        "S10": ("tool and plan pinning (replay)", lambda: gate_s10_replay(ctx, net, out_dir, snapshot_path)),
        "S11": ("output roots absent", lambda: p1b.gate_s11(ctx, list(rows))),
        "R5": ("no reuse of existing evidence", lambda: gate_r5_no_evidence_reuse(ctx, stage_dir, out_dir)),
        "S1": ("replay source snapshot intact", lambda: sn.gate_s1_replay(ctx, be, evidence_text=s1_evidence_text,
                                                                         evidence_snapshot=sn.SNAPSHOT_PATH_WIN if s1_evidence_text is not None else None,
                                                                         allowed=s1_allowed)),
    }
    for gid in GATE_ORDER:
        attempt(gid, *runners[gid])
    results.sort(key=lambda g: GATE_ORDER.index(g.id))
    all_pass = bool(results) and all(g.status == "PASS" for g in results) and sorted(g.id for g in results) == sorted(GATE_ORDER)     # a subset can never start a series
    report = {"tool": f"p4_replay.preflight {PACKAGE_VERSION}", "created_utc": be.now_utc(), "snapshot": sn.SNAPSHOT_ID, "boot_id": be.boot_id(),
              "gates": [g.as_dict() for g in results], "all_pass": all_pass, "evaluated": [g.id for g in results], "required": list(GATE_ORDER),
              "context": info, "thresholds_p1b_unchanged": thr.__dict__}
    p1b.write_json_new(out_dir / "preflight_report.json", report)
    return results, report
