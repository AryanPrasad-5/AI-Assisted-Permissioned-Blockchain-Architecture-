"""The live entry point for a launch (Phase 2C, Package A problem 2). ENABLED by the reviewed edit N9 of 2026-11-15 (the owner's master execution authorization for Experiment 1). No launch has ever been started by this file.
`launch` still refuses unless EVERY check below passes, and the owner-written series approval and entry authorization (which name the SHA-256 of this enabled file) do not come from this build.

Architecture: a new derived entry, because Phase 1 `driver.py` (frozen in the S1 snapshot) has no live entry and its `run` command is refused. This file does NOT edit `driver.py`
and does NOT turn live execution on. It is the one place that COULD, later, and only through all of these in this order:

   1. `LIVE_ENTRY_ENABLED` is True. It was False until edit N9 (2026-11-15). Changing it is a reviewed edit of THIS file under an owner authorization; no flag, argument or environment
      variable reaches it. `launch()` checks it as its FIRST statement, before it reads a file or builds a backend.
   2. The owner's ENTRY AUTHORIZATION (launch_approval.check_entry_authorization) names the SHA-256 of THIS file, so an authorization covers the reviewed code and nothing else.
      It also names the series approval, the pins file, the source manifest, the staged schedules and (measured launch) the dry-run summary; each is re-hashed here.
   3. `verify_entry` passes EVERY check: authorization well formed; entry hash; approval valid for the kind; pins file hash; snapshot identity (read-only); the source manifest exact
      against the tree, with nothing unlisted; the staged tree intact and for this kind; the matrix the seed generates equals the staged one; the S1 drift list is the signed one;
      the dry-run summary is the named one. A single failed check refuses the launch. A failed approval stops execution before any backend exists.
   4. The backend is built HERE (`_construct_live_backend`), never passed in, and must be the exact `p1b.Backend` class: a fake, a subclass, a look-alike or an undeclared
      object is refused. The orchestrator re-checks its mode (launch_series.require_backend_for_mode).
   5. `driver.LIVE_EXECUTION_ENABLED` is set True only inside `_live_execution_enabled()`, for the duration of `run_launch`, and restored in a `finally`. This context manager is the only
      assignment of that flag anywhere in the package (a test pins it). It is the single point the owner should review before enabling.
   6. `run_launch` runs all seventeen gates (launch_series.GATE_ORDER_V2). There is no argument that skips, reorders or subsets them.

Commands: `status` (what is enabled), `plan` (identifiers of a launch kind), `check` (steps 3 above, read-only), `budget`. `launch` is refused while LIVE_ENTRY_ENABLED is False.
`check` reads and hashes files and reads the snapshot folder's manifest; it starts nothing.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePath

from . import REPO_ROOT, ensure_paths
from . import driver as dr
from . import duration_model as dm
from . import launch_approval as la
from . import launch_ids as li
from . import launch_series as ls
from . import pins as pn

ensure_paths()
sys.dont_write_bytecode = True

LIVE_ENTRY_ENABLED = True           # edit N9, 2026-11-15: a reviewed change of this file under the owner's authorisation; never a flag (it was False before; see the pre-N9 hash in the execution record)


class LiveEntryRefused(dr.LiveExecutionRefused):
    """The live entry refused to start."""


@dataclass
class EntryPlan:
    ok: bool
    checks: list = field(default_factory=list)            # (name, ok, detail)
    auth: la.EntryAuthorization | None = None
    approval: la.LaunchApproval | None = None
    pins: pn.Pins | None = None
    source_manifest: dict | None = None
    rows: list | None = None
    stage_dir: Path | None = None
    s1_text: str | None = None
    artifact_hashes: dict = field(default_factory=dict)

    def failed(self) -> list[str]:
        return [f"{n}: {d}" for n, ok, d in self.checks if not ok]


def _read(path: Path | None) -> bytes:
    if path is None:
        raise FileNotFoundError("a required file was not given")
    return Path(path).read_bytes()


def verify_entry(*, authorization: Path, approval: Path, pins: Path, source_manifest: Path, prereg: Path, stage_root: Path, repo_root: Path = REPO_ROOT,
                 entry_file: Path | None = None, s1_list: Path | None = None, dry_summary: Path | None = None, snapshot_path: Path | None = None) -> EntryPlan:
    """Every pre-start check that needs no backend. Read-only. Collects all failures (it does not stop at the first) unless a file needed by the rest cannot be parsed."""
    plan = EntryPlan(ok=False)

    def step(name: str, fn):
        """Run fn and record exactly ONE check for it. Returns fn's value, or None if it failed. ApprovalRefused is a SystemExit: every defect is a failed check, none escapes."""
        try:
            out = fn()
        except (Exception, SystemExit) as exc:                                            # noqa: BLE001
            plan.checks.append((name, False, str(exc) if isinstance(exc, SystemExit) else repr(exc)))
            return None
        plan.checks.append((name, True, "ok"))
        return out

    auth_bytes = step("authorization file readable", lambda: _read(authorization))
    if auth_bytes is None:
        return plan
    plan.auth = step("authorization well formed", lambda: la.check_entry_authorization(auth_bytes))
    if plan.auth is None:
        return plan
    auth = plan.auth
    plan.artifact_hashes["authorization_sha256"] = pn.sha256_bytes(auth_bytes)
    entry_sha = pn.sha256_file(Path(entry_file) if entry_file is not None else Path(__file__))
    plan.artifact_hashes["entry_sha256"] = entry_sha
    step("entry module hash equals the authorization's entry_sha256",
         lambda: _must(entry_sha == auth.entry_sha256, f"live_entry.py hashes to {str(entry_sha)[:16]}..., the authorization names {auth.entry_sha256[:16]}...: it covers other code"))
    approval_bytes = step("series approval file readable", lambda: _read(approval))
    prereg_sha = step("preregistration file readable", lambda: _must(pn.sha256_file(prereg) is not None, f"{prereg} is not a file") or pn.sha256_file(prereg))
    if approval_bytes is not None and prereg_sha is not None:
        plan.approval = step("series approval valid for this launch kind", lambda: la.check_launch_approval(approval_bytes, expected_kind=auth.kind, prereg_sha256=prereg_sha))
    if plan.approval is not None:
        step("series approval hash equals the authorization's",
             lambda: _must(plan.approval.sha256 == auth.series_approval_sha256, "the series approval does not hash to the authorization's series_approval_sha256"))
        plan.artifact_hashes.update(series_approval_sha256=plan.approval.sha256, prereg_sha256=prereg_sha)
    plan.pins = step("pins file hash and schema", lambda: pn.parse_pins(_read(pins), expected_sha256=auth.pins_sha256))
    if plan.pins is not None:
        step("snapshot identity equals the pin (read-only)", lambda: _identity(plan.pins.snapshot, snapshot_path))
    plan.source_manifest = step("source manifest hash and format", lambda: pn.parse_source_manifest(_read(source_manifest), expected_sha256=auth.source_manifest_sha256))
    if plan.source_manifest is not None:
        plan.artifact_hashes["source_manifest_sha256"] = auth.source_manifest_sha256
        step("source manifest exact against the tree (nothing unlisted)", lambda: _manifest_exact(Path(repo_root), plan.source_manifest))
        step("source manifest covers the reviewed set", lambda: _covers(Path(repo_root), plan.source_manifest))
    stage = Path(stage_root)
    plan.stage_dir = stage
    step("staged tree intact and equal to the authorization", lambda: _stage_ok(stage, auth))
    if plan.approval is not None:
        plan.rows = step("launch identifiers generated from the approval", lambda: li.rows_for(auth.kind, plan.approval.run_order_seed))
    if plan.rows is not None:
        step("staged matrix equals this launch's matrix", lambda: _must((stage / "replay_matrix.csv").read_bytes() == dr.matrix_csv(plan.rows), "replay_matrix.csv differs from the matrix this approval generates"))
    step("S1 drift list equals the authorization's", lambda: _s1(plan, auth, s1_list))
    step("dry-run summary equals the authorization's", lambda: _dry(auth, dry_summary))
    plan.ok = all(ok for _, ok, _ in plan.checks)
    return plan


def _must(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def _identity(pin, snapshot_path):
    ident = pn.verify_snapshot_identity(pin, snapshot_path)
    if not ident["ok"]:
        raise ValueError(f"failed: {sorted(k for k, v in ident['checks'].items() if not v)}")


def _manifest_exact(repo: Path, manifest: dict) -> None:
    ver = pn.verify_source_manifest(repo, manifest)
    if not ver["ok"]:
        raise ValueError(f"mismatched {ver['mismatched'][:5]}, missing {ver['missing'][:5]}, unlisted {ver['unlisted'][:5]}")


def _covers(repo: Path, manifest: dict) -> None:
    uncovered = sorted(set(pn.reviewed_source_paths(repo)) - set(manifest))
    if uncovered:
        raise ValueError(f"not covered: {uncovered[:5]}")


def _stage_ok(stage: Path, auth: la.EntryAuthorization) -> None:
    ver = dr.verify_stage(stage)
    if not ver["ok"]:
        raise ValueError(f"stage verify: mismatched/missing {ver['mismatched_or_missing'][:5]}, unlisted {ver['unlisted'][:5]}")
    if li.stage_sums_sha256(stage) != auth.stage_sums_sha256:
        raise ValueError("the staged tree's SHA256SUMS.txt does not hash to the authorization's stage_sums_sha256")
    manifest = json.loads((stage / "series_manifest.json").read_text(encoding="utf-8"))
    if manifest.get("launch_kind") != auth.kind or manifest.get("status") != "STAGED_OFFLINE_NOT_EXECUTED":
        raise ValueError(f"staged tree is {manifest.get('launch_kind')!r}/{manifest.get('status')!r}, not an unexecuted {auth.kind}")


def _s1(plan: EntryPlan, auth: la.EntryAuthorization, s1_list: Path | None) -> None:
    text = None if s1_list is None else Path(s1_list).read_bytes()
    plan.s1_text = None if text is None else text.decode("utf-8")
    ls.bind_s1_allowance(auth, text)


def _dry(auth: la.EntryAuthorization, dry_summary: Path | None) -> None:
    if auth.kind == li.DRY:
        if dry_summary is not None:
            raise ValueError("a dry-run launch takes no dry-run summary")
        return
    if dry_summary is None or pn.sha256_file(dry_summary) != auth.dry_run_summary_sha256:
        raise ValueError("the dry-run summary is missing or does not hash to the authorization's dry_run_summary_sha256")


# ----------------------------------------------------------------------------------------------- the only place a real backend is built and the flag is touched
def _construct_live_backend():
    """The ONLY construction of a real backend. Never called while LIVE_ENTRY_ENABLED is False (launch() refuses first)."""
    return dr.p1b_module().Backend()


@contextlib.contextmanager
def _live_execution_enabled():
    """The ONLY assignment of driver.LIVE_EXECUTION_ENABLED in the package. Scoped to one launch and restored in a finally."""
    previous = dr.LIVE_EXECUTION_ENABLED
    dr.LIVE_EXECUTION_ENABLED = True
    try:
        yield
    finally:
        dr.LIVE_EXECUTION_ENABLED = previous


def _live_ctx(launch_id: str, win_snapshot: str):
    """The context of a live launch (edit N9b, 2026-11-15). `driver.posix_ctx` holds PurePosixPath fields so an offline listing keeps forward slashes on any host, but a real launch
    runs the frozen P1b gates and the orchestrator, which call exists(), mkdir() and read_text() on them (gate S11 raised AttributeError on the pure path, which would have failed the
    pre-flight and consumed the launch id). Every path field becomes a concrete pathlib.Path, as `Ctx.default()` of P1b already builds it: a PosixPath inside WSL, the only place a
    live launch can run."""
    ctx = dataclasses.replace(dr.posix_ctx(launch_id), win_snapshot=win_snapshot)
    return dataclasses.replace(ctx, **{f.name: Path(str(getattr(ctx, f.name))) for f in dataclasses.fields(ctx) if isinstance(getattr(ctx, f.name), PurePath)})


def _load_env_doc(ctx) -> dict:
    return json.loads(Path(ctx.env).read_text(encoding="utf-8"))


def _require_exact_live_backend(be) -> None:
    p1b = dr.p1b_module()
    if getattr(be, "OFFLINE_FAKE", False) is True or type(be) is not p1b.Backend:
        raise LiveEntryRefused(f"refused: the live entry accepts only the exact p1b.Backend class, got {type(be).__name__}")


def launch(*, authorization: Path, approval: Path, pins: Path, source_manifest: Path, prereg: Path, stage_root: Path, clock_dir: Path, repo_root: Path = REPO_ROOT,
           s1_list: Path | None = None, dry_summary: Path | None = None, snapshot_path: Path | None = None) -> dict:
    """Start one launch. Refused (before any file is read) while LIVE_ENTRY_ENABLED is False."""
    if not LIVE_ENTRY_ENABLED:
        raise LiveEntryRefused("refused: the live entry is disabled (LIVE_ENTRY_ENABLED is False). Enabling it is a reviewed change of live_entry.py under a separate owner authorisation.")
    plan = verify_entry(authorization=authorization, approval=approval, pins=pins, source_manifest=source_manifest, prereg=prereg, stage_root=stage_root, repo_root=repo_root,
                        s1_list=s1_list, dry_summary=dry_summary, snapshot_path=snapshot_path)
    if not plan.ok:
        raise LiveEntryRefused("refused: " + "; ".join(plan.failed()))
    p1b = dr.p1b_module()
    be = _construct_live_backend()                  # built only after every check above passed, and only here
    _require_exact_live_backend(be)
    ctx = _live_ctx(plan.auth.launch_id, plan.pins.snapshot.path_win)
    env_doc = _load_env_doc(ctx)
    preflight_dir = ctx.root.parent / f"{plan.auth.launch_id}_preflight"
    with _live_execution_enabled():
        session = p1b.Session(ctx=ctx, be=be, thr=dr.replay_thresholds())
        return ls.run_launch(session, plan.rows, plan.stage_dir, plan.approval, plan.auth, plan.pins, plan.source_manifest, mode=ls.MODE_LIVE, own_pid=os.getpid(), clock_dir=Path(clock_dir),
                             env_doc=env_doc, preflight_dir=preflight_dir, helpers=None, s1_allowed_text=plan.s1_text, dry_summary_path=dry_summary, snapshot_path=snapshot_path,
                             node=True, series_root_prefix=dr.SERIES_ROOT_WSL, artifact_hashes=plan.artifact_hashes)


# ----------------------------------------------------------------------------------------------- CLI
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="p4_replay.live_entry", description="Live entry for a launch. `launch` runs only when every artifact check passes (the exit code of `launch` carries no status: read series_summary.json). `check`, `plan`, `status` and `budget` are read-only.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status", help="print what is enabled")
    pl = sub.add_parser("plan", help="print the identifiers of a launch kind (offline)")
    pl.add_argument("--kind", choices=li.KINDS, required=True)
    pl.add_argument("--seed", type=int, help="measured only: the owner's run-order seed")
    bd = sub.add_parser("budget", help="print the keep-awake duration budget of a staged launch (offline)")
    bd.add_argument("--stage-root", type=Path, required=True)
    bd.add_argument("--caliper-timeout", type=float, default=dr.CALIPER_TIMEOUT_S_PROPOSED)
    for name in ("check", "launch"):
        c = sub.add_parser(name, help="read-only verification of every artifact" if name == "check" else "start one launch: refused unless every artifact check passes, and refused while LIVE_ENTRY_ENABLED is False")
        for flag in ("authorization", "approval", "pins", "source-manifest", "prereg", "stage-root"):
            c.add_argument(f"--{flag}", type=Path, required=True)
        c.add_argument("--s1-list", type=Path)
        c.add_argument("--dry-summary", type=Path)
        c.add_argument("--snapshot-path", type=Path, help="read the snapshot folder from here instead of the pinned path (offline use)")
        if name == "launch":
            c.add_argument("--clock-dir", type=Path, required=True)
    args = ap.parse_args(argv)
    try:
        if args.cmd == "status":
            print(json.dumps({"LIVE_ENTRY_ENABLED": LIVE_ENTRY_ENABLED, "driver.LIVE_EXECUTION_ENABLED": dr.LIVE_EXECUTION_ENABLED, "gates": list(ls.GATE_ORDER_V2),
                              "dry_run_ids": [li.DRY_RUN_ID], "measured_run_ids": list(li.MEASURED_RUN_IDS), "retired_run_ids": list(li.RETIRED_RUN_IDS)}, indent=2))
            return 0
        if args.cmd == "plan":
            rows = li.rows_for(args.kind, args.seed)
            sys.stdout.write(dr.matrix_csv(rows).decode("ascii"))
            return 0
        if args.cmd == "budget":
            manifest = json.loads((args.stage_root / "series_manifest.json").read_text(encoding="utf-8"))
            runs = [(int(v["row"][6]), v["tx_total"]) for v in manifest["runs"].values()]
            b = dm.build_budget(manifest["launch_kind"], runs, caliper_timeout_s=args.caliper_timeout, gate_ids=ls.GATE_ORDER_V2)
            print(json.dumps({"total_s": b.total_s, "preflight_s": b.preflight_s, "probe_s": b.probe_s, "components": b.table()}, indent=2))
            return 0
        kw = dict(authorization=args.authorization, approval=args.approval, pins=args.pins, source_manifest=args.source_manifest, prereg=args.prereg, stage_root=args.stage_root,
                  s1_list=args.s1_list, dry_summary=args.dry_summary, snapshot_path=args.snapshot_path)
        if args.cmd == "launch":
            launch(clock_dir=args.clock_dir, **kw)
            return 0
        plan = verify_entry(**kw)
        for name, ok, detail in plan.checks:
            print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
        print("ENTRY CHECKS PASS" if plan.ok else "ENTRY CHECKS FAILED")
        return 0 if plan.ok else 3
    except LiveEntryRefused as exc:
        print(exc)
        return 4
    except (OSError, ValueError, KeyError) as exc:
        print(f"INVALID: {exc!r}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
