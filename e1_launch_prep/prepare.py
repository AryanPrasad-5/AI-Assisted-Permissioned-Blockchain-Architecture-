"""Deterministic generators and read-only verifiers for the mechanical Experiment 1 launch artifacts (master execution, 2026-11-15). OFFLINE.

    python -m e1_launch_prep.prepare manifest --out <new file>                     source manifest of the reviewed set (88 paths), LF, byte-deterministic
    python -m e1_launch_prep.prepare pins     --snapshot <dir> --out <new file>   pins file for a verified snapshot, byte-deterministic (no timestamp)
    python -m e1_launch_prep.prepare drift    --snapshot <dir> --out <new file> --report <new file>
                                                                                   S1 drift of the Windows-side groups; writes the SOURCE_ADDED list only when nothing
                                                                                   was changed or removed (S1 never allows those)
    python -m e1_launch_prep.prepare stage    --stage-root <new dir>               renders the dry-run launch (rp90, D_33000, B=10) offline into a NEW tree
    python -m e1_launch_prep.prepare verify   --source-manifest F --pins F --stage-root D --drift-list F [--snapshot D]
                                                                                   every artifact-level check that needs no owner file; prints the hashes an
                                                                                   entry authorization would name

What this does NOT do: it never writes the series approval or the entry authorization, signs nothing, edits no existing file (every output is create-exclusive), and starts no
process. The owner alone chooses the files and names their hashes. Every generator fails closed: a missing file, an unexpected drift or an existing output is an error.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from p4_replay import driver as dr          # noqa: E402
from p4_replay import launch_ids as li      # noqa: E402
from p4_replay import pins as pn            # noqa: E402
from p4_replay import snapshot as sn        # noqa: E402

STAGE_ARGV = ["e1_launch_prep.prepare", "stage", "--kind", "DRY_RUN"]        # fixed, so the staged tree does not depend on how it was started
NETWORK_WSL = "/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/fabric-testbed/network/network-e1-recovery.json"
ENV_REL = "fabric-testbed/network/env-e1-caliper-benchmark.json"
NETWORK_REL = "fabric-testbed/network/network-e1-recovery.json"
ARCHIVE_MANIFEST_REL = "data/manifests/flashbots_mempool_manifest.yaml"
PROCESSED_REL = "data/processed/flashbots_ms"


class PrepError(ValueError):
    """A preparation step cannot be completed without producing an unsafe or unverifiable artifact."""


def write_new(path: Path, data: bytes) -> str:
    """Create-exclusive. An existing file is never overwritten or appended to. Returns the SHA-256 of what was written."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "xb") as fh:
        fh.write(data)
    return hashlib.sha256(data).hexdigest()


def build_source_manifest_bytes(repo_root: Path = REPO) -> bytes:
    """`<sha256>  <path>` for every reviewed path (the whole p4_replay tree, the frozen P1b files, the invoked tools, the rendering inputs). Sorted, LF, no comment lines."""
    paths = pn.reviewed_source_paths(repo_root)
    return pn.build_source_manifest(repo_root, paths).encode("utf-8")


def build_pins_bytes(snapshot_dir: Path, *, path_win: str | None = None, required_groups: tuple[str, ...] = sn.SNAPSHOT_REQUIRED_GROUPS, note: str | None = None) -> bytes:
    """The pins file for a snapshot folder, read from the folder itself. Refuses a snapshot whose manifest is not PASS with 0 problem files. No timestamp, so the bytes are a
    function of the snapshot alone."""
    snap = Path(snapshot_dir)
    manifest_path, sums_path = snap / "00_manifest" / "archive_manifest.json", snap / "SHA256SUMS.txt"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PrepError(f"cannot read the snapshot manifest: {exc}") from exc
    names = [g.get("name") for g in manifest.get("groups", [])]
    if manifest.get("result") != "PASS" or (manifest.get("totals") or {}).get("problem_files") != 0:
        raise PrepError("the snapshot manifest is not PASS with 0 problem files: refusing to pin it")
    missing = [g for g in required_groups if g not in names]
    if missing:
        raise PrepError(f"the snapshot lacks required groups {missing}")
    doc = {"schema": pn.PIN_SCHEMA,
           "snapshot": {"id": snap.name, "path_win": path_win or str(snap), "sums_sha256": pn.sha256_file(sums_path), "manifest_sha256": pn.sha256_file(manifest_path),
                        "git_head": manifest.get("git_head"), "group_count": len(names), "required_groups": list(required_groups),
                        "manifest_finished_utc": manifest.get("finished_utc")},
           "note": note or f"Names snapshot {snap.name}. Generated by e1_launch_prep.prepare from the snapshot folder itself; not an approval. Its exact bytes are what pins_sha256 names."}
    data = (json.dumps(doc, indent=1) + "\n").encode("utf-8")
    pn.parse_pins(data, expected_sha256=pn.sha256_bytes(data))                  # self-check: the strict parser accepts it
    return data


def _archive_evidence():
    path = REPO / "scripts" / "research_audit" / "archive_evidence.py"
    spec = importlib.util.spec_from_file_location("archive_evidence_for_prep", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["archive_evidence_for_prep"] = module
    spec.loader.exec_module(module)
    return module


def windows_group_drift(snapshot_dir: Path) -> dict:
    """Compare every Windows-side group of a snapshot with the live source, the way `archive_evidence.py verify --check-sources` does (same walk, same hash). WSL groups are
    reported as skipped: use e1_launch_prep.wsl_drift inside the distribution. Read-only."""
    ae = _archive_evidence()
    snap = Path(snapshot_dir)
    manifest = json.loads((snap / "00_manifest" / "archive_manifest.json").read_text(encoding="utf-8"))
    exclude = tuple(manifest["exclude_dirs"])
    groups: dict = {}
    for g in manifest["groups"]:
        name, source = g["name"], str(g["source"])
        if g.get("wsl") or source.startswith("\\\\"):
            groups[name] = {"skipped": "WSL group: use e1_launch_prep.wsl_drift inside the distribution"}
            continue
        recorded: dict[str, str] = {}
        for line in (snap / "00_manifest" / f"{name}.files.tsv").read_text(encoding="utf-8").split("\n")[1:]:
            if line:
                cols = line.split("\t")
                recorded[cols[0]] = cols[3]
        try:
            files, _skipped = ae.walk(Path(source), exclude)
        except OSError as exc:
            groups[name] = {"error": repr(exc), "source": source}
            continue
        current = {rel: ae.scan_file(path)["sha256"] for rel, path in files}
        groups[name] = {"source": source, "recorded_files": len(recorded), "current_files": len(current),
                        "SOURCE_ADDED": {rel: current[rel] for rel in sorted(set(current) - set(recorded))},
                        "SOURCE_REMOVED": sorted(set(recorded) - set(current)),
                        "SOURCE_CHANGED": sorted(rel for rel in set(current) & set(recorded) if current[rel] != recorded[rel])}
    added = {f"{n}/{rel}": h for n, g in groups.items() for rel, h in g.get("SOURCE_ADDED", {}).items()}
    changed = [f"{n}/{rel}" for n, g in groups.items() for rel in g.get("SOURCE_CHANGED", [])]
    removed = [f"{n}/{rel}" for n, g in groups.items() for rel in g.get("SOURCE_REMOVED", [])]
    errors = {n: g["error"] for n, g in groups.items() if "error" in g}
    unmappable = sorted(k for k in added if sn.repo_path_of(k, REPO) is None)
    return {"snapshot": snap.name, "groups": groups, "added": dict(sorted(added.items())), "changed": sorted(changed), "removed": sorted(removed), "errors": errors,
            "unmappable_added": unmappable, "summary": {"windows_groups": sum(1 for g in groups.values() if "skipped" not in g), "wsl_groups_skipped": sum(1 for g in groups.values() if "skipped" in g),
                                                       "added": len(added), "changed": len(changed), "removed": len(removed)}}


def build_drift_list_bytes(drift: dict) -> bytes:
    """The S1 allowance candidate: SOURCE_ADDED paths only, LF. Refuses (fail closed) when any file was changed or removed, when a group could not be read, or when an added path
    belongs to a group the S1 hash check cannot map to a file (it would be 'unverifiable' and fail the gate)."""
    problems = []
    if drift["changed"]:
        problems.append(f"SOURCE_CHANGED {drift['changed'][:5]} (never allowed)")
    if drift["removed"]:
        problems.append(f"SOURCE_REMOVED {drift['removed'][:5]} (never allowed)")
    if drift["errors"]:
        problems.append(f"unreadable groups {sorted(drift['errors'])}")
    if drift["unmappable_added"]:
        problems.append(f"added paths in groups the S1 hash check cannot map: {drift['unmappable_added'][:5]}")
    if problems:
        raise PrepError("; ".join(problems))
    return sn.signed_additions_text(drift["added"]).encode("utf-8")


_DEFAULT = object()


def stage_dry_run(stage_root: Path, *, repo_root: Path = REPO, processed_dir: Path | None = None, archive_manifest=_DEFAULT, env_path: Path | None = None,
                  network_file: Path | None = None, network_wsl: str = NETWORK_WSL) -> dict:
    """Render the dry-run launch (rp90, D_33000, B=10, channel e1-rp90-b10-r0) offline into a NEW tree. Same renderer and layout as the tests use. Starts nothing.
    `archive_manifest` defaults to the repository's manifest (source hashes and transaction identifiers are then read, read-only, from the verified archive); None skips lineage."""
    from p4_replay import render as rd
    repo_root = Path(repo_root)
    env = rd.lr.load_env(Path(env_path) if env_path else repo_root / ENV_REL)
    net_doc = json.loads(Path(network_file or repo_root / NETWORK_REL).read_text(encoding="utf-8"))
    return li.stage_launch(li.DRY, li.dry_rows(), env, network_wsl, Path(processed_dir) if processed_dir else repo_root / PROCESSED_REL, Path(stage_root),
                           archive_manifest=(repo_root / ARCHIVE_MANIFEST_REL if archive_manifest is _DEFAULT else (None if archive_manifest is None else Path(archive_manifest))),
                           argv=list(STAGE_ARGV), network_doc=net_doc)


def verify_artifacts(*, repo_root: Path = REPO, source_manifest: Path, pins: Path, stage_root: Path, drift_list: Path | None, snapshot_path: Path | None = None,
                     live_drift: dict | None = None, entry_file: Path | None = None) -> dict:
    """Read-only. Every check `live_entry.verify_entry` makes about the four mechanical artifacts, plus the S1 drift-list rules, without needing the series approval or the entry
    authorization. `live_drift` (from windows_group_drift) adds the check that the list equals the drift on disk. Returns checks and the hashes an authorization would name."""
    checks: list[tuple[str, bool, str]] = []
    hashes: dict[str, str | None] = {}

    def step(name, fn):
        try:
            value = fn()
        except Exception as exc:                                                         # noqa: BLE001 - every defect is a failed check
            checks.append((name, False, repr(exc)))
            return None
        checks.append((name, True, "ok"))
        return value

    sm_bytes = Path(source_manifest).read_bytes()
    hashes["source_manifest_sha256"] = pn.sha256_bytes(sm_bytes)
    manifest = step("source manifest format", lambda: pn.parse_source_manifest(sm_bytes, expected_sha256=hashes["source_manifest_sha256"]))
    if manifest is not None:
        def exact():
            ver = pn.verify_source_manifest(Path(repo_root), manifest)
            if not ver["ok"]:
                raise PrepError(f"mismatched {ver['mismatched'][:5]}, missing {ver['missing'][:5]}, unlisted {ver['unlisted'][:5]}")
        step("source manifest exact against the tree (nothing unlisted)", exact)

        def covers():
            uncovered = sorted(set(pn.reviewed_source_paths(Path(repo_root))) - set(manifest))
            if uncovered:
                raise PrepError(f"not covered: {uncovered[:5]}")
        step("source manifest covers the reviewed set", covers)
    pins_bytes = Path(pins).read_bytes()
    hashes["pins_sha256"] = pn.sha256_bytes(pins_bytes)
    parsed = step("pins file hash and schema", lambda: pn.parse_pins(pins_bytes, expected_sha256=hashes["pins_sha256"]))
    if parsed is not None:
        def ident():
            res = pn.verify_snapshot_identity(parsed.snapshot, snapshot_path)
            if not res["ok"]:
                raise PrepError(f"failed: {sorted(k for k, v in res['checks'].items() if not v)}")
        step("snapshot identity equals the pin (read-only)", ident)
    stage = Path(stage_root)

    def stage_ok():
        ver = dr.verify_stage(stage)
        if not ver["ok"]:
            raise PrepError(f"stage verify: mismatched/missing {ver['mismatched_or_missing'][:5]}, unlisted {ver['unlisted'][:5]}")
        sm = json.loads((stage / "series_manifest.json").read_text(encoding="utf-8"))
        if sm.get("launch_kind") != li.DRY or sm.get("status") != "STAGED_OFFLINE_NOT_EXECUTED":
            raise PrepError(f"staged tree is {sm.get('launch_kind')!r}/{sm.get('status')!r}, not an unexecuted DRY_RUN")
        if (stage / "replay_matrix.csv").read_bytes() != dr.matrix_csv(li.dry_rows()):
            raise PrepError("replay_matrix.csv is not the dry-run matrix")
        hashes["stage_sums_sha256"] = li.stage_sums_sha256(stage)
    step("staged tree intact, unexecuted, dry-run matrix", stage_ok)
    hashes.setdefault("stage_sums_sha256", None)
    entry = Path(entry_file) if entry_file else REPO / "p4_replay" / "live_entry.py"
    hashes["entry_sha256"] = pn.sha256_file(entry)
    if drift_list is not None:
        raw = Path(drift_list).read_bytes()
        hashes["s1_evidence_sha256"] = pn.sha256_bytes(raw)
        allowed = step("drift list: LF only and well formed", lambda: (_must(b"\r" not in raw, "CR found: a CRLF list fails closed") or sn.parse_signed_additions(raw.decode("utf-8"))))
        if allowed is not None:
            def hashes_match():
                bad = []
                for entry_path, digest in allowed.items():
                    target = sn.repo_path_of(entry_path, Path(repo_root))
                    if target is None or pn.sha256_file(target) != digest:
                        bad.append(entry_path)
                if bad:
                    raise PrepError(f"listed files whose hash differs or cannot be mapped: {bad[:5]}")
            step("drift list: every listed file still has its listed hash", hashes_match)
            if live_drift is not None:
                def equals():
                    if live_drift["changed"] or live_drift["removed"] or live_drift["errors"]:
                        raise PrepError(f"drift on disk is not additions only: changed {live_drift['changed'][:3]}, removed {live_drift['removed'][:3]}, errors {sorted(live_drift['errors'])}")
                    if set(allowed) != set(live_drift["added"]):
                        raise PrepError(f"list differs from disk: only in list {sorted(set(allowed) - set(live_drift['added']))[:5]}, only on disk {sorted(set(live_drift['added']) - set(allowed))[:5]}")
                step("drift list equals the Windows-side drift on disk", equals)
    else:
        hashes["s1_evidence_sha256"] = None
    return {"ok": all(ok for _, ok, _ in checks), "checks": [{"name": n, "ok": ok, "detail": d} for n, ok, d in checks], "hashes": hashes}


def _must(condition: bool, message: str) -> None:
    if not condition:
        raise PrepError(message)


EXTRA_ALLOWLIST = ("e1_launch_prep/__init__.py", "e1_launch_prep/prepare.py", "e1_launch_prep/evidence.py", "e1_launch_prep/wsl_drift.py", "e1_launch_prep/wsl_pylib.py",
                   "e1_launch_prep/prelaunch_gates.py", "e1_launch_prep/tests/__init__.py", "e1_launch_prep/tests/test_prepare.py", "e1_launch_prep/tests/test_evidence.py",
                   "e1_launch_prep/tests/test_wsl_pylib.py", "e1_launch_prep/tests/test_prelaunch_gates.py")


def allowlist_paths(repo_root: Path = REPO, extra: tuple[str, ...] = EXTRA_ALLOWLIST) -> list[str]:
    """The explicit allowlist: the reviewed set (88 paths) plus the named preparation-tool files. No wildcard, no directory, no path outside the repository."""
    out = set(pn.reviewed_source_paths(repo_root))
    for rel in extra:
        parts = Path(rel).parts
        if Path(rel).is_absolute() or ".." in parts or not parts or "\\" in rel:
            raise PrepError(f"allowlist path {rel!r} must be a relative posix path inside the repository")
        if not (Path(repo_root) / rel).is_file():
            raise PrepError(f"allowlist path {rel!r} is not a file")
        out.add(Path(rel).as_posix())
    return sorted(out)


def copy_allowlist(dest: Path, repo_root: Path = REPO, extra: tuple[str, ...] = EXTRA_ALLOWLIST) -> dict:
    """Copy exactly the allowlisted files into a NEW folder, byte for byte, re-hashing each copy, and write SHA256SUMS.txt (LF) beside them. The original tree is only read. An
    existing destination is refused. This is an isolated, hash-recorded copy of the code a launch runs; it is not a Git commit and not an approval."""
    dest, repo_root = Path(dest), Path(repo_root)
    if dest.exists():
        raise FileExistsError(dest)
    paths = allowlist_paths(repo_root, extra)
    lines = []
    for rel in paths:
        data = (repo_root / rel).read_bytes()
        write_new(dest / "tree" / rel, data)
        if pn.sha256_file(dest / "tree" / rel) != pn.sha256_bytes(data):
            raise PrepError(f"copy of {rel} does not hash like its source")
        lines.append(f"{pn.sha256_bytes(data)}  tree/{rel}")
    sums = ("\n".join(lines) + "\n").encode("utf-8")
    write_new(dest / "SHA256SUMS.txt", sums)
    return {"files": len(paths), "sums_sha256": pn.sha256_bytes(sums), "dest": str(dest)}


def verify_allowlist_copy(dest: Path, repo_root: Path = REPO, extra: tuple[str, ...] = EXTRA_ALLOWLIST) -> dict:
    """Read-only. Every listed file in the copy hashes as recorded AND equals the current source file; no file in the copy is unlisted; the copy covers the whole allowlist."""
    dest, repo_root = Path(dest), Path(repo_root)
    listed: dict[str, str] = {}
    for line in (dest / "SHA256SUMS.txt").read_text(encoding="utf-8").splitlines():
        digest, _, rel = line.partition("  ")
        listed[rel] = digest
    bad_copy, drifted = [], []
    for rel, digest in listed.items():
        copy, src = dest / rel, repo_root / rel.removeprefix("tree/")
        if pn.sha256_file(copy) != digest:
            bad_copy.append(rel)
        if pn.sha256_file(src) != digest:
            drifted.append(rel)
    actual = sorted(p.relative_to(dest).as_posix() for p in (dest / "tree").rglob("*") if p.is_file())
    unlisted = sorted(set(actual) - set(listed))
    uncovered = sorted(f"tree/{p}" for p in allowlist_paths(repo_root, extra) if f"tree/{p}" not in listed)
    return {"ok": not (bad_copy or drifted or unlisted or uncovered), "checked": len(listed), "copy_mismatched": bad_copy, "source_changed_since_copy": drifted, "unlisted": unlisted, "uncovered": uncovered}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="e1_launch_prep.prepare", description=__doc__.strip().splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("manifest")
    m.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("pins")
    p.add_argument("--snapshot", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    d = sub.add_parser("drift")
    d.add_argument("--snapshot", type=Path, required=True)
    d.add_argument("--out", type=Path, required=True)
    d.add_argument("--report", type=Path, required=True)
    s = sub.add_parser("stage")
    s.add_argument("--stage-root", type=Path, required=True)
    s.add_argument("--kind", choices=[li.DRY], default=li.DRY)
    c = sub.add_parser("provenance", help="copy the allowlisted source files (88 reviewed paths + the preparation tools) into a NEW folder with SHA256SUMS.txt, or verify such a copy")
    c.add_argument("--dest", type=Path, required=True)
    c.add_argument("--verify", action="store_true", help="verify an existing copy instead of creating one")
    v = sub.add_parser("verify")
    v.add_argument("--source-manifest", type=Path, required=True)
    v.add_argument("--pins", type=Path, required=True)
    v.add_argument("--stage-root", type=Path, required=True)
    v.add_argument("--drift-list", type=Path, required=True)
    v.add_argument("--snapshot", type=Path, help="snapshot folder to read instead of the pinned path")
    v.add_argument("--drift-report", type=Path, help="a new JSON file for the live drift computed during verification")
    args = ap.parse_args(argv)
    try:
        if args.cmd == "manifest":
            data = build_source_manifest_bytes()
            n_paths = data.count(b"\n")
            print(f"wrote {args.out}: {n_paths} paths; sha256 {write_new(args.out, data)}")
        elif args.cmd == "pins":
            data = build_pins_bytes(args.snapshot)
            print(f"wrote {args.out}; sha256 {write_new(args.out, data)}")
        elif args.cmd == "drift":
            drift = windows_group_drift(args.snapshot)
            write_new(args.report, (json.dumps(drift, indent=1) + "\n").encode("utf-8"))
            print(json.dumps(drift["summary"]))
            data = build_drift_list_bytes(drift)
            print(f"wrote {args.out}: {len(drift['added'])} SOURCE_ADDED entries; sha256 {write_new(args.out, data)}")
        elif args.cmd == "provenance":
            if args.verify:
                res = verify_allowlist_copy(args.dest)
                print(json.dumps(res, indent=1))
                return 0 if res["ok"] else 3
            res = copy_allowlist(args.dest)
            print(f"copied {res['files']} allowlisted files -> {args.dest}; SHA256SUMS.txt sha256 {res['sums_sha256']}")
        elif args.cmd == "stage":
            series = stage_dry_run(args.stage_root)
            print(f"staged {sorted(series['runs'])} -> {args.stage_root}; SHA256SUMS.txt sha256 {li.stage_sums_sha256(args.stage_root)} (rendered only; nothing executed)")
        else:
            live = windows_group_drift(args.snapshot) if args.snapshot else windows_group_drift(sn.default_snapshot_path())
            if args.drift_report:
                write_new(args.drift_report, (json.dumps(live, indent=1) + "\n").encode("utf-8"))
            result = verify_artifacts(source_manifest=args.source_manifest, pins=args.pins, stage_root=args.stage_root, drift_list=args.drift_list, snapshot_path=args.snapshot, live_drift=live)
            for c in result["checks"]:
                print(f"{'PASS' if c['ok'] else 'FAIL'}  {c['name']}: {c['detail']}")
            print(json.dumps(result["hashes"], indent=1))
            print("ARTIFACT CHECKS PASS" if result["ok"] else "ARTIFACT CHECKS FAILED")
            return 0 if result["ok"] else 3
    except (OSError, ValueError, KeyError, FileExistsError) as exc:
        print(f"INVALID: {exc!r}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
