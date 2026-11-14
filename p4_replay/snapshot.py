"""S1 snapshot integration for the replay pipeline (Phase 2). OFFLINE. Nothing here starts a process or writes a file.

The replay pipeline is tied to ONE source snapshot, passed as an explicit parameter and never taken from the frozen P1b `Ctx.default()`
(which names the OLDER second snapshot ARCHIVE_20261112T213119Z and stays untouched):

    ARCHIVE_20261114T113430Z    SHA256SUMS.txt sha256 40303028db1f826a3b1db7199b477f729062fe245dbfface17199da0f681faf9
                                verified and accepted by the owner under the original procedure (create, then verify). IMMUTABLE.

What this module does
  * pins the snapshot identity (id, SHA256SUMS hash, manifest hash, git head, group list) and checks it read-only (`verify_identity`);
  * builds the replay `p1b.Ctx` with `win_snapshot` set to that snapshot, by replacing a field on a new Ctx (`replay_ctx`);
  * builds the S1 verify command from the pinned snapshot and evaluates its output strictly (`gate_s1_replay`): no allowance by default,
    SOURCE_CHANGED and SOURCE_REMOVED are never allowed, and an allowed SOURCE_ADDED path must match a SHA-256 the owner signed;
  * lists the files this work adds to snapshot groups (`added_since_snapshot`), because `repo_p4_replay` is a snapshot group: every file
    added under p4_replay/ after the snapshot is SOURCE_ADDED drift on the launch-day S1 run.

The S1 verify output never names its snapshot, so the gate always BUILDS the command from the pinned path itself; for saved output the
caller must state which snapshot it came from, and a different one fails.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
from pathlib import Path

from . import REPO_ROOT, ensure_paths
from . import driver as dr

ensure_paths()

SNAPSHOT_ID = "ARCHIVE_20261114T113430Z"
SNAPSHOT_ROOT_WIN = r"D:\PLAN_BLOCK18\evidence_archive"
SNAPSHOT_PATH_WIN = SNAPSHOT_ROOT_WIN + "\\" + SNAPSHOT_ID
SNAPSHOT_PATH_WSL = f"/mnt/d/PLAN_BLOCK18/evidence_archive/{SNAPSHOT_ID}"
SNAPSHOT_SUMS_SHA256 = "40303028db1f826a3b1db7199b477f729062fe245dbfface17199da0f681faf9"        # owner-recorded
SNAPSHOT_MANIFEST_SHA256 = "25de62dfc241cb2631cdc1215cc109685d6886350c6a4b6d6b9ac7a41f2dbc0d"    # measured 2026-11-14 (Phase 2 baseline)
SNAPSHOT_GIT_HEAD = "c4c4310a56338b035a9a802faf636c5b74af64c4"
SNAPSHOT_GROUP_COUNT = 26
SNAPSHOT_REQUIRED_GROUPS = ("repo_src", "repo_scripts", "repo_tests", "repo_data", "repo_results", "repo_fabric_testbed", "repo_p4_replay", "wsl_e1")
SECOND_SNAPSHOT_ID = "ARCHIVE_20261112T213119Z"                                                  # the P1b default: must never be the replay baseline
P4_GROUP = "repo_p4_replay"
S1_EXCLUDE_DIRS = frozenset({".git", ".venv", "__pycache__", ".pytest_cache", "node_modules"})    # archive_evidence.py manifest 'exclude_dirs'
SUMMARY_RE = dr.S1_SUMMARY


class SnapshotError(ValueError):
    """The snapshot parameter is not the pinned snapshot."""


def default_snapshot_path() -> Path:
    return Path(SNAPSHOT_PATH_WIN if os.name == "nt" else SNAPSHOT_PATH_WSL)


def _sha256(path: Path) -> str | None:
    p = Path(path)
    if not p.is_file():
        return None
    h = hashlib.sha256()
    with p.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ----------------------------------------------------------------------------------------------- identity (read-only)
def verify_identity(path: Path | None = None, *, expected_sums: str = SNAPSHOT_SUMS_SHA256, expected_manifest: str = SNAPSHOT_MANIFEST_SHA256,
                    expected_head: str = SNAPSHOT_GIT_HEAD) -> dict:
    """Read-only identity check of the snapshot folder: SHA256SUMS.txt and archive_manifest.json hashes, manifest result, git head, group list.
    It does NOT re-hash the 59,000 archived files (that is the S1 verify, about 18 min); it proves the folder is the one the owner accepted."""
    root = Path(path) if path is not None else default_snapshot_path()
    checks: dict[str, bool] = {}
    detail: dict = {"path": str(root), "name_matches_pinned_id": root.name == SNAPSHOT_ID}
    sums, manifest_path = root / "SHA256SUMS.txt", root / "00_manifest" / "archive_manifest.json"
    detail["sums_sha256"], detail["manifest_sha256"] = _sha256(sums), _sha256(manifest_path)
    checks["folder_name_is_pinned_snapshot"] = root.name == SNAPSHOT_ID
    checks["sums_sha256_equals_owner_recorded"] = detail["sums_sha256"] == expected_sums
    checks["manifest_sha256_equals_pinned"] = detail["manifest_sha256"] == expected_manifest
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        manifest = {}
        detail["manifest_error"] = repr(exc)
    names = [g.get("name") for g in manifest.get("groups", [])]
    detail.update(manifest_result=manifest.get("result"), git_head=manifest.get("git_head"), group_count=len(names),
                  problem_files=(manifest.get("totals") or {}).get("problem_files"))
    checks["manifest_result_PASS"] = manifest.get("result") == "PASS" and detail["problem_files"] == 0
    checks["git_head_equals_pinned"] = manifest.get("git_head") == expected_head
    checks["group_count_equals_pinned"] = len(names) == SNAPSHOT_GROUP_COUNT
    checks["required_groups_present"] = all(g in names for g in SNAPSHOT_REQUIRED_GROUPS)
    detail["missing_required_groups"] = [g for g in SNAPSHOT_REQUIRED_GROUPS if g not in names]
    return {"ok": all(checks.values()), "checks": checks, "detail": detail}


# ----------------------------------------------------------------------------------------------- the replay Ctx (snapshot is a parameter)
def replay_ctx(series_name: str, *, snapshot: str = SNAPSHOT_PATH_WIN):
    """A p1b.Ctx for the replay series whose S1 baseline is the pinned third snapshot. A different snapshot, and the P1b default one in
    particular, is refused. `Ctx.default()` is not called and not edited: the Ctx is built by dr.posix_ctx and one field is replaced."""
    if snapshot != SNAPSHOT_PATH_WIN:
        raise SnapshotError(f"snapshot {snapshot!r} is not the pinned replay baseline {SNAPSHOT_PATH_WIN!r}"
                            + (" (it is the P1b default second snapshot)" if SECOND_SNAPSHOT_ID in snapshot else ""))
    return dataclasses.replace(dr.posix_ctx(series_name), win_snapshot=snapshot)


def assert_ctx_snapshot(ctx) -> None:
    if ctx.win_snapshot != SNAPSHOT_PATH_WIN:
        raise SnapshotError(f"Ctx.win_snapshot {ctx.win_snapshot!r} != pinned {SNAPSHOT_PATH_WIN!r}")


# ----------------------------------------------------------------------------------------------- S1 gate (replay variant)
def s1_verify_argv(ctx) -> list[str]:
    """The exact S1 command, built from the pinned snapshot. `--check-sources` also walks the WSL groups, so running it starts WSL access."""
    assert_ctx_snapshot(ctx)
    return [ctx.win_python, ctx.win_repo + r"\scripts\research_audit\archive_evidence.py", "verify", "--snapshot", ctx.win_snapshot, "--check-sources"]


def parse_signed_additions(text: str) -> dict[str, str]:
    """A signed expected-drift list: one line per allowed SOURCE_ADDED path, '<group>/<relative path>  <sha256>'. Blank lines and '#' comments ignored.
    Raises on a malformed line or a repeated path. The owner signs the file; its SHA-256 goes into the approval as s1_evidence_sha256."""
    out: dict[str, str] = {}
    for n, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = re.fullmatch(r"(\S+)\s+([0-9a-fA-F]{64})", line.strip())
        if not m:
            raise SnapshotError(f"signed additions: line {n} is not '<path>  <sha256>'")
        if m.group(1) in out:
            raise SnapshotError(f"signed additions: {m.group(1)!r} repeated on line {n}")
        out[m.group(1)] = m.group(2).lower()
    return out


def bind_allowance(text: str | bytes | None, *, route: str, evidence_sha256: str) -> dict[str, str]:
    """The S1 allowance a caller supplies must be EXACTLY what the owner signed: the route must be SIGNED_DRIFT_UPDATE and the SHA-256 of the list's bytes must equal the approval's
    s1_evidence_sha256. No list means no allowance (every drift event fails). Raises SnapshotError otherwise, before anything starts."""
    if text is None:
        return {}
    data = text if isinstance(text, bytes) else text.encode("utf-8")
    if route != "SIGNED_DRIFT_UPDATE":
        raise SnapshotError(f"an S1 allowance was supplied but the approval's s1_route is {route!r}, not SIGNED_DRIFT_UPDATE")
    if hashlib.sha256(data).hexdigest() != evidence_sha256.lower():
        raise SnapshotError("the S1 allowance list does not hash to the approval's s1_evidence_sha256: it is not what the owner signed")
    return parse_signed_additions(data.decode("utf-8"))


def repo_path_of(entry: str, repo_root: Path = REPO_ROOT) -> Path | None:
    """'repo_p4_replay/x.py' -> <repo>/p4_replay/x.py, for the repo groups that map to one folder."""
    group, _, rel = entry.partition("/")
    folder = {"repo_p4_replay": "p4_replay", "repo_src": "src", "repo_scripts": "scripts", "repo_tests": "tests", "repo_data": "data",
              "repo_results": "experiments/results", "repo_fabric_testbed": "fabric-testbed", "repo_docs": "docs"}.get(group)
    return None if folder is None or not rel else Path(repo_root) / folder / rel


def evaluate_s1_replay(verify_text: str, *, allowed: dict[str, str] | None = None, repo_root: Path = REPO_ROOT) -> dict:
    """Strict S1 evaluation. With no `allowed` map every drift event fails. An allowed path must be SOURCE_ADDED, and the file now on disk must
    still have the SHA-256 the owner signed (closes the hash gap noted in the Phase 1 S1 decision document)."""
    allowed = allowed or {}
    base = dr.evaluate_s1(verify_text, allowed_exact=frozenset(allowed))
    hash_mismatch: list[str] = []
    unverifiable: list[str] = []
    for entry, digest in allowed.items():
        if not any(line.split(None, 1)[:2] == ["SOURCE_ADDED", entry] for line in verify_text.splitlines()):
            continue
        target = repo_path_of(entry, repo_root)
        actual = _sha256(target) if target is not None else None
        if target is None:
            unverifiable.append(entry)
        elif actual != digest:
            hash_mismatch.append(entry)
    ok = base["verdict"] == "PASS" and not hash_mismatch and not unverifiable
    return {**base, "verdict": "PASS" if ok else "FAIL", "allowed_count": len(allowed), "allowed_hash_mismatch": hash_mismatch,
            "allowed_unverifiable": unverifiable}


def gate_s1_replay(ctx, be=None, *, evidence_text: str | None = None, evidence_snapshot: str | None = None, allowed: dict[str, str] | None = None,
                   identity: dict | None = None, repo_root: Path = REPO_ROOT):
    """GateResult S1. Live mode runs the verify command built from the pinned snapshot (needs a backend; LIVE_UNTESTED here). Saved-output mode needs
    `evidence_snapshot` naming the snapshot the output came from, because the output itself does not."""
    p1b = dr.p1b_module()
    rule = (f"{SNAPSHOT_ID}: identity matches the owner-recorded hashes, and archive_evidence.py verify --check-sources reports every archived hash intact "
            f"with source drift limited to owner-signed additions whose SHA-256 still matches ({len(allowed or {})} allowed)")
    try:
        assert_ctx_snapshot(ctx)
    except SnapshotError as exc:
        return p1b.GateResult("S1", "replay source snapshot intact", "FAIL", rule, f"snapshot mismatch: {exc}")
    ident = identity if identity is not None else verify_identity()
    if not ident["ok"]:
        failed = sorted(k for k, v in ident["checks"].items() if not v)
        return p1b.GateResult("S1", "replay source snapshot intact", "FAIL", rule, f"snapshot identity check failed: {failed}", {"identity": ident})
    if evidence_text is not None:
        if evidence_snapshot != SNAPSHOT_PATH_WIN:
            return p1b.GateResult("S1", "replay source snapshot intact", "FAIL", rule,
                                  f"saved verify output claims snapshot {evidence_snapshot!r}, not the pinned {SNAPSHOT_PATH_WIN!r}")
        text, how = evidence_text, f"saved output for {evidence_snapshot}"
    else:
        if be is None:
            return p1b.GateResult("S1", "replay source snapshot intact", "FAIL", rule, "no backend and no saved output")
        r = be.run(s1_verify_argv(ctx), None, 3600)
        text, how = (r.stdout + b"\n" + r.stderr).decode("utf-8", "replace"), f"executed, exit {r.code}"
    result = evaluate_s1_replay(text, allowed=allowed, repo_root=repo_root)
    detail = (f"{how}; checked {result.get('checked')}, mismatched {result.get('mismatched')}, missing {result.get('missing')}, unlisted {result.get('unlisted')}, "
              f"source drift {result.get('source_drift')}; unexpected {result.get('unexpected_count')}; allowed with changed hash {result.get('allowed_hash_mismatch')}")
    return p1b.GateResult("S1", "replay source snapshot intact", result["verdict"], rule, detail, {"result": result, "argv_snapshot": SNAPSHOT_PATH_WIN})


# ----------------------------------------------------------------------------------------------- what this work adds to the snapshot groups
def snapshot_group_files(repo_root: Path = REPO_ROOT, group_dir: str = "p4_replay") -> dict[str, str]:
    """{relative path: sha256} of the files the S1 walk would see under a repo folder (same excluded directory names as the archive manifest)."""
    root = Path(repo_root) / group_dir
    out: dict[str, str] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in S1_EXCLUDE_DIRS]
        for name in filenames:
            p = Path(dirpath) / name
            out[p.relative_to(root).as_posix()] = _sha256(p) or ""
    return dict(sorted(out.items()))


def recorded_group_files(snapshot_path: Path, group: str = P4_GROUP) -> dict[str, str]:
    """{relative path: sha256} the snapshot recorded for a group (00_manifest/<group>.files.tsv, column 0 = path, column 3 = sha256). Read-only."""
    tsv = (Path(snapshot_path) / "00_manifest" / f"{group}.files.tsv").read_text(encoding="utf-8").split("\n")[1:]
    out = {}
    for line in tsv:
        if line:
            cols = line.split("\t")
            out[cols[0]] = cols[3]
    return out


def added_since_snapshot(snapshot_path: Path | None = None, repo_root: Path = REPO_ROOT) -> dict:
    """Files under p4_replay/ now versus the snapshot's record. `added` are the S1 SOURCE_ADDED events this work causes; `changed` and `removed`
    must be empty (a changed Phase 1 file would be SOURCE_CHANGED, which is never allowed)."""
    recorded = recorded_group_files(snapshot_path or default_snapshot_path())
    current = snapshot_group_files(repo_root)
    return {"added": {f"{P4_GROUP}/{k}": v for k, v in current.items() if k not in recorded},
            "changed": sorted(k for k, v in current.items() if k in recorded and recorded[k] != v),
            "removed": sorted(k for k in recorded if k not in current), "recorded_files": len(recorded), "current_files": len(current)}


def signed_additions_text(added: dict[str, str]) -> str:
    """Candidate signed-drift list (PROPOSED: nothing is signed by creating this text)."""
    lines = ["# PROPOSED signed expected-drift list: SOURCE_ADDED paths allowed relative to " + SNAPSHOT_ID,
             "# '<group>/<relative path>  <sha256>'. Only the owner's signature (its SHA-256 in the approval as s1_evidence_sha256) makes it an allowance."]
    lines += [f"{k}  {v}" for k, v in sorted(added.items())]
    return "\n".join(lines) + "\n"


__all__ = ["SNAPSHOT_ID", "SNAPSHOT_PATH_WIN", "SNAPSHOT_PATH_WSL", "SNAPSHOT_SUMS_SHA256", "SNAPSHOT_MANIFEST_SHA256", "SnapshotError", "verify_identity",
           "replay_ctx", "assert_ctx_snapshot", "s1_verify_argv", "parse_signed_additions", "bind_allowance", "evaluate_s1_replay", "gate_s1_replay",
           "added_since_snapshot", "signed_additions_text"]
