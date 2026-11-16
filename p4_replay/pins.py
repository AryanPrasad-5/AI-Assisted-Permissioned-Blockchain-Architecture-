"""External pins: the snapshot identity and the reviewed source hashes, kept OUTSIDE the archived tree (Phase 2C, Package A problem 3). OFFLINE, read-only.

The circularity. Phase 2 `snapshot.py` carries the third snapshot's hashes as constants, and `repo_p4_replay` is a group of every later snapshot, so a snapshot of the final code
would contain a file naming ITS OWN hash: the hash depends on the file, the file on the hash, and the loop has no fixed point. Resolution used here: no Phase 2C module contains a
snapshot id, a snapshot hash or a source hash. They live in two owner-held artifacts that sit outside every snapshot group:
    pins file             JSON: the snapshot's id, path, SHA256SUMS hash, manifest hash, git head, group list, finish time
    source manifest       text: `<sha256>  <path>` for every reviewed source file
and the launch authorization (also outside the tree) names the SHA-256 of each. The code only ever compares a computed hash with a named one. A test scans every Phase 2C module
for snapshot ids and 64-hex literals to keep it that way.

Nothing here edits, creates or verifies a snapshot. `snapshot.py` (Phase 2) and the third snapshot are untouched; the third snapshot's pin can be written into a pins file like any
other. The fourth snapshot does not exist; when it does, its pins file is written AFTER it, from its own SHA256SUMS, and nothing in the repository has to change.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from . import REPO_ROOT, ensure_paths
from . import driver as dr
from . import snapshot as sn

ensure_paths()

PIN_SCHEMA = "p4_replay.pins/1"
ID_RE = re.compile(r"^ARCHIVE_\d{8}T\d{6}Z$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
HEAD_RE = re.compile(r"^[0-9a-f]{40}$")
MUST_HAVE_GROUPS = ("repo_p4_replay", "wsl_e1")
SNAPSHOT_KEYS = ("id", "path_win", "sums_sha256", "manifest_sha256", "git_head", "group_count", "required_groups", "manifest_finished_utc")


class PinError(ValueError):
    """The pins file, the source manifest or a named hash is not what it claims to be."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str | None:
    p = Path(path)
    return sha256_bytes(p.read_bytes()) if p.is_file() else None


@dataclass(frozen=True)
class SnapshotPin:
    id: str
    path_win: str
    sums_sha256: str
    manifest_sha256: str
    git_head: str
    group_count: int
    required_groups: tuple
    manifest_finished_utc: str


@dataclass(frozen=True)
class Pins:
    snapshot: SnapshotPin
    sha256: str
    raw: dict


def parse_pins(data: bytes, *, expected_sha256: str) -> Pins:
    """Strict. The bytes must hash to the named value, carry exactly the schema's keys, and every field must be well formed."""
    got = sha256_bytes(data)
    if got != str(expected_sha256).lower():
        raise PinError(f"the pins file hashes to {got[:16]}..., not the named {str(expected_sha256)[:16]}...")
    try:
        doc = json.loads(data.decode("utf-8"))
    except ValueError as exc:
        raise PinError(f"the pins file is not JSON: {exc}") from exc
    if not isinstance(doc, dict) or doc.get("schema") != PIN_SCHEMA or set(doc) - {"schema", "created_utc", "snapshot", "note"} or "snapshot" not in doc:
        raise PinError(f"the pins file must be a JSON object with schema {PIN_SCHEMA!r} and a 'snapshot' section")
    s = doc["snapshot"]
    if not isinstance(s, dict) or set(s) != set(SNAPSHOT_KEYS):
        raise PinError(f"snapshot section keys must be exactly {list(SNAPSHOT_KEYS)}")
    problems = []
    if not ID_RE.match(str(s["id"])):
        problems.append("id is not ARCHIVE_<yyyymmdd>T<hhmmss>Z")
    if not str(s["path_win"]).endswith("\\" + str(s["id"])):
        problems.append("path_win does not end with the snapshot id")
    for k in ("sums_sha256", "manifest_sha256"):
        if not SHA_RE.match(str(s[k])):
            problems.append(f"{k} is not a lowercase SHA-256")
    if not HEAD_RE.match(str(s["git_head"])):
        problems.append("git_head is not a 40-hex commit id")
    if not isinstance(s["group_count"], int) or isinstance(s["group_count"], bool) or s["group_count"] <= 0:
        problems.append("group_count must be a positive integer")
    groups = s["required_groups"]
    if not isinstance(groups, list) or not all(isinstance(g, str) for g in groups) or any(g not in groups for g in MUST_HAVE_GROUPS):
        problems.append(f"required_groups must be a list of names including {list(MUST_HAVE_GROUPS)}")
    if not re.match(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z$", str(s["manifest_finished_utc"])):
        problems.append("manifest_finished_utc must look like 2026-11-14T12:29:44.011919Z")
    if problems:
        raise PinError("; ".join(problems))
    pin = SnapshotPin(s["id"], s["path_win"], s["sums_sha256"], s["manifest_sha256"], s["git_head"], s["group_count"], tuple(groups), s["manifest_finished_utc"])
    return Pins(pin, got, doc)


def load_pins(path: Path, expected_sha256: str) -> Pins:
    try:
        return parse_pins(Path(path).read_bytes(), expected_sha256=expected_sha256)
    except OSError as exc:
        raise PinError(f"cannot read the pins file: {exc}") from exc


def snapshot_dir(pin: SnapshotPin) -> Path:
    """The snapshot folder as this process sees it (Windows path on Windows, /mnt/... in WSL)."""
    if os.name == "nt":
        return Path(pin.path_win)
    drive, rest = pin.path_win[0].lower(), pin.path_win[2:].replace("\\", "/")
    return Path(f"/mnt/{drive}{rest}")


def verify_snapshot_identity(pin: SnapshotPin, path: Path | None = None) -> dict:
    """Read-only identity check of the snapshot folder against the pin: folder name, SHA256SUMS hash, manifest hash, manifest result PASS with 0 problem files, git head, group count,
    required groups. Does not re-hash the archived files (that is the S1 verify)."""
    root = Path(path) if path is not None else snapshot_dir(pin)
    checks: dict[str, bool] = {}
    detail: dict = {"path": str(root)}
    sums, manifest_path = root / "SHA256SUMS.txt", root / "00_manifest" / "archive_manifest.json"
    detail["sums_sha256"], detail["manifest_sha256"] = sn._sha256(sums), sn._sha256(manifest_path)
    checks["folder_name_is_pinned_snapshot"] = root.name == pin.id
    checks["sums_sha256_equals_pin"] = detail["sums_sha256"] == pin.sums_sha256
    checks["manifest_sha256_equals_pin"] = detail["manifest_sha256"] == pin.manifest_sha256
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        manifest = {}
        detail["manifest_error"] = repr(exc)
    names = [g.get("name") for g in manifest.get("groups", [])]
    detail.update(manifest_result=manifest.get("result"), git_head=manifest.get("git_head"), group_count=len(names), finished_utc=manifest.get("finished_utc"),
                  problem_files=(manifest.get("totals") or {}).get("problem_files"))
    checks["manifest_result_PASS_no_problem_files"] = manifest.get("result") == "PASS" and detail["problem_files"] == 0
    checks["git_head_equals_pin"] = manifest.get("git_head") == pin.git_head
    checks["group_count_equals_pin"] = len(names) == pin.group_count
    checks["required_groups_present"] = all(g in names for g in pin.required_groups)
    checks["finish_time_equals_pin"] = manifest.get("finished_utc") == pin.manifest_finished_utc
    return {"ok": all(checks.values()), "checks": checks, "detail": detail}


# ----------------------------------------------------------------------------------------------- source manifest
def tree_files(repo_root: Path = REPO_ROOT, tree: str = "p4_replay") -> list[str]:
    """Every file the S1 walk would see under a repo folder, as '<tree>/<relative posix path>', same excluded directory names as the archive manifest."""
    root = Path(repo_root) / tree
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in sn.S1_EXCLUDE_DIRS]
        out += [f"{tree}/{(Path(dirpath) / n).relative_to(root).as_posix()}" for n in filenames]
    return sorted(out)


def reviewed_source_paths(repo_root: Path = REPO_ROOT) -> list[str]:
    """The reviewed set: the whole p4_replay tree, the eight frozen P1b files, the tools and workload modules a run invokes, and the rendering inputs."""
    from . import preflight as pf
    p1b = dr.p1b_module()
    paths = set(tree_files(repo_root)) | set(pf.FROZEN_P1B_SHA256) | {f"scripts/e1/{n}" for n in p1b.INVOKED_TOOLS} | set(p1b.PINNED_OTHER)
    paths |= {"fabric-testbed/caliper/workloads/e1_trace_replay.js", "fabric-testbed/network/network-e1-recovery.json", "fabric-testbed/network/env-e1-caliper-benchmark.json"}
    return sorted(paths)


def build_source_manifest(repo_root: Path, rel_paths: list[str]) -> str:
    """'<sha256>  <path>' lines, sorted, LF. Raises PinError on a missing file (a manifest never records an absent file)."""
    lines = []
    for rel in sorted(set(rel_paths)):
        h = sha256_file(Path(repo_root) / rel)
        if h is None:
            raise PinError(f"{rel}: not a file under {repo_root}")
        lines.append(f"{h}  {rel}")
    return "\n".join(lines) + "\n"


def parse_source_manifest(data: bytes, *, expected_sha256: str) -> dict[str, str]:
    got = sha256_bytes(data)
    if got != str(expected_sha256).lower():
        raise PinError(f"the source manifest hashes to {got[:16]}..., not the named {str(expected_sha256)[:16]}...")
    out: dict[str, str] = {}
    for n, line in enumerate(data.decode("utf-8").splitlines(), start=1):
        if not line.strip() or line.startswith("#"):
            continue
        m = re.fullmatch(r"([0-9a-f]{64})  (\S.*)", line)
        if not m:
            raise PinError(f"source manifest line {n} is not '<sha256>  <path>'")
        if m.group(2) in out:
            raise PinError(f"source manifest repeats {m.group(2)!r}")
        out[m.group(2)] = m.group(1)
    if not out:
        raise PinError("the source manifest lists no file")
    return out


def verify_source_manifest(repo_root: Path, manifest: dict[str, str], *, tree: str = "p4_replay") -> dict:
    """Exact: every listed file exists with the listed hash, and no file under `tree` is unlisted. A file added to p4_replay after the manifest was made fails."""
    mismatched, missing = [], []
    for rel, digest in manifest.items():
        h = sha256_file(Path(repo_root) / rel)
        if h is None:
            missing.append(rel)
        elif h != digest:
            mismatched.append(rel)
    unlisted = [f for f in tree_files(repo_root, tree) if f not in manifest]
    return {"ok": not (mismatched or missing or unlisted), "checked": len(manifest), "mismatched": mismatched, "missing": missing, "unlisted": unlisted}
