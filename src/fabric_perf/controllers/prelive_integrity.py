"""Candidate integrity manifest for the proposed pre-live validation package (task A0e, 2026-11-21).

A candidate manifest lists the files a future validation run would rely on, with their SHA-256, purpose, role and version.
It is evidence of what the files were on the day it was written. It is NOT an approval and NOT a freeze:

* ``status`` is always ``CANDIDATE_NOT_APPROVED`` and ``approved`` is always ``false`` here. Approval is a separate owner
  record that this module neither writes nor reads.
* It never modifies a historical manifest and never re-pins a historical file. The recovery-evidence manifest
  (``SHA256SUMS.txt``) can only be VERIFIED here (``verify_sha256sums``), never written.
* Files are only read. A manifest is written exclusively to a path that does not exist.

Pure standard library; no process, network or container API.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, NamedTuple

MANIFEST_SCHEMA = "prelive_candidate_manifest/1"
STATUS_CANDIDATE = "CANDIDATE_NOT_APPROVED"

# Statuses of one entry. None of them is an approval.
PENDING_OWNER_APPROVAL = "PENDING_OWNER_APPROVAL"
PINNED_ELSEWHERE_UNCHANGED = "PINNED_ELSEWHERE_UNCHANGED"        # a hash already pinned by an earlier, separate record
EXCLUDED_NOT_APPROVED_ENTRY_POINT = "EXCLUDED_NOT_APPROVED_ENTRY_POINT"


class FileSpec(NamedTuple):
    path: str            # repository-relative, POSIX
    purpose: str
    role: str
    approval_status: str = PENDING_OWNER_APPROVAL
    pinned_sha256: str | None = None


class IntegrityFinding(NamedTuple):
    code: str
    path: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "path": self.path, "message": self.message}


_VERSION_PATTERNS = (
    re.compile(r"""^(?:TOOL_VERSION|POLICY_VERSION|CONTRACT_VERSION|CLOCK_PREFLIGHT_VERSION|MANIFEST_SCHEMA)\s*=\s*['"]([^'"]+)['"]""", re.M),
    re.compile(r"""^\s*(?:version)\s*[:=]\s*['"]([^'"]+)['"]""", re.M),
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def declared_version(path: Path) -> str:
    """The version or schema identifier the file declares about itself, else ``unversioned`` (never invented)."""
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "unreadable"
    for pattern in _VERSION_PATTERNS:
        found = pattern.search(text)
        if found:
            return found.group(1)
    return "unversioned"


def read_git_head(repo_root: Path) -> dict[str, Any]:
    """Branch and commit, read from ``.git`` files only (no git process). Unresolvable parts are ``None``."""
    git = Path(repo_root) / ".git"
    out: dict[str, Any] = {"branch": None, "commit": None}
    try:
        head = (git / "HEAD").read_text(encoding="utf-8").strip()
    except OSError:
        return out
    if head.startswith("ref:"):
        ref = head.split(":", 1)[1].strip()
        out["branch"] = ref.removeprefix("refs/heads/")
        try:
            out["commit"] = (git / ref).read_text(encoding="utf-8").strip()
        except OSError:
            try:
                for line in (git / "packed-refs").read_text(encoding="utf-8").splitlines():
                    if line.endswith(" " + ref):
                        out["commit"] = line.split(" ", 1)[0]
            except OSError:
                pass
    else:
        out["commit"] = head
    return out


def build_candidate_manifest(repo_root: Path, specs: Iterable[FileSpec], *, created_utc: str | None = None,
                             launch_configuration: Mapping[str, Any] | None = None,
                             experimental_configuration_provenance: Mapping[str, Any] | None = None,
                             open_items: Iterable[str] = ()) -> dict[str, Any]:
    """Hash every listed file. A missing file is an error: a candidate manifest never lists what it cannot hash."""
    root = Path(repo_root)
    entries, seen = [], set()
    for spec in specs:
        if spec.path in seen:
            raise ValueError(f"duplicate manifest entry {spec.path}")
        seen.add(spec.path)
        target = root / spec.path
        if not target.is_file():
            raise FileNotFoundError(f"cannot hash {spec.path}: not a file under {root}")
        digest = sha256_file(target)
        if spec.pinned_sha256 is not None and digest != spec.pinned_sha256:
            raise ValueError(f"{spec.path} differs from the hash pinned by an earlier record "
                             f"({digest} != {spec.pinned_sha256}); refusing to list it as unchanged")
        entries.append({"path": spec.path, "sha256": digest, "size_bytes": target.stat().st_size, "purpose": spec.purpose,
                        "dependency_role": spec.role, "version_or_schema": declared_version(target),
                        "approval_status": spec.approval_status, "pinned_sha256": spec.pinned_sha256})
    return {
        "schema": MANIFEST_SCHEMA, "status": STATUS_CANDIDATE, "approved": False, "approval_record": None,
        "approval_note": "A hash listed here is the hash on the day it was written. Nothing in this file is an approval or a freeze; "
                         "owner approval is a separate record.",
        "created_utc": created_utc or datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git": read_git_head(root), "entries": entries,
        "launch_configuration": dict(launch_configuration or {}),
        "experimental_configuration_provenance": dict(experimental_configuration_provenance or {}),
        "open_items": list(open_items)}


def write_new_json(path: Path, document: Mapping[str, Any]) -> str:
    """Write ``document`` to a path that must not exist. Returns its SHA-256."""
    target = Path(path)
    data = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with target.open("xb") as fh:
        fh.write(data)
    return hashlib.sha256(data).hexdigest()


def verify_candidate_manifest(manifest: Mapping[str, Any], repo_root: Path) -> list[IntegrityFinding]:
    """Compare every entry with the file on disk. An empty list means every listed file is byte-identical to the manifest."""
    findings: list[IntegrityFinding] = []
    if manifest.get("schema") != MANIFEST_SCHEMA:
        findings.append(IntegrityFinding("schema", "<manifest>", f"schema is {manifest.get('schema')!r}, expected {MANIFEST_SCHEMA!r}"))
    if manifest.get("approved") is not False or manifest.get("status") != STATUS_CANDIDATE:
        findings.append(IntegrityFinding("status", "<manifest>", "a candidate manifest must say CANDIDATE_NOT_APPROVED and approved=false"))
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        return findings + [IntegrityFinding("no_entries", "<manifest>", "the manifest lists no files")]
    seen = set()
    for entry in entries:
        path = str(entry.get("path"))
        if path in seen:
            findings.append(IntegrityFinding("duplicate", path, "listed twice"))
        seen.add(path)
        target = Path(repo_root) / path
        if not target.is_file():
            findings.append(IntegrityFinding("missing", path, "listed in the manifest but not present"))
            continue
        actual = sha256_file(target)
        if actual != entry.get("sha256"):
            findings.append(IntegrityFinding("modified", path, f"sha256 {actual} differs from the manifest's {entry.get('sha256')}"))
        pinned = entry.get("pinned_sha256")
        if pinned is not None and actual != pinned:
            findings.append(IntegrityFinding("pin_violation", path, f"sha256 {actual} differs from the earlier pinned value {pinned}"))
    return findings


def verify_sha256sums(sums_file: Path, base_dir: Path | None = None) -> dict[str, Any]:
    """Verify a ``sha256sum``-format file WITHOUT writing anything. Returns counts and the list of failures."""
    sums = Path(sums_file)
    base = Path(base_dir) if base_dir is not None else sums.parent
    ok, failed, missing = 0, [], []
    for line in sums.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        parts = line.split(None, 1)                 # "<hash> <name>", "<hash>  <name>" or "<hash> *<name>" (binary marker)
        if len(parts) != 2:
            missing.append(line.strip())
            continue
        digest, name = parts[0], parts[1].strip()
        if name.startswith("*"):
            name = name[1:]
        target = base / name
        if not target.is_file():
            missing.append(name)
        elif sha256_file(target) == digest.strip().lower():
            ok += 1
        else:
            failed.append(name)
    return {"ok": ok, "failed": failed, "missing": missing, "listed": ok + len(failed) + len(missing)}
