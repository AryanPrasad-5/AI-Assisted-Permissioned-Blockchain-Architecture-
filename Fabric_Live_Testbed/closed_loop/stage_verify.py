"""Stdlib re-verification of a stage tree (used by the WSL runner; stage.py itself needs numpy)."""

from __future__ import annotations

import hashlib
from pathlib import Path


def verify_stage_tree(stage_root: Path) -> str:
    """Re-hash every file listed in SHA256SUMS.txt, refuse unlisted files. Returns the SHA-256 of SHA256SUMS.txt."""
    stage_root = Path(stage_root)
    body = (stage_root / "SHA256SUMS.txt").read_bytes()
    listed = {}
    for line in body.decode("ascii").splitlines():
        digest, name = line.split("  ", 1)
        if name in listed:
            raise ValueError(f"{name} listed twice")
        listed[name] = digest
    present = {p.relative_to(stage_root).as_posix() for p in stage_root.rglob("*") if p.is_file()} - {"SHA256SUMS.txt"}
    if present != set(listed):
        raise ValueError(f"stage files differ from SHA256SUMS.txt: extra {sorted(present - set(listed))[:5]}, "
                         f"missing {sorted(set(listed) - present)[:5]}")
    for name, digest in listed.items():
        if hashlib.sha256((stage_root / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"{name}: SHA-256 mismatch")
    return hashlib.sha256(body).hexdigest()
