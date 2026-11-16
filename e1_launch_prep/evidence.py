"""Evidence integrity manifest (standard library only, so it also runs under WSL's python3). Read-only over the evidence, create-exclusive over its output.

    python3 evidence.py --root series=/home/mailf18/p4_replay_series/p4r-dry-01 --root caliper=/home/mailf18/e1/caliper-ws-benchmark/runs/rp90 --out SHA256SUMS.txt

Each line is `<sha256>  <label>/<relative posix path>`, sorted, LF. Symbolic links are listed as an error, never followed. The manifest never records its own hash: report
that outside the file. `verify` re-hashes and reports MISMATCH / MISSING / UNLISTED, so a later change to preserved evidence is detected.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path

CHUNK = 1 << 20


class EvidenceError(ValueError):
    pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def walk_files(root: Path) -> list[str]:
    """Relative posix paths of every regular file under root. A symbolic link or a non-regular entry is an error (nothing is silently skipped)."""
    root = Path(root)
    if not root.is_dir():
        raise EvidenceError(f"{root} is not a directory")
    out: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in list(dirnames) + list(filenames):
            full = Path(dirpath) / name
            if full.is_symlink():
                raise EvidenceError(f"symbolic link in evidence: {full}")
        for name in filenames:
            full = Path(dirpath) / name
            if not full.is_file():
                raise EvidenceError(f"not a regular file: {full}")
            out.append(full.relative_to(root).as_posix())
    return sorted(out)


def build_manifest(roots: dict[str, Path]) -> str:
    """`<sha256>  <label>/<relative path>` for every file of every labelled root. Labels must be distinct and contain no slash."""
    lines: list[str] = []
    for label, root in roots.items():
        if not label or "/" in label or label.startswith("#"):
            raise EvidenceError(f"bad label {label!r}")
        for rel in walk_files(Path(root)):
            lines.append(f"{sha256_file(Path(root) / rel)}  {label}/{rel}")
    if not lines:
        raise EvidenceError("no evidence file found")
    return "\n".join(sorted(lines, key=lambda s: s.split("  ", 1)[1])) + "\n"


def verify_manifest(text: str, roots: dict[str, Path]) -> dict:
    listed: dict[str, str] = {}
    for n, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.startswith("#"):
            continue
        digest, sep, rel = line.partition("  ")
        if not sep or len(digest) != 64:
            raise EvidenceError(f"manifest line {n} is not '<sha256>  <path>'")
        listed[rel] = digest
    mismatched, missing, current = [], [], set()
    for rel, digest in listed.items():
        label, _, sub = rel.partition("/")
        root = roots.get(label)
        target = None if root is None else Path(root) / sub
        if target is None or not target.is_file():
            missing.append(rel)
        elif sha256_file(target) != digest:
            mismatched.append(rel)
    for label, root in roots.items():
        current |= {f"{label}/{r}" for r in walk_files(Path(root))}
    unlisted = sorted(current - set(listed))
    return {"ok": not (mismatched or missing or unlisted), "checked": len(listed), "mismatched": sorted(mismatched), "missing": sorted(missing), "unlisted": unlisted}


def write_new(path: Path, data: bytes) -> str:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "xb") as fh:                      # create-exclusive: an existing evidence manifest is never overwritten
        fh.write(data)
    return hashlib.sha256(data).hexdigest()


def _roots(items: list[str]) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for item in items:
        label, sep, path = item.partition("=")
        if not sep or label in out:
            raise EvidenceError(f"--root must be LABEL=PATH with a distinct label, got {item!r}")
        out[label] = Path(path)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="SHA-256 manifest of preserved evidence (read-only over evidence, create-exclusive output)")
    ap.add_argument("--root", action="append", required=True, metavar="LABEL=PATH")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--out", type=Path, help="write a new manifest here (refuses an existing file)")
    mode.add_argument("--verify", type=Path, help="verify the evidence against this manifest")
    args = ap.parse_args(argv)
    try:
        roots = _roots(args.root)
        if args.out is not None:
            text = build_manifest(roots)
            digest = write_new(args.out, text.encode("utf-8"))
            print(f"wrote {args.out}: {text.count(chr(10))} files; manifest sha256 {digest}")
            return 0
        result = verify_manifest(Path(args.verify).read_text(encoding="utf-8"), roots)
        print(result)
        return 0 if result["ok"] else 2
    except (OSError, EvidenceError) as exc:
        print(f"INVALID: {exc!r}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
