"""Read-only check of the active environment against the forensic generators' lock (LA-1, approval C2).

Compares every `name==version` pin in the lock with `importlib.metadata.version(name)` in the running
interpreter, and the interpreter itself with `--python` / `--platform` when given. With `--proposal`
it also checks that the lock's pin lines are byte-identical to those of the proposal it was adopted
from (the header may differ). Writes nothing, installs nothing.

Usage (repo root):
  .venv/Scripts/python.exe -B -I scripts/forensics/check_forensics_lock.py LOCK
         [--proposal PROPOSED_LOCK] [--python 3.14.3] [--platform Windows-11-10.0.26200-SP0]
Exit codes: 0 ALL MATCH, 1 MISMATCH (a pin, the interpreter or the proposal differs), 2 unreadable lock.
Standard library only.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata as md
import platform
import re
import sys
from pathlib import Path

_PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s#;]+)$")


def pin_lines(text: str) -> list[str]:
    """Non-comment, non-blank lines, in order. Every one must be a `name==version` pin."""
    out = []
    for n, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if not _PIN.match(line):
            raise ValueError(f"line {n} is not a name==version pin: {raw!r}")
        out.append(line)
    if not out:
        raise ValueError("no pins")
    names = [_PIN.match(p).group(1).lower().replace("_", "-") for p in out]
    if len(set(names)) != len(names):
        raise ValueError("a package is pinned twice")
    return out


def check(pins: list[str], installed=md.version) -> list[dict]:
    rows = []
    for p in pins:
        name, want = _PIN.match(p).groups()
        try:
            have = installed(name)
        except md.PackageNotFoundError:
            have = None
        rows.append({"package": name, "pinned": want, "installed": have, "match": have == want})
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("lock", type=Path)
    ap.add_argument("--proposal", type=Path)
    ap.add_argument("--python", help="expected platform.python_version()")
    ap.add_argument("--platform", help="expected platform.platform()")
    args = ap.parse_args(argv)
    try:
        data = args.lock.read_bytes()
        pins = pin_lines(data.decode("utf-8"))
        proposal = pin_lines(args.proposal.read_text(encoding="utf-8")) if args.proposal else None
    except (OSError, UnicodeDecodeError, ValueError) as err:
        print(f"UNREADABLE LOCK: {err}")
        return 2
    has_cr = b"\r" in data
    print(f"lock      {args.lock}  sha256={hashlib.sha256(data).hexdigest()}  crlf={has_cr}")
    print(f"python    {sys.version}")
    print(f"executable {sys.executable}")
    print(f"platform  {platform.platform()}")
    bad = []
    for r in check(pins):
        print(f"  {r['package']:<16} pinned={r['pinned']:<14} installed={r['installed']!s:<14} "
              f"{'OK' if r['match'] else 'MISMATCH'}")
        if not r["match"]:
            bad.append(r["package"])
    if args.python and platform.python_version() != args.python:
        bad.append(f"python {platform.python_version()} != {args.python}")
    if args.platform and platform.platform() != args.platform:
        bad.append(f"platform {platform.platform()} != {args.platform}")
    if proposal is not None:
        same = proposal == pins
        print(f"proposal  {args.proposal}  pin lines identical: {same}")
        if not same:
            bad.append("pin lines differ from the proposal")
    print("ALL MATCH" if not bad else f"MISMATCH: {bad}")
    return 0 if not bad else 1


if __name__ == "__main__":
    sys.exit(main())
