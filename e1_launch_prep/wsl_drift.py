"""Read-only WSL-side drift check against a snapshot's per-group file lists. Standard library only; run it inside WSL (`wsl -d Ubuntu-24.04 -- python3 wsl_drift.py ...`).

For each `--group NAME=/linux/path` it reads `<snapshot>/00_manifest/NAME.files.tsv` (column 0 = relative path, column 1 = size, column 6 = the hash the archive computed natively
inside the distribution), walks the live directory with the archive's excluded directory names, hashes every common file natively, and reports SOURCE_ADDED / SOURCE_REMOVED /
SOURCE_CHANGED exactly as `archive_evidence.py verify --check-sources` words them. Writes nothing except the JSON report whose path is given (create-exclusive).
This is an independent, faster cross-check of the WSL half of gate S1; it does not replace S1.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time

EXCLUDE_DIRS = {".git", ".venv", "__pycache__", ".pytest_cache", "node_modules"}      # archive manifest 'exclude_dirs'
CHUNK = 1 << 20


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def recorded(tsv_path: str) -> dict:
    out = {}
    with open(tsv_path, encoding="utf-8") as fh:
        for line in fh.read().split("\n")[1:]:
            if line:
                cols = line.split("\t")
                out[cols[0]] = {"size": int(cols[1]), "sha": cols[6]}
    return out


def live_files(root: str) -> dict:
    out = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for name in filenames:
            full = os.path.join(dirpath, name)
            out[os.path.relpath(full, root).replace(os.sep, "/")] = full
    return out


def compare(name: str, root: str, snapshot_manifest_dir: str) -> dict:
    t0 = time.time()
    rec = recorded(os.path.join(snapshot_manifest_dir, f"{name}.files.tsv"))
    cur = live_files(root)
    added, removed = sorted(set(cur) - set(rec)), sorted(set(rec) - set(cur))
    changed, size_changed = [], []
    for rel in sorted(set(cur) & set(rec)):
        if os.path.getsize(cur[rel]) != rec[rel]["size"]:
            size_changed.append(rel)
        if sha256_file(cur[rel]) != rec[rel]["sha"]:
            changed.append(rel)
    return {"root": root, "recorded_files": len(rec), "current_files": len(cur), "SOURCE_ADDED": added, "SOURCE_REMOVED": removed, "SOURCE_CHANGED": changed,
            "size_changed": size_changed, "seconds": round(time.time() - t0, 1)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--snapshot-manifest-dir", required=True, help="<snapshot>/00_manifest as seen from WSL")
    ap.add_argument("--group", action="append", required=True, metavar="NAME=/linux/path")
    ap.add_argument("--out", required=True, help="new JSON report (an existing file is refused)")
    args = ap.parse_args(argv)
    report = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "snapshot_manifest_dir": args.snapshot_manifest_dir, "groups": {}}
    drift = 0
    for item in args.group:
        name, sep, root = item.partition("=")
        if not sep or not os.path.isdir(root):
            print(f"INVALID: --group {item!r} needs NAME=/existing/dir", file=sys.stderr)
            return 2
        res = compare(name, root, args.snapshot_manifest_dir)
        report["groups"][name] = res
        n = len(res["SOURCE_ADDED"]) + len(res["SOURCE_REMOVED"]) + len(res["SOURCE_CHANGED"])
        drift += n
        print(f"{name}: recorded={res['recorded_files']} current={res['current_files']} added={len(res['SOURCE_ADDED'])} removed={len(res['SOURCE_REMOVED'])} "
              f"changed={len(res['SOURCE_CHANGED'])} ({res['seconds']} s)", flush=True)
        for kind in ("SOURCE_ADDED", "SOURCE_REMOVED", "SOURCE_CHANGED"):
            for rel in res[kind]:
                print(kind, f"{name}/{rel}")
    report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    report["total_drift_events"] = drift
    with open(args.out, "x", encoding="utf-8", newline="\n") as fh:                   # create-exclusive
        json.dump(report, fh, indent=2)
    print(f"total drift events: {drift}; report: {args.out}")
    return 0 if drift == 0 else 3


if __name__ == "__main__":
    sys.exit(main())
