"""Compare a reproduced forensic run directory with the original, exactly (LA-1(c)).

Status: written 2026-11-09, tested on synthetic directories only. It has NOT been run on a
reproduction: run_04-06 do not exist (REPRODUCTION NOT RUN; approvals C2 then C1 are open).

Rules:
  * every file of the original must exist in the reproduction; extra files are reported;
  * .json files are compared value by value, after dropping the top-level keys that are
    run-specific by construction (default: started, finished, command); numbers must be
    equal exactly (no tolerance), and the largest numeric difference is reported;
  * every other file is compared byte for byte; the first differing line is reported.
Reads both directories; writes nothing unless --report-out is given (refuses to overwrite).
Standard library only; imports no project code.

Usage:
  python scripts/forensics/compare_forensic_runs.py --original <run_01> --reproduction <run_04_repro>
         [--ignore-keys started,finished,command] [--report-out report.json]
Exit codes: 0 identical (apart from ignored keys), 1 differences, 2 unreadable input.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path

DEFAULT_IGNORE = ("started", "finished", "command")


def _files(root: Path) -> list[str]:
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())


def _json_diff(a, b, path: str, out: list[dict]) -> None:
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b), key=str):
            if k not in a or k not in b:
                out.append({"path": f"{path}/{k}", "original": a.get(k, "<absent>"), "reproduction": b.get(k, "<absent>")})
            else:
                _json_diff(a[k], b[k], f"{path}/{k}", out)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append({"path": path, "original": f"<list len {len(a)}>", "reproduction": f"<list len {len(b)}>"})
        for i, (x, y) in enumerate(zip(a, b)):
            _json_diff(x, y, f"{path}[{i}]", out)
    elif type(a) is not type(b) or (a != b and not (isinstance(a, float) and math.isnan(a) and math.isnan(b))):
        out.append({"path": path or "/", "original": a, "reproduction": b})


def compare(original: Path, reproduction: Path, ignore: tuple[str, ...] = DEFAULT_IGNORE) -> dict:
    if not original.is_dir() or not reproduction.is_dir():
        raise FileNotFoundError(f"not a directory: {original if not original.is_dir() else reproduction}")
    orig_files, repro_files = _files(original), _files(reproduction)
    report = {"original": str(original), "reproduction": str(reproduction), "ignored_top_level_json_keys": list(ignore),
              "missing_in_reproduction": sorted(set(orig_files) - set(repro_files)),
              "extra_in_reproduction": sorted(set(repro_files) - set(orig_files)), "files": []}
    for name in orig_files:
        if name not in repro_files:
            continue
        a_bytes, b_bytes = (original / name).read_bytes(), (reproduction / name).read_bytes()
        entry = {"file": name, "sha256_original": hashlib.sha256(a_bytes).hexdigest(),
                 "sha256_reproduction": hashlib.sha256(b_bytes).hexdigest()}
        if name.endswith(".json"):
            a, b = json.loads(a_bytes), json.loads(b_bytes)
            ignored = {}
            if isinstance(a, dict) and isinstance(b, dict):
                for k in ignore:
                    if k in a or k in b:
                        ignored[k] = {"original": a.pop(k, "<absent>"), "reproduction": b.pop(k, "<absent>")}
            diffs: list[dict] = []
            _json_diff(a, b, "", diffs)
            nums = [abs(d["original"] - d["reproduction"]) for d in diffs
                    if isinstance(d["original"], (int, float)) and isinstance(d["reproduction"], (int, float))
                    and not isinstance(d["original"], bool) and not isinstance(d["reproduction"], bool)]
            entry.update(mode="json", identical=not diffs, differences=diffs[:50], n_differences=len(diffs),
                         max_abs_numeric_difference=max(nums) if nums else None, ignored=ignored)
        else:
            same = a_bytes == b_bytes
            first = None
            if not same:
                la, lb = a_bytes.split(b"\n"), b_bytes.split(b"\n")
                first = next((i + 1 for i, (x, y) in enumerate(zip(la, lb)) if x != y), min(len(la), len(lb)) + 1)
            entry.update(mode="bytes", identical=same, first_differing_line=first)
        report["files"].append(entry)
    report["identical"] = (not report["missing_in_reproduction"] and not report["extra_in_reproduction"]
                           and all(f["identical"] for f in report["files"]))
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--original", required=True, type=Path)
    ap.add_argument("--reproduction", required=True, type=Path)
    ap.add_argument("--ignore-keys", default=",".join(DEFAULT_IGNORE))
    ap.add_argument("--report-out", type=Path)
    args = ap.parse_args(argv)
    ignore = tuple(k for k in args.ignore_keys.split(",") if k)
    try:
        report = compare(args.original, args.reproduction, ignore)
    except (OSError, ValueError) as err:
        print(f"UNREADABLE INPUT: {err}")
        return 2
    for f in report["files"]:
        extra = (f"first differing line {f['first_differing_line']}" if f["mode"] == "bytes" and not f["identical"]
                 else f"{f.get('n_differences', 0)} value differences, max |diff| {f.get('max_abs_numeric_difference')}"
                 if not f["identical"] else "")
        print(f"{'SAME' if f['identical'] else 'DIFF'} {f['file']} {extra}".rstrip())
    for name in report["missing_in_reproduction"]:
        print(f"MISSING {name}")
    for name in report["extra_in_reproduction"]:
        print(f"EXTRA {name}")
    print("RUNS IDENTICAL" if report["identical"] else "RUNS DIFFER")
    if args.report_out:
        try:
            with args.report_out.open("x", encoding="utf-8", newline="\n") as fh:
                json.dump(report, fh, indent=2, default=str)
                fh.write("\n")
        except FileExistsError:
            print(f"refusing to overwrite {args.report_out}")
            return 2
    return 0 if report["identical"] else 1


if __name__ == "__main__":
    sys.exit(main())
