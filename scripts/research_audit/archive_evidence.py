#!/usr/bin/env python
"""Hash-verified, byte-exact evidence archiver (standard library only).

Purpose: preserve research evidence without modifying the originals.

Guarantees
- Files are copied as bytes (shutil.copyfile): no newline translation, so historical line endings are
  preserved exactly. The line-ending style of every file is recorded.
- Sources are only opened for reading. Each source is hashed before the copy and again after every
  group has been copied; any difference marks the run FAIL.
- Each copy is re-read and hashed; it must equal the source hash. Files under WSL can additionally
  be cross-checked against an independent sha256sum run inside the distro.
- The archive root must not be inside a Git work tree or inside the repository, and an existing
  snapshot directory is never overwritten.
- A complete Git state record (HEAD, status, index, ignored list, diffs, config) is written as bytes.

Subcommands
  create  copy the listed groups into a new ARCHIVE_<UTC> snapshot directory under --archive-root
  verify  re-hash a snapshot against its SHA256SUMS.txt (and optionally the current sources)
"""
from __future__ import annotations

import argparse
import datetime as dt
import fnmatch
import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path

TOOL_VERSION = "archive_evidence 0.1 (2026-11-12)"
CHUNK = 1 << 20
DEFAULT_EXCLUDE_DIRS = (".git", ".venv", "__pycache__", ".pytest_cache", "node_modules")
DEFAULT_EXCLUDE_GLOBS = ("*.pyc",)
GROUP_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")

GIT_CAPTURES = (
    ("head.txt", ("rev-parse", "HEAD")),
    ("branch.txt", ("rev-parse", "--abbrev-ref", "HEAD")),
    ("status_long_uall.txt", ("status", "--long", "-uall")),
    ("status_porcelain_v2_branch_uall.txt", ("status", "--porcelain=v2", "--branch", "-uall")),
    ("ls_files_stage.txt", ("ls-files", "-s")),
    ("ls_files_untracked.txt", ("ls-files", "--others", "--exclude-standard")),
    ("ls_files_ignored_dirs.txt", ("ls-files", "--others", "--ignored", "--exclude-standard", "--directory")),
    ("log_30_fuller.txt", ("log", "-30", "--decorate", "--format=fuller")),
    ("stash_list.txt", ("stash", "list")),
    ("config_list_show_origin.txt", ("config", "--list", "--show-origin")),
    ("diff_cached.patch", ("diff", "--cached", "--binary")),
    ("diff_worktree.patch", ("diff", "--binary")),
)


class ArchiveError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def iso_from_ns(ns: int) -> str:
    sec, rem = divmod(int(ns), 1_000_000_000)
    return dt.datetime.fromtimestamp(sec, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{rem:09d}Z"


def scan_file(path: Path) -> dict:
    """One streaming read: SHA-256, size, NUL presence and newline statistics."""
    digest = hashlib.sha256()
    size = crlf = lf_total = cr_total = 0
    has_nul = prev_cr = False
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(CHUNK)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            has_nul = has_nul or (b"\x00" in chunk)
            lf_total += chunk.count(b"\n")
            cr_total += chunk.count(b"\r")
            crlf += chunk.count(b"\r\n") + (1 if prev_cr and chunk[:1] == b"\n" else 0)
            prev_cr = chunk.endswith(b"\r")
    lone_lf, lone_cr = lf_total - crlf, cr_total - crlf
    if has_nul:
        eol = "binary"
    elif crlf and not lone_lf and not lone_cr:
        eol = "crlf"
    elif lone_lf and not crlf and not lone_cr:
        eol = "lf"
    elif not (crlf or lone_lf or lone_cr):
        eol = "none"
    else:
        eol = "mixed"
    return {"sha256": digest.hexdigest(), "size": size, "eol": eol, "crlf": crlf, "lf": lone_lf, "cr": lone_cr}


def walk(root: Path, exclude_dirs=DEFAULT_EXCLUDE_DIRS, exclude_globs=DEFAULT_EXCLUDE_GLOBS):
    """Return ([(relative posix path, absolute Path)], [(relative path, reason)]) for regular files."""
    skipped: list[tuple[str, str]] = []
    if root.is_file():
        return [(root.name, root)], skipped
    out: list[tuple[str, Path]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        keep = []
        for d in sorted(dirnames):
            full_dir = Path(dirpath) / d
            rel_dir = full_dir.relative_to(root).as_posix()
            if d in exclude_dirs:
                skipped.append((rel_dir + "/", "excluded_dir"))
            elif full_dir.is_symlink():
                skipped.append((rel_dir + "/", "symlink_dir"))
            else:
                keep.append(d)
        dirnames[:] = keep
        for name in sorted(filenames):
            full = Path(dirpath) / name
            rel = full.relative_to(root).as_posix()
            if "\t" in rel or "\n" in rel:
                raise ArchiveError(f"path contains a tab or newline and cannot be recorded safely: {rel!r}")
            if any(fnmatch.fnmatch(name, g) for g in exclude_globs):
                skipped.append((rel, "excluded_glob"))
            elif full.is_symlink() or not full.is_file():
                skipped.append((rel, "symlink_or_not_regular"))
            else:
                out.append((rel, full))
    return out, skipped


def write_tsv(path: Path, header: list[str], rows: list[list]) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\t".join(header) + "\n")
        for row in rows:
            fh.write("\t".join(str(v) for v in row) + "\n")


def run_git(repo: Path, args: tuple[str, ...]) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "--no-optional-locks", "-C", str(repo), *args], capture_output=True, check=False)


def ensure_outside_git(archive_root: Path, repo: Path) -> dict:
    """The archive root must not be inside the repository path or inside any Git work tree."""
    a = archive_root.resolve()
    r = repo.resolve()
    try:
        a.relative_to(r)
    except ValueError:
        pass
    else:
        raise ArchiveError(f"archive root {a} is inside the repository {r}")
    probe = a
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    proc = subprocess.run(["git", "-C", str(probe), "rev-parse", "--is-inside-work-tree"], capture_output=True, check=False)
    inside = proc.returncode == 0 and proc.stdout.strip() == b"true"
    if inside:
        raise ArchiveError(f"archive root {a} is inside a Git work tree (probed at {probe})")
    return {"archive_root": str(a), "repository": str(r), "probe_dir": str(probe),
            "git_rev_parse_returncode": proc.returncode, "inside_work_tree": False}


def capture_git(repo: Path, outdir: Path, attr_paths: list[str]) -> dict:
    outdir.mkdir(parents=True, exist_ok=True)
    record = {}
    captures = list(GIT_CAPTURES)
    if attr_paths:
        captures.append(("check_attr.txt", ("check-attr", "-a", "--", *attr_paths)))
    for fname, args in captures:
        proc = run_git(repo, args)
        (outdir / fname).write_bytes(proc.stdout)
        entry = {"args": list(args), "returncode": proc.returncode, "bytes": len(proc.stdout)}
        if proc.stderr:
            (outdir / (fname + ".stderr")).write_bytes(proc.stderr)
            entry["stderr_bytes"] = len(proc.stderr)
        record[fname] = entry
    return record


def wsl_unc(distro: str, linux_path: str) -> Path:
    return Path("\\\\wsl.localhost\\" + distro + linux_path.replace("/", "\\"))


def wsl_native_sha256(distro: str, linux_root: str) -> dict[str, str]:
    """Independent hashes computed inside the distro with sha256sum (relative path -> hex)."""
    cmd = f"cd '{linux_root}' && find . -type f -print0 | sort -z | xargs -0 sha256sum"
    proc = subprocess.run(["wsl.exe", "-d", distro, "--exec", "bash", "-c", cmd], capture_output=True, check=False)
    if proc.returncode != 0:
        raise ArchiveError(f"wsl sha256sum failed for {linux_root}: {proc.stderr[:300]!r}")
    result: dict[str, str] = {}
    for line in proc.stdout.decode("utf-8", "strict").split("\n"):
        if not line:
            continue
        if line.startswith("\\"):
            raise ArchiveError(f"sha256sum escaped a file name; unsupported: {line[:120]!r}")
        digest, _, rel = line.partition("  ")
        result[rel[2:] if rel.startswith("./") else rel] = digest
    return result


def snapshot_name() -> str:
    return "ARCHIVE_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def cmd_create(args: argparse.Namespace) -> int:
    repo = Path(args.repo)
    archive_root = Path(args.archive_root)
    groups: list[dict] = []
    for spec in args.group or []:
        name, _, path = spec.partition("=")
        groups.append({"name": name, "source": Path(path), "wsl": None})
    for spec in args.wsl_group or []:
        name, _, rest = spec.partition("=")
        distro, _, linux_path = rest.partition(":")
        groups.append({"name": name, "source": wsl_unc(distro, linux_path), "wsl": (distro, linux_path)})
    names = [g["name"] for g in groups]
    if not groups or len(set(names)) != len(names) or not all(GROUP_NAME.match(n) for n in names):
        raise ArchiveError("need at least one group; group names must be unique and match [A-Za-z0-9_.-]+")
    for g in groups:
        if not g["source"].exists():
            raise ArchiveError(f"source for group {g['name']} does not exist: {g['source']}")
    guard = ensure_outside_git(archive_root, repo)
    exclude_dirs = tuple(DEFAULT_EXCLUDE_DIRS) + tuple(args.exclude_dir or ())
    listing = {g["name"]: walk(g["source"], exclude_dirs) for g in groups}
    if args.dry_run:
        for g in groups:
            files, skipped = listing[g["name"]]
            total = sum(p.stat().st_size for _, p in files)
            print(f"{g['name']}: files={len(files)} bytes={total} skipped={len(skipped)} source={g['source']}")
        return 0
    started = utc_now()
    snap = archive_root / snapshot_name()
    if snap.exists():
        raise ArchiveError(f"snapshot directory already exists: {snap}")
    snap.mkdir(parents=True)
    meta_dir = snap / "00_manifest"
    meta_dir.mkdir()
    git_record = capture_git(repo, snap / "01_git_state", list(args.check_attr or []))
    head = (snap / "01_git_state" / "head.txt").read_text(encoding="utf-8").strip()

    results = []
    for g in groups:
        files, skipped = listing[g["name"]]
        dest_root = snap / "data" / g["name"]
        native = wsl_native_sha256(*g["wsl"]) if g["wsl"] else None
        rows = []
        for rel, src in files:
            st = src.stat()
            before = scan_file(src)
            dst = dest_root / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
            os.utime(dst, ns=(st.st_mtime_ns, st.st_mtime_ns))
            copy = scan_file(dst)
            rows.append({"rel": rel, "src": src, "dst": dst, "mtime_ns": st.st_mtime_ns, "before": before, "copy": copy,
                         "native": None if native is None else native.get(rel)})
        results.append({"group": g, "rows": rows, "skipped": skipped, "native_count": None if native is None else len(native)})

    # Second pass over every source, after all copying is finished.
    failures = 0
    for res in results:
        for row in res["rows"]:
            row["after"] = scan_file(row["src"])
    summary_groups = []
    for res in results:
        g, rows = res["group"], res["rows"]
        tsv_rows = []
        counts: dict[str, int] = {}
        eol_counts: dict[str, int] = {}
        for row in rows:
            b, c, a, n = row["before"], row["copy"], row["after"], row["native"]
            problems = []
            if b["sha256"] != c["sha256"] or b["size"] != c["size"]:
                problems.append("COPY_MISMATCH")
            if b["sha256"] != a["sha256"] or b["size"] != a["size"]:
                problems.append("SOURCE_CHANGED")
            if res["native_count"] is not None and n != b["sha256"]:
                problems.append("WSL_NATIVE_MISMATCH")
            status = "OK" if not problems else "+".join(problems)
            counts[status] = counts.get(status, 0) + 1
            eol_counts[b["eol"]] = eol_counts.get(b["eol"], 0) + 1
            failures += 0 if status == "OK" else 1
            tsv_rows.append([row["rel"], b["size"], iso_from_ns(row["mtime_ns"]), b["sha256"], c["sha256"], a["sha256"],
                             "" if n is None else n, b["eol"], b["crlf"], b["lf"], b["cr"], status])
        if res["native_count"] is not None and res["native_count"] != len(rows):
            failures += 1
            counts["WSL_FILE_COUNT_MISMATCH"] = 1
        write_tsv(meta_dir / f"{g['name']}.files.tsv",
                  ["relative_path", "size", "mtime_utc", "sha256_source_before", "sha256_archive_copy", "sha256_source_after",
                   "sha256_wsl_native", "eol", "crlf", "lf_only", "cr_only", "status"], tsv_rows)
        write_tsv(meta_dir / f"{g['name']}.skipped.tsv", ["relative_path", "reason"], [[r, why] for r, why in res["skipped"]])
        summary_groups.append({"name": g["name"], "source": str(g["source"]), "wsl": g["wsl"], "files": len(rows),
                               "bytes": sum(r["before"]["size"] for r in rows), "skipped": len(res["skipped"]),
                               "status_counts": counts, "eol_counts": eol_counts})

    result = "PASS" if failures == 0 else "FAIL"
    manifest = {
        "tool": TOOL_VERSION, "tool_sha256": scan_file(Path(__file__))["sha256"], "argv": sys.argv,
        "started_utc": started, "finished_utc": utc_now(), "host": platform.node(), "os": platform.platform(),
        "python": sys.version, "label": args.label, "snapshot_dir": str(snap), "repository": str(repo),
        "git_head": head, "outside_git_check": guard, "git_capture": git_record, "groups": summary_groups,
        "totals": {"files": sum(g["files"] for g in summary_groups), "bytes": sum(g["bytes"] for g in summary_groups),
                   "problem_files": failures},
        "exclude_dirs": list(exclude_dirs), "exclude_globs": list(DEFAULT_EXCLUDE_GLOBS), "result": result,
    }
    (meta_dir / "archive_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")

    sums = []
    for p in sorted(snap.rglob("*")):
        if p.is_file() and p.name != "SHA256SUMS.txt":
            sums.append(f"{scan_file(p)['sha256']}  {p.relative_to(snap).as_posix()}")
    (snap / "SHA256SUMS.txt").write_text("\n".join(sums) + "\n", encoding="utf-8", newline="\n")
    if not args.no_readonly:
        for p in snap.rglob("*"):
            if p.is_file():
                os.chmod(p, stat.S_IREAD)
    print(f"snapshot: {snap}")
    print(f"result: {result}  files={manifest['totals']['files']}  bytes={manifest['totals']['bytes']}  problem_files={failures}")
    for g in summary_groups:
        print(f"  {g['name']}: files={g['files']} bytes={g['bytes']} skipped={g['skipped']} status={g['status_counts']} eol={g['eol_counts']}")
    return 0 if failures == 0 else 2


def cmd_verify(args: argparse.Namespace) -> int:
    snap = Path(args.snapshot)
    sums_file = snap / "SHA256SUMS.txt"
    if not sums_file.is_file():
        raise ArchiveError(f"missing {sums_file}")
    bad = missing = checked = 0
    listed = set()
    for line in sums_file.read_text(encoding="utf-8").split("\n"):
        if not line:
            continue
        digest, _, rel = line.partition("  ")
        listed.add(rel)
        path = snap / rel
        if not path.is_file():
            missing += 1
            print(f"MISSING {rel}")
            continue
        checked += 1
        if scan_file(path)["sha256"] != digest:
            bad += 1
            print(f"MISMATCH {rel}")
    extra = [p.relative_to(snap).as_posix() for p in snap.rglob("*") if p.is_file()
             and p.name != "SHA256SUMS.txt" and p.relative_to(snap).as_posix() not in listed]
    for rel in extra:
        print(f"UNLISTED {rel}")
    drift = 0
    if args.check_sources:
        manifest = json.loads((snap / "00_manifest" / "archive_manifest.json").read_text(encoding="utf-8"))
        for g in manifest["groups"]:
            root = Path(g["source"])
            tsv = (snap / "00_manifest" / f"{g['name']}.files.tsv").read_text(encoding="utf-8").split("\n")[1:]
            recorded = {}
            for line in tsv:
                if line:
                    cols = line.split("\t")
                    recorded[cols[0]] = cols[3]
            files, _ = walk(root, tuple(manifest["exclude_dirs"]))
            current = {rel: scan_file(p)["sha256"] for rel, p in files}
            for rel, digest in sorted(recorded.items()):
                if rel not in current:
                    drift += 1
                    print(f"SOURCE_REMOVED {g['name']}/{rel}")
                elif current[rel] != digest:
                    drift += 1
                    print(f"SOURCE_CHANGED {g['name']}/{rel}")
            for rel in sorted(set(current) - set(recorded)):
                drift += 1
                print(f"SOURCE_ADDED {g['name']}/{rel}")
    print(f"verify: checked={checked} mismatched={bad} missing={missing} unlisted={len(extra)} source_drift={drift}")
    return 0 if (bad == 0 and missing == 0 and not extra) else 2


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").strip().splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("create", help="create a new snapshot")
    c.add_argument("--archive-root", required=True)
    c.add_argument("--repo", required=True)
    c.add_argument("--label", default="")
    c.add_argument("--group", action="append", metavar="NAME=PATH", help="local directory or file to archive")
    c.add_argument("--wsl-group", action="append", metavar="NAME=DISTRO:/linux/path", help="WSL directory; cross-checked with sha256sum")
    c.add_argument("--exclude-dir", action="append", help="extra directory name to skip everywhere")
    c.add_argument("--check-attr", action="append", help="repo-relative path to run git check-attr on")
    c.add_argument("--no-readonly", action="store_true", help="do not mark archived files read-only")
    c.add_argument("--dry-run", action="store_true", help="list counts and sizes only; write nothing")
    v = sub.add_parser("verify", help="verify a snapshot")
    v.add_argument("--snapshot", required=True)
    v.add_argument("--check-sources", action="store_true", help="also report drift of the current sources")
    args = ap.parse_args(argv)
    try:
        return cmd_create(args) if args.cmd == "create" else cmd_verify(args)
    except ArchiveError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
