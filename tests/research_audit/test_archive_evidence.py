"""Fabric-free tests of the evidence archiver. Everything happens inside pytest's tmp_path; no repository
file, WSL path, Docker, Fabric or network resource is touched."""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "research_audit"))

import archive_evidence as ae  # noqa: E402

HAVE_GIT = shutil.which("git") is not None


def make_tree(root: Path) -> dict[str, bytes]:
    files = {
        "crlf.txt": b"line1\r\nline2\r\nline3\r\n",
        "lf.txt": b"line1\nline2\nline3\n",
        "mixed.txt": b"a\r\nb\nc\r\n",
        "none.txt": b"no newline at all",
        "binary.bin": bytes(range(256)) * 40 + b"\x00\r\n\x00",
        "sub/deep/nested.json": b'{"k": 1}\r\n',
        "__pycache__/skip.pyc": b"x",
    }
    for rel, data in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return files


def init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    (path / "tracked.txt").write_bytes(b"hello\r\n")


def create(tmp: Path, repo: Path, src: Path, extra=()):
    argv = ["create", "--archive-root", str(tmp / "archive"), "--repo", str(repo), "--group", f"data={src}",
            "--no-readonly", *extra]
    return ae.main(argv)


def only_snapshot(tmp: Path) -> Path:
    snaps = [p for p in (tmp / "archive").iterdir() if p.name.startswith("ARCHIVE_")]
    assert len(snaps) == 1
    return snaps[0]


@pytest.mark.parametrize("data,eol", [(b"a\r\nb\r\n", "crlf"), (b"a\nb\n", "lf"), (b"a\r\nb\n", "mixed"),
                                      (b"abc", "none"), (b"a\x00\r\n", "binary"), (b"a\rb", "mixed")])
def test_scan_file_eol_classification(tmp_path, data, eol):
    p = tmp_path / "f"
    p.write_bytes(data)
    assert ae.scan_file(p)["eol"] == eol


def test_crlf_split_across_chunks_is_counted_once(tmp_path, monkeypatch):
    monkeypatch.setattr(ae, "CHUNK", 3)
    p = tmp_path / "f"
    p.write_bytes(b"ab\r\ncd\r\nef\r\n")
    stats = ae.scan_file(p)
    assert (stats["crlf"], stats["lf"], stats["cr"], stats["eol"]) == (3, 0, 0, "crlf")


@pytest.mark.skipif(not HAVE_GIT, reason="git not available")
def test_create_copies_bytes_exactly_and_verifies(tmp_path):
    src, repo = tmp_path / "src", tmp_path / "repo"
    files = make_tree(src)
    init_repo(repo)
    assert create(tmp_path, repo, src) == 0
    snap = only_snapshot(tmp_path)
    for rel, data in files.items():
        copied = snap / "data" / "data" / rel
        if rel.startswith("__pycache__"):
            assert not copied.exists()
        else:
            assert copied.read_bytes() == data, rel
            assert abs(copied.stat().st_mtime_ns - (src / rel).stat().st_mtime_ns) < 2_000
    tsv = (snap / "00_manifest" / "data.files.tsv").read_text(encoding="utf-8").split("\n")
    rows = {line.split("\t")[0]: line.split("\t") for line in tsv[1:] if line}
    assert rows["crlf.txt"][7] == "crlf" and rows["lf.txt"][7] == "lf" and rows["mixed.txt"][7] == "mixed"
    assert rows["binary.bin"][7] == "binary"
    assert all(r[-1] == "OK" for r in rows.values())
    assert (snap / "01_git_state" / "head.txt").exists()
    assert ae.main(["verify", "--snapshot", str(snap), "--check-sources"]) == 0


@pytest.mark.skipif(not HAVE_GIT, reason="git not available")
def test_verify_detects_corruption_and_source_drift(tmp_path, capsys):
    src, repo = tmp_path / "src", tmp_path / "repo"
    make_tree(src)
    init_repo(repo)
    assert create(tmp_path, repo, src) == 0
    snap = only_snapshot(tmp_path)
    (src / "lf.txt").write_bytes(b"changed after archiving\n")
    assert ae.main(["verify", "--snapshot", str(snap), "--check-sources"]) == 0  # archive itself intact
    assert "SOURCE_CHANGED data/lf.txt" in capsys.readouterr().out
    victim = snap / "data" / "data" / "crlf.txt"
    victim.write_bytes(victim.read_bytes()[:-1] + b"X")
    assert ae.main(["verify", "--snapshot", str(snap)]) == 2
    assert "MISMATCH data/data/crlf.txt" in capsys.readouterr().out


def test_refuses_archive_root_inside_repository(tmp_path):
    repo = tmp_path / "repo"
    (repo / "inner").mkdir(parents=True)
    src = tmp_path / "src"
    make_tree(src)
    argv = ["create", "--archive-root", str(repo / "inner" / "archive"), "--repo", str(repo), "--group", f"data={src}"]
    assert ae.main(argv) == 1


@pytest.mark.skipif(not HAVE_GIT, reason="git not available")
def test_refuses_archive_root_inside_any_git_worktree(tmp_path):
    other = tmp_path / "other_repo"
    init_repo(other)
    repo, src = tmp_path / "repo", tmp_path / "src"
    init_repo(repo)
    make_tree(src)
    argv = ["create", "--archive-root", str(other / "archive"), "--repo", str(repo), "--group", f"data={src}"]
    assert ae.main(argv) == 1
    assert not (other / "archive").exists()


@pytest.mark.skipif(not HAVE_GIT, reason="git not available")
def test_dry_run_writes_nothing(tmp_path):
    src, repo = tmp_path / "src", tmp_path / "repo"
    make_tree(src)
    init_repo(repo)
    assert create(tmp_path, repo, src, ["--dry-run"]) == 0
    assert not (tmp_path / "archive").exists()


@pytest.mark.skipif(not HAVE_GIT, reason="git not available")
def test_symlinks_are_skipped_not_followed(tmp_path):
    src, repo = tmp_path / "src", tmp_path / "repo"
    make_tree(src)
    init_repo(repo)
    try:
        os.symlink(src / "lf.txt", src / "link.txt")
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not permitted on this host")
    assert create(tmp_path, repo, src) == 0
    snap = only_snapshot(tmp_path)
    assert not (snap / "data" / "data" / "link.txt").exists()
    skipped = (snap / "00_manifest" / "data.skipped.tsv").read_text(encoding="utf-8")
    assert "link.txt\tsymlink_or_not_regular" in skipped
