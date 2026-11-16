"""Offline tests of the Experiment 1 launch-preparation tools. Synthetic inputs only; nothing here is live evidence. The real tree is read (never written) for the source manifest.

Run (from the repository root, offline):   .venv/Scripts/python.exe -m pytest e1_launch_prep -p no:cacheprovider
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pytest

from e1_launch_prep import prepare as pp
from p4_replay import REPO_ROOT, driver as dr, launch_ids as li, pins as pn, snapshot as sn
from p4_replay.tests import support as sp
from p4_replay.tests import support_2c as s2
from p4_replay.tests.conftest import ENV

PKG = Path(__file__).resolve().parents[1]
GROUP = "repo_p4_replay"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_stage(tmp_path: Path, name: str = "stage") -> tuple[Path, dict]:
    """Stage the dry-run launch from synthetic processed data through the real staging function (no archive lineage)."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    env_file = tmp_path / "env.json"
    env_file.write_text(json.dumps(ENV), encoding="utf-8")
    net_file = tmp_path / "network.json"
    net_file.write_text(json.dumps(sp.NETWORK_DOC), encoding="utf-8")
    root = tmp_path / name
    series = pp.stage_dry_run(root, repo_root=REPO_ROOT, processed_dir=s2.processed_dir(tmp_path), archive_manifest=None, env_path=env_file, network_file=net_file,
                              network_wsl=str(net_file))
    return root, series


def make_drift_snapshot(tmp_path: Path, files: dict[str, bytes], *, recorded: dict[str, bytes], group: str = GROUP) -> Path:
    """A synthetic snapshot folder whose manifest has one Windows group `group` whose live source is tmp_path/src (files) and whose recorded list is `recorded`."""
    src = tmp_path / "src"
    for rel, data in files.items():
        (src / rel).parent.mkdir(parents=True, exist_ok=True)
        (src / rel).write_bytes(data)
    snap = tmp_path / "snap" / "ARCHIVE_20261120T100000Z"
    (snap / "00_manifest").mkdir(parents=True)
    manifest = {"result": "PASS", "git_head": "c" * 40, "finished_utc": "2026-11-20T10:30:00.000000Z", "exclude_dirs": [".git", ".venv", "__pycache__", ".pytest_cache", "node_modules"],
                "groups": [{"name": group, "source": str(src), "wsl": None}, {"name": "wsl_e1", "source": r"\\wsl.localhost\Ubuntu-24.04\home\x", "wsl": ["Ubuntu-24.04", "/home/x"]}],
                "totals": {"problem_files": 0}}
    (snap / "00_manifest" / "archive_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    header = "relative_path\tsize\tmtime_utc\tsha256_source_before\tsha256_archive_copy\tsha256_source_after\tsha256_wsl_native\teol\tcrlf\tlf_only\tcr_only\tstatus"
    rows = [f"{rel}\t{len(d)}\tT\t{sha(d)}\t{sha(d)}\t{sha(d)}\t{sha(d)}\tlf\t0\t1\t0\tOK" for rel, d in recorded.items()]
    (snap / "00_manifest" / f"{group}.files.tsv").write_text("\n".join([header] + rows) + "\n", encoding="utf-8")
    (snap / "00_manifest" / "wsl_e1.files.tsv").write_text(header + "\n", encoding="utf-8")
    (snap / "SHA256SUMS.txt").write_text("TEST\n", encoding="utf-8")
    return snap


def test_write_new_never_overwrites_and_returns_the_hash(tmp_path):
    target = tmp_path / "a" / "x.txt"
    assert pp.write_new(target, b"one\n") == sha(b"one\n")
    with pytest.raises(FileExistsError):
        pp.write_new(target, b"two\n")
    assert target.read_bytes() == b"one\n"


def test_source_manifest_is_deterministic_sorted_lf_and_exact_against_the_tree():
    first, second = pp.build_source_manifest_bytes(), pp.build_source_manifest_bytes()
    assert first == second and b"\r" not in first and first.endswith(b"\n")
    lines = first.decode("utf-8").splitlines()
    paths = [ln.split("  ", 1)[1] for ln in lines]
    assert paths == sorted(paths) and len(paths) == len(set(paths)) == len(pn.reviewed_source_paths(REPO_ROOT))
    assert all(re.fullmatch(r"[0-9a-f]{64}  \S.*", ln) for ln in lines)
    parsed = pn.parse_source_manifest(first, expected_sha256=sha(first))
    assert pn.verify_source_manifest(REPO_ROOT, parsed)["ok"] is True
    assert not any(p.startswith("e1_launch_prep/") for p in paths), "the preparation tools are not part of the reviewed set"


def test_generating_artifacts_writes_nothing_into_the_reviewed_tree(tmp_path):
    before = s2.tree_digest(REPO_ROOT / "p4_replay")
    pp.build_source_manifest_bytes()
    snap = make_drift_snapshot(tmp_path, {"a.py": b"A"}, recorded={"a.py": b"A"})
    pp.windows_group_drift(snap)
    assert s2.tree_digest(REPO_ROOT / "p4_replay") == before


def test_pins_bytes_are_deterministic_accepted_by_the_strict_parser_and_match_the_folder(tmp_path):
    snap = s2.make_snapshot(tmp_path)
    path_win = r"D:\PLAN_BLOCK18\evidence_archive" + "\\" + snap["root"].name
    first = pp.build_pins_bytes(snap["root"], path_win=path_win, required_groups=tuple(s2.GROUPS))
    assert first == pp.build_pins_bytes(snap["root"], path_win=path_win, required_groups=tuple(s2.GROUPS)) and b"created_utc" not in first
    parsed = pn.parse_pins(first, expected_sha256=sha(first))
    assert pn.verify_snapshot_identity(parsed.snapshot, snap["root"])["ok"] is True


@pytest.mark.parametrize("mutate", ["fail_result", "problem_files", "missing_group"])
def test_pins_refuses_a_snapshot_that_is_not_clean(tmp_path, mutate):
    snap = s2.make_snapshot(tmp_path)
    mp = snap["root"] / "00_manifest" / "archive_manifest.json"
    doc = json.loads(mp.read_text())
    if mutate == "fail_result":
        doc["result"] = "FAIL"
    elif mutate == "problem_files":
        doc["totals"]["problem_files"] = 2
    else:
        doc["groups"] = [g for g in doc["groups"] if g["name"] != "wsl_e1"]
    mp.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(pp.PrepError):
        pp.build_pins_bytes(snap["root"], path_win=r"D:\x" + "\\" + snap["root"].name, required_groups=tuple(s2.GROUPS))


def test_windows_group_drift_classifies_added_changed_removed_and_skips_wsl(tmp_path):
    snap = make_drift_snapshot(tmp_path, {"keep.py": b"K", "edited.py": b"NEW", "new1.py": b"N1", "sub/new2.py": b"N2", "__pycache__/x.pyc": b"ignored"},
                               recorded={"keep.py": b"K", "edited.py": b"OLD", "gone.py": b"G"})
    d = pp.windows_group_drift(snap)
    assert list(d["added"]) == [f"{GROUP}/new1.py", f"{GROUP}/sub/new2.py"] and d["added"][f"{GROUP}/new1.py"] == sha(b"N1")
    assert d["changed"] == [f"{GROUP}/edited.py"] and d["removed"] == [f"{GROUP}/gone.py"]
    assert d["groups"]["wsl_e1"].get("skipped") and d["summary"]["wsl_groups_skipped"] == 1 and d["errors"] == {}


def test_drift_list_is_lf_sorted_parsable_and_refuses_changed_or_removed_files(tmp_path):
    ok = make_drift_snapshot(tmp_path / "ok", {"keep.py": b"K", "b.py": b"B", "a.py": b"A"}, recorded={"keep.py": b"K"})
    data = pp.build_drift_list_bytes(pp.windows_group_drift(ok))
    assert b"\r" not in data and data.endswith(b"\n")
    allowed = sn.parse_signed_additions(data.decode("utf-8"))
    assert list(allowed) == [f"{GROUP}/a.py", f"{GROUP}/b.py"] and allowed[f"{GROUP}/a.py"] == sha(b"A")
    changed = make_drift_snapshot(tmp_path / "changed", {"keep.py": b"EDITED"}, recorded={"keep.py": b"K"})
    with pytest.raises(pp.PrepError, match="SOURCE_CHANGED"):
        pp.build_drift_list_bytes(pp.windows_group_drift(changed))
    removed = make_drift_snapshot(tmp_path / "removed", {"keep.py": b"K"}, recorded={"keep.py": b"K", "gone.py": b"G"})
    with pytest.raises(pp.PrepError, match="SOURCE_REMOVED"):
        pp.build_drift_list_bytes(pp.windows_group_drift(removed))
    other = make_drift_snapshot(tmp_path / "other", {"x.txt": b"X"}, recorded={}, group="p1a_plan")
    with pytest.raises(pp.PrepError, match="cannot map"):
        pp.build_drift_list_bytes(pp.windows_group_drift(other))


def test_stage_dry_run_renders_only_the_dry_run_offline_and_deterministically(tmp_path):
    a, series_a = make_stage(tmp_path / "one")
    b, series_b = make_stage(tmp_path / "two")
    assert sorted(series_a["runs"]) == ["rp90"] and series_a["launch_kind"] == li.DRY and series_a["status"] == "STAGED_OFFLINE_NOT_EXECUTED"
    assert (a / "replay_matrix.csv").read_bytes() == dr.matrix_csv(li.dry_rows())
    assert dr.verify_stage(a)["ok"] is True
    for rel in ("schedule_pre_roll.txt", "schedule_measured.txt", "tx_map.csv"):
        assert (a / "runs" / "rp90" / rel).read_bytes() == (b / "runs" / "rp90" / rel).read_bytes(), rel
    assert series_a["runs"]["rp90"]["schedules"] == series_b["runs"]["rp90"]["schedules"]
    with pytest.raises(FileExistsError):
        make_stage(tmp_path / "one")                                       # the same root again: refused, never merged


def build_set(tmp_path: Path) -> dict:
    snap = s2.make_snapshot(tmp_path)
    path_win = r"D:\PLAN_BLOCK18\evidence_archive" + "\\" + snap["root"].name
    pins_path = tmp_path / "pins.json"
    pins_path.write_bytes(pp.build_pins_bytes(snap["root"], path_win=path_win, required_groups=tuple(s2.GROUPS)))
    manifest_path = tmp_path / "source_manifest.txt"
    manifest_path.write_bytes(pp.build_source_manifest_bytes())
    stage, _ = make_stage(tmp_path)
    target = REPO_ROOT / "p4_replay" / "README.md"
    drift_path = tmp_path / "drift.txt"
    drift_path.write_bytes(sn.signed_additions_text({f"{GROUP}/README.md": pn.sha256_file(target)}).encode("utf-8"))
    return dict(source_manifest=manifest_path, pins=pins_path, stage_root=stage, drift_list=drift_path, snapshot_path=snap["root"], snap=snap)


def names_failed(res):
    return [c["name"] for c in res["checks"] if not c["ok"]]


def test_a_consistent_mechanical_set_passes_and_reports_the_hashes_an_authorization_names(tmp_path):
    s = build_set(tmp_path)
    res = pp.verify_artifacts(**{k: v for k, v in s.items() if k != "snap"})
    assert res["ok"] is True and names_failed(res) == [], res["checks"]
    h = res["hashes"]
    assert h["source_manifest_sha256"] == sha(s["source_manifest"].read_bytes()) and h["pins_sha256"] == sha(s["pins"].read_bytes())
    assert h["stage_sums_sha256"] == li.stage_sums_sha256(s["stage_root"]) and h["s1_evidence_sha256"] == sha(s["drift_list"].read_bytes())
    assert h["entry_sha256"] == pn.sha256_file(REPO_ROOT / "p4_replay" / "live_entry.py")


def test_each_defect_in_a_mechanical_artifact_is_a_failed_check(tmp_path):
    kw = lambda s: {k: v for k, v in s.items() if k != "snap"}      # noqa: E731
    s = build_set(tmp_path / "manifest")
    lines = s["source_manifest"].read_text().splitlines()
    lines[0] = "0" * 64 + "  " + lines[0].split("  ", 1)[1]
    s["source_manifest"].write_text("\n".join(lines) + "\n")
    assert "source manifest exact against the tree (nothing unlisted)" in names_failed(pp.verify_artifacts(**kw(s)))

    s = build_set(tmp_path / "stage")
    (s["stage_root"] / "runs" / "rp90" / "schedule_measured.txt").write_bytes(b"1\n2\n3")
    assert "staged tree intact, unexecuted, dry-run matrix" in names_failed(pp.verify_artifacts(**kw(s)))

    s = build_set(tmp_path / "pins")
    mp = s["snapshot_path"] / "00_manifest" / "archive_manifest.json"
    mp.write_text(mp.read_text() + " ")
    assert "snapshot identity equals the pin (read-only)" in names_failed(pp.verify_artifacts(**kw(s)))

    s = build_set(tmp_path / "crlf")
    s["drift_list"].write_bytes(s["drift_list"].read_bytes().replace(b"\n", b"\r\n"))
    assert "drift list: LF only and well formed" in names_failed(pp.verify_artifacts(**kw(s)))

    s = build_set(tmp_path / "hash")
    s["drift_list"].write_bytes(f"{GROUP}/README.md  {'a' * 64}\n".encode("ascii"))      # bytes, not write_text: Windows write_text would turn LF into CRLF
    assert "drift list: every listed file still has its listed hash" in names_failed(pp.verify_artifacts(**kw(s)))


def test_verify_compares_the_list_with_the_drift_on_disk(tmp_path):
    s = build_set(tmp_path)
    kw = {k: v for k, v in s.items() if k != "snap"}
    snap = make_drift_snapshot(tmp_path / "d", {"README.md": (REPO_ROOT / "p4_replay" / "README.md").read_bytes()}, recorded={})
    good = pp.verify_artifacts(**kw, live_drift=pp.windows_group_drift(snap))
    assert "drift list equals the Windows-side drift on disk" not in names_failed(good)
    extra = make_drift_snapshot(tmp_path / "e", {"README.md": (REPO_ROOT / "p4_replay" / "README.md").read_bytes(), "unlisted.py": b"U"}, recorded={})
    assert "drift list equals the Windows-side drift on disk" in names_failed(pp.verify_artifacts(**kw, live_drift=pp.windows_group_drift(extra)))


def test_allowlist_copy_is_exact_isolated_hashed_and_detects_tampering_and_drift(tmp_path):
    before = s2.tree_digest(REPO_ROOT / "p4_replay")
    dest = tmp_path / "copy"
    res = pp.copy_allowlist(dest)
    assert res["files"] == len(pp.allowlist_paths()) >= 88 + len(pp.EXTRA_ALLOWLIST) and s2.tree_digest(REPO_ROOT / "p4_replay") == before
    sums = (dest / "SHA256SUMS.txt").read_bytes()
    assert sha(sums) == res["sums_sha256"] and b"\r" not in sums and all(re.fullmatch(r"[0-9a-f]{64}  tree/\S.*", ln) for ln in sums.decode().splitlines())
    assert pp.verify_allowlist_copy(dest)["ok"] is True
    assert (dest / "tree" / "p4_replay" / "live_entry.py").read_bytes() == (REPO_ROOT / "p4_replay" / "live_entry.py").read_bytes()
    assert not (dest / "tree" / "p4_replay" / "__pycache__").exists()
    (dest / "tree" / "p4_replay" / "live_entry.py").write_bytes(b"tampered")                 # the COPY is altered: detected
    assert pp.verify_allowlist_copy(dest)["copy_mismatched"] == ["tree/p4_replay/live_entry.py"]
    (dest / "tree" / "stray.txt").write_bytes(b"x")                                          # an unlisted file in the copy: detected
    assert pp.verify_allowlist_copy(dest)["unlisted"] == ["tree/stray.txt"]
    with pytest.raises(FileExistsError):
        pp.copy_allowlist(dest)                                                                # never merged into an existing folder


@pytest.mark.parametrize("bad", ["../outside.py", "/etc/passwd", "p4_replay/does_not_exist.py", "C:\\x.py", "p4_replay"])
def test_the_allowlist_refuses_traversal_absolute_missing_and_directory_entries(bad):
    with pytest.raises(pp.PrepError):
        pp.allowlist_paths(extra=(bad,))


def test_the_preparation_tools_never_write_an_approval_authorization_or_signature():
    forbidden = ("approved_by", "authorized_by", "OWNER_REVIEWED", "approved_utc", "authorized_utc", "m3_convention_review", "thresholds_status")
    for path in sorted(PKG.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        assert not [w for w in forbidden if w in text], (path.name, [w for w in forbidden if w in text])


def test_cli_manifest_refuses_to_overwrite_and_exit_codes_are_meaningful(tmp_path):
    out = tmp_path / "manifest.txt"
    assert pp.main(["manifest", "--out", str(out)]) == 0 and out.is_file()
    first = out.read_bytes()
    assert pp.main(["manifest", "--out", str(out)]) == 2 and out.read_bytes() == first
