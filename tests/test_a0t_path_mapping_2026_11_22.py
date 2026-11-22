"""A0t workstream B: Windows <-> WSL <-> container path mapping. Pure string rules plus local file-system checks in a temporary directory. No WSL, Docker or network."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from tests import a0t_support as t
from tests.a0t_support import CONTAINER_MOUNT, MOUNT_ROOT, REPO

from fabric_perf.controllers import path_mapping as pm      # noqa: E402


def code_of(fn, *a, **k):
    with pytest.raises(pm.PathMappingError) as info:
        fn(*a, **k)
    return info.value.code


# ---- 18, 19: drive mapping and WSL mapping
def test_18_windows_drive_paths_map_to_the_wsl_mount():
    assert pm.windows_to_wsl(r"C:\OJT_SEM3\repo\file.txt", mount_root="/mnt") == "/mnt/c/OJT_SEM3/repo/file.txt"
    assert pm.windows_to_wsl("D:/PLAN_BLOCK18", mount_root="/mnt") == "/mnt/d/PLAN_BLOCK18"
    assert pm.windows_to_wsl("E:\\", mount_root="/mnt") == "/mnt/e"


def test_19_wsl_paths_map_back_only_under_a_drive_mount():
    assert pm.wsl_to_windows("/mnt/c/OJT_SEM3/repo", mount_root="/mnt") == "C:\\OJT_SEM3\\repo"
    assert code_of(pm.wsl_to_windows, "/home/mailf18/e1", mount_root="/mnt") == "wsl_native_path_has_no_drive_equivalent"
    assert code_of(pm.wsl_to_windows, "/mnt/cc/x", mount_root="/mnt") == "wsl_native_path_has_no_drive_equivalent"
    assert code_of(pm.wsl_to_windows, "/mnt", mount_root="/mnt") == "wsl_native_path_has_no_drive_equivalent"


def test_the_automount_root_has_no_default_and_is_never_slash():
    with pytest.raises(TypeError):
        pm.windows_to_wsl(r"C:\x")                                       # keyword-only and required
    assert code_of(pm.windows_to_wsl, r"C:\x", mount_root=None) == "mount_root_missing"
    assert code_of(pm.windows_to_wsl, r"C:\x", mount_root="/") == "mount_root_invalid"
    assert code_of(pm.windows_to_wsl, r"C:\x", mount_root="mnt") == "relative_path"


# ---- 20: container mount
def test_20_container_mount_maps_both_ways_and_rejects_outside_paths():
    m = CONTAINER_MOUNT
    assert m.to_container("/home/mailf18/e1/caliper-ws-benchmark/runs/r1/recorder") == "/hyperledger/caliper/workspace/runs/r1/recorder"
    assert m.to_wsl("/hyperledger/caliper/workspace/runs/r1") == "/home/mailf18/e1/caliper-ws-benchmark/runs/r1"
    assert m.to_container("/home/mailf18/e1/caliper-ws-benchmark") == "/hyperledger/caliper/workspace"
    assert code_of(m.to_container, "/home/mailf18/e1/caliper-ws-benchmark-other/x") == "mount_root_mismatch"       # segment-wise, not a string prefix
    assert code_of(m.to_wsl, "/hyperledger/caliper/workspace2/x") == "mount_root_mismatch"
    assert code_of(m.to_container, "/mnt/c/x") == "mount_root_mismatch"


# ---- 21: whitespace
def test_21_whitespace_inside_a_path_is_preserved_and_the_vector_is_not_split():
    assert pm.windows_to_wsl(r"C:\Program Files\My App\out dir", mount_root="/mnt") == "/mnt/c/Program Files/My App/out dir"
    argv = pm.translate_argv_for_wsl(["tool", "--out", r"C:\Program Files\My App\out dir", "x y"], mount_root="/mnt")
    assert argv == ["tool", "--out", "/mnt/c/Program Files/My App/out dir", "x y"] and len(argv) == 4


# ---- 22: UNC; mixed separators; drive-relative
@pytest.mark.parametrize("path", [r"\\server\share\x", r"\\wsl$\Ubuntu\home\u", r"\\?\C:\x", "//server/share/x"])
def test_22_unc_paths_are_unsupported(path):
    assert pm.classify(path) == "unc"
    assert code_of(pm.windows_to_wsl, path, mount_root="/mnt") == "unc_unsupported"
    assert code_of(pm.translate_token_for_wsl, path, mount_root="/mnt") == "unc_unsupported"


def test_mixed_separators_are_normalised_and_drive_relative_is_refused():
    assert pm.windows_to_wsl(r"C:\a/b\c/d", mount_root="/mnt") == "/mnt/c/a/b/c/d"
    assert pm.windows_to_wsl("C:/a//b/./c", mount_root="/mnt") == "/mnt/c/a/b/c"
    assert code_of(pm.windows_to_wsl, "C:relative", mount_root="/mnt") == "drive_relative_unsupported"
    assert code_of(pm.windows_to_wsl, "C:", mount_root="/mnt") == "drive_relative_unsupported"


# ---- 23: relative paths
@pytest.mark.parametrize("path", ["a/b", r"a\b", "./a", "file.txt"])
def test_23_relative_paths_are_rejected_by_the_mapper(path):
    assert code_of(pm.windows_to_wsl, path, mount_root="/mnt") == "relative_path"
    assert code_of(pm.normalize_posix, path) == "relative_path"


def test_empty_and_non_text_paths_are_rejected():
    assert code_of(pm.windows_to_wsl, "", mount_root="/mnt") == "empty_path"
    assert code_of(pm.windows_to_wsl, "   ", mount_root="/mnt") == "empty_path"
    assert code_of(pm.windows_to_wsl, None, mount_root="/mnt") == "wrong_type"


# ---- 24: traversal
@pytest.mark.parametrize("path", [r"C:\a\..\b", "C:/a/../../etc", "C:/.."])
def test_24_path_traversal_is_rejected_in_every_form(path):
    assert code_of(pm.windows_to_wsl, path, mount_root="/mnt") == "path_traversal"


def test_traversal_in_posix_and_in_argument_tokens():
    assert code_of(pm.normalize_posix, "/home/u/../root") == "path_traversal"
    assert code_of(pm.translate_token_for_wsl, "runs/../../x", mount_root="/mnt") == "path_traversal"
    assert code_of(pm.translate_token_for_wsl, "--out=C:/a/../b", mount_root="/mnt") == "path_traversal"
    assert pm.translate_token_for_wsl("1.5", mount_root="/mnt") == "1.5"                      # a version-like token is not a path


def test_windows_specific_invalid_paths_are_rejected():
    assert code_of(pm.windows_to_wsl, "C:/a/b.", mount_root="/mnt") == "windows_segment_invalid"        # trailing dot is stripped by Windows
    assert code_of(pm.windows_to_wsl, "C:/a/b ", mount_root="/mnt") == "windows_segment_invalid"
    assert code_of(pm.windows_to_wsl, "C:/a/b:stream", mount_root="/mnt") == "windows_character_invalid"
    assert code_of(pm.windows_to_wsl, "C:/a/b?c", mount_root="/mnt") == "windows_character_invalid"
    assert code_of(pm.windows_to_wsl, "C:/a/b\x00c", mount_root="/mnt") == "windows_character_invalid"


# ---- 25: mount-root mismatch
def test_25_mount_root_is_derived_from_a_declared_pair_and_a_mismatch_is_refused():
    assert pm.derive_mount_root(r"C:\OJT_SEM3\repo", "/mnt/c/OJT_SEM3/repo") == "/mnt"
    assert pm.derive_mount_root(r"C:\OJT_SEM3\repo", "/wslmnt/c/OJT_SEM3/repo") == "/wslmnt"
    assert code_of(pm.derive_mount_root, r"C:\OJT_SEM3\repo", "/mnt/d/OJT_SEM3/repo") == "mount_root_mismatch"
    assert code_of(pm.derive_mount_root, r"C:\OJT_SEM3\repo", "/home/u/repo") == "mount_root_mismatch"


def test_declared_repository_pair_in_the_repo_matches_the_checkout():
    assert pm.derive_mount_root(str(REPO), t.repo_wsl()) == MOUNT_ROOT


def test_a_wrong_supplied_automount_root_is_refused_when_building_the_record(tmp_path):
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    assert code_of(t.path_record, plan, mount_root="/wslmnt") == "mount_root_mismatch"
    assert t.path_record(plan, mount_root="/mnt")["automount_root"] == "/mnt"


# ---- 26: case sensitivity
def test_26_case_differences_are_never_folded():
    assert code_of(pm.derive_mount_root, r"C:\OJT_SEM3\Repo", "/mnt/c/ojt_sem3/repo") == "case_mismatch"
    assert not pm.within("/mnt/c/ojt_sem3/x", "/mnt/c/OJT_SEM3")
    assert pm.within("/mnt/c/OJT_SEM3/x", "/mnt/c/OJT_SEM3")
    assert pm.windows_to_wsl(r"C:\Mixed\Case", mount_root="/mnt") == "/mnt/c/Mixed/Case"          # case of the path is preserved; only the drive letter is lower-cased
    assert pm.windows_to_wsl(r"c:\x", mount_root="/mnt") == pm.windows_to_wsl(r"C:\x", mount_root="/mnt")


def test_within_is_segment_wise_and_requires_normalised_absolute_paths():
    assert pm.within("/a/b", "/a/b") and pm.within("/a/b/c", "/a/b")
    assert not pm.within("/a/bc", "/a/b")
    assert code_of(pm.within, "a/b", "/a") == "relative_path"
    assert code_of(pm.require_within, "/x/y", ["/a", "/b"], what="target") == "outside_approved_roots"


# ---- 27: output isolation
def test_27_every_planned_write_lies_inside_the_run_directories(tmp_path):
    from fabric_perf.controllers import process_runner as pr
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    assert pr.check_writes_isolated(plan, mount_root=MOUNT_ROOT) == []
    assert pr.write_roots(plan, mount_root=MOUNT_ROOT) == [pm.windows_to_wsl(plan["layout"]["output_dir"], mount_root=MOUNT_ROOT), plan["layout"]["worker_evidence_root_host"]]


def test_27_a_write_outside_the_approved_roots_is_found(tmp_path):
    from fabric_perf.controllers import process_runner as pr
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    bad = copy.deepcopy(plan)
    bad["steps"][3]["writes"] = ["/home/mailf18/e1/other-directory/x"]
    bad["config_inspection"]["initial"]["block_path"] = "/mnt/c/Users/elsewhere/config.pb"
    problems = pr.check_writes_isolated(bad, mount_root=MOUNT_ROOT)
    assert len(problems) == 2 and {p["code"] for p in problems} == {"outside_approved_roots"}


def test_27_a_traversal_target_is_found(tmp_path):
    from fabric_perf.controllers import process_runner as pr
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    bad = copy.deepcopy(plan)
    bad["steps"][3]["writes"] = [plan["layout"]["output_dir"] + "\\..\\escape"]
    assert {p["code"] for p in pr.check_writes_isolated(bad, mount_root=MOUNT_ROOT)} == {"path_traversal"}


def test_symlink_escape_is_detected_where_the_platform_allows_links(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "inside.txt").write_text("x", encoding="utf-8")
    assert pm.check_symlink_escape(root / "inside.txt", root)["escape_checked"] is True
    assert pm.check_symlink_escape(root / "missing.txt", root) == {"path": str(root / "missing.txt"), "exists": False, "escape_checked": False,
                                                                     "note": "the target does not exist, so a symlink escape cannot be assessed"}
    link = root / "link"
    try:
        os.symlink(outside, link, target_is_directory=True)
    except (OSError, NotImplementedError):
        if os.name != "nt":
            pytest.skip("this account may not create symbolic links")
        import subprocess                                                    # a directory junction needs no privilege; mklink is a local file-system command only
        made = subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(outside)], capture_output=True, text=True, timeout=30)
        if made.returncode != 0 or not link.exists():
            pytest.skip(f"neither a symbolic link nor a junction could be created here: {made.stderr.strip()[:80]}")
    assert code_of(pm.check_symlink_escape, link, root) == "symlink_escape"
    assert code_of(pm.check_symlink_escape, root / "x", tmp_path / "nonexistent") == "root_missing"


# ---- 28: unverified runtime classification + the record
def test_28_the_record_never_claims_runtime_verification(tmp_path):
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    rec = t.path_record(plan)
    assert rec["runtime_verified"] is False and rec["highest_offline_level_reached"] == pm.L_REPO
    assert {e["runtime_status"] for e in rec["entries"]} == {pm.RUNTIME_STATUS}
    assert {e["verification_level"] for e in rec["entries"]} <= set(pm.OFFLINE_LEVELS)
    assert pm.L_RUNTIME not in {e["verification_level"] for e in rec["entries"]}
    assert pm.verify_path_record(rec) == []


def test_28_a_runtime_level_cannot_be_claimed_offline():
    assert code_of(pm._entry, "x", host=None, wsl="/x", container=None, source="s", level=pm.L_RUNTIME) == "verification_level_unreachable"


def test_28_without_a_container_mount_declaration_the_workspace_is_only_syntactic(tmp_path):
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    rec = t.path_record(plan, container_mount=None)
    ws = next(e for e in rec["entries"] if e["role"] == "caliper_workspace")
    assert ws["verification_level"] == pm.L_SYNTACTIC and "UNVERIFIED" in ws["note"] and "operator-supplied mount mapping is required" in ws["note"]
    assert all(e["verification_level"] == pm.L_SYNTACTIC for e in rec["entries"] if e["role"].startswith("round_"))


def test_record_content_fields_and_hashes(tmp_path):
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    rec = t.path_record(plan)
    roles = {e["role"]: e for e in rec["entries"]}
    assert rec["run_id"] == plan["run_id"] and rec["variant"] == plan["variant"] and rec["plan_sha256"] == plan["plan_sha256"]
    assert rec["artifact_hashes"] == plan["artifact_hashes"] and len(rec["artifact_hashes"]) > 5
    assert roles["repository"]["host_path"] == str(REPO) and roles["repository"]["wsl_path"] == t.repo_wsl()
    assert roles["caliper_workspace"]["container_path"] == "/hyperledger/caliper/workspace" and roles["caliper_workspace"]["mapping_source"].startswith("scripts/e1/e1_la2_render.py:233")
    assert roles["worker_run_directory"]["container_path"] == plan["layout"]["container_run_dir"]
    assert "UNC" in roles["worker_run_directory"]["note"]
    assert rec["record_sha256"] and json.loads(json.dumps(rec)) == rec


def test_record_refuses_an_operator_path_that_disagrees_with_the_rule(tmp_path):
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    for field in ("output_dir_wsl", "rendered_dir_wsl", "network_description_wsl", "wsl_repo_path"):
        bad = copy.deepcopy(plan)
        bad["request"]["tools"][field] = "/mnt/c/somewhere/else"
        assert code_of(t.path_record, bad) == "operator_path_disagrees_with_rule", field


def test_record_refuses_a_container_mapping_that_disagrees_with_the_plan(tmp_path):
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    wrong_root = pm.ContainerMount(wsl_root="/home/mailf18/e1/other-ws", container_root="/hyperledger/caliper/workspace", container_name="caliper", source="x")
    assert code_of(t.path_record, plan, container_mount=wrong_root) == "mount_root_mismatch"
    wrong_container = pm.ContainerMount(wsl_root="/home/mailf18/e1/caliper-ws-benchmark", container_root="/workspace", container_name="caliper", source="x")
    assert code_of(t.path_record, plan, container_mount=wrong_container) == "mount_root_mismatch"


def test_record_edits_and_runtime_claims_are_detected(tmp_path):
    plan, _req, _root = t.build_a0t_plan(tmp_path)
    rec = t.path_record(plan)
    edited = copy.deepcopy(rec)
    edited["entries"][0]["wsl_path"] = "/mnt/c/other"
    assert {p["code"] for p in pm.verify_path_record(edited)} == {"record_digest"}
    claimed = copy.deepcopy(rec)
    claimed["runtime_verified"] = True
    assert "runtime_claim" in {p["code"] for p in pm.verify_path_record(claimed)}
    levelled = copy.deepcopy(rec)
    levelled["entries"][0]["verification_level"] = pm.L_RUNTIME
    assert "level_unreachable" in {p["code"] for p in pm.verify_path_record(levelled)}


def test_windows_path_tokens_in_a_wsl_vector_are_translated_and_recorded():
    from fabric_perf.controllers import process_runner as pr
    inv = pr.build_invocation("S", ["tool", "--out-dir", r"C:\Users\x y\out", "--flag=D:/data/f"], None, 10, launch_mode=pr.LAUNCH_FOREGROUND, distro="Ubuntu", mount_root="/mnt")
    assert inv.os_argv == ("wsl.exe", "-d", "Ubuntu", "--exec", "tool", "--out-dir", "/mnt/c/Users/x y/out", "--flag=/mnt/d/data/f")
    assert inv.translations == ((r"C:\Users\x y\out", "/mnt/c/Users/x y/out"), ("--flag=D:/data/f", "--flag=/mnt/d/data/f"))
    assert pr.host_read_path("/mnt/c/Users/x/out.json", mount_root="/mnt") == "C:\\Users\\x\\out.json"
    assert code_of(pr.host_read_path, "/home/mailf18/e1/x", mount_root="/mnt") == "wsl_native_path_has_no_drive_equivalent"
