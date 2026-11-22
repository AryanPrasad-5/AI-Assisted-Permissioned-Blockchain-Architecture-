"""A0u Workstream B: the deterministic file / directory digest and its path-safety rules (offline; local fixtures and an in-memory file system; nothing is started)."""

from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil

import pytest

from tests import a0u_support as u
from tests.a0u_support import FakeFs, digest, request

from fabric_perf.controllers import tree_digest as td      # noqa: E402

TREE = {"/w/runs/r1/b.txt": ("file", b"bee"), "/w/runs/r1/a.txt": ("file", b"ay"), "/w/runs/r1/sub/c.txt": ("file", b"sea"), "/w/runs/r1/sub/deep/d.txt": ("file", b"dee"),
        "/w/runs": ("dir",), "/w/runs/r1": ("dir",)}


def codes(exc_info) -> str:
    return exc_info.value.code


# ---- 10: correct file SHA-256 and the single-file form
def test_10_a_file_digest_is_the_sha256_of_its_content_and_bound_to_the_dot_path():
    fs = FakeFs({"/w/runs/f.bin": ("file", b"hello"), "/w/runs": ("dir",)})
    result = digest(fs, "/w/runs/f.bin", expected_type="file")
    assert result["entries"] == [{"path": ".", "type": "file", "size": 5, "sha256": hashlib.sha256(b"hello").hexdigest()}]
    assert result["tree_digest"] == td.tree_digest_of(result["entries"]) and result["total_bytes"] == 5


def test_10_a_real_file_system_gives_the_same_content_hash(tmp_path):
    (tmp_path / "w" / "runs").mkdir(parents=True)
    (tmp_path / "w" / "runs" / "f.bin").write_bytes(b"x" * 3_000_000)         # larger than one read chunk
    fs = td.RealFs(lambda p: tmp_path / p.lstrip("/"))
    assert digest(fs, "/w/runs/f.bin", expected_type="file", max_total_bytes=4_000_000)["entries"][0]["sha256"] == hashlib.sha256(b"x" * 3_000_000).hexdigest()


# ---- 11, 12: deterministic digest and stable ordering
def test_11_the_directory_digest_is_deterministic_and_independent_of_listing_order():
    first = digest(FakeFs(TREE))
    shuffled = FakeFs(dict(reversed(list(TREE.items()))))
    assert digest(shuffled)["tree_digest"] == first["tree_digest"] == digest(FakeFs(TREE))["tree_digest"]
    changed = dict(TREE)
    changed["/w/runs/r1/b.txt"] = ("file", b"bex")
    assert digest(FakeFs(changed))["tree_digest"] != first["tree_digest"]


def test_11_content_not_mtime_decides_the_digest():
    fs = FakeFs(TREE)
    before = digest(fs)["tree_digest"]
    fs.mtime["/w/runs/r1/a.txt"] = 999_999
    assert digest(fs)["tree_digest"] == before


def test_12_entries_are_sorted_by_utf8_bytes_with_relative_posix_paths():
    nodes = {"/w/runs/r1/z": ("file", b"1"), "/w/runs/r1/B": ("file", b"2"), "/w/runs/r1/é": ("file", b"3"), "/w/runs/r1/a/b": ("file", b"4"), "/w/runs/r1/a-b": ("file", b"5"), "/w/runs": ("dir",)}
    paths = [e["path"] for e in digest(FakeFs(nodes))["entries"]]
    assert paths == sorted(paths, key=lambda p: p.encode("utf-8")) and paths[0] == "B" and "a/b" in paths and not any(p.startswith("/") or "\\" in p for p in paths)


def test_12_the_real_and_the_in_memory_file_systems_agree(tmp_path):
    for rel, node in TREE.items():
        if node[0] == "file":
            target = tmp_path / rel.lstrip("/")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(node[1])
    assert digest(td.RealFs(lambda p: tmp_path / p.lstrip("/")))["tree_digest"] == digest(FakeFs(TREE))["tree_digest"]


def test_the_digest_definition_is_pinned_by_a_known_vector():
    entries = [{"path": "a", "type": "file", "size": 1, "sha256": hashlib.sha256(b"x").hexdigest()}]
    want = hashlib.sha256(json.dumps({"entries": entries, "schema": "a0u_tree_digest/1"}, sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()
    assert td.tree_digest_of(entries) == want


# ---- 13: symlinks
def link_tree(target: str) -> dict:
    return {**TREE, "/w/runs/r1/lnk": ("link", target)}


def test_13_a_symlink_inside_the_tree_is_refused_by_default():
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs(link_tree("a.txt")))
    assert codes(info) == "symlink_refused"


def test_13_an_internal_symlink_is_recorded_not_followed_under_the_record_policy():
    result = digest(FakeFs(link_tree("a.txt")), symlink_policy="record_internal")
    assert {"path": "lnk", "type": "symlink", "link_target": "a.txt"} in result["entries"] and result["symlink_count"] == 1


@pytest.mark.parametrize("target", ["../../outside", "/etc/passwd", "sub/../../../x"])
def test_13_a_symlink_that_escapes_the_tree_is_refused_even_when_recording_is_allowed(target):
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs(link_tree(target)), symlink_policy="record_internal")
    assert codes(info) == "symlink_escapes_root"


def test_13_a_symlink_as_the_target_or_as_an_ancestor_is_refused():
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs({**TREE, "/w/runs/r1": ("link", "/elsewhere")}))
    assert codes(info) == "target_is_symlink"
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs({**{k.replace("/w/runs", "/w/real"): v for k, v in TREE.items()}, "/w/runs": ("link", "/w/real")}), "/w/runs/r1")
    assert codes(info) == "ancestor_symlink"


def test_13_a_real_symlink_is_refused_when_the_platform_can_create_one(tmp_path):
    (tmp_path / "w" / "runs" / "r1").mkdir(parents=True)
    (tmp_path / "w" / "runs" / "r1" / "f").write_bytes(b"1")
    try:
        os.symlink(tmp_path / "w" / "runs" / "r1" / "f", tmp_path / "w" / "runs" / "r1" / "l")
    except (OSError, NotImplementedError):
        pytest.skip("this account cannot create symbolic links (SKIPPED; the in-memory tests above cover the policy)")
    with pytest.raises(td.DigestRefused) as info:
        digest(td.RealFs(lambda p: tmp_path / p.lstrip("/")))
    assert codes(info) == "symlink_refused"


# ---- 14: path traversal and malformed paths
@pytest.mark.parametrize("target", ["/w/runs/r1/../r2", "/w/runs/../x", "w/runs/r1", "/w/runs/r1/", "/w//runs/r1", "/w/runs/./r1", "/w/runs/r1\\x", "/w/runs/r1\n"])
def test_14_traversal_relative_and_unnormalised_paths_are_refused(target):
    with pytest.raises(td.DigestRefused):
        digest(FakeFs(TREE), target)


def test_14_a_name_with_a_control_character_or_invalid_utf8_is_refused():
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs({**TREE, "/w/runs/r1/bad\nname": ("file", b"1")}))
    assert codes(info) == "name_invalid"
    fs = FakeFs(TREE)
    fs.list_script["/w/runs/r1"] = [["a.txt", "b\udcff.txt"]]
    with pytest.raises(td.DigestRefused) as info:
        digest(fs)
    assert codes(info) == "path_not_utf8"


# ---- 15, 16, 17, 18: missing target, permission failure, change during enumeration, special files
def test_15_a_missing_target_or_ancestor_and_a_type_mismatch_are_refusals():
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs({"/w/runs": ("dir",)}))
    assert codes(info) == "target_missing"
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs({"/w/zzz": ("dir",)}), "/w/runs/r1", approved_root="/w/runs")
    assert codes(info) == "ancestor_missing"
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs(TREE), expected_type="file")
    assert codes(info) == "type_mismatch"
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs({"/w/runs/r1": ("file", b"x"), "/w/runs": ("dir",)}))
    assert codes(info) == "type_mismatch"


@pytest.mark.parametrize("where", ["denied_stat", "denied_list", "denied_open"])
def test_16_a_permission_error_is_a_refusal_never_a_skipped_file(where):
    fs = FakeFs(TREE)
    getattr(fs, where).add({"denied_stat": "/w/runs/r1/sub", "denied_list": "/w/runs/r1/sub", "denied_open": "/w/runs/r1/a.txt"}[where])
    with pytest.raises(td.DigestRefused) as info:
        digest(fs)
    assert codes(info) == "permission_denied"


def test_17_a_file_that_changes_while_it_is_read_is_refused():
    fs = FakeFs(TREE)

    def mutate():
        fs.nodes["/w/runs/r1/a.txt"] = ("file", b"AY")
        fs.mtime["/w/runs/r1/a.txt"] = 5
    fs.on_read["/w/runs/r1/a.txt"] = mutate
    with pytest.raises(td.DigestRefused) as info:
        digest(fs)
    assert codes(info) == "changed_during_enumeration"


def test_17_a_file_whose_size_changes_without_an_mtime_change_is_still_refused():
    fs = FakeFs(TREE)
    fs.on_read["/w/runs/r1/a.txt"] = lambda: fs.nodes.__setitem__("/w/runs/r1/a.txt", ("file", b"longer than before"))
    with pytest.raises(td.DigestRefused) as info:
        digest(fs)
    assert codes(info) == "changed_during_enumeration"


def test_17_a_directory_listing_that_changes_during_the_walk_is_refused():
    fs = FakeFs(TREE)
    fs.list_script["/w/runs/r1/sub"] = [["c.txt", "deep"], ["c.txt", "deep", "new.txt"]]
    with pytest.raises(td.DigestRefused) as info:
        digest(fs)
    assert codes(info) == "changed_during_enumeration"


def test_18_special_files_are_refused():
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs({**TREE, "/w/runs/r1/pipe": ("fifo",)}))
    assert codes(info) == "special_file"


# ---- 19: approved root
def test_19_a_target_outside_the_approved_root_is_refused():
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs({**TREE, "/other/r1/x": ("file", b"1")}), "/other/r1")
    assert codes(info) == "outside_approved_root"
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs(TREE), "/w/runs-evil/r1", approved_root="/w/runs")
    assert codes(info) == "outside_approved_root"


def test_19_the_request_validator_refuses_a_broad_root_a_missing_field_and_an_unknown_key():
    assert td.validate_request(request()) == []
    assert {i["code"] for i in td.validate_request(request(approved_root="/w", target="/w/runs/r1"))} == {"approved_root_too_broad"}
    assert {i["code"] for i in td.validate_request(request(target="/w/other/r1"))} == {"outside_approved_root"}
    assert any(i["code"] == "missing" for i in td.validate_request({k: v for k, v in request().items() if k != "max_depth"}))
    assert any(i["code"] == "unknown_key" for i in td.validate_request({**request(), "shell": "rm -rf /"}))
    for key, value in (("max_entries", 0), ("max_entries", True), ("max_total_bytes", td.HARD_MAX_BYTES + 1), ("max_depth", -1), ("distro", "bad name"), ("phase", "during"), ("operation", "anything"),
                       ("expected_type", "symlink"), ("symlink_policy", "follow"), ("run_id", "a/b"), ("plan_sha256", "ABC")):
        assert td.validate_request(request(**{key: value})), (key, value)


# ---- bounds: unbounded traversal is refused
@pytest.mark.parametrize("bound", [{"max_entries": 3}, {"max_total_bytes": 5}, {"max_depth": 1}])
def test_every_bound_is_enforced_and_required(bound):
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs(TREE), **bound)
    assert codes(info) == "bound_exceeded"
    with pytest.raises(td.DigestRefused) as info:
        digest(FakeFs(TREE), max_entries=None)
    assert codes(info) == "bound_invalid"


# ---- the evidence document
def evidence(fs=None, **req):
    return td.build_evidence(request(**req), fs or FakeFs(TREE), observed_distro="Ubuntu", observed_utc=u.NOW, platform="linux", tool_sha256="c" * 64, library_sha256="d" * 64)


def test_the_evidence_document_binds_identity_hash_algorithm_provenance_and_encoding():
    doc = evidence()
    assert doc["result"]["verdict"] == "DIGESTED" and td.verify_evidence(doc) == []
    assert doc["hash_algorithm"] == "sha256" and doc["digest_definition"] == td.DIGEST_DEFINITION
    assert doc["binding"] == request() and doc["observation"]["observed_distro"] == "Ubuntu" and doc["observation"]["platform"] == "linux" and doc["observation"]["encoding"] == "utf-8"
    assert doc["observation"]["mtime_used_as_hash"] is False and doc["observation"]["tool_sha256"] == "c" * 64 and doc["observation"]["observed_utc"] == u.NOW
    assert td.flat_file_map(doc)["sub/deep/d.txt"] == hashlib.sha256(b"dee").hexdigest()


def test_a_refusal_is_a_document_that_cannot_be_mistaken_for_a_digest():
    doc = evidence(FakeFs({"/w/runs": ("dir",)}))
    assert doc["result"]["verdict"] == "REFUSED" and doc["result"]["code"] == "target_missing" and doc["tree"] is None
    assert td.verify_evidence(doc)[0]["code"] == "not_digested"
    wrong =td.build_evidence(request(), FakeFs(TREE), observed_distro="Debian", observed_utc=u.NOW, platform="linux")
    assert wrong["result"]["code"] == "distro_mismatch"
    absent = td.build_evidence(request(), FakeFs(TREE), observed_distro=None, observed_utc=u.NOW, platform="linux")
    assert absent["result"]["code"] == "distro_mismatch"


def test_an_edited_document_or_tree_is_detected():
    doc = evidence()
    edited = json.loads(json.dumps(doc))
    edited["tree"]["entries"][0]["size"] = 999
    assert {p["code"] for p in td.verify_evidence(edited)} >= {"document_digest"}
    forged = json.loads(json.dumps(doc))
    forged["tree"]["entries"][0]["sha256"] = "0" * 64
    forged["document_sha256"] = td._document_digest(forged)
    assert "tree_digest" in {p["code"] for p in td.verify_evidence(forged)}
    unsorted = json.loads(json.dumps(doc))
    unsorted["tree"]["entries"].reverse()
    unsorted["document_sha256"] = td._document_digest(unsorted)
    assert "entries_not_sorted_or_duplicated" in {p["code"] for p in td.verify_evidence(unsorted)}


def test_the_library_is_python_3_9_compatible_and_imports_no_process_or_environment_api():
    path = u.REPO / "src/fabric_perf/controllers/tree_digest.py"
    text = path.read_text(encoding="utf-8")
    ast.parse(text, feature_version=(3, 9))
    ast.parse((u.REPO / "scripts/e1/e1_wsl_tree_digest.py").read_text(encoding="utf-8"), feature_version=(3, 9))
    imported = {n.names[0].name.split(".")[0] for n in ast.walk(ast.parse(text)) if isinstance(n, ast.Import)} | {n.module.split(".")[0] for n in ast.walk(ast.parse(text)) if isinstance(n, ast.ImportFrom) and n.module}
    assert imported <= {"__future__", "hashlib", "json", "posixpath", "re", "stat", "pathlib", "typing"}
    for needle in ("environ", "subprocess", "os.system", "Popen", "import socket", ".write(", ".write_bytes(", ".write_text(", "unlink", "rmdir", "rename(", "chmod", "open(\"w", "open('w", "\"wb\"", "\"xb\""):
        assert needle not in text, needle


# ---- the WSL-side script
def run_tool(capsys, fs, *, env_distro="Ubuntu", platform="linux", **req):
    r = request(**req)
    argv = ["digest", "--target", r["target"], "--approved-root", r["approved_root"], "--expect", r["expected_type"], "--run-id", r["run_id"], "--variant", r["variant"],
            "--plan-sha256", r["plan_sha256"], "--path-record-sha256", r["path_record_sha256"], "--distro", r["distro"], "--phase", r["phase"], "--operation", r["operation"],
            "--symlinks", r["symlink_policy"], "--max-entries", str(r["max_entries"]), "--max-bytes", str(r["max_total_bytes"]), "--max-depth", str(r["max_depth"])]
    code = u.tool.main(argv, fs=fs, env_distro=env_distro, platform=platform, now_utc=u.NOW)
    out = capsys.readouterr().out
    assert out.count("\n") == 1, "exactly one JSON document and nothing else on stdout"
    return code, json.loads(out)


def test_the_script_prints_one_document_and_exits_0_for_a_digest_and_3_for_a_refusal(capsys):
    code, doc = run_tool(capsys, FakeFs(TREE))
    assert (code, doc["result"]["verdict"]) == (0, "DIGESTED") and td.verify_evidence(doc) == []
    code, doc = run_tool(capsys, FakeFs({"/w/runs": ("dir",)}))
    assert (code, doc["result"]["code"]) == (3, "target_missing")
    code, doc = run_tool(capsys, FakeFs(TREE), env_distro="Debian")
    assert (code, doc["result"]["code"]) == (3, "distro_mismatch")


def test_the_script_never_presents_a_non_linux_digest_as_a_wsl_observation(capsys):
    code, doc = run_tool(capsys, FakeFs(TREE), platform="win32")
    assert (code, doc["result"]["code"], doc["tree"]) == (3, "not_a_linux_process", None) and doc["observation"]["platform"] == "win32"


def test_the_script_has_no_default_for_any_argument(capsys):
    assert u.tool.main(["digest", "--target", "/w/runs/r1"]) == 2
    assert u.tool.main(["digest"]) == 2
    assert u.tool.main([]) == 2


def test_the_script_is_read_only_a_real_tree_is_unchanged_after_digesting(tmp_path, capsys):
    root = tmp_path / "w" / "runs" / "r1"
    (root / "sub").mkdir(parents=True)
    (root / "a.txt").write_bytes(b"ay")
    (root / "sub" / "c.txt").write_bytes(b"sea")
    before = sorted((p.relative_to(tmp_path).as_posix(), p.stat().st_mtime_ns, p.stat().st_size) for p in tmp_path.rglob("*"))
    run_tool(capsys, td.RealFs(lambda p: tmp_path / p.lstrip("/")))
    assert sorted((p.relative_to(tmp_path).as_posix(), p.stat().st_mtime_ns, p.stat().st_size) for p in tmp_path.rglob("*")) == before
    shutil.rmtree(tmp_path / "w")
