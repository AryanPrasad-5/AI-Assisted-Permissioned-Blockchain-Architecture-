"""Task A0e (2026-11-21), Workstream D: candidate integrity manifest. Hashes only; nothing here is an approval."""

import ast
import hashlib
import json
from pathlib import Path

import pytest

from fabric_perf.controllers import prelive_integrity as pi
from tests.a0e_support import REPO, sha, tree_hashes

import e1_prelive_validation as pv  # noqa: E402  (importable once tests.a0e_support has put scripts/e1 on sys.path)

STORED = REPO / "docs/research/e1/prelive_candidate_manifest_2026-11-21.json"   # outside the roots the A0d v2-naming scan covers
SUMS = REPO / "Integration/system_integration/observability_recovery_2026-11-17/SHA256SUMS.txt"
# The historical recovery manifest, hashed on 2026-11-21 before any A0e change. It is verified here, never rewritten.
SUMS_SHA256 = "843c601a803ca68d3ffbdea84e758ed5e45bf8ee214c6da5071471b26e407a23"


def mini_repo(tmp_path: Path) -> Path:
    root = tmp_path / "mini"
    (root / "pkg").mkdir(parents=True)
    (root / "pkg" / "a.py").write_text('TOOL_VERSION = "tool 0.3 (test)"\nprint(1)\n', encoding="utf-8")
    (root / "pkg" / "b.py").write_text("print(2)\n", encoding="utf-8")
    (root / "pkg" / "c.js").write_bytes(b"// js\n")
    return root


SPECS = [pi.FileSpec("pkg/a.py", "tool a", "role_a"), pi.FileSpec("pkg/b.py", "tool b", "role_b"),
         pi.FileSpec("pkg/c.js", "script c", "role_c", pi.PINNED_ELSEWHERE_UNCHANGED, hashlib.sha256(b"// js\n").hexdigest())]


# ---------------------------------------------------------------- building
def test_a_built_manifest_lists_the_correct_digests_sizes_roles_and_versions(tmp_path):
    root = mini_repo(tmp_path)
    manifest = pi.build_candidate_manifest(root, SPECS, created_utc="2026-11-21T00:00:00+00:00", open_items=["x"],
                                           launch_configuration={"phases": ["prepare"]}, experimental_configuration_provenance={"source": "operator"})
    entries = {e["path"]: e for e in manifest["entries"]}
    assert set(entries) == {"pkg/a.py", "pkg/b.py", "pkg/c.js"}
    for path, entry in entries.items():
        assert entry["sha256"] == hashlib.sha256((root / path).read_bytes()).hexdigest() and entry["size_bytes"] == (root / path).stat().st_size
    assert entries["pkg/a.py"]["version_or_schema"] == "tool 0.3 (test)" and entries["pkg/b.py"]["version_or_schema"] == "unversioned"
    assert entries["pkg/c.js"]["approval_status"] == pi.PINNED_ELSEWHERE_UNCHANGED and entries["pkg/a.py"]["approval_status"] == pi.PENDING_OWNER_APPROVAL
    assert entries["pkg/a.py"]["purpose"] == "tool a" and entries["pkg/a.py"]["dependency_role"] == "role_a"
    assert manifest["launch_configuration"] == {"phases": ["prepare"]} and manifest["experimental_configuration_provenance"] == {"source": "operator"}


def test_a_candidate_manifest_always_says_it_is_not_approved(tmp_path):
    manifest = pi.build_candidate_manifest(mini_repo(tmp_path), SPECS)
    assert manifest["status"] == pi.STATUS_CANDIDATE == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest["approval_record"] is None
    assert "Nothing in this file is an approval" in manifest["approval_note"]


def test_a_missing_file_a_duplicate_or_a_changed_pinned_file_stops_the_build(tmp_path):
    root = mini_repo(tmp_path)
    with pytest.raises(FileNotFoundError):
        pi.build_candidate_manifest(root, [*SPECS, pi.FileSpec("pkg/missing.py", "x", "y")])
    with pytest.raises(ValueError, match="duplicate"):
        pi.build_candidate_manifest(root, [SPECS[0], SPECS[0]])
    (root / "pkg" / "c.js").write_bytes(b"// js changed\n")
    with pytest.raises(ValueError, match="differs from the hash pinned"):
        pi.build_candidate_manifest(root, SPECS)


def test_a_manifest_is_written_only_to_a_path_that_does_not_exist(tmp_path):
    out = tmp_path / "m.json"
    digest = pi.write_new_json(out, {"a": 1})
    assert digest == sha(out)
    before = out.read_bytes()
    with pytest.raises(FileExistsError):
        pi.write_new_json(out, {"a": 2})
    assert out.read_bytes() == before                                                     # the existing file is untouched


def test_git_state_is_read_from_files_not_from_a_process(tmp_path):
    root = tmp_path / "g"
    (root / ".git" / "refs" / "heads").mkdir(parents=True)
    (root / ".git" / "HEAD").write_text("ref: refs/heads/some-branch\n", encoding="utf-8")
    (root / ".git" / "refs" / "heads" / "some-branch").write_text("a" * 40 + "\n", encoding="utf-8")
    assert pi.read_git_head(root) == {"branch": "some-branch", "commit": "a" * 40}
    assert pi.read_git_head(tmp_path / "nowhere") == {"branch": None, "commit": None}
    head = pi.read_git_head(REPO)
    assert head["branch"] == "research/construct-validity-nov-2026" and len(head["commit"] or "") == 40


# ---------------------------------------------------------------- verifying
def test_a_clean_manifest_verifies_and_each_kind_of_change_is_reported(tmp_path):
    root = mini_repo(tmp_path)
    manifest = pi.build_candidate_manifest(root, SPECS)
    assert pi.verify_candidate_manifest(manifest, root) == []
    (root / "pkg" / "b.py").write_text("print('changed')\n", encoding="utf-8")
    (root / "pkg" / "a.py").unlink()
    found = {(f.code, f.path) for f in pi.verify_candidate_manifest(manifest, root)}
    assert found == {("modified", "pkg/b.py"), ("missing", "pkg/a.py")}


def test_a_modified_pinned_file_is_reported_as_a_pin_violation(tmp_path):
    root = mini_repo(tmp_path)
    manifest = pi.build_candidate_manifest(root, SPECS)
    (root / "pkg" / "c.js").write_bytes(b"// other\n")
    codes = {f.code for f in pi.verify_candidate_manifest(manifest, root) if f.path == "pkg/c.js"}
    assert codes == {"modified", "pin_violation"}


@pytest.mark.parametrize("change", [{"approved": True}, {"status": "APPROVED"}, {"schema": "other"}])
def test_a_manifest_that_claims_approval_or_has_the_wrong_schema_is_rejected(tmp_path, change):
    root = mini_repo(tmp_path)
    manifest = {**pi.build_candidate_manifest(root, SPECS), **change}
    assert pi.verify_candidate_manifest(manifest, root)


def test_an_empty_or_duplicated_manifest_is_reported(tmp_path):
    root = mini_repo(tmp_path)
    manifest = pi.build_candidate_manifest(root, SPECS)
    assert pi.verify_candidate_manifest({**manifest, "entries": []}, root)[0].code == "no_entries"
    twice = {**manifest, "entries": [*manifest["entries"], manifest["entries"][0]]}
    assert "duplicate" in {f.code for f in pi.verify_candidate_manifest(twice, root)}


def test_the_sha256sums_verifier_reads_the_three_common_formats_and_writes_nothing(tmp_path):
    (tmp_path / "x.txt").write_text("x", encoding="utf-8")
    (tmp_path / "y.txt").write_text("y", encoding="utf-8")
    hx, hy = hashlib.sha256(b"x").hexdigest(), hashlib.sha256(b"y").hexdigest()
    sums = tmp_path / "SHA256SUMS.txt"
    sums.write_text(f"{hx}  x.txt\n{hy} *./y.txt\n{'1' * 64}  z.txt\n{hx}  y.txt\n\n", encoding="utf-8")
    before = tree_hashes(tmp_path)
    counts = pi.verify_sha256sums(sums)
    assert counts == {"ok": 2, "failed": ["y.txt"], "missing": ["z.txt"], "listed": 4}
    assert tree_hashes(tmp_path) == before


def test_the_module_offers_no_way_to_rewrite_an_existing_manifest_or_approve_one():
    tree = ast.parse((REPO / "src/fabric_perf/controllers/prelive_integrity.py").read_text(encoding="utf-8"))
    modes = [c.args[0].value for c in ast.walk(tree) if isinstance(c, ast.Call) and getattr(c.func, "attr", "") == "open" and c.args
             and isinstance(c.args[0], ast.Constant)]
    assert set(modes) <= {"rb", "xb"}                                                    # read, or exclusive create; never 'w' or 'a'
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert not {n for n in names if "approve" in n or "pin" in n.replace("pinned", "") or "freeze" in n}
    source = (REPO / "src/fabric_perf/controllers/prelive_integrity.py").read_text(encoding="utf-8")
    assert "write_text" not in source and "write_bytes" not in source


# ---------------------------------------------------------------- the stored candidate manifest and the historical manifest
def test_the_stored_candidate_manifest_matches_the_current_files_and_is_not_an_approval():
    """If this fails after a legitimate edit, the candidate manifest is stale: regenerate it to a NEW file (never overwrite)."""
    manifest = json.loads(STORED.read_text(encoding="utf-8"))
    assert pi.verify_candidate_manifest(manifest, REPO) == []
    assert manifest["status"] == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest["approval_record"] is None
    assert {e["approval_status"] for e in manifest["entries"]} == {pi.PENDING_OWNER_APPROVAL, pi.PINNED_ELSEWHERE_UNCHANGED, pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT}
    assert manifest["open_items"] and manifest["git"]["branch"] == "research/construct-validity-nov-2026"


def test_the_stored_candidate_manifest_lists_exactly_the_orchestrators_candidate_files():
    manifest = json.loads(STORED.read_text(encoding="utf-8"))
    assert [e["path"] for e in manifest["entries"]] == [s.path for s in pv.CANDIDATE_SPECS]
    for entry in manifest["entries"]:
        assert set(entry) == {"path", "sha256", "size_bytes", "purpose", "dependency_role", "version_or_schema", "approval_status", "pinned_sha256"}
        assert len(entry["sha256"]) == 64 and entry["purpose"] and entry["dependency_role"]


def test_every_tool_the_launch_manifest_records_is_in_the_candidate_manifest():
    listed = {s.path for s in pv.CANDIDATE_SPECS}
    assert set(pv.TOOL_FILES.values()) <= listed


def test_the_legacy_runner_and_the_pinned_files_are_listed_with_their_earlier_hashes():
    manifest = {e["path"]: e for e in json.loads(STORED.read_text(encoding="utf-8"))["entries"]}
    legacy = manifest["scripts/e1/run_contemporaneous_capture.py"]
    assert legacy["approval_status"] == pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT and legacy["dependency_role"] == "legacy_not_approved"
    assert manifest["scripts/e1/caliper/e1_per_tx_recorder.js"]["sha256"] == "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8"
    assert manifest["scripts/e1/e1_la2_render.py"]["pinned_sha256"] == manifest["scripts/e1/e1_la2_render.py"]["sha256"]


def test_the_historical_recovery_manifest_is_unchanged_and_all_199_entries_verify():
    assert sha(SUMS) == SUMS_SHA256
    counts = pi.verify_sha256sums(SUMS)
    assert counts["ok"] == 199 and counts["failed"] == [] and counts["missing"] == [] and counts["listed"] == 199


def test_the_candidate_manifest_cli_never_overwrites_and_never_touches_the_stored_manifest(tmp_path):
    before = sha(STORED)
    assert pv.main(["candidate-manifest", "--out", str(STORED)]) == 2                      # exists: refused
    assert sha(STORED) == before
    out = tmp_path / "fresh.json"
    assert pv.main(["candidate-manifest", "--out", str(out)]) == 0
    fresh, stored = json.loads(out.read_text(encoding="utf-8")), json.loads(STORED.read_text(encoding="utf-8"))
    assert [(e["path"], e["sha256"]) for e in fresh["entries"]] == [(e["path"], e["sha256"]) for e in stored["entries"]]
