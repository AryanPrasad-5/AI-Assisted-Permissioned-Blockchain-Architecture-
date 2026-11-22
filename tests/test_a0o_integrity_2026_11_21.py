"""A0o workstream F (checklist items 45-50): candidate manifest, historical manifests, frozen files, and the offline-only guarantees of the A0o tools."""

from __future__ import annotations

import ast
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.a0o_support import REPO

from fabric_perf.controllers import prelive_integrity as pi   # noqa: E402

MANIFEST_DIR = REPO / "docs" / "research" / "e1"
A0O = MANIFEST_DIR / "prelive_candidate_manifest_2026-11-21_a0o.json"
A0M = MANIFEST_DIR / "prelive_candidate_manifest_2026-11-21_a0m.json"
# SHA-256 of the eight earlier candidate manifests, recorded at the start of A0o (before any A0o file existed)
HISTORICAL = {
    "prelive_candidate_manifest_2026-11-21.json": "13640a7f683adcfec96f26f9060b05f29306bb2ebcfeb4c4b90c5fc43fa2b984",
    "prelive_candidate_manifest_2026-11-21_a0g.json": "491c7d0c296515bf24cd2a8d4462be3f34a81926f933e5d7542a45105eb5a5e7",
    "prelive_candidate_manifest_2026-11-21_a0h.json": "d6c451cfae089642d8c5c6be2c80d1fbdef7f718d1d2888c95d86275c1fda689",
    "prelive_candidate_manifest_2026-11-21_a0i.json": "7bc73bc7f648c92e3d515e4e9e98f41df1a552d2c9bd8bad204f78761d2a7c66",
    "prelive_candidate_manifest_2026-11-21_a0j.json": "831cb76ecdbd75318ebc7d4fe9be0d0c3eedfe7873a9a2190fa3446dd736c594",
    "prelive_candidate_manifest_2026-11-21_a0k.json": "d648a7151bf7d2688d2f658594a4d5adae4d08201b4428de5924372688ff756c",
    "prelive_candidate_manifest_2026-11-21_a0l.json": "b256ba9e04e699be412d362c3d167a7c5b58204c442cb3bb17afac5c2b0efcb4",
    "prelive_candidate_manifest_2026-11-21_a0m.json": "64687487752fc097f4d18e5e86593936421b0738dc967d0c23f06b3b3ccdd0fd",
}
NEW_RUNTIME = [
    "src/fabric_perf/controllers/sourcecount_reader.py", "scripts/e1/e1_sourcecount_reader.py", "src/fabric_perf/controllers/timing_sampler.py",
    "scripts/e1/e1_a0o_timing_sampler.py", "src/fabric_perf/controllers/launch_variant.py", "scripts/e1/e1_a0o_launch_variant.py",
]
FORBIDDEN_IMPORTS = {"subprocess", "socket", "requests", "http", "urllib", "docker", "asyncio", "multiprocessing", "ssl", "ftplib", "telnetlib", "paramiko", "pickle", "joblib",
                     "sklearn", "torch", "tensorflow", "xgboost", "lightgbm"}
FORBIDDEN_CALLS = {"system", "popen", "Popen", "run", "check_output", "check_call", "startfile", "spawn", "execv", "execve", "fork", "urlopen", "connect", "load"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ----------------------------------------------------------------------------------------------------------------------------- 45 manifest verification
def test_45_the_a0o_candidate_manifest_verifies_and_stays_a_candidate():
    manifest = json.loads(A0O.read_text(encoding="utf-8"))
    assert pi.verify_candidate_manifest(manifest, REPO) == []
    assert manifest["status"] == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest["approval_record"] is None
    assert len(manifest["entries"]) == 85 and manifest["changed_since_a0m"] == [] and manifest["removed_since_a0m"] == [] and manifest["superseded_entries"] == []
    assert manifest["added_since_a0m"] == sorted(NEW_RUNTIME)
    by_path = {e["path"]: e for e in manifest["entries"]}
    for path in NEW_RUNTIME:
        assert by_path[path]["sha256"] == sha(REPO / path) and by_path[path]["approval_status"] == pi.PENDING_OWNER_APPROVAL and by_path[path]["pinned_sha256"] is None
    assert manifest["supersedes"]["sha256"] == sha(A0M) and manifest["launch_configuration"]["approved"] is False and manifest["launch_configuration"]["live_run"] is False
    assert {e["approval_status"] for e in manifest["entries"]} <= {pi.PENDING_OWNER_APPROVAL, pi.PINNED_ELSEWHERE_UNCHANGED, pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT}
    assert not any("approved" in str(e["approval_status"]).lower().replace("pending_owner_approval", "").replace("excluded_not_approved_entry_point", "") for e in manifest["entries"])
    assert any("six A0o additions has not been given" in item for item in manifest["open_items"])


def test_45b_the_a0m_entries_are_repeated_with_unchanged_hashes():
    a0o = {e["path"]: e["sha256"] for e in json.loads(A0O.read_text(encoding="utf-8"))["entries"]}
    a0m = {e["path"]: e["sha256"] for e in json.loads(A0M.read_text(encoding="utf-8"))["entries"]}
    assert len(a0m) == 79 and all(a0o[p] == h for p, h in a0m.items())
    assert pi.verify_candidate_manifest(json.loads(A0M.read_text(encoding="utf-8")), REPO) == []


def test_45c_a_tampered_candidate_manifest_or_file_is_detected(tmp_path):
    manifest = json.loads(A0O.read_text(encoding="utf-8"))
    manifest["entries"][-1]["sha256"] = "0" * 64
    assert [f.code for f in pi.verify_candidate_manifest(manifest, REPO)] == ["modified"]
    manifest["entries"][-1]["path"] = "src/fabric_perf/controllers/not_a_file.py"
    assert "missing" in {f.code for f in pi.verify_candidate_manifest(manifest, REPO)}
    approved = json.loads(A0O.read_text(encoding="utf-8"))
    approved["approved"] = True
    assert "status" in {f.code for f in pi.verify_candidate_manifest(approved, REPO)}
    sandbox = tmp_path / "repo"
    (sandbox / "src/fabric_perf/controllers").mkdir(parents=True)
    copy = json.loads(A0O.read_text(encoding="utf-8"))
    copy["entries"] = [e for e in copy["entries"] if e["path"] in NEW_RUNTIME]
    for e in copy["entries"]:
        (sandbox / e["path"]).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / e["path"], sandbox / e["path"])
    assert pi.verify_candidate_manifest(copy, sandbox) == []
    victim = sandbox / "src/fabric_perf/controllers/launch_variant.py"
    victim.write_text(victim.read_text() + "\n# edited\n")
    assert [f.path for f in pi.verify_candidate_manifest(copy, sandbox)] == ["src/fabric_perf/controllers/launch_variant.py"]


# ----------------------------------------------------------------------------------------------------------------------------- 46-47 historical and frozen
def test_46_the_eight_earlier_candidate_manifests_are_byte_identical_to_their_starting_hashes():
    for name, digest in HISTORICAL.items():
        assert sha(MANIFEST_DIR / name) == digest, name


def test_47_pinned_and_frozen_files_are_unchanged():
    manifest = json.loads(A0O.read_text(encoding="utf-8"))
    pinned = [e for e in manifest["entries"] if e["pinned_sha256"] is not None]
    pinned_in_a0m = {e["path"]: e["pinned_sha256"] for e in json.loads(A0M.read_text(encoding="utf-8"))["entries"] if e["pinned_sha256"] is not None}
    assert {e["path"]: e["pinned_sha256"] for e in pinned} == pinned_in_a0m                                  # the same pins, none added, none dropped
    assert {"scripts/e1/e1_la2_render.py", "scripts/e1/caliper/e1_per_tx_recorder.js"} <= set(pinned_in_a0m) and all(sha(REPO / e["path"]) == e["pinned_sha256"] for e in pinned)
    assert sha(REPO / "scripts/e1/e1_la2_render.py").startswith("f77a83e7") and sha(REPO / "scripts/e1/caliper/e1_per_tx_recorder.js").startswith("b5496af3")
    # none of the A0e / A0m / A0l runtime sources was edited by A0o: every one of the 79 A0m entries still has the A0m hash (test 45b) and, specifically, the two orchestrators
    a0m = {e["path"]: e["sha256"] for e in json.loads(A0M.read_text(encoding="utf-8"))["entries"]}
    for path in ("scripts/e1/e1_prelive_validation.py", "scripts/e1/e1_a0m_dry_render.py", "src/fabric_perf/controllers/issue_log_reader.py",
                 "src/fabric_perf/controllers/issue_log_coverage.py", "src/fabric_perf/controllers/recorder_health_bridge.py", "src/fabric_perf/controllers/source_completeness.py"):
        assert sha(REPO / path) == a0m[path], path
    # the stored historical evidence the new tools read is read-only data: its digests are the ones recorded in the A0m evidence manifest
    stored = REPO / "docs/research/e1/a0m_dry_render_evidence_2026-11-21/render/a0m-evidence-001/a0m_config_manifest.json"
    recorded = json.loads(stored.read_text(encoding="utf-8"))["files"]
    base = stored.parent
    assert all(sha(base / name) == digest for name, digest in recorded.items())


# ----------------------------------------------------------------------------------------------------------------------------- 48-49 offline only
def imports_and_calls(rel: str):
    tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
    names, calls = set(), []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
        elif isinstance(node, ast.Call):
            func = node.func
            calls.append(func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", ""))
    return names, calls


@pytest.mark.parametrize("rel", NEW_RUNTIME)
def test_48_no_a0o_runtime_file_imports_or_calls_a_process_network_container_or_model_api(rel):
    names, calls = imports_and_calls(rel)
    assert not names & FORBIDDEN_IMPORTS, (rel, names & FORBIDDEN_IMPORTS)
    assert not set(calls) & (FORBIDDEN_CALLS - {"load"}), (rel, set(calls) & FORBIDDEN_CALLS)


def test_49_no_a0o_runtime_file_can_start_caliper_node_docker_or_wsl_and_every_execute_or_follow_path_is_refused():
    for rel in NEW_RUNTIME:
        text = (REPO / rel).read_text(encoding="utf-8")
        for token in ("os.environ", "getenv", "shutil.which", "wsl.exe", "docker run", "docker exec", "npx ", "node ", "caliper launch", "caliper-cli"):
            assert token not in text.replace("Node built-ins", ""), (rel, token)
    import importlib.util
    refusals = {"scripts/e1/e1_a0o_launch_variant.py": ["execute"], "scripts/e1/e1_sourcecount_reader.py": ["follow"]}
    for rel, argv in refusals.items():
        spec = importlib.util.spec_from_file_location(Path(rel).stem, REPO / rel)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.main(argv) == 5
    assert "REAL_INFERENCE_AUTHORIZED = True" not in "".join((REPO / rel).read_text(encoding="utf-8") for rel in NEW_RUNTIME)


# ----------------------------------------------------------------------------------------------------------------------------- 50 no staging or commits
def test_50_nothing_is_staged_and_head_is_the_starting_commit():
    git = shutil.which("git")
    if git is None:
        pytest.skip("git is not available")
    staged = subprocess.run([git, "-C", str(REPO), "diff", "--cached", "--name-only"], capture_output=True, text=True, check=False, timeout=60)
    head = subprocess.run([git, "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True, check=False, timeout=60)
    assert staged.returncode == 0 and staged.stdout.strip() == ""
    assert head.stdout.strip() == "8c67a4678b3901c8d601fb1a33210d7d8c84cf78"
