"""A0g: candidate manifest successor and preservation of historical artifacts (2026-11-21).

Checklist items 36-40. Reads files and hashes them; nothing is written, loaded, unpickled or executed. The frozen model files are
HASHED (streamed bytes, never deserialized) only to show they are byte-identical to the pins that already exist in
``Integration/system_integration/offline_replay_inputs.json``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from tests import a0g_support as g
from tests.a0k_pins import a0k_entry_paths, changed_by_a0k
from fabric_perf.controllers import prelive_integrity as pi

import e1_a0g_candidate_manifest as mk
import e1_prelive_validation as pv

REPO = g.REPO
A0E_MANIFEST_SHA256 = "13640a7f683adcfec96f26f9060b05f29306bb2ebcfeb4c4b90c5fc43fa2b984"   # as recorded by the A0f report
FROZEN_DIR = REPO / "Integration" / "system_integration" / "observability_recovery_2026-11-17"
PINNED_FILES = {"scripts/e1/e1_la2_render.py": "f77a83e7de106b3c1dac28f93333e8dbaf67ea869836944b906f9bc1aabe3d83",
                "scripts/e1/caliper/e1_per_tx_recorder.js": "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8"}


def successor() -> dict:
    return json.loads(g.SUCCESSOR_MANIFEST.read_text(encoding="utf-8"))


# A0h (2026-11-21) edited three files that the A0g manifest lists. The A0g manifest is left byte-identical (it records what those files
# were at A0g); the A0h successor manifest records their current hashes. A file may differ from its A0g entry ONLY if the A0h manifest
# names it in ``changed_since_a0g`` AND the file on disk matches the A0h entry. Any other difference still fails.
A0H_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0h.json"
A0H_SUPERSEDED = {"scripts/e1/e1_stage3_shadow_runner.py", "src/fabric_perf/controllers/stage3_predictive.py",
                  "src/fabric_perf/controllers/stage3_shadow_runner.py"}


# A0i (2026-11-21) edited three more candidate files (the captured-history adapter, the Stage 3 runner and the predictive module). Same rule one level
# up: a file may differ from its A0h entry ONLY if the A0i manifest names it in ``changed_since_a0h`` AND the file on disk matches the A0i entry.
A0I_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0i.json"
A0I_SUPERSEDED = {"src/fabric_perf/controllers/captured_history.py", "src/fabric_perf/controllers/stage3_predictive.py",
                  "src/fabric_perf/controllers/stage3_shadow_runner.py"}


def superseded_by_a0i() -> set:
    if not A0I_MANIFEST.is_file():
        return set()
    a0i = json.loads(A0I_MANIFEST.read_text(encoding="utf-8"))
    current = {e["path"]: e["sha256"] for e in a0i["entries"]}
    paths = set(a0i["changed_since_a0h"])
    assert paths == A0I_SUPERSEDED, sorted(paths ^ A0I_SUPERSEDED)       # no other A0h file may have changed
    again = changed_by_a0k()                                              # A0k edited captured_history.py once more: its A0k entry is checked instead
    for path in paths:
        if path not in again:
            assert sha(REPO / path) == current[path], path                # and each one must match its A0i entry
    return paths


def superseded_by_a0h() -> set:
    if not A0H_MANIFEST.is_file():
        return set()
    a0h = json.loads(A0H_MANIFEST.read_text(encoding="utf-8"))
    current = {e["path"]: e["sha256"] for e in a0h["entries"]}
    paths = set(a0h["changed_since_a0g"])
    assert paths == A0H_SUPERSEDED, sorted(paths ^ A0H_SUPERSEDED)       # no other A0g file may have changed
    later = superseded_by_a0i()
    for path in paths:
        if path not in later:                                             # a file A0i changed again is checked against its A0i entry instead
            assert sha(REPO / path) == current[path], path                # otherwise it must match its A0h entry
    return paths


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 22), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------------------------------- 36 coverage
def test_36_the_successor_lists_every_file_the_stage_3_runner_imports():
    listed = {e["path"] for e in successor()["entries"]}
    if A0H_MANIFEST.is_file():                                            # A0h added modules the runner now imports
        listed |= {e["path"] for e in json.loads(A0H_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    if A0I_MANIFEST.is_file():                                            # and A0i added more
        listed |= {e["path"] for e in json.loads(A0I_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    listed |= a0k_entry_paths()                                           # A0k added modules the history adapter and coverage now import
    closure = mk.local_import_closure()
    assert closure <= listed, sorted(closure - listed)
    assert {"scripts/e1/e1_stage3_shadow_runner.py", "src/fabric_perf/controllers/stage3_shadow_runner.py",
            "src/fabric_perf/controllers/stage3_predictive.py", "src/fabric_perf/controllers/adapters.py",
            "src/fabric_perf/controllers/live_foundation.py", "src/fabric_perf/models/artifacts.py",
            "src/fabric_perf/models/baseline.py", "src/fabric_perf/controllers/telemetry_freshness.py"} <= listed


def test_36b_every_a0e_entry_is_repeated_with_its_role_and_status_unchanged():
    old = {e["path"]: e for e in json.loads(g.A0E_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    new = {e["path"]: e for e in successor()["entries"]}
    assert set(old) <= set(new) and len(old) == 25
    for path, entry in old.items():
        assert {k: new[path][k] for k in ("sha256", "dependency_role", "approval_status", "pinned_sha256")} == \
               {k: entry[k] for k in ("sha256", "dependency_role", "approval_status", "pinned_sha256")}, path
    assert [e["path"] for e in successor()["entries"]][:25] == [s.path for s in pv.CANDIDATE_SPECS]


def test_36c_exclusions_and_unresolved_dependencies_are_explicit():
    manifest = successor()
    excluded = " ".join(item["item"] for item in manifest["exclusions"])
    assert "throughput.joblib" in excluded and "HFBTP.csv" in excluded and "selection_frozen" in excluded
    assert not {e["path"] for e in manifest["entries"]} & {"experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib",
                                                           "experiments/artifacts/hfbtp/metadata.json", "data/raw/hfbtp/HFBTP.csv"}
    assert any("WORKLOAD HISTORY" in item for item in manifest["unresolved_dependencies"])
    assert any("REAL_INFERENCE_AUTHORIZED" in item for item in manifest["unresolved_dependencies"])
    assert any("10 tx/s" in item for item in manifest["unresolved_dependencies"])


# ---------------------------------------------------------------------------------------------------- 37 hashes
def test_37_every_listed_hash_matches_the_file_on_disk():
    manifest = successor()
    superseded = superseded_by_a0h()
    findings = pi.verify_candidate_manifest(manifest, REPO)
    assert {(f.code, f.path) for f in findings} == {("modified", path) for path in superseded}, findings
    for entry in manifest["entries"]:
        if entry["path"] in superseded:
            continue
        assert sha(REPO / entry["path"]) == entry["sha256"] and (REPO / entry["path"]).stat().st_size == entry["size_bytes"]


def test_37b_the_pinned_files_keep_their_earlier_hashes_in_the_successor():
    entries = {e["path"]: e for e in successor()["entries"]}
    for path, expected in PINNED_FILES.items():
        assert entries[path]["sha256"] == expected and sha(REPO / path) == expected
    assert entries["scripts/e1/e1_la2_render.py"]["approval_status"] == pi.PINNED_ELSEWHERE_UNCHANGED
    assert entries["scripts/e1/run_contemporaneous_capture.py"]["approval_status"] == pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT


def test_37c_the_builder_reproduces_the_stored_entries_from_the_current_files():
    superseded = superseded_by_a0h()
    rebuilt = mk.build(REPO, created_utc=successor()["created_utc"])
    keep = lambda entries: [e for e in entries if e["path"] not in superseded]      # noqa: E731
    assert keep(rebuilt["entries"]) == keep(successor()["entries"])


# ---------------------------------------------------------------------------------------------------- 38 frozen manifests
def test_38_the_a0e_candidate_manifest_and_the_recovery_manifest_are_unchanged():
    assert sha(g.A0E_MANIFEST) == A0E_MANIFEST_SHA256
    assert pi.verify_candidate_manifest(json.loads(g.A0E_MANIFEST.read_text(encoding="utf-8")), REPO) == []
    result = pi.verify_sha256sums(FROZEN_DIR / "SHA256SUMS.txt")
    assert result["failed"] == [] and result["missing"] == [] and result["ok"] == result["listed"] == 199
    assert successor()["supersedes"]["sha256"] == A0E_MANIFEST_SHA256


# ---------------------------------------------------------------------------------------------------- 39 model artifacts
def test_39_the_historical_model_artifacts_are_byte_identical_to_their_existing_pins():
    pins = json.loads(g.MODEL_PINS.read_text(encoding="utf-8"))["files"]
    for key in ("model_metadata", "throughput_model", "latency_model", "selection", "series"):
        path = REPO / pins[key]["path"]
        assert path.is_file(), path
        assert sha(path).upper() == pins[key]["sha256"].upper(), key


# ---------------------------------------------------------------------------------------------------- 40 status
def test_40_approval_status_remains_unapproved_everywhere():
    manifest = successor()
    assert manifest["status"] == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest["approval_record"] is None
    statuses = {e["approval_status"] for e in manifest["entries"]}
    assert statuses == {pi.PENDING_OWNER_APPROVAL, pi.PINNED_ELSEWHERE_UNCHANGED, pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT}
    new = [e for e in manifest["entries"] if e["path"] not in {s.path for s in pv.CANDIDATE_SPECS}]
    assert new and all(e["approval_status"] == pi.PENDING_OWNER_APPROVAL for e in new)
    assert "not an approval" in manifest["approval_note"].lower() or "nothing in this file is an approval" in manifest["approval_note"].lower()
    assert manifest["git"]["branch"] == "research/construct-validity-nov-2026"


def test_the_builder_cannot_overwrite_and_refuses_a_missing_file(tmp_path):
    out = tmp_path / "m.json"
    out.write_text("{}", encoding="utf-8")
    assert mk.main(["write", "--out", str(out)]) == 2 and out.read_text(encoding="utf-8") == "{}"
    with pytest.raises(FileNotFoundError):
        pi.build_candidate_manifest(REPO, [pi.FileSpec("src/fabric_perf/does_not_exist.py", "x", "y")])


def test_the_builder_detects_a_changed_file_instead_of_hiding_it(tmp_path):
    (tmp_path / "a.py").write_text("one", encoding="utf-8")
    manifest = pi.build_candidate_manifest(tmp_path, [pi.FileSpec("a.py", "x", "y")])
    (tmp_path / "a.py").write_text("two", encoding="utf-8")
    assert {f.code for f in pi.verify_candidate_manifest(manifest, tmp_path)} == {"modified"}


def test_the_manifest_modules_use_no_process_network_or_model_api():
    import ast
    for path in (REPO / "scripts/e1/e1_a0g_candidate_manifest.py",):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
                   {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert not imported & {"subprocess", "socket", "urllib", "http", "requests", "joblib", "pickle", "sklearn", "shutil", "os"}
