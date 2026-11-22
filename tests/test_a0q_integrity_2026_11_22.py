"""A0q workstreams F-G (integrity items 41-45): earlier manifests untouched, the A0q candidate manifest verifies, historical pins preserved, timing and configuration compatibility, and no
process / network / Git use by the new code. Reads and hashes files only."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

from tests.a0o_support import REPO

from fabric_perf.controllers import issue_log_bridge_recovery as rec
from fabric_perf.controllers import prelive_integrity as pi

MANIFESTS = REPO / "docs" / "research" / "e1"
EARLIER = {
    "prelive_candidate_manifest_2026-11-21.json": "13640a7f683adcfec96f26f9060b05f29306bb2ebcfeb4c4b90c5fc43fa2b984",
    "prelive_candidate_manifest_2026-11-21_a0g.json": "491c7d0c296515bf24cd2a8d4462be3f34a81926f933e5d7542a45105eb5a5e7",
    "prelive_candidate_manifest_2026-11-21_a0h.json": "d6c451cfae089642d8c5c6be2c80d1fbdef7f718d1d2888c95d86275c1fda689",
    "prelive_candidate_manifest_2026-11-21_a0i.json": "7bc73bc7f648c92e3d515e4e9e98f41df1a552d2c9bd8bad204f78761d2a7c66",
    "prelive_candidate_manifest_2026-11-21_a0j.json": "831cb76ecdbd75318ebc7d4fe9be0d0c3eedfe7873a9a2190fa3446dd736c594",
    "prelive_candidate_manifest_2026-11-21_a0k.json": "d648a7151bf7d2688d2f658594a4d5adae4d08201b4428de5924372688ff756c",
    "prelive_candidate_manifest_2026-11-21_a0l.json": "b256ba9e04e699be412d362c3d167a7c5b58204c442cb3bb17afac5c2b0efcb4",
    "prelive_candidate_manifest_2026-11-21_a0m.json": "64687487752fc097f4d18e5e86593936421b0738dc967d0c23f06b3b3ccdd0fd",
    "prelive_candidate_manifest_2026-11-21_a0o.json": "e152ebae4d5f35cbea36acac3ca8d1854cf1899f1aa3ccac616c2a44e7d8ecf7",
    "prelive_candidate_manifest_2026-11-22_a0p.json": "f9843de8837cac54c677ee3bbf47bcee2dc8c30937e66de616c284d9bf394e92",
}
A0Q_MANIFEST = MANIFESTS / "prelive_candidate_manifest_2026-11-22_a0q.json"
A0Q_NEW = {"src/fabric_perf/controllers/issue_log_bridge_recovery.py", "scripts/e1/e1_a0q_crash_matrix.py", "scripts/e1/e1_a0q_candidate_manifest.py",
           "tests/recorder_v3_selection_guard.py", "tests/test_a0j_integrity_2026_11_21.py", "tests/test_a0m_integrity_2026_11_21.py"}
# files the generator route depends on that A0q must NOT have changed (their hashes are the A0p manifest's)
UNCHANGED = ("src/fabric_perf/controllers/sourcecount_reader.py", "src/fabric_perf/controllers/issue_log_coverage.py", "src/fabric_perf/controllers/issue_log_reader.py",
             "src/fabric_perf/controllers/collector_health.py", "src/fabric_perf/controllers/launch_variant.py", "src/fabric_perf/controllers/workload_coverage.py",
             "src/fabric_perf/controllers/recorder_health_incremental.py", "src/fabric_perf/controllers/timing_durable.py", "src/fabric_perf/controllers/timing_sampler.py",
             "scripts/e1/e1_a0m_dry_render.py")


def sha(path: Path) -> str:
    return pi.sha256_file(path)


def test_41_every_earlier_candidate_manifest_is_byte_identical():
    for name, want in EARLIER.items():
        assert sha(MANIFESTS / name) == want, name


def test_42_the_a0q_candidate_manifest_verifies_and_stays_unapproved():
    doc = json.loads(A0Q_MANIFEST.read_text(encoding="utf-8"))
    assert pi.verify_candidate_manifest(doc, REPO) == []
    assert doc["status"] == "CANDIDATE_NOT_APPROVED" and doc["approved"] is False and doc["approval_record"] is None
    assert doc["changed_since_a0p"] == [] and doc["superseded_entries"] == [] and doc["removed_since_a0p"] == []
    assert set(doc["added_since_a0p"]) == A0Q_NEW and len(doc["entries"]) == 99
    assert all(e["approval_status"] != "APPROVED" for e in doc["entries"])
    assert all(e["pinned_sha256"] is None for e in doc["entries"] if e["path"] in A0Q_NEW)                  # A0q pins nothing
    assert doc["supersedes"]["sha256"] == EARLIER["prelive_candidate_manifest_2026-11-22_a0p.json"]
    changed = doc["changed_verification_files"]
    assert [c["path"] for c in changed] == ["tests/test_a0j_integrity_2026_11_21.py", "tests/test_a0m_integrity_2026_11_21.py"]
    sums = {line.partition(" ")[2].lstrip(" *"): line.partition(" ")[0] for line in
            (REPO / "Integration/system_integration/a0m_2026-11-21/SHA256SUMS.txt").read_text(encoding="utf-8").splitlines()}
    for c in changed:
        assert c["old_sha256"] == sums[c["path"]] != c["new_sha256"] == sha(REPO / c["path"])               # the old hash IS the frozen A0m pin; the new one IS the file
    assert doc["launch_configuration"]["live_run"] is False and doc["launch_configuration"]["approved"] is False


def test_43_historical_pins_and_every_a0p_entry_are_preserved():
    a0p = json.loads((MANIFESTS / "prelive_candidate_manifest_2026-11-22_a0p.json").read_text(encoding="utf-8"))
    a0q = json.loads(A0Q_MANIFEST.read_text(encoding="utf-8"))
    old = {e["path"]: e["sha256"] for e in a0p["entries"]}
    new = {e["path"]: e["sha256"] for e in a0q["entries"]}
    assert all(new[path] == digest for path, digest in old.items())                                  # every A0p entry has the same hash
    for path in UNCHANGED:
        assert sha(REPO / path) == old[path], path
    for entry in a0q["entries"]:
        if entry["pinned_sha256"] is not None:
            assert sha(REPO / entry["path"]) == entry["pinned_sha256"]


def test_44_the_new_code_uses_no_process_network_container_model_or_git():
    forbidden_imports = {"socket", "subprocess", "urllib", "http", "requests", "docker", "pickle", "joblib", "sklearn", "numpy", "torch", "multiprocessing", "ssl", "asyncio"}
    for rel in ("src/fabric_perf/controllers/issue_log_bridge_recovery.py", "scripts/e1/e1_a0q_crash_matrix.py", "scripts/e1/e1_a0q_candidate_manifest.py",
                "tests/recorder_v3_selection_guard.py"):
        text = (REPO / rel).read_text(encoding="utf-8")
        tree = ast.parse(text)
        imported = {x.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for x in n.names}
        imported |= {(n.module or "").split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0}
        assert not imported & forbidden_imports, (rel, imported & forbidden_imports)
        assert not re.search(r"git\s+(add|commit|push|stash|checkout|reset|rm)\b", text), rel
        assert "os.system" not in text and "Popen" not in text, rel


def test_45_recovery_never_uses_the_system_clock_as_evidence():
    """The only wall-clock read is the default of the recovery RECORD's own timestamp; every observation time is a recorded read time."""
    tree = ast.parse(Path(rec.__file__).read_text(encoding="utf-8"))
    clock_calls = [(n.lineno, ast.unparse(n)) for n in ast.walk(tree) if isinstance(n, ast.Call) and ast.unparse(n.func) in {"datetime.now", "time.time", "time.monotonic"}]
    assert len(clock_calls) == 1 and "timezone.utc" in clock_calls[0][1]
    source = Path(rec.__file__).read_text(encoding="utf-8")
    assert source.count("wall_clock") >= 3 and "never used as an observation time" in source.lower()


def test_46_timing_and_configuration_modules_of_a0o_and_a0p_are_untouched_and_versions_do_not_mix():
    a0p = json.loads((MANIFESTS / "prelive_candidate_manifest_2026-11-22_a0p.json").read_text(encoding="utf-8"))
    schemas = a0p["schema_versions"]
    a0q = json.loads(A0Q_MANIFEST.read_text(encoding="utf-8"))["schema_versions"]
    assert all(a0q[key] == value for key, value in schemas.items())                                  # no schema of A0p changed meaning
    assert "generator_bridge_recovery/1" in a0q["generator_bridge_recovery_record"]
    # the schema names the recovery reads and writes are the existing ones
    assert rec.RECOVERY_SCHEMA == "generator_bridge_recovery/1"
    from fabric_perf.controllers import issue_log_reader as ilr
    assert ilr.CHECKPOINT_SCHEMA == "e1_issue_log_checkpoint/1" and ilr.ISSUE_LOG_SCHEMA == "e1_issue_log/1"


@pytest.mark.parametrize("name", sorted(EARLIER))
def test_47_manifest_files_are_listed_once_each(name):
    doc = json.loads((MANIFESTS / name).read_text(encoding="utf-8"))
    paths = [e["path"] for e in doc["entries"]]
    assert len(paths) == len(set(paths))
