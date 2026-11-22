"""A0j integrity and safety (checklist items 33-40): historical recorder and renderer pins, protected channels, disabled real inference,
no process/network/Fabric/Caliper use by the new code, frozen evidence, and the successor candidate manifest. Reads and hashes files only.
"""

from __future__ import annotations

import ast
import json
import pickle
import re
import socket
import subprocess
import urllib.request
from pathlib import Path

import pytest

from tests import a0j_support as j
from tests.a0j_support import bridge, rc, wc
from tests.test_a0h_integrity_2026_11_21 import A0E_SHA, A0G_SHA, FROZEN, LIVE_INTEGRATION_SHA
from tests.test_a0i_integrity_2026_11_21 import A0H_SHA
from tests.a0k_pins import a0k_entry_paths, tolerated_in

from fabric_perf.controllers import channel_selection
from fabric_perf.controllers import live_foundation as lf
from fabric_perf.controllers import prelive_integrity as pi
from fabric_perf.models import artifacts

import e1_a0j_candidate_manifest as mk
import e1_a0j_recorder_health_demo as demo

REPO = j.REPO
A0I_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0i.json"
A0J_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0j.json"
A0J_DIR = REPO / "Integration" / "system_integration" / "a0j_2026-11-21"
A0I_MANIFEST_SHA = "7bc73bc7f648c92e3d515e4e9e98f41df1a552d2c9bd8bad204f78761d2a7c66"
RECORDER_V1_SHA = "b5496af3ca03"                     # prefix of the pinned hash recorded in the A0e/A0i manifests
NEW_SOURCES = ("src/fabric_perf/controllers/recorder_health_bridge.py", "src/fabric_perf/controllers/recorder_coverage.py")
NEW_SCRIPTS = ("scripts/e1/e1_a0j_recorder_health_demo.py", "scripts/e1/e1_a0j_candidate_manifest.py")
NEW_JS = "scripts/e1/caliper/e1_per_tx_recorder_v3.js"
A0I_PINNED_AND_SIBLINGS = ("src/fabric_perf/controllers/workload_coverage.py", "src/fabric_perf/controllers/collector_health.py",
                           "src/fabric_perf/controllers/captured_history.py", "src/fabric_perf/controllers/stage3_predictive.py",
                           "src/fabric_perf/controllers/stage3_shadow_runner.py", "src/fabric_perf/controllers/abstention_accounting.py")


def manifest() -> dict:
    return json.loads(A0J_MANIFEST.read_text(encoding="utf-8"))


def a0i() -> dict:
    return json.loads(A0I_MANIFEST.read_text(encoding="utf-8"))


# ----------------------------------------------------------------------------------- 33 recorder v1, 34 pinned renderer unchanged
def test_33_recorder_v1_and_v2_are_byte_identical_to_the_manifest_entries_and_pins():
    entries = {e["path"]: e for e in a0i()["entries"]}
    v1 = entries["scripts/e1/caliper/e1_per_tx_recorder.js"]
    assert j.s.sha(REPO / v1["path"]) == v1["sha256"] == v1["pinned_sha256"] and v1["sha256"].startswith(RECORDER_V1_SHA)
    v2 = entries["scripts/e1/caliper/e1_per_tx_recorder_v2.js"]
    assert j.s.sha(REPO / v2["path"]) == v2["sha256"]                         # v3 extends v2; it does not edit it
    wire = entries["scripts/e1/e1_recorder_v2_wire.py"]
    assert j.s.sha(REPO / wire["path"]) == wire["sha256"]


def test_33b_v3_requires_v2_as_a_sibling_and_adds_only_files_it_names():
    source = (REPO / NEW_JS).read_text(encoding="utf-8")
    assert "require('./e1_per_tx_recorder_v2.js')" in source and "e1_per_tx_recorder.js'" not in source.split("require('./e1_per_tx_recorder_v2.js')")[1][:200]
    assert source.count("Object.assign") >= 1 and "child_process" not in source


def test_34_every_pinned_entry_still_matches_its_pin_including_the_renderer():
    pinned = [e for e in a0i()["entries"] if e["pinned_sha256"]]
    assert {e["path"] for e in pinned} >= {"scripts/e1/e1_la2_render.py", "scripts/e1/caliper/e1_per_tx_recorder.js"}
    for entry in pinned:
        assert j.s.sha(REPO / entry["path"]) == entry["pinned_sha256"], entry["path"]


def test_34b_the_a0i_files_that_the_recorder_route_builds_on_are_byte_identical_to_the_a0i_manifest():
    entries = {e["path"]: e for e in a0i()["entries"]}
    again = tolerated_in(a0i()["entries"])                                              # A0k edited three of these: each is checked against its A0k entry instead
    for path in A0I_PINNED_AND_SIBLINGS:
        if path not in again:
            assert j.s.sha(REPO / path) == entries[path]["sha256"], path


# ------------------------------------------------------------------------------------------------- 35 protected channels rejected
@pytest.mark.parametrize("channel", ["e1-r002", "E1-CFG001", " e1-r002 ", "e1-b001", "e1-ql01-b40-l10", "not-an-e1-channel", ""])
def test_35_the_bridge_refuses_protected_and_earlier_channels_and_creates_no_journal(tmp_path, channel):
    rec = j.short_recorder(tmp_path)
    rec.write()
    with pytest.raises((channel_selection.ChannelSelectionError, wc.CoverageEvidenceError)):
        bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=j.FAST, channel_id=channel)
    assert not (tmp_path / "journal.jsonl").exists()


def test_35b_a_dedicated_channel_is_accepted_and_recorded_unchanged(tmp_path):
    rec = j.short_recorder(tmp_path)
    rec.write()
    bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=j.FAST, channel_id="e1-val-a")
    assert json.loads((tmp_path / "journal.jsonl").read_text(encoding="utf-8").splitlines()[0])["channel_id"] == "e1-val-a"


# --------------------------------------------------------------------------------------------------------- 36 real inference disabled
def test_36_real_inference_is_still_a_single_false_constant_and_the_new_code_never_reads_it():
    source = (REPO / "src/fabric_perf/controllers/stage3_predictive.py").read_text(encoding="utf-8")
    assert re.findall(r"^REAL_INFERENCE_AUTHORIZED\s*=\s*(\w+)\s*$", source, re.M) == ["False"]
    for relative in NEW_SOURCES + NEW_SCRIPTS:
        text = (REPO / relative).read_text(encoding="utf-8")
        assert "REAL_INFERENCE_AUTHORIZED" not in text and "load_hfbtp_artifact" not in text and "joblib" not in text, relative


# -------------------------------------------------------------------- 37 no process or network, 38 no Fabric or Caliper access
@pytest.fixture()
def forbidden(monkeypatch):
    fired: list[str] = []

    def trap(name):
        def refuse(*args, **kwargs):
            fired.append(name)
            raise AssertionError(f"{name} is forbidden")
        return refuse
    import joblib
    for target, attr in ((subprocess, "Popen"), (subprocess, "run"), (socket, "socket"), (urllib.request, "urlopen"), (pickle, "load"),
                         (pickle, "loads"), (joblib, "load")):
        monkeypatch.setattr(target, attr, trap(f"{target.__name__}.{attr}"))
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", trap("StreamingEWMA"))
    monkeypatch.setattr(artifacts, "load_hfbtp_artifact", trap("load_hfbtp_artifact"))
    yield fired
    assert fired == []


def test_37_38_the_whole_recorder_pipeline_runs_with_process_network_and_model_loading_trapped(tmp_path, forbidden):
    out = tmp_path / "demo"
    assert demo.main(["--out-dir", str(out)]) == 0
    fresh = json.loads((out / "demo_summary.json").read_text(encoding="utf-8"))
    assert [r["manifest_verifies"] for r in fresh["runs"]] == [True] * 6 and fresh["synthetic"] is True and fresh["live_source_coverage_verified"] is False
    stored = json.loads((A0J_DIR / "demo" / "demo_summary.json").read_text(encoding="utf-8"))
    assert fresh == stored                                                           # the stored demonstration is exactly reproducible
    assert mk.build(REPO, created_utc="2026-11-21T00:00:00+00:00")["entries"]


def test_37b_the_new_python_code_imports_no_process_network_model_container_or_numeric_library():
    banned = {"subprocess", "socket", "urllib", "http", "requests", "joblib", "pickle", "sklearn", "shutil", "numpy", "pandas", "multiprocessing", "ctypes",
              "docker", "paramiko", "asyncio", "os"}
    for relative in NEW_SOURCES + NEW_SCRIPTS:
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
                   {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module and not n.level}
        assert not imported & banned, (relative, imported & banned)


def test_38_the_v3_recorder_requires_no_process_or_network_module_and_names_no_fabric_or_caliper_client():
    source = (REPO / NEW_JS).read_text(encoding="utf-8")
    required = set(re.findall(r"require\('([^']+)'\)", source))
    assert required == {"fs", "path", "./e1_per_tx_recorder_v2.js"}, required
    for needle in ("child_process", "net'", "http", "dgram", "docker", "fabric-network", "@hyperledger", "peer ", "orderer"):
        assert needle not in source.replace("Nothing here talks to Fabric, Docker or a network.", ""), needle


def test_38b_the_new_python_code_names_no_container_or_fabric_command():
    for relative in NEW_SOURCES + ("scripts/e1/e1_a0j_recorder_health_demo.py",):
        text = (REPO / relative).read_text(encoding="utf-8").lower()
        for needle in ("docker run", "docker exec", "wsl.exe", "peer channel", "peer chaincode", "caliper launch", "prometheus", "requests.get"):
            assert needle not in text, (relative, needle)


def test_38c_the_new_code_writes_only_new_files_it_is_given():
    for relative in ("src/fabric_perf/controllers/recorder_coverage.py",):
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        assert not calls & {"write_text", "write_bytes", "unlink", "rmdir", "rename", "mkdir", "touch"}, relative
    tree = ast.parse((REPO / "src/fabric_perf/controllers/recorder_health_bridge.py").read_text(encoding="utf-8"))
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not calls & {"write_text", "write_bytes", "unlink", "rmdir", "rename", "mkdir", "touch"}
    assert not [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "open"]


def test_38d_the_bridge_writes_only_the_journal_it_is_given(tmp_path):
    rec = j.short_recorder(tmp_path)
    rec.write()
    before = sorted(p.name for p in tmp_path.iterdir())
    bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=j.FAST)
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(before + ["journal.jsonl"])


# ---------------------------------------------------------------------------------------------------------- 39 frozen evidence
def test_39_frozen_evidence_and_earlier_manifests_are_byte_identical():
    assert j.s.sha(j.s.h.f.CANDIDATE_MANIFEST) == A0E_SHA and j.s.sha(j.s.h.A0G_MANIFEST) == A0G_SHA
    assert j.s.sha(REPO / "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0h.json") == A0H_SHA
    assert j.s.sha(A0I_MANIFEST) == A0I_MANIFEST_SHA
    assert j.s.sha(REPO / "src/fabric_perf/controllers/live_integration.py") == LIVE_INTEGRATION_SHA
    for path, expected in FROZEN.items():
        assert j.s.sha(REPO / path) == expected, path


def test_39b_exercising_the_new_code_changes_no_stored_file_and_no_modification_time(tmp_path):
    watched = list(FROZEN) + ["experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib",
                              "Integration/system_integration/offline_replay_inputs.json", "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0i.json",
                              "scripts/e1/caliper/e1_per_tx_recorder.js", "scripts/e1/caliper/e1_per_tx_recorder_v2.js"]
    before = {p: (j.s.sha(REPO / p), (REPO / p).stat().st_mtime_ns) for p in watched}
    rec = j.short_recorder(tmp_path, zero={5})
    j.snapshot_for(rec, j.SHORT + 1)
    mk.build(REPO, created_utc="2026-11-21T00:00:00+00:00")
    assert {p: (j.s.sha(REPO / p), (REPO / p).stat().st_mtime_ns) for p in watched} == before


# --------------------------------------------------------------------------------------------------------- 40 candidate manifest
# A0k edited eight files this manifest lists. This manifest is left byte-identical (it records what they were at A0j). A file may differ from its A0j entry ONLY if the
# A0k manifest names it in ``changed_since_a0j`` AND the file on disk matches the A0k entry (``tests/a0k_pins.py``). Any other difference still fails.
def test_40_every_listed_hash_matches_the_file_on_disk():
    m = manifest()
    superseded = tolerated_in(m["entries"])
    findings = pi.verify_candidate_manifest(m, REPO)
    assert {(f.code, f.path) for f in findings} == {("modified", path) for path in superseded} and len(m["entries"]) == 70, findings
    for entry in m["entries"]:
        if entry["path"] not in superseded:
            assert j.s.sha(REPO / entry["path"]) == entry["sha256"] and (REPO / entry["path"]).stat().st_size == entry["size_bytes"], entry["path"]
    assert mk.main(["verify", "--manifest", str(A0J_MANIFEST)]) == (1 if superseded else 0)


def test_40b_the_successor_repeats_every_a0i_entry_and_names_exactly_what_was_added():
    m = manifest()
    old = {e["path"]: e for e in a0i()["entries"]}
    new = {e["path"]: e for e in m["entries"]}
    assert set(old) <= set(new) and len(old) == 66
    assert m["changed_since_a0i"] == [] and {p for p in old if new[p]["sha256"] != old[p]["sha256"]} == set()     # A0j edited no A0i-listed file
    for path, entry in old.items():
        assert {k: new[path][k] for k in ("sha256", "dependency_role", "approval_status", "pinned_sha256")} == \
               {k: entry[k] for k in ("sha256", "dependency_role", "approval_status", "pinned_sha256")}, path
    assert set(m["added_since_a0i"]) == set(new) - set(old) == set(NEW_SOURCES) | {NEW_JS, "scripts/e1/e1_a0j_recorder_health_demo.py"}
    assert new["src/fabric_perf/controllers/live_integration.py"]["sha256"] == LIVE_INTEGRATION_SHA


def test_40c_the_status_stays_candidate_not_approved_and_the_unresolved_dependencies_are_stated():
    m = manifest()
    assert m["status"] == "CANDIDATE_NOT_APPROVED" and m["approved"] is False and m["approval_record"] is None
    assert "nothing in this file is an approval" in m["approval_note"].lower()
    assert m["supersedes"]["sha256"] == A0I_MANIFEST_SHA and m["git"]["branch"] == "research/construct-validity-nov-2026"
    assert all(e["approval_status"] == pi.PENDING_OWNER_APPROVAL for e in m["entries"] if e["path"] in set(m["added_since_a0i"]))
    text = " ".join(m["unresolved_dependencies"])
    for needle in ("NEVER run under real Caliper", "No independent source-completeness counter", "DECLARED by the collector", "passive observer",
                   "engineering proposals", "workers = 1", "not tamper-evident until", "No live process replays"):
        assert needle in text, needle
    assert any("owner approval of the a0j additions" in item.lower() for item in m["open_items"])
    assert "a0j_2026-11-21" in " ".join(item["item"] for item in m["exclusions"])


def test_40d_the_import_closure_is_fully_listed_and_no_frozen_artifact_is_listed():
    listed = {e["path"] for e in manifest()["entries"]} | a0k_entry_paths()           # A0k added modules that the coverage contract and the history now import
    assert mk.a0i.a0h.a0g.local_import_closure(mk.CLOSURE_ROOTS) <= listed
    assert not listed & {"experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib", "data/raw/hfbtp/HFBTP.csv",
                         "data/processed/flashbots/series_2026-09-21_1s.csv.gz"}


def test_40e_the_builder_reproduces_the_stored_entries_and_cannot_overwrite(tmp_path):
    superseded = tolerated_in(manifest()["entries"])
    keep = lambda entries: [e for e in entries if e["path"] not in superseded]      # noqa: E731
    assert keep(mk.build(REPO, created_utc=manifest()["created_utc"])["entries"]) == keep(manifest()["entries"])
    out = tmp_path / "m.json"
    out.write_text("{}", encoding="utf-8")
    assert mk.main(["write", "--out", str(out)]) == 2 and out.read_text(encoding="utf-8") == "{}"


def test_40f_the_verifier_detects_a_changed_file_instead_of_hiding_it():
    forged = json.loads(A0J_MANIFEST.read_text(encoding="utf-8"))
    forged["entries"][0]["sha256"] = "0" * 64
    superseded = tolerated_in(forged["entries"])
    assert {(f.code, f.path) for f in pi.verify_candidate_manifest(forged, REPO)} == {("modified", forged["entries"][0]["path"])} | {("modified", p) for p in superseded}


def test_41_no_a0j_file_stages_commits_or_pushes():
    for relative in NEW_SOURCES + NEW_SCRIPTS + (NEW_JS, "tests/a0j_support.py", "tests/e1/js/a0j_v3_round.js"):
        text = (REPO / relative).read_text(encoding="utf-8")
        assert not re.search(r"git\s+(add|commit|push|stash|checkout|reset|rm)\b", text), relative


def test_42_nothing_selects_recorder_v3_implicitly():
    """The A0d guard keeps v2 from being pulled in by a launcher; the same rule holds for v3. A0q (2026-11-22) replaced the filename/text scan, which failed on every later file that
    merely named v3, with the syntax-aware scan in ``tests/recorder_v3_selection_guard.py``: a mention counts only as a string literal in executable code, a default argument, a dynamic
    name, an environment lookup, a Caliper ``module`` entry or a benchmark-shaped document. Its injected-selector and behavioural checks are in test_a0q_recorder_v3_guard_2026_11_22.py.
    The pinned renderer still writes v1 by default."""
    from tests import recorder_v3_selection_guard as guard
    violations, _records = guard.scan_tree(REPO)
    assert violations == [], [(f.path, f.line, f.kind) for f in violations]
    render = (REPO / "scripts/e1/e1_la2_render.py").read_text(encoding="utf-8")
    assert "e1_per_tx_recorder_v3" not in render and "e1_per_tx_recorder_v2" not in render
