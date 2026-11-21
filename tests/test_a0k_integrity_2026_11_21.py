"""A0k integrity and safety (checklist items 38-44): earlier manifests and stored evidence unchanged, the successor candidate manifest, protected channels,
pinned artifacts, no process/network/Fabric/Caliper use, no frozen-model loading, and no Git staging. Reads and hashes files; nothing is loaded or executed
beyond offline code.
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
from tests import a0k_pins as pins
from tests import a0k_support as k
from tests.a0k_support import C, DOMAIN, FAST, SHORT, at, bridge, rc, sc, wc, wk
from tests.test_a0h_integrity_2026_11_21 import A0E_SHA, A0G_SHA, FROZEN, LIVE_INTEGRATION_SHA
from tests.test_a0i_integrity_2026_11_21 import A0H_SHA

from fabric_perf.controllers import captured_history as ch
from fabric_perf.controllers import channel_selection
from fabric_perf.controllers import collector_health as chj
from fabric_perf.controllers import live_foundation as lf
from fabric_perf.controllers import prelive_integrity as pi
from fabric_perf.models import artifacts

import e1_a0i_coverage_demo as a0i_demo
import e1_a0j_recorder_health_demo as a0j_demo
import e1_a0k_candidate_manifest as mk

REPO = j.REPO
A0I_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0i.json"
A0J_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0j.json"
A0K_MANIFEST = pins.A0K_MANIFEST
A0I_MANIFEST_SHA = "7bc73bc7f648c92e3d515e4e9e98f41df1a552d2c9bd8bad204f78761d2a7c66"
A0J_MANIFEST_SHA = "831cb76ecdbd75318ebc7d4fe9be0d0c3eedfe7873a9a2190fa3446dd736c594"
RECORDER_V1_SHA = "b5496af3ca03"                                   # prefix of the pinned hash recorded in the A0e/A0i manifests
NEW_SOURCES = ("src/fabric_perf/controllers/source_completeness.py", "src/fabric_perf/controllers/worker_coverage.py")
NEW_SCRIPTS = ("scripts/e1/e1_a0k_candidate_manifest.py",)
EVIDENCE_DIR = REPO / "Integration" / "system_integration" / "a0k_2026-11-21"
V3 = next((REPO / "scripts" / "e1" / "caliper").glob("*recorder_v3.js")).relative_to(REPO).as_posix()


def manifest() -> dict:
    return json.loads(A0K_MANIFEST.read_text(encoding="utf-8"))


def a0j() -> dict:
    return json.loads(A0J_MANIFEST.read_text(encoding="utf-8"))


# ------------------------------------------------------------------------------------------------------- 38 previous manifests unchanged
def test_38_every_earlier_manifest_and_frozen_input_is_byte_identical():
    assert j.s.sha(j.s.h.f.CANDIDATE_MANIFEST) == A0E_SHA and j.s.sha(j.s.h.A0G_MANIFEST) == A0G_SHA
    assert j.s.sha(REPO / "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0h.json") == A0H_SHA
    assert j.s.sha(A0I_MANIFEST) == A0I_MANIFEST_SHA and j.s.sha(A0J_MANIFEST) == A0J_MANIFEST_SHA
    assert j.s.sha(REPO / "src/fabric_perf/controllers/live_integration.py") == LIVE_INTEGRATION_SHA
    for path, expected in FROZEN.items():
        assert j.s.sha(REPO / path) == expected, path


def _sums(directory: Path):
    for line in (directory / "SHA256SUMS.txt").read_text(encoding="utf-8").splitlines():
        digest, _sep, name = line.partition(" ")
        name = name.lstrip(" *")
        yield digest, (directory / name[2:] if name.startswith("./") else REPO / name)


# The A0i and A0j SHA256SUMS also list some source and test files; A0k edited eight candidate sources and nine test/support files on purpose (the manifest and the
# A0k report name them). Those lines are the only ones allowed to differ; every stored demo, journal and result file must still verify.
EDITED_BY_A0K = pins.A0K_CHANGED | {"tests/a0i_support.py", "tests/test_a0i_coverage_2026_11_21.py", "tests/test_a0i_integrity_2026_11_21.py", "tests/a0j_support.py",
                                    "tests/test_a0j_coverage_2026_11_21.py", "tests/test_a0j_integrity_2026_11_21.py", "tests/test_a0g_integrity_2026_11_21.py",
                                    "tests/test_a0h_integrity_2026_11_21.py", "tests/e1/js/a0j_v3_round.js"}


@pytest.mark.parametrize("directory", ["a0i_2026-11-21", "a0j_2026-11-21"])
def test_38b_the_stored_a0i_and_a0j_evidence_still_verifies_against_its_own_sums(directory):
    base = REPO / "Integration" / "system_integration" / directory
    checked, drifted = 0, set()
    for digest, path in _sums(base):
        assert path.is_file(), path
        if j.s.sha(path) != digest:
            drifted.add(path.relative_to(REPO).as_posix())
        checked += 1
    assert drifted <= EDITED_BY_A0K, sorted(drifted - EDITED_BY_A0K)
    assert checked >= 70 and checked - len(drifted) >= 60            # most lines are stored demo, journal and result files, and all of them verify


def test_38c_exercising_the_new_code_changes_no_stored_file_and_no_modification_time(tmp_path):
    watched = list(FROZEN) + ["experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib",
                              "Integration/system_integration/offline_replay_inputs.json", "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0i.json",
                              "docs/research/e1/prelive_candidate_manifest_2026-11-21_a0j.json", "scripts/e1/caliper/e1_per_tx_recorder.js",
                              "scripts/e1/caliper/e1_per_tx_recorder_v2.js", "Integration/system_integration/a0j_2026-11-21/demo/demo_summary.json"]
    before = {p: (j.s.sha(REPO / p), (REPO / p).stat().st_mtime_ns) for p in watched}
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    k.recorder_coverage(rec, tmp_path, source_evidence=k.dispatch_evidence(rec))[1].snapshot(at(SHORT + 1))
    mk.build(REPO, created_utc="2026-11-21T00:00:00+00:00")
    assert {p: (j.s.sha(REPO / p), (REPO / p).stat().st_mtime_ns) for p in watched} == before


# ---------------------------------------------------------------------------------------------------------- 39 successor manifest correctness
def test_39_every_listed_hash_matches_the_file_on_disk():
    m = manifest()
    assert pi.verify_candidate_manifest(m, REPO) == [] and len(m["entries"]) == 72
    for entry in m["entries"]:
        assert j.s.sha(REPO / entry["path"]) == entry["sha256"] and (REPO / entry["path"]).stat().st_size == entry["size_bytes"], entry["path"]
    assert mk.main(["verify", "--manifest", str(A0K_MANIFEST)]) == 0


def test_39b_the_successor_repeats_every_a0j_entry_and_names_exactly_what_changed_and_what_was_added():
    m = manifest()
    old = {e["path"]: e for e in a0j()["entries"]}
    new = {e["path"]: e for e in m["entries"]}
    assert set(old) <= set(new) and len(old) == 70
    changed = {p for p in old if new[p]["sha256"] != old[p]["sha256"]}
    assert changed == set(m["changed_since_a0j"]) == pins.A0K_CHANGED
    assert set(m["added_since_a0j"]) == set(new) - set(old) == pins.A0K_ADDED == set(NEW_SOURCES)
    for path, entry in old.items():
        assert {kk: new[path][kk] for kk in ("dependency_role", "approval_status", "pinned_sha256")} == {kk: entry[kk] for kk in ("dependency_role", "approval_status", "pinned_sha256")}, path
        if path not in changed:
            assert new[path]["sha256"] == entry["sha256"], path
    assert new["src/fabric_perf/controllers/live_integration.py"]["sha256"] == LIVE_INTEGRATION_SHA


def test_39c_every_changed_file_is_a_candidate_not_a_pinned_or_approved_one_and_its_change_is_explained():
    m = manifest()
    old = {e["path"]: e for e in a0j()["entries"]}
    assert [e["path"] for e in m["superseded_entries"]] == sorted(pins.A0K_CHANGED)
    for entry in m["superseded_entries"]:
        before = old[entry["path"]]
        assert before["approval_status"] == pi.PENDING_OWNER_APPROVAL and before["pinned_sha256"] is None, entry["path"]       # editable: never frozen, never approved
        assert entry["a0j_sha256"] == before["sha256"] and entry["a0k_sha256"] == j.s.sha(REPO / entry["path"]) and entry["a0j_sha256"] != entry["a0k_sha256"]
        assert entry["reason"] and not entry["reason"].startswith("UNEXPLAINED")


def test_39d_the_status_stays_candidate_not_approved_and_the_unresolved_dependencies_are_stated():
    m = manifest()
    assert m["status"] == "CANDIDATE_NOT_APPROVED" and m["approved"] is False and m["approval_record"] is None
    assert "nothing in this file is an approval" in m["approval_note"].lower()
    assert m["supersedes"]["sha256"] == A0J_MANIFEST_SHA and m["git"]["branch"] == "research/construct-validity-nov-2026"
    assert all(e["approval_status"] == pi.PENDING_OWNER_APPROVAL for e in m["entries"] if e["path"] in set(m["added_since_a0j"]) | set(m["changed_since_a0j"]))
    text = " ".join(m["unresolved_dependencies"])
    for needle in ("Source completeness remains UNVERIFIED", "DECLARED by the collector", "max_callback_lag_ms is a declared, unmeasured assumption",
                   "source_stream_observer is a DECLARED claim", "multi-worker E1 run cannot happen today", "declare no coverage scope", "left byte-identical"):
        assert needle in text, needle
    assert any("owner approval of the a0k additions" in item.lower() for item in m["open_items"])
    assert "a0k_2026-11-21" in " ".join(item["item"] for item in m["exclusions"])


def test_39e_the_import_closure_is_fully_listed_and_no_frozen_artifact_is_listed():
    listed = {e["path"] for e in manifest()["entries"]}
    assert mk.a0j.a0i.a0h.a0g.local_import_closure(mk.CLOSURE_ROOTS) <= listed
    assert not listed & {"experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib", "data/raw/hfbtp/HFBTP.csv",
                         "data/processed/flashbots/series_2026-09-21_1s.csv.gz"}


def test_39f_the_builder_reproduces_the_stored_entries_and_cannot_overwrite(tmp_path):
    assert mk.build(REPO, created_utc=manifest()["created_utc"])["entries"] == manifest()["entries"]
    out = tmp_path / "m.json"
    out.write_text("{}", encoding="utf-8")
    assert mk.main(["write", "--out", str(out)]) == 2 and out.read_text(encoding="utf-8") == "{}"


def test_39g_the_verifier_detects_a_changed_file_instead_of_hiding_it():
    forged = json.loads(A0K_MANIFEST.read_text(encoding="utf-8"))
    forged["entries"][0]["sha256"] = "0" * 64
    assert [f.code for f in pi.verify_candidate_manifest(forged, REPO)] == ["modified"]


def test_39h_the_pin_helper_tolerates_exactly_the_eight_changed_files_and_nothing_else():
    assert pins.changed_by_a0k() == pins.A0K_CHANGED and len(pins.A0K_CHANGED) == 8
    old_entries = a0j()["entries"]
    assert pins.tolerated_in(old_entries) == pins.A0K_CHANGED
    assert pins.tolerated_in([{"path": "src/fabric_perf/controllers/stage3_predictive.py"}]) == set()


# ------------------------------------------------------------------------------------------------------------- 40 protected channels
@pytest.mark.parametrize("channel", ["e1-r002", "E1-CFG001", " e1-r002 ", "e1-b001", "e1-r001", "e1-ql01-b40-l10", "e1-cp02-b45-l20", "not-an-e1-channel", ""])
def test_40_the_journal_writer_and_the_bridge_still_refuse_protected_and_earlier_channels(tmp_path, channel):
    path = tmp_path / "j.jsonl"
    with pytest.raises((channel_selection.ChannelSelectionError, wc.CoverageEvidenceError)):
        chj.CollectorHealthJournal(path, source_id="s", collector_id="c", run_id="r", channel_id=channel)
    assert not path.exists()
    rec = j.short_recorder(tmp_path)
    rec.write()
    with pytest.raises((channel_selection.ChannelSelectionError, wc.CoverageEvidenceError)):
        bridge.ingest_spool(rec.spool_path, tmp_path / "journal.jsonl", source_id=j.SOURCE_ID, policy=FAST, channel_id=channel)
    assert not (tmp_path / "journal.jsonl").exists()


def test_40b_a_dedicated_channel_is_accepted_and_the_declared_scope_is_recorded_beside_it(tmp_path):
    journal = chj.CollectorHealthJournal(tmp_path / "j.jsonl", source_id="s", collector_id="c", run_id="r", channel_id="e1-val-a", clock=j.s.ScriptClock())
    journal.start(observation_start_utc=at(0), coverage_scope=wc.SCOPE_SOURCE_STREAM)
    row = json.loads((tmp_path / "j.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["channel_id"] == "e1-val-a" and row["coverage_scope"] == "source_stream_observer"


# -------------------------------------------------------------------------------------------------------------- 41 pinned artifacts preserved
def test_41_the_pinned_recorder_renderer_and_runner_files_still_match_their_pins():
    entries = {e["path"]: e for e in a0j()["entries"]}
    v1 = entries["scripts/e1/caliper/e1_per_tx_recorder.js"]
    assert j.s.sha(REPO / v1["path"]) == v1["pinned_sha256"] and v1["pinned_sha256"].startswith(RECORDER_V1_SHA)
    pinned = [e for e in manifest()["entries"] if e["pinned_sha256"]]
    assert {e["path"] for e in pinned} >= {"scripts/e1/e1_la2_render.py", "scripts/e1/caliper/e1_per_tx_recorder.js"}
    for entry in pinned:
        assert j.s.sha(REPO / entry["path"]) == entry["pinned_sha256"], entry["path"]
    v2 = entries["scripts/e1/caliper/e1_per_tx_recorder_v2.js"]
    assert j.s.sha(REPO / v2["path"]) == v2["sha256"]                                  # v3 still extends v2; neither v1 nor v2 was edited


def test_41b_the_v3_recorder_still_requires_v2_and_adds_only_the_descriptor_pass_through():
    source = (REPO / V3).read_text(encoding="utf-8")
    assert "require('./e1_per_tx_recorder_v2.js')" in source and source.count("Object.assign") >= 1 and "child_process" not in source
    assert set(re.findall(r"require\('([^']+)'\)", source)) == {"fs", "path", "./e1_per_tx_recorder_v2.js"}
    assert "source_counter" in source and "sourceCount.descriptor" in source


def test_41c_the_scope_and_source_kind_vocabularies_agree_across_python_and_javascript():
    assert bridge.COVERAGE_SCOPE_RECORDER == wc.SCOPE_RECORDER_CALLBACK == sc.SCOPE_RECORDER_CALLBACK == "recorder_callback"
    assert sc.SCOPE_SOURCE_STREAM == wc.SCOPE_SOURCE_STREAM and set(wc.COVERAGE_SCOPES) == {"recorder_callback", "source_stream_observer"}
    assert ch.SOURCE_KIND_CALIPER_CLIENT == rc.RecorderArrivalSource(source_id="s", utc_day="2026-09-21").kind
    assert f"const COVERAGE_SCOPE = '{wc.SCOPE_RECORDER_CALLBACK}';" in (REPO / V3).read_text(encoding="utf-8")


# --------------------------------------------------------------------------------- 42 no process, network, Fabric or Caliper; 43 no frozen model
@pytest.fixture()
def forbidden(monkeypatch):
    fired: list[str] = []

    def trap(name):
        def refuse(*args, **kwargs):
            fired.append(name)
            raise AssertionError(f"{name} is forbidden")
        return refuse
    import joblib
    for target, attr in ((subprocess, "Popen"), (subprocess, "run"), (socket, "socket"), (urllib.request, "urlopen"), (pickle, "load"), (pickle, "loads"), (joblib, "load")):
        monkeypatch.setattr(target, attr, trap(f"{target.__name__}.{attr}"))
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", trap("StreamingEWMA"))
    monkeypatch.setattr(artifacts, "load_hfbtp_artifact", trap("load_hfbtp_artifact"))
    yield fired
    assert fired == []


def test_42_43_the_whole_a0k_pipeline_runs_with_process_network_and_model_loading_trapped(tmp_path, forbidden):
    (tmp_path / "a").mkdir()
    rec = j.short_recorder(tmp_path / "a", zero={5}, source_count=False)
    cov = k.recorder_coverage(rec, tmp_path / "a", source_evidence=k.dispatch_evidence(rec))[1]
    assert cov.snapshot(at(SHORT + 1)).classify(at(5), 0)[0] == C.VERIFIED_ZERO
    (tmp_path / "b").mkdir()
    recs = {0: k.short_worker(tmp_path / "b", 0, zero={5}), 1: k.short_worker(tmp_path / "b", 1, zero={5})}
    multi, _per = k.worker_coverage(tmp_path / "b", recs)
    assert multi.snapshot(at(SHORT + 1)).classify(at(5), 0)[0] == C.VERIFIED_ZERO
    out = tmp_path / "demo"
    assert a0j_demo.main(["--out-dir", str(out)]) == 0
    assert [r["manifest_verifies"] for r in json.loads((out / "demo_summary.json").read_text(encoding="utf-8"))["runs"]] == [True] * 6
    assert a0i_demo.main(["--out-dir", str(tmp_path / "demo_a0i")]) == 0
    assert mk.build(REPO, created_utc="2026-11-21T00:00:00+00:00")["entries"]


def test_42b_the_new_modules_import_no_process_network_model_container_numeric_or_os_library():
    banned = {"subprocess", "socket", "urllib", "http", "requests", "joblib", "pickle", "sklearn", "shutil", "numpy", "pandas", "multiprocessing", "ctypes",
              "docker", "paramiko", "asyncio", "os", "sys", "importlib"}
    for relative in NEW_SOURCES:
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
                   {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module and not n.level}
        assert not imported & banned, (relative, imported & banned)


def test_42c_the_new_modules_name_no_container_fabric_or_caliper_command_and_write_no_files():
    for relative in NEW_SOURCES:
        text = (REPO / relative).read_text(encoding="utf-8")
        for needle in ("docker run", "docker exec", "wsl.exe", "peer channel", "peer chaincode", "caliper launch", "prometheus", "requests.get", "REAL_INFERENCE_AUTHORIZED",
                       "load_hfbtp_artifact", "joblib"):
            assert needle.lower() not in text.lower(), (relative, needle)
        tree = ast.parse(text)
        calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        assert not calls & {"write_text", "write_bytes", "unlink", "rmdir", "rename", "mkdir", "touch", "fsync"}, relative
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "open"], relative


def test_43b_real_inference_is_still_a_single_false_constant_and_no_a0k_source_reads_it():
    source = (REPO / "src/fabric_perf/controllers/stage3_predictive.py").read_text(encoding="utf-8")
    assert re.findall(r"^REAL_INFERENCE_AUTHORIZED\s*=\s*(\w+)\s*$", source, re.M) == ["False"]
    for relative in NEW_SOURCES + NEW_SCRIPTS + tuple(sorted(pins.A0K_CHANGED - {V3, "scripts/e1/e1_a0i_coverage_demo.py", "scripts/e1/e1_a0j_recorder_health_demo.py"})):
        text = (REPO / relative).read_text(encoding="utf-8")
        assert "REAL_INFERENCE_AUTHORIZED" not in text and "load_hfbtp_artifact" not in text and "joblib" not in text, relative


# ------------------------------------------------------------------------------------------------------ 44 no staging, no commit, no push
def test_44_no_a0k_file_stages_commits_or_pushes():
    files = list(NEW_SOURCES + NEW_SCRIPTS + (V3, "tests/a0k_support.py", "tests/a0k_pins.py", "tests/e1/js/e1_per_tx_recorder_v3_descriptor.test.js",
                                              "Integration/system_integration/a0k_2026-11-21/mutation_check.py"))
    files += [f"tests/test_a0k_{name}_2026_11_21.py" for name in ("coverage_scope", "source_completeness", "workers", "stage3", "integrity")]
    for relative in files:
        text = (REPO / relative).read_text(encoding="utf-8")
        if relative.endswith(("mutation_check.py", "test_a0k_integrity_2026_11_21.py")):
            text = text.replace("git add", "").replace("git commit", "")
        assert not re.search(r"git\s+(add|commit|push|stash|checkout|reset|rm)\b", text), relative


def test_44b_the_evidence_scripts_write_only_new_files_and_the_mutation_runner_restores_what_it_touches():
    text = (EVIDENCE_DIR / "mutation_check.py").read_text(encoding="utf-8")
    assert "finally:\n            path.write_bytes(original)" in text and "RESTORE FAILED" in text and "if args.out.exists()" in text


def test_44c_nothing_in_the_new_code_selects_the_v3_recorder_implicitly():
    """The A0j guard (test_42) keeps the v3 path out of every source file but its own; the A0k tools respect it by finding the file by pattern."""
    for relative in NEW_SOURCES + NEW_SCRIPTS:
        assert "e1_per_tx_recorder_v3" not in (REPO / relative).read_text(encoding="utf-8"), relative
    assert "e1_per_tx_recorder_v3" not in (EVIDENCE_DIR / "mutation_check.py").read_text(encoding="utf-8")
