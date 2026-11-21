"""A0i integrity and safety (checklist items 43-48): the successor candidate manifest, preservation of historical pins and frozen evidence,
protected-channel rejection, and the computation boundary. Reads and hashes files; nothing is loaded, unpickled or executed beyond offline code.
"""

from __future__ import annotations

import ast
import json
import pickle
import socket
import subprocess
import urllib.request
from pathlib import Path

import pytest

from tests import a0i_support as s
from tests.a0i_support import chj, smoke, wc
from tests.a0k_pins import a0k_entry_paths, tolerated_in
from tests.test_a0h_integrity_2026_11_21 import A0E_SHA, A0G_SHA, FROZEN, LIVE_INTEGRATION_SHA

from fabric_perf.controllers import channel_selection
from fabric_perf.controllers import live_foundation as lf
from fabric_perf.controllers import prelive_integrity as pi
from fabric_perf.models import artifacts

import e1_a0i_candidate_manifest as mk
import e1_a0i_coverage_demo as demo

REPO = s.REPO
A0H_SHA = "d6c451cfae089642d8c5c6be2c80d1fbdef7f718d1d2888c95d86275c1fda689"
NEW_SOURCES = ("src/fabric_perf/controllers/workload_coverage.py", "src/fabric_perf/controllers/collector_health.py",
               "src/fabric_perf/controllers/abstention_accounting.py", "src/fabric_perf/controllers/real_inference_smoke.py")
NEW_SCRIPTS = ("scripts/e1/e1_a0i_real_inference_smoke.py", "scripts/e1/e1_a0i_coverage_demo.py", "scripts/e1/e1_a0i_candidate_manifest.py")
EDITED = {"src/fabric_perf/controllers/captured_history.py", "src/fabric_perf/controllers/stage3_predictive.py",
          "src/fabric_perf/controllers/stage3_shadow_runner.py"}
A0H_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0h.json"


def manifest() -> dict:
    return json.loads(s.SUCCESSOR_MANIFEST.read_text(encoding="utf-8"))


# ------------------------------------------------------------------------------------------------- 43 candidate manifest verification
# A0k edited three files this manifest lists (workload_coverage.py, collector_health.py, captured_history.py) and one script (e1_a0i_coverage_demo.py). This manifest
# is left byte-identical (it records what they were at A0i). A file may differ from its A0i entry ONLY if the A0k manifest names it in ``changed_since_a0j`` AND the
# file on disk matches the A0k entry (``tests/a0k_pins.py``). Any other difference still fails.
def test_43_every_listed_hash_matches_the_file_on_disk():
    m = manifest()
    superseded = tolerated_in(m["entries"])
    findings = pi.verify_candidate_manifest(m, REPO)
    assert {(f.code, f.path) for f in findings} == {("modified", path) for path in superseded} and len(m["entries"]) == 66, findings
    for entry in m["entries"]:
        if entry["path"] not in superseded:
            assert s.sha(REPO / entry["path"]) == entry["sha256"] and (REPO / entry["path"]).stat().st_size == entry["size_bytes"], entry["path"]
    assert mk.main(["verify", "--manifest", str(s.SUCCESSOR_MANIFEST)]) == (1 if superseded else 0)       # the A0i manifest still describes A0i, so A0k's edits show


def test_43b_the_successor_repeats_every_a0h_entry_and_names_exactly_what_changed_and_what_was_added():
    m = manifest()
    old = {e["path"]: e for e in json.loads(A0H_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    new = {e["path"]: e for e in m["entries"]}
    assert set(old) <= set(new) and len(old) == 60
    changed = {p for p in old if new[p]["sha256"] != old[p]["sha256"]}
    assert changed == set(m["changed_since_a0h"]) == EDITED
    for path, entry in old.items():                                     # everything else: same hash, role, status, pin
        if path not in changed:
            assert {k: new[path][k] for k in ("sha256", "dependency_role", "approval_status", "pinned_sha256")} == \
                   {k: entry[k] for k in ("sha256", "dependency_role", "approval_status", "pinned_sha256")}, path
    assert set(m["added_since_a0h"]) == set(new) - set(old) == set(NEW_SOURCES) | {"scripts/e1/e1_a0i_real_inference_smoke.py", "scripts/e1/e1_a0i_coverage_demo.py"}
    assert set(m["changed_since_a0g"]) == {"scripts/e1/e1_stage3_shadow_runner.py", "src/fabric_perf/controllers/stage3_predictive.py",
                                           "src/fabric_perf/controllers/stage3_shadow_runner.py"}
    assert new["src/fabric_perf/controllers/live_integration.py"]["sha256"] == LIVE_INTEGRATION_SHA


def test_43c_the_status_stays_candidate_not_approved_and_unresolved_dependencies_are_stated():
    m = manifest()
    assert m["status"] == "CANDIDATE_NOT_APPROVED" and m["approved"] is False and m["approval_record"] is None
    assert "nothing in this file is an approval" in m["approval_note"].lower()
    assert m["supersedes"]["sha256"] == A0H_SHA and m["git"]["branch"] == "research/construct-validity-nov-2026"
    assert all(e["approval_status"] == pi.PENDING_OWNER_APPROVAL for e in m["entries"] if e["path"] in set(m["added_since_a0h"]) | set(m["changed_since_a0h"]))
    assert {e["approval_status"] for e in m["entries"]} == {pi.PENDING_OWNER_APPROVAL, pi.PINNED_ELSEWHERE_UNCHANGED, pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT}
    text = " ".join(m["unresolved_dependencies"])
    for needle in ("No running recorder writes collector-health journals", "carry no coverage evidence", "placeholder", "DESIGNED BUT NOT EXECUTED",
                   "reproducibility, not independent validation", "528 of 2,150", "recommendations, not realised performance", "live_integration.py is unchanged"):
        assert needle in text, needle
    assert any("approval of the A0i additions" in item for item in m["open_items"])
    excluded = " ".join(item["item"] for item in m["exclusions"])
    assert "throughput.joblib" in excluded and "stored Flashbots series" in excluded and "a0i_2026-11-21" in excluded


def test_43d_the_import_closure_is_fully_listed_and_no_frozen_artifact_is_listed():
    listed = {e["path"] for e in manifest()["entries"]} | a0k_entry_paths()          # A0k added modules that workload_coverage and the history now import
    assert mk.a0h.a0g.local_import_closure(mk.CLOSURE_ROOTS) <= listed
    assert not listed & {"experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib", "data/raw/hfbtp/HFBTP.csv",
                         "data/processed/flashbots/series_2026-09-21_1s.csv.gz"}


def test_43e_the_builder_reproduces_the_stored_entries_and_cannot_overwrite(tmp_path):
    superseded = tolerated_in(manifest()["entries"])
    keep = lambda entries: [e for e in entries if e["path"] not in superseded]      # noqa: E731
    assert keep(mk.build(REPO, created_utc=manifest()["created_utc"])["entries"]) == keep(manifest()["entries"])
    out = tmp_path / "m.json"
    out.write_text("{}", encoding="utf-8")
    assert mk.main(["write", "--out", str(out)]) == 2 and out.read_text(encoding="utf-8") == "{}"


def test_43f_the_verifier_detects_a_changed_file_instead_of_hiding_it(tmp_path):
    forged = json.loads(s.SUCCESSOR_MANIFEST.read_text(encoding="utf-8"))
    forged["entries"][0]["sha256"] = "0" * 64
    superseded = tolerated_in(forged["entries"])
    assert {(f.code, f.path) for f in pi.verify_candidate_manifest(forged, REPO)} == {("modified", forged["entries"][0]["path"])} | {("modified", p) for p in superseded}


# ------------------------------------------------------------------------------------------------ 44 historical pin preservation
def test_44_earlier_manifests_pins_and_frozen_evidence_are_byte_identical():
    assert s.sha(s.h.f.CANDIDATE_MANIFEST) == A0E_SHA and s.sha(s.h.A0G_MANIFEST) == A0G_SHA and s.sha(A0H_MANIFEST) == A0H_SHA
    assert s.sha(REPO / "src/fabric_perf/controllers/live_integration.py") == LIVE_INTEGRATION_SHA
    for path, expected in FROZEN.items():
        assert s.sha(REPO / path) == expected, path
    assert s.sha(REPO / "Integration/system_integration/a0h_2026-11-21/results/forecast_boundary_analysis.json") == \
        "0cd131fd5cf75c39664839186c6bc12ec52c25e14612c09f66a8c2fa696f763a"
    report = smoke.verify_inputs(REPO)                                             # models, selection, series: pin file, spec and disk agree
    assert report["all_ok"] is True


def test_44b_exercising_the_new_code_changes_no_stored_file(tmp_path):
    watched = list(FROZEN) + ["experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib",
                              "experiments/artifacts/hfbtp/metadata.json", "Integration/system_integration/offline_replay_inputs.json"]
    before = {p: (s.sha(REPO / p), (REPO / p).stat().st_mtime_ns) for p in watched}
    smoke.verify_inputs(REPO)
    smoke.read_stored_reference(REPO)
    from fabric_perf.controllers import abstention_accounting as acc
    acc.historical_below_grid_proxy(REPO / "Integration/system_integration/a0h_2026-11-21/results/forecast_boundary_analysis.json")
    source = s.ch.CapturedSource(source_id="flashbots-mempool-2026-09-21", utc_day="2026-09-21")
    binned = s.ch.load_binned_series(s.h.SERIES_09_21, source, expected_sha256=s.h.SERIES_09_21_SHA)
    history = s.ch.CapturedHistory(binned, coverage=wc.VerifiedCoverage.no_evidence(source.source_id, "archive without collector evidence"))
    history.advance_to(smoke.CUTOFF_UTC)
    assert history.evidence(smoke.CUTOFF_UTC)["coverage_provenance"]["mode"] == "strict_no_evidence"
    assert {p: (s.sha(REPO / p), (REPO / p).stat().st_mtime_ns) for p in watched} == before


def test_44c_the_historical_archive_attestation_certifies_nothing():
    """The honest statement about the stored Flashbots days: no collector evidence exists, so no zero second can be certified."""
    source = {"dataset_id": "flashbots-mempool-2026-09-21", "dataset_sha256": s.h.SERIES_09_21_SHA, "kind": "ethereum_mempool_first_seen", "is_fabric_observation": False}
    doc = wc.build_attestation(wc.VerifiedCoverage.no_evidence("flashbots-mempool-2026-09-21", "archive: sources column empty; no heartbeat, sequence or lifecycle record"),
                               source=source, as_of="2026-09-21T12:00:00+00:00", created_utc="2026-11-21T00:00:00+00:00")
    assert doc["integrity"]["status"] == "NO_EVIDENCE" and doc["coverage_intervals"] == [] and doc["collector"] is None and doc["journal"] is None
    assert doc["source"]["dataset_sha256"] == s.h.SERIES_09_21_SHA and doc["source"]["is_fabric_observation"] is False


# ------------------------------------------------------------------------------------------------ 45 protected channel rejection
@pytest.mark.parametrize("channel", ["e1-r002", "E1-CFG001", " e1-r002 ", "e1-b001", "e1-r001", "e1-ql01-b40-l10", "e1-cp02-b45-l20", "e1-rp01-b10-r3", "not-an-e1-channel", ""])
def test_45_the_collector_journal_refuses_protected_and_earlier_channels_and_creates_nothing(tmp_path, channel):
    path = tmp_path / "j.jsonl"
    with pytest.raises((channel_selection.ChannelSelectionError, wc.CoverageEvidenceError)):
        chj.CollectorHealthJournal(path, source_id="s", collector_id="c", run_id="r", channel_id=channel)
    assert not path.exists()


def test_45b_a_dedicated_channel_is_accepted_and_recorded_unchanged(tmp_path):
    journal = chj.CollectorHealthJournal(tmp_path / "j.jsonl", source_id="s", collector_id="c", run_id="r", channel_id="e1-val-a",
                                         clock=s.ScriptClock())
    journal.start(observation_start_utc=s.at(0))
    assert json.loads((tmp_path / "j.jsonl").read_text(encoding="utf-8").splitlines()[0])["channel_id"] == "e1-val-a"


# ----------------------------------------------------------------------------------- 46 no process or network, 47 no frozen-model loading
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


def test_46_47_the_whole_a0i_pipeline_runs_with_process_network_and_model_loading_trapped(tmp_path, forbidden):
    out = tmp_path / "demo"
    assert demo.main(["--out-dir", str(out)]) == 0
    fresh = json.loads((out / "demo_summary.json").read_text(encoding="utf-8"))
    stored = json.loads((s.A0I_DIR / "demo" / "demo_summary.json").read_text(encoding="utf-8"))
    assert fresh == stored                                                           # the stored demonstration is exactly reproducible
    assert [r["manifest_verifies"] for r in fresh["runs"]] == [True, True, True]
    assert smoke.verify_inputs(REPO)["all_ok"] is True
    assert mk.build(REPO, created_utc="2026-11-21T00:00:00+00:00")["entries"]


def test_46b_the_new_modules_import_no_process_network_model_or_numeric_library():
    banned = {"subprocess", "socket", "urllib", "http", "requests", "joblib", "pickle", "sklearn", "shutil", "numpy", "pandas", "multiprocessing", "ctypes"}
    for relative in NEW_SOURCES + NEW_SCRIPTS:
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
                   {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module and not n.level}
        assert not imported & banned, (relative, imported & banned)
        if relative != "src/fabric_perf/controllers/collector_health.py":
            assert "os" not in imported, relative                                    # only the journal writer touches the OS (os.fsync)
    tree = ast.parse((REPO / "src/fabric_perf/controllers/collector_health.py").read_text(encoding="utf-8"))
    used = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "os"}
    assert used == {"fsync"}, used


def test_47b_real_inference_is_still_a_single_false_constant_and_only_the_smoke_gate_reads_it():
    import re
    source = (REPO / "src/fabric_perf/controllers/stage3_predictive.py").read_text(encoding="utf-8")
    assert re.findall(r"^REAL_INFERENCE_AUTHORIZED\s*=\s*(\w+)\s*$", source, re.M) == ["False"]
    for relative in ("src/fabric_perf/controllers/workload_coverage.py", "src/fabric_perf/controllers/collector_health.py",
                     "src/fabric_perf/controllers/abstention_accounting.py", "src/fabric_perf/controllers/captured_history.py"):
        assert "REAL_INFERENCE_AUTHORIZED" not in (REPO / relative).read_text(encoding="utf-8"), relative
    for relative in ("src/fabric_perf/controllers/real_inference_smoke.py", "src/fabric_perf/controllers/captured_history.py",
                     "src/fabric_perf/controllers/workload_coverage.py", "src/fabric_perf/controllers/abstention_accounting.py"):
        assert "load_hfbtp_artifact" not in (REPO / relative).read_text(encoding="utf-8"), relative


def test_47c_the_new_code_writes_only_new_files_it_is_given():
    for relative in ("src/fabric_perf/controllers/workload_coverage.py", "src/fabric_perf/controllers/abstention_accounting.py",
                     "src/fabric_perf/controllers/real_inference_smoke.py"):
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        assert not calls & {"write_text", "write_bytes", "unlink", "rmdir", "rename", "mkdir", "touch"}, relative
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "open"
                    and len(n.args) > 1 and isinstance(n.args[1], ast.Constant) and set("wax+") & set(str(n.args[1].value))], relative


def test_47d_the_journal_writer_creates_only_its_own_new_file(tmp_path):
    journal, clock, path = s.new_journal(tmp_path)
    s.beat(journal, clock, 10)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["journal.jsonl"]


# ---------------------------------------------------------------------------------------------- 48 no staging, no commit, no push
def test_48_no_a0i_file_stages_commits_or_pushes():
    import re
    for relative in NEW_SOURCES + NEW_SCRIPTS + ("tests/a0i_support.py",):
        text = (REPO / relative).read_text(encoding="utf-8")
        assert not re.search(r"git\s+(add|commit|push|stash|checkout|reset|rm)\b", text), relative
        assert "subprocess" not in text or relative.endswith("test_a0i_integrity_2026_11_21.py"), relative
