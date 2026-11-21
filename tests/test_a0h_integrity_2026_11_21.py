"""A0h: successor candidate manifest, preservation of frozen and historical artifacts, and the computation boundary (2026-11-21).

Checklist items 45-50. Reads and hashes files; nothing is written outside ``tmp_path``, loaded, unpickled or executed. The model
artifacts are not touched at all by this file (A0g test 39 already shows them byte-identical to their pins).
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import socket
import subprocess
import urllib.request
from pathlib import Path

import pytest

from tests import a0h_support as h
from tests.a0k_pins import a0k_entry_paths, changed_by_a0k
from fabric_perf.controllers import captured_history as ch
from fabric_perf.controllers import live_foundation as lf
from fabric_perf.controllers import prelive_integrity as pi
from fabric_perf.models import artifacts

import e1_a0h_candidate_manifest as mk
import e1_a0h_forecast_boundary as boundary

REPO = h.REPO
A0E_SHA = "13640a7f683adcfec96f26f9060b05f29306bb2ebcfeb4c4b90c5fc43fa2b984"
A0G_SHA = "491c7d0c296515bf24cd2a8d4462be3f34a81926f933e5d7542a45105eb5a5e7"
LIVE_INTEGRATION_SHA = "25441ed423ac2e73b315f9c060bf6859b6df1b8aa2fcd89b80b8e9283b1fd978"
FROZEN = {                                                                # recorded before A0h edited anything (see the report, section 2)
    "data/processed/flashbots/series_2026-09-21_1s.csv.gz": "2d54edd894dd7ccda67d9df81de1a753d7b4cef4b6da0271f5fd418d2a98ffb6",
    "data/processed/flashbots/series_2026-10-12_1s.csv.gz": "e0ce980226c381c039d3afbc4095f341d82a1b1104a7e902b3a9bb46e6601822",
    "experiments/results/flashbots_horizon_benchmark_v2/select/selection_frozen.json": "888e4a48b59a5c543d9016878a5194266de47eeaabdf0f33f51b10f146f05a72",
    "experiments/results/flashbots_horizon_benchmark_v2/select/forecasts_V_H60.csv": "99d73e43cf2e7d779b56eaa8027ba6b5d30e6f5b71ffde411331802a8b41ab41",
    "experiments/results/flashbots_horizon_benchmark_v2/final/forecasts_F_H60.csv": "5f65ff6e4e152698243e37181f67d154c119fde701c974126e0a0c8d82a7bb0b",
    "Flashbots/results/FB-FC-v1/select/forecasts_V_arrival_rate_H60.csv": "b8f98310684fa131fa3d3cbac892678f174e06d9ebd5dcb51cb3aa1dfeec3d3a",
    "Flashbots/results/FB-FC-v1/final/forecasts_F_arrival_rate_H60.csv": "2954e6186d6036fab78bccb5234bb8c15ce9c0e4a34883312625afbf87f01ff8",
}
NEW_SOURCES = ("src/fabric_perf/controllers/captured_history.py", "src/fabric_perf/controllers/forecast_boundary.py",
               "scripts/e1/e1_a0h_forecast_boundary.py", "scripts/e1/e1_a0h_candidate_manifest.py")


def sha(path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 22), b""):
            digest.update(chunk)
    return digest.hexdigest()


def manifest() -> dict:
    return json.loads(h.SUCCESSOR_MANIFEST.read_text(encoding="utf-8"))


# A0i (2026-11-21) edited three files this manifest lists. This manifest is left byte-identical (it records what they were at A0h). A file may differ
# from its A0h entry ONLY if the A0i manifest names it in ``changed_since_a0h`` AND the file on disk matches the A0i entry. Any other difference still fails.
A0I_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0i.json"
A0I_SUPERSEDED = {"src/fabric_perf/controllers/captured_history.py", "src/fabric_perf/controllers/stage3_predictive.py",
                  "src/fabric_perf/controllers/stage3_shadow_runner.py"}


def superseded_by_a0i() -> set:
    if not A0I_MANIFEST.is_file():
        return set()
    a0i = json.loads(A0I_MANIFEST.read_text(encoding="utf-8"))
    current = {e["path"]: e["sha256"] for e in a0i["entries"]}
    paths = set(a0i["changed_since_a0h"])
    assert paths == A0I_SUPERSEDED, sorted(paths ^ A0I_SUPERSEDED)
    again = changed_by_a0k()                                                # A0k edited captured_history.py once more: its A0k entry is checked instead
    for path in paths:
        if path not in again:
            assert sha(REPO / path) == current[path], path
    return paths


# ------------------------------------------------------------------------------------------- 45 candidate hash verification
def test_45_every_listed_hash_matches_the_file_on_disk():
    m = manifest()
    superseded = superseded_by_a0i()
    findings = pi.verify_candidate_manifest(m, REPO)
    assert {(f.code, f.path) for f in findings} == {("modified", path) for path in superseded} and len(m["entries"]) == 60, findings
    for entry in m["entries"]:
        if entry["path"] not in superseded:
            assert sha(REPO / entry["path"]) == entry["sha256"] and (REPO / entry["path"]).stat().st_size == entry["size_bytes"], entry["path"]


def test_45b_the_successor_repeats_every_a0g_entry_and_names_exactly_what_changed():
    m = manifest()
    old = {e["path"]: e for e in json.loads(h.A0G_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    new = {e["path"]: e for e in m["entries"]}
    assert set(old) <= set(new) and len(old) == 57
    changed = {p for p in old if new[p]["sha256"] != old[p]["sha256"]}
    assert changed == set(m["changed_since_a0g"]) == {"scripts/e1/e1_stage3_shadow_runner.py", "src/fabric_perf/controllers/stage3_predictive.py",
                                                      "src/fabric_perf/controllers/stage3_shadow_runner.py"}
    for path, entry in old.items():                                       # everything else: same hash, role, status, pin
        if path not in changed:
            assert {k: new[path][k] for k in ("sha256", "dependency_role", "approval_status", "pinned_sha256")} == \
                   {k: entry[k] for k in ("sha256", "dependency_role", "approval_status", "pinned_sha256")}, path
    assert set(m["added_since_a0g"]) == set(new) - set(old) == {"scripts/e1/e1_a0h_forecast_boundary.py", "src/fabric_perf/controllers/captured_history.py",
                                                                "src/fabric_perf/controllers/forecast_boundary.py"}
    assert new["src/fabric_perf/controllers/live_integration.py"]["sha256"] == LIVE_INTEGRATION_SHA


def test_45c_the_a0e_entries_and_the_pinned_files_are_unchanged_in_the_successor():
    m = manifest()
    a0e = {e["path"]: e for e in json.loads(h.f.CANDIDATE_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    new = {e["path"]: e for e in m["entries"]}
    assert len(a0e) == 25
    for path, entry in a0e.items():
        assert {k: new[path][k] for k in ("sha256", "approval_status", "pinned_sha256")} == {k: entry[k] for k in ("sha256", "approval_status", "pinned_sha256")}
    assert new["scripts/e1/e1_la2_render.py"]["approval_status"] == pi.PINNED_ELSEWHERE_UNCHANGED
    assert new["scripts/e1/run_contemporaneous_capture.py"]["approval_status"] == pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT


def test_45d_the_status_is_candidate_not_approved_and_nothing_is_approved():
    m = manifest()
    assert m["status"] == "CANDIDATE_NOT_APPROVED" and m["approved"] is False and m["approval_record"] is None
    assert {e["approval_status"] for e in m["entries"]} == {pi.PENDING_OWNER_APPROVAL, pi.PINNED_ELSEWHERE_UNCHANGED, pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT}
    assert all(e["approval_status"] == pi.PENDING_OWNER_APPROVAL for e in m["entries"] if e["path"] in set(m["added_since_a0g"]) | set(m["changed_since_a0g"]))
    assert "nothing in this file is an approval" in m["approval_note"].lower()
    assert m["supersedes"]["sha256"] == A0G_SHA and m["git"]["branch"] == "research/construct-validity-nov-2026"


def test_45e_the_import_closure_is_fully_listed_and_the_unresolved_items_are_stated():
    m = manifest()
    listed = {e["path"] for e in m["entries"]}
    if A0I_MANIFEST.is_file():                                              # A0i added modules the adapter and runner now import
        listed |= {e["path"] for e in json.loads(A0I_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    listed |= a0k_entry_paths()                                             # A0k added modules the history adapter and coverage now import
    assert mk.a0g.local_import_closure(mk.CLOSURE_ROOTS) <= listed
    assert {"src/fabric_perf/controllers/captured_history.py", "src/fabric_perf/controllers/forecast_boundary.py"} <= listed
    text = " ".join(m["unresolved_dependencies"])
    for needle in ("FIXTURE mode only", "benchmark_zero_fill", "REAL_INFERENCE_AUTHORIZED", "below 10 tx/s", "live_integration.py is pinned"):
        assert needle in text, needle
    assert any("throughput.joblib" in item["item"] for item in m["exclusions"]) and any("stored Flashbots series" in item["item"] for item in m["exclusions"])
    assert not listed & {"experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib", "data/raw/hfbtp/HFBTP.csv"}


def test_45f_the_builder_reproduces_the_stored_entries_and_cannot_overwrite(tmp_path):
    superseded = superseded_by_a0i()
    keep = lambda entries: [e for e in entries if e["path"] not in superseded]      # noqa: E731
    assert keep(mk.build(REPO, created_utc=manifest()["created_utc"])["entries"]) == keep(manifest()["entries"])
    out = tmp_path / "m.json"
    out.write_text("{}", encoding="utf-8")
    assert mk.main(["write", "--out", str(out)]) == 2 and out.read_text(encoding="utf-8") == "{}"
    assert mk.main(["verify", "--manifest", str(h.SUCCESSOR_MANIFEST)]) == (1 if superseded else 0)      # the A0h manifest still describes A0h, so A0i's three edits show


# ------------------------------------------------------------------------------------------------ 46 frozen-file preservation
def test_46_frozen_and_historical_files_are_byte_identical_to_their_recorded_hashes():
    assert sha(h.f.CANDIDATE_MANIFEST) == A0E_SHA and sha(h.A0G_MANIFEST) == A0G_SHA
    assert sha(REPO / "src/fabric_perf/controllers/live_integration.py") == LIVE_INTEGRATION_SHA
    pins = json.loads(h.g.MODEL_PINS.read_text(encoding="utf-8"))["files"]
    for key in ("series", "selection"):
        assert sha(REPO / pins[key]["path"]).upper() == pins[key]["sha256"].upper()
    for path, expected in FROZEN.items():
        assert sha(REPO / path) == expected, path


def test_46b_exercising_the_adapter_and_the_analysis_changes_no_stored_file(tmp_path):
    before = {p: (sha(REPO / p), (REPO / p).stat().st_mtime_ns) for p in FROZEN}
    day = ch.load_binned_series(h.SERIES_09_21, ch.CapturedSource("flashbots-mempool-2026-09-21", "2026-09-21"), expected_sha256=h.SERIES_09_21_SHA)
    captured = ch.CapturedHistory(day, zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL)
    captured.advance_to("2026-09-21T01:00:00+00:00")
    assert boundary.analyse()["segments"]
    assert before == {p: (sha(REPO / p), (REPO / p).stat().st_mtime_ns) for p in FROZEN}


# ------------------------------------------------------------------------- 47 no model load, 48 no network or process, 49 data
@pytest.fixture
def forbidden(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a model, process or network operation was attempted")
    import joblib
    import pickle
    for obj, names in ((joblib, ("load",)), (pickle, ("load", "loads")), (artifacts, ("load_hfbtp_artifact",)),
                       (subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",)), (lf.StreamingEWMA, ("__init__", "predict_timestamped"))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)


def test_47_48_the_captured_history_pipeline_loads_no_model_runs_no_forecaster_and_uses_no_process_or_network(tmp_path, forbidden):
    manifest_out, rows, _ = h.run_captured(tmp_path, h.history())
    assert manifest_out["models"]["frozen_hfbtp_model_loaded"] is False and manifest_out["models"]["frozen_ewma_loaded"] is False
    assert any(r["controller"] == "predictive" and r["predictive_evidence"]["forecast"]["value_tps"] == 55.0 for r in rows)
    report = boundary.analyse()
    assert report["computation_performed"].startswith("none") and report["segments"]
    assert mk.build(REPO, created_utc="2026-11-21T00:00:00+00:00")["entries"]


def test_47b_the_new_modules_import_no_model_process_network_or_file_system_mutation_api():
    banned = {"subprocess", "socket", "urllib", "http", "requests", "joblib", "pickle", "sklearn", "shutil", "os", "numpy", "pandas"}
    for relative in NEW_SOURCES:
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
                   {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module and not n.level}
        assert not imported & banned, (relative, imported & banned)


def test_47c_real_inference_is_still_a_single_false_constant_and_the_new_code_never_reads_it():
    source = (REPO / "src/fabric_perf/controllers/stage3_predictive.py").read_text(encoding="utf-8")
    assert re.findall(r"^REAL_INFERENCE_AUTHORIZED\s*=\s*(\w+)\s*$", source, re.M) == ["False"]
    for relative in NEW_SOURCES[:2]:
        assert "REAL_INFERENCE_AUTHORIZED" not in (REPO / relative).read_text(encoding="utf-8")
    assert "load_hfbtp_artifact" not in (REPO / "src/fabric_perf/controllers/captured_history.py").read_text(encoding="utf-8")


def test_49_the_adapter_never_writes_a_file_and_the_analysis_writes_only_a_new_named_file():
    for relative in ("src/fabric_perf/controllers/captured_history.py", "src/fabric_perf/controllers/forecast_boundary.py"):
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        assert not calls & {"write_text", "write_bytes", "unlink", "rmdir", "rename", "mkdir", "touch"}, relative
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "open"
                    and len(n.args) > 1 and isinstance(n.args[1], ast.Constant) and set("wax+") & set(str(n.args[1].value))]
    script = (REPO / "scripts/e1/e1_a0h_forecast_boundary.py").read_text(encoding="utf-8")
    assert script.count('.open("x"') == 1 and "write_text" not in script


# --------------------------------------------------------------------------------------------------------- 50 no git mutation
def test_50_no_a0h_file_stages_commits_or_pushes():
    for relative in NEW_SOURCES + ("src/fabric_perf/controllers/stage3_shadow_runner.py", "scripts/e1/e1_stage3_shadow_runner.py",
                                   "src/fabric_perf/controllers/stage3_predictive.py"):
        text = (REPO / relative).read_text(encoding="utf-8")
        assert not re.search(r"git\s+(add|commit|push|stash|reset|checkout|rebase|merge)\b", text), relative
        assert "subprocess" not in text and "os.system" not in text, relative
    for path in (REPO / "tests").glob("test_a0h_*.py"):                  # the tests themselves never stage, commit or push either
        if path.name != Path(__file__).name:                              # (this file holds the pattern as a literal)
            assert not re.search(r"git\s+(add|commit|push)", path.read_text(encoding="utf-8")), path.name
