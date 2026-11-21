"""A0l integrity and safety (checklist items 40-46): protected channels, frozen and candidate files unchanged, previous manifests byte-identical, the successor candidate
manifest, no process/network/Fabric/Caliper use, no frozen-model loading, and no Git staging. Reads and hashes files; nothing is loaded or executed beyond offline code.
"""

from __future__ import annotations

import ast
import hashlib
import json
import pickle
import re
import socket
import subprocess
import urllib.request
from pathlib import Path

import pytest

from tests import a0j_support as j
from tests import a0l_support as a
from tests.a0l_support import at, wc
from tests.test_a0h_integrity_2026_11_21 import A0E_SHA, A0G_SHA, FROZEN, LIVE_INTEGRATION_SHA
from tests.test_a0i_integrity_2026_11_21 import A0H_SHA
from tests.test_a0k_integrity_2026_11_21 import A0I_MANIFEST_SHA, A0J_MANIFEST_SHA

from fabric_perf.controllers import channel_selection
from fabric_perf.controllers import collector_health as chj
from fabric_perf.controllers import issue_log_coverage as gc
from fabric_perf.controllers import issue_log_reader as ilr
from fabric_perf.controllers import live_foundation as lf
from fabric_perf.controllers import prelive_integrity as pi
from fabric_perf.models import artifacts

import e1_a0l_candidate_manifest as mk  # noqa: E402

REPO = j.REPO
E1 = REPO / "docs" / "research" / "e1"
A0K_MANIFEST = E1 / "prelive_candidate_manifest_2026-11-21_a0k.json"
A0L_MANIFEST = E1 / "prelive_candidate_manifest_2026-11-21_a0l.json"
A0K_MANIFEST_SHA = "d648a7151bf7d2688d2f658594a4d5adae4d08201b4428de5924372688ff756c"
START_HEAD = "8c67a4678b3901c8d601fb1a33210d7d8c84cf78"
W = "fabric-testbed/caliper/workloads/"
NEW_PY = ("src/fabric_perf/controllers/issue_log_reader.py", "src/fabric_perf/controllers/issue_log_coverage.py")
NEW_JS = (W + "e1_issue_log.js", W + "e1_create_asset_issue_logged.js")
NEW_SCRIPTS = ("scripts/e1/e1_a0l_candidate_manifest.py", "Integration/system_integration/a0l_2026-11-21/e2e_demo.py",
               "Integration/system_integration/a0l_2026-11-21/mutation_check.py")
NEW_TESTS = ("tests/a0l_support.py", "tests/e1/js/e1_issue_log.test.js", "tests/e1/js/a0l_issue_round.js") + tuple(
    f"tests/test_a0l_{n}_2026_11_21.py" for n in ("reader", "coverage", "stage3", "workload_demo", "integrity"))
WORKLOADS_UNCHANGED = (W + "e1_create_asset.js", W + "e1_workload_core.js", W + "e1_trace_replay.js")


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def manifest() -> dict:
    return json.loads(A0L_MANIFEST.read_text(encoding="utf-8"))


def git(*args) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=False)


# ------------------------------------------------------------------------------------------------------------------ 40 protected channels
@pytest.mark.parametrize("channel", ["e1-r002", "E1-CFG001", " e1-r002 ", "e1-b001", "e1-r001", "e1-ql01-b40-l10", "e1-cp02-b45-l20", "not-an-e1-channel", ""])
def test_40_the_bridge_and_the_journal_still_refuse_protected_and_earlier_channels_before_any_file_exists(tmp_path, channel):
    log = a.SyntheticIssueLog(tmp_path / "c.jsonl")
    a.Scripted(log).start(-0.5)
    tail = ilr.IssueLogTail(log.path, a.policy(max_tick_gap_ms=a.SHORT_GAP_MS))
    with pytest.raises((channel_selection.ChannelSelectionError, wc.CoverageEvidenceError)):
        gc.IssueLogJournalBridge(tail, tmp_path / "j.jsonl", source_id=a.SOURCE_ID, origin=gc.ORIGIN_SYNTHETIC, channel_id=channel)
    assert not (tmp_path / "j.jsonl").exists()
    with pytest.raises((channel_selection.ChannelSelectionError, wc.CoverageEvidenceError)):
        chj.CollectorHealthJournal(tmp_path / "k.jsonl", source_id="s", collector_id="c", run_id="r", channel_id=channel)
    assert not (tmp_path / "k.jsonl").exists()


def test_40b_a_dedicated_channel_is_accepted_and_recorded_beside_the_declared_scope_and_subject(tmp_path):
    gen, reader, _cov = a.short_history(tmp_path)
    log = a.SyntheticIssueLog(tmp_path / "d.jsonl")
    a.Scripted(log).start(-0.5)
    r2 = a.Reader(log.path).with_bridge(tmp_path / "dj.jsonl", channel_id="e1-val-a")
    r2.poll(-0.4)
    row = json.loads((tmp_path / "dj.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert row["channel_id"] == "e1-val-a" and row["coverage_scope"] == "source_stream_observer" and row["coverage_subject"] == "generator_issuance"


def test_40c_no_issue_log_file_names_a_channel_a_network_or_a_cell():
    for relative in NEW_JS + NEW_PY[:1]:
        text = (REPO / relative).read_text(encoding="utf-8")
        assert not re.search(r"e1-(r00|cfg|b00|ql|cp|val)", text), relative


# ------------------------------------------------------------------------------------------------------------------ 41 frozen files
def test_41_every_file_the_a0k_manifest_lists_is_byte_identical_so_a0l_changed_no_candidate_file():
    findings = pi.verify_candidate_manifest(json.loads(A0K_MANIFEST.read_text(encoding="utf-8")), REPO)
    assert findings == []
    for path, expected in FROZEN.items():
        assert sha(REPO / path) == expected, path
    assert sha(REPO / "src/fabric_perf/controllers/live_integration.py") == LIVE_INTEGRATION_SHA


def test_41b_the_pilot_workload_and_its_helpers_are_preserved_byte_for_byte():
    entries = {e["path"]: e for e in json.loads(A0K_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    for relative in WORKLOADS_UNCHANGED[:2]:
        assert sha(REPO / relative) == entries[relative]["sha256"], relative
    assert entries[W + "e1_create_asset.js"]["sha256"] == "c5c234b71666e6b8e135f26f8c65d2e1639c3d54784213cfca0cc15a50f5667f"
    tracked = git("ls-files", "--error-unmatch", *WORKLOADS_UNCHANGED)
    if tracked.returncode == 0:                                                           # a repository checkout: the files also equal their committed blobs
        assert git("diff", "--quiet", "HEAD", "--", *WORKLOADS_UNCHANGED).returncode == 0
    assert "issue" not in (REPO / W / "e1_create_asset.js").read_text(encoding="utf-8").lower()


def test_41c_the_pinned_recorder_renderer_and_runner_files_still_match_their_pins():
    pinned = [e for e in json.loads(A0K_MANIFEST.read_text(encoding="utf-8"))["entries"] if e["pinned_sha256"]]
    assert {e["path"] for e in pinned} >= {"scripts/e1/e1_la2_render.py", "scripts/e1/caliper/e1_per_tx_recorder.js"}
    for entry in pinned:
        assert sha(REPO / entry["path"]) == entry["pinned_sha256"], entry["path"]
    assert "e1_create_asset_issue_logged" not in (REPO / "scripts/e1/e1_la2_render.py").read_text(encoding="utf-8")      # the renderer does not select the successor


def test_41d_the_successor_workload_extends_the_legacy_one_through_its_exported_build_and_adds_no_other_dependency():
    source = (REPO / W / "e1_create_asset_issue_logged.js").read_text(encoding="utf-8")
    assert "require('./e1_create_asset.js')" in source and "legacy.build(Base, sleep)" in source
    assert set(re.findall(r"require\('([^']+)'\)", source)) == {"./e1_create_asset.js", "./e1_workload_core.js", "./e1_issue_log.js", "@hyperledger/caliper-core"}
    writer = (REPO / W / "e1_issue_log.js").read_text(encoding="utf-8")
    assert set(re.findall(r"require\('([^']+)'\)", writer)) == {"fs", "crypto", "path"}
    for text in (source, writer):
        assert not re.search(r"child_process|require\('(net|http|https|dgram|tls|os)'\)|fetch\(|XMLHttpRequest|WebSocket", text)


# ------------------------------------------------------------------------------------------------------------------ 42 previous manifests
def test_42_every_earlier_manifest_is_byte_identical():
    assert sha(j.s.h.f.CANDIDATE_MANIFEST) == A0E_SHA and sha(j.s.h.A0G_MANIFEST) == A0G_SHA
    assert sha(E1 / "prelive_candidate_manifest_2026-11-21_a0h.json") == A0H_SHA
    assert sha(E1 / "prelive_candidate_manifest_2026-11-21_a0i.json") == A0I_MANIFEST_SHA and sha(E1 / "prelive_candidate_manifest_2026-11-21_a0j.json") == A0J_MANIFEST_SHA
    assert sha(A0K_MANIFEST) == A0K_MANIFEST_SHA


def test_42b_the_stored_a0k_evidence_still_verifies_against_its_own_sums():
    base = REPO / "Integration" / "system_integration" / "a0k_2026-11-21"
    checked = drifted = 0
    for line in (base / "SHA256SUMS.txt").read_text(encoding="utf-8").splitlines():
        digest, _sep, name = line.partition(" ")
        name = name.lstrip(" *")
        path = base / name[2:] if name.startswith("./") else REPO / name
        assert path.is_file(), path
        checked += 1
        drifted += sha(path) != digest
    assert checked >= 10 and drifted == 0


# ------------------------------------------------------------------------------------------------------------------ 43 successor manifest
def test_43_every_listed_hash_matches_the_file_on_disk_and_the_manifest_is_a_candidate_not_an_approval():
    m = manifest()
    assert pi.verify_candidate_manifest(m, REPO) == [] and len(m["entries"]) == 76
    for entry in m["entries"]:
        assert sha(REPO / entry["path"]) == entry["sha256"] and (REPO / entry["path"]).stat().st_size == entry["size_bytes"], entry["path"]
    assert mk.main(["verify", "--manifest", str(A0L_MANIFEST)]) == 0
    assert m["status"] == "CANDIDATE_NOT_APPROVED" and m["approved"] is False and m["approval_record"] is None
    assert "nothing in this file is an approval" in m["approval_note"].lower() and m["supersedes"]["sha256"] == A0K_MANIFEST_SHA
    assert m["git"]["branch"] == "research/construct-validity-nov-2026" and m["git"]["commit"] == START_HEAD


def test_43b_the_successor_repeats_every_a0k_entry_adds_exactly_the_four_files_and_supersedes_nothing():
    m = manifest()
    old = {e["path"]: e for e in json.loads(A0K_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    new = {e["path"]: e for e in m["entries"]}
    assert set(old) < set(new) and len(old) == 72
    assert set(m["added_since_a0k"]) == set(new) - set(old) == set(NEW_PY + NEW_JS)
    assert m["changed_since_a0k"] == [] and m["removed_since_a0k"] == [] and m["superseded_entries"] == [] and "nothing is superseded" in m["superseded_note"]
    for path, entry in old.items():
        assert new[path] == entry, path                                                  # the whole entry: hash, size, role, approval status and pin
    assert all(new[p]["approval_status"] == pi.PENDING_OWNER_APPROVAL and new[p]["pinned_sha256"] is None for p in NEW_PY + NEW_JS)


def test_43c_the_unresolved_dependencies_and_open_items_state_the_live_limits():
    m = manifest()
    text = " ".join(m["unresolved_dependencies"])
    for needle in ("OFFLINE ONLY", "DECLARED by data flow", "placeholders with no scientific justification", "experimental generator's issuance only", "tamper-EVIDENT",
                   "One worker only", "reader's read time as t_utc", "live_source_coverage_verified remains false", "pilot renderer (pinned) still selects e1_create_asset.js"):
        assert needle in text, needle
    assert any("owner approval of the four a0l additions" in item.lower() for item in m["open_items"])
    assert "a0l_2026-11-21" in " ".join(item["item"] for item in m["exclusions"]) and m["launch_configuration"]["issue_log"]["live_run"] is False


def test_43d_the_import_closure_is_fully_listed_and_no_frozen_artifact_is_listed():
    listed = {e["path"] for e in manifest()["entries"]}
    assert mk.a0k.a0j.a0i.a0h.a0g.local_import_closure(mk.CLOSURE_ROOTS) <= listed
    assert not listed & {"experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib", "data/raw/hfbtp/HFBTP.csv",
                         "data/processed/flashbots/series_2026-09-21_1s.csv.gz"}


def test_43e_the_builder_reproduces_the_stored_entries_cannot_overwrite_and_the_verifier_detects_a_change(tmp_path):
    assert mk.build(REPO, created_utc=manifest()["created_utc"])["entries"] == manifest()["entries"]
    out = tmp_path / "m.json"
    out.write_text("{}", encoding="utf-8")
    assert mk.main(["write", "--out", str(out)]) == 2 and out.read_text(encoding="utf-8") == "{}"
    forged = manifest()
    forged["entries"][-1]["sha256"] = "0" * 64
    assert [f.code for f in pi.verify_candidate_manifest(forged, REPO)] == ["modified"]


# ------------------------------------------------------------------------------------------------------------------ 44 no process/network/Fabric/Caliper, 45 no model
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


def test_44_45_the_whole_a0l_pipeline_runs_with_process_network_and_model_loading_trapped(tmp_path, forbidden):
    from tests.test_a0l_stage3_2026_11_21 import r1_run, r2_run
    (tmp_path / "a").mkdir()
    reader, history, manifest_doc, rows, out = r2_run(tmp_path / "a")
    assert manifest_doc["status"] == "COMPLETED" and [r["outcome"] for r in j.by_controller(rows)["predictive"]][1:] == ["propose"] * 3
    (tmp_path / "b").mkdir()
    reader, history, manifest_doc, rows, out = r1_run(tmp_path / "b")
    assert manifest_doc["status"] == "COMPLETED"
    gen, rd, cov = a.short_history(tmp_path)
    assert cov.snapshot(at(a.SHORT + 1)).classify(at(3), 2)[0] == a.C.OBSERVED_POSITIVE
    assert mk.build(REPO, created_utc="2026-11-21T00:00:00+00:00")["entries"]


def test_44b_the_new_modules_import_no_process_network_model_container_numeric_or_dynamic_import_library():
    banned = {"subprocess", "socket", "urllib", "http", "requests", "joblib", "pickle", "sklearn", "shutil", "numpy", "pandas", "multiprocessing", "ctypes", "docker",
              "paramiko", "asyncio", "importlib", "sys", "tempfile"}
    for relative in NEW_PY + ("scripts/e1/e1_a0l_candidate_manifest.py",):
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        imported = {n_.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for n_ in n.names} | \
                   {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module and not n.level}
        assert not imported & (banned - ({"sys"} if relative.endswith("manifest.py") else set())), (relative, imported & banned)


def test_44c_the_new_modules_name_no_container_fabric_or_caliper_command():
    for relative in NEW_PY + NEW_JS + NEW_SCRIPTS[:1]:
        text = (REPO / relative).read_text(encoding="utf-8").lower()
        for needle in ("docker run", "docker exec", "wsl.exe", "peer channel", "peer chaincode", "caliper launch", "prometheus", "requests.get", "real_inference_authorized",
                       "load_hfbtp_artifact", "joblib", "streamingewma"):
            assert needle not in text, (relative, needle)


def test_44d_the_new_python_modules_write_only_their_own_checkpoint_and_journal_and_never_delete_or_rename():
    for relative in NEW_PY:
        tree = ast.parse((REPO / relative).read_text(encoding="utf-8"))
        attrs = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        assert not attrs & {"write_text", "write_bytes", "unlink", "rmdir", "rename", "touch", "mkdir", "remove", "removedirs", "rmtree"}, (relative, attrs)
        assert not [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "open"], relative
    reader = (REPO / NEW_PY[0]).read_text(encoding="utf-8")
    assert reader.count("os.replace(") == 1 and "def save_checkpoint" in reader and reader.count('.open("wb")') == 1          # one atomic checkpoint write, nothing else
    assert '.open("ab")' not in reader and '.open("r+b")' not in reader                                                            # the reader never appends to or edits the log


def test_45b_real_inference_is_still_a_single_false_constant_and_no_a0l_source_reads_it():
    source = (REPO / "src/fabric_perf/controllers/stage3_predictive.py").read_text(encoding="utf-8")
    assert re.findall(r"^REAL_INFERENCE_AUTHORIZED\s*=\s*(\w+)\s*$", source, re.M) == ["False"]
    for relative in NEW_PY + NEW_JS + NEW_SCRIPTS[:1]:
        text = (REPO / relative).read_text(encoding="utf-8")
        assert "REAL_INFERENCE_AUTHORIZED" not in text and "load_hfbtp_artifact" not in text and "joblib" not in text and "StreamingEWMA" not in text, relative


# ------------------------------------------------------------------------------------------------------------------ 46 no staging, no commit, no push
def test_46_no_a0l_file_stages_commits_or_pushes():
    files = list(NEW_PY + NEW_JS + NEW_SCRIPTS + NEW_TESTS)
    for relative in files:
        text = (REPO / relative).read_text(encoding="utf-8")
        if relative.endswith(("mutation_check.py", "test_a0l_integrity_2026_11_21.py")):
            text = text.replace("git add", "").replace("git commit", "").replace('git("', "")
        assert not re.search(r"git\s+(add|commit|push|stash|checkout|reset|rm)\b", text), relative


def test_46b_nothing_is_staged_and_head_has_not_moved():
    staged = git("diff", "--cached", "--name-only")
    if staged.returncode != 0:
        pytest.skip("git is not available")
    assert staged.stdout.strip() == ""
    assert git("rev-parse", "HEAD").stdout.strip() == START_HEAD
    assert git("branch", "--show-current").stdout.strip() == "research/construct-validity-nov-2026"


def test_46c_the_demo_and_the_manifest_tool_refuse_to_overwrite_existing_output(tmp_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("a0l_e2e_demo_guard", REPO / NEW_SCRIPTS[1])
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "keep.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(SystemExit):
        module.run(tmp_path / "out")
    assert (tmp_path / "out" / "keep.txt").read_text(encoding="utf-8") == "keep"
    text = (REPO / NEW_SCRIPTS[2]).read_text(encoding="utf-8")
    assert "finally:" in text and "RESTORE FAILED" in text and "if args.out.exists()" in text
