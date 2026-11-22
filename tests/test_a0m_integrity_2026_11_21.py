"""A0m integrity and safety (2026-11-21): the A0m candidate manifest, earlier manifests and pinned/historical files byte-identical, no tracked file touched by A0m, no Git
staging, no process/network use by the new tool, and the stored mutation results. Reads and hashes files; nothing is loaded or executed beyond offline code."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path

import pytest

from tests import a0j_support as j
from tests.test_a0l_integrity_2026_11_21 import A0K_MANIFEST_SHA

from fabric_perf.controllers import prelive_integrity as pi

import e1_a0m_candidate_manifest as mk  # noqa: E402

REPO = j.REPO
E1 = REPO / "docs" / "research" / "e1"
A0L_MANIFEST = E1 / "prelive_candidate_manifest_2026-11-21_a0l.json"
A0M_MANIFEST = E1 / "prelive_candidate_manifest_2026-11-21_a0m.json"
EVIDENCE = REPO / "Integration" / "system_integration" / "a0m_2026-11-21"
START_HEAD = "8c67a4678b3901c8d601fb1a33210d7d8c84cf78"
# SHA-256 of every earlier candidate manifest, captured at the start of A0m (before anything was written).
EARLIER = {
    "prelive_candidate_manifest_2026-11-21.json": "13640a7f683adcfec96f26f9060b05f29306bb2ebcfeb4c4b90c5fc43fa2b984",
    "prelive_candidate_manifest_2026-11-21_a0g.json": "491c7d0c296515bf24cd2a8d4462be3f34a81926f933e5d7542a45105eb5a5e7",
    "prelive_candidate_manifest_2026-11-21_a0h.json": "d6c451cfae089642d8c5c6be2c80d1fbdef7f718d1d2888c95d86275c1fda689",
    "prelive_candidate_manifest_2026-11-21_a0i.json": "7bc73bc7f648c92e3d515e4e9e98f41df1a552d2c9bd8bad204f78761d2a7c66",
    "prelive_candidate_manifest_2026-11-21_a0j.json": "831cb76ecdbd75318ebc7d4fe9be0d0c3eedfe7873a9a2190fa3446dd736c594",
    "prelive_candidate_manifest_2026-11-21_a0k.json": A0K_MANIFEST_SHA,
    "prelive_candidate_manifest_2026-11-21_a0l.json": "b256ba9e04e699be412d362c3d167a7c5b58204c442cb3bb17afac5c2b0efcb4",
}
PINNED = {"scripts/e1/e1_la2_render.py": "f77a83e7de106b3c1dac28f93333e8dbaf67ea869836944b906f9bc1aabe3d83",
          "scripts/e1/caliper/e1_per_tx_recorder.js": "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8"}
LEGACY_WORKLOAD = "fabric-testbed/caliper/workloads/e1_create_asset.js"
LEGACY_SHA = "c5c234b71666e6b8e135f26f8c65d2e1639c3d54784213cfca0cc15a50f5667f"
NEW = ("scripts/e1/e1_a0m_dry_render.py", "fabric-testbed/network/env-e1-caliper-benchmark.json", "fabric-testbed/network/network-e1-recovery.json")
# The 21 tracked files that were already modified (uncommitted work of earlier tasks) when A0m started.
PRE_EXISTING_MODIFIED = {
    "Fabric_Live_Testbed/controllers/run_production_shadow_validation.py", "Flashbots/forecasting/benchmark_flashbots_baselines.py", "Flashbots/forecasting/run_forecast_benchmark.py",
    "Flashbots/time_series/build_flashbots_series.py", "Integration/system_integration/run_integrated_controller.py", "scripts/e1/build_contemporaneous_transaction_evidence.py",
    "scripts/e1/e1_existing_channel_pilot.py", "scripts/e1/e1_final_comparison.py", "scripts/e1/e1_independent_config_verify.py", "scripts/e1/e1_metrics_scraper.py",
    "scripts/e1/run_contemporaneous_capture.py", "scripts/e1/synchronized_shadow_replay.py", "src/fabric_perf/controllers/__init__.py", "src/fabric_perf/controllers/adapters.py",
    "src/fabric_perf/controllers/base.py", "src/fabric_perf/controllers/harness.py", "src/fabric_perf/controllers/live_integration.py", "src/fabric_perf/controllers/strategies.py",
    "src/fabric_perf/controllers/telemetry.py", "src/fabric_perf/models/artifacts.py", "tests/test_controllers.py"}


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def manifest() -> dict:
    return json.loads(A0M_MANIFEST.read_text(encoding="utf-8"))


def git(*args):
    try:
        return subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True, check=False)
    except OSError:
        pytest.skip("git is not available")


def test_29_every_listed_hash_matches_and_the_manifest_is_a_candidate_not_an_approval():
    m = manifest()
    assert pi.verify_candidate_manifest(m, REPO) == [] and len(m["entries"]) == 79
    assert mk.main(["verify", "--manifest", str(A0M_MANIFEST)]) == 0
    assert m["status"] == "CANDIDATE_NOT_APPROVED" and m["approved"] is False and m["approval_record"] is None
    assert "nothing in this file is an approval" in m["approval_note"].lower()
    assert all(e["approval_status"] in {pi.PENDING_OWNER_APPROVAL, pi.PINNED_ELSEWHERE_UNCHANGED, pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT} for e in m["entries"])
    assert m["launch_configuration"]["live_run"] is False and m["launch_configuration"]["phases"]["execute"].startswith("refused")


def test_29b_the_successor_repeats_every_a0l_entry_adds_exactly_the_three_files_and_supersedes_nothing():
    m = manifest()
    old = {e["path"]: e for e in json.loads(A0L_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    new = {e["path"]: e for e in m["entries"]}
    assert set(old) < set(new) and len(old) == 76
    assert set(m["added_since_a0l"]) == set(new) - set(old) == set(NEW)
    assert m["changed_since_a0l"] == [] and m["removed_since_a0l"] == [] and m["superseded_entries"] == [] and "nothing is superseded" in m["superseded_note"]
    for path, entry in old.items():
        assert new[path] == entry, path                                                  # the whole entry: hash, size, role, approval status and pin
    assert all(new[p]["approval_status"] == pi.PENDING_OWNER_APPROVAL and new[p]["pinned_sha256"] is None for p in NEW)
    assert m["supersedes"]["sha256"] == EARLIER["prelive_candidate_manifest_2026-11-21_a0l.json"] == sha(A0L_MANIFEST)
    assert m["git"]["branch"] == "research/construct-validity-nov-2026" and m["git"]["commit"] == START_HEAD


def test_29c_the_unresolved_dependencies_and_open_items_state_the_live_limits():
    m = manifest()
    text = " ".join(m["unresolved_dependencies"])
    for needle in ("OFFLINE ONLY", "FAKE Caliper base class", "sourceCount", "recorder_exceeds_source", "poll cadence", "One worker only", "@hyperledger/caliper-core",
                   "DECLARED by data flow", "placeholders with no scientific justification"):
        assert needle.lower() in text.lower(), needle
    assert any("owner approval of the three a0m additions" in item.lower() for item in m["open_items"])
    assert "a0m_2026-11-21" in " ".join(item["item"] for item in m["exclusions"])


def test_29d_the_builder_reproduces_the_stored_entries_cannot_overwrite_and_the_verifier_detects_a_change(tmp_path):
    assert mk.build(REPO, created_utc=manifest()["created_utc"])["entries"] == manifest()["entries"]
    out = tmp_path / "m.json"
    out.write_text("{}", encoding="utf-8")
    assert mk.main(["write", "--out", str(out)]) == 2 and out.read_text(encoding="utf-8") == "{}"
    forged = manifest()
    forged["entries"][-1]["sha256"] = "0" * 64
    assert [f.code for f in pi.verify_candidate_manifest(forged, REPO)] == ["modified"]


def test_29e_the_import_closure_is_listed_and_no_frozen_artifact_is_listed():
    listed = {e["path"] for e in manifest()["entries"]}
    assert mk.a0l.a0k.a0j.a0i.a0h.a0g.local_import_closure(mk.CLOSURE_ROOTS) <= listed
    assert {"scripts/e1/e1_prelive_validation.py", "scripts/e1/e1_recorder_v2_wire.py", "scripts/e1/e1_telemetry_common.py", "scripts/e1/e1_run_orchestrator.py",
            "scripts/e1/e1_channel_run.py", "scripts/e1/caliper/e1_per_tx_recorder_v3.js", "fabric-testbed/caliper/workloads/e1_issue_log.js"} <= listed
    assert not listed & {"experiments/artifacts/hfbtp/throughput.joblib", "experiments/artifacts/hfbtp/avg_latency.joblib", "data/raw/hfbtp/HFBTP.csv"}


def test_30_every_earlier_manifest_is_byte_identical():
    for name, digest in EARLIER.items():
        assert sha(E1 / name) == digest, name


def test_31_pinned_historical_and_candidate_files_are_byte_identical():
    for path, digest in PINNED.items():
        assert sha(REPO / path) == digest, path
    assert sha(REPO / LEGACY_WORKLOAD) == LEGACY_SHA
    assert "issue" not in (REPO / LEGACY_WORKLOAD).read_text(encoding="utf-8").lower()
    tracked = git("ls-files", "--error-unmatch", LEGACY_WORKLOAD, "scripts/e1/e1_la2_render.py", "scripts/e1/caliper/e1_per_tx_recorder.js")
    if tracked.returncode == 0:                                                           # a checkout: the files also equal their committed blobs
        assert git("diff", "--quiet", "HEAD", "--", LEGACY_WORKLOAD, "scripts/e1/e1_la2_render.py", "scripts/e1/caliper/e1_per_tx_recorder.js").returncode == 0
    renderer = (REPO / "scripts/e1/e1_la2_render.py").read_text(encoding="utf-8")
    assert "e1_create_asset_issue_logged" not in renderer and "recorder_v3" not in renderer      # the pinned renderer does not select the successors


def test_32_a0m_modified_no_tracked_file_beyond_the_ones_already_modified_before_it_started():
    out = git("diff", "--name-only", "HEAD")
    modified = {line for line in out.stdout.splitlines() if line}
    assert modified <= PRE_EXISTING_MODIFIED, sorted(modified - PRE_EXISTING_MODIFIED)


def test_33_the_new_tool_and_runner_never_stage_commit_or_push():
    for relative in ("scripts/e1/e1_a0m_dry_render.py", "scripts/e1/e1_a0m_candidate_manifest.py", "tests/a0m_support.py", "tests/e1/js/a0m_generated_run.js",
                     "Integration/system_integration/a0m_2026-11-21/mutation_runner.py"):
        assert not re.search(r"git\s+(add|commit|push|stash|checkout|reset|rm)\b", (REPO / relative).read_text(encoding="utf-8")), relative


def test_33b_nothing_is_staged_and_head_has_not_moved():
    assert git("diff", "--cached", "--name-only").stdout.strip() == ""
    assert git("rev-parse", "HEAD").stdout.strip() == START_HEAD
    assert git("branch", "--show-current").stdout.strip() == "research/construct-validity-nov-2026"


def test_34_the_stored_mutation_results_account_for_every_planned_mutant_and_count_nothing_unexecuted_as_killed():
    summary = json.loads((EVIDENCE / "mutation_summary.json").read_text(encoding="utf-8"))
    assert summary["planned"] == {"a0l": 48, "a0m": 20} and len(summary["mutants"]) == 68
    states = {m["id"]: m["state"] for m in summary["mutants"]}
    assert "NOT_EXECUTED" not in states.values() and "SURVIVED" not in states.values() and "INFRA_ERROR" not in states.values()
    assert [k for k, v in states.items() if v == "EQUIVALENT"] == ["A08"] and sum(v == "KILLED" for v in states.values()) == 67
    assert "UNREACHABLE" in summary["equivalent_arguments"]["A08"]
    rows = [json.loads(line) for line in (EVIDENCE / "mutation_results.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    last = {r["id"]: r for r in rows}
    assert set(last) == set(states) and all(r["outcome"] in {"KILLED", "SURVIVED"} for r in last.values())
    first_pass_survivors = {r["id"] for r in rows if r["outcome"] == "SURVIVED"}
    assert first_pass_survivors == {"M44", "A08", "A09", "A10", "A11", "A15"}             # recorded, then closed by new tests (A08: equivalent)


def test_35_the_stored_dry_render_evidence_still_verifies_against_its_own_sums():
    sums = EVIDENCE / "SHA256SUMS.txt"
    if not sums.exists():
        pytest.skip("evidence sums not written yet")
    superseded = _superseded_verification_files()
    checked = drifted = documented = 0
    for line in sums.read_text(encoding="utf-8").splitlines():
        digest, _sep, name = line.partition(" ")
        name = name.lstrip(" *")
        path = REPO / name
        assert path.is_file(), path
        checked += 1
        if sha(path) != digest:
            # A0q (2026-11-22): a verification file may differ from its A0m-era hash ONLY when the A0q candidate manifest records exactly this old and this new hash.
            if superseded.get(name) == (digest, sha(path)):
                documented += 1
            else:
                drifted += 1
    assert checked >= 40 and drifted == 0 and documented <= 2


def _superseded_verification_files() -> dict:
    manifest = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-22_a0q.json"
    if not manifest.exists():
        return {}
    doc = json.loads(manifest.read_text(encoding="utf-8"))
    return {c["path"]: (c["old_sha256"], c["new_sha256"]) for c in doc.get("changed_verification_files", [])}
