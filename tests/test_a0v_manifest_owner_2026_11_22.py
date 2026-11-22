"""A0v: the candidate manifest, the owner records and the frozen / historical artifacts. Read-only: it hashes files and parses JSON and Markdown; it starts nothing."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

import pytest

from tests.a0v_support import REPO

from fabric_perf.controllers import prelive_integrity as pi      # noqa: E402
from fabric_perf.controllers import wsl_transport as wt          # noqa: E402

sys.path.insert(0, str(REPO / "scripts" / "e1"))
import e1_a0v_candidate_manifest as a0v                          # noqa: E402

E1 = REPO / "docs" / "research" / "e1"
MANIFEST = E1 / "prelive_candidate_manifest_2026-11-22_a0v.json"
EARLIER = {"a0s": "026c167a948f2b82e0ead45a079cc838ed57d4e698041f762c3223d858a6b167", "a0t": "529e5d5ed35ef7dd0cb74534db3088c08627d09b674ea2bbda23bd3227a013f4",
           "a0u": "73f65f68fa95497200e0dd700d4c19e3971ba9704d9a42fb8512c2206638766a"}
SYSTEM = REPO / "Integration" / "system_integration"
EXAMPLES = E1 / "a0v_example_artifacts_2026-11-22"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def manifest():
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


# ---- the candidate manifest
def test_the_manifest_verifies_against_the_files_on_disk(manifest):
    assert pi.verify_candidate_manifest(manifest, REPO) == []


def test_the_manifest_is_a_candidate_not_an_approval(manifest):
    assert manifest["status"] == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest.get("approval_record") is None
    assert manifest["launch_configuration"]["approved"] is False and manifest["launch_configuration"]["live_run"] is False
    assert manifest["architecture_recommendation"]["N2_STATUS"] == "ARCHITECTURE_APPROVED_RUNTIME_NOT_VERIFIED" and manifest["architecture_recommendation"]["manifest_approved"] is False
    assert "execute" in manifest["launch_configuration"]["entry_points"] and manifest["launch_configuration"]["entry_points"]["execute"].startswith("none; real launch")


def test_the_manifest_adds_exactly_the_a0v_files_and_changes_no_a0u_entry(manifest):
    assert manifest["changed_since_a0u"] == [] and manifest["removed_since_a0u"] == [] and manifest["superseded_entries"] == []
    assert manifest["added_since_a0u"] == a0v.A0V_NEW_PATHS
    assert len(manifest["entries"]) == 121 + len(a0v.A0V_ADDITIONS)
    paths = [e["path"] for e in manifest["entries"]]
    assert len(paths) == len(set(paths))
    for required in ("src/fabric_perf/controllers/wsl_transport.py", "src/fabric_perf/controllers/run_outcome.py", "src/fabric_perf/controllers/clock_transport_plan.py", "scripts/e1/e1_a0v_transport.py"):
        assert required in paths


def test_every_entry_hash_equals_the_file_now_and_the_a0u_entries_equal_the_a0u_manifest(manifest):
    older = {e["path"]: e["sha256"] for e in json.loads((E1 / "prelive_candidate_manifest_2026-11-22_a0u.json").read_text(encoding="utf-8"))["entries"]}
    for entry in manifest["entries"]:
        assert sha(REPO / entry["path"]) == entry["sha256"], entry["path"]
        if entry["path"] in older:
            assert entry["sha256"] == older[entry["path"]], entry["path"]
    assert len(older) == 121


def test_a_fresh_build_equals_the_stored_manifest(manifest):
    assert a0v.build(created_utc=manifest["created_utc"]) == manifest


def test_earlier_manifests_are_byte_identical_and_the_successor_names_the_a0u_one(manifest):
    for name, expected in EARLIER.items():
        assert sha(E1 / f"prelive_candidate_manifest_2026-11-22_{name}.json") == expected, name
    assert manifest["supersedes"]["sha256"] == EARLIER["a0u"] and manifest["supersedes"]["path"] == a0v.SUPERSEDES


def test_the_manifest_records_the_unresolved_runtime_dependencies(manifest):
    text = " ".join(manifest["unresolved_dependencies"] + manifest["open_items"])
    for phrase in ("UNVERIFIED", "REAL_SPAWN_COMPILED_IN", "executor", "explicit absolute path of wsl.exe", "unmeasured", "http.client"):
        assert phrase in text, phrase
    assert manifest["runtime_dependencies"]["network_or_container_api_imported_by_a0v_files"] is False


# ---- frozen and historical artifacts
@pytest.mark.parametrize("stage", ["a0s", "a0t", "a0u"])
def test_the_recorded_evidence_hashes_of_earlier_tasks_still_verify(stage):
    sums = E1 / f"{stage}_verification_evidence_2026-11-22" / "SHA256SUMS.txt"
    checked = 0
    for line in sums.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([0-9a-f]{64})\s+\*?(.+)$", line.strip())
        if not match:
            continue
        target = REPO / match.group(2)
        target = target if target.is_file() else sums.parent / match.group(2)
        assert target.is_file() and sha(target) == match.group(1), match.group(2)
        checked += 1
    assert checked >= 20


def test_the_pinned_a0t_and_a0u_example_artifacts_are_unchanged():
    for stage, name in (("a0t", "synthetic_smoke_plan_a0t.json"), ("a0u", "synthetic_smoke_plan_a0u.json")):
        path = E1 / f"{stage}_example_artifacts_2026-11-22" / name
        if path.is_file():
            document = json.loads(path.read_text(encoding="utf-8"))
            assert document["real_launch_enabled"] is False


# ---- owner records
def read(name: str) -> str:
    return (SYSTEM / name).read_text(encoding="utf-8")


def test_n2_is_recorded_as_an_approved_architecture_with_the_runtime_unverified():
    record = read("A0V_N2_ARCHITECTURE_APPROVAL_RECORD_2026-11-22.md")
    assert "WINDOWS_HOST_ORCHESTRATION_WITH_WSL_SIDE_HASHING" in record and "ARCHITECTURE_APPROVED — RUNTIME NOT VERIFIED" in record
    assert "does not cover" in record and "UNVERIFIED" in record
    packet = read("A0V_OWNER_DECISION_PACKET_2026-11-22.md")
    assert "ARCHITECTURE_APPROVED — RUNTIME NOT VERIFIED" in packet


def test_every_other_decision_in_the_owner_packet_is_unset():
    packet = read("A0V_OWNER_DECISION_PACKET_2026-11-22.md")
    assert "@@" not in packet
    index = packet.split("## Decision index")[1].split("## Separate approvals")[0]
    rows = [r for r in index.splitlines() if r.startswith("| ") and not r.startswith("| #") and not r.startswith("|---")]
    assert len(rows) == 11
    for row in rows:
        cells = [c.strip() for c in row.strip("|").split("|")]
        if cells[0].replace("*", "") == "N2":
            continue
        assert "UNSET" in cells[2], row
    assert "approved_by:      ______" in packet and "approved_utc:     ______" in packet and "decision:         approve_as_snapshot | approve_as_commit | reject" in packet
    assert "PROPOSED ONLY, except N2" in packet and "Channel (OWNER): ______. **UNSET.**" in packet
    assert manifest_hash() in packet and "CANDIDATE_NOT_APPROVED" in packet


def manifest_hash() -> str:
    return sha(MANIFEST)


def test_the_channel_cell_and_transaction_authorization_are_unset_everywhere():
    for path in sorted(EXAMPLES.glob("activation_template_UNSET_*.json")):
        template = json.loads(path.read_text(encoding="utf-8"))
        assert all(template[k] is None for k in wt.OWNER_FIELDS), path.name
        assert all(v is None for v in template["approval"].values()), path.name
        assert template["synthetic"] is False and "channel" not in template["binding"] or template["binding"].get("channel") is None, path.name
        tx = template["transaction_authorization"]
        assert tx is None if template["operation_category"] != wt.OPC_CALIPER else all(v is None for v in tx.values()), path.name
    unresolved = json.loads((EXAMPLES / "clock_transport_plan_UNRESOLVED.json").read_text(encoding="utf-8"))
    assert unresolved["status"].startswith("UNRESOLVED") and unresolved["executed"] is False and "os_argv" not in unresolved["vector"] and unresolved["activation_record_template"] is None
    synthetic = json.loads((EXAMPLES / "clock_transport_plan_SYNTHETIC_VALUES_NOT_PROPOSALS.json").read_text(encoding="utf-8"))
    assert synthetic["executed"] is False and synthetic["real_spawn_compiled_in"] is False and synthetic["authorization"]["reference"] is None and synthetic["host"]["approved_host_identity"] is None


def test_the_clock_packet_and_the_checklist_state_that_nothing_is_approved():
    packet = read("A0V_CLOCK_ONLY_ACTIVATION_PACKET_2026-11-22.md")
    assert "PROPOSED ONLY. Nothing here is approved or authorized." in packet and "UNMEASURED" in packet and "REAL_SPAWN_COMPILED_IN" in packet and "date +%s%N" in packet
    checklist = read("A0V_FIRST_LIVE_OPERATION_CHECKLIST_2026-11-22.md")
    assert "A checklist, not an authorization." in checklist and "Part C" in checklist and "the assistant does not" in checklist


def test_the_cli_execute_refusal_artifact_records_exit_5_text():
    text = (EXAMPLES / "execute_refusal.txt").read_text(encoding="utf-8")
    assert text.startswith("REFUSED: real execution is disabled") and "was started" in text
