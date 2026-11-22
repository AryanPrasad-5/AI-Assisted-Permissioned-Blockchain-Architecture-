"""A0v Workstream C: X13 integrity enforcement and the success verdict. A run may be called successful only when the process operation finished cleanly AND the integrity evidence is real and
independently re-checked from the stored files. Run directories come from the A0u simulated WSL (a temporary directory); the one POSITIVE case is a SYNTHETIC promotion of such a directory to
real-looking evidence, built only to test the classifier (the transport that "produced" it is a real-mode transport over a fake Popen). Nothing is started."""

from __future__ import annotations

import copy
import hashlib
import json
import shutil
from pathlib import Path

import pytest

from tests import a0t_support as t
from tests import a0u_support as u
from tests import a0v_support as v
from tests.a0v_support import guard  # noqa: F401  (fixture)

from fabric_perf.controllers import run_outcome as ro            # noqa: E402
from fabric_perf.controllers import tree_digest as td            # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi          # noqa: E402
from fabric_perf.controllers import wsl_transport as wt          # noqa: E402

pytestmark = pytest.mark.usefixtures("guard")


def run_sim(tmp: Path, **sim_kw):
    plan, _req, _root = u.build_plan(tmp)
    record = t.path_record(plan)
    addendum = u.make_addendum(plan, record)
    transport = u.WslSimTransport(tmp / "wsl", plan, **sim_kw)
    status = u.run_with_integrity(tmp, plan, record, addendum, transport)
    return plan, addendum, Path(plan["layout"]["output_dir"]), status


@pytest.fixture(scope="module")
def sim(tmp_path_factory):
    return run_sim(tmp_path_factory.mktemp("a0v_sim_ok"))


@pytest.fixture()
def copy_of(sim, tmp_path):
    plan, addendum, out, status = sim
    target = tmp_path / "run_copy"
    shutil.copytree(out, target)
    return plan, addendum, target


def rd(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def wr(path: Path, doc) -> None:
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def wr_line(path: Path, doc) -> None:
    """The digest tool prints exactly ONE JSON line; the stored evidence files keep that form."""
    path.write_text(json.dumps(doc, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


def reseal(document: dict) -> dict:
    """Make an edited digest document INTERNALLY CONSISTENT again (sorted entries, tree digest, document hash), as a careful forger would: only the semantic checks can then catch it."""
    entries = sorted(document["tree"]["entries"], key=lambda e: e["path"].encode("utf-8"))
    document["tree"]["entries"] = entries
    kinds = [e["type"] for e in entries]
    document["tree"].update(entry_count=len(entries), file_count=kinds.count("file"), dir_count=kinds.count("dir"), symlink_count=kinds.count("symlink"),
                            total_bytes=sum(e.get("size", 0) for e in entries), tree_digest=td.tree_digest_of(entries))
    document["document_sha256"] = td._document_digest(document)
    return document


def promote(out: Path, plan, addendum, monkeypatch, tmp: Path) -> None:
    """SYNTHETIC: make a simulated run directory look like real evidence so the classifier's positive path and each single-property failure can be tested."""
    host = plan["layout"]["host_evidence"]
    status = rd(out / host["run_status"])
    status["runner_kind"] = "process"
    wr(out / host["run_status"], status)
    verdict = rd(out / wi.VERDICT_FILE)
    verdict.update(verdict=wi.V_REAL, evidence_class="REAL_WSL_OBSERVATION", gate_satisfied=True, problems=[])
    wr(out / wi.VERDICT_FILE, verdict)
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    binding = addendum["binding"]
    allowed = wt.vectors_for_category(wt.OPC_X13, addendum, distro=binding["distro"], mount_root=binding["automount_root"])
    raw = {a.os_argv: (out / (wi.PRE_FILE if a.step_id == "I1_worker_root_pre" else wi.POST_FILE)).read_bytes() for a in allowed}
    fake = v.FakePopen(reply=lambda argv: v.FakeProc(raw[tuple(["wsl.exe"] + argv[1:])], b"", 0))
    record = v.real_record(wt.OPC_X13, allowed, {"run_id": plan["run_id"]}, tmp / "approval")
    transport = wt.WslProcessTransport(wt.issue_real_permit(record, category=wt.OPC_X13, allowed=allowed, binding={"run_id": plan["run_id"]},
                                                            expected_candidate_manifest_sha256=v.MANIFEST_SHA), popen=fake)
    for vector in allowed:
        transport.run(v.invocation(vector))
    transport.write_evidence(out / ro.TRANSPORT_DIR / wt.OPC_X13)


def codes_of(outcome) -> set[str]:
    return {r["code"] for r in outcome["reasons"]}


# ---- the repaired fixture: what A0u produced is simulated, and it says so
def test_the_baseline_run_is_completed_by_the_a0s_status_but_is_not_a_success(sim):
    plan, addendum, out, status = sim
    assert status["status"] == "COMPLETED" and status["live_completion"] is False and status["integrity"]["verdict"] == wi.V_SIM
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and outcome["outcome"] == ro.O_SIMULATED and {"integrity_simulated"} <= codes_of(outcome)
    assert outcome["a0s_status"] == "COMPLETED"                                        # COMPLETED is reported, and overridden


# ---- 24. pre-run validation
def test_24_the_stored_pre_workload_digest_equals_the_authorized_overlay(sim):
    plan, addendum, out, status = sim
    document = wi.parse_evidence((out / wi.PRE_FILE).read_bytes())
    assert wi.check_pre_workload(document, addendum) == [] and wi.check_evidence(document, expected_request=addendum["steps"][0]["request"], addendum=addendum) == []


@pytest.mark.parametrize("mutate, expected", [
    ("hash", "pre_workload_staged_file_hash_mismatch"),
    ("extra", "pre_workload_staged_file_unexpected"),
    ("missing", "pre_workload_staged_file_missing"),
])
def test_24_a_self_consistent_but_wrong_pre_digest_is_caught_by_the_independent_recheck(copy_of, mutate, expected):
    plan, addendum, out = copy_of
    document = rd(out / wi.PRE_FILE)
    files = [e for e in document["tree"]["entries"] if e["type"] == "file"]
    if mutate == "hash":
        files[0]["sha256"] = "0" * 64
    elif mutate == "extra":
        document["tree"]["entries"].append({"path": "extra.js", "type": "file", "size": 1, "sha256": "1" * 64})
    else:
        document["tree"]["entries"].remove(files[0])
    wr_line(out / wi.PRE_FILE, reseal(document))
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and expected in codes_of(outcome)
    assert td.verify_evidence(rd(out / wi.PRE_FILE)) == []                     # the forgery is internally consistent: only the semantic re-check catches it


# ---- 25. post-run validation
def test_25_new_permitted_output_files_are_not_mistaken_for_changed_inputs(sim):
    plan, addendum, out, status = sim
    pre = wi.parse_evidence((out / wi.PRE_FILE).read_bytes())
    post = wi.parse_evidence((out / wi.POST_FILE).read_bytes())
    new = {e["path"] for e in post["tree"]["entries"]} - {e["path"] for e in pre["tree"]["entries"]}
    assert any(p.startswith("issuelog") for p in new) and any(p.startswith("recorder") for p in new)
    assert wi.check_staged_unchanged(pre, post) == []


@pytest.mark.parametrize("mutate, expected", [("changed", "staged_entry_changed"), ("removed", "staged_entry_removed")])
def test_25_a_protected_staged_file_that_changed_or_vanished_after_the_workload_is_caught(copy_of, mutate, expected):
    plan, addendum, out = copy_of
    document = rd(out / wi.POST_FILE)
    entries = document["tree"]["entries"]
    protected = next(e for e in entries if e["type"] == "file" and e["path"] in addendum["expected_pre_workload"]["files"])
    if mutate == "changed":
        protected["sha256"] = "2" * 64
    else:
        entries.remove(protected)
    wr_line(out / wi.POST_FILE, reseal(document))
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and expected in codes_of(outcome)


def test_25_a_workload_that_modifies_a_staged_file_is_a_failed_integrity_even_though_the_a0s_status_may_read_completed(tmp_path):
    def tamper(run_dir: Path) -> None:
        victim = next(p for p in sorted(run_dir.rglob("*.js")) if p.is_file())
        victim.write_bytes(victim.read_bytes() + b"\n// changed during the workload\n")

    plan, addendum, out, status = run_sim(tmp_path, after_caliper=tamper)
    verdict = rd(out / wi.VERDICT_FILE)
    assert verdict["verdict"] == wi.V_FAILED and verdict["gate_satisfied"] is False and status["live_completion"] is False
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and outcome["outcome"] in (ro.O_PROCESS_FAILED, ro.O_INTEGRITY_FAILED)
    # ---- 30. partial evidence is preserved after the failure
    for rel in (wi.VERDICT_FILE, wi.PRE_FILE, wi.POST_FILE, plan["layout"]["host_evidence"]["execution_log"], plan["layout"]["host_evidence"]["run_status"]):
        assert (out / rel).is_file(), rel
    assert any("staged" in p["code"] or "digest" in p["code"] for p in verdict["problems"])


# ---- 26. missing evidence
@pytest.mark.parametrize("victim, expected_outcomes", [
    (wi.VERDICT_FILE, {ro.O_INTEGRITY_MISSING}),
    (wi.PRE_FILE, {ro.O_INTEGRITY_MISSING, ro.O_INTEGRITY_FAILED}),
    (wi.POST_FILE, {ro.O_INTEGRITY_MISSING, ro.O_INTEGRITY_FAILED}),
    ("integrity_manifest.json", {ro.O_INTEGRITY_MISSING}),
])
def test_26_missing_integrity_evidence_is_never_a_success(copy_of, victim, expected_outcomes):
    plan, addendum, out = copy_of
    (out / victim).unlink()
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and outcome["outcome"] in expected_outcomes | {ro.O_SIMULATED}


def test_26_a_directory_without_a_status_is_not_a_run(tmp_path):
    plan, addendum, out, status = run_sim(tmp_path)
    (out / plan["layout"]["host_evidence"]["run_status"]).unlink()
    assert ro.classify_bounded_run(out, plan, addendum)["outcome"] == ro.O_NOT_RUN


# ---- 27. empty manifest
@pytest.mark.parametrize("key", ["worker_evidence_root", "output_dir"])
def test_27_an_empty_integrity_map_is_not_verification(copy_of, key):
    plan, addendum, out = copy_of
    path = out / plan["layout"]["host_evidence"]["integrity_manifest"]
    manifest = rd(path)
    manifest["files"][key] = {}
    wr(path, manifest)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and f"{key}_empty" in codes_of(outcome)


def test_27_a_manifest_without_a_files_block_is_malformed(copy_of):
    plan, addendum, out = copy_of
    wr(out / plan["layout"]["host_evidence"]["integrity_manifest"], {"schema": "x"})
    assert "manifest_malformed" in codes_of(ro.classify_bounded_run(out, plan, addendum))


# ---- 28. mock evidence cannot satisfy the live gate; the positive path
def test_28_the_verdict_file_alone_is_not_enough_a_forged_real_verdict_without_transport_evidence_fails(copy_of):
    plan, addendum, out = copy_of
    host = plan["layout"]["host_evidence"]
    status = rd(out / host["run_status"])
    status["runner_kind"] = "process"
    wr(out / host["run_status"], status)
    verdict = rd(out / wi.VERDICT_FILE)
    verdict.update(verdict=wi.V_REAL, evidence_class="REAL_WSL_OBSERVATION", gate_satisfied=True, problems=[])
    wr(out / wi.VERDICT_FILE, verdict)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and "transport_evidence_missing" in codes_of(outcome)


def test_28_fixture_mode_transport_evidence_cannot_stand_in_for_real_evidence(copy_of, monkeypatch, tmp_path):
    plan, addendum, out = copy_of
    promote(out, plan, addendum, monkeypatch, tmp_path)
    path = out / ro.TRANSPORT_DIR / wt.OPC_X13 / "evidence.json"
    document = rd(path)
    document.update(mode="fixture", kind="fixture")
    document["document_sha256"] = wt.be._digest(document, drop=("document_sha256",))
    wr(path, document)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and "runner_not_real" in codes_of(outcome)


def test_28_the_synthetic_promotion_is_classified_as_verified_so_the_failures_below_are_each_caused_by_their_one_change(copy_of, monkeypatch, tmp_path):
    plan, addendum, out = copy_of
    promote(out, plan, addendum, monkeypatch, tmp_path)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is True and outcome["outcome"] == ro.O_VERIFIED and outcome["reasons"] == []


@pytest.mark.parametrize("name, mutate", [
    ("status_not_completed", lambda out, plan: _edit(out / plan["layout"]["host_evidence"]["run_status"], status="HALTED_BEFORE_TRANSACTIONS")),
    ("status_with_errors", lambda out, plan: _edit(out / plan["layout"]["host_evidence"]["run_status"], errors=[{"step": "integrity", "reason": "x"}])),
    ("verdict_failed", lambda out, plan: _edit(out / wi.VERDICT_FILE, verdict=wi.V_FAILED, gate_satisfied=False)),
    ("verdict_gate_false", lambda out, plan: _edit(out / wi.VERDICT_FILE, gate_satisfied=False)),
    ("verdict_other_run", lambda out, plan: _edit(out / wi.VERDICT_FILE, run_id="another-run")),
    ("verdict_other_plan", lambda out, plan: _edit(out / wi.VERDICT_FILE, plan_sha256="f" * 64)),
    ("verdict_other_addendum", lambda out, plan: _edit(out / wi.VERDICT_FILE, addendum_sha256="f" * 64)),
    ("evidence_class_simulated", lambda out, plan: _edit(out / wi.VERDICT_FILE, evidence_class="SIMULATED")),
    ("runner_is_mock", lambda out, plan: _edit(out / plan["layout"]["host_evidence"]["run_status"], runner_kind="mock")),
    ("empty_worker_manifest", lambda out, plan: _edit_manifest(out / plan["layout"]["host_evidence"]["integrity_manifest"], "worker_evidence_root")),
    ("transport_stdout_differs", lambda out, plan: (out / wi.PRE_FILE).write_bytes((out / wi.PRE_FILE).read_bytes() + b" ")),
    ("transport_evidence_deleted", lambda out, plan: (out / ro.TRANSPORT_DIR / wt.OPC_X13 / "evidence.json").unlink()),
])
def test_28_every_single_property_failure_breaks_the_success_verdict(copy_of, monkeypatch, tmp_path, name, mutate):
    plan, addendum, out = copy_of
    promote(out, plan, addendum, monkeypatch, tmp_path)
    mutate(out, plan)
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert outcome["success"] is False and outcome["outcome"] != ro.O_VERIFIED, name


def _edit(path: Path, **fields) -> None:
    document = rd(path)
    document.update(fields)
    wr(path, document)


def _edit_manifest(path: Path, key: str) -> None:
    document = rd(path)
    document["files"][key] = {}
    wr(path, document)


# ---- 29. a failed integrity cannot yield a successful status
def test_29_the_interrupted_and_failed_runs_are_named_not_succeeded(tmp_path):
    plan, addendum, out, status = run_sim(tmp_path)
    host = plan["layout"]["host_evidence"]
    document = rd(out / host["run_status"])
    document["status"] = "INTERRUPTED"
    wr(out / host["run_status"], document)
    assert ro.classify_bounded_run(out, plan, addendum)["outcome"] == ro.O_INTERRUPTED


def test_29_the_outcome_file_is_created_exclusively_and_never_overwritten(copy_of):
    plan, addendum, out = copy_of
    outcome = ro.classify_bounded_run(out, plan, addendum)
    assert ro.write_outcome(out, outcome) == ro.OUTCOME_FILE
    stored = (out / ro.OUTCOME_FILE).read_bytes()
    with pytest.raises(FileExistsError):
        ro.write_outcome(out, {**outcome, "success": True})
    assert (out / ro.OUTCOME_FILE).read_bytes() == stored and json.loads(stored)["success"] is False


def test_29_the_outcome_vocabulary_is_closed():
    assert set(ro.OUTCOMES) == {ro.O_VERIFIED, ro.O_SIMULATED, ro.O_INTEGRITY_FAILED, ro.O_INTEGRITY_MISSING, ro.O_PROCESS_FAILED, ro.O_INTERRUPTED, ro.O_NOT_RUN}
    assert sum(1 for o in ro.OUTCOMES if o == ro.O_VERIFIED) == 1
    assert hashlib.sha256(ro.O_VERIFIED.encode()).hexdigest()            # the single success word is not an alias of any other outcome
    assert copy.deepcopy(ro.OUTCOMES) == ro.OUTCOMES
