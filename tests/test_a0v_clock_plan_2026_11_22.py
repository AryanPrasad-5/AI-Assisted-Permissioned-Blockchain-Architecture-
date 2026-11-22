"""A0v Workstream D: the clock-only transport plan and a clock-only round driven through the transport in FIXTURE mode (harmless local commands standing in for ``wsl.exe ... date``). Nothing samples
the real Windows or WSL clocks through ``wsl.exe``; the remote reading comes from a local Python child, and the tolerance and every count are SYNTHETIC test values, not proposals. The unchanged A0e
evaluator re-evaluates the stored raw samples."""

from __future__ import annotations

import json
import math
import time
from pathlib import Path

import pytest

from tests import a0v_support as v
from tests.a0v_support import guard  # noqa: F401  (fixture)
from tests.test_a0t_clock_only_2026_11_22 import approved as clock_auth

from fabric_perf.controllers import clock_only_readiness as cor  # noqa: E402
from fabric_perf.controllers import clock_only_round as co        # noqa: E402
from fabric_perf.controllers import clock_transport_plan as ctp   # noqa: E402
from fabric_perf.controllers import run_outcome as ro             # noqa: E402
from fabric_perf.controllers import wsl_clock_probe as wp         # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi           # noqa: E402
from fabric_perf.controllers import wsl_transport as wt           # noqa: E402

pytestmark = pytest.mark.usefixtures("guard")
PROBE = ("wsl.exe", "-d", "Ubuntu", "--exec", "date", "+%s%N")


def plan_for(tmp: Path, **over):
    return ctp.build_clock_transport_plan(v.clock_request(tmp, **over), created_utc=v.CREATED)


# ---- 31. valid plan generation
def test_31_a_complete_request_yields_a_resolved_plan_with_the_exact_vector_and_nothing_executed(tmp_path):
    plan = plan_for(tmp_path)
    assert plan["status"] == "RESOLVED_NOT_APPROVED_NOT_EXECUTED" and plan["executed"] is False and plan["real_spawn_compiled_in"] is False and plan["transactions_planned"] == 0
    assert tuple(plan["vector"]["os_argv"]) == PROBE and plan["vector"]["working_directory"] is None and plan["vector"]["shell"] is False and plan["vector"]["class"] == "read_only_observation"
    assert plan["schedule"]["process_starts_total"] == 6 and plan["schedule"]["max_active_processes"] == 1 and plan["schedule"]["phases"] == ["pre_run", "post_run"]
    assert plan["schedule"]["worst_case_total_s"] == 6 * 20 + 1 and plan["audit_findings"] == []
    assert plan["output"]["created_exclusively"] is True and plan["parser"]["evaluator"].startswith("A0e clock_preflight")
    assert plan["operation_category"] == wt.OPC_CLOCK and plan["architecture"] == wt.ARCHITECTURE and plan["architecture_status"] == "ARCHITECTURE_APPROVED_RUNTIME_NOT_VERIFIED"
    assert plan["activation_record_template"]["operation_category"] == wt.OPC_CLOCK and all(plan["activation_record_template"][k] is None for k in wt.OWNER_FIELDS)
    assert set(plan["owner_decisions_unset"]) == set(ctp.OWNER_ONLY) and "approved_host_identity" in plan["owner_decisions_unset"] and plan["host"]["approved_host_identity"] is None
    assert plan["authorization"]["reference"] is None and plan["executable"]["approved_path"] is None


def test_31_the_plan_digest_is_deterministic_and_ignores_only_the_creation_time(tmp_path):
    a = ctp.build_clock_transport_plan(v.clock_request(tmp_path), created_utc="2026-11-22T09:00:00Z")
    b = ctp.build_clock_transport_plan(v.clock_request(tmp_path), created_utc="2030-01-01T00:00:00Z")
    c = ctp.build_clock_transport_plan(v.clock_request(tmp_path, samples_per_phase=4), created_utc="2026-11-22T09:00:00Z")
    assert a["plan_sha256"] == b["plan_sha256"] != c["plan_sha256"]
    assert json.loads(ctp.plan_json(a))["plan_sha256"] == a["plan_sha256"]


def test_31_the_blank_template_yields_an_unresolved_plan_without_a_vector_or_an_invented_value():
    plan = ctp.build_clock_transport_plan(co.blank_request_template(), created_utc=v.CREATED)
    assert plan["status"] == "UNRESOLVED_NOT_APPROVED_NOT_EXECUTED" and "os_argv" not in plan["vector"] and plan["activation_record_template"] is None
    assert set(plan["unresolved_request_fields"]) == {k for k in co.REQUEST_KEYS if k != "schema"}
    assert plan["vector"]["template"][2] is None and plan["schedule"]["process_starts_total"] is None and "worst_case_total_s" not in plan["schedule"]
    assert set(plan["unmeasured_not_proposed"]) == set(ctp.UNMEASURED)


# ---- 32. clock-pair identity
@pytest.mark.parametrize("key", ["reference_domain", "remote_domain"])
def test_32_a_missing_clock_pair_identity_leaves_the_plan_unresolved(tmp_path, key):
    plan = plan_for(tmp_path, **{key: None})
    assert plan["status"].startswith("UNRESOLVED") and key in plan["unresolved_request_fields"] and "os_argv" not in plan["vector"]
    assert co.validate_request(v.clock_request(tmp_path, **{key: ""}))                                  # the A0t validator also refuses it


# ---- 33. sample count
@pytest.mark.parametrize("bad", [None, 0, -1, True, 2.5, "3"])
def test_33_a_missing_or_invalid_sample_count_never_resolves(tmp_path, bad):
    plan = plan_for(tmp_path, samples_per_phase=bad)
    assert plan["status"].startswith("UNRESOLVED") and "os_argv" not in plan["vector"] and plan["activation_record_template"] is None


def test_33_the_minimum_valid_samples_cannot_exceed_the_sample_count(tmp_path):
    plan = plan_for(tmp_path, min_valid_samples=9)
    assert plan["status"].startswith("UNRESOLVED") and plan["request_issues"]


# ---- 34. sampling gap
@pytest.mark.parametrize("bad", [0, -1, "1", True, math.inf, math.nan])
def test_34_an_invalid_sampling_gap_never_resolves(tmp_path, bad):
    plan = plan_for(tmp_path, phase_gap_s=bad)
    assert plan["status"].startswith("UNRESOLVED") and "os_argv" not in plan["vector"]


def test_34_a_missing_gap_or_probe_timeout_or_lifetime_stays_unresolved(tmp_path):
    for key in ("phase_gap_s", "probe_timeout_s", "max_process_lifetime_s"):
        plan = plan_for(tmp_path, **{key: None})
        assert plan["status"].startswith("UNRESOLVED") and key in plan["unresolved_request_fields"]


# ---- 36. no transaction-producing command
def test_36_the_only_vector_is_the_clock_probe_and_nothing_can_produce_a_transaction(tmp_path):
    clock_plan = v.clock_plan(tmp_path)
    allowed = v.clock_allowed(clock_plan)
    assert len(allowed) == 1 and allowed[0].os_argv == PROBE and allowed[0].klass == "read_only_observation"
    assert cor.audit_clock_only_plan(clock_plan) == [] and clock_plan["transactions_planned"] == 0 and clock_plan["channel_created"] is False and clock_plan["configuration_changed"] is False
    flat = [tok.lower() for a in allowed for tok in a.os_argv]
    assert not any(word in flat for word in ("docker", "caliper", "peer", "configtxlator", "orderer", "invoke", "launch", "channel", "python3", "bash", "sh"))
    transport = v.fixture_transport(wt.OPC_CLOCK, allowed, lambda inv: "pass", lifetime=60)
    for argv in (("wsl.exe", "-d", "Ubuntu", "--exec", "docker", "run", "x"), ("wsl.exe", "-d", "Ubuntu", "--exec", "peer", "chaincode", "invoke"),
                 ("wsl.exe", "-d", "Ubuntu", "--exec", "date", "+%s")):
        assert transport.problems(v.invocation(allowed[0], os_argv=argv))[0]["code"] == "vector_not_permitted"


def test_36_a_clock_plan_with_a_transaction_or_a_second_program_fails_the_audit(tmp_path):
    clock_plan = v.clock_plan(tmp_path)
    forged = json.loads(json.dumps(clock_plan))
    forged["steps"][0]["argv"] = ["wsl.exe", "-d", "Ubuntu", "--exec", "docker", "exec", "caliper", "launch"]
    forged["transactions_planned"] = 1
    assert {"unexpected_vector", "not_clock_only"} <= {f["code"] for f in cor.audit_clock_only_plan(forged)}
    with pytest.raises(wt.ActivationRefused):
        wt.vectors_for_category(wt.OPC_CLOCK, {**clock_plan, "schema": "other"}, distro=v.DISTRO, mount_root=v.MOUNT)


# ---- 37. no implicit tolerance
@pytest.mark.parametrize("bad", [None, 0, -5, True, "5", math.inf])
def test_37_the_tolerance_has_no_default_and_an_invalid_value_never_resolves(tmp_path, bad):
    plan = plan_for(tmp_path, tolerance_ms=bad)
    assert plan["status"].startswith("UNRESOLVED") and "os_argv" not in plan["vector"]


def test_37_the_plan_never_contains_a_tolerance_the_owner_did_not_supply():
    blank = ctp.build_clock_transport_plan(co.blank_request_template(), created_utc=v.CREATED)
    text = ctp.plan_json(blank)
    assert "tolerance_ms" in blank["unmeasured_not_proposed"] and "tolerance_ms" in blank["unresolved_request_fields"]
    assert blank["unmeasured_not_proposed"].count("tolerance_ms") == 1 and '"tolerance_ms": 0' not in text and "approved_tolerance_ms" in blank["owner_decisions_unset"]


# ---- 35. A0e compatibility, end to end through the transport (fixture mode)
def run_round(tmp: Path, **over):
    plan = v.clock_plan(tmp, **over)
    allowed = v.clock_allowed(plan)
    transport = v.fixture_transport(wt.OPC_CLOCK, allowed, lambda inv: "import sys, time; sys.stdout.write(str(time.time_ns()) + '\\n')", lifetime=60)
    status = co.execute_clock_only(plan, clock_auth(plan, tmp, synthetic=True), transport, wall_ns=time.time_ns, mono_ns=time.monotonic_ns, sleep=lambda s: None, now_utc=v.now_utc,
                                   allow_synthetic=True, hash_tree=wi.host_hash_tree)
    return plan, transport, status, Path(plan["request"]["output_dir"])


@pytest.fixture(scope="module")
def round_ok(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("a0v_clock_round")
    return run_round(tmp)


def test_35_the_round_driven_through_the_transport_is_accepted_by_the_unchanged_a0e_evaluator(round_ok):
    plan, transport, status, out = round_ok
    assert status["status"] == "COMPLETED" and status["runner_kind"] == "fixture" and len(transport.evidence) == 6
    assert all(row["returncode"] == 0 and row["timed_out"] is False and row["retried"] is False for row in transport.evidence)
    assessment = cor.assess_round_directory(out, plan)
    assert assessment["phases_present"] == ["pre_run", "post_run"] and assessment["raw_samples_preserved"] is True and assessment["sample_order_valid"] is True
    assert assessment["verdicts_equal"] is True and assessment["evaluator"].startswith("clock_preflight.assess_phases") and assessment["tolerance_source"].startswith("the operator value")
    records = [json.loads((out / "clock" / f"{p}.json").read_text(encoding="utf-8")) for p in cor.PHASES]
    assert all(r["schema"] == wp.PHASE_SCHEMA and r["count_collected"] == 3 for r in records)
    evaluator_phases = wp.to_evaluator_phases(records)
    assert set(evaluator_phases) == {"pre_run", "post_run"}


def test_35_a_simulated_round_is_never_a_verified_measurement(round_ok, tmp_path):
    plan, transport, status, out = round_ok
    outcome = ro.classify_clock_round(out, plan)
    assert outcome["success"] is False and outcome["outcome"] == ro.O_SIMULATED and outcome["a0t_status"] == "COMPLETED" and "runner_not_real" in {r["code"] for r in outcome["reasons"]}


def _promote_clock(out: Path, plan, monkeypatch, tmp: Path, *, rows: int | None = None) -> None:
    status = json.loads((out / "round_status.json").read_text(encoding="utf-8"))
    status["runner_kind"] = "process"
    (out / "round_status.json").write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    monkeypatch.setattr(wt, "REAL_SPAWN_COMPILED_IN", True)
    allowed = v.clock_allowed(plan)
    record = v.real_record(wt.OPC_CLOCK, allowed, v.clock_binding(plan), tmp / "approval")
    permit = wt.issue_real_permit(record, category=wt.OPC_CLOCK, allowed=allowed, binding=v.clock_binding(plan), expected_candidate_manifest_sha256=v.MANIFEST_SHA)
    transport = wt.WslProcessTransport(permit, popen=v.FakePopen(reply=lambda argv: v.FakeProc(b"1\n", b"", 0)))
    for _ in range(rows if rows is not None else 6):
        transport.run(v.invocation(allowed[0], timeout_s=5))
    transport.write_evidence(out / ro.TRANSPORT_DIR / wt.OPC_CLOCK)


def test_35_the_synthetic_promotion_is_a_verified_measurement_and_the_clock_label_is_reported_separately(round_ok, tmp_path, monkeypatch):
    import shutil
    plan, transport, status, out = round_ok
    copy = tmp_path / "copy"
    shutil.copytree(out, copy)
    _promote_clock(copy, plan, monkeypatch, tmp_path)
    outcome = ro.classify_clock_round(copy, plan)
    assert outcome["success"] is True and outcome["outcome"] == ro.O_VERIFIED and outcome["reasons"] == []
    assert outcome["clock_label"] in ("PASS", "FAIL", "INCONCLUSIVE")                     # a measurement result, independent of whether the run succeeded


@pytest.mark.parametrize("name", ["empty_manifest", "manifest_missing_file", "status_error", "wrong_row_count", "failed_probe_row"])
def test_35_each_single_failure_of_a_promoted_round_breaks_success(round_ok, tmp_path, monkeypatch, name):
    import shutil
    plan, transport, status, out = round_ok
    copy = tmp_path / "copy"
    shutil.copytree(out, copy)
    _promote_clock(copy, plan, monkeypatch, tmp_path, rows=5 if name == "wrong_row_count" else None)
    manifest_path = copy / "integrity_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    status_path = copy / "round_status.json"
    doc = json.loads(status_path.read_text(encoding="utf-8"))
    if name == "empty_manifest":
        manifest["files"] = {}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    elif name == "manifest_missing_file":
        manifest["files"].pop("clock/post_run.json")
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    elif name == "status_error":
        doc["errors"] = [{"error": "integrity", "message": "hash failed"}]
        status_path.write_text(json.dumps(doc), encoding="utf-8")
    elif name == "failed_probe_row":
        evidence_path = copy / ro.TRANSPORT_DIR / wt.OPC_CLOCK / "evidence.json"
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        evidence["evidence"][0]["returncode"] = 1
        evidence["document_sha256"] = wt.be._digest(evidence, drop=("document_sha256",))
        evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    outcome = ro.classify_clock_round(copy, plan)
    assert outcome["success"] is False and outcome["outcome"] != ro.O_VERIFIED, name


def test_35_an_integrity_failure_in_the_clock_round_leaves_the_a0t_status_completed_and_the_outcome_failed(tmp_path):
    def broken_hash(root):
        raise RuntimeError("disk error while hashing")

    plan = v.clock_plan(tmp_path)
    allowed = v.clock_allowed(plan)
    transport = v.fixture_transport(wt.OPC_CLOCK, allowed, lambda inv: "import sys, time; sys.stdout.write(str(time.time_ns()) + '\\n')", lifetime=60)
    status = co.execute_clock_only(plan, clock_auth(plan, tmp_path, synthetic=True), transport, wall_ns=time.time_ns, mono_ns=time.monotonic_ns, sleep=lambda s: None, now_utc=v.now_utc,
                                   allow_synthetic=True, hash_tree=broken_hash)
    assert status["status"] == "COMPLETED" and any(e["error"] == "integrity" for e in status["errors"])          # the defect class: the executor still says COMPLETED
    outcome = ro.classify_clock_round(Path(plan["request"]["output_dir"]), plan)
    assert outcome["success"] is False and outcome["outcome"] == ro.O_INTEGRITY_FAILED and "integrity_step_error" in {r["code"] for r in outcome["reasons"]}
    assert (Path(plan["request"]["output_dir"]) / "clock" / "pre_run.json").is_file()                    # partial evidence preserved


def test_35_a_round_directory_without_a_status_is_not_a_round(tmp_path):
    plan = v.clock_plan(tmp_path)
    (tmp_path / "empty").mkdir()
    assert ro.classify_clock_round(tmp_path / "empty", plan)["outcome"] == ro.O_NOT_RUN
