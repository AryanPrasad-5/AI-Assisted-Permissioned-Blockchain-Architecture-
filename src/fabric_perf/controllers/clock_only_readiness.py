"""Clock-only round: transaction-freedom audit, A0e compatibility check and the (unapproved) A0u approval packet (task A0u, 2026-11-22). OFFLINE; starts nothing.

``clock_only_round`` (A0t) already plans a round whose only vector is ``wsl.exe -d <distro> --exec date +%s%N``. This module does not change it. It adds three things the first real
measurement needs and A0t did not state as checks:

* ``audit_clock_only_plan``: a structural proof, from the plan itself, that the round submits no transaction, creates or joins no channel, changes no configuration, loads no model and names no
  Docker/Caliper/Fabric/Prometheus vector; that its only vector is exactly the probe; and that nothing in it is defaulted (every operator value is present).
* ``assess_round_directory``: reads the stored ``clock/pre_run.json`` and ``clock/post_run.json`` of a finished (fixture) round and hands the RAW samples to the unchanged A0e evaluator, checking
  that the samples are preserved, ordered, and accepted. No tolerance is chosen here: the evaluator receives the plan's operator value.
* ``approval_packet_template`` / ``verify_approval_packet``: the clock-only approval record with EVERY owner field unset. Plan values are shown for review in a separate block; an approved field
  is accepted only when the owner set it AND it equals the plan's value. Nothing here is a measurement, a proposal for a tolerance, or an approval.

No offset, round trip, tolerance, sample count, clock-pair identity, warm-up or gap value is supplied by this module; the round trip of ``wsl.exe`` + ``date`` has never been measured.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from . import bounded_execution as be
from . import clock_only_round as co
from . import clock_preflight as cp
from . import process_runner as pr
from . import wsl_clock_probe as wp

READINESS_VERSION = "clock_only_readiness 0.1 (A0u, 2026-11-22)"
PACKET_SCHEMA = "a0u_clock_only_approval_packet/1"
FORBIDDEN_TOKENS = ("docker", "caliper", "peer", "configtxlator", "orderer", "python3", "python", "node", "cp", "mkdir", "curl", "sh", "bash", "kubectl", "npm")
PHASES = ("pre_run", "post_run")
STOP_CONDITIONS = ("unexpected_command_vector", "probe_timeout_exceeds_bound", "process_lifetime_exceeded", "evidence_write_failure", "interrupt", "output_directory_exists", "authorization_mismatch")
OPERATOR_KEYS = ("distro", "samples_per_phase", "min_valid_samples", "tolerance_ms", "max_wall_monotonic_disagreement_ms", "remote_resolution_ns", "probe_timeout_s", "phase_gap_s",
                 "reference_domain", "remote_domain", "max_process_lifetime_s", "output_dir", "automount_root")


def audit_clock_only_plan(plan: Mapping[str, Any]) -> list[dict[str, str]]:
    """Problems that would make the round more than a clock measurement. An empty list means the plan is a transaction-free clock-only round."""
    problems: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        problems.append({"code": code, "message": message})

    for key, want in (("transactions_planned", 0), ("channel_created", False), ("configuration_changed", False), ("model_inference", False), ("real_launch_enabled", False)):
        if plan.get(key) != want or isinstance(plan.get(key), bool) != isinstance(want, bool):
            bad("not_clock_only", f"{key} is {plan.get(key)!r}, must be {want!r}")
    if plan.get("operation_category") != pr.OP_CLOCK_ONLY:
        bad("operation_category", f"{plan.get('operation_category')!r}")
    if sorted(plan.get("permitted_operations") or []) != sorted(co.PERMITTED_OPERATIONS) or sorted(plan.get("prohibited_operations") or []) != sorted(co.PROHIBITED):
        bad("operation_lists", "the permitted or prohibited operation lists differ from clock_only_round's")
    req = plan.get("request") or {}
    expected_probe = ["wsl.exe", "-d", req.get("distro"), "--exec", "date", "+%s%N"]
    vectors = [s["argv"] for s in plan.get("steps", []) if s.get("argv")]
    if any(v != expected_probe for v in vectors) or (plan.get("probe") or {}).get("argv") != expected_probe:
        bad("unexpected_vector", f"the only permitted vector is {expected_probe}; found {vectors}")
    for argv in vectors:
        tail = [t.lower() for t in argv[3:]]
        if any(t in FORBIDDEN_TOKENS or t.endswith(("docker", "caliper")) for t in tail):
            bad("forbidden_token", f"a non-clock program appears in {argv}")
    kinds = [s["kind"] for s in plan.get("steps", [])]
    if any(k not in ("gate_clock", "wait", "evaluate", "gate_integrity") for k in kinds):
        bad("unexpected_step_kind", f"step kinds {kinds}")
    if any(s["class"] not in ("read_only_observation", "offline_internal") for s in plan.get("steps", [])):
        bad("step_class", "a clock-only round may only hold read-only observation and offline steps")
    missing = [k for k in co.REQUEST_KEYS if req.get(k) is None]
    if missing:
        bad("operator_value_missing", f"no value for {missing}; nothing is defaulted")
    return problems


def assess_round_directory(round_dir: Path, plan: Mapping[str, Any]) -> dict[str, Any]:
    """Re-evaluate the stored raw samples of a finished round with the unchanged A0e evaluator and report what was preserved. The tolerance is the plan's operator value, never a default."""
    base = Path(round_dir)
    records = [json.loads((base / "clock" / f"{phase}.json").read_text(encoding="utf-8")) for phase in PHASES]
    req = plan["request"]
    structural = {r["phase"]: wp.check_phase(r) for r in records}
    verdict = wp.evaluate(records, tolerance_ms=req["tolerance_ms"], min_valid_samples=req["min_valid_samples"], max_wall_monotonic_disagreement_ms=req["max_wall_monotonic_disagreement_ms"])
    stored = json.loads((base / "clock" / "clock_verdict.json").read_text(encoding="utf-8"))
    return {"phases_present": [r["phase"] for r in records], "raw_samples_preserved": all(isinstance(r.get("samples"), list) and r["count_collected"] == len(r["samples"]) for r in records),
            "sample_order_valid": all(not v for v in structural.values()), "structural_problems": structural, "reevaluated_verdict": verdict["verdict"], "stored_verdict": stored["verdict"],
            "verdicts_equal": verdict["verdict"] == stored["verdict"], "evaluator": f"clock_preflight.assess_phases ({cp.CLOCK_PREFLIGHT_VERSION})",
            "tolerance_ms_used": req["tolerance_ms"], "tolerance_source": "the operator value in the plan; no default exists", "claims": "evidence about these samples only"}


def approval_packet_template(plan: Mapping[str, Any]) -> dict[str, Any]:
    """The clock-only approval packet. ``plan_values_for_review`` is a copy of the plan (copying is not approving); every ``owner`` field is None."""
    req = plan["request"]
    return {"schema": PACKET_SCHEMA, "version": READINESS_VERSION, "_status": "TEMPLATE. NOT APPROVED. Every null under 'owner' is an owner decision; nothing here authorizes a measurement.",
            "synthetic": False, "plan_sha256": plan["plan_sha256"], "operation_category": pr.OP_CLOCK_ONLY, "transactions_authorized": 0,
            "plan_values_for_review": {"probe_argv": list(plan["probe"]["argv"]), "sampling_phases": list(PHASES), "stop_conditions": list(STOP_CONDITIONS), **{k: req[k] for k in OPERATOR_KEYS}},
            "prohibited_operations": list(co.PROHIBITED), "permitted_operations": list(co.PERMITTED_OPERATIONS),
            "owner": {"approved_host_identity": None, "approved_wsl_distribution": None, "approved_clock_pair": {"reference_domain": None, "remote_domain": None},
                      "approved_sampling_commands": None, "approved_sample_count_per_phase": None, "approved_min_valid_samples": None, "approved_sampling_phases": None,
                      "approved_phase_gap_s": None, "approved_max_process_duration_s": None, "approved_output_paths": {"output_dir": None}, "approved_tolerance_ms": None,
                      "approved_wall_monotonic_bound_ms": None, "approved_remote_resolution_ns": None, "approved_probe_timeout_s": None, "approved_automount_root": None,
                      "stop_conditions": None, "owner_authorization_reference": None, "process_transport": None, "execution_environment": None, "real_transport_authorized": None},
            "approval": {k: None for k in be.APPROVAL_KEYS}}


def verify_approval_packet(packet: Any, plan: Mapping[str, Any], *, allow_synthetic: bool = False) -> dict[str, Any]:
    findings: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        findings.append({"code": code, "message": message})

    if not isinstance(packet, Mapping) or packet.get("schema") != PACKET_SCHEMA:
        return {"valid": False, "findings": [{"code": "packet_schema", "message": "not an a0u_clock_only_approval_packet/1"}], "real_transport_permitted": False}
    if audit := audit_clock_only_plan(plan):
        bad("plan_not_clock_only", str(audit))
    owner, req = packet.get("owner") or {}, plan["request"]
    flat = {"approved_host_identity": None, "approved_wsl_distribution": req["distro"], "approved_sample_count_per_phase": req["samples_per_phase"], "approved_min_valid_samples": req["min_valid_samples"],
            "approved_phase_gap_s": req["phase_gap_s"], "approved_max_process_duration_s": req["max_process_lifetime_s"], "approved_tolerance_ms": req["tolerance_ms"],
            "approved_wall_monotonic_bound_ms": req["max_wall_monotonic_disagreement_ms"], "approved_remote_resolution_ns": req["remote_resolution_ns"],
            "approved_probe_timeout_s": req["probe_timeout_s"], "approved_automount_root": req["automount_root"], "approved_sampling_phases": list(PHASES),
            "approved_sampling_commands": [list(plan["probe"]["argv"])], "stop_conditions": list(STOP_CONDITIONS)}
    for key, want in flat.items():
        value = owner.get(key)
        if value in (None, ""):
            bad("owner_field_unset", key)
        elif want is not None and value != want:
            bad("owner_value_differs_from_plan", f"{key}: approved {value!r}, planned {want!r}")
    pair = owner.get("approved_clock_pair") or {}
    if pair.get("reference_domain") in (None, "") or pair.get("remote_domain") in (None, ""):
        bad("owner_field_unset", "approved_clock_pair")
    elif (pair["reference_domain"], pair["remote_domain"]) != (req["reference_domain"], req["remote_domain"]):
        bad("clock_pair_mismatch", f"approved {pair!r}, planned {(req['reference_domain'], req['remote_domain'])!r}")
    if (owner.get("approved_output_paths") or {}).get("output_dir") in (None, ""):
        bad("owner_field_unset", "approved_output_paths.output_dir")
    elif owner["approved_output_paths"]["output_dir"] != req["output_dir"]:
        bad("output_path_mismatch", "the approved output directory differs from the plan's")
    for key in ("owner_authorization_reference", "process_transport", "execution_environment"):
        if owner.get(key) in (None, ""):
            bad("owner_field_unset", key)
    if owner.get("process_transport") not in (None, pr.TRANSPORT_WSL, pr.TRANSPORT_FIXTURE):
        bad("process_transport", f"{owner.get('process_transport')!r}")
    if owner.get("process_transport") == pr.TRANSPORT_WSL and packet.get("synthetic") is True:
        bad("synthetic_for_real_transport", "a synthetic packet may never name the real transport")
    if owner.get("process_transport") == pr.TRANSPORT_FIXTURE and not (allow_synthetic and packet.get("synthetic") is True):
        bad("fixture_transport_not_operational", "the fixture transport is accepted only with a synthetic packet in a test")
    if owner.get("execution_environment") not in (None, pr.ENV_WINDOWS_ORCHESTRATOR):
        bad("execution_environment", f"{owner.get('execution_environment')!r}")
    if not isinstance(owner.get("real_transport_authorized"), bool):
        bad("real_transport_authorized", "must be an explicit true or false")
    if packet.get("plan_sha256") != plan["plan_sha256"]:
        bad("plan_digest_mismatch", "the packet binds another plan")
    if packet.get("transactions_authorized") != 0:
        bad("transactions_authorized", "a clock-only packet authorizes exactly zero transactions")
    approval = packet.get("approval")
    if not isinstance(approval, Mapping) or any(approval.get(k) in (None, "") for k in be.APPROVAL_KEYS):
        bad("approval_unset", f"every approval field {list(be.APPROVAL_KEYS)} must be set by the owner")
    elif approval["status"] != "APPROVED_BY_OWNER":
        bad("approval_status", f"approval.status is {approval['status']!r}")
    if packet.get("synthetic") is True and not allow_synthetic:
        bad("synthetic_packet", "a synthetic packet is accepted only in a test and is never a real approval")
    return {"valid": not findings, "findings": findings, "real_transport_permitted": False if not pr.REAL_TRANSPORT_ENABLED else not findings,
            "note": "a valid packet is still not a measurement: the real transport and the real probe are disabled in this version"}
