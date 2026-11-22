"""Inspectable clock-only transport plan (task A0v, 2026-11-22). OFFLINE; text only; nothing is sampled, started or contacted.

It joins the three things a first clock-only measurement needs, so the owner can read them in one place before any approval:

1. the exact process vector the transport would start (``wsl.exe -d <distro> --exec date +%s%N``), its working directory (none), host / distribution identity, schedule, bounds and output destination;
2. the operation-specific activation record template (category ``wsl_clock_sampling``) and the clock-only authorization reference, both UNSET;
3. the parser and evaluator the output feeds (``wsl_clock_probe`` phase documents -> ``to_evaluator_phases`` -> the unchanged A0e ``clock_preflight.assess_phases``).

Anything the owner has not decided, or that nobody has measured, stays ``None`` with the reason; a request with a missing value yields an UNRESOLVED plan without a vector, never an invented one.
Unmeasured and never proposed: the Windows-WSL offset, the round trip, the resolution uncertainty, the tolerance, the sample count, the phase gap, the process lifetime.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from . import bounded_execution as be
from . import clock_only_readiness as cor
from . import clock_only_round as co
from . import wsl_clock_probe as wp
from . import wsl_transport as wt

PLAN_VERSION = "clock_transport_plan 0.1 (A0v, 2026-11-22)"
PLAN_SCHEMA = "a0v_clock_transport_plan/1"
UNMEASURED = ("windows_wsl_clock_offset_ms", "round_trip_ms", "remote_resolution_uncertainty_ns", "tolerance_ms", "wall_monotonic_disagreement_ms")
OWNER_ONLY = ("approved_host_identity", "approved_wsl_distribution", "approved_executable_path", "approved_clock_pair", "approved_sample_count_per_phase", "approved_phase_gap_s",
              "approved_max_process_lifetime_s", "approved_output_dir", "approved_tolerance_ms", "owner_authorization_reference", "activation_record_approval")
SESSION_CONTEXT = {"host_os": "Windows 11 Home Single Language 10.0.26200", "source": "the coding-session environment description; NOT verified by any tool and NOT an approved host identity"}


def build_clock_transport_plan(request: Mapping[str, Any], *, created_utc: str) -> dict[str, Any]:
    """The plan for a (possibly incomplete) clock-only request. Complete and valid -> RESOLVED with the real vectors; otherwise UNRESOLVED and the missing fields are listed."""
    keys = [k for k in co.REQUEST_KEYS if k != "schema"]
    missing = [k for k in keys if request.get(k) in (None, "")]
    issues = [] if missing else co.validate_request(dict(request))
    resolved = not missing and not issues
    document: dict[str, Any] = {
        "schema": PLAN_SCHEMA, "version": PLAN_VERSION, "created_utc": created_utc, "status": "RESOLVED_NOT_APPROVED_NOT_EXECUTED" if resolved else "UNRESOLVED_NOT_APPROVED_NOT_EXECUTED",
        "operation_category": wt.OPC_CLOCK, "architecture": wt.ARCHITECTURE, "architecture_status": "ARCHITECTURE_APPROVED_RUNTIME_NOT_VERIFIED",
        "executed": False, "real_spawn_compiled_in": wt.REAL_SPAWN_COMPILED_IN, "transactions_planned": 0,
        "prohibited": list(co.PROHIBITED), "host": {"declared_by_session": SESSION_CONTEXT, "approved_host_identity": None},
        "distribution": {"name": request.get("distro"), "approved": None}, "executable": {"approved_path": None, "rule": "an explicit absolute Windows path to wsl.exe; no search path, no default"},
        "vector": {"template": ["wsl.exe", "-d", request.get("distro"), "--exec", "date", "+%s%N"], "working_directory": None, "shell": False, "launch_mode": "foreground", "class": "read_only_observation"},
        "schedule": {"phases": list(cor.PHASES), "samples_per_phase": request.get("samples_per_phase"), "phase_gap_s": request.get("phase_gap_s"), "process_starts_total": None,
                     "sequential": True, "max_active_processes": 1, "probe_timeout_s": request.get("probe_timeout_s"), "max_process_lifetime_s": request.get("max_process_lifetime_s")},
        "clock_pair": {"reference_domain": request.get("reference_domain"), "remote_domain": request.get("remote_domain")},
        "output": {"output_dir": request.get("output_dir"), "files": ["clock/pre_run.json", "clock/post_run.json", "clock/clock_verdict.json", "integrity_manifest.json", "round_status.json", "run_outcome.json"],
                   "transport_evidence": "transport/evidence.json plus one stdout and stderr file per process", "created_exclusively": True, "overwrites_nothing": True},
        "parser": {"phase_schema": wp.PHASE_SCHEMA, "sample_keys": list(wp.SAMPLE_KEYS), "remote_digits": wp.REMOTE_DIGITS_NS, "converter": "wsl_clock_probe.to_evaluator_phases",
                   "evaluator": "A0e clock_preflight.assess_phases (unchanged), via wsl_clock_probe.evaluate", "stored_raw_samples": True, "reevaluation": "clock_only_readiness.assess_round_directory"},
        "stop_conditions": list(cor.STOP_CONDITIONS) + ["transport refusal (any code)", "second invocation of a vector beyond its approved count", "an active process still running at the phase gap"],
        "authorization": {"clock_only_authorization_schema": co.AUTH_SCHEMA, "activation_record_schema": wt.ACTIVATION_SCHEMA, "operation_category": wt.OPC_CLOCK, "reference": None,
                          "note": "separate from discovery, X13, Caliper, channel creation and everything else; a clock approval starts nothing but the probe vector"},
        "owner_decisions_unset": list(OWNER_ONLY), "unmeasured_not_proposed": list(UNMEASURED),
        "unresolved_request_fields": missing, "request_issues": [dict(i) for i in issues],
        "claims_not_made": ["wsl.exe --exec date +%s%N works on the target machine (never run)", "any offset, round trip, resolution or tolerance", "that the distribution exists", "that termination reaches processes inside the distribution"],
    }
    if resolved:
        clock_plan = co.build_clock_only_plan({k: v for k, v in request.items() if k != "_status"}, created_utc=created_utc)
        allowed = wt.vectors_for_category(wt.OPC_CLOCK, clock_plan, distro=request["distro"], mount_root=request["automount_root"])
        total = int(request["samples_per_phase"]) * 2
        document["vector"].update(plan_argv=list(clock_plan["probe"]["argv"]), os_argv=list(allowed[0].os_argv), max_uses=allowed[0].max_uses, timeout_s=allowed[0].timeout_s)
        document["schedule"]["process_starts_total"] = total
        document["schedule"]["worst_case_total_s"] = total * float(request["probe_timeout_s"]) + float(request["phase_gap_s"])
        document["audit_findings"] = cor.audit_clock_only_plan(clock_plan)
        document["clock_only_plan_sha256"] = clock_plan["plan_sha256"]
        binding = {"round_id": request["round_id"], "plan_sha256": clock_plan["plan_sha256"], "operation": wt.OPC_CLOCK}
        document["activation_record_template"] = wt.activation_template(wt.OPC_CLOCK, allowed, binding)
        document["activation_description"] = wt.describe_activation(wt.OPC_CLOCK, allowed)
    else:
        document["audit_findings"] = [{"code": "unresolved", "message": f"request values missing: {missing}"}] if missing else [dict(i) for i in issues]
        document["activation_record_template"] = None
    document["plan_sha256"] = be._digest(document, drop=("plan_sha256", "created_utc"))
    return document


def plan_json(document: Mapping[str, Any]) -> str:
    return json.dumps(dict(document), indent=2, sort_keys=True) + "\n"
