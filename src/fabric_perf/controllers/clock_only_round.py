"""First clock-only measurement round: plan, authorization record and fixture execution, task A0t (2026-11-22). OFFLINE; the real probe stays disabled.

A clock-only round measures the Windows <-> WSL clock offset interval and nothing else. It produces NO transaction, creates NO channel, changes NO configuration, runs NO model, touches NO
Fabric component and starts NO Docker or Caliper process. Its only process vector is the probe ``wsl.exe -d <distro> --exec date +%s%N`` (from ``wsl_clock_probe.probe_plan``).

Phases. The A0e evaluator (``clock_preflight.assess_phases``) wants one phase BEFORE and one AFTER the live round so a drifting or stepping clock shows up. A standalone round therefore takes a
``pre_run`` phase, waits an owner-supplied ``phase_gap_s`` (an INJECTED sleep; this module has none), and takes a ``post_run`` phase. Every number (tolerance, sample counts, bounds, gap,
resolution, timeouts) is supplied by the operator; there is no default and no proposal, because the round trip of ``wsl.exe`` + ``date`` has never been measured.

Output (new directory, files created exclusively): ``clock/pre_run.json`` and ``clock/post_run.json`` (``wsl_clock_phase/1`` with the RAW samples), ``clock/clock_verdict.json`` (the A0e
evaluator's verdict, which contains the derived offset interval per phase), ``process_logs/``, ``round_status.json``, ``integrity_manifest.json``. A verdict is PASS (AGREE),
FAIL (DISAGREE) or INCONCLUSIVE; it is evidence about those samples only.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import bounded_execution as be
from . import config_inspection as ci
from . import path_mapping as pm
from . import prelive_integrity as pi
from . import process_runner as pr
from . import wsl_clock_probe as wp

CLOCK_ONLY_VERSION = "clock_only_round 0.1 (A0t, 2026-11-22)"
REQUEST_SCHEMA = "a0t_clock_only_request/1"
PLAN_SCHEMA = "a0t_clock_only_plan/1"
AUTH_SCHEMA = "a0t_clock_only_authorization/1"
STATUS_SCHEMA = "a0t_clock_only_status/1"
REQUEST_KEYS = ("schema", "round_id", "distro", "samples_per_phase", "min_valid_samples", "tolerance_ms", "max_wall_monotonic_disagreement_ms", "remote_resolution_ns", "probe_timeout_s",
                "phase_gap_s", "reference_domain", "remote_domain", "max_process_lifetime_s", "output_dir", "automount_root")
POSITIVE_NUMBERS = ("tolerance_ms", "max_wall_monotonic_disagreement_ms", "probe_timeout_s", "phase_gap_s", "max_process_lifetime_s")
POSITIVE_INTS = ("samples_per_phase", "min_valid_samples", "remote_resolution_ns")
PERMITTED_OPERATIONS = ("clock_probe_wsl_date",)
PROHIBITED = ("transaction_submission", "channel_creation", "configuration_change", "model_inference", "docker", "caliper", "fabric_contact", "prometheus", "network_operation")
ROUND_ID = re.compile(r"[A-Za-z0-9_.-]{1,40}")
APPROVAL_KEYS = be.APPROVAL_KEYS
VERDICT_LABEL = {"AGREE": "PASS", "DISAGREE": "FAIL", "INCONCLUSIVE": "INCONCLUSIVE"}


def _number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and v == v and abs(v) != float("inf")


def validate_request(request: Any) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []

    def bad(code: str, field: str, message: str) -> None:
        issues.append({"code": code, "field": field, "message": message})

    if not isinstance(request, Mapping):
        return [{"code": "not_an_object", "field": "<request>", "message": "the request must be a JSON object"}]
    for key in REQUEST_KEYS:
        if request.get(key) is None:
            bad("missing", key, "no value was supplied; there is no default")
    extra = sorted(set(request) - set(REQUEST_KEYS))
    if extra:
        bad("unknown_key", "<request>", f"unknown key(s) {extra}")
    if issues:
        return issues
    if request["schema"] != REQUEST_SCHEMA:
        bad("schema", "schema", f"must be {REQUEST_SCHEMA!r}")
    if not (isinstance(request["round_id"], str) and ROUND_ID.fullmatch(request["round_id"])):
        bad("round_id", "round_id", f"must match {ROUND_ID.pattern}")
    for key in POSITIVE_NUMBERS:
        if not (_number(request[key]) and request[key] > 0):
            bad("value", key, "must be a finite number > 0 (operator-supplied)")
    for key in POSITIVE_INTS:
        if isinstance(request[key], bool) or not isinstance(request[key], int) or request[key] < 1:
            bad("value", key, "must be a positive integer")
    if not issues and request["min_valid_samples"] > request["samples_per_phase"]:
        bad("value", "samples_per_phase", "must be at least min_valid_samples")
    if not issues and request["max_process_lifetime_s"] < request["probe_timeout_s"]:
        bad("value", "max_process_lifetime_s", "must be at least probe_timeout_s")
    for key in ("reference_domain", "remote_domain"):
        if not (isinstance(request[key], str) and request[key]):
            bad("value", key, "a clock-domain label is required")
    if request["reference_domain"] == request["remote_domain"]:
        bad("value", "remote_domain", "the two clock-domain labels must differ")
    for key in ("output_dir",):
        if pm.classify(request[key]) != "windows_drive":
            bad("path", key, "must be an absolute Windows drive path")
    try:
        pm.check_mount_root(request["automount_root"])
    except pm.PathMappingError as exc:
        bad("path", "automount_root", str(exc))
    try:
        wp.probe_plan(request["distro"], remote_resolution_ns=request["remote_resolution_ns"], timeout_s=request["probe_timeout_s"])
    except wp.ProbeRefused as exc:
        bad("distro", "distro", str(exc))
    return issues


def build_clock_only_plan(request: Mapping[str, Any], *, created_utc: str) -> dict[str, Any]:
    issues = validate_request(request)
    if issues:
        raise be.RequestInvalid(issues)
    if Path(request["output_dir"]).exists():
        raise be.RequestInvalid([{"code": "output_occupied", "field": "output_dir", "message": "the output directory already exists; evidence is never overwritten"}])
    probe = wp.probe_plan(request["distro"], remote_resolution_ns=request["remote_resolution_ns"], timeout_s=request["probe_timeout_s"])
    steps = [{"step": "K01_probe_pre_run", "kind": "gate_clock", "class": "read_only_observation", "argv": probe["argv"], "cwd": None, "timeout_s": request["probe_timeout_s"],
              "writes": ["clock/pre_run.json"], "note": f"{request['samples_per_phase']} round trips"},
             {"step": "K02_phase_gap", "kind": "wait", "class": "offline_internal", "argv": None, "cwd": None, "timeout_s": request["phase_gap_s"], "writes": [], "note": "injected sleep; no process"},
             {"step": "K03_probe_post_run", "kind": "gate_clock", "class": "read_only_observation", "argv": probe["argv"], "cwd": None, "timeout_s": request["probe_timeout_s"],
              "writes": ["clock/post_run.json"], "note": "same parameters"},
             {"step": "K04_evaluate", "kind": "evaluate", "class": "offline_internal", "argv": None, "cwd": None, "timeout_s": None, "writes": ["clock/clock_verdict.json"],
              "note": "clock_preflight.assess_phases over the raw samples"},
             {"step": "K05_integrity", "kind": "gate_integrity", "class": "offline_internal", "argv": None, "cwd": None, "timeout_s": None, "writes": ["integrity_manifest.json"],
              "note": "hash every file in the output directory"}]
    plan = {"schema": PLAN_SCHEMA, "version": CLOCK_ONLY_VERSION, "created_utc": ci._aware_utc(created_utc, "created_utc"), "status": "PLANNED_NOT_AUTHORIZED_NOT_EXECUTED",
            "real_launch_enabled": False, "operation_category": pr.OP_CLOCK_ONLY, "request": json.loads(json.dumps(request)), "probe": probe, "steps": steps,
            "transactions_planned": 0, "channel_created": False, "configuration_changed": False, "model_inference": False,
            "permitted_operations": list(PERMITTED_OPERATIONS), "prohibited_operations": list(PROHIBITED),
            "verdict_criteria": {"PASS": "AGREE: the whole offset interval of BOTH phases lies within +-tolerance_ms", "FAIL": "DISAGREE: an interval lies beyond the tolerance even in the best case",
                                 "INCONCLUSIVE": "too few valid samples, an interval that straddles the tolerance, or samples that contradict each other"},
            "claims_not_made": ["the probe command works on the target machine", "any round trip or uncertainty value", "that any tolerance is achievable"]}
    plan["plan_sha256"] = be._digest(plan, drop=("plan_sha256", "created_utc"))
    return plan


def clock_only_vectors(plan: Mapping[str, Any]) -> list[pr.PlanVector]:
    req = plan["request"]
    return [pr.PlanVector("K01+K03:clock_probe", tuple(plan["probe"]["argv"]), None, float(req["probe_timeout_s"]), pr.LAUNCH_FOREGROUND, "read_only_observation",
                          2 * req["samples_per_phase"], cwd_may_be_none=True)]


def authorization_template(plan: Mapping[str, Any]) -> dict[str, Any]:
    """The clock-only authorization record with every owner field UNSET (null)."""
    req = plan["request"]
    return {"schema": AUTH_SCHEMA, "synthetic": False, "plan_sha256": plan["plan_sha256"], "operation_category": pr.OP_CLOCK_ONLY, "round_id": plan["request"]["round_id"],
            "permitted_operations": list(PERMITTED_OPERATIONS), "prohibited_operations": list(PROHIBITED), "transactions_authorized": 0,
            "clock": {k: req[k] for k in ("distro", "samples_per_phase", "min_valid_samples", "tolerance_ms", "max_wall_monotonic_disagreement_ms", "remote_resolution_ns", "probe_timeout_s",
                                           "phase_gap_s", "reference_domain", "remote_domain")},
            "output_dir": req["output_dir"], "automount_root": req["automount_root"], "max_process_lifetime_s": req["max_process_lifetime_s"],
            "process_transport": None, "execution_environment": None, "real_transport_authorized": None,
            "approval": {k: None for k in APPROVAL_KEYS},
            "note": "TEMPLATE. The clock values are copied from the request for review; copying is not approving. Every approval and transport field is unset."}


def blank_request_template() -> dict[str, Any]:
    """A clock-only request with EVERY value unset. There is no proposal for any number: the round trip of ``wsl.exe`` + ``date`` is unmeasured, so a tolerance or bound chosen now would be invented."""
    return {"_status": "TEMPLATE ONLY. NOT APPROVED. null = the operator must supply the value; there is no default and no proposal. Not valid input until every null is filled and _status is removed.",
            **{key: None for key in REQUEST_KEYS}, "schema": REQUEST_SCHEMA}


def blank_authorization_template() -> dict[str, Any]:
    """The authorization fields a future clock-only round needs, every value unset. A plan-bound template is produced by :func:`authorization_template`."""
    return {"_status": "TEMPLATE ONLY. NOT APPROVED. Every null is an owner decision; nothing here authorizes a measurement.", "schema": AUTH_SCHEMA, "synthetic": False, "plan_sha256": None,
            "operation_category": pr.OP_CLOCK_ONLY, "round_id": None, "permitted_operations": list(PERMITTED_OPERATIONS), "prohibited_operations": list(PROHIBITED), "transactions_authorized": 0,
            "clock": {k: None for k in ("distro", "samples_per_phase", "min_valid_samples", "tolerance_ms", "max_wall_monotonic_disagreement_ms", "remote_resolution_ns", "probe_timeout_s",
                                        "phase_gap_s", "reference_domain", "remote_domain")},
            "output_dir": None, "automount_root": None, "max_process_lifetime_s": None, "process_transport": None, "execution_environment": None, "real_transport_authorized": None,
            "approval": {k: None for k in APPROVAL_KEYS}}


def verify_authorization(auth: Any, plan: Mapping[str, Any], *, allow_synthetic: bool = False) -> dict[str, Any]:
    findings: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        findings.append({"code": code, "message": message})

    if not isinstance(auth, Mapping):
        return {"valid": False, "findings": [{"code": "authorization_missing", "message": "no record"}], "real_transport_permitted": False}
    if auth.get("schema") != AUTH_SCHEMA:
        bad("authorization_schema", f"schema {auth.get('schema')!r}")
    approval = auth.get("approval")
    if not isinstance(approval, Mapping) or any(approval.get(k) in (None, "") for k in APPROVAL_KEYS):
        bad("approval_unset", f"every approval field {list(APPROVAL_KEYS)} must be set by the owner")
    else:
        if approval["status"] != "APPROVED_BY_OWNER":
            bad("approval_status", f"approval.status is {approval['status']!r}")
        try:
            ci._aware_utc(approval["approved_utc"], "approval.approved_utc")
        except ci.InspectionError as exc:
            bad("approval_time", str(exc))
        record = Path(str(approval["record_path"]))
        if not (isinstance(approval["record_sha256"], str) and be.SHA.fullmatch(approval["record_sha256"])):
            bad("approval_record_hash", "record_sha256 must be 64 lowercase hex characters")
        elif not record.is_file():
            bad("approval_record_unreadable", f"{record} is not a readable file")
        elif pi.sha256_file(record) != approval["record_sha256"]:
            bad("approval_record_hash", "the approval document's hash differs from the one recorded")
    if auth.get("synthetic") is True and not allow_synthetic:
        bad("synthetic_authorization", "a synthetic authorization is accepted only for a fixture transport in a test")
    if auth.get("plan_sha256") != plan.get("plan_sha256"):
        bad("plan_digest_mismatch", "the authorization binds a different plan")
    if auth.get("operation_category") != pr.OP_CLOCK_ONLY:
        bad("operation_category_mismatch", f"{auth.get('operation_category')!r}")
    if auth.get("transactions_authorized") != 0:
        bad("transactions_authorized", "a clock-only round authorizes exactly zero transactions")
    if sorted(auth.get("permitted_operations") or []) != sorted(PERMITTED_OPERATIONS):
        bad("permitted_operations", f"must be exactly {list(PERMITTED_OPERATIONS)}")
    if sorted(auth.get("prohibited_operations") or []) != sorted(PROHIBITED):
        bad("prohibited_operations", f"must be exactly {list(PROHIBITED)}")
    req = plan["request"]
    for key in ("distro", "samples_per_phase", "min_valid_samples", "tolerance_ms", "max_wall_monotonic_disagreement_ms", "remote_resolution_ns", "probe_timeout_s", "phase_gap_s",
                "reference_domain", "remote_domain"):
        if (auth.get("clock") or {}).get(key) != req[key]:
            bad("clock_value_mismatch", f"clock.{key}: authorized {(auth.get('clock') or {}).get(key)!r}, planned {req[key]!r}")
    for key in ("output_dir", "automount_root", "max_process_lifetime_s"):
        if auth.get(key) != req[key]:
            bad(f"{key}_mismatch", f"authorized {auth.get(key)!r}, planned {req[key]!r}")
    if auth.get("process_transport") not in (pr.TRANSPORT_WSL, pr.TRANSPORT_FIXTURE):
        bad("process_transport", f"{auth.get('process_transport')!r}")
    if auth.get("process_transport") == pr.TRANSPORT_FIXTURE and not (allow_synthetic and auth.get("synthetic") is True):
        bad("fixture_transport_not_operational", "the fixture transport is accepted only with a synthetic authorization in a test")
    if auth.get("execution_environment") != pr.ENV_WINDOWS_ORCHESTRATOR:
        bad("execution_environment", f"{auth.get('execution_environment')!r}")
    if not isinstance(auth.get("real_transport_authorized"), bool):
        bad("real_transport_authorized", "must be an explicit true or false")
    return {"valid": not findings, "findings": findings, "real_transport_permitted": False if not pr.REAL_TRANSPORT_ENABLED else not findings,
            "synthetic": auth.get("synthetic") is True, "note": "a valid record is still not a measurement: the real transport and the real probe are disabled in this version"}


def execute_clock_only(plan: Mapping[str, Any], auth: Mapping[str, Any], transport: Any, *, wall_ns: Callable[[], int], mono_ns: Callable[[], int], sleep: Callable[[float], None],
                       now_utc: Callable[[], Any], allow_synthetic: bool, hash_tree: Callable[[str], Mapping[str, str]]) -> dict[str, Any]:
    """Run the clock-only plan through an INJECTED fixture/mock transport. Refused for a process transport. Writes only under the new output directory."""
    verdict = verify_authorization(auth, plan, allow_synthetic=allow_synthetic)
    if not verdict["valid"]:
        raise be.LaunchRefused("the clock-only authorization does not verify", verdict["findings"])
    if getattr(transport, "kind", None) not in ("mock", "fixture") or auth.get("process_transport") == pr.TRANSPORT_WSL:
        raise be.LaunchRefused("the real WSL process transport is disabled in this version; nothing was started")
    req = plan["request"]
    out = Path(req["output_dir"])
    if out.exists():
        raise be.LaunchRefused(f"output directory {out} already exists; nothing is overwritten")
    out.mkdir(parents=True, exist_ok=False)
    runner = pr.PlanBoundRunner(plan, transport, distro=req["distro"], mount_root=req["automount_root"], max_process_lifetime_s=req["max_process_lifetime_s"], vectors=clock_only_vectors(plan))

    def write(rel: str, doc: Any) -> None:
        be._create_new(out / rel, (json.dumps(doc, indent=2, sort_keys=True) + "\n").encode("utf-8"))

    class _Call:                                              # adapts the runner to the (argv, timeout) -> (code, out, err) callable of the probe
        kind = runner.kind

        def __call__(self, argv: Sequence[str], timeout_s: float):
            r = runner.run(list(argv), timeout_s, None)
            return (124 if r.timed_out else r.returncode), r.stdout, r.stderr

    status: dict[str, Any] = {"schema": STATUS_SCHEMA, "version": CLOCK_ONLY_VERSION, "status": "COMPLETED", "errors": [], "plan_sha256": plan["plan_sha256"], "transactions_attempted": 0,
                              "runner_kind": runner.kind, "real_process_started": False}
    records: list[dict[str, Any]] = []
    interruption: BaseException | None = None
    try:
        for phase in ("pre_run", "post_run"):
            probe = wp.WslClockProbe(_Call(), wall_ns=wall_ns, mono_ns=mono_ns, argv=plan["probe"]["argv"], remote_resolution_ns=req["remote_resolution_ns"], timeout_s=req["probe_timeout_s"],
                                     phase=phase, reference_domain=req["reference_domain"], remote_domain=req["remote_domain"])
            record = wp.collect_phase(probe, req["samples_per_phase"])
            records.append(record)
            write(f"clock/{phase}.json", record)
            if phase == "pre_run":
                sleep(float(req["phase_gap_s"]))
        verdict_doc = wp.evaluate(records, tolerance_ms=req["tolerance_ms"], min_valid_samples=req["min_valid_samples"],
                                  max_wall_monotonic_disagreement_ms=req["max_wall_monotonic_disagreement_ms"])
        verdict_doc["round_label"] = VERDICT_LABEL.get(verdict_doc["verdict"], "INCONCLUSIVE")
        write("clock/clock_verdict.json", verdict_doc)
        status["verdict"], status["round_label"] = verdict_doc["verdict"], verdict_doc["round_label"]
    except BaseException as exc:                              # noqa: BLE001 - evidence first, then re-raise
        interruption = exc
        status["status"] = "INTERRUPTED" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "ERROR"
        status["errors"].append({"error": type(exc).__name__, "message": str(exc)})
    finally:
        try:
            write("integrity_manifest.json", {"schema": "a0t_clock_only_integrity/1", "files": dict(hash_tree(str(out)))})
        except Exception as exc:                              # noqa: BLE001
            status["errors"].append({"error": "integrity", "message": f"{type(exc).__name__}: {exc}"})
        status["ended_utc"] = now_utc().isoformat()
        status["runner_log"] = list(runner.log)
        write("round_status.json", status)
    if isinstance(interruption, (KeyboardInterrupt, SystemExit)):
        raise interruption
    return status
