"""Run-outcome classification: the X13 success rule, checked independently from the files on disk (task A0v, 2026-11-22). OFFLINE; reads files, starts nothing.

The defect class. ``bounded_execution.execute_bounded`` and ``clock_only_round.execute_clock_only`` leave ``status = "COMPLETED"`` when the integrity step failed or produced nothing. A0u added an
integrity verdict and a ``live_completion`` flag, but the stored ``run_status.json`` still reads COMPLETED on its own. This module defines the ONE place that decides whether a finished run directory
may be called a success, and it decides from the stored evidence, not from what the orchestrator returned:

    success  <=>  the process operation finished cleanly (status COMPLETED, no recorded error, no transaction beyond the plan)
             AND  the integrity verdict file exists, belongs to this plan / addendum / run, says INTEGRITY_VERIFIED_REAL_WSL with gate_satisfied true
             AND  the integrity manifest lists files for both the output directory and the worker evidence root (an empty map is not verification)
             AND  the stored pre- and post-workload digest documents parse, bind the planned requests, the pre digest equals the authorized overlay and the post digest leaves every staged entry unchanged
             AND  the evidence is REAL (runner kind ``process``), never simulated.

A process exit code of zero is not enough, and neither is a status of COMPLETED. Anything else gets a named outcome and the list of reasons. The outcome file ``run_outcome.json`` is created exclusively
(never overwritten) next to the other evidence. Partial evidence is never deleted.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from . import bounded_execution as be
from . import wsl_integrity as wi
from . import wsl_transport as wt

OUTCOME_VERSION = "run_outcome 0.1 (A0v, 2026-11-22)"
OUTCOME_SCHEMA = "a0v_run_outcome/1"
OUTCOME_FILE = "run_outcome.json"
O_VERIFIED = "VERIFIED_SUCCESS_REAL_EVIDENCE"
O_SIMULATED = "SIMULATED_ONLY_NOT_A_LIVE_RESULT"
O_INTEGRITY_FAILED = "INTEGRITY_FAILED"
O_INTEGRITY_MISSING = "INTEGRITY_EVIDENCE_MISSING"
O_PROCESS_FAILED = "PROCESS_OPERATION_FAILED"
O_INTERRUPTED = "INTERRUPTED"
O_NOT_RUN = "NOT_RUN_OR_NO_STATUS"
OUTCOMES = (O_VERIFIED, O_SIMULATED, O_INTEGRITY_FAILED, O_INTEGRITY_MISSING, O_PROCESS_FAILED, O_INTERRUPTED, O_NOT_RUN)


def _load(path: Path) -> tuple[Any, str | None]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except FileNotFoundError:
        return None, "missing"
    except (OSError, ValueError) as exc:
        return None, f"unreadable: {type(exc).__name__}"


def _finish(outcome: str, reasons: list[dict[str, str]], extra: Mapping[str, Any]) -> dict[str, Any]:
    return {"schema": OUTCOME_SCHEMA, "version": OUTCOME_VERSION, "outcome": outcome, "success": outcome == O_VERIFIED, "reasons": reasons, **dict(extra),
            "note": "success requires a completed process operation AND real, independently re-checked integrity evidence; COMPLETED alone is never success"}


TRANSPORT_DIR = "transport"


def _transport_document(out: Path, category: str) -> tuple[Mapping[str, Any] | None, list[dict[str, str]]]:
    """The stored transport evidence of one operation category (``transport/<category>/evidence.json``), re-validated: schema, own hash, real mode."""
    problems: list[dict[str, str]] = []
    document, problem = _load(out / TRANSPORT_DIR / category / "evidence.json")
    if document is None:
        return None, [{"code": "transport_evidence_missing", "message": f"{TRANSPORT_DIR}/{category}/evidence.json: {problem}"}]
    if document.get("schema") != wt.EVIDENCE_SCHEMA or document.get("category") != category:
        problems.append({"code": "transport_evidence_schema", "message": f"schema {document.get('schema')!r}, category {document.get('category')!r}"})
    if document.get("document_sha256") != be._digest(document, drop=("document_sha256",)):
        problems.append({"code": "transport_evidence_hash", "message": "the evidence document's own hash does not match its content"})
    if document.get("mode") != wt.MODE_REAL or document.get("kind") != "process":
        problems.append({"code": "runner_not_real", "message": f"transport mode {document.get('mode')!r}, kind {document.get('kind')!r}: not a real process"})
    return document, problems


def _rows(document: Mapping[str, Any], step: str) -> list[Mapping[str, Any]]:
    return [r for r in document.get("evidence", []) if r.get("step") == step]


def classify_bounded_run(out_dir: Path | str, plan: Mapping[str, Any], addendum: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a finished A0s/A0u run directory. Reads the stored files only."""
    out = Path(out_dir)
    host = plan["layout"]["host_evidence"]
    reasons: list[dict[str, str]] = []

    def why(code: str, message: str) -> None:
        reasons.append({"code": code, "message": message})

    status, problem = _load(out / host["run_status"])
    if status is None:
        why("run_status_" + (problem or "missing").split(":")[0], f"{host['run_status']}: {problem}")
        return _finish(O_NOT_RUN, reasons, {"run_id": plan["run_id"], "plan_sha256": plan["plan_sha256"]})
    base = {"run_id": plan["run_id"], "plan_sha256": plan["plan_sha256"], "a0s_status": status.get("status"), "runner_kind": status.get("runner_kind")}
    if status.get("plan_sha256") not in (None, plan["plan_sha256"]):
        why("status_for_another_plan", "the stored status binds another plan")
    if status.get("status") == "INTERRUPTED":
        why("interrupted", "the run was interrupted")
        return _finish(O_INTERRUPTED, reasons, base)
    process_ok = status.get("status") == "COMPLETED" and not status.get("errors")
    if status.get("status") != "COMPLETED":
        why("status_not_completed", f"A0s status is {status.get('status')!r}")
    if status.get("errors"):
        why("status_has_errors", f"{len(status['errors'])} recorded error(s); first step {status['errors'][0].get('step')!r}")
    verdict, vproblem = _load(out / wi.VERDICT_FILE)
    manifest, mproblem = _load(out / host["integrity_manifest"])
    integrity_reasons: list[dict[str, str]] = []

    def iwhy(code: str, message: str) -> None:
        integrity_reasons.append({"code": code, "message": message})

    if verdict is None:
        iwhy("integrity_verdict_" + (vproblem or "missing").split(":")[0], f"{wi.VERDICT_FILE}: {vproblem}")
    else:
        if verdict.get("schema") != wi.VERDICT_SCHEMA:
            iwhy("verdict_schema", f"schema {verdict.get('schema')!r}")
        for key, expected in (("run_id", plan["run_id"]), ("plan_sha256", plan["plan_sha256"]), ("addendum_sha256", addendum["addendum_sha256"])):
            if verdict.get(key) != expected:
                iwhy("verdict_binding_mismatch", f"{key}: stored {verdict.get(key)!r}, expected {expected!r}")
        if verdict.get("verdict") == wi.V_FAILED:
            iwhy("integrity_failed", "the integrity verdict is INTEGRITY_FAILED: " + "; ".join(p.get("code", "?") for p in verdict.get("problems", [])[:6]))
        elif verdict.get("verdict") == wi.V_NA:
            iwhy("integrity_not_applicable", "nothing was staged, so nothing was verified; that is not a pass")
        elif verdict.get("verdict") == wi.V_SIM or verdict.get("evidence_class") != "REAL_WSL_OBSERVATION":
            iwhy("integrity_simulated", f"verdict {verdict.get('verdict')!r}, evidence class {verdict.get('evidence_class')!r}: simulated evidence never satisfies a live gate")
        elif verdict.get("verdict") != wi.V_REAL or verdict.get("gate_satisfied") is not True:
            iwhy("integrity_gate_not_satisfied", f"verdict {verdict.get('verdict')!r}, gate_satisfied {verdict.get('gate_satisfied')!r}")
    if manifest is None:
        iwhy("integrity_manifest_" + (mproblem or "missing").split(":")[0], f"{host['integrity_manifest']}: {mproblem}")
    else:
        for finding in wi.audit_integrity_manifest(manifest):
            iwhy(finding["code"], finding["message"])
    if status.get("runner_kind") not in (None, "process"):
        iwhy("runner_not_real", f"runner kind {status.get('runner_kind')!r}: the evidence is not from a real process")
    # independent re-check of the stored digest documents (does not trust the verdict file)
    digests: dict[str, Any] = {}
    for phase, rel in (("pre_workload", wi.PRE_FILE), ("post_workload", wi.POST_FILE)):
        path = out / rel
        if not path.is_file():
            iwhy(f"{phase}_evidence_missing", f"{rel} is not present")
            continue
        try:
            document = wi.parse_evidence(path.read_bytes())
            step = next(s for s in addendum["steps"] if s["kind"] == "integrity_wsl_digest" and s["phase"] == phase)
            found = wi.check_evidence(document, expected_request=step["request"], addendum=addendum)
            if phase == "pre_workload":
                found += wi.check_pre_workload(document, addendum)
            digests[phase] = document
            for finding in found:
                iwhy(f"{phase}_{finding['code']}", finding["message"])
        except (wi.IntegrityRefused, StopIteration, KeyError, TypeError) as exc:
            iwhy(f"{phase}_evidence_invalid", f"{type(exc).__name__}: {exc}")
    if "pre_workload" in digests and "post_workload" in digests:
        for finding in wi.check_staged_unchanged(digests["pre_workload"], digests["post_workload"]):
            iwhy(finding["code"], finding["message"])
    # the stored digest files must be the very bytes a REAL process printed (a verdict file alone can be forged or copied from a simulation)
    transport, tproblems = _transport_document(out, wt.OPC_X13)
    for finding in tproblems:
        iwhy(finding["code"], finding["message"])
    if transport is not None:
        binding = addendum["binding"]
        try:
            expected_digest = wt.vectors_digest(wt.vectors_for_category(wt.OPC_X13, addendum, distro=binding["distro"], mount_root=binding["automount_root"]))
            if transport.get("vectors_sha256") != expected_digest:
                iwhy("transport_vectors_mismatch", "the transport evidence belongs to other vectors than the addendum's digest vectors")
        except (wt.ActivationRefused, KeyError) as exc:
            iwhy("transport_vectors_underivable", str(exc))
        for step in (s for s in addendum["steps"] if s["kind"] == "integrity_wsl_digest"):
            rows, stored = _rows(transport, step["step"]), out / step["output"]
            if len(rows) != 1:
                iwhy("transport_row_count", f"{step['step']}: {len(rows)} process record(s), expected exactly 1")
                continue
            row = rows[0]
            if row.get("returncode") != 0 or row.get("timed_out") or row.get("interrupted") or row.get("output_truncated") or row.get("capture_incomplete") or row.get("spawn_error"):
                iwhy("transport_row_not_clean", f"{step['step']}: returncode {row.get('returncode')!r}, timed_out {row.get('timed_out')!r}, interrupted {row.get('interrupted')!r}, "
                                                f"truncated {row.get('output_truncated')!r}, capture_incomplete {row.get('capture_incomplete')!r}")
            if stored.is_file() and hashlib.sha256(stored.read_bytes()).hexdigest() != row.get("stdout_sha256"):
                iwhy("transport_stdout_hash_mismatch", f"{step['output']} is not the stdout the transport recorded for {step['step']}")
    reasons += integrity_reasons
    base["integrity_verdict"] = (verdict or {}).get("verdict")
    base["evidence_class"] = (verdict or {}).get("evidence_class")
    if not process_ok:
        return _finish(O_PROCESS_FAILED, reasons, base)
    if not integrity_reasons:
        return _finish(O_VERIFIED, reasons, base)
    codes = {r["code"] for r in integrity_reasons}
    if codes & {"integrity_simulated", "runner_not_real"} and codes <= {"integrity_simulated", "runner_not_real", "transport_evidence_missing"}:
        return _finish(O_SIMULATED, reasons, base)
    if any(c.endswith(("_missing", "_unreadable")) or c.startswith("integrity_verdict_") for c in codes) and not any(c == "integrity_failed" or "mismatch" in c or "changed" in c for c in codes):
        return _finish(O_INTEGRITY_MISSING, reasons, base)
    return _finish(O_INTEGRITY_FAILED, reasons, base)


def classify_clock_round(out_dir: Path | str, plan: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a finished clock-only round directory. The round's PASS/FAIL/INCONCLUSIVE clock verdict is a measurement result and is reported separately; success here means only that the
    measurement is complete, real and its evidence manifest is not empty and not an error."""
    out = Path(out_dir)
    reasons: list[dict[str, str]] = []

    def why(code: str, message: str) -> None:
        reasons.append({"code": code, "message": message})

    status, problem = _load(out / "round_status.json")
    base = {"round_id": plan["request"]["round_id"], "plan_sha256": plan["plan_sha256"]}
    if status is None:
        why("round_status_" + (problem or "missing").split(":")[0], f"round_status.json: {problem}")
        return _finish(O_NOT_RUN, reasons, base)
    base.update(a0t_status=status.get("status"), runner_kind=status.get("runner_kind"), clock_verdict=status.get("verdict"), clock_label=status.get("round_label"))
    if status.get("plan_sha256") != plan["plan_sha256"]:
        why("status_for_another_plan", "the stored status binds another plan")
    if status.get("status") == "INTERRUPTED":
        why("interrupted", "the round was interrupted")
        return _finish(O_INTERRUPTED, reasons, base)
    errors = status.get("errors") or []
    integrity_errors = [e for e in errors if e.get("error") == "integrity"]
    other_errors = [e for e in errors if e.get("error") != "integrity"]
    if status.get("status") != "COMPLETED" or other_errors:
        why("process_failed", f"status {status.get('status')!r}, {len(other_errors)} error(s)")
    manifest, mproblem = _load(out / "integrity_manifest.json")
    integrity_reasons: list[dict[str, str]] = []
    if integrity_errors:
        integrity_reasons.append({"code": "integrity_step_error", "message": str(integrity_errors[0].get("message"))})
    if manifest is None:
        integrity_reasons.append({"code": "integrity_manifest_" + (mproblem or "missing").split(":")[0], "message": f"integrity_manifest.json: {mproblem}"})
    else:
        files = manifest.get("files") if isinstance(manifest, Mapping) else None
        expected = {"clock/pre_run.json", "clock/post_run.json", "clock/clock_verdict.json"}
        if not isinstance(files, Mapping) or not files:
            integrity_reasons.append({"code": "integrity_manifest_empty", "message": "no file is listed; an empty map is not verification"})
        else:
            missing = sorted(expected - set(files))
            if missing:
                integrity_reasons.append({"code": "integrity_manifest_incomplete", "message": f"files not listed: {missing}"})
    for rel in ("clock/pre_run.json", "clock/post_run.json", "clock/clock_verdict.json"):
        if not (out / rel).is_file():
            integrity_reasons.append({"code": "clock_evidence_missing", "message": f"{rel} is not present"})
    if status.get("runner_kind") != "process":
        integrity_reasons.append({"code": "runner_not_real", "message": f"runner kind {status.get('runner_kind')!r}: the measurement is not from a real process"})
    transport, tproblems = _transport_document(out, wt.OPC_CLOCK)
    integrity_reasons += tproblems
    if transport is not None:
        rows = [r for r in transport.get("evidence", []) if r.get("step") == "K01+K03:clock_probe"]
        expected = 2 * int(plan["request"]["samples_per_phase"])
        if len(rows) != expected:
            integrity_reasons.append({"code": "transport_row_count", "message": f"{len(rows)} probe process record(s), expected {expected}"})
        if any(r.get("returncode") != 0 or r.get("timed_out") or r.get("interrupted") or r.get("capture_incomplete") or r.get("spawn_error") for r in rows):
            integrity_reasons.append({"code": "transport_row_not_clean", "message": "a probe process did not exit cleanly"})
    reasons += integrity_reasons
    if status.get("status") != "COMPLETED" or other_errors:
        return _finish(O_PROCESS_FAILED, reasons, base)
    if not integrity_reasons:
        return _finish(O_VERIFIED, reasons, base)
    codes = {r["code"] for r in integrity_reasons}
    if codes & {"runner_not_real"} and codes <= {"runner_not_real", "transport_evidence_missing"}:
        return _finish(O_SIMULATED, reasons, base)
    if all(c.endswith(("_missing", "_unreadable")) for c in codes):
        return _finish(O_INTEGRITY_MISSING, reasons, base)
    return _finish(O_INTEGRITY_FAILED, reasons, base)


def write_outcome(out_dir: Path | str, outcome: Mapping[str, Any]) -> str:
    """Exclusive-create ``run_outcome.json`` (never overwritten, so a later reader cannot find a changed verdict). Returns the file name."""
    be._create_new(Path(out_dir) / OUTCOME_FILE, (json.dumps(dict(outcome), indent=2, sort_keys=True) + "\n").encode("utf-8"))
    return OUTCOME_FILE
