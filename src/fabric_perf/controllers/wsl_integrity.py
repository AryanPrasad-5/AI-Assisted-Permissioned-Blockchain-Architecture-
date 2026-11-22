"""X13 repair: WSL-side integrity digest, bound to the approved run (task A0u, 2026-11-22). OFFLINE. This module contains NO process launcher and reads NO environment variable.

The defect (CONFIRMED in source). ``bounded_execution.execute_bounded`` ends by calling ``deps.hash_tree(plan["mounts"]["run_dir_host"])``. That value is the worker evidence root, a WSL-native
directory such as ``/home/<user>/e1/caliper-ws-benchmark/runs/<run>``. A Windows-host ``hash_tree`` cannot open it (``path_mapping`` refuses UNC forms; ``Path('/home/..')`` becomes ``C:\\home\\..``),
and the reference ``hash_tree`` of the test support returns ``{}`` for a missing directory. The integrity manifest then records ``"worker_evidence_root": {}`` and the run status stays COMPLETED:
an empty manifest that reads as a successful verification.

The repair is an ADDENDUM and a runner hook; no A0s or A0t file changes and no earlier plan is rewritten.

* ``build_integrity_addendum`` reassigns the worker-root part of X13 to the WSL distribution: two structured ``wsl.exe -d <distro> --cd <repo> --exec python3 scripts/e1/e1_wsl_tree_digest.py digest ...``
  vectors (``pre_workload`` and ``post_workload``), bound to the plan digest, the run id, the variant, the distribution and the path-mapping record, with explicit bounds and timeout. The output
  directory part of X13 stays on the Windows host (a Windows path, hashed locally, and now refused when missing or empty).
* ``IntegrityBoundRunner`` is a ``PlanBoundRunner`` whose vector table also holds the two digest vectors. Directly after the staging step (X05) succeeds it runs the PRE digest and compares the staged
  run directory with the authorized overlay hashes (exact files, exact hashes, no extras); a mismatch makes the staging step fail, so ``execute_bounded`` halts BEFORE any transaction. Its ``hash_tree``
  dispatch runs the POST digest for the worker root and checks that every staged file is unchanged; a failure raises, so ``execute_bounded`` writes NO integrity manifest.
* ``execute_with_integrity`` is the wiring (the counterpart of ``process_runner.execute_through_adapter``) and writes ``integrity/integrity_verdict.json``. The verdict, not the A0s run status, is the
  integrity gate: the A0s status can still read COMPLETED when the integrity step errored.

Evidence classes. A document is ``REAL_WSL_OBSERVATION`` only if it came through a transport of kind ``process`` while ``process_runner.REAL_TRANSPORT_ENABLED`` is true; otherwise it is ``SIMULATED``
whatever it claims about itself. In this version the real transport is disabled, so no integrity evidence can satisfy a live gate (``gate_satisfied`` is False for every simulated run).
"""

from __future__ import annotations

import json
import posixpath
import re
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import bounded_execution as be
from . import path_mapping as pm
from . import prelive_integrity as pi
from . import process_runner as pr
from . import tree_digest as td

INTEGRITY_VERSION = "wsl_integrity 0.1 (A0u, 2026-11-22)"
ADDENDUM_SCHEMA = "a0u_integrity_addendum/1"
A0U_AUTH_SCHEMA = "a0u_integrity_authorization/1"
VERDICT_SCHEMA = "a0u_integrity_verdict/1"
ENV_WSL_SIDE_INTEGRITY = "wsl_side_digest_via_wsl_exec"
DIGEST_TOOL = "scripts/e1/e1_wsl_tree_digest.py"
DIGEST_LIB = "src/fabric_perf/controllers/tree_digest.py"
STAGE_STEP, X13_STEP = "X05_stage_rendered_run", "X13_integrity"
EVIDENCE_DIR = "integrity"
PRE_FILE, POST_FILE, VERDICT_FILE = f"{EVIDENCE_DIR}/worker_root_pre_workload.json", f"{EVIDENCE_DIR}/worker_root_post_workload.json", f"{EVIDENCE_DIR}/integrity_verdict.json"
V_REAL, V_SIM, V_FAILED, V_NA = "INTEGRITY_VERIFIED_REAL_WSL", "INTEGRITY_SIMULATED_ONLY", "INTEGRITY_FAILED", "INTEGRITY_NOT_APPLICABLE_NEVER_STAGED"
EXIT_PRE_FAILED = 97
MAX_EVIDENCE_BYTES = 16 << 20
LIMIT_KEYS = ("max_entries", "max_total_bytes", "max_depth")
A0U_OWNER_KEYS = ("integrity_environment", "distro", "caliper_container", "authorization_reference", "real_integrity_transport_authorized")
A0U_COPIED_KEYS = ("schema", "integrity_addendum_sha256", "operation_category", "phases", "approved_root", "worker_root", "symlink_policy", "digest_limits", "digest_timeout_s")


class IntegrityRefused(ValueError):
    """An integrity step cannot be built, run or accepted. ``code`` is a stable machine-readable cause; nothing is repaired."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{message} [{code}]")
        self.code = code


def evidence_class(runner_kind: Any) -> str:
    """``REAL_WSL_OBSERVATION`` only for a ``process`` transport that is enabled; everything else is ``SIMULATED`` regardless of what a document says about itself."""
    return "REAL_WSL_OBSERVATION" if (runner_kind == "process" and pr.REAL_TRANSPORT_ENABLED) else "SIMULATED"


def _wsl_form(path: str, mount_root: str, what: str) -> str:
    kind = pm.classify(path)
    if kind == "windows_drive":
        return pm.windows_to_wsl(path, mount_root=mount_root)
    return pm.normalize_posix(path, what=what)


# ------------------------------------------------------------------------------------------------ expectation derived from the plan
def overlay_expectation(plan: Mapping[str, Any]) -> dict[str, Any]:
    """What the staged run directory must hold right after X05: every authorized overlay file at its authorized hash, in its parent directories, and nothing else."""
    overlay = Path(plan["layout"]["launch_authorizing_directory"])
    prefix = f"rendered:{overlay.parent.name}/{overlay.name}/"
    files = {key[len(prefix):]: want for key, want in plan["artifact_hashes"].items() if key.startswith(prefix)}
    if not files:
        raise IntegrityRefused("no_overlay_files", f"the plan lists no authorized overlay file under {prefix!r}")
    dirs = sorted({posixpath.dirname(rel) for rel in files if posixpath.dirname(rel)} | {d for rel in files for d in _parents(rel)})
    return {"files": dict(sorted(files.items())), "directories": dirs, "rule": "after X05 the staged run directory holds exactly these files at these hashes and exactly these parent directories (no symlink)"}


def _parents(rel: str) -> list[str]:
    parts = rel.split("/")[:-1]
    return ["/".join(parts[: i + 1]) for i in range(len(parts))]


def digest_argv(request: Mapping[str, Any]) -> list[str]:
    """The structured vector for one digest request (every value from the request; nothing is joined into a string)."""
    return ["python3", DIGEST_TOOL, "digest", "--target", request["target"], "--approved-root", request["approved_root"], "--expect", request["expected_type"], "--run-id", request["run_id"],
            "--variant", request["variant"], "--plan-sha256", request["plan_sha256"], "--path-record-sha256", request["path_record_sha256"], "--distro", request["distro"],
            "--phase", request["phase"], "--operation", request["operation"], "--symlinks", request["symlink_policy"], "--max-entries", str(request["max_entries"]),
            "--max-bytes", str(request["max_total_bytes"]), "--max-depth", str(request["max_depth"])]


def digest_requests(plan: Mapping[str, Any], *, distro: str, mount_root: str, record: Mapping[str, Any], symlink_policy: str, limits: Mapping[str, int]) -> dict[str, dict[str, Any]]:
    """One request per phase; the target is the plan's worker evidence root, the approved root is ``<caliper workspace>/runs``."""
    run_dir = _wsl_form(plan["mounts"]["run_dir_host"], mount_root, "worker evidence root")
    workspace = _wsl_form(plan["mounts"]["host_workspace"], mount_root, "Caliper workspace")
    approved = workspace.rstrip("/") + "/runs"
    if not pm.within(run_dir, approved):
        raise IntegrityRefused("worker_root_outside_workspace", f"{run_dir!r} is not inside {approved!r}")
    base = {"run_id": plan["run_id"], "variant": plan["variant"], "plan_sha256": plan["plan_sha256"], "distro": distro, "operation": td.OPERATION, "approved_root": approved,
            "target": run_dir, "expected_type": "directory", "symlink_policy": symlink_policy, "max_entries": limits["max_entries"], "max_total_bytes": limits["max_total_bytes"],
            "max_depth": limits["max_depth"], "path_record_sha256": record["record_sha256"]}
    requests = {phase: {**base, "phase": phase} for phase in td.PHASES}
    for request in requests.values():
        issues = td.validate_request(request)
        if issues:
            raise IntegrityRefused("digest_request_invalid", "; ".join(f"{i['field']}: {i['message']}" for i in issues))
    return requests


# ------------------------------------------------------------------------------------------------ the addendum
def build_integrity_addendum(plan: Mapping[str, Any], *, record: Mapping[str, Any], distro: Any, mount_root: Any, symlink_policy: Any, limits: Any, digest_timeout_s: Any,
                             repo_root: Path) -> dict[str, Any]:
    """The X13 successor for ONE plan. Every parameter is required (no default); the limits and the timeout are owner values. Reads files; writes nothing; starts nothing."""
    if plan.get("schema") != be.PLAN_SCHEMA or plan.get("real_launch_enabled") is not False:
        raise IntegrityRefused("plan_not_acceptable", "the base plan must be an a0s_execution_plan/1 with real_launch_enabled false")
    if pm.verify_path_record(record):
        raise IntegrityRefused("path_record_invalid", str(pm.verify_path_record(record)))
    if record.get("plan_sha256") != plan["plan_sha256"] or record.get("run_id") != plan["run_id"]:
        raise IntegrityRefused("path_record_for_another_plan", "the path-mapping record binds another plan or run")
    if not (isinstance(distro, str) and td.DISTRO.fullmatch(distro)):
        raise IntegrityRefused("distro_required", "the WSL distribution is required (no default)")
    try:
        mount_root = pm.check_mount_root(mount_root)
    except pm.PathMappingError as exc:
        raise IntegrityRefused("mount_root_required", str(exc)) from exc
    if mount_root != record["automount_root"]:
        raise IntegrityRefused("mount_root_mismatch", "the automount root differs from the path-mapping record's")
    if symlink_policy not in td.SYMLINK_POLICIES:
        raise IntegrityRefused("symlink_policy_required", f"must be one of {list(td.SYMLINK_POLICIES)} (no default)")
    if not (isinstance(limits, Mapping) and set(limits) == set(LIMIT_KEYS)):
        raise IntegrityRefused("limits_required", f"exactly {list(LIMIT_KEYS)} are required (no default)")
    if isinstance(digest_timeout_s, bool) or not isinstance(digest_timeout_s, (int, float)) or not 0 < digest_timeout_s < float("inf"):
        raise IntegrityRefused("timeout_required", "digest_timeout_s must be a finite number > 0 (no default)")
    root = Path(repo_root)
    manifest = json.loads((root / plan["candidate_manifest"]["path"]).read_text(encoding="utf-8"))
    listed = {e["path"]: e["sha256"] for e in manifest.get("entries", [])}
    tool_hashes = {}
    for rel in (DIGEST_TOOL, DIGEST_LIB):
        if rel not in listed:
            raise IntegrityRefused("digest_tool_not_in_candidate_manifest", f"{rel} is not listed in the candidate manifest the plan names; rebuild the plan against a manifest that lists it")
        actual = pi.sha256_file(root / rel)
        if actual != listed[rel]:
            raise IntegrityRefused("digest_tool_hash_mismatch", f"{rel} differs from the candidate manifest")
        tool_hashes[rel] = actual
    requests = digest_requests(plan, distro=distro, mount_root=mount_root, record=record, symlink_policy=symlink_policy, limits=dict(limits))
    repo_cwd = plan["request"]["tools"]["wsl_repo_path"].rstrip("/")
    steps, invocations = [], []
    for step_id, phase, trigger in (("I1_worker_root_pre", "pre_workload", f"immediately after {STAGE_STEP} succeeds, inside the same runner call; before any ledger follower, Prometheus scrape or Caliper step"),
                                    ("I2_worker_root_post", "post_workload", f"during {X13_STEP} (the hash_tree call of the worker evidence root), after the collectors stopped, the clock gate and the final configuration check")):
        argv = digest_argv(requests[phase])
        inv = pr.build_invocation(step_id, argv, repo_cwd, float(digest_timeout_s), launch_mode=pr.LAUNCH_FOREGROUND, distro=distro, mount_root=mount_root)
        steps.append({"step": step_id, "phase": phase, "kind": "integrity_wsl_digest", "class": "read_only_observation", "argv": argv, "cwd": repo_cwd, "timeout_s": float(digest_timeout_s),
                      "max_uses": 1, "trigger": trigger, "request": requests[phase], "output": PRE_FILE if phase == "pre_workload" else POST_FILE})
        invocations.append({"step": step_id, "domain": inv.domain, "os_argv": list(inv.os_argv), "wsl_cwd": inv.wsl_cwd, "timeout_s": inv.timeout_s, "path_translations": [list(t) for t in inv.translations]})
    steps.append({"step": "I3_output_dir_host", "phase": "post_workload", "kind": "integrity_host_digest", "class": "offline_internal", "argv": None, "cwd": None, "timeout_s": None, "max_uses": 1,
                  "trigger": f"during {X13_STEP} (the hash_tree call of the output directory); a Windows path hashed on the Windows host; refused when missing, empty, symlinked or special",
                  "request": None, "output": "integrity_manifest.json"})
    addendum = {
        "schema": ADDENDUM_SCHEMA, "version": INTEGRITY_VERSION, "status": "PLANNED_NOT_AUTHORIZED_NOT_EXECUTED", "real_transport_enabled": pr.REAL_TRANSPORT_ENABLED,
        "supersedes_step": {"plan_schema": plan["schema"], "plan_sha256": plan["plan_sha256"], "step": X13_STEP, "plan_rewritten": False,
                            "defect": "the worker evidence root is WSL-native and was hashed from the Windows host; the host-side hash returned {} (an empty manifest) and the run still read COMPLETED",
                            "reassignment": {"worker_evidence_root": "wsl_distribution (structured wsl.exe --exec vector, read-only)", "output_directory": "windows_host (local, fail-closed)"}},
        "binding": {"run_id": plan["run_id"], "variant": plan["variant"], "channel": plan["channel"], "cell_id": plan["cell_id"], "plan_sha256": plan["plan_sha256"],
                    "path_record_sha256": record["record_sha256"], "distro": distro, "automount_root": mount_root},
        "environment": {"orchestrator": "windows_host", "digest_environment": "wsl_distribution", "name": ENV_WSL_SIDE_INTEGRITY, "operation_category": td.OPERATION,
                        "base_execution_environment": pr.ENV_WINDOWS_ORCHESTRATOR},
        "worker_root": {"wsl_path": requests["pre_workload"]["target"], "approved_root": requests["pre_workload"]["approved_root"], "container_path": plan["mounts"]["run_dir_container"],
                        "expected_type": "directory", "symlink_policy": symlink_policy, "limits": {k: limits[k] for k in LIMIT_KEYS}, "digest_timeout_s": float(digest_timeout_s)},
        "expected_pre_workload": overlay_expectation(plan),
        "tool": {"script": DIGEST_TOOL, "script_sha256": tool_hashes[DIGEST_TOOL], "library": DIGEST_LIB, "library_sha256": tool_hashes[DIGEST_LIB], "repo_cwd": repo_cwd,
                 "interpreter": "python3 (version inside the distribution is UNVERIFIED; the tool avoids syntax newer than Python 3.9)", "reads": ["WSL_DISTRO_NAME (attestation only; it can only cause a refusal)"]},
        "steps": steps, "invocations": invocations,
        "evidence_outputs": {"pre_workload": PRE_FILE, "post_workload": POST_FILE, "verdict": VERDICT_FILE, "created_exclusively_under": plan["layout"]["output_dir"]},
        "lifecycle": {"before_workload": "I1: the staged run directory equals the authorized overlay (files, hashes, no extras). Without it a post-run digest cannot show the execution environment was the authorized one.",
                      "after_workload": "I2: the worker evidence root is digested again; every staged file must be unchanged; the produced files (issue log, recorder files) are hashed for the evidence manifest.",
                      "host": "I3: the output directory is hashed on the Windows host as before."},
        "failure_behavior": {"pre_workload_failure": "the staging step returns a failure; execute_bounded halts HALTED_BEFORE_TRANSACTIONS; no transaction is attempted",
                             "post_workload_failure": "hash_tree raises; execute_bounded writes no integrity manifest and records the error; the verdict is INTEGRITY_FAILED",
                             "refusal_document": "a refusal is printed as a document and exits 3; it is never treated as a digest",
                             "never_staged": f"{V_NA}: nothing was staged, so there is no worker root to verify (not a pass)",
                             "simulated_evidence": f"{V_SIM}: never satisfies a live gate"},
        "runtime_verification_required": ["python3 exists in the distribution and runs the tool", "WSL_DISTRO_NAME equals the authorized distribution", "the repository is visible at the declared /mnt path",
                                          "the workspace runs/ directory exists and the run directory is created by X04", "the staged directory contains no empty directory the overlay manifest does not imply",
                                          "no symbolic link exists in the run directory", "the container's workspace mount equals the declared host path (separate read-only discovery)"],
        "claims_not_made": ["the vector works on the target machine (never run)", "python3 or the repository is available inside the distribution", "any path exists", "any real digest was produced"],
    }
    addendum["addendum_sha256"] = be._digest(addendum, drop=("addendum_sha256",))
    return addendum


def verify_addendum(addendum: Any, plan: Mapping[str, Any], record: Mapping[str, Any], *, repo_root: Path) -> list[dict[str, str]]:
    """Problems of a stored addendum against its plan, path record and the files on disk (an empty list means intact)."""
    problems: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        problems.append({"code": code, "message": message})

    if not isinstance(addendum, Mapping) or addendum.get("schema") != ADDENDUM_SCHEMA:
        return [{"code": "addendum_schema", "message": "not an a0u_integrity_addendum/1"}]
    if be._digest(addendum, drop=("addendum_sha256",)) != addendum.get("addendum_sha256"):
        bad("addendum_digest", "the addendum was edited after it was written")
    if addendum.get("real_transport_enabled") is not False:
        bad("real_transport_flag", "an addendum may not carry real_transport_enabled true")
    binding = addendum.get("binding") or {}
    for key, want in (("run_id", plan["run_id"]), ("variant", plan["variant"]), ("channel", plan["channel"]), ("cell_id", plan["cell_id"]), ("plan_sha256", plan["plan_sha256"]),
                      ("path_record_sha256", record.get("record_sha256")), ("automount_root", record.get("automount_root"))):
        if binding.get(key) != want:
            bad("binding_mismatch", f"{key}: addendum {binding.get(key)!r}, plan/record {want!r}")
    if (addendum.get("supersedes_step") or {}).get("plan_sha256") != plan["plan_sha256"]:
        bad("base_plan_mismatch", "the addendum supersedes the X13 step of another plan")
    tool = addendum.get("tool") or {}
    for rel, key in ((DIGEST_TOOL, "script_sha256"), (DIGEST_LIB, "library_sha256")):
        target = Path(repo_root) / rel
        if not target.is_file() or pi.sha256_file(target) != tool.get(key):
            bad("digest_tool_hash", f"{rel} differs from the addendum")
        if plan["candidate_manifest"] and plan.get("artifact_hashes") is not None:
            manifest = json.loads((Path(repo_root) / plan["candidate_manifest"]["path"]).read_text(encoding="utf-8"))
            if {e["path"]: e["sha256"] for e in manifest["entries"]}.get(rel) != tool.get(key):
                bad("digest_tool_not_in_candidate_manifest", f"{rel} is not in the plan's candidate manifest at the addendum's hash")
    try:
        worker = addendum["worker_root"]
        rebuilt = build_integrity_addendum(plan, record=record, distro=binding.get("distro"), mount_root=binding.get("automount_root"), symlink_policy=worker["symlink_policy"],
                                           limits=worker["limits"], digest_timeout_s=worker["digest_timeout_s"], repo_root=Path(repo_root))
        for key in ("worker_root", "expected_pre_workload", "steps", "invocations", "evidence_outputs"):
            if rebuilt[key] != addendum.get(key):
                bad("addendum_not_reproducible", f"{key} differs from what the plan and record imply")
    except (IntegrityRefused, KeyError, pm.PathMappingError, pr.ArgvRefused) as exc:
        bad("addendum_not_reproducible", f"{type(exc).__name__}: {exc}")
    return problems


# ------------------------------------------------------------------------------------------------ authorization extension (an 'a0u' block next to the 'a0t' block)
def a0u_authorization_extension(addendum: Mapping[str, Any]) -> dict[str, Any]:
    """The ``a0u`` block with every owner-decided field UNSET (None). Fields copied from the addendum are for review; copying is not approving."""
    worker = addendum["worker_root"]
    return {"schema": A0U_AUTH_SCHEMA, "integrity_addendum_sha256": addendum["addendum_sha256"], "operation_category": td.OPERATION, "phases": list(td.PHASES), "approved_root": worker["approved_root"],
            "worker_root": worker["wsl_path"], "symlink_policy": worker["symlink_policy"], "digest_limits": dict(worker["limits"]), "digest_timeout_s": worker["digest_timeout_s"],
            "integrity_environment": None, "distro": None, "caliper_container": None, "authorization_reference": None, "real_integrity_transport_authorized": None}


def verify_a0u_authorization(auth: Any, plan: Mapping[str, Any], addendum: Mapping[str, Any], *, allow_synthetic: bool = False) -> dict[str, Any]:
    findings: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        findings.append({"code": code, "message": message})

    ext = auth.get("a0u") if isinstance(auth, Mapping) else None
    if not isinstance(ext, Mapping):
        bad("a0u_extension_missing", "the authorization has no 'a0u' block")
        return {"valid": False, "findings": findings, "real_transport_permitted": False}
    unset = [k for k in A0U_OWNER_KEYS if ext.get(k) in (None, "")]
    if unset:
        bad("a0u_field_unset", f"unset owner fields: {unset}")
    expected = a0u_authorization_extension(addendum)
    for key in A0U_COPIED_KEYS:
        if ext.get(key) != expected[key]:
            bad("a0u_copied_field_mismatch", f"{key}: authorized {ext.get(key)!r}, addendum {expected[key]!r}")
    if ext.get("integrity_environment") != ENV_WSL_SIDE_INTEGRITY:
        bad("integrity_environment", f"{ext.get('integrity_environment')!r}; the only supported value is {ENV_WSL_SIDE_INTEGRITY!r}")
    if ext.get("distro") != addendum["binding"]["distro"] or ((auth.get("a0t") or {}).get("distro") != ext.get("distro")):
        bad("distro_mismatch", "the A0u distribution must equal the addendum's and the A0t block's")
    if ext.get("caliper_container") != plan["mounts"]["caliper_container"]:
        bad("caliper_container_mismatch", f"authorized {ext.get('caliper_container')!r}, the plan launches in {plan['mounts']['caliper_container']!r}; a different container needs a new plan, not a silent substitution")
    if not (isinstance(ext.get("authorization_reference"), str) and ext.get("authorization_reference").strip()):
        bad("authorization_reference", "an authorization reference (text) is required")
    if not isinstance(ext.get("real_integrity_transport_authorized"), bool):
        bad("real_integrity_transport_authorized", "must be an explicit true or false")
    life = (auth.get("a0t") or {}).get("max_process_lifetime_s")
    if isinstance(life, (int, float)) and not isinstance(life, bool) and addendum["worker_root"]["digest_timeout_s"] > life:
        bad("digest_timeout_above_lifetime", "digest_timeout_s exceeds max_process_lifetime_s")
    if auth.get("synthetic") is True and not allow_synthetic:
        bad("synthetic_authorization", "a synthetic authorization is accepted only for a mock or fixture transport in a test")
    return {"valid": not findings, "findings": findings, "real_transport_permitted": False if not pr.REAL_TRANSPORT_ENABLED else not findings,
            "note": "a valid record is still not a measurement: the real transport is disabled in this version"}


# ------------------------------------------------------------------------------------------------ evidence acceptance
def parse_evidence(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, (bytes, bytearray)) or not raw:
        raise IntegrityRefused("evidence_missing", "the digest tool printed nothing")
    if len(raw) > MAX_EVIDENCE_BYTES:
        raise IntegrityRefused("evidence_too_large", f"{len(raw)} bytes exceed {MAX_EVIDENCE_BYTES}")
    try:
        text = bytes(raw).decode("utf-8")
    except UnicodeDecodeError:
        raise IntegrityRefused("evidence_not_utf8", "the digest output is not UTF-8") from None
    lines = [line for line in text.replace("\r\n", "\n").split("\n") if line.strip()]
    if len(lines) != 1:
        raise IntegrityRefused("evidence_not_single_document", f"expected exactly one JSON document, got {len(lines)} lines")
    try:
        document = json.loads(lines[0])
    except ValueError:
        raise IntegrityRefused("evidence_not_json", "the digest output is not JSON") from None
    if not isinstance(document, dict):
        raise IntegrityRefused("evidence_not_object", "the digest output is not a JSON object")
    return document


def check_evidence(document: Mapping[str, Any], *, expected_request: Mapping[str, Any], addendum: Mapping[str, Any]) -> list[dict[str, str]]:
    """Everything the orchestrator re-checks: internal consistency, the binding it asked for, the distribution, a Linux platform and the tool hashes it authorized."""
    problems = list(td.verify_evidence(document))
    if document.get("binding") != {k: expected_request[k] for k in td.REQUEST_KEYS}:
        problems.append({"code": "binding_mismatch", "message": "the document answers another request than the one that was authorized"})
    obs = document.get("observation") or {}
    if obs.get("observed_distro") != addendum["binding"]["distro"]:
        problems.append({"code": "distro_mismatch", "message": f"observed {obs.get('observed_distro')!r}, authorized {addendum['binding']['distro']!r}"})
    if obs.get("platform") != "linux":
        problems.append({"code": "not_a_linux_process", "message": f"platform {obs.get('platform')!r}"})
    for key, want in (("tool_sha256", addendum["tool"]["script_sha256"]), ("library_sha256", addendum["tool"]["library_sha256"])):
        if obs.get(key) != want:
            problems.append({"code": "tool_hash_mismatch", "message": f"{key} {obs.get(key)!r} differs from the authorized {want!r}"})
    return problems


def check_pre_workload(document: Mapping[str, Any], addendum: Mapping[str, Any]) -> list[dict[str, str]]:
    """The staged run directory must equal the authorized overlay: same files, same hashes, same parent directories, no symlink, nothing else."""
    expected, problems = addendum["expected_pre_workload"], []
    entries = document["tree"]["entries"]
    files = {e["path"]: e["sha256"] for e in entries if e["type"] == "file"}
    dirs = sorted(e["path"] for e in entries if e["type"] == "dir")
    for rel in sorted(set(expected["files"]) - set(files)):
        problems.append({"code": "staged_file_missing", "message": rel})
    for rel in sorted(set(files) - set(expected["files"])):
        problems.append({"code": "staged_file_unexpected", "message": rel})
    for rel in sorted(set(files) & set(expected["files"])):
        if files[rel] != expected["files"][rel]:
            problems.append({"code": "staged_file_hash_mismatch", "message": rel})
    if dirs != sorted(expected["directories"]):
        problems.append({"code": "staged_directories_differ", "message": f"found {dirs}, expected {sorted(expected['directories'])}"})
    if any(e["type"] == "symlink" for e in entries):
        problems.append({"code": "staged_symlink", "message": "a symbolic link exists in the staged run directory"})
    return problems


def check_staged_unchanged(pre: Mapping[str, Any], post: Mapping[str, Any]) -> list[dict[str, str]]:
    post_entries = {e["path"]: e for e in post["tree"]["entries"]}
    problems = []
    for entry in pre["tree"]["entries"]:
        now = post_entries.get(entry["path"])
        if now is None:
            problems.append({"code": "staged_entry_removed", "message": entry["path"]})
        elif now != entry:
            problems.append({"code": "staged_entry_changed", "message": entry["path"]})
    return problems


def host_hash_tree(root: str) -> dict[str, str]:
    """The Windows-host hash of the output directory, fail-closed: a missing, non-directory, empty, symlinked or special tree is an error, never ``{}``."""
    base = Path(root)
    if not base.is_dir():
        raise IntegrityRefused("output_dir_missing", f"{root} is not a directory on this machine")
    out: dict[str, str] = {}
    for path in sorted(base.rglob("*")):
        if path.is_symlink():
            raise IntegrityRefused("output_dir_symlink", f"{path} is a symbolic link")
        if path.is_file():
            out[path.relative_to(base).as_posix()] = pi.sha256_file(path)
        elif not path.is_dir():
            raise IntegrityRefused("output_dir_special_file", f"{path} is not a regular file or directory")
    if not out:
        raise IntegrityRefused("output_dir_empty", f"{root} holds no file; an empty manifest is not verification")
    return out


def audit_integrity_manifest(manifest: Mapping[str, Any]) -> list[dict[str, str]]:
    """Problems with an A0s-format ``integrity_manifest.json`` (``files.output_dir`` / ``files.worker_evidence_root``). An empty worker root is NOT verification (the X13 defect's signature)."""
    files = (manifest or {}).get("files") if isinstance(manifest, Mapping) else None
    if not isinstance(files, Mapping):
        return [{"code": "manifest_malformed", "message": "no files block"}]
    problems = []
    for name in ("output_dir", "worker_evidence_root"):
        if not files.get(name):
            problems.append({"code": f"{name}_empty", "message": f"{name} lists no file: an empty hash map does not verify anything"})
    return problems


# ------------------------------------------------------------------------------------------------ the runner hook
class IntegrityBoundRunner(pr.PlanBoundRunner):
    """``PlanBoundRunner`` whose table also holds the two digest vectors. It runs the PRE digest straight after the staging step and the POST digest on request."""

    def __init__(self, plan: Mapping[str, Any], transport: Any, *, addendum: Mapping[str, Any], distro: str, mount_root: str, max_process_lifetime_s: float):
        self.addendum = addendum
        steps = {s["phase"]: s for s in addendum["steps"] if s["kind"] == "integrity_wsl_digest"}
        extra = [pr.PlanVector(s["step"], tuple(s["argv"]), s["cwd"], float(s["timeout_s"]), pr.LAUNCH_FOREGROUND, "read_only_observation", 1) for s in steps.values()]
        super().__init__(plan, transport, distro=distro, mount_root=mount_root, max_process_lifetime_s=max_process_lifetime_s, vectors=pr.planned_vectors(plan) + extra)
        stage = next((s for s in plan["steps"] if s["step"] == STAGE_STEP), None)
        if stage is None or not stage.get("argv"):
            raise IntegrityRefused("stage_step_missing", f"the plan has no {STAGE_STEP} vector")
        self._stage_argv = tuple(stage["argv"])
        self._steps = steps
        self.stage_started = False
        self.evidence: dict[str, dict[str, Any]] = {}
        self.problems: list[dict[str, str]] = []

    def _digest(self, phase: str) -> bool:
        step = self._steps[phase]
        record: dict[str, Any] = {"phase": phase, "step": step["step"], "raw": b"", "document": None, "returncode": None, "problems": []}
        self.evidence[phase] = record
        try:
            result = pr.PlanBoundRunner.run(self, step["argv"], step["timeout_s"], step["cwd"])
            record.update(raw=bytes(result.stdout or b""), returncode=result.returncode, timed_out=bool(result.timed_out), stderr=bytes(result.stderr or b""))
            if result.timed_out:
                record["problems"].append({"code": "digest_timed_out", "message": f"exceeded {step['timeout_s']} s"})
            document = parse_evidence(result.stdout)
            record["document"] = document
            record["problems"] += check_evidence(document, expected_request=step["request"], addendum=self.addendum)
            if result.returncode != 0:
                record["problems"].append({"code": "digest_tool_exit_nonzero", "message": f"exit code {result.returncode}"})
            if not record["problems"]:
                if phase == "pre_workload":
                    record["problems"] += check_pre_workload(document, self.addendum)
                elif "pre_workload" in self.evidence and self.evidence["pre_workload"]["document"] and not self.evidence["pre_workload"]["problems"]:
                    record["problems"] += check_staged_unchanged(self.evidence["pre_workload"]["document"], document)
                else:
                    record["problems"].append({"code": "pre_workload_evidence_missing", "message": "the staged files cannot be shown unchanged without a verified pre-workload digest"})
        except (IntegrityRefused, pr.RunnerRefused, pr.TransportDisabled, pr.ArgvRefused, pm.PathMappingError) as exc:
            record["problems"].append({"code": getattr(exc, "code", type(exc).__name__), "message": str(exc)})
        self.problems += [{"phase": phase, **p} for p in record["problems"]]
        return not record["problems"]

    def run(self, argv: Sequence[str], timeout_s: float, cwd: Any) -> be.RunResult:
        result = super().run(argv, timeout_s, cwd)
        if tuple(argv) != self._stage_argv:
            return result
        self.stage_started = True
        if result.returncode != 0 or result.timed_out:
            return result
        if self._digest("pre_workload"):
            return result
        note = ("\n[a0u] pre-workload integrity digest FAILED: " + "; ".join(f"{p['code']}" for p in self.evidence["pre_workload"]["problems"])).encode("utf-8")
        return be.RunResult(EXIT_PRE_FAILED, result.stdout, (result.stderr or b"") + note, False, result.started_utc, result.ended_utc)

    def worker_root_flat_map(self) -> dict[str, str]:
        """The post-workload digest as the flat ``{path: sha256}`` the A0s manifest uses. ``{}`` ONLY when nothing was ever staged; any failure raises."""
        if not self.stage_started:
            return {}
        if not self._digest("post_workload"):
            raise IntegrityRefused("post_workload_digest_failed", "; ".join(f"{p['code']}: {p['message']}" for p in self.evidence["post_workload"]["problems"]))
        return td.flat_file_map(self.evidence["post_workload"]["document"])


def make_hash_tree(runner: IntegrityBoundRunner, plan: Mapping[str, Any]) -> Callable[[str], dict[str, str]]:
    out_dir, worker_root = str(Path(plan["layout"]["output_dir"])), plan["mounts"]["run_dir_host"]

    def hash_tree(root: str) -> dict[str, str]:
        if str(root) == out_dir:
            return host_hash_tree(root)
        if str(root) == worker_root:
            return runner.worker_root_flat_map()
        raise IntegrityRefused("unplanned_root", f"{root!r} is neither the output directory nor the worker evidence root of the plan")
    return hash_tree


# ------------------------------------------------------------------------------------------------ verdict and wiring
def build_verdict(plan: Mapping[str, Any], addendum: Mapping[str, Any], runner: IntegrityBoundRunner, status: Mapping[str, Any], *, authorization_reference: str, manifest_present: bool) -> dict[str, Any]:
    klass = evidence_class(runner.kind)
    problems = list(runner.problems)
    integrity_errors = [e for e in status.get("errors", []) if e.get("step") == "integrity"]
    if integrity_errors:
        problems += [{"phase": "integrity_step", "code": "integrity_step_error", "message": str(e.get("reason"))} for e in integrity_errors]
    if runner.stage_started and not manifest_present:
        problems.append({"phase": "integrity_step", "code": "integrity_manifest_missing", "message": "execute_bounded wrote no integrity manifest"})
    if not runner.stage_started:
        verdict = V_NA
    elif problems:
        verdict = V_FAILED
    else:
        verdict = V_REAL if klass == "REAL_WSL_OBSERVATION" else V_SIM
    phases = {}
    for phase in td.PHASES:
        rec = runner.evidence.get(phase)
        doc = rec["document"] if rec else None
        phases[phase] = {"attempted": rec is not None, "ok": bool(rec) and not rec["problems"], "returncode": rec["returncode"] if rec else None,
                         "tree_digest": ((doc or {}).get("tree") or {}).get("tree_digest"), "document_sha256": (doc or {}).get("document_sha256"),
                         "problems": rec["problems"] if rec else []}
    return {"schema": VERDICT_SCHEMA, "version": INTEGRITY_VERSION, "run_id": plan["run_id"], "plan_sha256": plan["plan_sha256"], "addendum_sha256": addendum["addendum_sha256"],
            "authorization_reference": authorization_reference, "verdict": verdict, "evidence_class": klass, "gate_satisfied": verdict == V_REAL, "stage_started": runner.stage_started,
            "phases": phases, "problems": problems, "a0s_status": status.get("status"),
            "note": "gate_satisfied is True only for REAL_WSL_OBSERVATION evidence; simulated evidence never satisfies a live gate; the A0s run status alone is not the integrity gate"}


def _persist(out: Path, runner: IntegrityBoundRunner, verdict_doc: Mapping[str, Any]) -> None:
    for phase, rel in (("pre_workload", PRE_FILE), ("post_workload", POST_FILE)):
        rec = runner.evidence.get(phase)
        if rec is not None:
            be._create_new(out / rel, rec["raw"] or b"")
    be._create_new(out / VERDICT_FILE, (json.dumps(verdict_doc, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def execute_with_integrity(plan: Mapping[str, Any], auth: Mapping[str, Any], record: Mapping[str, Any], addendum: Mapping[str, Any], transport: Any, deps_parts: Mapping[str, Any], *,
                           repo_root: Any, expected_category: str, allow_synthetic: bool) -> dict[str, Any]:
    """``process_runner.execute_through_adapter`` plus the X13 repair. ``deps_parts`` supplies ``now_utc``, ``wall_ns``, ``mono_ns`` and ``read_file``; it may NOT supply ``hash_tree`` (the
    dispatch is built here so the worker root can only be hashed inside the distribution). A ``process`` transport is refused here, by ``process_runner`` and by ``execute_bounded``."""
    if "hash_tree" in deps_parts:
        raise be.LaunchRefused("deps_parts may not supply hash_tree: the X13 dispatch is built by wsl_integrity")
    ext = auth.get("a0t") if isinstance(auth.get("a0t"), Mapping) else {}
    try:
        description = pr.describe_plan_invocations(plan, distro=str(ext.get("distro")), mount_root=str(ext.get("automount_root")), path_record=record)
    except (pm.PathMappingError, pr.ArgvRefused, pr.RunnerRefused) as exc:
        description = {"error": f"{type(exc).__name__}: {exc}"}
    base = pr.verify_a0t_authorization(auth, plan, record, description, repo_root=repo_root, allow_synthetic=allow_synthetic, expected_category=expected_category)
    if not base["valid"]:
        raise be.LaunchRefused("the A0t authorization does not verify", base["findings"])
    extension = verify_a0u_authorization(auth, plan, addendum, allow_synthetic=allow_synthetic)
    if not extension["valid"]:
        raise be.LaunchRefused("the A0u integrity authorization does not verify", extension["findings"])
    problems = verify_addendum(addendum, plan, record, repo_root=Path(repo_root))
    if problems:
        raise be.LaunchRefused("the integrity addendum is not intact", problems)
    if getattr(transport, "kind", None) == "process" or ext.get("process_transport") == pr.TRANSPORT_WSL:
        raise be.LaunchRefused("the real WSL process transport is disabled in this version; nothing was started")
    writes = pr.check_writes_isolated(plan, mount_root=ext["automount_root"])
    if writes:
        raise be.LaunchRefused("a planned write lies outside the approved run directories", writes)
    runner = IntegrityBoundRunner(plan, transport, addendum=addendum, distro=ext["distro"], mount_root=ext["automount_root"], max_process_lifetime_s=ext["max_process_lifetime_s"])
    deps = be.Deps(runner=runner, hash_tree=make_hash_tree(runner, plan), **dict(deps_parts))
    out = Path(plan["layout"]["output_dir"])
    reference = auth["a0u"]["authorization_reference"]
    try:
        status = be.execute_bounded(plan, auth, deps, repo_root=repo_root, allow_synthetic=allow_synthetic)
    except BaseException:
        if out.exists():                                        # the interruption or refusal still leaves what was digested, with a verdict, before it propagates
            try:
                _persist(out, runner, build_verdict(plan, addendum, runner, {"status": "INTERRUPTED", "errors": []}, authorization_reference=reference, manifest_present=False))
            except Exception:                                   # noqa: BLE001 - never hide the primary outcome
                pass
        raise
    manifest_present = (out / plan["layout"]["host_evidence"]["integrity_manifest"]).is_file()
    verdict_doc = build_verdict(plan, addendum, runner, status, authorization_reference=reference, manifest_present=manifest_present)
    _persist(out, runner, verdict_doc)
    result = dict(status)
    result["runner_log"] = list(runner.log)
    result["integrity"] = {"verdict": verdict_doc["verdict"], "evidence_class": verdict_doc["evidence_class"], "gate_satisfied": verdict_doc["gate_satisfied"], "file": VERDICT_FILE}
    result["live_completion"] = bool(status.get("status") == "COMPLETED" and verdict_doc["gate_satisfied"])
    return result
