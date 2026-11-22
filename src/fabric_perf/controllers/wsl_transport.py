"""Real WSL process transport, operation-scoped and doubly gated (task A0v, 2026-11-22). OFFLINE: nothing in this version can start ``wsl.exe``.

What A0t/A0u left open. ``process_runner.DisabledWslTransport`` refused everything, so the intended launcher could not be inspected or tested. This module supplies the launcher
(:class:`WslProcessTransport`) behind a **spawn permit**. A permit is the only way to construct a transport, and it carries ONE operation category and the exact vectors that category may start.

Operation categories (separate permits; no generic "run a command" category exists):

* ``wsl_clock_sampling``       the clock probe vector of a clock-only plan (``wsl.exe -d <distro> --exec date +%s%N``), nothing else.
* ``wsl_readonly_discovery``   the read-only discovery vectors D1-D6 of ``runtime_discovery`` (container identity and mount observation), nothing else.
* ``wsl_x13_directory_hashing`` the two WSL-side digest vectors of the A0u integrity addendum, nothing else.
* ``wsl_bounded_caliper``      the vectors of an A0s plan, including the one transaction-producing step; needs its own transaction authorization block in the record.

A permit for one category can never start another category's vector (the vector sets are disjoint and every start is checked against the permit's own set), so a clock approval cannot start Caliper,
a discovery approval cannot create a channel, and a hash approval cannot run a workload. ``RoutingTransport`` lets a future bounded run hold SEVERAL permits (each from its own record) and routes each
vector to the transport whose permit lists it; a vector no permit lists is refused.

Two permit modes:

* ``fixture``: harmless LOCAL commands chosen by an injected ``command_for``. A fixture permit refuses ``wsl.exe``, shells, Docker, Caliper and Fabric tools as its executable. It is for tests only.
* ``real``: built only by :func:`issue_real_permit` from an operation-specific activation record that verifies, AND only while ``REAL_SPAWN_COMPILED_IN`` is True. That constant is False in this version and
  changes only by a reviewed source edit (this file's hash is in the candidate manifest). No environment variable, flag, command-line option or Boolean in a record turns anything on. Even then
  the existing executors (``bounded_execution.execute_bounded``, ``process_runner.execute_through_adapter``, ``wsl_integrity.execute_with_integrity``, ``clock_only_round.execute_clock_only``)
  still refuse a ``process`` transport, and the evidence class stays SIMULATED, until their own switches (``process_runner.REAL_TRANSPORT_ENABLED``, ``bounded_execution.REAL_LAUNCH_ENABLED``,
  ``wsl_clock_probe.LIVE_CLOCK_SAMPLING_ENABLED``) and checks are changed by separate reviewed edits. The keys are independent and none of them is held by this module; see
  :func:`describe_activation` for the list that applies to a category.

Process handling: structured vectors only (``process_runner.validate_argv`` is applied again here), ``shell=False``, no stdin, bounded output capture, one new process group per child (POSIX) or one new
process group (Windows), termination only of children this transport started, no retry anywhere, stdout, stderr, exit status, timeout and interruption preserved as evidence.

What this module does not claim: that ``wsl.exe`` behaves as described, that termination reaches processes inside the distribution (Windows termination ends the ``wsl.exe`` process; descendants inside
the distribution are UNVERIFIED), or that any vector works on the target machine.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import bounded_execution as be
from . import config_inspection as ci
from . import path_mapping as pm
from . import prelive_integrity as pi
from . import process_runner as pr

TRANSPORT_VERSION = "wsl_transport 0.1 (A0v, 2026-11-22)"
REAL_SPAWN_COMPILED_IN = False          # key 2 of 3. A reviewed source edit, never a flag, record or environment variable. No code in this repository sets it.
ACTIVATION_SCHEMA = "a0v_wsl_operation_activation/1"
EVIDENCE_SCHEMA = "a0v_wsl_transport_evidence/1"
ARCHITECTURE = "WINDOWS_HOST_ORCHESTRATION_WITH_WSL_SIDE_HASHING"

OPC_CLOCK = "wsl_clock_sampling"
OPC_DISCOVERY = "wsl_readonly_discovery"
OPC_X13 = "wsl_x13_directory_hashing"
OPC_CALIPER = "wsl_bounded_caliper"
CATEGORIES = (OPC_CLOCK, OPC_DISCOVERY, OPC_X13, OPC_CALIPER)
READ_ONLY = ("read_only_observation",)
CATEGORY_RULES: dict[str, dict[str, Any]] = {
    OPC_CLOCK: {"classes": READ_ONLY, "max_active": 1, "transaction_producing": False},
    OPC_DISCOVERY: {"classes": READ_ONLY, "max_active": 1, "transaction_producing": False},
    OPC_X13: {"classes": READ_ONLY, "max_active": 1, "transaction_producing": False},
    OPC_CALIPER: {"classes": ("read_only_observation", "local_staging", "transaction_producing"), "max_active": 2, "transaction_producing": True},
}
MODE_REAL, MODE_FIXTURE = "real", "fixture"
REAL_EXECUTABLE = re.compile(r"(?!.*\.\.)[A-Za-z]:\\(?:[A-Za-z0-9_. -]+\\)*[Ww][Ss][Ll]\.[Ee][Xx][Ee]")      # an explicit absolute Windows path ending in wsl.exe, no '..' anywhere
FORBIDDEN_FIXTURE_EXECUTABLES = frozenset({"wsl.exe", "wsl", "docker", "docker.exe", "caliper", "peer", "configtxlator", "orderer", "curl", "ssh", "kubectl"}) | pr.SHELL_NAMES
OWNER_FIELDS = ("distro", "executable", "max_process_lifetime_s", "max_active_processes", "max_capture_bytes", "candidate_manifest_sha256", "activation_reference", "real_spawn_authorized")
_SHA = re.compile(r"[0-9a-f]{64}")
TERMINATE_GRACE_S = 5.0


class ActivationRefused(ValueError):
    """The transport could not be activated. ``code`` is a stable machine-readable cause; ``findings`` lists every problem found. Nothing was started."""

    def __init__(self, code: str, message: str, findings: Sequence[Mapping[str, str]] = ()):
        super().__init__(f"{message} [{code}]")
        self.code, self.findings = code, list(findings)


# ------------------------------------------------------------------------------------------------ allowed vectors per category
@dataclass(frozen=True)
class AllowedVector:
    """One OS-level vector a permit lists, with its approved working directory, bound, class and use count."""

    step_id: str
    os_argv: tuple[str, ...]
    wsl_cwd: str | None
    timeout_s: float
    klass: str
    max_uses: int
    launch_mode: str = pr.LAUNCH_FOREGROUND

    def as_dict(self) -> dict[str, Any]:
        return {"step": self.step_id, "os_argv": list(self.os_argv), "wsl_cwd": self.wsl_cwd, "timeout_s": self.timeout_s, "class": self.klass, "max_uses": self.max_uses, "launch_mode": self.launch_mode}


def vectors_digest(allowed: Sequence[AllowedVector]) -> str:
    return hashlib.sha256(json.dumps(sorted((a.as_dict() for a in allowed), key=lambda d: json.dumps(d, sort_keys=True)), sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _as_vector(step_id: str, argv: Sequence[str], cwd: str | None, timeout_s: float, klass: str, uses: int, mode: str, distro: str, mount_root: str) -> AllowedVector:
    inv = pr.build_invocation(step_id, argv, cwd, timeout_s, launch_mode=mode, distro=distro, mount_root=mount_root)
    return AllowedVector(step_id, inv.os_argv, inv.wsl_cwd, float(timeout_s), klass, int(uses), mode)


def vectors_for_category(category: str, source: Mapping[str, Any], *, distro: str, mount_root: str, container_name: str | None = None, step_timeout_s: float | None = None) -> list[AllowedVector]:
    """The exact vector set of ONE category, derived only from the document that defines it (clock-only plan, discovery specification, integrity addendum or A0s plan)."""
    if category not in CATEGORIES:
        raise ActivationRefused("operation_category", f"{category!r} is not one of {list(CATEGORIES)}; there is no generic execution category")
    if not (isinstance(distro, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", distro)):
        raise ActivationRefused("distro", "the WSL distribution is required (no default)")
    root = pm.check_mount_root(mount_root)
    out: list[AllowedVector] = []
    if category == OPC_CLOCK:
        if source.get("schema") != "a0t_clock_only_plan/1":
            raise ActivationRefused("source_schema", "a clock-sampling permit is derived from an a0t_clock_only_plan/1")
        req = source["request"]
        out.append(_as_vector("K01+K03:clock_probe", source["probe"]["argv"], None, float(req["probe_timeout_s"]), "read_only_observation", 2 * int(req["samples_per_phase"]), pr.LAUNCH_FOREGROUND, distro, root))
    elif category == OPC_DISCOVERY:
        if source.get("schema") != "a0u_runtime_discovery_spec/1":
            raise ActivationRefused("source_schema", "a discovery permit is derived from an a0u_runtime_discovery_spec/1")
        if isinstance(step_timeout_s, bool) or not isinstance(step_timeout_s, (int, float)) or not 0 < step_timeout_s < float("inf"):
            raise ActivationRefused("timeout_required", "step_timeout_s must be a finite number > 0 (owner value, no default)")
        placeholder = "AUTHORIZED_CONTAINER_NAME_REQUIRED"
        for step in source["steps"]:
            argv = list(step["argv"])
            if placeholder in argv:
                if container_name is None:
                    continue                                                 # not listed: a vector with an unresolved container name can never be started
                if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", container_name):
                    raise ActivationRefused("container_name", f"{container_name!r} is not a valid Docker name")
                argv = [container_name if t == placeholder else t for t in argv]
            out.append(_as_vector(step["step"], argv, None, float(step_timeout_s), "read_only_observation", 1, pr.LAUNCH_FOREGROUND, distro, root))
    elif category == OPC_X13:
        if source.get("schema") != "a0u_integrity_addendum/1":
            raise ActivationRefused("source_schema", "an X13 permit is derived from an a0u_integrity_addendum/1")
        for step in source["steps"]:
            if step["kind"] == "integrity_wsl_digest":
                out.append(_as_vector(step["step"], step["argv"], step["cwd"], float(step["timeout_s"]), "read_only_observation", int(step["max_uses"]), pr.LAUNCH_FOREGROUND, distro, root))
    else:
        if source.get("schema") != be.PLAN_SCHEMA:
            raise ActivationRefused("source_schema", "a Caliper permit is derived from an a0s_execution_plan/1")
        for entry in pr.planned_vectors(source):
            inv = pr.build_invocation(entry.step_id, entry.argv, entry.cwd, entry.timeout_s, launch_mode=entry.launch_mode, distro=distro, mount_root=root)
            out.append(AllowedVector(entry.step_id, inv.os_argv, inv.wsl_cwd, float(entry.timeout_s), entry.klass, entry.max_uses, entry.launch_mode))
        if not any(v.klass == "transaction_producing" for v in out):
            raise ActivationRefused("no_transaction_step", "the plan has no transaction-producing vector")
    if not out:
        raise ActivationRefused("empty_vector_set", f"category {category} yields no vector")
    seen: dict[tuple[str, ...], str] = {}
    for vector in out:
        if vector.os_argv in seen:
            raise ActivationRefused("duplicate_vector", f"{vector.step_id} repeats {seen[vector.os_argv]}")
        seen[vector.os_argv] = vector.step_id
    return out


# ------------------------------------------------------------------------------------------------ the operation-specific activation record
def activation_template(category: str, allowed: Sequence[AllowedVector], binding: Mapping[str, Any]) -> dict[str, Any]:
    """The activation record for ONE category with every owner field UNSET (None). Values the tool can copy (vector digest, binding) are copied for review; copying is not approving."""
    if category not in CATEGORIES:
        raise ActivationRefused("operation_category", f"{category!r} is not one of {list(CATEGORIES)}")
    record: dict[str, Any] = {
        "schema": ACTIVATION_SCHEMA, "version": TRANSPORT_VERSION, "synthetic": False, "operation_category": category, "architecture": ARCHITECTURE,
        "architecture_status": "ARCHITECTURE_APPROVED_RUNTIME_NOT_VERIFIED", "binding": json.loads(json.dumps(dict(binding))),
        "allowed_vectors": [a.as_dict() for a in allowed], "allowed_vectors_sha256": vectors_digest(allowed),
        "transaction_authorization": ({"authorized": None, "max_transactions": None, "authorization_reference": None} if CATEGORY_RULES[category]["transaction_producing"] else None),
        **{key: None for key in OWNER_FIELDS},
        "approval": {key: None for key in be.APPROVAL_KEYS},
        "note": "TEMPLATE. NOT APPROVED. Every null is an owner decision. The approval document named in approval.record_path must contain this record's record_sha256 on its own (the pin). "
                "A valid record is not a launch: REAL_SPAWN_COMPILED_IN must also be True and the executor gate must accept a process transport.",
    }
    record["record_sha256"] = be._digest(record, drop=("record_sha256", "approval"))
    return record


def verify_activation_record(record: Any, *, category: str, allowed: Sequence[AllowedVector], binding: Mapping[str, Any], expected_candidate_manifest_sha256: str | None,
                             allow_synthetic: bool = False) -> list[dict[str, str]]:
    """Every problem with an activation record against what it must bind. Empty = the record verifies (still not a launch). Never reads anything but the approval document named in the record."""
    findings: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        findings.append({"code": code, "message": message})

    if not isinstance(record, Mapping):
        return [{"code": "activation_record_missing", "message": "no record"}]
    if record.get("schema") != ACTIVATION_SCHEMA:
        bad("activation_schema", f"schema {record.get('schema')!r}")
    if record.get("operation_category") != category:
        bad("operation_category_mismatch", f"record authorizes {record.get('operation_category')!r}; this transport is for {category!r}")
    if record.get("synthetic") is True and not allow_synthetic:
        bad("synthetic_record_for_real_transport", "a synthetic record can never activate the real transport")
    if record.get("synthetic") is not False and record.get("synthetic") is not True:
        bad("synthetic_flag", "synthetic must be an explicit true or false")
    if record.get("architecture") != ARCHITECTURE:
        bad("architecture", f"{record.get('architecture')!r}; only {ARCHITECTURE!r} is implemented")
    unset = [k for k in OWNER_FIELDS if record.get(k) in (None, "")]
    if unset:
        bad("owner_field_unset", f"unset owner fields: {unset}")
    if not (record.get("real_spawn_authorized") is True):
        bad("real_spawn_not_authorized", "real_spawn_authorized must be an explicit true set by the owner for THIS record")
    if dict(record.get("binding") or {}) != dict(binding):
        bad("binding_mismatch", f"the record binds {record.get('binding')!r}; this run is {dict(binding)!r}")
    if record.get("allowed_vectors_sha256") != vectors_digest(allowed) or record.get("allowed_vectors") != [a.as_dict() for a in allowed]:
        bad("vector_set_mismatch", "the record binds other vectors than the ones this operation would start")
    if record.get("record_sha256") != be._digest(record, drop=("record_sha256", "approval")):
        bad("record_hash", "record_sha256 does not match the record's content")
    manifest = record.get("candidate_manifest_sha256")
    if expected_candidate_manifest_sha256 is None:
        bad("manifest_expectation_missing", "the caller supplied no candidate-manifest hash to compare")
    elif manifest != expected_candidate_manifest_sha256:
        bad("candidate_manifest_mismatch", "the record binds another candidate manifest than the one verified")
    if not (isinstance(manifest, str) and _SHA.fullmatch(manifest)):
        bad("candidate_manifest_hash", "candidate_manifest_sha256 must be 64 lowercase hex characters")
    distro = record.get("distro")
    if not (isinstance(distro, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", distro)):
        bad("distro", "the WSL distribution is required (no default)")
    elif any(len(v.os_argv) < 3 or v.os_argv[:2] != ("wsl.exe", "-d") or v.os_argv[2] != distro for v in allowed):
        bad("distro_mismatch", "an allowed vector is not 'wsl.exe -d <the record's distribution> ...'")
    exe = record.get("executable")
    if not (isinstance(exe, str) and REAL_EXECUTABLE.fullmatch(exe)):
        bad("executable", "the executable must be an explicit absolute Windows path to wsl.exe (no search path, no default)")
    for key in ("max_process_lifetime_s", "max_capture_bytes"):
        value = record.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < float("inf"):
            bad(key, f"{key} must be a finite number > 0")
    active = record.get("max_active_processes")
    if isinstance(active, bool) or not isinstance(active, int) or not 1 <= active <= CATEGORY_RULES.get(category, {"max_active": 1})["max_active"]:
        bad("max_active_processes", f"must be an integer in 1..{CATEGORY_RULES.get(category, {'max_active': 1})['max_active']} for {category}")
    if isinstance(record.get("max_process_lifetime_s"), (int, float)) and not isinstance(record.get("max_process_lifetime_s"), bool):
        longest = max((v.timeout_s for v in allowed), default=0.0)
        if record["max_process_lifetime_s"] < longest:
            bad("lifetime_below_vector_timeout", f"the lifetime limit {record['max_process_lifetime_s']} is shorter than the longest vector bound {longest}")
    tx = record.get("transaction_authorization")
    if CATEGORY_RULES.get(category, {}).get("transaction_producing"):
        if not (isinstance(tx, Mapping) and tx.get("authorized") is True and isinstance(tx.get("max_transactions"), int) and not isinstance(tx.get("max_transactions"), bool)
                and tx["max_transactions"] > 0 and tx.get("authorization_reference") not in (None, "")):
            bad("transaction_authorization_missing", "a Caliper permit needs its own transaction authorization: authorized true, max_transactions > 0 and an authorization_reference")
    elif tx is not None:
        bad("transaction_authorization_not_permitted", f"a {category} record carries no transaction authorization; a transaction-producing operation is a different, separately approved category")
    approval = record.get("approval")
    if not isinstance(approval, Mapping) or any(approval.get(k) in (None, "") for k in be.APPROVAL_KEYS):
        bad("approval_unset", f"every approval field {list(be.APPROVAL_KEYS)} must be set by the owner")
    else:
        if approval["status"] != "APPROVED_BY_OWNER":
            bad("approval_status", f"approval.status is {approval['status']!r}")
        try:
            ci._aware_utc(approval["approved_utc"], "approval.approved_utc")
        except ci.InspectionError as exc:
            bad("approval_time", str(exc))
        document = Path(str(approval["record_path"]))
        if not (isinstance(approval["record_sha256"], str) and _SHA.fullmatch(approval["record_sha256"])):
            bad("approval_record_hash", "approval.record_sha256 must be 64 lowercase hex characters")
        elif not document.is_file():
            bad("approval_record_unreadable", f"{document} is not a readable file")
        else:
            try:
                actual = pi.sha256_file(document)
                text = document.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                actual, text = "", ""
                bad("approval_record_unreadable", str(exc))
            if actual and actual != approval["record_sha256"]:
                bad("approval_record_hash", "the approval document's hash differs from the one recorded")
            if text and str(record.get("record_sha256")) not in text:
                bad("approval_document_does_not_pin_record", "the approval document does not contain this record's record_sha256, so it does not approve this record")
    return findings


# ------------------------------------------------------------------------------------------------ permits
@dataclass(frozen=True)
class SpawnPermit:
    """What a transport may do. Built only by :func:`issue_real_permit` or :func:`issue_fixture_permit`."""

    mode: str
    category: str
    executable: str
    distro: str | None
    allowed: tuple[AllowedVector, ...]
    max_lifetime_s: float
    max_active: int
    max_capture_bytes: int
    record_sha256: str | None
    binding: Mapping[str, Any] = field(default_factory=dict)
    _origin: Any = field(default=None, repr=False)


_ISSUER = object()          # a private token the two issuers attach; it guards against accidental direct construction of a permit (it is not a security boundary against deliberate code)


def _refuse_wrong_classes(category: str, allowed: Sequence[AllowedVector]) -> None:
    """A permit never lists a vector whose class its category does not allow (a read-only category cannot list a staging or transaction-producing vector)."""
    permitted = CATEGORY_RULES[category]["classes"]
    wrong = [a.step_id for a in allowed if a.klass not in permitted]
    if wrong:
        raise ActivationRefused("class_not_permitted_in_category", f"{category} may not list vector(s) {wrong}: their class is outside {list(permitted)}")


def issue_real_permit(record: Any, *, category: str, allowed: Sequence[AllowedVector], binding: Mapping[str, Any], expected_candidate_manifest_sha256: str | None) -> SpawnPermit:
    """A real permit, or :class:`ActivationRefused`. Requires BOTH a verifying operation-specific record and ``REAL_SPAWN_COMPILED_IN``. A synthetic record is never accepted."""
    findings = verify_activation_record(record, category=category, allowed=allowed, binding=binding, expected_candidate_manifest_sha256=expected_candidate_manifest_sha256, allow_synthetic=False)
    if findings:
        raise ActivationRefused(findings[0]["code"], f"the activation record does not verify ({len(findings)} finding(s))", findings)
    _refuse_wrong_classes(category, allowed)
    if not REAL_SPAWN_COMPILED_IN:
        raise ActivationRefused("real_spawn_not_compiled_in", "the activation record verifies, but real process start-up is not compiled in (wsl_transport.REAL_SPAWN_COMPILED_IN is False); nothing was started")
    return SpawnPermit(MODE_REAL, category, record["executable"], record["distro"], tuple(allowed), float(record["max_process_lifetime_s"]), int(record["max_active_processes"]),
                       int(record["max_capture_bytes"]), record["record_sha256"], dict(binding), _origin=_ISSUER)


def issue_fixture_permit(category: str, allowed: Sequence[AllowedVector], *, executable: str, max_lifetime_s: float, max_active: int | None = None, max_capture_bytes: int = 1 << 20,
                         binding: Mapping[str, Any] | None = None, acknowledge_fixture_only: bool = False) -> SpawnPermit:
    """A permit for HARMLESS LOCAL commands (tests). It refuses ``wsl.exe``, shells, Docker, Caliper and Fabric tools as the executable and needs an explicit acknowledgement."""
    if not acknowledge_fixture_only:
        raise ActivationRefused("fixture_not_acknowledged", "a fixture permit is for synthetic tests only and must be requested explicitly")
    if category not in CATEGORIES:
        raise ActivationRefused("operation_category", f"{category!r} is not one of {list(CATEGORIES)}")
    base = re.split(r"[\\/]", str(executable))[-1].lower()
    if base in FORBIDDEN_FIXTURE_EXECUTABLES or base.startswith("wsl"):
        raise ActivationRefused("fixture_executable_forbidden", f"a fixture permit may not name {base!r}")
    if isinstance(max_lifetime_s, bool) or not isinstance(max_lifetime_s, (int, float)) or max_lifetime_s <= 0:
        raise ActivationRefused("lifetime_unbounded", "max_lifetime_s must be a number > 0")
    _refuse_wrong_classes(category, allowed)
    return SpawnPermit(MODE_FIXTURE, category, str(executable), None, tuple(allowed), float(max_lifetime_s), int(max_active or CATEGORY_RULES[category]["max_active"]),
                       int(max_capture_bytes), None, dict(binding or {}), _origin=_ISSUER)


# ------------------------------------------------------------------------------------------------ supervised child process
@dataclass
class _Child:
    index: int
    vector: AllowedVector
    proc: Any
    argv: tuple[str, ...]
    started_mono: float
    started_utc: str
    stdout: bytearray = field(default_factory=bytearray)
    stderr: bytearray = field(default_factory=bytearray)
    truncated: bool = False
    readers: list = field(default_factory=list)
    finished: bool = False
    timed_out: bool = False
    interrupted: bool = False
    termination: str = "none"


class WslProcessTransport:
    """``kind`` ``process`` (real permit) or ``fixture`` (fixture permit). Implements ``run/start/finish/terminate`` for ``process_runner.PlanBoundRunner``."""

    def __init__(self, permit: SpawnPermit, *, popen: Callable[..., Any] | None = None, command_for: Callable[[pr.Invocation], Sequence[str]] | None = None,
                 now_utc: Callable[[], datetime] | None = None):
        if not isinstance(permit, SpawnPermit) or permit._origin is not _ISSUER:
            raise ActivationRefused("permit_required", "a transport is built only from a permit issued by this module")
        if permit.mode == MODE_FIXTURE and command_for is None:
            raise ActivationRefused("fixture_command_required", "a fixture transport needs the injected command_for")
        if permit.mode == MODE_REAL and command_for is not None:
            raise ActivationRefused("command_for_on_real_transport", "a real transport never rewrites a vector")
        self.permit = permit
        self.kind = "process" if permit.mode == MODE_REAL else "fixture"
        self._table = {v.os_argv: v for v in permit.allowed}
        self._popen = popen
        self._command_for = command_for
        self._now = now_utc or (lambda: datetime.now(timezone.utc))
        self._uses: dict[tuple[str, ...], int] = {}
        self._children: dict[int, _Child] = {}
        self._n = 0
        self.evidence: list[dict[str, Any]] = []
        self.refusals: list[dict[str, str]] = []
        self._captures: dict[int, tuple[bytes, bytes]] = {}

    # -- admission
    def problems(self, inv: pr.Invocation) -> list[dict[str, str]]:
        """Why this transport would refuse ``inv`` (empty = it would accept). No side effect; used by the router."""
        found: list[dict[str, str]] = []

        def bad(code: str, message: str) -> None:
            found.append({"code": code, "message": message})

        if not isinstance(inv, pr.Invocation):
            return [{"code": "not_an_invocation", "message": f"{type(inv).__name__}"}]
        if not isinstance(inv.os_argv, (tuple, list)):
            return [{"code": "argv_not_a_vector", "message": "a command must be a tuple or list of tokens, never a single string"}]
        try:
            pr.validate_argv(list(inv.os_argv))
        except pr.ArgvRefused as exc:
            bad(exc.code, str(exc))
            return found
        if inv.os_argv and re.split(r"[\\/]", str(inv.os_argv[0]))[-1].lower() == "wsl.exe" and "--exec" in inv.os_argv:
            try:                                                   # the program run INSIDE the distribution is checked as a vector of its own (validate_argv looks only at the first token)
                pr.validate_argv(list(inv.os_argv[list(inv.os_argv).index("--exec") + 1:]))
            except pr.ArgvRefused as exc:
                bad(exc.code, f"inside the distribution: {exc}")
                return found
        vector = self._table.get(tuple(inv.os_argv))
        if vector is None:
            bad("vector_not_permitted", f"the vector is not one of the {len(self._table)} vector(s) of category {self.permit.category}")
            return found
        if vector.klass not in CATEGORY_RULES[self.permit.category]["classes"]:
            bad("class_not_permitted", f"class {vector.klass!r} is not allowed in {self.permit.category}")
        if inv.wsl_cwd != vector.wsl_cwd:
            bad("cwd_not_approved", f"working directory {inv.wsl_cwd!r} differs from the approved {vector.wsl_cwd!r}")
        if inv.launch_mode != vector.launch_mode:
            bad("launch_mode_not_approved", f"{inv.launch_mode} requested, {vector.launch_mode} approved")
        timeout = inv.timeout_s
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
            bad("timeout_missing", "every process needs a finite timeout > 0")
        elif timeout > vector.timeout_s or timeout > self.permit.max_lifetime_s:
            bad("timeout_exceeds_bound", f"timeout {timeout} exceeds min(vector bound {vector.timeout_s}, process lifetime {self.permit.max_lifetime_s})")
        if self._uses.get(tuple(inv.os_argv), 0) >= vector.max_uses:
            bad("no_retry", f"{vector.step_id} was already started {self._uses.get(tuple(inv.os_argv), 0)} time(s) (limit {vector.max_uses}); nothing is retried")
        if sum(1 for c in self._children.values() if not c.finished) >= self.permit.max_active:
            bad("active_operation_limit", f"{self.permit.max_active} process(es) already active")
        if self.permit.mode == MODE_REAL:
            if not REAL_SPAWN_COMPILED_IN:
                bad("real_spawn_not_compiled_in", "real process start-up is not compiled in")
            if re.split(r"[\\/]", inv.os_argv[0])[-1].lower() != "wsl.exe":
                bad("executable_not_wsl", "a real transport starts wsl.exe only")
        return found

    def permits(self, inv: pr.Invocation) -> bool:
        """True when this transport's permit LISTS the vector (used for routing; other admission checks are separate)."""
        return isinstance(inv, pr.Invocation) and tuple(inv.os_argv) in self._table

    def _admit(self, inv: pr.Invocation) -> AllowedVector:
        found = self.problems(inv)
        if found:
            self.refusals.append({"step": getattr(inv, "step_id", "?"), **found[0]})
            raise pr.RunnerRefused(found[0]["code"], found[0]["message"])
        return self._table[tuple(inv.os_argv)]

    # -- process start-up
    def _spawn(self, inv: pr.Invocation, vector: AllowedVector) -> _Child:
        if self.permit.mode == MODE_REAL:
            if not REAL_SPAWN_COMPILED_IN:                                            # re-checked at the moment of use
                raise ActivationRefused("real_spawn_not_compiled_in", "real process start-up is not compiled in")
            argv = (self.permit.executable,) + tuple(inv.os_argv[1:])
        else:
            argv = tuple(self._command_for(inv))                                      # type: ignore[misc]
            if not argv or re.split(r"[\\/]", argv[0])[-1].lower() in FORBIDDEN_FIXTURE_EXECUTABLES:
                raise pr.RunnerRefused("fixture_command_forbidden", "the fixture command names a forbidden executable")
        popen = self._popen or subprocess.Popen
        kwargs: dict[str, Any] = {"stdin": subprocess.DEVNULL, "stdout": subprocess.PIPE, "stderr": subprocess.PIPE, "shell": False, "close_fds": True}
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        else:
            kwargs["start_new_session"] = True
        self._uses[tuple(inv.os_argv)] = self._uses.get(tuple(inv.os_argv), 0) + 1          # the ATTEMPT counts: a start that fails is never retried either
        self._n += 1
        started_utc = self._now().astimezone(timezone.utc).isoformat(timespec="milliseconds")
        try:
            proc = popen(list(argv), **kwargs)
        except OSError as exc:
            empty = hashlib.sha256(b"").hexdigest()
            self.evidence.append({"index": self._n, "step": vector.step_id, "category": self.permit.category, "mode": self.permit.mode, "spawned_argv": list(argv), "started_utc": started_utc,
                                  "ended_utc": started_utc, "duration_s": 0.0, "returncode": None, "timed_out": False, "interrupted": False, "termination": "none", "stdout_bytes": 0,
                                  "stdout_sha256": empty, "stderr_bytes": 0, "stderr_sha256": empty, "output_truncated": False, "capture_incomplete": False,
                                  "capture_limit_bytes": self.permit.max_capture_bytes, "retried": False, "spawn_error": f"{type(exc).__name__}: {exc}"})
            raise pr.RunnerRefused("spawn_failed", f"the process could not be started ({type(exc).__name__}); the attempt is counted and not retried") from exc
        child = _Child(self._n, vector, proc, argv, time.monotonic(), started_utc)
        self._children[child.index] = child
        for name in ("stdout", "stderr"):
            stream = getattr(proc, name, None)
            if stream is not None:
                thread = threading.Thread(target=self._drain, args=(child, name, stream), daemon=True)
                thread.start()
                child.readers.append(thread)
        return child

    def _drain(self, child: _Child, name: str, stream: Any) -> None:
        buffer = getattr(child, name)
        cap = self.permit.max_capture_bytes
        read = getattr(stream, "read1", None) or stream.read                              # read1 returns what is available; read(n) would hold output back until n bytes or EOF
        try:
            while True:
                chunk = read(65536)
                if not chunk:
                    return
                room = cap - len(buffer)
                if room > 0:
                    buffer.extend(chunk[:room])
                if len(chunk) > max(room, 0):
                    child.truncated = True                                            # keep draining so the child never blocks on a full pipe
        except (OSError, ValueError):
            return

    def _terminate_own(self, child: _Child, grace_s: float) -> str:
        """Terminate ONLY the child this transport started (its own process group on POSIX). Descendants inside the WSL distribution are not addressed and stay UNVERIFIED."""
        proc = child.proc
        if proc.poll() is not None:
            return "already_exited"
        method = "terminate"
        try:
            if os.name == "nt":
                ctrl_break = getattr(signal, "CTRL_BREAK_EVENT", None)
                if ctrl_break is not None:
                    try:
                        proc.send_signal(ctrl_break)
                        method = "ctrl_break"
                    except (OSError, ValueError):
                        proc.terminate()
                else:
                    proc.terminate()
            else:
                if os.getpgid(proc.pid) == proc.pid:                               # only a group this transport created
                    os.killpg(proc.pid, signal.SIGTERM)
                    method = "sigterm_group"
                else:
                    proc.terminate()
            try:
                proc.wait(timeout=grace_s)
                return method
            except subprocess.TimeoutExpired:
                pass
            if os.name != "nt" and os.getpgid(proc.pid) == proc.pid:
                os.killpg(proc.pid, signal.SIGKILL)
                method += "+sigkill_group"
            else:
                proc.kill()
                method += "+kill"
            proc.wait(timeout=grace_s)
        except (OSError, ProcessLookupError, subprocess.TimeoutExpired):
            method += "+error"
        return method

    def _collect(self, child: _Child, *, timed_out: bool, interrupted: bool, termination: str) -> be.RunResult:
        for thread in child.readers:
            thread.join(timeout=2.0)
        incomplete = any(thread.is_alive() for thread in child.readers)                   # a descendant still holds a pipe open: the capture may be missing its tail
        child.finished, child.timed_out, child.interrupted, child.termination = True, timed_out, interrupted, termination
        ended = self._now().astimezone(timezone.utc).isoformat(timespec="milliseconds")
        out, err = bytes(child.stdout), bytes(child.stderr)
        self._captures[child.index] = (out, err)
        code = child.proc.poll()
        self.evidence.append({"index": child.index, "step": child.vector.step_id, "category": self.permit.category, "mode": self.permit.mode, "spawned_argv": list(child.argv),
                              "started_utc": child.started_utc, "ended_utc": ended, "duration_s": round(time.monotonic() - child.started_mono, 6), "returncode": code,
                              "timed_out": timed_out, "interrupted": interrupted, "termination": termination, "stdout_bytes": len(out), "stdout_sha256": hashlib.sha256(out).hexdigest(),
                              "stderr_bytes": len(err), "stderr_sha256": hashlib.sha256(err).hexdigest(), "output_truncated": child.truncated,
                              "capture_incomplete": incomplete, "capture_limit_bytes": self.permit.max_capture_bytes, "retried": False})
        return be.RunResult(code, out, err, timed_out, child.started_utc, ended)

    def _wait(self, child: _Child, timeout_s: float) -> be.RunResult:
        """Wait for the child. A timeout or an interrupt terminates only this child and still returns / re-raises with the evidence preserved."""
        try:
            child.proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            method = self._terminate_own(child, TERMINATE_GRACE_S)
            return self._collect(child, timed_out=True, interrupted=False, termination=method)
        except BaseException:                                                          # KeyboardInterrupt, SystemExit: evidence first, then propagate
            method = self._terminate_own(child, TERMINATE_GRACE_S)
            self._collect(child, timed_out=False, interrupted=True, termination=method)
            raise
        return self._collect(child, timed_out=False, interrupted=False, termination="none")

    # -- the runner interface
    def run(self, inv: pr.Invocation) -> be.RunResult:
        vector = self._admit(inv)
        child = self._spawn(inv, vector)
        return self._wait(child, float(inv.timeout_s))

    def start(self, inv: pr.Invocation) -> int:
        vector = self._admit(inv)
        return self._spawn(inv, vector).index

    def finish(self, handle: int, timeout_s: float) -> be.RunResult:
        child = self._children.get(handle)
        if child is None or child.finished:
            raise pr.RunnerRefused("unknown_handle", "no such running process")
        lifetime_left = self.permit.max_lifetime_s - (time.monotonic() - child.started_mono)
        if lifetime_left <= 0:                                                          # the approved process lifetime is over: end it
            method = self._terminate_own(child, TERMINATE_GRACE_S)
            return self._collect(child, timed_out=True, interrupted=False, termination=method + "+lifetime")
        try:
            child.proc.wait(timeout=min(float(timeout_s), lifetime_left))
        except subprocess.TimeoutExpired:
            return be.RunResult(None, b"", b"", True)                                   # still running: the caller decides (terminate)
        except BaseException:
            method = self._terminate_own(child, TERMINATE_GRACE_S)
            self._collect(child, timed_out=False, interrupted=True, termination=method)
            raise
        return self._collect(child, timed_out=False, interrupted=False, termination="none")

    def terminate(self, handle: int, grace_s: float) -> be.RunResult:
        child = self._children.get(handle)
        if child is None:
            raise pr.RunnerRefused("unknown_handle", "only a process this transport started can be terminated")
        if child.finished:
            raise pr.RunnerRefused("already_finished", "that process was already collected")
        method = self._terminate_own(child, float(grace_s))
        return self._collect(child, timed_out=False, interrupted=False, termination=method)

    def shutdown(self) -> None:
        """Terminate every child of this transport that is still running (cleanup after an interruption or a test)."""
        for child in list(self._children.values()):
            if not child.finished:
                method = self._terminate_own(child, 1.0)
                self._collect(child, timed_out=False, interrupted=True, termination=method + "+shutdown")

    # -- evidence
    def evidence_document(self) -> dict[str, Any]:
        doc = {"schema": EVIDENCE_SCHEMA, "version": TRANSPORT_VERSION, "category": self.permit.category, "mode": self.permit.mode, "kind": self.kind, "permit_record_sha256": self.permit.record_sha256,
               "vectors_sha256": vectors_digest(self.permit.allowed), "evidence": list(self.evidence), "refusals": list(self.refusals), "real_process_started": self.permit.mode == MODE_REAL and bool(self.evidence),
               "descendants_in_distribution_terminated": "UNVERIFIED"}
        doc["document_sha256"] = be._digest(doc, drop=("document_sha256",))
        return doc

    def write_evidence(self, directory: Path) -> list[str]:
        """Exclusive-create ``evidence.json`` and one stdout and stderr file per child under ``directory`` (which may exist; no file is overwritten)."""
        directory = Path(directory)
        written = []
        for rel, data in [("evidence.json", (json.dumps(self.evidence_document(), indent=2, sort_keys=True) + "\n").encode("utf-8"))] + \
                         [(f"{index:03d}.{stream}", blob) for index, (out, err) in sorted(self._captures.items()) for stream, blob in (("stdout", out), ("stderr", err))]:
            be._create_new(directory / rel, data)
            written.append(rel)
        return written


class RoutingTransport:
    """Holds several single-category transports (each from its own record) and sends every vector to the one whose permit lists it. A vector no permit lists is refused."""

    def __init__(self, transports: Sequence[WslProcessTransport]):
        if not transports:
            raise ActivationRefused("no_transport", "at least one transport is required")
        kinds = {t.kind for t in transports}
        if len(kinds) != 1:
            raise ActivationRefused("mixed_transport_kinds", "real and fixture transports are never mixed")
        categories = [t.permit.category for t in transports]
        if len(set(categories)) != len(categories):
            raise ActivationRefused("duplicate_category", "one transport per operation category")
        self.kind = kinds.pop()
        self._transports = list(transports)
        self._handles: dict[Any, tuple[int, Any]] = {}
        self._n = 0

    def _select(self, inv: pr.Invocation) -> WslProcessTransport:
        owners = [t for t in self._transports if t.permits(inv)]
        if len(owners) > 1:                                       # a bounded run's own clock gate is also a clock-sampling vector: the narrower, read-only category owns it
            narrower = [t for t in owners if t.permit.category != OPC_CALIPER]
            owners = narrower if narrower else owners
        if len(owners) != 1:
            raise pr.RunnerRefused("vector_not_permitted" if not owners else "vector_in_several_categories", f"{len(owners)} permit(s) list this vector; exactly one must")
        return owners[0]

    def run(self, inv: pr.Invocation) -> be.RunResult:
        return self._select(inv).run(inv)

    def start(self, inv: pr.Invocation) -> Any:
        transport = self._select(inv)
        handle = transport.start(inv)
        self._n += 1
        key = ("route", self._n)
        self._handles[key] = (self._transports.index(transport), handle)
        return key

    def finish(self, handle: Any, timeout_s: float) -> be.RunResult:
        index, inner = self._handles[handle]
        return self._transports[index].finish(inner, timeout_s)

    def terminate(self, handle: Any, grace_s: float) -> be.RunResult:
        index, inner = self._handles[handle]
        return self._transports[index].terminate(inner, grace_s)

    def shutdown(self) -> None:
        for transport in self._transports:
            transport.shutdown()


def describe_activation(category: str, allowed: Sequence[AllowedVector], *, executable: str | None = None) -> dict[str, Any]:
    """Text only: what an activation of ``category`` would start. Starts nothing."""
    keys = ["an operation-specific activation record that verifies and is pinned by the owner's approval document",
            "wsl_transport.REAL_SPAWN_COMPILED_IN = True by a reviewed source edit (the file hash is in the candidate manifest)",
            "process_runner.REAL_TRANSPORT_ENABLED = True by a reviewed source edit (it also makes the integrity evidence class REAL)"]
    if category == OPC_CLOCK:
        keys += ["wsl_clock_probe.LIVE_CLOCK_SAMPLING_ENABLED = True by a reviewed source edit", "a reviewed change of clock_only_round.execute_clock_only, which refuses any transport kind except mock and fixture"]
    elif category == OPC_CALIPER:
        keys += ["bounded_execution.REAL_LAUNCH_ENABLED = True and 'process' added to PERMITTED_RUNNER_KINDS by reviewed source edits",
                 "a transaction authorization inside the activation record, separate from every other category"]
    else:
        keys += ["a reviewed entry point that runs this read-only operation (none exists)"]
    return {"category": category, "real_spawn_compiled_in": REAL_SPAWN_COMPILED_IN, "executor_gates_accept_process_transport": False, "nothing_started": True,
            "explicit_executable": executable, "vectors": [a.as_dict() for a in allowed], "vectors_sha256": vectors_digest(allowed), "keys_required": keys}


def python_executable_for_fixtures() -> str:
    """The running interpreter, for fixture permits in tests."""
    return sys.executable
