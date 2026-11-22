"""WSL-aware, plan-bound process-runner adapter for the bounded execution entry point (task A0t, 2026-11-22). OFFLINE. This module contains NO process launcher.

What A0s left open. ``bounded_execution.execute_bounded`` runs a plan through an injected runner (``kind``, ``run``, ``start_background``, ``finish``, ``terminate``) and refuses a runner
of kind ``process``; no runner and no Windows -> WSL path handling existed. This module supplies the runner as a THIN, VALIDATING layer over an injected *transport*:

    execute_bounded --> PlanBoundRunner --> transport.run(Invocation)        (the transport is the only thing that could start an OS process)

* ``PlanBoundRunner`` accepts exactly the argument vectors the plan lists (steps, read-only configuration vectors, the clock probe), with the plan's working directory, no longer a timeout
  than the plan and the authorization allow, at most the number of times the plan lists them, and no launch mode other than the plan's. Anything else is a ``RunnerRefused``
  (a recorded halt in ``execute_bounded``), never a silent fallback. There is no retry anywhere.
* Vectors are STRUCTURED: a list of text tokens. Nothing is ever joined into a shell string. ``validate_argv`` rejects shells, inline-code flags of interpreters, shell metacharacters,
  quotes, control characters and ``..`` path segments, so an ambiguous or injectable token cannot be handed on.
* WSL-side steps are wrapped as ``wsl.exe -d <distro> --cd <cwd> --exec <argv...>`` (``--exec`` means no shell) and every Windows drive path inside the vector is translated with
  :mod:`path_mapping` (explicit automount root). The Windows-side clock probe (``wsl.exe ... date``) is passed through unchanged.
* Transports: any object with ``kind`` and ``run/start/finish/terminate``. ``FixtureTransport`` lives in ``tests/a0t_support.py`` (it runs harmless local commands, never ``wsl.exe``).
  ``DisabledWslTransport`` is the future real transport: it declares ``kind = "process"`` and every method raises :class:`TransportDisabled`. ``REAL_TRANSPORT_ENABLED`` is False; no record,
  flag or environment variable changes that, this module reads none, and ``bounded_execution`` independently refuses kind ``process``. A synthetic authorization never yields a real transport.

What this module does not claim: that ``wsl.exe --cd/--exec`` behaves as described on the target machine, that the vectors work, or that any mapped path exists inside the distribution or the
Caliper container. ``describe_plan_invocations`` prints what WOULD be run.
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from . import bounded_execution as be
from . import path_mapping as pm

RUNNER_VERSION = "process_runner 0.1 (A0t, 2026-11-22)"
REAL_TRANSPORT_ENABLED = False                    # no record, flag or environment variable can turn this on in this version
A0T_AUTH_SCHEMA = "a0t_process_authorization/1"
INVOCATION_SCHEMA = "a0t_planned_invocations/1"
OP_CLOCK_ONLY = "clock_only_measurement"
OP_SMOKE = "bounded_transaction_smoke"
OPERATION_CATEGORIES = (OP_CLOCK_ONLY, OP_SMOKE)
TRANSPORT_WSL = "wsl_process_transport"            # the only transport label a real authorization may name (and it is disabled)
TRANSPORT_FIXTURE = "fixture_transport"            # synthetic tests only
ENV_WINDOWS_ORCHESTRATOR = "windows_host_orchestrator_with_wsl_steps"
INSPECTION_TIMEOUT_S = 30.0                        # mirrors the literal in config_inspection.execute_inspection (a test pins the two together)
DOMAIN_WINDOWS, DOMAIN_WSL = "windows_host", "wsl"
LAUNCH_FOREGROUND, LAUNCH_BACKGROUND = "foreground", "background"
MAX_ARGV_LEN, MAX_TOKEN_LEN = 256, 4096
SHELL_NAMES = frozenset({"sh", "bash", "dash", "zsh", "ksh", "fish", "csh", "tcsh", "ash", "busybox", "cmd", "cmd.exe", "powershell", "powershell.exe", "pwsh", "pwsh.exe"})
INTERPRETER_NAMES = frozenset({"python", "python3", "python.exe", "py", "node", "node.exe", "perl", "ruby", "php", "lua"})
INLINE_CODE_FLAGS = frozenset({"-c", "-e", "-E", "--eval", "-p", "-r", "-m", "-command", "-encodedcommand"})
_META = re.compile(r"[;&|`<>$\"'\x00-\x1f\x7f]")


class ArgvRefused(ValueError):
    """An argument vector is not acceptable. ``code`` is a stable machine-readable cause."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{message} [{code}]")
        self.code = code


class RunnerRefused(ValueError):
    """The plan-bound runner refused to hand a request to the transport. Nothing was started."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{message} [{code}]")
        self.code = code


class TransportDisabled(RuntimeError):
    """The real process transport is disabled in this version."""


# ------------------------------------------------------------------------------------------------ structured argument vectors
def _basename(token: str) -> str:
    return re.split(r"[\\/]", token)[-1].lower()


def validate_argv(argv: Any) -> list[str]:
    """The vector as a list of tokens, or :class:`ArgvRefused`. The vector is never joined, quoted, split or repaired."""
    if isinstance(argv, (str, bytes)) or not isinstance(argv, (list, tuple)):
        raise ArgvRefused("argv_not_a_vector", "a command must be a list of tokens, never a single string")
    if not argv or len(argv) > MAX_ARGV_LEN:
        raise ArgvRefused("argv_length", f"a vector needs 1..{MAX_ARGV_LEN} tokens")
    tokens: list[str] = []
    for index, token in enumerate(argv):
        if not isinstance(token, str):
            raise ArgvRefused("token_not_text", f"token {index} is {type(token).__name__}")
        if len(token) > MAX_TOKEN_LEN:
            raise ArgvRefused("token_too_long", f"token {index} has {len(token)} characters")
        if token == "" and index == 0:
            raise ArgvRefused("empty_executable", "the executable token is empty")
        match = _META.search(token)
        if match:
            raise ArgvRefused("shell_metacharacter_or_quote", f"token {index} contains {match.group(0)!r}; vectors never go through a shell, so such characters are never legitimate")
        if ".." in re.split(r"[\\/]", token) and ("/" in token or "\\" in token):
            raise ArgvRefused("path_traversal", f"token {index} contains a '..' path segment")
        tokens.append(token)
    names = [_basename(t) for t in tokens]
    if any(n in SHELL_NAMES for n in names):
        raise ArgvRefused("shell_executable", f"a shell or shell-like executable appears in the vector: {[t for t, n in zip(tokens, names) if n in SHELL_NAMES]}")
    if names[0] in INTERPRETER_NAMES and any(t in INLINE_CODE_FLAGS for t in tokens[1:]):
        raise ArgvRefused("inline_code", f"{tokens[0]!r} with an inline-code flag would run text, not a file")
    if names[0] == "wsl.exe" and "--exec" not in tokens:
        raise ArgvRefused("wsl_without_exec", "wsl.exe without --exec runs the command through the distribution's default shell")
    if names[0] == "wsl.exe" and any(t in ("-e",) for t in tokens):
        raise ArgvRefused("ambiguous_wsl_flag", "use the long form --exec so the structure is unambiguous")
    return list(tokens)


def domain_of(argv: Sequence[str]) -> str:
    """``windows_host`` for a vector that starts ``wsl.exe`` (the clock probe), otherwise ``wsl``. Text rule only."""
    return DOMAIN_WINDOWS if _basename(argv[0]) == "wsl.exe" else DOMAIN_WSL


@dataclass(frozen=True)
class Invocation:
    """What a transport is asked to start: the OS-level vector, its domain, the approved WSL working directory and the bounds."""

    step_id: str
    domain: str
    os_argv: tuple[str, ...]
    wsl_cwd: str | None
    timeout_s: float | None
    launch_mode: str
    plan_argv: tuple[str, ...]
    translations: tuple[tuple[str, str], ...] = ()


def build_invocation(step_id: str, argv: Sequence[str], cwd: str | None, timeout_s: float | None, *, launch_mode: str, distro: str, mount_root: str) -> Invocation:
    tokens = validate_argv(argv)
    if launch_mode not in (LAUNCH_FOREGROUND, LAUNCH_BACKGROUND):
        raise ArgvRefused("launch_mode", f"unknown launch mode {launch_mode!r}")
    domain = domain_of(tokens)
    if domain == DOMAIN_WINDOWS:
        if cwd is not None:
            raise ArgvRefused("cwd_on_windows_step", "a Windows-side vector (wsl.exe ...) takes no working directory")
        return Invocation(step_id, domain, tuple(tokens), None, timeout_s, launch_mode, tuple(tokens))
    translated = pm.translate_argv_for_wsl(tokens, mount_root=mount_root)
    translations = tuple((a, b) for a, b in zip(tokens, translated) if a != b)
    wsl_cwd = None
    if cwd is not None:
        wsl_cwd = pm.translate_token_for_wsl(cwd, mount_root=mount_root)
        if pm.classify(wsl_cwd) != "wsl_posix":
            raise ArgvRefused("cwd_not_absolute", f"working directory {cwd!r} is not an absolute path")
    os_argv = ["wsl.exe", "-d", distro] + (["--cd", wsl_cwd] if wsl_cwd else []) + ["--exec", *translated]
    return Invocation(step_id, domain, tuple(validate_argv(os_argv)), wsl_cwd, timeout_s, launch_mode, tuple(tokens), translations)


# ------------------------------------------------------------------------------------------------ transports
class DisabledWslTransport:
    """The future real transport. It exists so the intended wiring can be inspected; every operation refuses."""

    kind = "process"

    def _refuse(self, *_a: Any, **_k: Any) -> Any:
        raise TransportDisabled("the real WSL process transport is disabled in this version (process_runner.REAL_TRANSPORT_ENABLED is False); nothing was started")

    run = start = finish = terminate = _refuse


def make_transport(label: Any, *, synthetic_authorization: bool, fixture_factory: Callable[[], Any] | None = None) -> Any:
    """The transport named by an authorization. ``wsl_process_transport`` yields the DISABLED transport; the fixture label needs a synthetic authorization AND an injected factory;
    nothing else exists. A synthetic authorization can never reach the real transport."""
    if label == TRANSPORT_WSL:
        if synthetic_authorization:
            raise RunnerRefused("synthetic_authorization_for_real_transport", "a synthetic authorization may not name the real transport")
        return DisabledWslTransport()
    if label == TRANSPORT_FIXTURE:
        if not synthetic_authorization or fixture_factory is None:
            raise RunnerRefused("fixture_transport_needs_synthetic_authorization", "the fixture transport is for synthetic tests only")
        transport = fixture_factory()
        if getattr(transport, "kind", None) not in ("mock", "fixture"):
            raise RunnerRefused("fixture_transport_kind", f"the fixture factory returned kind {getattr(transport, 'kind', None)!r}")
        return transport
    raise RunnerRefused("transport_unknown", f"transport {label!r} is not one of {[TRANSPORT_WSL, TRANSPORT_FIXTURE]}")


# ------------------------------------------------------------------------------------------------ the vector table of one plan
@dataclass
class PlanVector:
    step_id: str
    argv: tuple[str, ...]
    cwd: str | None
    timeout_s: float
    launch_mode: str
    klass: str
    max_uses: int
    uses: int = 0
    cwd_may_be_none: bool = False


def planned_vectors(plan: Mapping[str, Any]) -> list[PlanVector]:
    """Every vector ``execute_bounded`` can hand to a runner, with its approved cwd, bound and use count. Derived only from the plan."""
    entries: dict[tuple[str, ...], PlanVector] = {}
    request = plan["request"]
    clock = request["clock"]

    def add(step_id: str, argv: Sequence[str], cwd: str | None, timeout_s: float, mode: str, klass: str, uses: int, cwd_may_be_none: bool = False) -> None:
        key = tuple(argv)
        if key in entries:                                     # the same vector in two steps (peer version in the initial and the final inspection): one entry, summed uses
            entries[key].max_uses += uses
            return
        entries[key] = PlanVector(step_id, key, cwd, float(timeout_s), mode, klass, uses, cwd_may_be_none=cwd_may_be_none)

    for step in plan["steps"]:
        if step["kind"] in ("run_foreground", "tx_foreground"):
            add(step["step"], step["argv"], step["cwd"], step["timeout_s"], LAUNCH_FOREGROUND, step["class"], 1)
        elif step["kind"] == "run_background":
            add(step["step"], step["argv"], step["cwd"], plan["envelope"]["limits"]["collector_wait_s"] + plan["envelope"]["limits"]["max_total_duration_s"], LAUNCH_BACKGROUND, step["class"], 1)
        elif step["kind"] == "gate_clock":
            add(step["step"], step["argv"], None, clock["probe_timeout_s"], LAUNCH_FOREGROUND, step["class"], clock["samples_per_phase"], cwd_may_be_none=True)
    repo_cwd = next((s["cwd"] for s in plan["steps"] if s["kind"] == "gate_config"), None)
    for stage in ("initial", "final"):
        for vector in plan["config_inspection"][stage]["steps"]:
            add(f"{stage}:{vector['step']}", vector["argv"], repo_cwd, INSPECTION_TIMEOUT_S, LAUNCH_FOREGROUND, "read_only_observation", 1, cwd_may_be_none=True)
    return list(entries.values())


def _clock_uses_total(plan: Mapping[str, Any]) -> int:
    return sum(1 for s in plan["steps"] if s["kind"] == "gate_clock") * plan["request"]["clock"]["samples_per_phase"]


class PlanBoundRunner:
    """The runner interface ``bounded_execution.Deps.runner`` expects, bound to ONE plan. It validates, wraps, counts and delegates; it starts nothing itself."""

    def __init__(self, plan: Mapping[str, Any], transport: Any, *, distro: str, mount_root: str, max_process_lifetime_s: float, vectors: Sequence[PlanVector] | None = None):
        if plan.get("real_launch_enabled") is not False:
            raise RunnerRefused("plan_flag", "a plan may not carry real_launch_enabled true")
        if not (isinstance(max_process_lifetime_s, (int, float)) and not isinstance(max_process_lifetime_s, bool) and max_process_lifetime_s > 0):
            raise RunnerRefused("lifetime_unbounded", "max_process_lifetime_s must be a number > 0 (no default)")
        if not isinstance(distro, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", distro):
            raise RunnerRefused("distro", "the WSL distribution name is required (no default)")
        self.kind = getattr(transport, "kind", None)
        if self.kind not in ("mock", "fixture", "process"):
            raise RunnerRefused("transport_kind", f"transport kind {self.kind!r} is not recognised")
        self._transport, self._plan, self._distro = transport, plan, distro
        self._mount_root = pm.check_mount_root(mount_root)
        self._limit = float(max_process_lifetime_s)
        self._table = {e.argv: e for e in (vectors if vectors is not None else planned_vectors(plan))}
        self._tx_vectors = {tuple(s["argv"]) for s in plan.get("steps", []) if s.get("class") == "transaction_producing" and s.get("argv")}
        self._handles: dict[Any, PlanVector] = {}
        self.log: list[dict[str, Any]] = []

    # -- internal
    def _admit(self, argv: Sequence[str], timeout_s: Any, cwd: Any, mode: str) -> tuple[PlanVector, Invocation]:
        try:
            tokens = validate_argv(argv)
        except ArgvRefused as exc:
            self._record("refused", argv, exc.code)
            raise RunnerRefused(exc.code, str(exc)) from exc
        entry = self._table.get(tuple(tokens))
        if entry is None:
            self._record("refused", tokens, "vector_not_in_plan")
            raise RunnerRefused("vector_not_in_plan", f"{tokens[:3]}... is not a vector of the approved plan")
        if entry.launch_mode != mode:
            self._record("refused", tokens, "launch_mode_not_approved")
            raise RunnerRefused("launch_mode_not_approved", f"step {entry.step_id} is {entry.launch_mode}, requested {mode}")
        if cwd is not None and cwd != entry.cwd:
            self._record("refused", tokens, "cwd_not_approved")
            raise RunnerRefused("cwd_not_approved", f"working directory {cwd!r} differs from the approved {entry.cwd!r}")
        if cwd is None and entry.cwd is not None and not entry.cwd_may_be_none:
            self._record("refused", tokens, "cwd_missing")
            raise RunnerRefused("cwd_missing", f"step {entry.step_id} must run in the approved working directory {entry.cwd!r}")
        if timeout_s is not None:
            if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)) or timeout_s <= 0:
                raise RunnerRefused("timeout_invalid", f"timeout {timeout_s!r} is not a number > 0")
            if timeout_s > entry.timeout_s or timeout_s > self._limit:
                self._record("refused", tokens, "timeout_exceeds_bound")
                raise RunnerRefused("timeout_exceeds_bound", f"timeout {timeout_s} exceeds min(step bound {entry.timeout_s}, process lifetime limit {self._limit})")
        elif mode == LAUNCH_FOREGROUND:
            raise RunnerRefused("timeout_missing", "every foreground step needs a timeout")
        if entry.uses >= entry.max_uses:
            code = "second_transaction_attempt" if tuple(tokens) in self._tx_vectors else "no_retry"
            self._record("refused", tokens, code)
            raise RunnerRefused(code, f"step {entry.step_id} was already started {entry.uses} time(s) (limit {entry.max_uses}); nothing is retried")
        invocation = build_invocation(entry.step_id, tokens, entry.cwd, timeout_s if timeout_s is not None else min(entry.timeout_s, self._limit), launch_mode=mode,
                                      distro=self._distro, mount_root=self._mount_root)
        entry.uses += 1
        return entry, invocation

    def _record(self, event: str, argv: Sequence[Any], detail: str) -> None:
        self.log.append({"event": event, "argv_head": [str(t)[:60] for t in list(argv)[:4]], "detail": detail})

    # -- the runner interface
    def run(self, argv: Sequence[str], timeout_s: float, cwd: str | None) -> be.RunResult:
        entry, invocation = self._admit(argv, timeout_s, cwd, LAUNCH_FOREGROUND)
        self._record("run", invocation.os_argv, entry.step_id)
        return self._transport.run(invocation)

    def start_background(self, argv: Sequence[str], cwd: str | None) -> Any:
        entry, invocation = self._admit(argv, None, cwd, LAUNCH_BACKGROUND)
        self._record("start_background", invocation.os_argv, entry.step_id)
        handle = self._transport.start(invocation)
        self._handles[handle] = entry
        return handle

    def finish(self, handle: Any, timeout_s: float) -> be.RunResult:
        if handle not in self._handles:
            raise RunnerRefused("unknown_handle", "no such background process")
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)) or timeout_s <= 0 or timeout_s > self._limit:
            raise RunnerRefused("timeout_exceeds_bound", f"finish timeout {timeout_s!r} must be in (0, {self._limit}]")
        return self._transport.finish(handle, timeout_s)

    def terminate(self, handle: Any, grace_s: float) -> be.RunResult:
        if handle not in self._handles:
            raise RunnerRefused("unknown_handle", "only a process this runner started can be terminated (no process group other than the authorized one)")
        if isinstance(grace_s, bool) or not isinstance(grace_s, (int, float)) or grace_s <= 0 or grace_s > self._limit:
            raise RunnerRefused("timeout_exceeds_bound", f"grace {grace_s!r} must be in (0, {self._limit}]")
        return self._transport.terminate(handle, grace_s)


# ------------------------------------------------------------------------------------------------ dry inspection
def describe_plan_invocations(plan: Mapping[str, Any], *, distro: str, mount_root: str, path_record: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Exactly what WOULD be handed to the OS for every vector of the plan. Nothing is started; text only."""
    root = pm.check_mount_root(mount_root)
    rows = []
    for entry in planned_vectors(plan):
        inv = build_invocation(entry.step_id, entry.argv, entry.cwd, entry.timeout_s, launch_mode=entry.launch_mode, distro=distro, mount_root=root)
        rows.append({"step": entry.step_id, "class": entry.klass, "domain": inv.domain, "launch_mode": inv.launch_mode, "max_uses": entry.max_uses, "timeout_s": entry.timeout_s,
                     "approved_cwd": entry.cwd, "plan_argv": list(inv.plan_argv), "os_argv": list(inv.os_argv), "path_translations": [list(t) for t in inv.translations]})
    tx = [r for r in rows if r["class"] == "transaction_producing"]
    return {"schema": INVOCATION_SCHEMA, "version": RUNNER_VERSION, "plan_sha256": plan["plan_sha256"], "automount_root": root, "distro": distro,
            "real_transport_enabled": REAL_TRANSPORT_ENABLED, "nothing_started": True, "invocations": rows, "transaction_producing_vectors": len(tx),
            "path_mapping_record_sha256": (path_record or {}).get("record_sha256"),
            "claims_not_made": ["wsl.exe --cd/--exec behaves as described on the target machine", "any vector works", "any mapped path exists in the distribution or container",
                                "the Caliper container named in the vector exists"]}


def write_roots(plan: Mapping[str, Any], *, mount_root: str) -> list[str]:
    """The only WSL directories the plan may write to: the run output directory and the worker evidence root."""
    return [pm.windows_to_wsl(plan["layout"]["output_dir"], mount_root=mount_root) if pm.classify(plan["layout"]["output_dir"]) == "windows_drive"
            else pm.normalize_posix(plan["layout"]["output_dir"], what="output_dir"),
            pm.normalize_posix(plan["layout"]["worker_evidence_root_host"], what="worker evidence root")]


def check_writes_isolated(plan: Mapping[str, Any], *, mount_root: str) -> list[dict[str, str]]:
    """Every file or directory a step writes must lie inside the run output directory or the worker evidence root (after translation). Returns problems (empty = none)."""
    problems: list[dict[str, str]] = []
    roots = write_roots(plan, mount_root=mount_root)
    out_host = plan["layout"]["output_dir"]
    for step in plan["steps"]:
        for target in step["writes"]:
            as_text = str(target)
            if pm.classify(as_text) == "relative":
                as_text = out_host.rstrip("\\/") + "/" + as_text            # host-evidence names are relative to the output directory
            try:
                wsl = pm.windows_to_wsl(as_text, mount_root=mount_root) if pm.classify(as_text) == "windows_drive" else pm.normalize_posix(as_text, what="write target")
                pm.require_within(wsl, roots, what=f"{step['step']} write target")
            except pm.PathMappingError as exc:
                problems.append({"code": exc.code, "message": f"{step['step']}: {exc}"})
    for stage in ("initial", "final"):
        for key in ("block_path", "decoded_json_path"):
            target = plan["config_inspection"][stage][key]
            try:
                wsl = pm.windows_to_wsl(target, mount_root=mount_root) if pm.classify(target) == "windows_drive" else pm.normalize_posix(target, what="inspection output")
                pm.require_within(wsl, roots, what=f"config inspection {stage} {key}")
            except pm.PathMappingError as exc:
                problems.append({"code": exc.code, "message": f"config inspection {stage}: {exc}"})
    return problems


def host_read_path(path: str, *, mount_root: str) -> str:
    """The Windows path a host-side reader should open for a path the plan gave in WSL form (``/mnt/c/...``). A WSL-native path is refused (UNC unsupported)."""
    return pm.wsl_to_windows(path, mount_root=mount_root) if pm.classify(path) == "wsl_posix" else path


# ------------------------------------------------------------------------------------------------ the A0t authorization extension
A0T_KEYS = ("schema", "operation_category", "process_transport", "execution_environment", "distro", "automount_root", "max_process_lifetime_s", "path_mapping_record_sha256",
            "invocation_description_sha256", "real_transport_authorized")


def a0t_authorization_extension(plan: Mapping[str, Any], record: Mapping[str, Any], description: Mapping[str, Any], *, category: str) -> dict[str, Any]:
    """The extension block of an authorization, every owner-decided field UNSET (null). Values the tool can copy (hashes, the derived root) are copied for review; copying is not approving."""
    if category not in OPERATION_CATEGORIES:
        raise RunnerRefused("operation_category", f"{category!r} is not one of {list(OPERATION_CATEGORIES)}")
    return {"schema": A0T_AUTH_SCHEMA, "operation_category": category, "process_transport": None, "execution_environment": None, "distro": None, "automount_root": record["automount_root"],
            "max_process_lifetime_s": None, "path_mapping_record_sha256": record["record_sha256"], "invocation_description_sha256": pm._digest(dict(description)),
            "real_transport_authorized": None}


def verify_a0t_authorization(auth: Any, plan: Mapping[str, Any], record: Mapping[str, Any], description: Mapping[str, Any], *, repo_root: Any, allow_synthetic: bool = False,
                             expected_category: str) -> dict[str, Any]:
    """Findings for the A0t extension PLUS every A0s finding. ``real_transport_permitted`` is False while ``REAL_TRANSPORT_ENABLED`` is False, for every input."""
    base = be.verify_authorization(auth, plan, repo_root=repo_root, allow_synthetic=allow_synthetic)
    findings = list(base["findings"])

    def bad(code: str, message: str) -> None:
        findings.append({"code": code, "message": message})

    ext = auth.get("a0t") if isinstance(auth, Mapping) else None
    if not isinstance(ext, Mapping):
        bad("a0t_extension_missing", "the authorization has no 'a0t' block")
    else:
        unset = [k for k in A0T_KEYS if ext.get(k) in (None, "")]
        if unset:
            bad("a0t_field_unset", f"unset owner fields: {unset}")
        if ext.get("schema") != A0T_AUTH_SCHEMA:
            bad("a0t_schema", f"schema {ext.get('schema')!r}")
        if ext.get("operation_category") not in OPERATION_CATEGORIES:
            bad("operation_category", f"{ext.get('operation_category')!r} is not one of {list(OPERATION_CATEGORIES)}")
        elif ext.get("operation_category") != expected_category:
            bad("operation_category_mismatch", f"authorized {ext.get('operation_category')!r}, this run is {expected_category!r}")
        if ext.get("process_transport") not in (TRANSPORT_WSL, TRANSPORT_FIXTURE):
            bad("process_transport", f"{ext.get('process_transport')!r} is not one of {[TRANSPORT_WSL, TRANSPORT_FIXTURE]}")
        if ext.get("process_transport") == TRANSPORT_FIXTURE and not (allow_synthetic and auth.get("synthetic") is True):
            bad("fixture_transport_not_operational", "the fixture transport is accepted only with a synthetic authorization in a test")
        if ext.get("execution_environment") != ENV_WINDOWS_ORCHESTRATOR:
            bad("execution_environment", f"{ext.get('execution_environment')!r}; this adapter implements only {ENV_WINDOWS_ORCHESTRATOR!r}")
        if ext.get("automount_root") != record.get("automount_root"):
            bad("automount_root_mismatch", "the authorized automount root differs from the path-mapping record's")
        if ext.get("path_mapping_record_sha256") != record.get("record_sha256"):
            bad("path_mapping_record_mismatch", "the authorization binds another path-mapping record")
        if ext.get("invocation_description_sha256") != pm._digest(dict(description)):
            bad("invocation_description_mismatch", "the authorization binds other intended process vectors")
        life = ext.get("max_process_lifetime_s")
        if isinstance(life, bool) or not isinstance(life, (int, float)) or life <= 0:
            bad("lifetime_unbounded", "max_process_lifetime_s must be a number > 0")
        elif life < plan["envelope"]["limits"]["caliper_timeout_s"] and expected_category == OP_SMOKE:
            bad("lifetime_below_caliper_timeout", "the process lifetime limit is shorter than the Caliper timeout in the plan")
        if not isinstance(ext.get("real_transport_authorized"), bool):
            bad("real_transport_authorized", "must be an explicit true or false")
    if pm.verify_path_record(record):
        bad("path_record_invalid", str(pm.verify_path_record(record)))
    valid = not findings
    return {"valid": valid, "findings": findings, "real_transport_permitted": False if not REAL_TRANSPORT_ENABLED else valid, "synthetic": bool(isinstance(auth, Mapping) and auth.get("synthetic")),
            "note": "a valid record is still not a launch: the real transport and real launch are disabled in this version"}


def execute_through_adapter(plan: Mapping[str, Any], auth: Mapping[str, Any], record: Mapping[str, Any], transport: Any, deps_parts: Mapping[str, Any], *, repo_root: Any,
                            expected_category: str, allow_synthetic: bool, fixture_factory: Callable[[], Any] | None = None) -> dict[str, Any]:
    """The wiring of the adapter to ``bounded_execution.execute_bounded``. ``transport`` must be what :func:`make_transport` returned for the authorization's label.

    ``deps_parts`` supplies ``now_utc``, ``wall_ns``, ``mono_ns``, ``read_file`` and ``hash_tree`` (injected; this module reads no clock and no file). A transport of kind ``process`` is
    refused here and, independently, by ``execute_bounded``."""
    ext = auth.get("a0t") if isinstance(auth.get("a0t"), Mapping) else {}
    try:
        description = describe_plan_invocations(plan, distro=str(ext.get("distro")), mount_root=str(ext.get("automount_root")), path_record=record)
    except (pm.PathMappingError, ArgvRefused, RunnerRefused) as exc:       # unset or malformed owner fields: the verdict below reports them; nothing is described
        description = {"error": f"{type(exc).__name__}: {exc}"}
    verdict = verify_a0t_authorization(auth, plan, record, description, repo_root=repo_root, allow_synthetic=allow_synthetic, expected_category=expected_category)
    if not verdict["valid"]:
        raise be.LaunchRefused("the A0t authorization does not verify", verdict["findings"])
    if getattr(transport, "kind", None) == "process" or ext.get("process_transport") == TRANSPORT_WSL:
        raise be.LaunchRefused("the real WSL process transport is disabled in this version; nothing was started")
    problems = check_writes_isolated(plan, mount_root=ext["automount_root"])
    if problems:
        raise be.LaunchRefused("a planned write lies outside the approved run directories", problems)
    runner = PlanBoundRunner(plan, transport, distro=ext["distro"], mount_root=ext["automount_root"], max_process_lifetime_s=ext["max_process_lifetime_s"])
    deps = be.Deps(runner=runner, **dict(deps_parts))
    status = be.execute_bounded(plan, auth, deps, repo_root=repo_root, allow_synthetic=allow_synthetic)
    status["runner_log"] = list(runner.log)
    return status
