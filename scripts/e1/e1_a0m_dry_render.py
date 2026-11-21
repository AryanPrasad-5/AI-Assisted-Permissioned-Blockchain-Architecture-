"""Versioned DRY-RENDER workflow for the issue-logged workload and recorder v3 (task A0m, 2026-11-21). OFFLINE ONLY: it reads and writes files.

What it prepares. One new, isolated directory that holds a Caliper run configuration in which

  * every round names the versioned workload ``e1_create_asset_issue_logged.js`` and carries an ``issueLog`` round argument (one new log file per round);
  * the one transaction monitor names recorder v3 ``e1_per_tx_recorder_v3.js`` with an explicit ``health`` block;
  * ``modules/`` holds every file those two need (v3 -> v2 -> v1 for the recorder; issue-logged -> legacy workload + core + issue log for the workload);
  * a machine-readable manifest (``a0m_config_manifest.json``) lists every file with its SHA-256, the selections, the per-round output paths and the identities.

How it reuses what exists (nothing pinned is edited, nothing is copied into a competing framework):

  1. the A0e ``prepare`` step runs unchanged: operator-parameter contract, A0b channel rules, the PINNED renderer, the A0d v2 wiring, the A0e launch manifest;
  2. this tool then derives a NEW directory from that v2-wired run and replaces the monitor and the workload module there. Historical (v2) preparation stays
     available and verifiable on its own through ``e1_prelive_validation.py preflight``.

Selection is explicit and has no default: the A0m parameter file must name the workload, the recorder, the worker count, the issue-log settings and the recorder
health settings. A missing, unknown or unsupported value is refused BEFORE any directory is created. Only one worker is supported (both workloads and the writer
reject more); another value is refused, never replaced by 1.

What a pass means. A rendered configuration is not an executed experiment, and an offline preflight is not a live validation. This tool never starts Caliper,
Docker, WSL, Fabric, Prometheus, a model or a network connection, imports no process or network API (a test asserts that), and has no execute phase: ``execute`` is
refused, always. Nothing here shows that recorder v3 or the issue-logged workload loads inside a real Caliper worker.

    python scripts/e1/e1_a0m_dry_render.py dry-render --params P --a0m-params Q --channel NAME --out-root DIR --render-id ID
    python scripts/e1/e1_a0m_dry_render.py preflight  --run-dir DIR/ID [--candidate-manifest M]
    python scripts/e1/e1_a0m_dry_render.py execute            (always refused)

Exit codes: 0 pass, 1 a check failed, 2 invalid request or parameters (nothing created), 3 inconclusive, 5 execute refused.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
for _entry in (str(HERE), str(REPO / "src")):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

import e1_la2_render as lr                       # noqa: E402  (the pinned renderer, imported not edited; only its constants are read)
import e1_prelive_validation as pv               # noqa: E402  (A0e: prepare and preflight, reused unchanged)
import e1_recorder_v2_wire as wire               # noqa: E402  (A0d: v1/v2 module names and the protected-name scan)
from fabric_perf.controllers import channel_selection as cs     # noqa: E402
from fabric_perf.controllers import prelive_contract as pc       # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi      # noqa: E402

TOOL_VERSION = "e1_a0m_dry_render 0.1 (A0m, 2026-11-21)"
PARAMS_SCHEMA = "a0m_render_params/1"
MANIFEST_SCHEMA = "a0m_dry_render_manifest/1"
RESULT_SCHEMA = "a0m_preflight_result/1"
STATUS_RENDERED = "RENDERED_OFFLINE_NOT_EXECUTED"
INCOMPLETE_MARKER = "A0M_RENDER_INCOMPLETE.json"
MANIFEST_NAME = "a0m_config_manifest.json"
VALIDATED_PARAMS_NAME = "a0m_params.validated.json"
RENDER_ID = re.compile(r"[A-Za-z0-9_.-]{1,60}")
EXIT_EXECUTE_REFUSED = 5

SELECTED_WORKLOAD = "e1_create_asset_issue_logged/1"          # the WORKLOAD_ID the successor workload writes into its issue log
SELECTED_RECORDER = "e1_per_tx_recorder_v3"
WORKLOAD_LEGACY, WORKLOAD_V3 = "e1_create_asset.js", "e1_create_asset_issue_logged.js"
RECORDER_V1, RECORDER_V2, RECORDER_V3 = wire.V1_NAME, wire.V2_NAME, "e1_per_tx_recorder_v3.js"
ISSUE_LOG_MODULE, CORE_MODULE = "e1_issue_log.js", "e1_workload_core.js"
WORKER_ID = "w0"                                              # the one supported worker, index 0
ISSUE_LOG_DIR, RECORDER_DIR = "issuelog", "recorder"
EVIDENCE_ORIGINS = ("live_recorder", "synthetic_fixture")
FSYNC_POLICIES = ("tick", "every", "none")
PRINTABLE = re.compile(r"[\x20-\x7e]+")
TOP_KEYS = ("schema", "selected_workload", "selected_recorder", "worker_count", "issue_log", "recorder_health")
ISSUE_LOG_KEYS = ("clock_domain", "tick_interval_ms", "fsync_policy")
HEALTH_KEYS = ("source_id", "collector_id", "evidence_origin", "heartbeat_interval_ms", "checkpoint_every_heartbeats")

# module name -> (repo-relative source, role). The first three are the v1 -> v2 -> v3 recorder chain; the last four are the workload chain.
MODULE_SOURCES = {
    RECORDER_V1: ("scripts/e1/caliper/" + RECORDER_V1, "recorder_v1_pinned"),
    RECORDER_V2: ("scripts/e1/caliper/" + RECORDER_V2, "recorder_v2"),
    RECORDER_V3: ("scripts/e1/caliper/" + RECORDER_V3, "recorder_v3_selected"),
    WORKLOAD_LEGACY: ("fabric-testbed/caliper/workloads/" + WORKLOAD_LEGACY, "workload_legacy_dependency"),
    WORKLOAD_V3: ("fabric-testbed/caliper/workloads/" + WORKLOAD_V3, "workload_issue_logged_selected"),
    ISSUE_LOG_MODULE: ("fabric-testbed/caliper/workloads/" + ISSUE_LOG_MODULE, "workload_issue_log_writer"),
    CORE_MODULE: ("fabric-testbed/caliper/workloads/" + CORE_MODULE, "workload_core"),
}
ENTRY_MODULES = (RECORDER_V3, WORKLOAD_V3)
NODE_BUILTINS = {"fs", "path", "crypto", "os", "perf_hooks"}       # the built-ins the seven modules actually require (listed from their source)
EXTERNAL_EXPECTED = ("@hyperledger/caliper-core",)             # resolved only inside a Caliper worker (also its package.json): cannot be checked offline
REQUIRE_RE = re.compile(r"""require\(\s*(['"])([^'"]+)\1\s*\)""")

PASS, FAIL, INCONCLUSIVE, NOT_CHECKED = "PASS", "FAIL", "INCONCLUSIVE", "NOT_CHECKED"


class A0mError(ValueError):
    """The dry render cannot proceed or a rendered directory is inconsistent. Nothing finished is left behind."""


class A0mParameterError(ValueError):
    """The A0m parameter file is invalid; ``issues`` lists every problem. Nothing was created."""

    def __init__(self, issues: list[pc.ParameterIssue]):
        super().__init__("; ".join(f"{i.field}: {i.message}" for i in issues))
        self.issues = issues


# ------------------------------------------------------------------------------------------------------- parameters
def _int(value: Any, *, minimum: int, maximum: int | None = None) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum and (maximum is None or value <= maximum)


def validate_a0m_parameters(raw: Any) -> dict[str, Any]:
    """Return the validated values or raise ``A0mParameterError`` with every problem. There are no defaults: every field is required."""
    issues: list[pc.ParameterIssue] = []

    def bad(code: str, path: str, message: str) -> None:
        issues.append(pc.ParameterIssue(code, path, message))

    if not isinstance(raw, Mapping):
        raise A0mParameterError([pc.ParameterIssue("wrong_type", "<root>", "the A0m parameter file must contain a JSON object")])
    if raw.get("schema") != PARAMS_SCHEMA:
        bad("schema", "schema", f"must be {PARAMS_SCHEMA!r}, got {raw.get('schema')!r}")
    for key in raw:
        if key not in TOP_KEYS:
            bad("unknown_key", str(key), f"not a known key; allowed: {list(TOP_KEYS)}")
    for key, want in (("selected_workload", SELECTED_WORKLOAD), ("selected_recorder", SELECTED_RECORDER)):
        if key not in raw or raw.get(key) in (None, ""):
            bad("selection_missing", key, f"the selection must be explicit; the only supported value is {want!r}")
        elif raw.get(key) != want:
            bad("selection_unsupported", key, f"must be {want!r}, got {raw.get(key)!r}")
    workers = raw.get("worker_count")
    if workers is None:
        bad("missing", "worker_count", "the worker count must be stated; only 1 is supported (no value is assumed)")
    elif not _int(workers, minimum=1) or workers != 1:
        bad("worker_count_unsupported", "worker_count",
            f"exactly 1 worker is supported (both workloads, the issue-log writer and the reader reject more; asset keys carry no worker index), got {workers!r}")

    def section(name: str, allowed: tuple[str, ...]) -> Mapping[str, Any] | None:
        value = raw.get(name)
        if value is None:
            bad("missing", name, "section is required")
            return None
        if not isinstance(value, Mapping):
            bad("wrong_type", name, "must be an object")
            return None
        for key in value:
            if key not in allowed:
                bad("unknown_key", f"{name}.{key}", f"not a known key; allowed: {list(allowed)}")
        for key in allowed:
            if key not in value or value[key] is None:
                bad("missing", f"{name}.{key}", "required; no default is applied")
        return value

    log = section("issue_log", ISSUE_LOG_KEYS)
    if log is not None:
        domain = log.get("clock_domain")
        if "clock_domain" in log and not (isinstance(domain, str) and PRINTABLE.fullmatch(domain) and len(domain) <= 80 and domain.strip() == domain):
            bad("malformed", "issue_log.clock_domain", "must be a non-blank printable-ASCII label of at most 80 characters without surrounding blanks")
        if "tick_interval_ms" in log and not _int(log.get("tick_interval_ms"), minimum=10, maximum=600_000):
            bad("out_of_range", "issue_log.tick_interval_ms", f"must be an integer in 10..600000 (the writer's own limits), got {log.get('tick_interval_ms')!r}")
        if "fsync_policy" in log and log.get("fsync_policy") not in FSYNC_POLICIES:
            bad("not_allowed", "issue_log.fsync_policy", f"must be one of {list(FSYNC_POLICIES)}, got {log.get('fsync_policy')!r}")
    health = section("recorder_health", HEALTH_KEYS)
    if health is not None:
        for key in ("source_id", "collector_id"):
            value = health.get(key)
            if key in health and not (isinstance(value, str) and PRINTABLE.fullmatch(value) and value.strip() == value and len(value) <= 80):
                bad("malformed", f"recorder_health.{key}", "must be a non-blank printable-ASCII string of at most 80 characters")
        if "evidence_origin" in health and health.get("evidence_origin") not in EVIDENCE_ORIGINS:
            bad("not_allowed", "recorder_health.evidence_origin", f"must be one of {list(EVIDENCE_ORIGINS)}, got {health.get('evidence_origin')!r}")
        hb = health.get("heartbeat_interval_ms")
        if "heartbeat_interval_ms" in health and not (_int(hb, minimum=1000) and hb % 1000 == 0):
            bad("out_of_range", "recorder_health.heartbeat_interval_ms", f"must be a whole number of seconds in ms (>= 1000, multiple of 1000), got {hb!r}")
        if "checkpoint_every_heartbeats" in health and not _int(health.get("checkpoint_every_heartbeats"), minimum=1):
            bad("out_of_range", "recorder_health.checkpoint_every_heartbeats", "must be a positive integer")
    if issues:
        raise A0mParameterError(issues)
    return {"schema": PARAMS_SCHEMA, "selected_workload": raw["selected_workload"], "selected_recorder": raw["selected_recorder"],
            "worker_count": int(raw["worker_count"]), "issue_log": {k: log[k] for k in ISSUE_LOG_KEYS},
            "recorder_health": {k: health[k] for k in HEALTH_KEYS}}


def load_a0m_parameters(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise A0mParameterError([pc.ParameterIssue("unreadable", str(path), str(exc))]) from None
    except ValueError as exc:
        raise A0mParameterError([pc.ParameterIssue("not_json", str(path), str(exc))]) from None
    return validate_a0m_parameters(raw)


# ------------------------------------------------------------------------------------------------------- derivation
def recorder_base(run_id: str, round_index: int, label: str) -> str:
    """The base name recorder v1 gives a round's files: ``<runId>_w<worker>_r<round>_<label>`` (the same rule as ``e1_prelive_validation.expected_live_files``)."""
    return f"{run_id}_w0_r{round_index}_{label}"


def round_paths(run_container_dir: str, run_id: str, round_index: int, label: str) -> dict[str, str]:
    base = recorder_base(run_id, round_index, label)
    recorder_dir = posixpath.join(run_container_dir, RECORDER_DIR)
    return {"round": round_index, "label": label, "recorder_base": base,
            "issue_log_path": posixpath.join(run_container_dir, ISSUE_LOG_DIR, f"{base}.issuelog.jsonl"),
            "recorder_health_spool": posixpath.join(recorder_dir, f"{base}.health.jsonl"),
            "recorder_live_submits": posixpath.join(recorder_dir, f"{base}.live.submits.csv"),
            "recorder_live_csv": posixpath.join(recorder_dir, f"{base}.live.csv")}


def health_options(values: Mapping[str, Any]) -> dict[str, Any]:
    h = values["recorder_health"]
    return {"sourceId": h["source_id"], "collectorId": h["collector_id"], "evidenceOrigin": h["evidence_origin"],
            "heartbeatIntervalMs": h["heartbeat_interval_ms"], "checkpointEveryHeartbeats": h["checkpoint_every_heartbeats"]}


def issue_log_argument(values: Mapping[str, Any], path: str) -> dict[str, Any]:
    log = values["issue_log"]
    return {"path": path, "workerId": WORKER_ID, "expectedWorkers": [WORKER_ID], "clockDomain": log["clock_domain"],
            "tickIntervalMs": log["tick_interval_ms"], "fsyncPolicy": log["fsync_policy"]}


def select_v3(benchmark: Mapping[str, Any], values: Mapping[str, Any], run_id: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """A v2-wired benchmark -> the v3 + issue-logged benchmark and the per-round path table. The input is not modified; anything unexpected is refused."""
    doc = json.loads(json.dumps(benchmark))
    workers = (doc.get("test") or {}).get("workers", {}).get("number")
    if workers != values["worker_count"] or workers != 1:
        raise A0mError(f"the benchmark declares {workers!r} worker(s); exactly 1 is supported and it must equal the requested worker_count")
    monitors = (doc.get("monitors") or {}).get("transaction")
    if not isinstance(monitors, list) or len(monitors) != 1 or not isinstance(monitors[0], dict):
        raise A0mError("the benchmark must have exactly one transaction monitor")
    monitor = monitors[0]
    module = monitor.get("module")
    if not isinstance(module, str) or posixpath.basename(module) != RECORDER_V2:
        raise A0mError(f"the transaction monitor names {module!r}; the A0m workflow starts from the v2-wired run (expected {RECORDER_V2})")
    options = dict(monitor.get("options") or {})
    if options.get("runId") != run_id:
        raise A0mError(f"the monitor's runId {options.get('runId')!r} differs from the run {run_id!r}")
    if "health" in options:
        raise A0mError("the monitor already carries a health block; refusing to wire a run twice")
    out_dir = options.get("outputDir")
    if not isinstance(out_dir, str) or posixpath.basename(out_dir) != RECORDER_DIR or not out_dir.startswith("/"):
        raise A0mError(f"the monitor outputDir {out_dir!r} must be an absolute POSIX path ending in /{RECORDER_DIR}")
    run_dir_container = posixpath.dirname(out_dir)
    rounds = (doc.get("test") or {}).get("rounds")
    if not isinstance(rounds, list) or not rounds:
        raise A0mError("the benchmark has no rounds")
    table = []
    for index, rnd in enumerate(rounds):
        workload = rnd.get("workload") if isinstance(rnd, dict) else None
        label = rnd.get("label") if isinstance(rnd, dict) else None
        if not isinstance(workload, dict) or not isinstance(label, str) or not label:
            raise A0mError(f"round {index} has no workload or no label")
        if posixpath.basename(str(workload.get("module"))) != WORKLOAD_LEGACY:
            raise A0mError(f"round {index} workload module {workload.get('module')!r} is not {WORKLOAD_LEGACY}; refusing to guess")
        args = workload.get("arguments")
        if not isinstance(args, dict) or args.get("runId") != run_id:
            raise A0mError(f"round {index} arguments carry runId {None if not isinstance(args, dict) else args.get('runId')!r}, not {run_id!r}")
        if "issueLog" in args:
            raise A0mError(f"round {index} already carries issueLog; refusing to wire a run twice")
        paths = round_paths(run_dir_container, run_id, index, label)
        table.append(paths)
        workload["module"] = posixpath.join(posixpath.dirname(workload["module"]), WORKLOAD_V3)
        args["issueLog"] = issue_log_argument(values, paths["issue_log_path"])
    monitor["module"] = posixpath.join(posixpath.dirname(module), RECORDER_V3)
    options["health"] = health_options(values)
    monitor["options"] = options
    return doc, table


def resolve_requires(modules_dir: Path, entries: tuple[str, ...] = ENTRY_MODULES) -> dict[str, Any]:
    """Static module resolution: follow every relative ``require`` from the entry modules inside ``modules_dir`` only. Raises ``A0mError`` for a relative require that
    does not resolve to a file there. Built-ins and Caliper's own package are reported separately (the latter cannot be checked offline)."""
    graph: dict[str, list[str]] = {}
    external: set[str] = set()
    pending = list(entries)
    while pending:
        name = pending.pop()
        if name in graph:
            continue
        file = Path(modules_dir) / name
        if not file.is_file():
            raise A0mError(f"modules/{name} is required but missing")
        deps = []
        for _quote, target in REQUIRE_RE.findall(file.read_text(encoding="utf-8")):
            if target.startswith("./"):
                dep = target[2:]
                if "/" in dep or ".." in dep:
                    raise A0mError(f"modules/{name} requires {target!r}, which leaves the modules directory")
                if not (Path(modules_dir) / dep).is_file():
                    raise A0mError(f"modules/{name} requires {target!r}, which is not in the modules directory")
                deps.append(dep)
                pending.append(dep)
            elif target in NODE_BUILTINS:
                continue
            elif target in EXTERNAL_EXPECTED or target.startswith(EXTERNAL_EXPECTED[0] + "/"):
                external.add(target)
            else:
                raise A0mError(f"modules/{name} requires {target!r}, which is neither a sibling, a Node built-in nor Caliper's own package")
        graph[name] = sorted(set(deps))
    return {"graph": {k: graph[k] for k in sorted(graph)}, "external_unverifiable_offline": sorted(external)}


def _sha_tree(root: Path) -> dict[str, str]:
    skip = {MANIFEST_NAME}
    return {p.relative_to(root).as_posix(): pi.sha256_file(p) for p in sorted(root.rglob("*"))
            if p.is_file() and p.relative_to(root).as_posix() not in skip
            and not {"preflight_results", "a0m_preflight_results"} & set(p.relative_to(root).parts)}


def _read_json(path: Path) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise A0mError(f"cannot read {path}: {exc}") from exc


def _utc(now: datetime | None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(timespec="seconds")


# ------------------------------------------------------------------------------------------------------- verification
def verify_v3(run_root: Path, *, repo_root: Path = REPO) -> dict[str, Any]:
    """Re-read what a dry render wrote and prove, from the files, what a run directory would select. Raises ``A0mError`` on the first inconsistency."""
    run_root, repo = Path(run_root), Path(repo_root)
    manifest = _read_json(run_root / MANIFEST_NAME)
    if manifest.get("schema") != MANIFEST_SCHEMA or manifest.get("status") != STATUS_RENDERED:
        raise A0mError("the A0m manifest has the wrong schema or status")
    run_id, channel = manifest["run_id"], manifest["channel"]
    values = validate_a0m_parameters(_read_json(run_root / VALIDATED_PARAMS_NAME))
    cs.select_experiment_channel(channel, purpose="an A0m dry render", option="channel")
    wired = run_root / manifest["wired_v3_dir"]
    bench = _read_json(wired / "benchmark.yaml")
    network = _read_json(wired / "networkconfig.yaml")
    if wire.network_channels(network) != [channel]:
        raise cs.ChannelSelectionError(cs.MISMATCH, f"the network configuration names {wire.network_channels(network)} but the run is for {channel!r}", value=channel)
    wire._scan_protected(run_root)
    # selections, derived from the generated benchmark (never from the manifest)
    monitors = bench["monitors"]["transaction"]
    if len(monitors) != 1 or posixpath.basename(monitors[0]["module"]) != RECORDER_V3:
        raise A0mError(f"the generated benchmark does not select recorder v3 (monitor: {[m.get('module') for m in monitors]})")
    rounds = bench["test"]["rounds"]
    for index, rnd in enumerate(rounds):
        if posixpath.basename(rnd["workload"]["module"]) != WORKLOAD_V3:
            raise A0mError(f"round {index} does not select {WORKLOAD_V3}: {rnd['workload']['module']!r}")
    if bench["test"]["workers"]["number"] != 1 or values["worker_count"] != 1:
        raise A0mError("the generated benchmark does not declare exactly 1 worker")
    # modules: present, equal to the repository sources, statically resolvable from the directory itself
    modules = wired / "modules"
    for name, (rel, _role) in MODULE_SOURCES.items():
        shipped, source = modules / name, repo / rel
        if not shipped.is_file():
            raise A0mError(f"modules/{name} is missing")
        if pi.sha256_file(shipped) != pi.sha256_file(source):
            raise A0mError(f"modules/{name} differs from {rel}")
    if pi.sha256_file(modules / RECORDER_V1) != wire.PINNED_V1_SHA256:
        raise A0mError("modules/e1_per_tx_recorder.js is not the pinned v1 recorder")
    resolution = resolve_requires(modules)
    # per-round identity and path consistency
    monitor_options = monitors[0]["options"]
    run_container = posixpath.dirname(monitor_options["outputDir"])
    if monitor_options.get("runId") != run_id:
        raise A0mError("the monitor runId differs from the run id")
    if monitor_options.get("health") != health_options(values):
        raise A0mError("the monitor health block differs from the validated parameters")
    seen_logs: set[str] = set()
    table = []
    for index, rnd in enumerate(rounds):
        args, label = rnd["workload"]["arguments"], rnd["label"]
        expected = round_paths(run_container, run_id, index, label)
        want = issue_log_argument(values, expected["issue_log_path"])
        if args.get("issueLog") != want:
            raise A0mError(f"round {index} issueLog {args.get('issueLog')!r} differs from the expected {want!r}")
        if args.get("runId") != run_id:
            raise A0mError(f"round {index} runId differs from the run id")
        path = args["issueLog"]["path"]
        if path in seen_logs:
            raise A0mError(f"two rounds share the issue-log path {path}")
        seen_logs.add(path)
        recorder_files = {expected[k] for k in ("recorder_health_spool", "recorder_live_submits", "recorder_live_csv")}
        if path in recorder_files or posixpath.dirname(path) == monitor_options["outputDir"]:
            raise A0mError(f"the issue-log path {path} collides with the recorder output directory")
        table.append(expected)
    return {"run_id": run_id, "channel": channel, "rounds": table, "module_resolution": resolution, "workers": 1,
            "selected_monitor_module": monitors[0]["module"], "selected_workload_modules": [r["workload"]["module"] for r in rounds]}


# ------------------------------------------------------------------------------------------------------- phase 1
def dry_render(a0e_params_path: Path, a0m_params_path: Path, out_root: Path, render_id: str, *, channel: str | None, repo_root: Path = REPO,
               now: datetime | None = None, argv: list[str] | None = None) -> dict[str, Any]:
    """Render one isolated directory. Raises before creating anything for invalid parameters, a protected or mismatching channel, an unsupported worker count or an
    existing target. Returns the manifest."""
    values = load_a0m_parameters(a0m_params_path)                                    # explicit selections and settings, refused here if wrong
    validated = pv.load_parameters(a0e_params_path)                                  # the A0e contract: run id, channel, cell, environment ...
    a0e = validated.values
    selected = cs.select_experiment_channel(channel, purpose="an A0m dry render", option="--channel")
    if selected != a0e["channel"]:
        raise cs.ChannelSelectionError(cs.MISMATCH, f"--channel {selected!r} differs from the channel {a0e['channel']!r} in the A0e parameter file", value=selected)
    if not RENDER_ID.fullmatch(render_id):
        raise A0mError(f"render id {render_id!r} must match {RENDER_ID.pattern}")
    root = Path(out_root) / render_id
    if root.exists():
        raise A0mError(f"refusing to reuse {root}: every dry render gets a new, isolated directory")
    repo = Path(repo_root)
    for name, (rel, _role) in MODULE_SOURCES.items():
        if not (repo / rel).is_file():
            raise A0mError(f"required source {rel} is missing; nothing was created")
    if pi.sha256_file(repo / MODULE_SOURCES[RECORDER_V1][0]) != wire.PINNED_V1_SHA256:
        raise A0mError("the v1 recorder no longer matches its pinned hash; nothing was created")
    try:
        base_manifest = pv.prepare(a0e_params_path, root / "a0e_base", "prepared", repo_root=repo, now=now, argv=argv)   # A0e, unchanged
        run_id = a0e["run_id"]
        source = root / "a0e_base" / "prepared" / base_manifest["wired_run_dir"]
        target = root / "wired_v3" / run_id
        shutil.copytree(source, target)
        bench, table = select_v3(_read_json(source / "benchmark.yaml"), values, run_id)
        for name, (rel, _role) in MODULE_SOURCES.items():
            shutil.copyfile(repo / rel, target / "modules" / name)
        (target / "benchmark.yaml").write_text(json.dumps(bench, indent=2) + "\n", encoding="utf-8")
        pi.write_new_json(root / VALIDATED_PARAMS_NAME, values)
        pi.write_new_json(root / "a0e_params.copy.json", validated.as_dict())
        manifest = {
            "schema": MANIFEST_SCHEMA, "tool": TOOL_VERSION, "status": STATUS_RENDERED, "phase": "dry-render", "created_utc": _utc(now),
            "caliper_invoked": False, "fabric_contacted": False, "docker_or_wsl_started": False, "prometheus_contacted": False,
            "model_inference_enabled": False, "actuator_invoked": False, "transactions_submitted": 0,
            "execution": {"status": "NOT_AUTHORIZED", "phase_implemented": False,
                          "note": "A rendered configuration is not permission to run it. This tool has no execute phase."},
            "run_id": run_id, "channel": selected,
            "channel_selection": cs.selection_record(selected, role=cs.ROLE_EXPERIMENT, option="--channel"),
            "owner_approval_of_channel": "NOT_GRANTED", "channel_exists": "UNVERIFIED",
            "cell": a0e["cell"],
            "selections": {"workload": values["selected_workload"], "workload_file": WORKLOAD_V3, "recorder": values["selected_recorder"],
                           "recorder_file": RECORDER_V3, "recorder_dependencies": [RECORDER_V2, RECORDER_V1],
                           "workload_dependencies": [WORKLOAD_LEGACY, ISSUE_LOG_MODULE, CORE_MODULE]},
            "worker": {"count": 1, "worker_id": WORKER_ID, "index": 0, "multi_worker": "REFUSED: unsupported by both workloads and the writer"},
            "issue_log": values["issue_log"], "recorder_health": values["recorder_health"],
            "rounds": table, "wired_v3_dir": f"wired_v3/{run_id}", "a0e_base_dir": "a0e_base/prepared",
            "a0e_base_launch_manifest_sha256": pi.sha256_file(root / "a0e_base" / "prepared" / "launch_manifest.json"),
            "modules": {name: {"source": rel, "role": role, "sha256": pi.sha256_file(target / "modules" / name)} for name, (rel, role) in MODULE_SOURCES.items()},
            "inputs": {"a0e_params": {"file": str(Path(a0e_params_path).resolve()), "sha256": pi.sha256_file(Path(a0e_params_path))},
                       "a0m_params": {"file": str(Path(a0m_params_path).resolve()), "sha256": pi.sha256_file(Path(a0m_params_path))}},
            "tool_hashes": {"a0m_renderer": pi.sha256_file(Path(__file__)), "a0e_orchestrator": pi.sha256_file(repo / "scripts/e1/e1_prelive_validation.py"),
                            "pinned_renderer": pi.sha256_file(repo / "scripts/e1/e1_la2_render.py"), "recorder_wiring": pi.sha256_file(repo / "scripts/e1/e1_recorder_v2_wire.py")},
            "source_count": {"recorder_health_sourceCount": "NOT_CONFIGURED: a JSON benchmark cannot carry a provider function; the recorder's own count certifies nothing "
                                                               "without one", "independent_source": "the generator issue log (read by the Python reader), declared independent, "
                                                                                                    "never verified live"},
            "evidence_streams": {"generator_issue_log": "<runDir>/issuelog/<runId>_w0_r<round>_<label>.issuelog.jsonl (e1_issue_log/1; time = workload Date.now at the attempt record)",
                                 "recorder": "<runDir>/recorder/<runId>_w0_r<round>_<label>.* (v1/v2 files and the v3 .health.jsonl spool; time = the moment the observer saw TxSubmitted)",
                                 "distinct": "different directories, different suffixes, written by different modules; their timestamps are different quantities and are never merged"},
            "limitations": [
                "NOT EXECUTED: no Caliper worker has loaded e1_create_asset_issue_logged.js or recorder v3; Caliper's WorkloadModuleBase, sendRequests return value and "
                "txSubmitted behaviour are unread.",
                "Source completeness is not established by a rendered configuration. The issue log is a declared-independent source count, never verified live.",
                "Reader lag: a recorder callback whose generator record has not been read yet reads as recorder_exceeds_source (fail closed, invalid at that decision time) "
                "and resolves when the record is read; the reader's poll cadence is not set or measured here.",
                "One worker only. All timing values (tick interval, heartbeat) are operator-supplied placeholders without scientific justification.",
                "Windows-WSL clock agreement is not measured. Real predictive accuracy and controller superiority are not established.",
            ],
            "live_gates": [{"gate": g, "state": s} for g, s in (("owner_approval_of_validation_channel", "NOT_GRANTED"), ("validation_channel_exists", "UNVERIFIED"),
                                                              ("owner_approval_of_artifact_hashes", "NOT_GRANTED"), ("windows_wsl_clock_agreement", "UNVERIFIED"),
                                                              ("recorder_v3_inside_a_real_caliper_worker", "UNVERIFIED"),
                                                              ("issue_logged_workload_inside_a_real_caliper_worker", "UNVERIFIED"),
                                                              ("authorization_to_execute_any_live_step", "NOT_GRANTED"))],
        }
        manifest["files"] = _sha_tree(root)
        pi.write_new_json(root / MANIFEST_NAME, manifest)
        verify_v3(root, repo_root=repo)
        return manifest
    except BaseException as exc:
        if root.exists():
            (root / INCOMPLETE_MARKER).write_text(json.dumps({"status": "RENDER_INCOMPLETE", "error": f"{type(exc).__name__}: {exc}"}, indent=2) + "\n", encoding="utf-8")
        raise


# ------------------------------------------------------------------------------------------------------- phase 2
def _check(checks: list[dict[str, Any]], check_id: str, status: str, detail: Any) -> None:
    checks.append({"id": check_id, "status": status, "detail": detail})


def preflight(run_root: Path, *, repo_root: Path = REPO, candidate_manifest: Path | None = None, write: bool = True,
              now: datetime | None = None) -> dict[str, Any]:
    """Offline checks on a rendered directory. Never contacts anything; always ``live_ready: false``."""
    root, repo = Path(run_root), Path(repo_root)
    checks: list[dict[str, Any]] = []
    manifest: dict[str, Any] = {}
    try:
        if (root / INCOMPLETE_MARKER).exists():
            raise A0mError(f"the directory carries {INCOMPLETE_MARKER}: the render did not finish")
        manifest = _read_json(root / MANIFEST_NAME)
        if manifest.get("schema") != MANIFEST_SCHEMA or manifest.get("status") != STATUS_RENDERED:
            raise A0mError("the manifest has the wrong schema or status")
        _check(checks, "A01_manifest", PASS, f"{manifest['status']} ({manifest['run_id']})")
    except (A0mError, KeyError) as exc:
        _check(checks, "A01_manifest", FAIL, str(exc))
        return _finish(checks, root, manifest, write, now)
    try:
        recorded, actual = manifest["files"], _sha_tree(root)
        problems = [f"{n}: {'missing' if n not in actual else 'modified'}" for n, d in sorted(recorded.items()) if actual.get(n) != d]
        problems += [f"{n}: not in the manifest" for n in sorted(set(actual) - set(recorded))]
        _check(checks, "A02_file_inventory", FAIL if problems else PASS, problems or f"{len(recorded)} files match")
    except (KeyError, TypeError, OSError) as exc:
        _check(checks, "A02_file_inventory", FAIL, repr(exc))
    try:
        base = pv.preflight(root / manifest["a0e_base_dir"], repo_root=repo, write=False, now=now)
        _check(checks, "A03_a0e_base_preflight", {PASS: PASS, FAIL: FAIL, INCONCLUSIVE: INCONCLUSIVE}[base["offline_preflight"]],
               {"offline_preflight": base["offline_preflight"], "counts": base["counts"], "live_ready": base["live_ready"],
                "failed": [c["id"] for c in base["checks"] if c["status"] == FAIL]})
    except (OSError, KeyError, ValueError) as exc:
        _check(checks, "A03_a0e_base_preflight", FAIL, repr(exc))
    facts: dict[str, Any] = {}
    try:
        facts = verify_v3(root, repo_root=repo)
        _check(checks, "A04_selections_and_modules", PASS,
               {"monitor": facts["selected_monitor_module"], "workload_modules": sorted(set(facts["selected_workload_modules"])),
                "modules": len(facts["module_resolution"]["graph"]), "external_unverifiable_offline": facts["module_resolution"]["external_unverifiable_offline"]})
    except (A0mError, cs.ChannelSelectionError, wire.WireError, KeyError, TypeError, ValueError, OSError) as exc:
        _check(checks, "A04_selections_and_modules", FAIL, f"{type(exc).__name__}: {exc}")
    try:
        rounds = facts["rounds"]
        logs = [r["issue_log_path"] for r in rounds]
        if len(set(logs)) != len(logs):
            raise A0mError("issue-log paths are not unique")
        _check(checks, "A05_identities_and_paths", PASS, {"workers": 1, "worker_id": WORKER_ID, "rounds": len(rounds), "issue_logs": logs})
    except (A0mError, KeyError) as exc:
        _check(checks, "A05_identities_and_paths", FAIL, str(exc) if facts else "selections did not verify")
    try:
        flags = [k for k in ("caliper_invoked", "fabric_contacted", "docker_or_wsl_started", "prometheus_contacted", "model_inference_enabled", "actuator_invoked") if manifest.get(k)]
        if manifest.get("transactions_submitted") != 0:
            flags.append("transactions_submitted")
        if (manifest.get("execution") or {}).get("phase_implemented") is not False:
            flags.append("execution.phase_implemented")
        _check(checks, "A06_nothing_executed", FAIL if flags else PASS, flags or "no execution flag is set")
    except (AttributeError, TypeError) as exc:
        _check(checks, "A06_nothing_executed", FAIL, repr(exc))
    if candidate_manifest is None:
        _check(checks, "A07_candidate_manifest", NOT_CHECKED, "no candidate manifest was supplied")
    else:
        try:
            findings = pi.verify_candidate_manifest(_read_json(candidate_manifest), repo)
            _check(checks, "A07_candidate_manifest", FAIL if findings else PASS,
                   [f.as_dict() for f in findings] or "every listed file matches; the manifest is a CANDIDATE and is not approved")
        except (A0mError, ValueError, OSError) as exc:
            _check(checks, "A07_candidate_manifest", FAIL, str(exc))
    return _finish(checks, root, manifest, write, now)


def _finish(checks: list[dict[str, Any]], root: Path, manifest: Mapping[str, Any], write: bool, now: datetime | None) -> dict[str, Any]:
    statuses = [c["status"] for c in checks]
    offline = FAIL if FAIL in statuses else (INCONCLUSIVE if INCONCLUSIVE in statuses else PASS)
    result = {"schema": RESULT_SCHEMA, "tool": TOOL_VERSION, "run_dir": str(root), "checked_utc": _utc(now), "run_id": manifest.get("run_id"), "channel": manifest.get("channel"),
              "checks": checks, "counts": {s: statuses.count(s) for s in (PASS, FAIL, INCONCLUSIVE, NOT_CHECKED)}, "offline_preflight": offline, "live_ready": False,
              "live_gates": manifest.get("live_gates", []),
              "statement": "An offline pass shows the rendered files are consistent. It does not show that anything loads or works inside a real Caliper worker or on a live "
                           "network, and it is not an authorization."}
    if write and root.is_dir():
        out = root / "a0m_preflight_results"
        out.mkdir(exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        result["written_to"] = [f"a0m_preflight_results/preflight_{stamp}.json"]
        pi.write_new_json(out / f"preflight_{stamp}.json", result)
    return result


def summary_markdown(result: Mapping[str, Any]) -> str:
    lines = [f"# A0m dry-render preflight: {result.get('run_id')} on {result.get('channel')}", "",
             f"**Offline preflight: {result['offline_preflight']}.  Live ready: {str(result['live_ready']).lower()}.**", "", result["statement"], "",
             "| Check | Status | Detail |", "|---|---|---|"]
    for item in result["checks"]:
        lines.append(f"| {item['id']} | {item['status']} | {json.dumps(item['detail'], sort_keys=True)[:300]} |")
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------------------------------------------- CLI
def refuse_execute() -> int:
    print("REFUSED: the execute phase is not available in this tool. A rendered configuration is not permission to run it; a live run needs a separate, versioned "
          "execution tool and explicit owner authorization for each live step. No flag or environment variable enables it.")
    return EXIT_EXECUTE_REFUSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    render = sub.add_parser("dry-render", help="render the issue-logged workload + recorder v3 into a new directory (offline)")
    render.add_argument("--params", required=True, type=Path, help="the A0e prelive_params/1 file (run id, channel, cell, environment)")
    render.add_argument("--a0m-params", required=True, type=Path, help="the a0m_render_params/1 file (explicit selections and settings)")
    render.add_argument("--channel", required=True, type=cs.experiment_channel_arg, metavar="CHANNEL", help="the dedicated validation channel; must equal the one in --params")
    render.add_argument("--out-root", required=True, type=Path)
    render.add_argument("--render-id", required=True)
    pre = sub.add_parser("preflight", help="offline checks on a rendered directory")
    pre.add_argument("--run-dir", required=True, type=Path)
    pre.add_argument("--candidate-manifest", type=Path)
    pre.add_argument("--no-write", action="store_true")
    sub.add_parser("execute", help="always refused")
    args = parser.parse_args(argv)
    if args.phase == "execute":
        return refuse_execute()
    if args.phase == "dry-render":
        try:
            manifest = dry_render(args.params, args.a0m_params, args.out_root, args.render_id, channel=args.channel, argv=sys.argv if argv is None else argv)
        except (A0mParameterError, pc.PreliveParameterError) as exc:
            print(json.dumps({"valid": False, "issues": [i.as_dict() for i in exc.issues]}, indent=2))
            return 2
        except (A0mError, pv.PrepareError, wire.WireError, cs.ChannelSelectionError, OSError, ValueError, KeyError) as exc:
            print(f"REFUSED: {type(exc).__name__}: {exc}")
            return 2
        print(f"dry render {args.out_root / args.render_id}: {manifest['status']}; Caliper, Fabric, Docker and WSL were not touched")
        return 0
    result = preflight(args.run_dir, candidate_manifest=args.candidate_manifest, write=not args.no_write)
    print(summary_markdown(result))
    return {PASS: 0, FAIL: 1, INCONCLUSIVE: 3}[result["offline_preflight"]]


if __name__ == "__main__":
    sys.exit(main())
