"""Bounded, explicitly gated execution entry point for ONE future Caliper validation run (task A0s, 2026-11-22). OFFLINE: no process, network or container API.

Four operations, kept apart:

  prepare           validate one explicit execution request, verify the candidate-manifest hashes, build the run layout from a rendered A0p variant
                    directory and write an inspectable execution plan (argument vectors, working directories, mounts, outputs, limits, step order).
  preflight         re-verify a stored plan: its digest, the artifact hashes on disk, the layout, the channel, the planned transaction envelope.
  inspect-plan      the human-readable view of a plan.
  execute           REFUSED in this version, always (``REAL_LAUNCH_ENABLED = False``). ``execute_bounded`` is the future adapter: it needs a verified authorization
                    record and an INJECTED runner. A runner whose ``kind`` is ``process`` is refused; only ``mock`` and ``fixture`` runners are accepted. This module
                    contains no process launcher, so a real launch needs a new, separately approved tool that supplies one and flips the constant.

Nothing is defaulted. The request names the manifest and its hash, the variant, the channel, the cell, the run, the workload, the recorder, the worker count, every
round, the rate/duration/transaction/timeout limits, the operator timing values, the clock-probe parameters, the collectors, the tool paths, the rendered directory and
the output directory. The authorization record must bind the SAME values plus the plan digest and the hashes of every launch artifact, and carry an owner approval
with a hash of the approval document. None of these is an authorization: a candidate manifest, a passing offline preflight, an ``--execute`` flag, an environment
variable (this module reads none), a synthetic fixture (accepted only with a mock runner and flagged), an unverified approval document, an assistant-written packet.

Safeguards in ``execute_bounded`` (all tested with mock runners): one worker; no protected or reserved channel; no silent workload, recorder or variant substitution; the
output directory must not exist (it is created exclusively; evidence files are created exclusively); planned rate, round duration, total duration and transaction count
must lie inside the authorized limits; exactly ONE transaction-producing step and it is attempted at most once (no retry, whatever happens); every foreground step
has a timeout; on failure, timeout or interruption the collectors are terminated, the partial evidence is kept and hashed, and a status file records what happened.
No step changes a channel configuration; the read-only inspection vectors are validated by ``config_inspection`` immediately before use.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import channel_selection as cs
from . import config_evidence as ce
from . import config_inspection as ci
from . import prelive_integrity as pi
from . import run_layout as rl
from . import timed_variant as tv
from . import wsl_clock_probe as wp

EXECUTION_VERSION = "bounded_execution 0.1 (A0s, 2026-11-22)"
REAL_LAUNCH_ENABLED = False                       # no record, flag or environment can turn this on in this version
REQUEST_SCHEMA = "a0s_execution_request/1"
PLAN_SCHEMA = "a0s_execution_plan/1"
AUTH_SCHEMA = "a0s_bounded_launch_authorization/1"
STATUS_SCHEMA = "a0s_run_status/1"
EXECUTABLE_VARIANTS = (tv.VARIANT_A0P,)           # the first validation runs exactly one instrumentation variant
SELECTED_RECORDER = "e1_per_tx_recorder_v3"
SELECTED_WORKLOAD = {"file": tv.WORKLOAD_TIMED, "identity": "e1_create_asset_issue_logged/1"}
FSYNC = ("tick", "every", "none")
STOP_CONDITIONS = ("caliper_timeout", "nonzero_exit", "channel_mismatch", "transaction_count_above_bound", "configuration_changed", "interrupt", "evidence_write_failure",
                   "initial_configuration_mismatch", "clock_not_agree")
PERMITTED_RUNNER_KINDS = ("mock", "fixture")
RUN_ID = re.compile(r"[A-Za-z0-9_.-]{1,40}")
SHA = re.compile(r"[0-9a-f]{64}")
# tool files the steps rely on (all must be listed, with matching hashes, in the candidate manifest the request names)
REQUIRED_TOOL_FILES = ("src/fabric_perf/controllers/bounded_execution.py", "src/fabric_perf/controllers/config_inspection.py", "src/fabric_perf/controllers/wsl_clock_probe.py",
                       "src/fabric_perf/controllers/run_layout.py", "src/fabric_perf/controllers/clock_preflight.py", "src/fabric_perf/controllers/channel_selection.py",
                       "scripts/e1/e1_ledger_follow.py", "scripts/e1/e1_a0s_bounded_execute.py")
OPTIONAL_TOOL_FILES = {"before_after": ("scripts/e1/e1_metrics_scraper.py",)}


class RequestInvalid(ValueError):
    """The execution request or plan is not acceptable. ``issues`` lists every problem found; nothing was created."""

    def __init__(self, issues: Sequence[Mapping[str, str]]):
        self.issues = [dict(i) for i in issues]
        super().__init__("; ".join(f"{i['field']}: {i['message']} [{i['code']}]" for i in self.issues))


class LaunchRefused(RuntimeError):
    """Execution was refused before any step ran. ``findings`` names every reason."""

    def __init__(self, message: str, findings: Sequence[Mapping[str, Any]] = ()):
        self.findings = [dict(f) for f in findings]
        super().__init__(message)


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == value and abs(value) != float("inf")


def _digest(document: Mapping[str, Any], drop: Sequence[str] = ()) -> str:
    return ci.sha256_hex(ci.canonical_json({k: v for k, v in document.items() if k not in drop}))


# ------------------------------------------------------------------------------------------------ request
REQUEST_KEYS = ("schema", "candidate_manifest", "variant", "channel", "cell", "cell_id", "run_id", "workload", "recorder_version", "worker_count", "rounds", "limits",
                "operator_timing", "clock", "ledger", "prometheus", "tools", "rendered_root", "output_dir", "authorization_path")
LIMIT_KEYS = ("max_rate_tps", "max_round_duration_s", "max_total_duration_s", "max_transactions", "caliper_timeout_s", "terminate_grace_s", "collector_wait_s")
TIMING_KEYS = ("tick_interval_ms", "fsync_policy", "clock_domain", "heartbeat_interval_ms", "checkpoint_every_heartbeats")
CLOCK_KEYS = ("tolerance_ms", "min_valid_samples", "max_wall_monotonic_disagreement_ms", "samples_per_phase", "distro", "remote_resolution_ns", "probe_timeout_s",
              "reference_domain", "remote_domain")
TOOL_KEYS = ("peer", "configtxlator", "orderer", "orderer_tls_hostname", "orderer_ca", "wsl_repo_path", "network_description_wsl", "rendered_dir_wsl", "output_dir_wsl")


def validate_request(request: Any) -> list[dict[str, str]]:
    """Every problem with a request (an empty list means none). Nothing is repaired or defaulted."""
    issues: list[dict[str, str]] = []

    def bad(code: str, field_: str, message: str) -> None:
        issues.append({"code": code, "field": field_, "message": message})

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
    manifest = request["candidate_manifest"]
    if not (isinstance(manifest, Mapping) and isinstance(manifest.get("path"), str) and isinstance(manifest.get("sha256"), str) and SHA.fullmatch(manifest["sha256"])):
        bad("manifest", "candidate_manifest", "needs a path and a 64-hex sha256")
    if request["variant"] not in EXECUTABLE_VARIANTS:
        bad("variant_not_executable", "variant", f"{request['variant']!r} is not executable by this version; allowed: {list(EXECUTABLE_VARIANTS)}")
    try:
        cs.select_experiment_channel(request["channel"], purpose="a bounded execution", option="channel")
    except cs.ChannelSelectionError as exc:
        bad("channel_refused", "channel", str(exc))
    cell = None
    try:
        cell = ci.expected_cell(request["cell"])
        if request["cell_id"] != ci.cell_identifier(cell):
            bad("cell_id_mismatch", "cell_id", f"the cell is {ci.cell_identifier(cell)!r}, the request says {request['cell_id']!r}")
    except ci.InspectionError as exc:
        bad("cell_invalid", "cell", str(exc))
    if not (isinstance(request["run_id"], str) and RUN_ID.fullmatch(request["run_id"])):
        bad("run_id_invalid", "run_id", f"must match {RUN_ID.pattern}")
    if dict(request["workload"]) != SELECTED_WORKLOAD:
        bad("workload_not_selected", "workload", f"must be exactly {SELECTED_WORKLOAD}; no substitution")
    if request["recorder_version"] != SELECTED_RECORDER:
        bad("recorder_not_selected", "recorder_version", f"must be {SELECTED_RECORDER!r}; no substitution")
    if isinstance(request["worker_count"], bool) or request["worker_count"] != 1:
        bad("worker_count", "worker_count", "exactly one worker is supported")
    rounds = request["rounds"]
    if not (isinstance(rounds, list) and rounds):
        bad("rounds", "rounds", "a non-empty list of rounds is required")
    else:
        for index, rnd in enumerate(rounds):
            ok = (isinstance(rnd, Mapping) and set(rnd) == {"label", "role", "tx_duration_s", "rate_tps"} and isinstance(rnd["label"], str) and rnd["role"] in ("warmup", "measured")
                  and all(_number(rnd[k]) and rnd[k] > 0 for k in ("tx_duration_s", "rate_tps")))
            if not ok:
                bad("round_invalid", f"rounds[{index}]", "needs exactly label, role (warmup|measured), tx_duration_s > 0, rate_tps > 0")
    limits = request["limits"]
    if not (isinstance(limits, Mapping) and set(limits) == set(LIMIT_KEYS) and all(_number(limits[k]) and limits[k] > 0 for k in LIMIT_KEYS)):
        bad("limits", "limits", f"every one of {list(LIMIT_KEYS)} is required and must be a finite number > 0")
    timing = request["operator_timing"]
    if not (isinstance(timing, Mapping) and set(timing) == set(TIMING_KEYS) and timing["fsync_policy"] in FSYNC and isinstance(timing["clock_domain"], str) and timing["clock_domain"]
            and all(_number(timing[k]) and timing[k] > 0 for k in ("tick_interval_ms", "heartbeat_interval_ms", "checkpoint_every_heartbeats"))):
        bad("operator_timing", "operator_timing", f"needs {list(TIMING_KEYS)}; fsync_policy in {FSYNC}")
    clock = request["clock"]
    if not (isinstance(clock, Mapping) and set(clock) == set(CLOCK_KEYS)):
        bad("clock", "clock", f"needs exactly {list(CLOCK_KEYS)}")
    else:
        for key in ("tolerance_ms", "max_wall_monotonic_disagreement_ms", "probe_timeout_s"):
            if not (_number(clock[key]) and clock[key] > 0):
                bad("clock_value", f"clock.{key}", "must be a finite number > 0 (operator-supplied)")
        for key in ("min_valid_samples", "samples_per_phase", "remote_resolution_ns"):
            if isinstance(clock[key], bool) or not isinstance(clock[key], int) or clock[key] < 1:
                bad("clock_value", f"clock.{key}", "must be a positive integer")
        if _number(clock.get("min_valid_samples")) and _number(clock.get("samples_per_phase")) and clock["min_valid_samples"] > clock["samples_per_phase"]:
            bad("clock_value", "clock.samples_per_phase", "must be at least min_valid_samples")
    ledger = request["ledger"]
    if not (isinstance(ledger, Mapping) and set(ledger) == {"poll_interval_s", "max_polls"} and _number(ledger["poll_interval_s"]) and ledger["poll_interval_s"] > 0
            and isinstance(ledger["max_polls"], int) and not isinstance(ledger["max_polls"], bool) and ledger["max_polls"] > 0):
        bad("ledger", "ledger", "needs poll_interval_s > 0 and an integer max_polls > 0 (no unbounded collector)")
    prom = request["prometheus"]
    if not (isinstance(prom, Mapping) and prom.get("mode") in rl.PROMETHEUS_MODES):
        bad("prometheus", "prometheus", f"mode must be one of {rl.PROMETHEUS_MODES}")
    elif prom["mode"] == "before_after" and not (set(prom) == {"mode", "interval_s", "max_rounds", "timeout_s"} and all(_number(prom[k]) and prom[k] > 0 for k in ("interval_s", "max_rounds", "timeout_s"))):
        bad("prometheus", "prometheus", "before_after needs interval_s, max_rounds and timeout_s, all > 0")
    elif prom["mode"] == "deferred" and set(prom) != {"mode"}:
        bad("prometheus", "prometheus", "deferred takes no other keys")
    tools = request["tools"]
    if not (isinstance(tools, Mapping) and set(tools) == set(TOOL_KEYS) and all(isinstance(tools[k], str) and tools[k].strip() for k in TOOL_KEYS)):
        bad("tools", "tools", f"needs non-empty strings for exactly {list(TOOL_KEYS)}")
    for key in ("rendered_root", "output_dir", "authorization_path"):
        if not isinstance(request[key], str) or not request[key].strip():
            bad("path", key, "must be a non-empty string")
    return issues


# ------------------------------------------------------------------------------------------------ plan
STEP_CLASSES = ("offline_internal", "read_only_observation", "local_staging", "transaction_producing")


def _step(step_id: str, kind: str, klass: str, argv: list[str] | None, *, cwd: str | None, produces: Sequence[str] = (), reads: Sequence[str] = (), timeout_s: float | None = None,
          note: str = "") -> dict[str, Any]:
    if klass not in STEP_CLASSES:
        raise ValueError(f"unknown step class {klass!r}")
    return {"step": step_id, "kind": kind, "class": klass, "argv": argv, "cwd": cwd, "reads": list(reads), "writes": list(produces), "timeout_s": timeout_s, "note": note}


def build_execution_plan(request: Mapping[str, Any], *, repo_root: Path, created_utc: str) -> dict[str, Any]:
    """An execution plan, or :class:`RequestInvalid` listing every problem. Reads files; writes nothing; starts nothing."""
    issues = validate_request(request)
    if issues:
        raise RequestInvalid(issues)
    repo_root = Path(repo_root)
    created = ci._aware_utc(created_utc, "created_utc")
    manifest_path = Path(repo_root) / request["candidate_manifest"]["path"]
    problems: list[dict[str, str]] = []

    def bad(code: str, field_: str, message: str) -> None:
        problems.append({"code": code, "field": field_, "message": message})

    if not manifest_path.is_file():
        raise RequestInvalid([{"code": "manifest_missing", "field": "candidate_manifest.path", "message": str(manifest_path)}])
    if pi.sha256_file(manifest_path) != request["candidate_manifest"]["sha256"]:
        bad("manifest_hash", "candidate_manifest.sha256", "differs from the file on disk")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for finding in pi.verify_candidate_manifest(manifest, repo_root):
        bad("artifact_hash", finding.path, f"{finding.code}: {finding.message}")
    entries = {e["path"]: e["sha256"] for e in manifest.get("entries", [])}
    tool_files = list(REQUIRED_TOOL_FILES) + list(OPTIONAL_TOOL_FILES.get(request["prometheus"]["mode"], ()))
    tool_hashes = {}
    for path in tool_files:
        if path not in entries:
            bad("tool_not_in_manifest", path, "a file this plan uses is not listed in the candidate manifest")
        else:
            tool_hashes[path] = entries[path]
    if Path(request["output_dir"]).exists():
        bad("output_occupied", "output_dir", f"{request['output_dir']} already exists; evidence is never overwritten")
    if problems:
        raise RequestInvalid(problems)
    try:
        layout = rl.build_layout(Path(request["rendered_root"]), Path(request["output_dir"]), repo_root=repo_root, prometheus_mode=request["prometheus"]["mode"])
    except rl.LayoutError as exc:
        raise RequestInvalid([{"code": exc.code, "field": "rendered_root", "message": str(exc)}]) from exc
    if layout["run_id"] != request["run_id"] and layout["run_id"] != tv.qualified_run_id(request["run_id"]):
        bad("run_id_mismatch", "run_id", f"the rendered run is {layout['run_id']!r}; the request names {request['run_id']!r} (the variant adds the suffix {tv.RUN_ID_SUFFIX!r})")
    if layout["channel"] != request["channel"]:
        bad("channel_mismatch", "channel", f"the rendered run was made for {layout['channel']!r}")
    cell, rendered_cell = ci.expected_cell(request["cell"]), layout["planned_cell"]
    try:
        timeout_ms = ce.parse_batch_timeout_ms(rendered_cell["tau"])
    except ce.ConfigEvidenceError as exc:
        timeout_ms = None
        bad("cell_mismatch", "cell", str(exc))
    if (rendered_cell.get("B"), timeout_ms, rendered_cell.get("P")) != (cell["max_message_count"], cell["batch_timeout_ms"], cell["preferred_max_bytes"]):
        bad("cell_mismatch", "cell", f"rendered cell (B, tau, P) = {(rendered_cell.get('B'), rendered_cell.get('tau'), rendered_cell.get('P'))} differs from the request")
    rendered_rounds = [{"label": r["label"], "role": r["role"], "tx_duration_s": r["tx_duration_s"], "rate_tps": r["rate_tps"]} for r in layout["rounds"]]
    if rendered_rounds != [dict(r) for r in request["rounds"]]:
        bad("rounds_mismatch", "rounds", f"the rendered rounds {rendered_rounds} differ from the request")
    limits, rounds = request["limits"], layout["rounds"]
    planned_tx = sum(r["rate_tps"] * r["tx_duration_s"] for r in rounds)
    planned_total = sum(r["tx_duration_s"] for r in rounds)
    if max(r["rate_tps"] for r in rounds) > limits["max_rate_tps"]:
        bad("rate_above_limit", "limits.max_rate_tps", "a planned rate exceeds the limit")
    if max(r["tx_duration_s"] for r in rounds) > limits["max_round_duration_s"]:
        bad("round_duration_above_limit", "limits.max_round_duration_s", "a planned round is longer than the limit")
    if planned_total > limits["max_total_duration_s"]:
        bad("total_duration_above_limit", "limits.max_total_duration_s", f"planned {planned_total} s")
    if planned_tx > limits["max_transactions"]:
        bad("transactions_above_limit", "limits.max_transactions", f"planned {planned_tx} transactions")
    if not planned_total <= limits["caliper_timeout_s"] <= limits["max_total_duration_s"]:
        bad("caliper_timeout", "limits.caliper_timeout_s", f"must lie in [planned total {planned_total} s, max_total_duration_s]")
    bench = json.loads((Path(layout["launch_authorizing_directory"]) / "benchmark.yaml").read_text(encoding="utf-8"))
    if bench["test"]["workers"]["number"] != 1:
        bad("worker_count", "worker_count", "the rendered benchmark does not use exactly one worker")
    first_args = bench["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"]
    rendered_timing = {"tick_interval_ms": first_args["tickIntervalMs"], "fsync_policy": first_args["fsyncPolicy"], "clock_domain": first_args["clockDomain"]}
    health = bench["monitors"]["transaction"][0]["options"]["health"]
    rendered_timing.update(heartbeat_interval_ms=health["heartbeatIntervalMs"], checkpoint_every_heartbeats=health["checkpointEveryHeartbeats"])
    if rendered_timing != {k: request["operator_timing"][k] for k in TIMING_KEYS}:
        bad("operator_timing_mismatch", "operator_timing", f"rendered {rendered_timing} differs from the request")
    if problems:
        raise RequestInvalid(problems)

    tools, clock, ledger = request["tools"], request["clock"], request["ledger"]
    out = tools["output_dir_wsl"]
    outp = lambda rel: f"{out.rstrip('/')}/{rel}"
    channel, run_id = request["channel"], layout["run_id"]
    host_ws, run_dir_ws = layout["caliper_workspace_host"], layout["worker_evidence_root_host"]
    inspection_args = dict(peer=tools["peer"], configtxlator=tools["configtxlator"], orderer=tools["orderer"], orderer_tls_hostname=tools["orderer_tls_hostname"], orderer_ca=tools["orderer_ca"])
    initial = ci.build_inspection_plan(channel, cell, out_dir=outp("config_inspection/initial"), label="initial", **inspection_args)
    final = ci.build_inspection_plan(channel, cell, out_dir=outp("config_inspection/final"), label="final", **inspection_args)
    probe = wp.probe_plan(clock["distro"], remote_resolution_ns=clock["remote_resolution_ns"], timeout_s=clock["probe_timeout_s"])
    repo_wsl = tools["wsl_repo_path"].rstrip("/")
    ledger_argv = ["python3", "scripts/e1/e1_ledger_follow.py", "--channel", channel, "--network", tools["network_description_wsl"], "--out-dir", outp("ledger/journal"),
                   "--collector-run-id", f"{run_id}-ledger", "--max-polls", str(ledger["max_polls"]), "--poll-interval-s", str(ledger["poll_interval_s"]), "--execute"]
    steps = [
        _step("X01_verify_artifacts", "gate_artifacts", "offline_internal", None, cwd=None, reads=sorted(tool_hashes), note="recompute every bound artifact hash; any difference halts before anything else"),
        _step("X02_config_initial", "gate_config", "read_only_observation", None, cwd=repo_wsl, produces=[initial["block_path"], initial["decoded_json_path"]],
              note="read-only: peer channel fetch config + configtxlator proto_decode; must equal the approved cell"),
        _step("X03_clock_pre", "gate_clock", "read_only_observation", probe["argv"], cwd=None, produces=[layout["host_evidence"]["clock_pre"]], timeout_s=clock["probe_timeout_s"],
              note=f"{clock['samples_per_phase']} round trips; verdict must be AGREE (clock_preflight)"),
        _step("X04_run_dir_absent", "run_foreground", "local_staging", ["mkdir", run_dir_ws], cwd=None, timeout_s=10.0, note="mkdir without -p fails if the run directory exists: no overwrite"),
        _step("X05_stage_rendered_run", "run_foreground", "local_staging", ["cp", "-r", f"{tools['rendered_dir_wsl'].rstrip('/')}/.", run_dir_ws], cwd=None, timeout_s=120.0,
              reads=[layout["launch_authorizing_directory"]], produces=[run_dir_ws], note="copy ONLY the launch-authorizing overlay directory (wired_v3_timed/<run>)"),
        _step("X06_ledger_follower", "run_background", "read_only_observation", ledger_argv, cwd=repo_wsl, produces=[outp("ledger/journal")], note="bounded by --max-polls; terminated at X09 if still running"),
    ]
    if request["prometheus"]["mode"] == "before_after":
        prom = request["prometheus"]
        targets = [arg for t in layout["metrics_targets"] for arg in ("--target", t)]
        for phase in ("before",):
            steps.append(_step("X07_prometheus_before", "run_foreground", "read_only_observation", ["python3", "scripts/e1/e1_metrics_scraper.py", "scrape", *targets, "--phase", phase, "--out-dir", outp("prometheus/before"),
                                                                       "--interval", str(prom["interval_s"]), "--max-rounds", str(int(prom["max_rounds"])), "--timeout", str(prom["timeout_s"]), "--execute"],
                               cwd=repo_wsl, produces=[outp("prometheus/before")], timeout_s=prom["interval_s"] * prom["max_rounds"] + 30.0, note="read-only HTTP GETs of the metrics endpoints"))
    steps.append(_step("X08_caliper", "tx_foreground", "transaction_producing", list(layout["caliper_argv"]), cwd=None, reads=[run_dir_ws], produces=[run_dir_ws + "/issuelog", run_dir_ws + "/recorder"],
                       timeout_s=limits["caliper_timeout_s"], note="the ONLY transaction-producing step; attempted at most once; never retried"))
    steps.append(_step("X09_collectors_stop", "collect_background", "offline_internal", None, cwd=None, timeout_s=limits["collector_wait_s"], note="wait for the follower up to collector_wait_s, then terminate"))
    if request["prometheus"]["mode"] == "before_after":
        prom = request["prometheus"]
        targets = [arg for t in layout["metrics_targets"] for arg in ("--target", t)]
        steps.append(_step("X10_prometheus_after", "run_foreground", "read_only_observation", ["python3", "scripts/e1/e1_metrics_scraper.py", "scrape", *targets, "--phase", "after", "--out-dir", outp("prometheus/after"),
                                                                   "--interval", str(prom["interval_s"]), "--max-rounds", str(int(prom["max_rounds"])), "--timeout", str(prom["timeout_s"]), "--execute"],
                           cwd=repo_wsl, produces=[outp("prometheus/after")], timeout_s=prom["interval_s"] * prom["max_rounds"] + 30.0, note="read-only"))
    steps += [_step("X11_clock_post", "gate_clock", "read_only_observation", probe["argv"], cwd=None, produces=[layout["host_evidence"]["clock_post"]], timeout_s=clock["probe_timeout_s"], note="same parameters; AGREE required"),
              _step("X12_config_final", "gate_config", "read_only_observation", None, cwd=repo_wsl, produces=[final["block_path"], final["decoded_json_path"]],
                    note="read-only; must equal the initial inspection (same sequence, block and digest)"),
              _step("X13_integrity", "gate_integrity", "offline_internal", None, cwd=None, produces=[layout["host_evidence"]["integrity_manifest"]], note="hash every file under the output directory and the worker evidence root")]
    by_class = lambda klass: [st["step"] for st in steps if st["class"] == klass]
    plan = {
        "schema": PLAN_SCHEMA, "version": EXECUTION_VERSION, "created_utc": created, "status": "PLANNED_NOT_AUTHORIZED_NOT_EXECUTED",
        "real_launch_enabled": REAL_LAUNCH_ENABLED, "request": json.loads(json.dumps(request)), "layout": layout,
        "candidate_manifest": {"path": request["candidate_manifest"]["path"], "sha256": request["candidate_manifest"]["sha256"], "status": manifest.get("status"), "entries": len(entries)},
        "artifact_hashes": {**tool_hashes, **{f"rendered:{k}": v for k, v in json.loads((Path(layout["launch_authorizing_directory"]).parents[1] / "a0p_overlay_manifest.json").read_text(encoding="utf-8"))["files"].items()}},
        "variant": request["variant"], "channel": channel, "cell": cell, "cell_id": request["cell_id"], "run_id": run_id, "worker_count": 1,
        "envelope": {"planned_transactions": planned_tx, "planned_total_duration_s": planned_total, "limits": dict(limits)},
        "config_inspection": {"initial": initial, "final": final}, "clock_probe": probe, "steps": steps,
        "permitted_read_only_operations": by_class("read_only_observation"),
        "permitted_local_staging_operations": by_class("local_staging"),
        "permitted_transaction_producing_operations": by_class("transaction_producing"),
        "mounts": {"caliper_container": layout["caliper_container"], "container_workspace": rl.CALIPER_WS_CONTAINER, "host_workspace": host_ws, "run_dir_host": run_dir_ws,
                   "run_dir_container": layout["container_run_dir"]},
        "termination": {"grace_s": limits["terminate_grace_s"], "behavior": "runner.terminate(handle, grace_s): polite stop, wait grace_s, then kill; the partial evidence is kept, never deleted",
                        "interruption": "KeyboardInterrupt or SystemExit: collectors terminated, status INTERRUPTED written, then the interruption is re-raised"},
        "retry_policy": "none: the transaction-producing step is attempted at most once",
        "claims_not_made": ["the argument vectors work on the target machine (never run)", "Caliper loads the modules (never run)", "the clock probe command works or has a usable round trip (never run)",
                            "the channel exists (never queried)"],
    }
    plan["plan_sha256"] = _digest(plan, drop=("plan_sha256", "created_utc"))
    return plan


def verify_plan(plan: Mapping[str, Any], *, repo_root: Path) -> list[dict[str, str]]:
    """Re-verify a stored plan against the files on disk. An empty list means it is intact."""
    problems: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        problems.append({"code": code, "message": message})

    if plan.get("schema") != PLAN_SCHEMA:
        return [{"code": "plan_schema", "message": f"schema {plan.get('schema')!r}"}]
    if _digest(plan, drop=("plan_sha256", "created_utc")) != plan.get("plan_sha256"):
        bad("plan_digest", "the plan was edited after it was written")
    if plan.get("real_launch_enabled") is not False:
        bad("real_launch_flag", "a plan may not carry real_launch_enabled true")
    for path, want in (plan.get("artifact_hashes") or {}).items():
        if path.startswith("rendered:"):
            target = Path(plan["layout"]["launch_authorizing_directory"]).parents[1] / path[len("rendered:"):]
        else:
            target = Path(repo_root) / path
        if not target.is_file():
            bad("artifact_missing", path)
        elif pi.sha256_file(target) != want:
            bad("artifact_hash", f"{path} differs from the plan")
    problems += [{"code": p["code"], "message": p["message"]} for p in rl.verify_layout(plan["layout"], repo_root=Path(repo_root))]
    problems += [{"code": "request_invalid", "message": f"{i['field']}: {i['message']}"} for i in validate_request(plan["request"])]
    planned_tx = sum(r["rate_tps"] * r["tx_duration_s"] for r in plan["layout"]["rounds"])
    if planned_tx != plan["envelope"]["planned_transactions"] or planned_tx > plan["envelope"]["limits"]["max_transactions"]:
        bad("envelope", f"planned transactions {planned_tx} do not fit the envelope")
    if [s["step"] for s in plan["steps"] if s["class"] == "transaction_producing"] != plan["permitted_transaction_producing_operations"] or len(plan["permitted_transaction_producing_operations"]) != 1:
        bad("transaction_step", "exactly one transaction-producing step is required")
    for stage in ("initial", "final"):
        for step in plan["config_inspection"][stage]["steps"]:
            try:
                ci.validate_read_only_argv(step["argv"])
            except ci.MutationRefused as exc:
                bad("mutation_capable_vector", str(exc))
    return problems


# ------------------------------------------------------------------------------------------------ authorization
APPROVAL_KEYS = ("status", "approved_by", "approved_utc", "record_path", "record_sha256")
BOUND_FIELDS = ("variant", "channel", "cell_id", "run_id", "worker_count")


def authorization_template(plan: Mapping[str, Any]) -> dict[str, Any]:
    """The future authorization record with every approval field UNSET. The bound values are copied from the plan so the owner reviews them; copying is not approving."""
    return {
        "schema": AUTH_SCHEMA, "synthetic": False, "plan_sha256": plan["plan_sha256"], "manifest": dict(plan["candidate_manifest"]),
        "artifact_hashes": dict(plan["artifact_hashes"]), "variant": plan["variant"], "channel": plan["channel"], "cell_id": plan["cell_id"], "run_id": plan["run_id"],
        "workload": dict(plan["request"]["workload"]), "recorder_version": plan["request"]["recorder_version"], "worker_count": plan["worker_count"],
        "rounds": [dict(r) for r in plan["request"]["rounds"]], "limits": dict(plan["envelope"]["limits"]), "operator_timing": dict(plan["request"]["operator_timing"]),
        "output_dir": plan["layout"]["output_dir"],
        "permitted_read_only_operations": list(plan["permitted_read_only_operations"]),
        "permitted_local_staging_operations": list(plan["permitted_local_staging_operations"]),
        "permitted_transaction_producing_operations": list(plan["permitted_transaction_producing_operations"]),
        "stop_conditions": list(STOP_CONDITIONS), "prohibited_configuration_changes": "ALL",
        "approval": {k: None for k in APPROVAL_KEYS},
        "note": "TEMPLATE. Every approval field is unset. Filling a field is the owner's act; this tool never does it.",
    }


def verify_authorization(auth: Any, plan: Mapping[str, Any], *, repo_root: Path, allow_synthetic: bool = False) -> dict[str, Any]:
    """Findings (empty = every check passed). ``real_launch_permitted`` is always False while ``REAL_LAUNCH_ENABLED`` is False."""
    findings: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        findings.append({"code": code, "message": message})

    if not isinstance(auth, Mapping):
        bad("authorization_missing", "no authorization record was supplied")
        return {"valid": False, "findings": findings, "real_launch_permitted": False}
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
        if not (isinstance(approval["record_sha256"], str) and SHA.fullmatch(approval["record_sha256"])):
            bad("approval_record_hash", "approval.record_sha256 must be 64 lowercase hex characters")
        else:
            record = Path(str(approval["record_path"]))
            if not record.is_file():
                bad("approval_record_unreadable", f"{record} is not a readable file")
            elif pi.sha256_file(record) != approval["record_sha256"]:
                bad("approval_record_hash", "the approval document's hash differs from the one recorded")
    if auth.get("synthetic") is True and not allow_synthetic:
        bad("synthetic_authorization", "a synthetic authorization is accepted only for a mock runner and is never a real approval")
    if auth.get("plan_sha256") != plan.get("plan_sha256"):
        bad("plan_digest_mismatch", "the authorization binds a different plan")
    if (auth.get("manifest") or {}).get("sha256") != plan["candidate_manifest"]["sha256"] or (auth.get("manifest") or {}).get("path") != plan["candidate_manifest"]["path"]:
        bad("manifest_mismatch", "the authorization names another candidate manifest")
    if auth.get("artifact_hashes") != plan["artifact_hashes"]:
        extra = sorted(set(auth.get("artifact_hashes") or {}) ^ set(plan["artifact_hashes"]))
        wrong = sorted(k for k in plan["artifact_hashes"] if (auth.get("artifact_hashes") or {}).get(k) != plan["artifact_hashes"][k])
        bad("artifact_hash_mismatch", f"differing or missing artifacts: {wrong or extra}")
    for key in BOUND_FIELDS:
        if auth.get(key) != plan.get(key):
            bad(f"{key}_mismatch", f"authorized {auth.get(key)!r}, planned {plan.get(key)!r}")
    try:
        cs.select_experiment_channel(auth.get("channel"), purpose="a bounded execution", option="channel")
    except cs.ChannelSelectionError as exc:
        bad("channel_refused", str(exc))
    if auth.get("worker_count") != 1 or isinstance(auth.get("worker_count"), bool):
        bad("worker_count_unsupported", "exactly one worker")
    if auth.get("variant") not in EXECUTABLE_VARIANTS:
        bad("variant_not_executable", f"{auth.get('variant')!r}")
    for key, want in (("workload", plan["request"]["workload"]), ("recorder_version", plan["request"]["recorder_version"]), ("rounds", plan["request"]["rounds"]),
                      ("operator_timing", plan["request"]["operator_timing"]), ("output_dir", plan["layout"]["output_dir"])):
        if auth.get(key) != want:
            bad(f"{key}_mismatch", f"authorized {auth.get(key)!r}")
    limits = auth.get("limits")
    if not (isinstance(limits, Mapping) and set(limits) == set(LIMIT_KEYS) and all(_number(limits[k]) and limits[k] > 0 for k in LIMIT_KEYS)):
        bad("limits_unbounded", f"every one of {list(LIMIT_KEYS)} must be present and > 0; execution is never unbounded")
    else:
        env = plan["envelope"]
        for name, planned, limit in (("transactions", env["planned_transactions"], limits["max_transactions"]), ("total duration", env["planned_total_duration_s"], limits["max_total_duration_s"]),
                                     ("rate", max(r["rate_tps"] for r in plan["layout"]["rounds"]), limits["max_rate_tps"]),
                                     ("round duration", max(r["tx_duration_s"] for r in plan["layout"]["rounds"]), limits["max_round_duration_s"])):
            if planned > limit:
                bad("planned_exceeds_authorized_limit", f"planned {name} {planned} exceeds the authorized {limit}")
        if limits != plan["envelope"]["limits"]:
            bad("limits_differ_from_plan", "the authorized limits are not the limits the plan was built for")
    if sorted(auth.get("permitted_read_only_operations") or []) != sorted(plan["permitted_read_only_operations"]):
        bad("read_only_operations_mismatch", "the permitted read-only operations differ from the plan's")
    if sorted(auth.get("permitted_local_staging_operations") or []) != sorted(plan["permitted_local_staging_operations"]):
        bad("local_staging_operations_mismatch", "the permitted local staging operations differ from the plan's")
    if auth.get("permitted_transaction_producing_operations") != plan["permitted_transaction_producing_operations"]:
        bad("transaction_operations_mismatch", "the permitted transaction-producing operations differ from the plan's (exactly one)")
    if sorted(auth.get("stop_conditions") or []) != sorted(STOP_CONDITIONS):
        bad("stop_conditions", f"the stop conditions must be exactly {list(STOP_CONDITIONS)}")
    if auth.get("prohibited_configuration_changes") != "ALL":
        bad("configuration_changes_not_prohibited", "prohibited_configuration_changes must be 'ALL'")
    return {"valid": not findings, "findings": findings, "real_launch_permitted": False if not REAL_LAUNCH_ENABLED else not findings,
            "synthetic": auth.get("synthetic") is True, "note": "a valid record is still not a launch: real launch is disabled in this version"}


# ------------------------------------------------------------------------------------------------ future adapter (injected runner only)
@dataclass
class RunResult:
    returncode: int | None
    stdout: bytes = b""
    stderr: bytes = b""
    timed_out: bool = False
    started_utc: str | None = None
    ended_utc: str | None = None


@dataclass
class Deps:
    """Everything ``execute_bounded`` needs from outside. All are injected; none is a default."""

    runner: Any                                     # kind, run(argv, timeout_s, cwd), start_background(argv, cwd), finish(handle, timeout_s), terminate(handle, grace_s)
    now_utc: Callable[[], datetime]
    wall_ns: Callable[[], int]
    mono_ns: Callable[[], int]
    read_file: Callable[[str], bytes]
    hash_tree: Callable[[str], dict[str, str]]
    sha256_file: Callable[[str], str] = field(default=lambda p: pi.sha256_file(Path(p)))


class _CallableRunner:
    """Adapts the runner to the (argv, timeout) -> (code, out, err) callable the inspection and clock modules take. It mirrors the runner's kind."""

    def __init__(self, runner: Any):
        self.kind = getattr(runner, "kind", None)
        self._runner = runner

    def __call__(self, argv: Sequence[str], timeout_s: float) -> tuple[int | None, bytes, bytes]:
        result = self._runner.run(list(argv), timeout_s, None)
        if result.timed_out:
            return 124, result.stdout, result.stderr
        return result.returncode, result.stdout, result.stderr


def _iso(deps: Deps) -> str:
    return deps.now_utc().astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _create_new(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "xb") as handle:
        handle.write(data)


def execute_bounded(plan: Mapping[str, Any], auth: Mapping[str, Any], deps: Deps, *, repo_root: Path, allow_synthetic: bool = False) -> dict[str, Any]:
    """Run an authorized plan through an INJECTED runner. Refused (nothing created) unless the plan is intact, the authorization verifies and the runner is ``mock`` or ``fixture``."""
    kind = getattr(deps.runner, "kind", None)
    if kind == "process" and not REAL_LAUNCH_ENABLED:
        raise LaunchRefused("real launch is disabled in this version; a process runner is refused")
    if kind not in PERMITTED_RUNNER_KINDS:
        raise LaunchRefused(f"runner kind {kind!r} is not accepted (only {list(PERMITTED_RUNNER_KINDS)} while real launch is disabled)")
    plan_problems = verify_plan(plan, repo_root=Path(repo_root))
    if plan_problems:
        raise LaunchRefused("the plan is not intact", plan_problems)
    verdict = verify_authorization(auth, plan, repo_root=Path(repo_root), allow_synthetic=allow_synthetic)
    if not verdict["valid"]:
        raise LaunchRefused("the authorization does not verify", verdict["findings"])
    out = Path(plan["layout"]["output_dir"])
    if out.exists():
        raise LaunchRefused(f"output directory {out} already exists; nothing is overwritten")
    out.mkdir(parents=True, exist_ok=False)
    host = plan["layout"]["host_evidence"]
    log_path = out / host["execution_log"]
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def log(event: str, **data: Any) -> None:
        row = {"t_utc": _iso(deps), "event": event, **data}
        with open(log_path, "ab") as handle:                       # append-only
            handle.write((json.dumps(row, sort_keys=True) + "\n").encode("utf-8"))

    def write_json(relative: str, document: Any) -> None:
        _create_new(out / relative, (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8"))

    write_json(host["execution_plan"], plan)
    write_json(host["approval_reference"], {"authorization": dict(auth), "verdict": verdict})
    log("start", plan_sha256=plan["plan_sha256"], runner_kind=kind, synthetic_authorization=bool(auth.get("synthetic")))
    state: dict[str, Any] = {"status": "COMPLETED", "halted_at": None, "tx_attempts": 0, "background": {}, "results": {}, "inspections": {}, "clock": {}, "errors": []}
    callable_runner = _CallableRunner(deps.runner)
    clock_req = plan["request"]["clock"]

    def halt(status: str, step_id: str, reason: str) -> None:
        state["errors"].append({"step": step_id, "reason": reason})
        if state["halted_at"] is None:                              # the first failure names the outcome; later ones are only recorded
            state["status"], state["halted_at"] = status, step_id
        log("halt", step=step_id, status=status, reason=reason)

    def record_output(step_id: str, result: RunResult) -> None:
        for suffix, data in (("stdout", result.stdout), ("stderr", result.stderr)):
            _create_new(out / "process_logs" / f"{step_id}.{suffix}", data or b"")

    def stop_collectors(wait_s: float) -> None:
        for sid, handle in list(state["background"].items()):
            result = deps.runner.finish(handle, wait_s)
            if result.timed_out:
                result = deps.runner.terminate(handle, plan["termination"]["grace_s"])
                log("collector_terminated", step=sid)
            record_output(sid, result)
            state["results"][sid] = {"returncode": result.returncode, "timed_out": result.timed_out}
            del state["background"][sid]

    def run_step(step: Mapping[str, Any]) -> bool:
        """True to continue. A False return has already called ``halt``."""
        sid, kind_ = step["step"], step["kind"]
        pre_tx = state["tx_attempts"] == 0
        log("step_begin", step=sid, kind=kind_, klass=step["class"], argv=step["argv"])
        if kind_ == "gate_artifacts":
            for path, want in plan["artifact_hashes"].items():
                target = (str(Path(plan["layout"]["launch_authorizing_directory"]).parents[1] / path[len("rendered:"):]) if path.startswith("rendered:") else str(Path(repo_root) / path))
                if deps.sha256_file(target) != want:
                    halt("HALTED_BEFORE_TRANSACTIONS", sid, f"artifact {path} differs from the authorized hash")
                    return False
            return True
        if kind_ == "gate_config":
            stage = "initial" if pre_tx else "final"
            try:
                evidence = ci.execute_inspection(plan["config_inspection"][stage], callable_runner, read_file=deps.read_file, captured_utc=_iso(deps))
            except (ci.InspectionError, ci.MutationRefused) as exc:
                halt("HALTED_BEFORE_TRANSACTIONS" if pre_tx else "FINAL_CHECK_FAILED", sid, f"{type(exc).__name__}: {exc}")
                return False
            state["inspections"][stage] = evidence
            write_json(f"config_inspection/{stage}/evidence.json", evidence)
            if evidence["verdict"] != "MATCH":
                halt("HALTED_BEFORE_TRANSACTIONS" if pre_tx else "FINAL_CHECK_FAILED", sid, f"configuration differs from the approved cell in {evidence['differing_fields']}")
                return False
            if not pre_tx and "initial" in state["inspections"]:
                same = ci.compare_inspections(state["inspections"]["initial"], evidence)
                if not same["unchanged"]:
                    halt("CONFIGURATION_CHANGED", sid, "; ".join(same["differences"]))
                    return False
            return True
        if kind_ == "gate_clock":
            phase = "pre_run" if pre_tx else "post_run"
            probe = wp.WslClockProbe(callable_runner, wall_ns=deps.wall_ns, mono_ns=deps.mono_ns, argv=step["argv"], remote_resolution_ns=clock_req["remote_resolution_ns"],
                                     timeout_s=clock_req["probe_timeout_s"], phase=phase, reference_domain=clock_req["reference_domain"], remote_domain=clock_req["remote_domain"])
            record = wp.collect_phase(probe, clock_req["samples_per_phase"])
            state["clock"][phase] = record
            write_json(step["writes"][0], record)
            clock_verdict = wp.evaluate(list(state["clock"].values()), tolerance_ms=clock_req["tolerance_ms"], min_valid_samples=clock_req["min_valid_samples"],
                                        max_wall_monotonic_disagreement_ms=clock_req["max_wall_monotonic_disagreement_ms"])
            write_json(host["clock_verdict"] if not pre_tx else "clock/pre_run_verdict.json", clock_verdict)
            if clock_verdict["verdict"] != wp.cp.AGREE:
                halt("HALTED_BEFORE_TRANSACTIONS" if pre_tx else "CLOCK_NOT_AGREE", sid, f"clock verdict {clock_verdict['verdict']}")
                return False
            return True
        if kind_ in ("run_foreground", "tx_foreground"):
            if kind_ == "tx_foreground":
                if state["tx_attempts"] >= 1:
                    halt("REFUSED_SECOND_TRANSACTION_ATTEMPT", sid, "a transaction-producing step is attempted at most once")
                    return False
                state["tx_attempts"] += 1
            result = deps.runner.run(list(step["argv"]), step["timeout_s"], step["cwd"])
            record_output(sid, result)
            state["results"][sid] = {"returncode": result.returncode, "timed_out": result.timed_out}
            log("step_end", step=sid, returncode=result.returncode, timed_out=result.timed_out)
            if result.timed_out:
                halt("TIMED_OUT" if kind_ == "tx_foreground" else "HALTED_BEFORE_TRANSACTIONS", sid, f"exceeded {step['timeout_s']} s")
                return False
            if result.returncode != 0:
                halt("FAILED_DURING_TRANSACTIONS" if kind_ == "tx_foreground" else ("HALTED_BEFORE_TRANSACTIONS" if pre_tx else "POST_STEP_FAILED"), sid, f"exit code {result.returncode}")
                return False
            return True
        if kind_ == "run_background":
            state["background"][sid] = deps.runner.start_background(list(step["argv"]), step["cwd"])
            return True
        if kind_ == "collect_background":
            stop_collectors(step["timeout_s"])
            return True
        if kind_ == "gate_integrity":
            return True                                             # the hashes are written once, in the final block below, so a halt still produces them
        raise LaunchRefused(f"unknown step kind {kind_!r}")

    def allowed_after_halt(step: Mapping[str, Any]) -> bool:
        """After a halt only collector shutdown and evidence hashing run; after the transaction step has been attempted, read-only observation also runs."""
        if step["kind"] in ("collect_background", "gate_integrity"):
            return True
        return state["tx_attempts"] == 1 and step["class"] == "read_only_observation" and step["kind"] != "run_background"

    interruption: BaseException | None = None
    try:
        for step in plan["steps"]:
            if state["halted_at"] is not None and not allowed_after_halt(step):
                continue
            try:
                run_step(step)
            except LaunchRefused:
                raise
            except Exception as exc:                                # noqa: BLE001 - a runner or file error becomes a recorded halt, never a retry
                halt("ERROR", step["step"], f"{type(exc).__name__}: {exc}")
    except BaseException as exc:                                    # KeyboardInterrupt, SystemExit, LaunchRefused: evidence first, then re-raise
        interruption = exc
        state["errors"].append({"step": "interrupt", "reason": type(exc).__name__})
        if state["halted_at"] is None:
            state["status"], state["halted_at"] = "INTERRUPTED", "interrupt"
        log("interrupt", error=type(exc).__name__)
    finally:
        for sid, handle in list(state["background"].items()):
            try:
                result = deps.runner.terminate(handle, plan["termination"]["grace_s"])
                record_output(sid, result)
                log("collector_terminated", step=sid)
            except Exception as exc:                                # noqa: BLE001 - cleanup must not hide the primary outcome
                state["errors"].append({"step": sid, "reason": f"terminate failed: {type(exc).__name__}: {exc}"})
            state["background"].pop(sid, None)
        try:
            write_json(host["integrity_manifest"], {"schema": "a0s_integrity_manifest/1", "files": {"output_dir": deps.hash_tree(str(out)),
                                                                                                     "worker_evidence_root": deps.hash_tree(plan["mounts"]["run_dir_host"])}})
        except Exception as exc:                                    # noqa: BLE001
            state["errors"].append({"step": "integrity", "reason": f"{type(exc).__name__}: {exc}"})
        status = {"schema": STATUS_SCHEMA, "version": EXECUTION_VERSION, "status": state["status"], "halted_at": state["halted_at"], "transaction_attempts": state["tx_attempts"],
                  "steps": state["results"], "errors": state["errors"], "plan_sha256": plan["plan_sha256"], "runner_kind": kind, "partial_evidence_preserved": state["status"] != "COMPLETED",
                  "real_process_started": False, "ended_utc": _iso(deps)}
        try:
            write_json(host["run_status"], status)
        except OSError as exc:
            status["errors"].append({"step": "status", "reason": str(exc)})
    if interruption is not None:
        raise interruption
    return status
