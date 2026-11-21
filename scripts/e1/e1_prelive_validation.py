"""Pre-live validation orchestrator: prepare and offline preflight ONLY (task A0e, 2026-11-21).

One reproducible path for a future bounded validation, in explicit phases. This tool is OFFLINE by construction: it imports
no process, network or container API (an AST test asserts that), and its ``execute`` phase has no implementation.

  prepare    Validate the operator parameters (one contract), vet the channel (A0b rules), render the Caliper run with the
             pinned renderer, select recorder v2 with the A0d wrapper, and write a launch manifest into a NEW, isolated
             directory. It never starts Caliper and never touches Fabric, Docker or WSL.
  preflight  Re-read a prepared directory and check it: references, recorder-v2 selection in the generated benchmark, declared
             output paths, hashes, conflicting values, protected channels. Optionally verify a candidate manifest, the frozen
             recovery manifest, and operator-supplied clock samples. Writes a machine-readable and a human-readable result.
             It always reports ``live_ready: false``: an offline pass is not a live validation.
  execute    Refused, always. A future task with separate owner authorization has to supply its own versioned execution tool.
             No environment variable, flag or default turns this phase on.
  candidate-manifest
             Write a candidate integrity manifest (hashes only; never an approval) to a path that does not exist.

The historical capture runner ``scripts/e1/run_contemporaneous_capture.py`` is NOT an entry point of this workflow: it copies a
WSL template, never selects recorder v2, and launches Caliper in the same step. It is left unchanged and is listed in the
candidate manifest only as an excluded legacy file.

Exit codes: 0 pass, 1 a check failed, 2 invalid request or parameters (nothing created), 3 inconclusive, 5 execute refused.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO / "src"))

import e1_la2_render as lr                      # noqa: E402  (the pinned renderer, imported not edited)
import e1_recorder_v2_wire as wire               # noqa: E402  (A0d wrapper)
from fabric_perf.controllers import channel_selection as cs        # noqa: E402
from fabric_perf.controllers import clock_preflight as cp           # noqa: E402
from fabric_perf.controllers import prelive_contract as pc          # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi         # noqa: E402

TOOL_VERSION = "e1_prelive_validation 0.1 (A0e, 2026-11-21)"
LAUNCH_SCHEMA = "prelive_launch_manifest/1"
RESULT_SCHEMA = "prelive_preflight_result/1"
STATUS_PREPARED = "PREPARED_OFFLINE_NOT_EXECUTED"
PREPARE_ID = re.compile(r"[A-Za-z0-9_.-]{1,60}")
INCOMPLETE_MARKER = "PREPARE_INCOMPLETE.json"
EXIT_EXECUTE_REFUSED = 5

PASS, FAIL, INCONCLUSIVE, NOT_CHECKED = "PASS", "FAIL", "INCONCLUSIVE", "NOT_CHECKED"
# Directories whose contents are protected evidence: nothing declared by a run may point into them.
PROTECTED_EVIDENCE_PREFIXES = ("Integration/system_integration/observability_recovery_2026-11-17", "experiments/results",
                               "data/raw", "data/processed")

# Tool files whose hashes a prepared run records and a preflight re-checks (role -> repo-relative path).
TOOL_FILES = {
    "renderer": "scripts/e1/e1_la2_render.py", "recorder_wiring": "scripts/e1/e1_recorder_v2_wire.py",
    "recorder_v1": "scripts/e1/caliper/e1_per_tx_recorder.js", "recorder_v2": "scripts/e1/caliper/" + wire.V2_NAME,
    "ledger_follower": "scripts/e1/e1_ledger_follow.py", "ledger_collector": "src/fabric_perf/controllers/ledger_evidence.py",
    "telemetry_adapter": "src/fabric_perf/controllers/telemetry.py",
    "shadow_capture": "Fabric_Live_Testbed/controllers/run_production_shadow_validation.py",
    "stage3_contract": "src/fabric_perf/controllers/stage3_frame_quality.py",
    "channel_policy": "src/fabric_perf/controllers/channel_selection.py",
    "parameter_contract": "src/fabric_perf/controllers/prelive_contract.py",
}

CANDIDATE_SPECS = [
    pi.FileSpec("scripts/e1/e1_prelive_validation.py", "A0e launch orchestrator (prepare, preflight, execute refusal)", "launch_orchestrator"),
    pi.FileSpec("src/fabric_perf/controllers/prelive_contract.py", "single operator-parameter contract", "parameter_contract"),
    pi.FileSpec("src/fabric_perf/controllers/clock_preflight.py", "offline clock-agreement evidence evaluator", "clock_preflight"),
    pi.FileSpec("src/fabric_perf/controllers/prelive_integrity.py", "candidate manifest builder and verifier", "integrity"),
    pi.FileSpec("src/fabric_perf/controllers/channel_selection.py", "A0b channel validation (protected/reserved/malformed)", "channel_policy"),
    pi.FileSpec("scripts/e1/e1_la2_render.py", "pinned Caliper run renderer (writes the v1 monitor)", "renderer_pinned",
                pi.PINNED_ELSEWHERE_UNCHANGED, "f77a83e7de106b3c1dac28f93333e8dbaf67ea869836944b906f9bc1aabe3d83"),
    pi.FileSpec("scripts/e1/e1_recorder_v2_wire.py", "A0d wrapper selecting recorder v2 in a new run directory", "recorder_wiring"),
    pi.FileSpec("scripts/e1/caliper/e1_per_tx_recorder.js", "recorder v1 (v2 requires it as a sibling)", "recorder_v1_pinned",
                pi.PINNED_ELSEWHERE_UNCHANGED, wire.PINNED_V1_SHA256),
    pi.FileSpec("scripts/e1/caliper/" + wire.V2_NAME, "recorder v2: continuous live stream and latency_ms", "recorder_v2"),
    pi.FileSpec("fabric-testbed/caliper/workloads/e1_create_asset.js", "Caliper workload module copied into the run", "workload_module"),
    pi.FileSpec("fabric-testbed/caliper/workloads/e1_workload_core.js", "workload helper copied into the run", "workload_module"),
    pi.FileSpec("scripts/e1/e1_telemetry_common.py", "renderer dependency", "renderer_dependency"),
    pi.FileSpec("scripts/e1/e1_run_orchestrator.py", "renderer dependency (plan validation only)", "renderer_dependency"),
    pi.FileSpec("scripts/e1/e1_channel_run.py", "renderer dependency (request/network validation)", "renderer_dependency"),
    pi.FileSpec("scripts/e1/e1_ledger_follow.py", "A0d continuous ledger follower (peer CLI; WSL only; never run)", "ledger_follower"),
    pi.FileSpec("src/fabric_perf/controllers/ledger_evidence.py", "A0d append-only ledger collector and windowed rate", "ledger_collector"),
    pi.FileSpec("scripts/e1/e1_block_fetch.py", "block decoder and peer CLI argv builders used by the follower", "ledger_decoder"),
    pi.FileSpec("scripts/e1/e1_ledger_blocks.py", "ledger block decoder", "ledger_decoder"),
    pi.FileSpec("src/fabric_perf/controllers/telemetry.py", "continuous Caliper + ledger telemetry adapter", "telemetry_adapter"),
    pi.FileSpec("src/fabric_perf/controllers/client_latency.py", "single client-latency definition", "telemetry_adapter"),
    pi.FileSpec("src/fabric_perf/controllers/live_integration.py", "integrated controller runner (shadow)", "stage3_runner"),
    pi.FileSpec("src/fabric_perf/controllers/stage3_frame_quality.py", "Stage 3 frame-quality contract", "stage3_contract"),
    pi.FileSpec("Fabric_Live_Testbed/controllers/run_production_shadow_validation.py", "read-only shadow capture CLI (now with --adapter-window-s)", "shadow_capture"),
    pi.FileSpec("Integration/system_integration/run_integrated_controller.py", "Stage 3 replay/shadow runner CLI over captured frames", "stage3_runner"),
    pi.FileSpec("scripts/e1/run_contemporaneous_capture.py", "legacy all-in-one capture runner: NOT an approved entry point (v1 only, template copy, launches Caliper)",
                "legacy_not_approved", pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT,
                "81dc1f4f77f85387bf9578e4293f9793051b93d5724496586878797ef904bd54"),
]
CANDIDATE_OPEN_ITEMS = (
    "Owner approval of every PENDING_OWNER_APPROVAL file (hash pinning) has not been given.",
    "The dedicated validation channel name and the experimental cell are not approved.",
    "Runtime inputs chosen at launch are not hashed here: the HFBTP artifact directory and the Flashbots series used by the "
    "predictive controller, the network description, the verified channel-configuration block.",
    "telemetry.py, live_integration.py and live_foundation.py pins in older appendices are stale (A0d report section 14).",
)


class PrepareError(ValueError):
    """Preparation cannot proceed; nothing is left that looks like a finished run."""


def _utc(now: datetime | None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(timespec="seconds")


def _sha_tree(root: Path, *, skip: tuple[str, ...] = ()) -> dict[str, str]:
    return {p.relative_to(root).as_posix(): pi.sha256_file(p) for p in sorted(root.rglob("*"))
            if p.is_file() and p.relative_to(root).as_posix() not in skip and not p.relative_to(root).parts[0] == "preflight_results"}


def load_parameters(path: Path) -> pc.ValidatedParameters:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise pc.PreliveParameterError([pc.ParameterIssue("unreadable", str(path), str(exc))]) from None
    except ValueError as exc:
        raise pc.PreliveParameterError([pc.ParameterIssue("not_json", str(path), str(exc))]) from None
    return pc.validate_operator_parameters(raw)


def expected_live_files(plan: dict[str, Any], run_id: str) -> dict[str, str]:
    """Recorder v2 live-stream names for the measured round, derived from the render plan (recorder v1's ``_base()``)."""
    caliper = plan["caliper"]
    base = f"{run_id}_w{caliper['worker']}_r{caliper['measured_round']}_{caliper['measured_label']}"
    directory = caliper["recorder_dir"].rstrip("/")
    return {"live_csv": f"{directory}/{base}.live.csv", "live_submits_csv": f"{directory}/{base}.live.submits.csv",
            "live_open_json": f"{directory}/{base}.live.open.json", "live_final_json": f"{directory}/{base}.live.final.json"}


def future_execution_steps(values: dict[str, Any], plan: dict[str, Any], evidence_dir: Path, live: dict[str, str]) -> list[dict[str, Any]]:
    """The documented invocations for a LATER, separately authorized run. Text only: this tool never invokes any of them."""
    channel, run_id = values["channel"], values["run_id"]
    journal = (evidence_dir / "ledger_journal").as_posix()
    endpoints = [arg for item in values["telemetry_sources"]["prometheus_endpoints"] for arg in ("--prom-endpoint", item)]
    collector, adapter, stage3 = values["ledger_collector"], values["telemetry_adapter"], values["stage3"]
    base = {"status": "NOT_AUTHORIZED_NOT_RUN", "invoked": False}
    stage3_args = ["--stage3-frame-quality", "--max-consecutive-abstentions", str(stage3["max_consecutive_abstentions"])]
    if stage3["max_initialization_frames"] is not None:
        stage3_args += ["--max-initialization-frames", str(stage3["max_initialization_frames"])]
    return [
        {**base, "step": "R06_clock_probe", "kind": "read_only_observation", "authorization": "OWNER_GO_CLOCK_PROBE",
         "argv": None, "note": "Collect the samples listed in clock_preflight.REQUIRED_MEASUREMENTS, then evaluate them with "
                               "python -m fabric_perf.controllers.clock_preflight. No probe implementation exists in this repository."},
        {**base, "step": "R07_ledger_follow", "kind": "read_only_observation", "authorization": "OWNER_GO_PEER_READ",
         "argv": ["python3", "scripts/e1/e1_ledger_follow.py", "--channel", channel, "--network", "<WSL path of the network description>",
                  "--out-dir", "<WSL path of " + journal + ">", "--collector-run-id", f"{run_id}-ledger",
                  "--max-polls", str(collector["max_polls"]), "--poll-interval-s", str(collector["poll_interval_s"]), "--execute"],
         "note": "Linux/WSL only; the journal directory must be created by the follower (it must not exist)."},
        {**base, "step": "R09_caliper_smoke", "kind": "transaction_producing", "authorization": "OWNER_GO_CALIPER_RUN",
         "argv": plan["caliper"]["argv"],
         "note": "Run inside the Caliper container after the wired run directory is copied to the workspace. Produces transactions."},
        {**base, "step": "R10_shadow_capture", "kind": "read_only_observation", "authorization": "OWNER_GO_PROMETHEUS_READ",
         "argv": ["python", "Fabric_Live_Testbed/controllers/run_production_shadow_validation.py", "--run-id", run_id,
                  "--channel", channel, "--out", (evidence_dir / "shadow_capture.json").as_posix(),
                  "--recorder-csv", live["live_csv"], "--submits-csv", live["live_submits_csv"],
                  "--ledger-tx-csv", journal + "/ledger_tx.csv", "--ledger-observations-csv", journal + "/ledger_observations.csv",
                  "--recorder-latency", adapter["recorder_latency"], "--adapter-window-s", str(adapter["window_s"]),
                  "--telemetry-samples", "<OPERATOR: not part of the A0e contract>", *endpoints],
         "note": "Needs the verified configuration block (--config-json) as well. Reads Prometheus endpoints only."},
        {**base, "step": "R11_stage3_shadow", "kind": "preparation", "authorization": "OWNER_GO_COMPUTATION_PREDICTIONS",
         "status": "BLOCKED_NO_OPERATIONAL_LIVE_ENTRY_POINT",
         "argv": ["python", "Integration/system_integration/run_integrated_controller.py", "--run-id", run_id, *stage3_args,
                  "--snapshots", "<frames converted to JSONL: UNVERIFIED conversion>", "--out-dir", "<new directory>",
                  "--hfbtp-artifact-dir", "<owner>", "--flashbots-series", "<owner>", "--candidate-batch-sizes", "<owner>",
                  "--hfbtp-block-map", "<owner>", "--batch-size", str(values["cell"]["max_message_count"])],
         "note": "A live Stage 3 loop exists only as the library function make_shadow_foundation; no CLI wires the A0c/A0d pieces "
                 "together. The replay route needs the frozen HFBTP model (a computation that needs its own authorization) and an "
                 "unverified conversion of captured frames to JSONL."},
    ]


def prepare(params_path: Path, out_root: Path, prepare_id: str, *, repo_root: Path = REPO, now: datetime | None = None,
            argv: list[str] | None = None) -> dict[str, Any]:
    """Phase 1. Returns the launch manifest. Raises ``PreliveParameterError`` or ``PrepareError`` (nothing created)."""
    validated = load_parameters(params_path)                      # raises before anything is created
    values = validated.values
    if not PREPARE_ID.fullmatch(prepare_id):
        raise PrepareError(f"prepare id {prepare_id!r} must match {PREPARE_ID.pattern}")
    target = Path(out_root) / prepare_id
    if target.exists():
        raise PrepareError(f"refusing to reuse {target}: every preparation gets a new, isolated directory")
    env_path, network_path = Path(values["environment"]["renderer_env_path"]), Path(values["environment"]["network_json_path"])
    for label, path in (("renderer_env_path", env_path), ("network_json_path", network_path)):
        if not path.is_file():
            raise PrepareError(f"environment.{label} {path} is not a readable file")
    try:
        env = lr.load_env(env_path)
    except (ValueError, OSError) as exc:
        raise PrepareError(f"renderer environment file rejected: {exc}") from exc
    cell = values["cell"]
    target.mkdir(parents=True, exist_ok=False)
    try:
        pi.write_new_json(target / "params.validated.json", validated.as_dict())
        rendered = target / "rendered"
        result = lr.render_run(
            env, str(network_path), values["run_id"], cell["max_message_count"], cell["batch_timeout"], cell["preferred_max_bytes"],
            cell.get("tx_count"), float(cell["rate_tps"]), cell["warmup_s"], cell["workload"], rendered / "plan.json",
            repo_root=Path(repo_root), ws_root=rendered, argv=argv, channel=values["channel"], duration_s=cell.get("duration_s"))
        option_names = {"live_flush_rows": "liveFlushRows", "live_flush_interval_ms": "liveFlushIntervalMs"}   # contract key -> recorder v2 option
        live_options = {option_names[key]: value for key, value in values["recorder"].items()}
        record = wire.wire_run(result["dir"], target / "wired", channel=values["channel"], live_options=live_options, argv=argv)
        plan = result["plan"]
        live_files = expected_live_files(plan, values["run_id"])
        evidence_dir = target / "evidence"
        manifest = {
            "schema": LAUNCH_SCHEMA, "tool": TOOL_VERSION, "status": STATUS_PREPARED, "phase": "prepare",
            "caliper_invoked": False, "fabric_contacted": False, "created_utc": _utc(now),
            "run_id": values["run_id"], "channel": values["channel"],
            "channel_selection": cs.selection_record(values["channel"], role=cs.ROLE_EXPERIMENT, option="channel"),
            "owner_approval_of_channel": "NOT_GRANTED", "channel_exists": "UNVERIFIED",
            "parameters": {"file": str(Path(params_path).resolve()), "file_sha256": pi.sha256_file(Path(params_path)),
                           "validated_copy": "params.validated.json", "provenance": "operator-supplied; no scientific justification claimed"},
            "wired_run_dir": "wired/" + values["run_id"], "source_run_dir": "rendered/runs/" + values["run_id"],
            "recorder_wire_record": record["status"],
            "expected_live_files": live_files, "evidence_dir": "evidence",
            "declared_outputs": {"evidence_dir": "evidence", "ledger_journal_dir": "evidence/ledger_journal",
                                 "ledger_raw_blocks_dir": "evidence/ledger_journal.blocks_raw",
                                 "shadow_capture": "evidence/shadow_capture.json", "recorder_dir_in_workspace": plan["caliper"]["recorder_dir"]},
            "tool_hashes": {role: {"path": rel, "sha256": pi.sha256_file(Path(repo_root) / rel)} for role, rel in sorted(TOOL_FILES.items())},
            "derived": validated.as_dict()["derived"],
            "future_execution": {"status": "NOT_AUTHORIZED", "invoked": False,
                                 "steps": future_execution_steps(values, plan, evidence_dir, live_files),
                                 "authorization_note": "Each step needs its own owner authorization in a future task; approvals are not chained."},
            "unresolved_operator_inputs": ["telemetry capture sample count and interval", "WSL path translation of the evidence directory",
                                           "network description path inside WSL", "verified configuration block for --config-json"],
        }
        manifest["files"] = _sha_tree(target)
        pi.write_new_json(target / "launch_manifest.json", manifest)
        return manifest
    except BaseException as exc:
        (target / INCOMPLETE_MARKER).write_text(json.dumps({"status": "PREPARE_INCOMPLETE", "error": f"{type(exc).__name__}: {exc}"}, indent=2) + "\n", encoding="utf-8")
        raise


# ------------------------------------------------------------------------------------------------ phase 2: preflight
def _check(checks: list[dict[str, Any]], check_id: str, status: str, detail: Any) -> None:
    checks.append({"id": check_id, "status": status, "detail": detail})


def _load_json(path: Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def preflight(run_dir: Path, *, repo_root: Path = REPO, candidate_manifest: Path | None = None, frozen_sums: Path | None = None,
              clock_samples: Path | None = None, clock_parameters: dict[str, Any] | None = None, write: bool = True,
              now: datetime | None = None) -> dict[str, Any]:
    """Phase 2. Offline checks on a prepared directory. Never contacts anything; always ``live_ready: false``."""
    run_dir, repo = Path(run_dir), Path(repo_root)
    checks: list[dict[str, Any]] = []
    manifest: dict[str, Any] = {}
    try:
        if (run_dir / INCOMPLETE_MARKER).exists():
            raise ValueError("the directory carries PREPARE_INCOMPLETE.json: preparation did not finish")
        manifest = _load_json(run_dir / "launch_manifest.json")
        if manifest.get("schema") != LAUNCH_SCHEMA or manifest.get("status") != STATUS_PREPARED or manifest.get("invoked"):
            raise ValueError("launch_manifest.json has the wrong schema or status")
        _check(checks, "P01_launch_manifest", PASS, f"{manifest['status']} ({manifest['run_id']})")
    except (OSError, ValueError, KeyError) as exc:
        _check(checks, "P01_launch_manifest", FAIL, str(exc))
        return _finish(checks, run_dir, manifest, None, write, now)
    run_id, channel = manifest["run_id"], manifest["channel"]
    params: dict[str, Any] | None = None

    # P02 parameters re-validated from the stored copy; channel re-vetted
    try:
        stored = _load_json(run_dir / "params.validated.json")
        validated = pc.validate_operator_parameters(stored["values"])
        params = dict(validated.values)
        if params["run_id"] != run_id or params["channel"] != channel:
            raise ValueError("run id or channel in the parameter copy differs from the launch manifest")
        cs.select_experiment_channel(channel)
        _check(checks, "P02_parameters_and_channel", PASS,
               {"window_margin_s": validated.window_margin_s, "planned_round_s": validated.planned_round_s,
                "collector_span_s": validated.collector_span_s})
    except (OSError, ValueError, KeyError, pc.PreliveParameterError, cs.ChannelSelectionError) as exc:
        _check(checks, "P02_parameters_and_channel", FAIL, str(exc))

    # P03 inventory: every recorded file present and unchanged, nothing extra
    try:
        recorded = manifest["files"]
        actual = _sha_tree(run_dir, skip=("launch_manifest.json",))
        problems = [f"{name}: {'missing' if name not in actual else 'modified'}" for name, digest in sorted(recorded.items()) if actual.get(name) != digest]
        problems += [f"{name}: not in the manifest" for name in sorted(set(actual) - set(recorded)) if not name.startswith("evidence/")]
        _check(checks, "P03_file_inventory", FAIL if problems else PASS, problems or f"{len(recorded)} files match")
    except (OSError, KeyError, TypeError) as exc:
        _check(checks, "P03_file_inventory", FAIL, repr(exc))

    # P04 recorder v2 selected, proven from the generated benchmark; v1 beside it; channel in the network configuration
    wired = run_dir / manifest.get("wired_run_dir", "wired/" + run_id)
    try:
        selected = wire.verify_wired(wired, channel=channel)
        _check(checks, "P04_recorder_v2_selected", PASS, selected["selected_module"])
    except (wire.WireError, cs.ChannelSelectionError, OSError, KeyError, ValueError) as exc:
        _check(checks, "P04_recorder_v2_selected", FAIL, str(exc))

    # P05 conflicting values: the generated files must say what the parameters say
    try:
        if params is None:
            raise ValueError("parameters did not validate; cannot compare")
        cell = params["cell"]
        bench = _load_json(wired / "benchmark.yaml")
        plan = _load_json(run_dir / "rendered" / "plan.json")
        rounds = bench["test"]["rounds"]
        warm = cell["warmup_s"]
        issues: list[str] = []
        if len(rounds) != (2 if warm > 0 else 1):
            issues.append(f"expected {2 if warm > 0 else 1} round(s), found {len(rounds)}")
        measured = rounds[-1]
        if cell.get("duration_s") is not None and measured.get("txDuration") != cell["duration_s"]:
            issues.append(f"measured txDuration {measured.get('txDuration')} != duration_s {cell['duration_s']}")
        if cell.get("tx_count") is not None and measured.get("txNumber") != cell["tx_count"]:
            issues.append(f"measured txNumber {measured.get('txNumber')} != tx_count {cell['tx_count']}")
        if warm > 0 and rounds[0].get("txDuration") != warm:
            issues.append(f"warm-up txDuration {rounds[0].get('txDuration')} != warmup_s {warm}")
        for item in rounds:
            if item["rateControl"]["opts"]["tps"] != float(cell["rate_tps"]):
                issues.append(f"round {item.get('label')} rate {item['rateControl']['opts']['tps']} != rate_tps {cell['rate_tps']}")
        if measured["workload"]["arguments"].get("mode") != cell["workload"]:
            issues.append("measured round workload mode differs from cell.workload")
        settle = measured["workload"]["arguments"].get("settleSeconds")
        if settle != (pc.RENDERER_SETTLE_SECONDS_AFTER_WARMUP if warm > 0 else 0):
            issues.append(f"settleSeconds {settle} differs from the contract's assumption")
        monitor = bench["monitors"]["transaction"][0]["options"]
        if monitor.get("runId") != run_id:
            issues.append("monitor runId differs from the run id")
        for json_key, opt in (("live_flush_rows", "liveFlushRows"), ("live_flush_interval_ms", "liveFlushIntervalMs")):
            if params["recorder"].get(json_key) != monitor.get(opt):
                issues.append(f"recorder option {opt} ({monitor.get(opt)!r}) differs from the parameter {json_key} ({params['recorder'].get(json_key)!r})")
        if plan["channel"] != channel or plan["run_id"] != run_id:
            issues.append("render plan names a different channel or run id")
        for plan_key, value in (("B", cell["max_message_count"]), ("tau", cell["batch_timeout"]), ("P", cell["preferred_max_bytes"]), ("rate_tps", float(cell["rate_tps"]))):
            if plan["cell"].get(plan_key) != value:
                issues.append(f"plan cell {plan_key} {plan['cell'].get(plan_key)!r} != parameter {value!r}")
        if expected_live_files(plan, run_id) != manifest.get("expected_live_files"):
            issues.append("expected live file names in the manifest differ from the render plan")
        _check(checks, "P05_no_conflicting_values", FAIL if issues else PASS, issues or "benchmark, plan and parameters agree")
    except (OSError, KeyError, TypeError, IndexError, ValueError) as exc:
        _check(checks, "P05_no_conflicting_values", FAIL, f"{type(exc).__name__}: {exc}")

    # P06 declared output paths: new, inside this run, not inside protected evidence, distinct
    try:
        outs = manifest["declared_outputs"]
        problems: list[str] = []
        host = {k: v for k, v in outs.items() if k != "recorder_dir_in_workspace"}
        resolved = {}
        for key, rel in host.items():
            if Path(rel).is_absolute() or ".." in Path(rel).parts:
                problems.append(f"{key}: {rel!r} must be relative to the run directory without '..'")
                continue
            path = (run_dir / rel).resolve()
            resolved[key] = path
            if path.exists():
                problems.append(f"{key}: {rel} already exists; outputs are created exclusively by the run")
            if not str(path).startswith(str(run_dir.resolve())):
                problems.append(f"{key}: {rel} resolves outside the run directory")
            for prefix in PROTECTED_EVIDENCE_PREFIXES:
                if str(path).startswith(str((repo / prefix).resolve())):
                    problems.append(f"{key}: {rel} points into protected evidence {prefix}")
        if len({str(p) for p in resolved.values()}) != len(resolved):
            problems.append("two declared outputs resolve to the same path")
        workspace = outs.get("recorder_dir_in_workspace", "")
        if not workspace.startswith("/") or ".." in workspace.split("/") or any(n in workspace for n in cs.PROTECTED_REFERENCE_CHANNELS):
            problems.append(f"recorder_dir_in_workspace {workspace!r} must be an absolute POSIX path without '..' or a protected channel name")
        _check(checks, "P06_declared_outputs", FAIL if problems else PASS, problems or f"{len(outs)} declared outputs are new and inside the run")
    except (KeyError, TypeError, AttributeError) as exc:
        _check(checks, "P06_declared_outputs", FAIL, repr(exc))

    # P07 telemetry and ledger dependencies present, with the hashes recorded at preparation
    try:
        problems = []
        for role, entry in sorted(manifest["tool_hashes"].items()):
            target = repo / entry["path"]
            if not target.is_file():
                problems.append(f"{role}: {entry['path']} is missing")
            elif pi.sha256_file(target) != entry["sha256"]:
                problems.append(f"{role}: {entry['path']} changed since the run was prepared")
        missing_roles = sorted(set(TOOL_FILES) - set(manifest["tool_hashes"]))
        problems += [f"{role}: not recorded in the manifest" for role in missing_roles]
        if params is not None and not params["telemetry_sources"]["prometheus_endpoints"]:
            problems.append("no Prometheus endpoint is configured")
        steps = {s["step"]: s for s in manifest["future_execution"]["steps"]}
        for needed in ("R07_ledger_follow", "R10_shadow_capture", "R09_caliper_smoke"):
            if needed not in steps:
                problems.append(f"documented step {needed} is missing")
        _check(checks, "P07_dependencies", FAIL if problems else PASS, problems or f"{len(manifest['tool_hashes'])} dependencies present and unchanged")
    except (KeyError, TypeError, OSError) as exc:
        _check(checks, "P07_dependencies", FAIL, repr(exc))

    # P08 no execution has been performed or is armed
    try:
        armed = [s["step"] for s in manifest["future_execution"]["steps"] if s.get("invoked") or s.get("status") not in ("NOT_AUTHORIZED_NOT_RUN", "BLOCKED_NO_OPERATIONAL_LIVE_ENTRY_POINT")]
        flags = [k for k in ("caliper_invoked", "fabric_contacted") if manifest.get(k)]
        bad = armed + flags + ([] if manifest["future_execution"].get("invoked") is False else ["future_execution.invoked"])
        _check(checks, "P08_nothing_executed", FAIL if bad else PASS, bad or "no step is invoked or authorized")
    except (KeyError, TypeError) as exc:
        _check(checks, "P08_nothing_executed", FAIL, repr(exc))

    # P09 candidate manifest (optional): hashes match; approval is reported as NOT granted either way
    if candidate_manifest is None:
        _check(checks, "P09_candidate_manifest", NOT_CHECKED, "no candidate manifest was supplied")
    else:
        try:
            findings = pi.verify_candidate_manifest(_load_json(candidate_manifest), repo)
            _check(checks, "P09_candidate_manifest", FAIL if findings else PASS,
                   [f.as_dict() for f in findings] or "every listed file matches; the manifest is a CANDIDATE and is not approved")
        except (OSError, ValueError) as exc:
            _check(checks, "P09_candidate_manifest", FAIL, str(exc))

    # P10 frozen recovery manifest (optional): verified, never written
    if frozen_sums is None:
        _check(checks, "P10_frozen_manifest", NOT_CHECKED, "no frozen manifest was supplied")
    else:
        try:
            counts = pi.verify_sha256sums(Path(frozen_sums))
            _check(checks, "P10_frozen_manifest", FAIL if counts["failed"] or counts["missing"] or not counts["ok"] else PASS, counts)
        except OSError as exc:
            _check(checks, "P10_frozen_manifest", FAIL, str(exc))

    # P11 clock evidence (optional): operator-supplied samples only
    clock_verdict = None
    if clock_samples is None:
        _check(checks, "P11_clock_agreement", NOT_CHECKED, "no clock samples were supplied; agreement is UNVERIFIED")
    else:
        try:
            data = _load_json(clock_samples)
            phases = data if isinstance(data, dict) else {"phase": data}
            clock_verdict = cp.assess_phases(phases, **(clock_parameters or {}))
            _check(checks, "P11_clock_agreement", {cp.AGREE: PASS, cp.DISAGREE: FAIL, cp.INCONCLUSIVE: INCONCLUSIVE}[clock_verdict["verdict"]],
                   {"verdict": clock_verdict["verdict"], "phases": {k: {"verdict": v["verdict"], "reason": v["reason"]} for k, v in clock_verdict["phases"].items()}})
        except (OSError, ValueError, TypeError) as exc:
            _check(checks, "P11_clock_agreement", FAIL, f"{type(exc).__name__}: {exc}")
    return _finish(checks, run_dir, manifest, clock_verdict, write, now, candidate_manifest_supplied=candidate_manifest is not None)


def _finish(checks, run_dir, manifest, clock_verdict, write, now, candidate_manifest_supplied=False) -> dict[str, Any]:
    statuses = [c["status"] for c in checks]
    offline = FAIL if FAIL in statuses else (INCONCLUSIVE if INCONCLUSIVE in statuses else PASS)
    clock_state = "UNVERIFIED (no samples)" if clock_verdict is None else clock_verdict["verdict"]
    gates = [
        {"gate": "owner_approval_of_validation_channel", "state": "NOT_GRANTED"},
        {"gate": "validation_channel_exists_with_expected_configuration", "state": "UNVERIFIED"},
        {"gate": "owner_approval_of_artifact_hashes", "state": "NOT_GRANTED" + ("" if candidate_manifest_supplied else " (no candidate manifest supplied)")},
        {"gate": "windows_wsl_clock_agreement", "state": clock_state},
        {"gate": "recorder_v2_inside_a_real_caliper_worker", "state": "UNVERIFIED"},
        {"gate": "ledger_follower_against_a_real_peer", "state": "UNVERIFIED"},
        {"gate": "stage3_operational_live_entry_point", "state": "BLOCKED_NO_OPERATIONAL_ENTRY_POINT"},
        {"gate": "authorization_to_execute_any_live_step", "state": "NOT_GRANTED"},
    ]
    result = {"schema": RESULT_SCHEMA, "tool": TOOL_VERSION, "run_dir": str(run_dir), "checked_utc": _utc(now),
              "run_id": manifest.get("run_id"), "channel": manifest.get("channel"), "checks": checks,
              "counts": {s: statuses.count(s) for s in (PASS, FAIL, INCONCLUSIVE, NOT_CHECKED)},
              "offline_preflight": offline, "live_ready": False, "live_gates": gates,
              "statement": "An offline pass shows the prepared files are consistent. It does not show that anything works on a live "
                           "network, and it is not an authorization."}
    if write and run_dir.is_dir():
        out_dir = run_dir / "preflight_results"
        out_dir.mkdir(exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        result["written_to"] = [f"preflight_results/preflight_{stamp}.json", f"preflight_results/preflight_{stamp}.md"]
        pi.write_new_json(out_dir / f"preflight_{stamp}.json", result)
        with (out_dir / f"preflight_{stamp}.md").open("x", encoding="utf-8") as fh:
            fh.write(summary_markdown(result))
    return result


def summary_markdown(result: dict[str, Any]) -> str:
    lines = [f"# Pre-live preflight: {result.get('run_id')} on {result.get('channel')}", "",
             f"**Offline preflight: {result['offline_preflight']}.  Live ready: {str(result['live_ready']).lower()}.**", "",
             result["statement"], "", "| Check | Status | Detail |", "|---|---|---|"]
    for item in result["checks"]:
        detail = json.dumps(item["detail"], sort_keys=True)
        lines.append(f"| {item['id']} | {item['status']} | {detail[:300]} |")
    lines += ["", "## Gates that remain open", ""] + [f"* {g['gate']}: {g['state']}" for g in result["live_gates"]] + [""]
    return "\n".join(lines)


# ------------------------------------------------------------------------------------------------ CLI
def refuse_execute() -> int:
    print("REFUSED: the execute phase is not available in this tool. A live run needs a separate, versioned execution tool and "
          "explicit owner authorization for each live step, supplied in a future task. No flag or environment variable enables it.")
    return EXIT_EXECUTE_REFUSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    prep = sub.add_parser("prepare", help="validate, render, wire recorder v2, write a launch manifest (offline)")
    prep.add_argument("--params", required=True, type=Path)
    prep.add_argument("--out-root", required=True, type=Path)
    prep.add_argument("--prepare-id", required=True)
    pre = sub.add_parser("preflight", help="offline checks on a prepared directory")
    pre.add_argument("--run-dir", required=True, type=Path)
    pre.add_argument("--candidate-manifest", type=Path)
    pre.add_argument("--frozen-sums", type=Path)
    pre.add_argument("--clock-samples", type=Path)
    pre.add_argument("--clock-tolerance-ms", type=float)
    pre.add_argument("--clock-min-valid-samples", type=int)
    pre.add_argument("--clock-max-wall-monotonic-disagreement-ms", type=float)
    pre.add_argument("--no-write", action="store_true")
    sub.add_parser("execute", help="always refused")
    cand = sub.add_parser("candidate-manifest", help="write a candidate integrity manifest (not an approval)")
    cand.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.phase == "execute":
        return refuse_execute()
    if args.phase == "candidate-manifest":
        try:
            manifest = pi.build_candidate_manifest(REPO, CANDIDATE_SPECS, open_items=CANDIDATE_OPEN_ITEMS,
                                                   launch_configuration={"orchestrator": TOOL_VERSION, "entry_point": "scripts/e1/e1_prelive_validation.py",
                                                                         "phases": ["prepare", "preflight"], "execute": "not implemented"},
                                                   experimental_configuration_provenance={"source": "operator-supplied parameters (prelive_params/1)",
                                                                                          "note": "no cell or threshold is approved; none is claimed scientifically justified"})
            digest = pi.write_new_json(args.out, manifest)
        except (OSError, ValueError) as exc:
            print(f"REFUSED: {exc}")
            return 2
        print(f"candidate manifest -> {args.out} ({len(manifest['entries'])} files, sha256 {digest}); status {manifest['status']}")
        return 0
    if args.phase == "prepare":
        try:
            manifest = prepare(args.params, args.out_root, args.prepare_id, argv=sys.argv if argv is None else argv)
        except pc.PreliveParameterError as exc:
            print(json.dumps({"valid": False, "issues": [i.as_dict() for i in exc.issues]}, indent=2))
            return 2
        except (PrepareError, wire.WireError, cs.ChannelSelectionError, OSError, ValueError, KeyError) as exc:
            print(f"REFUSED: {type(exc).__name__}: {exc}")
            return 2
        print(f"prepared {args.out_root / args.prepare_id}: {manifest['status']}; Caliper and Fabric were not touched")
        return 0
    clock = (args.clock_tolerance_ms, args.clock_min_valid_samples, args.clock_max_wall_monotonic_disagreement_ms)
    if args.clock_samples is not None and any(v is None for v in clock):
        print("REFUSED: --clock-samples needs --clock-tolerance-ms, --clock-min-valid-samples and "
              "--clock-max-wall-monotonic-disagreement-ms (no defaults)")
        return 2
    result = preflight(args.run_dir, candidate_manifest=args.candidate_manifest, frozen_sums=args.frozen_sums,
                       clock_samples=args.clock_samples, write=not args.no_write,
                       clock_parameters=None if args.clock_samples is None else {
                           "tolerance_ms": clock[0], "min_valid_samples": clock[1], "max_wall_monotonic_disagreement_ms": clock[2]})
    print(summary_markdown(result))
    return {PASS: 0, FAIL: 1, INCONCLUSIVE: 3}[result["offline_preflight"]]


if __name__ == "__main__":
    sys.exit(main())
