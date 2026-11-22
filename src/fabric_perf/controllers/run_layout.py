"""One explicit, versioned run layout and instrumentation selection for the first bounded live validation (task A0s, 2026-11-22). OFFLINE: reads files, returns dicts.

Why. A0e..A0q built the pieces (timed issue-log writer, recorder v3 with a health spool, ledger collector, post-run readers and bridges, timing sampler, clock
evaluator). Nothing declared which of them one validation uses, where each writes, or that their schemas fit. This module is that declaration. It does not add
an instrumentation framework: it reads a rendered A0p variant directory (verified with ``timed_variant.verify_overlay``), derives every per-round path from the
rendered benchmark (nothing is invented), and checks that the selected components agree on schema versions.

Selection (explicit; there is no "latest"): ``SELECTION`` names the workload, writer, recorder, readers, bridges, ledger capture, Prometheus and clock components
and, for each, whether the first validation RUNS it, uses it only POST-RUN on finished files, or DEFERS it with the effect on the measurements stated. Prometheus is
an owner choice (``before_after`` or ``deferred``); it is never defaulted.

Worker-written evidence (issue log, timing sidecar, recorder files) stays where Caliper writes it, in the run directory of the Caliper workspace, and is hashed
there. Host-collected evidence (plan, approval reference, config inspections, clock samples, ledger journal, Prometheus snapshots, process logs, derived
post-run results, status, integrity manifest) goes under one NEW output directory. Every round's files carry the round index and label in their names and every
post-run input is bound to one round; ``verify_layout`` rejects a path collision, a path inside protected evidence, and a post-run input that names another round.
"""

from __future__ import annotations

import json
import posixpath
import re
from pathlib import Path
from typing import Any, Mapping

from . import channel_selection as cs
from . import issue_log_reader as ilr
from . import issue_log_timing as ilt
from . import ledger_evidence as le
from . import recorder_health_bridge as rhb
from . import recorder_health_incremental as rhi
from . import sourcecount_reader as sr
from . import timed_variant as tv
from . import timing_durable as td
from . import launch_variant as lv

LAYOUT_SCHEMA = "a0s_run_layout/1"
LAYOUT_VERSION = "run_layout 0.1 (A0s, 2026-11-22)"
FIRST_LIVE_VARIANT = tv.VARIANT_A0P
CALIPER_WS_CONTAINER = "/hyperledger/caliper/workspace"
PROMETHEUS_MODES = ("before_after", "deferred")
PROTECTED_EVIDENCE_PREFIXES = ("Integration/system_integration/observability_recovery_2026-11-17", "experiments/results", "data/raw", "data/processed")
RECORDER_SUFFIXES = (".csv", ".submits.csv", ".manifest.json", ".live.csv", ".live.submits.csv", ".live.open.json", ".live.final.json", ".health.jsonl")

# state: RUN = started by the first validation; POST_RUN = used only on finished files; DEFERRED = not used (effect stated); OWNER_CHOICE = the owner picks.
SELECTION: dict[str, dict[str, Any]] = {
    "workload": {"state": "RUN", "file": tv.WORKLOAD_TIMED, "identity": "e1_create_asset_issue_logged/1", "why": "the only workload that writes the generator-side issue log"},
    "issue_log_writer": {"state": "RUN", "file": "e1_issue_log_timed.js", "log_schema": ilt.LOG_SCHEMA, "sidecar_schema": ilt.TIMING_SCHEMA,
                         "why": "durable write timing is what T01-T06 need"},
    "recorder": {"state": "RUN", "file": lv.RECORDER_V3, "spool_schema": rhb.SPOOL_SCHEMA, "why": "client-side completion, latency and the recorder's own health"},
    "ledger_capture": {"state": "RUN", "file": "scripts/e1/e1_ledger_follow.py", "evidence_schema": le.SCHEMA_VERSION,
                       "why": "the only committed-throughput source (client completion is not ledger commitment)"},
    "clock_probe": {"state": "RUN", "module": "wsl_clock_probe", "why": "cross-domain timing is uninterpretable without an offset interval"},
    "config_inspection": {"state": "RUN", "module": "config_inspection", "why": "proves the approved cell before and no change after"},
    "sourcecount_reader": {"state": "POST_RUN", "module": "sourcecount_reader", "evidence_schema": sr.EVIDENCE_SCHEMA, "mode": "inspect or replay over the finished issue log",
                           "effect": "no in-run generator coverage; the follow mode stays refused; a crashed run is discarded, not recovered"},
    "generator_route_recovery": {"state": "DEFERRED", "module": "issue_log_bridge_recovery", "effect": "a crash cannot be resumed; the first validation tolerates that (discard and rerun)"},
    "recorder_health_bridge": {"state": "POST_RUN", "module": "recorder_health_incremental", "checkpoint_schema": rhi.CHECKPOINT_SCHEMA, "report_schema": rhi.REPORT_SCHEMA,
                               "mode": "e1_a0p_timing_tools.py bridge over the finished spool", "effect": "no live recorder-health journal; Stage 3 gets it only after the run"},
    "timing_sampler": {"state": "POST_RUN", "module": "timing_durable", "result_schema": td.RESULT_SCHEMA, "mode": "e1_a0p_timing_tools.py sample over finished files"},
    "prometheus_capture": {"state": "OWNER_CHOICE", "file": "scripts/e1/e1_metrics_scraper.py", "choices": list(PROMETHEUS_MODES),
                           "effect_if_deferred": "no orderer or peer metric evidence: block-cut causes and queue pressure are unobserved and Stage 3 cannot be assessed on this run"},
    "stage3_shadow": {"state": "DEFERRED", "effect": "no controller recommendation is produced; the first run validates the measurement chain only"},
    "real_inference": {"state": "DEFERRED", "effect": "no model or forecaster runs (REAL_INFERENCE_AUTHORIZED stays false)"},
}


class LayoutError(ValueError):
    """The layout cannot be built or fails verification. ``code`` is a stable machine-readable cause."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _fail(code: str, message: str) -> LayoutError:
    return LayoutError(code, message)


def check_component_compatibility() -> list[dict[str, str]]:
    """Schema agreement between the selected components, read from the modules themselves. An empty list means no disagreement was found."""
    expected = (("issue log schema", ilt.LOG_SCHEMA, "e1_issue_log/1"), ("timing sidecar schema", ilt.TIMING_SCHEMA, "e1_issue_log_timing/1"),
                ("recorder health spool schema", rhb.SPOOL_SCHEMA, "recorder_health_spool/1"), ("ledger evidence schema", le.SCHEMA_VERSION, "fabric_ledger_evidence/1"),
                ("source-count evidence schema", sr.EVIDENCE_SCHEMA, "source_count_evidence/1"), ("timing results schema", td.RESULT_SCHEMA, "a0p_timing_results/1"),
                ("incremental bridge checkpoint schema", rhi.CHECKPOINT_SCHEMA, "recorder_health_bridge_checkpoint/1"),
                ("timed workload file", tv.WORKLOAD_TIMED, "e1_create_asset_issue_logged_timed.js"), ("recorder file", lv.RECORDER_V3, "e1_per_tx_recorder_v3.js"))
    problems = [{"item": name, "found": found, "expected": want} for name, found, want in expected if found != want]
    if not (SELECTION["issue_log_writer"]["log_schema"] == ilr.ISSUE_LOG_SCHEMA == ilt.LOG_SCHEMA):
        problems.append({"item": "reader/writer issue-log schema", "found": f"{SELECTION['issue_log_writer']['log_schema']} / {ilr.ISSUE_LOG_SCHEMA} / {ilt.LOG_SCHEMA}", "expected": "all equal"})
    return problems


def _read_json(path: Path) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise _fail("unreadable_input", f"{path}: {exc}") from exc


def _round_role(label: str) -> str:
    return "warmup" if label.startswith("wu-") else "measured"


def build_layout(rendered_root: Path, output_dir: Path, *, repo_root: Path, prometheus_mode: str) -> dict[str, Any]:
    """The layout of one validation run from a rendered A0p variant directory. Raises :class:`LayoutError` for any inconsistency."""
    if prometheus_mode not in PROMETHEUS_MODES:
        raise _fail("prometheus_choice_missing", f"prometheus_mode {prometheus_mode!r} must be one of {PROMETHEUS_MODES}; there is no default")
    root = Path(rendered_root)
    overlay = tv.overlay_directory(root)
    try:
        verified = tv.verify_overlay(root, overlay, Path(repo_root))
    except (tv.TimedVariantError, OSError, ValueError, KeyError) as exc:
        raise _fail("variant_not_verified", f"{type(exc).__name__}: {exc}") from exc
    marker = _read_json(tv.marker_path(root))
    if marker.get("variant") != FIRST_LIVE_VARIANT:
        raise _fail("variant_not_selected", f"the rendered directory is variant {marker.get('variant')!r}; the first validation selects {FIRST_LIVE_VARIANT!r}")
    run_id = verified["run_id"]
    a0m = _read_json(root / "a0m_config_manifest.json")
    launch_manifest = _read_json(root / "a0e_base" / "prepared" / "launch_manifest.json")
    plan = _read_json(root / "a0e_base" / "prepared" / "rendered" / "plan.json")
    overlay_wired = Path(verified["overlay"])
    bench = _read_json(overlay_wired / "benchmark.yaml")
    container_run_dir = verified["container_run_dir"]
    recorder_container = plan["caliper"]["recorder_dir_in_container"]
    recorder_host = plan["caliper"]["recorder_dir"]
    if not recorder_container.startswith(CALIPER_WS_CONTAINER + "/") or not recorder_host.endswith(recorder_container[len(CALIPER_WS_CONTAINER):]):
        raise _fail("workspace_mapping_unverifiable", "the container and host recorder directories do not map onto each other")
    host_prefix = recorder_host[: len(recorder_host) - len(recorder_container) + len(CALIPER_WS_CONTAINER)]
    to_host = lambda container_path: host_prefix + container_path[len(CALIPER_WS_CONTAINER):]
    rounds = []
    for index, rnd in enumerate(bench["test"]["rounds"]):
        label = rnd["label"]
        base = f"{run_id}_w0_r{index}_{label}"
        args = rnd["workload"]["arguments"]["issueLogTimed"]
        log_c, timing_c = args["path"], args["timingPath"]
        if not (posixpath.basename(log_c).startswith(base + ".") and posixpath.basename(timing_c).startswith(base + ".")):
            raise _fail("round_path_not_bound_to_round", f"round {index}: the issue-log names do not start with {base!r}")
        rate = rnd["rateControl"]["opts"]["tps"]
        rounds.append({
            "round_index": index, "label": label, "role": _round_role(label), "base": base, "tx_duration_s": rnd["txDuration"], "rate_tps": rate,
            "issue_log": {"container": log_c, "host": to_host(log_c)}, "timing_sidecar": {"container": timing_c, "host": to_host(timing_c)},
            "recorder_files": {suffix: posixpath.join(recorder_host, base + suffix) for suffix in RECORDER_SUFFIXES},
        })
    out = Path(output_dir)
    host = {
        "execution_plan": "00_plan/execution_plan.json", "approval_reference": "00_plan/approval_reference.json", "execution_log": "00_plan/execution_log.jsonl",
        "run_status": "run_status.json", "process_logs": "process_logs/", "config_initial": "config_inspection/initial/", "config_final": "config_inspection/final/",
        "clock_pre": "clock/pre_run.json", "clock_post": "clock/post_run.json", "clock_verdict": "clock/clock_verdict.json",
        "ledger_journal": "ledger/journal/", "ledger_blocks_raw": "ledger/journal.blocks_raw/",
        "prometheus_before": "prometheus/before/" if prometheus_mode == "before_after" else None,
        "prometheus_after": "prometheus/after/" if prometheus_mode == "before_after" else None,
        "measurement_summary": "post_run/measurement_summary.json", "integrity_manifest": "integrity_manifest.json",
        "reader_checkpoints": "post_run/reader_checkpoints/", "generator_coverage_journal": "post_run/generator_coverage_journal/",
        "recorder_health_journal": "post_run/recorder_health_journal/", "timing_results": "post_run/timing_results.json",
    }
    post_run = []
    for rnd in rounds:
        i = rnd["round_index"]
        post_run += [
            {"tool": "e1_a0p_timing_tools.py verify-write-timing", "round": i, "inputs": [rnd["issue_log"]["host"], rnd["timing_sidecar"]["host"]], "output": f"post_run/write_timing_r{i}.json"},
            {"tool": "e1_sourcecount_reader.py inspect", "round": i, "inputs": [rnd["issue_log"]["host"]], "output": f"post_run/sourcecount_inspect_r{i}.json",
             "checkpoint": f"post_run/reader_checkpoints/r{i}.json"},
            {"tool": "e1_a0p_timing_tools.py bridge", "round": i, "inputs": [rnd["recorder_files"][".health.jsonl"]], "output": f"post_run/bridge_report_r{i}.json",
             "journal": f"post_run/recorder_health_journal/r{i}.jsonl", "ledger": f"post_run/recorder_health_journal/r{i}.ingest.jsonl", "checkpoint": f"post_run/recorder_health_journal/r{i}.checkpoint.json"},
        ]
    layout = {"schema": LAYOUT_SCHEMA, "version": LAYOUT_VERSION, "variant": FIRST_LIVE_VARIANT, "run_id": run_id, "channel": a0m.get("channel") or verified["channel"],
              "container_run_dir": container_run_dir, "worker_evidence_root_host": to_host(container_run_dir), "caliper_workspace_host": host_prefix,
              "rounds": rounds, "output_dir": str(out), "host_evidence": host, "post_run_steps": post_run,
              "prometheus_mode": prometheus_mode, "selection": SELECTION, "launch_authorizing_directory": str(overlay_wired),
              "caliper_argv": list(plan["caliper"]["argv"]), "caliper_container": plan["caliper"]["container"],
              "network_description": plan["network"], "metrics_targets": list(plan["metrics_targets"]),
              "planned_cell": dict(plan["cell"]), "launch_manifest_status": launch_manifest.get("status"),
              "nothing_started": True}
    problems = verify_layout(layout, repo_root=Path(repo_root))
    if problems:
        raise _fail("layout_invalid", json.dumps(problems))
    return layout


def verify_layout(layout: Mapping[str, Any], *, repo_root: Path | None = None) -> list[dict[str, str]]:
    """Problems of a layout (an empty list means none): collisions, protected paths, cross-round consumption, bad selection, schema disagreement."""
    problems: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        problems.append({"code": code, "message": message})

    problems += [{"code": "schema_disagreement", "message": json.dumps(item)} for item in check_component_compatibility()]
    if layout.get("schema") != LAYOUT_SCHEMA:
        bad("layout_schema", f"schema is {layout.get('schema')!r}")
    if layout.get("variant") != FIRST_LIVE_VARIANT:
        bad("variant_not_selected", f"variant {layout.get('variant')!r}")
    if layout.get("prometheus_mode") not in PROMETHEUS_MODES:
        bad("prometheus_choice_missing", f"{layout.get('prometheus_mode')!r}")
    try:
        cs.select_experiment_channel(layout.get("channel"), purpose="the run layout", option="channel")
    except cs.ChannelSelectionError as exc:
        bad("channel_refused", str(exc))
    rounds = layout.get("rounds") or []
    if not rounds:
        bad("no_rounds", "the layout has no rounds")
    seen: dict[str, str] = {}

    def claim(path: str, owner: str) -> None:
        key = path.replace("\\", "/")
        if key in seen:
            bad("path_collision", f"{path!r} is used by {seen[key]} and {owner}")
        seen[key] = owner

    for rnd in rounds:
        i = rnd["round_index"]
        tag = f"r{i}_{rnd['label']}"
        if f"_r{i}_" not in rnd["base"] or not rnd["base"].endswith(rnd["label"]):
            bad("round_base_not_bound", f"round {i}: base {rnd['base']!r}")
        for name, entry in (("issue_log", rnd["issue_log"]), ("timing_sidecar", rnd["timing_sidecar"])):
            for side in ("container", "host"):
                claim(entry[side], f"round {i} {name} {side}")
                if f"_r{i}_" not in entry[side]:
                    bad("path_not_bound_to_round", f"round {i}: {entry[side]!r}")
        for suffix, path in rnd["recorder_files"].items():
            claim(path, f"round {i} recorder {suffix}")
            if f"_r{i}_" not in path:
                bad("path_not_bound_to_round", f"round {i}: {path!r}")
    for key, path in (layout.get("host_evidence") or {}).items():
        if path is not None:
            claim("host:" + str(path), f"host evidence {key}")
    for step in layout.get("post_run_steps", []):
        i = step["round"]
        for path in step["inputs"]:
            if f"_r{i}_" not in path:
                bad("cross_round_input", f"{step['tool']} for round {i} reads {path!r}, which is not bound to round {i}")
        for key in ("output", "journal", "ledger", "checkpoint"):
            if step.get(key):
                claim("host:" + step[key], f"post-run {step['tool']} round {i} {key}")
                if f"r{i}" not in step[key]:
                    bad("output_not_bound_to_round", f"{step[key]!r} does not name round {i}")
    out_posix = Path(layout.get("output_dir", "")).as_posix()
    for prefix in PROTECTED_EVIDENCE_PREFIXES:
        if re.search(rf"(^|/){re.escape(prefix)}(/|$)", out_posix):
            bad("protected_path", f"the output directory {out_posix!r} lies inside protected evidence {prefix!r}")
    for name, spec in (layout.get("selection") or {}).items():
        if spec.get("state") not in ("RUN", "POST_RUN", "DEFERRED", "OWNER_CHOICE"):
            bad("selection_state", f"{name}: {spec.get('state')!r}")
        if spec.get("state") == "DEFERRED" and not spec.get("effect"):
            bad("deferred_without_effect", f"{name} is deferred but its effect on the measurements is not stated")
    return problems
