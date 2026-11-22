"""Explicit selection of the timed issue-log writer for a future authorized validation (task A0p, 2026-11-22). OFFLINE ONLY: it reads and compares files.

Variants. ``launch_variant`` (A0o) names two: the historical A0e (recorder v2, legacy workload) and the A0m variant (recorder v3 + the issue-logged workload). This module adds a THIRD, never
a replacement: ``a0p_recorder_v3_timed_issue_log`` = recorder v3 + ``e1_create_asset_issue_logged_timed.js``, whose round arguments carry ``issueLogTimed`` (log path AND timing-sidecar path).
It is selected only by name; nothing upgrades an A0m configuration silently, and nothing downgrades a timed one.

How it is built without touching a pinned or historical file. The A0m dry render (``e1_a0m_dry_render``) cannot be asked for a different workload module (its module table and its
preflight, which hashes every file in the render root, are fixed). The timed variant is therefore an OVERLAY next to an untouched A0m render under a variant-qualified run id
(``<base>-v3ilt``): the overlay is a copy of the render's ``wired_v3`` directory in which (a) the workload module is the timed successor, (b) every round's ``issueLog`` argument became
``issueLogTimed`` with a ``timingPath`` beside the log, and (c) the two new modules are present. The transformation is a pure function of the A0m benchmark (``timed_benchmark``), so the
preflight can re-derive the overlay and require byte equality. The overlay, not the A0m render, is the launch-authorizing directory; the A0m preflight still runs on the untouched render.

Mixed versions are refused: a timed workload with an ``issueLog`` argument, an A0m workload with ``issueLogTimed``, recorder v2 with the timed workload, a timing path equal to the log path
or outside the run directory, and a sidecar or bridge schema of another version. There is no fallback to the A0l workload, to recorder v2 or to a protected channel, and one worker only.
"""

from __future__ import annotations

import copy
import json
import posixpath
from pathlib import Path
from typing import Any, Mapping

from . import launch_variant as lv

VARIANT_A0P = "a0p_recorder_v3_timed_issue_log"
RUN_ID_SUFFIX = "-v3ilt"
SLUG = "a0p-v3ilt"
MARKER_SCHEMA = "a0p_timed_variant_marker/1"
OVERLAY_SCHEMA = "a0p_timed_overlay_manifest/1"
OVERLAY_DIR_SUFFIX = ".timed_overlay"
WORKLOAD_TIMED, WRITER_TIMED = "e1_create_asset_issue_logged_timed.js", "e1_issue_log_timed.js"
SOURCES = {WORKLOAD_TIMED: "fabric-testbed/caliper/workloads/" + WORKLOAD_TIMED, WRITER_TIMED: "fabric-testbed/caliper/workloads/" + WRITER_TIMED}
REQUIRED_MODULES = (lv.RECORDER_V3, WORKLOAD_TIMED, WRITER_TIMED, "e1_issue_log.js", "e1_workload_core.js", lv.WORKLOAD_LEGACY)
LOG_SUFFIX, TIMING_SUFFIX = ".issuelog.jsonl", ".issuelog.timing.jsonl"
ENTRY_MODULES = (lv.RECORDER_V3, WORKLOAD_TIMED)
TIMED_ARG_KEYS = ("path", "timingPath", "workerId", "expectedWorkers", "clockDomain", "tickIntervalMs", "fsyncPolicy")


class TimedVariantError(ValueError):
    """The timed-variant configuration is mixed, incomplete or inconsistent. ``code`` is machine-readable."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _fail(code: str, message: str) -> TimedVariantError:
    return TimedVariantError(code, message)


def qualified_run_id(base_run_id: str) -> str:
    """The run id of the timed variant. The base id must be unqualified (no A0o or A0p suffix); it is never truncated."""
    if not isinstance(base_run_id, str) or not lv.RUN_ID_RE.fullmatch(base_run_id):
        raise _fail("run_id_invalid", f"base run id {base_run_id!r} must match {lv.RUN_ID_RE.pattern}")
    for suffix in (RUN_ID_SUFFIX, *(v["run_id_suffix"] for v in lv.VARIANTS.values() if v["run_id_suffix"])):
        if base_run_id.endswith(suffix):
            raise _fail("run_id_already_qualified", f"{base_run_id!r} already ends with the variant suffix {suffix!r}; supply the BASE run id")
    qualified = base_run_id + RUN_ID_SUFFIX
    if len(qualified) > lv.RUN_ID_MAX:
        raise _fail("run_id_too_long", f"{qualified!r} would be {len(qualified)} characters; the renderer allows {lv.RUN_ID_MAX}. The id is never truncated")
    return qualified


def variant_directory(out_root: Path, render_id: str) -> Path:
    return Path(out_root) / SLUG / render_id


def overlay_directory(root: Path) -> Path:
    root = Path(root)
    return root.with_name(root.name + OVERLAY_DIR_SUFFIX)


def marker_path(root: Path) -> Path:
    return lv.marker_path(root)


def timing_path_for(log_path: str) -> str:
    if not log_path.endswith(LOG_SUFFIX):
        raise _fail("issue_log_path_unexpected", f"{log_path!r} does not end with {LOG_SUFFIX!r}")
    return log_path[: -len(LOG_SUFFIX)] + TIMING_SUFFIX


def timed_benchmark(a0m_benchmark: Mapping[str, Any]) -> dict[str, Any]:
    """The timed variant's benchmark: a pure function of the A0m benchmark. Refuses anything that is not exactly the A0m variant."""
    try:
        facts = lv.derive_variant(a0m_benchmark)
    except lv.LaunchVariantError as exc:
        raise _fail("not_the_a0m_variant", f"the source benchmark is not exactly the A0m variant ({exc.code}); nothing is converted") from exc
    if facts["variant"] != lv.VARIANT_A0M:
        raise _fail("not_the_a0m_variant", f"the source benchmark is variant {facts['variant']!r}; only the A0m variant is converted")
    out = copy.deepcopy(dict(a0m_benchmark))
    for index, rnd in enumerate(out["test"]["rounds"]):
        workload = rnd["workload"]
        workload["module"] = posixpath.join(posixpath.dirname(workload["module"]), WORKLOAD_TIMED)
        args = workload["arguments"]
        log = args.pop("issueLog")
        timed = {k: log[k] for k in ("path", "workerId", "expectedWorkers", "clockDomain", "tickIntervalMs", "fsyncPolicy") if k in log}
        timed["timingPath"] = timing_path_for(log["path"])
        args["issueLogTimed"] = timed
    return out


def derive_timed_variant(benchmark: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a generated benchmark as the timed variant, or raise. A mixed writer version is named as such."""
    monitors = (benchmark.get("monitors") or {}).get("transaction")
    if not isinstance(monitors, list) or len(monitors) != 1 or not isinstance(monitors[0], dict):
        raise _fail("monitor_not_single", "the benchmark must have exactly one transaction monitor")
    monitor = monitors[0]
    recorder = posixpath.basename(str(monitor.get("module")))
    options = monitor.get("options") or {}
    rounds = (benchmark.get("test") or {}).get("rounds")
    if not isinstance(rounds, list) or not rounds:
        raise _fail("no_rounds", "the benchmark has no rounds")
    workloads, timed_args, plain_args = set(), [], []
    for index, rnd in enumerate(rounds):
        workload = rnd.get("workload") if isinstance(rnd, dict) else None
        if not isinstance(workload, dict):
            raise _fail("round_without_workload", f"round {index} has no workload")
        workloads.add(posixpath.basename(str(workload.get("module"))))
        args = workload.get("arguments") or {}
        timed_args.append(args.get("issueLogTimed") if isinstance(args, dict) else None)
        plain_args.append(args.get("issueLog") if isinstance(args, dict) else None)
    if workloads != {WORKLOAD_TIMED}:
        raise _fail("not_the_timed_workload", f"the benchmark selects workload(s) {sorted(workloads)}; the timed variant selects exactly {WORKLOAD_TIMED!r}")
    if any(entry is not None for entry in plain_args):
        raise _fail("mixed_issue_log_schema", "a round of the timed workload carries an issueLog (A0l) argument: a different writer version; refused, not converted")
    if recorder != lv.RECORDER_V3 or "health" not in options:
        raise _fail("recorder_mismatch", f"the timed variant needs recorder {lv.RECORDER_V3!r} with a health block (found {recorder!r}, health {'health' in options})")
    if any(not isinstance(entry, dict) for entry in timed_args):
        raise _fail("issue_log_timed_missing", "every round must carry an issueLogTimed argument")
    run_dir = posixpath.dirname(str(options.get("outputDir")))
    out_dir = options.get("outputDir")
    if not isinstance(out_dir, str) or not out_dir.startswith("/"):
        raise _fail("output_dir_invalid", f"the monitor outputDir {out_dir!r} must be an absolute POSIX path")
    seen: set[str] = set()
    for index, entry in enumerate(timed_args):
        if set(entry) - set(TIMED_ARG_KEYS):
            raise _fail("issue_log_timed_keys", f"round {index} issueLogTimed has unexpected keys {sorted(set(entry) - set(TIMED_ARG_KEYS))}")
        log, timing = entry.get("path"), entry.get("timingPath")
        if not isinstance(log, str) or not isinstance(timing, str) or not log.startswith("/") or not timing.startswith("/"):
            raise _fail("timing_path_invalid", f"round {index}: path and timingPath must be absolute POSIX paths")
        if log == timing:
            raise _fail("timing_path_invalid", f"round {index}: the timing sidecar must be a different file from the log")
        for value in (log, timing):
            if not value.startswith(run_dir + "/"):
                raise _fail("issue_log_outside_run_directory", f"{value!r} is not inside {run_dir!r}")
            if value in seen:
                raise _fail("log_path_reused", f"{value!r} is used by more than one round: every round needs its own log and sidecar")
            seen.add(value)
        if entry.get("workerId") != "w0" or entry.get("expectedWorkers") != ["w0"]:
            raise _fail("worker_restriction", f"round {index}: exactly one worker, w0, is supported")
    return {"variant": VARIANT_A0P, "monitor_module": recorder, "workload_modules": sorted(workloads), "run_id": options.get("runId"), "monitor_output_dir": out_dir,
            "container_run_dir": run_dir, "issue_log_paths": [e["path"] for e in timed_args], "timing_paths": [e["timingPath"] for e in timed_args]}


def verify_modules(overlay_wired: Path, repo_root: Path) -> dict[str, Any]:
    """The overlay's modules directory holds what the timed variant needs, and the two new modules are byte-identical to the repository sources."""
    modules = Path(overlay_wired) / "modules"
    present = sorted(p.name for p in modules.glob("*.js")) if modules.is_dir() else []
    missing = [m for m in REQUIRED_MODULES if m not in present]
    if missing:
        raise _fail("modules_do_not_match_variant", f"missing {missing}")
    for name, rel in SOURCES.items():
        if (modules / name).read_bytes() != (Path(repo_root) / rel).read_bytes():
            raise _fail("module_differs_from_source", f"modules/{name} differs from {rel}")
    return {"present": present}


def verify_overlay(root: Path, overlay: Path, repo_root: Path, expected_base_run_id: str | None = None) -> dict[str, Any]:
    """Prove the overlay against the untouched A0m render beside it. Raises ``TimedVariantError`` on the first inconsistency."""
    root, overlay = Path(root), Path(overlay)
    marker_file = marker_path(root)
    if not marker_file.is_file():
        raise _fail("variant_marker_missing", f"{marker_file.name} is missing: an A0m render without the timed-variant marker is never accepted as the timed variant")
    marker = json.loads(marker_file.read_text(encoding="utf-8"))
    if marker.get("schema") != MARKER_SCHEMA:
        raise _fail("variant_marker_schema", f"marker schema is {marker.get('schema')!r}" + (" (an A0o marker: another variant)" if marker.get("schema") == lv.MARKER_SCHEMA else ""))
    if marker.get("variant") != VARIANT_A0P:
        raise _fail("variant_marker_mismatch", f"the marker says {marker.get('variant')!r}")
    if marker.get("fallback") not in (None, "none"):
        raise _fail("fallback_declared", "a variant marker may not declare a fallback")
    qualified = qualified_run_id(marker.get("base_run_id"))
    if expected_base_run_id is not None and marker["base_run_id"] != expected_base_run_id:
        raise _fail("base_run_id_mismatch", f"marker base run id {marker['base_run_id']!r}, expected {expected_base_run_id!r}")
    manifest = json.loads((root / "a0m_config_manifest.json").read_text(encoding="utf-8"))
    base_wired = root / manifest["wired_v3_dir"]
    base_bench = json.loads((base_wired / "benchmark.yaml").read_text(encoding="utf-8"))
    overlay_wired = overlay / "wired_v3_timed" / base_wired.name
    overlay_bench = json.loads((overlay_wired / "benchmark.yaml").read_text(encoding="utf-8"))
    facts = derive_timed_variant(overlay_bench)
    if facts["run_id"] != qualified or manifest.get("run_id") != qualified or posixpath.basename(facts["container_run_dir"]) != qualified:
        raise _fail("run_id_not_qualified", f"expected run id {qualified!r}; manifest {manifest.get('run_id')!r}, benchmark {facts['run_id']!r}")
    if timed_benchmark(base_bench) != overlay_bench:
        raise _fail("overlay_not_derived_from_render", "the overlay benchmark is not the deterministic timed conversion of the A0m benchmark beside it")
    for path in sorted(p for p in base_wired.rglob("*") if p.is_file()):
        rel = path.relative_to(base_wired).as_posix()
        if rel in ("benchmark.yaml",):
            continue
        twin = overlay_wired / rel
        if not twin.is_file() or twin.read_bytes() != path.read_bytes():
            raise _fail("overlay_file_differs", f"{rel} in the overlay differs from the A0m render")
    extra = sorted(p.relative_to(overlay_wired).as_posix() for p in overlay_wired.rglob("*") if p.is_file()
                   and not (base_wired / p.relative_to(overlay_wired)).is_file() and p.name not in SOURCES)
    if extra:
        raise _fail("overlay_has_unexpected_files", f"{extra}")
    verify_modules(overlay_wired, repo_root)
    return {"variant": VARIANT_A0P, "run_id": qualified, "container_run_dir": facts["container_run_dir"], "issue_log_paths": facts["issue_log_paths"],
            "timing_paths": facts["timing_paths"], "overlay": str(overlay_wired), "channel": manifest.get("channel")}
