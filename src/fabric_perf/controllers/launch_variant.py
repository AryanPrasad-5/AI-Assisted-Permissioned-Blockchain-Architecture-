"""Launch-variant identity and isolation for the A0e and A0m run configurations (task A0o, 2026-11-21). OFFLINE ONLY: it reads and compares files.

The defect this module closes (reproduced before any change, ``Integration/system_integration/a0o_2026-11-21/collision_reproduction_before_change.json``):

* the A0e ``prepare`` step (recorder v2 + legacy workload) and the A0m dry render (recorder v3 + issue-logged workload) take the SAME run id from the same parameter
  file and therefore write the SAME container path ``/hyperledger/caliper/workspace/runs/<runId>/...``;
* neither manifest says which variant it is, and the A0e launch manifest's step R09 names ``runs/<runId>/benchmark.yaml`` for both, so copying the wrong directory would
  silently run recorder v2 with the legacy workload while every manifest still looked consistent.

What this module does (nothing historical is edited; the pinned renderer, the A0e orchestrator and the A0m orchestrator are imported or left alone):

* names the two variants and what each one MUST select (recorder module, workload module, whether an issue log and a health block are present);
* derives the variant from the GENERATED ``benchmark.yaml`` (never from a manifest) and refuses a mixed configuration (for example recorder v3 with the legacy workload);
* gives the A0m variant a variant-qualified run id and directory, so its container path can never equal an A0e path (A0e keeps its historical unqualified id);
* refuses an existing directory that belongs to another variant, an unclassified one, and a manifest whose schema belongs to the other variant;
* has no fallback: no variant is chosen by default, and nothing falls back to recorder v2, the legacy workload or a protected reference channel.

It imports no process, network, container, model or Caliper API (a test asserts that).
"""

from __future__ import annotations

import json
import posixpath
import re
from pathlib import Path
from typing import Any, Mapping

VARIANT_A0E = "a0e_recorder_v2_legacy_workload"
VARIANT_A0M = "a0m_recorder_v3_issue_logged"
VARIANT_NONE = "unqualified_or_unclassified"
MARKER_SCHEMA = "launch_variant_marker/1"
MARKER_SUFFIX = ".launch_variant.json"
RUN_ID_MAX = 40                                                 # the pinned renderer's run id limit (e1_la2_render.RUN_ID)
RUN_ID_RE = re.compile(r"[A-Za-z0-9_.-]{1,%d}" % RUN_ID_MAX)
A0E_LAUNCH_SCHEMA = "prelive_launch_manifest/1"
A0M_RENDER_SCHEMA = "a0m_dry_render_manifest/1"

RECORDER_V2, RECORDER_V3 = "e1_per_tx_recorder_v2.js", "e1_per_tx_recorder_v3.js"
WORKLOAD_LEGACY, WORKLOAD_LOGGED = "e1_create_asset.js", "e1_create_asset_issue_logged.js"

VARIANTS: dict[str, dict[str, Any]] = {
    VARIANT_A0E: {"recorder": RECORDER_V2, "workload": WORKLOAD_LEGACY, "issue_log": False, "health": False, "run_id_suffix": None, "slug": "a0e-v2",
                  "manifest_schema": A0E_LAUNCH_SCHEMA, "forbidden_modules": (RECORDER_V3, WORKLOAD_LOGGED, "e1_issue_log.js"),
                  "note": "HISTORICAL variant; its tool (e1_prelive_validation.py prepare) is unchanged and keeps its unqualified run id"},
    VARIANT_A0M: {"recorder": RECORDER_V3, "workload": WORKLOAD_LOGGED, "issue_log": True, "health": True, "run_id_suffix": "-v3il", "slug": "a0m-v3il",
                  "manifest_schema": A0M_RENDER_SCHEMA, "forbidden_modules": (),
                  "required_modules": (RECORDER_V3, WORKLOAD_LOGGED, "e1_issue_log.js", "e1_workload_core.js"),
                  "note": "issue-logged workload + recorder v3; its run id carries the variant suffix so its container path differs from every A0e path"},
}


class LaunchVariantError(ValueError):
    """A launch configuration is mixed, belongs to another variant, collides, or cannot be classified. ``code`` is machine-readable."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code, self.detail = code, message


def _fail(code: str, message: str) -> LaunchVariantError:
    return LaunchVariantError(code, message)


# ---------------------------------------------------------------------------------------------------------------- run id
def qualified_run_id(base_run_id: str, variant: str) -> str:
    """The run id a variant must use. A0m adds its suffix (never truncating: a base that would exceed the limit is refused); A0e keeps the historical id."""
    spec = VARIANTS.get(variant)
    if spec is None:
        raise _fail("variant_unknown", f"{variant!r} is not one of {sorted(VARIANTS)}; no variant is assumed")
    if not isinstance(base_run_id, str) or not RUN_ID_RE.fullmatch(base_run_id):
        raise _fail("run_id_invalid", f"base run id {base_run_id!r} must match {RUN_ID_RE.pattern}")
    for other in VARIANTS.values():
        suffix = other["run_id_suffix"]
        if suffix and base_run_id.endswith(suffix):
            raise _fail("run_id_already_qualified", f"{base_run_id!r} already ends with the variant suffix {suffix!r}; supply the BASE run id")
    suffix = spec["run_id_suffix"]
    if suffix is None:
        return base_run_id
    qualified = base_run_id + suffix
    if len(qualified) > RUN_ID_MAX:
        raise _fail("run_id_too_long", f"{qualified!r} would be {len(qualified)} characters; the renderer allows {RUN_ID_MAX}. The id is never truncated: shorten the base run id")
    return qualified


def variant_directory(out_root: Path, variant: str, render_id: str) -> Path:
    """``<out_root>/<variant slug>/<render id>``: two variants can never resolve to the same final directory."""
    spec = VARIANTS.get(variant)
    if spec is None:
        raise _fail("variant_unknown", f"{variant!r} is not one of {sorted(VARIANTS)}")
    return Path(out_root) / spec["slug"] / render_id


def marker_path(root: Path) -> Path:
    """The variant marker is a SIBLING of the rendered root, not a file inside it: the A0m preflight hashes every file inside the root and would reject an extra one."""
    root = Path(root)
    return root.with_name(root.name + MARKER_SUFFIX)


# ---------------------------------------------------------------------------------------------------------------- derivation from generated files
def _read_json(path: Path) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise _fail("file_unreadable", f"cannot read {path}: {exc}") from exc


def derive_variant(benchmark: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a generated benchmark from what it actually names. Raises ``LaunchVariantError`` for anything that is not exactly one of the two variants.

    Facts returned: the variant, the monitor module, the sorted workload modules, the run id, the container run directory and the monitor output directory."""
    monitors = (benchmark.get("monitors") or {}).get("transaction")
    if not isinstance(monitors, list) or len(monitors) != 1 or not isinstance(monitors[0], dict):
        raise _fail("monitor_not_single", "the benchmark must have exactly one transaction monitor")
    monitor = monitors[0]
    recorder = posixpath.basename(str(monitor.get("module")))
    options = monitor.get("options") or {}
    rounds = (benchmark.get("test") or {}).get("rounds")
    if not isinstance(rounds, list) or not rounds:
        raise _fail("no_rounds", "the benchmark has no rounds")
    workloads, issue_logs = set(), []
    for index, rnd in enumerate(rounds):
        workload = rnd.get("workload") if isinstance(rnd, dict) else None
        if not isinstance(workload, dict):
            raise _fail("round_without_workload", f"round {index} has no workload")
        workloads.add(posixpath.basename(str(workload.get("module"))))
        args = workload.get("arguments") or {}
        issue_logs.append(args.get("issueLog") if isinstance(args, dict) else None)
    has_health = "health" in options
    matches = []
    for name, spec in VARIANTS.items():
        if (recorder == spec["recorder"] and workloads == {spec["workload"]} and has_health == spec["health"]
                and all((entry is not None) == spec["issue_log"] for entry in issue_logs)):
            matches.append(name)
    if len(matches) != 1:
        raise _fail("variant_mixed_or_unknown",
                    f"the benchmark selects recorder {recorder!r}, workload(s) {sorted(workloads)}, health block {has_health}, issue-log arguments "
                    f"{[e is not None for e in issue_logs]}: that is not exactly the A0e or the A0m variant (a mixed configuration is refused, never repaired)")
    out_dir = options.get("outputDir")
    if not isinstance(out_dir, str) or not out_dir.startswith("/"):
        raise _fail("output_dir_invalid", f"the monitor outputDir {out_dir!r} must be an absolute POSIX path")
    return {"variant": matches[0], "monitor_module": recorder, "workload_modules": sorted(workloads), "run_id": options.get("runId"),
            "monitor_output_dir": out_dir, "container_run_dir": posixpath.dirname(out_dir), "issue_log_paths": [e["path"] for e in issue_logs if isinstance(e, Mapping)]}


def classify_directory(path: Path) -> dict[str, Any]:
    """Which variant an existing directory belongs to, from its OWN generated files (the A0m manifest names the v3 run directory; the A0e launch manifest names the wired one).
    A directory that is neither, or whose files disagree, is ``unclassified``."""
    path = Path(path)
    if (path / "a0m_config_manifest.json").is_file():
        try:
            manifest = _read_json(path / "a0m_config_manifest.json")
            facts = derive_variant(_read_json(path / manifest["wired_v3_dir"] / "benchmark.yaml"))
            return {"variant": facts["variant"], "facts": facts, "manifest_schema": manifest.get("schema")}
        except (LaunchVariantError, KeyError, TypeError) as exc:
            return {"variant": VARIANT_NONE, "reason": str(exc)}
    if (path / "launch_manifest.json").is_file():
        try:
            manifest = _read_json(path / "launch_manifest.json")
            facts = derive_variant(_read_json(path / manifest["wired_run_dir"] / "benchmark.yaml"))
            return {"variant": facts["variant"], "facts": facts, "manifest_schema": manifest.get("schema")}
        except (LaunchVariantError, KeyError, TypeError) as exc:
            return {"variant": VARIANT_NONE, "reason": str(exc)}
    return {"variant": VARIANT_NONE, "reason": "no launch or render manifest"}


def reject_if_occupied(root: Path, variant: str) -> None:
    """Called before a render claims ``root``. Every render gets a NEW directory; an occupied one is refused, and the message says whose it is."""
    root = Path(root)
    if not root.exists() and not marker_path(root).exists():
        return
    if marker_path(root).exists() and not root.exists():
        raise _fail("stale_marker_without_directory", f"{marker_path(root).name} exists but {root.name} does not: refusing to reuse the claim")
    found = classify_directory(root)
    if found["variant"] == VARIANT_NONE:
        raise _fail("directory_unclassified", f"{root} exists and cannot be classified ({found.get('reason')}); refusing to reuse it")
    if found["variant"] != variant:
        raise _fail("directory_belongs_to_other_variant", f"{root} belongs to variant {found['variant']!r}, not {variant!r}")
    raise _fail("directory_exists_same_variant", f"{root} already holds a {variant!r} run: every render gets a new, isolated directory")


# ---------------------------------------------------------------------------------------------------------------- verification
def check_collision(run_dirs: list[Path]) -> dict[str, Any]:
    """Report run directories that would resolve to the same container run path while belonging to different variants (the historical A0e/A0m defect)."""
    by_path: dict[str, list[dict[str, Any]]] = {}
    unclassified = []
    for directory in run_dirs:
        found = classify_directory(directory)
        if found["variant"] == VARIANT_NONE:
            unclassified.append({"dir": str(directory), "reason": found.get("reason")})
            continue
        by_path.setdefault(found["facts"]["container_run_dir"], []).append({"dir": str(directory), "variant": found["variant"], "run_id": found["facts"]["run_id"]})
    collisions = [{"container_run_dir": path, "entries": entries} for path, entries in sorted(by_path.items())
                  if len({e["variant"] for e in entries}) > 1]
    return {"collisions": collisions, "unclassified": unclassified, "checked": len(run_dirs), "ok": not collisions and not unclassified}


def authorize_manifest(manifest: Mapping[str, Any], expected_variant: str) -> None:
    """Refuse a manifest that belongs to another variant. A stale A0e launch manifest never authorizes an A0m run, and an A0m manifest never authorizes an A0e run."""
    spec = VARIANTS.get(expected_variant)
    if spec is None:
        raise _fail("variant_unknown", f"{expected_variant!r} is not one of {sorted(VARIANTS)}")
    schema = manifest.get("schema")
    if schema != spec["manifest_schema"]:
        other = next((n for n, s in VARIANTS.items() if s["manifest_schema"] == schema), None)
        raise _fail("stale_manifest_wrong_variant" if other else "manifest_schema_unknown",
                    f"the manifest schema is {schema!r} ({other or 'no known variant'}); variant {expected_variant!r} needs {spec['manifest_schema']!r}")


def verify_marker(root: Path, expected_variant: str) -> dict[str, Any]:
    """Read the sibling marker and prove it against the files. Returns the marker's facts. Raises on any inconsistency."""
    root = Path(root)
    path = marker_path(root)
    if not path.is_file():
        raise _fail("variant_marker_missing", f"{path.name} is missing: a directory without a variant marker (including every historical A0m render) is never accepted as variant {expected_variant!r}")
    marker = _read_json(path)
    if marker.get("schema") != MARKER_SCHEMA:
        raise _fail("variant_marker_schema", f"marker schema is {marker.get('schema')!r}")
    if marker.get("variant") != expected_variant:
        raise _fail("variant_marker_mismatch", f"the marker says {marker.get('variant')!r}, the caller expects {expected_variant!r}")
    spec = VARIANTS[expected_variant]
    found = classify_directory(root)
    if found["variant"] != expected_variant:
        raise _fail("generated_files_do_not_match_variant", f"the generated files select {found['variant']!r} ({found.get('reason', 'derived from benchmark.yaml')}), not {expected_variant!r}")
    facts = found["facts"]
    expected_id = qualified_run_id(marker.get("base_run_id"), expected_variant)
    if marker.get("qualified_run_id") != expected_id or facts["run_id"] != expected_id:
        raise _fail("run_id_not_qualified", f"marker run id {marker.get('qualified_run_id')!r}, benchmark run id {facts['run_id']!r}, expected {expected_id!r}")
    if posixpath.basename(facts["container_run_dir"]) != expected_id:
        raise _fail("container_path_not_qualified", f"the container run directory {facts['container_run_dir']!r} does not end with the qualified run id {expected_id!r}")
    for path_text in facts["issue_log_paths"]:
        if not path_text.startswith(facts["container_run_dir"] + "/"):
            raise _fail("issue_log_outside_run_directory", f"{path_text!r} is not inside {facts['container_run_dir']!r}")
    if marker.get("selected_recorder_file") != spec["recorder"] or marker.get("selected_workload_file") != spec["workload"]:
        raise _fail("marker_selection_mismatch", "the marker's recorder or workload file differs from the variant's")
    if marker.get("fallback") not in (None, "none"):
        raise _fail("fallback_declared", "a variant marker may not declare a fallback recorder or workload")
    return {"variant": expected_variant, "run_id": expected_id, "container_run_dir": facts["container_run_dir"], "monitor_module": facts["monitor_module"],
            "workload_modules": facts["workload_modules"], "marker": path.name}


def verify_modules(wired_dir: Path, variant: str) -> dict[str, Any]:
    """The modules directory beside the generated benchmark must hold what the variant needs, and (A0e) must not hold what belongs to the other variant."""
    spec = VARIANTS[variant]
    modules = Path(wired_dir) / "modules"
    present = sorted(p.name for p in modules.glob("*.js")) if modules.is_dir() else []
    missing = [m for m in spec.get("required_modules", ()) if m not in present]
    forbidden = [m for m in spec["forbidden_modules"] if m in present]
    if missing or forbidden:
        raise _fail("modules_do_not_match_variant", f"missing {missing}, forbidden {forbidden} for variant {variant!r}")
    return {"present": present, "missing": [], "forbidden_present": []}
