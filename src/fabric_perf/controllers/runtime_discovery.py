"""Caliper container identity and mount discovery: what the repository declares, and the read-only procedure that must settle the rest (task A0u, 2026-11-22). OFFLINE.

Why. The A0t report found two Caliper container names: ``caliper`` (the rendered plan, ``scripts/e1/e1_la2_render.py``) and ``caliper-benchmark-001`` (the earlier benchmark scripts and the 2026-11-10 note).
They are different Docker containers (a compose SERVICE name, a container NAME, a container ID, a container-side workspace path and a host-side mount source are five different things). This module
(1) scans the repository's own declarations with ``ast``/regular expressions (it reads source files only) so the discrepancy is evidence, not recollection, (2) writes the future runtime-discovery
procedure as structured vectors that are NOT executed, and (3) evaluates what such a discovery would return against the plan. It never runs ``docker`` or ``wsl.exe``, never guesses a name, and
never lets a synthetic observation satisfy a live gate.

CONFIRMED by the scan (see ``scan_container_declarations``): the rendered plan names ``caliper``; ``e1_quicklook*.py``, ``e1_final_comparison.py``, ``e1_existing_channel_pilot.py``,
``run_contemporaneous_capture.py`` and ``synchronized_shadow_replay.py`` name ``caliper-benchmark-001`` (``e1_quicklook.caliper_argv`` substitutes ``argv[4]`` of the rendered vector); no compose file
declares a Caliper service (the containers were created with ``docker run``). The 2026-11-17 proposal recorded both containers as existing and Exited(255); that is a dated observation, not a
current one. UNRESOLVED offline: which container exists now, which one the measured P1b runs used for the bound SUT packages, and the mount of each.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import process_runner as pr

DISCOVERY_VERSION = "runtime_discovery 0.1 (A0u, 2026-11-22)"
DISCOVERY_SCHEMA = "a0u_runtime_discovery_spec/1"
NAME_CONSTANTS = ("CAL_CONTAINER", "CALIPER", "CALIPER_CONTAINER")
AUTH_CLASS = "SEPARATELY_AUTHORIZED_READ_ONLY_RUNTIME_INSPECTION_NOT_AUTHORIZED"
OBSERVATION_REAL, OBSERVATION_SYNTHETIC = "REAL_RUNTIME_OBSERVATION", "SYNTHETIC"
PLACEHOLDER = "AUTHORIZED_CONTAINER_NAME_REQUIRED"        # a plain token: '<' and '>' are shell metacharacters and are refused in every vector
_CONTAINER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_COMPOSE_NAME = re.compile(r"^\s*container_name:\s*([A-Za-z0-9_.-]+)\s*$", re.MULTILINE)
_COMPOSE_SERVICE = re.compile(r"^  (caliper[A-Za-z0-9_.-]*):\s*$", re.MULTILINE)


def scan_container_declarations(repo_root: Path) -> list[dict[str, Any]]:
    """Every place the repository names a Caliper container. Reads ``scripts/e1/*.py`` and ``fabric-testbed`` compose files; starts nothing."""
    root, found = Path(repo_root), []
    for path in sorted((root / "scripts" / "e1").glob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError, UnicodeDecodeError):
            continue
        rel = path.relative_to(root).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    pairs = list(zip(target.elts, node.value.elts)) if isinstance(target, ast.Tuple) and isinstance(node.value, ast.Tuple) else [(target, node.value)]
                    for name_node, value in pairs:
                        if isinstance(name_node, ast.Name) and name_node.id in NAME_CONSTANTS and isinstance(value, ast.Constant) and isinstance(value.value, str):
                            found.append({"file": rel, "line": node.lineno, "kind": "constant", "symbol": name_node.id, "name": value.value})
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "add_argument" and node.args:
                first = node.args[0]
                if isinstance(first, ast.Constant) and first.value in ("--caliper-container", "--container"):
                    default = next((k.value.value for k in node.keywords if k.arg == "default" and isinstance(k.value, ast.Constant)), None)
                    if isinstance(default, str) and "caliper" in default:
                        found.append({"file": rel, "line": node.lineno, "kind": "argparse_default", "symbol": first.value, "name": default})
    for path in sorted((root / "fabric-testbed").rglob("*.y*ml")):
        text = path.read_text(encoding="utf-8", errors="replace")
        rel = path.relative_to(root).as_posix()
        for match in _COMPOSE_NAME.finditer(text):
            if "caliper" in match.group(1):
                found.append({"file": rel, "line": text[: match.start()].count("\n") + 1, "kind": "compose_container_name", "symbol": "container_name", "name": match.group(1)})
        for match in _COMPOSE_SERVICE.finditer(text):
            found.append({"file": rel, "line": text[: match.start()].count("\n") + 1, "kind": "compose_service", "symbol": "service", "name": match.group(1)})
    return found


def summarize_declarations(declarations: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    names: dict[str, list[str]] = {}
    for d in declarations:
        names.setdefault(d["name"], []).append(f"{d['file']}:{d['line']} ({d['kind']})")
    container_names = {n for n, where in names.items() if any("compose_service" not in w for w in where)}
    return {"names": {k: names[k] for k in sorted(names)}, "distinct_container_names": sorted(container_names), "conflict": len(container_names) > 1,
            "compose_services": sorted({d["name"] for d in declarations if d["kind"] == "compose_service"}),
            "interpretation": "two distinct container names are declared; they are separate containers, not aliases; which exists and which the measured runs used is UNRESOLVED offline"}


def identity_concepts(plan: Mapping[str, Any]) -> dict[str, Any]:
    """The five things that are not interchangeable, with what the plan says about each."""
    return {"compose_service_name": {"value": None, "note": "no compose file declares a Caliper service; the containers were created with docker run"},
            "container_name": {"plan_value": plan["mounts"]["caliper_container"], "note": "the Docker name used in 'docker exec'; the rendered plan says this; the benchmark scripts substitute another"},
            "container_id": {"plan_value": None, "note": "assigned by Docker at creation; must be read at discovery and recorded; a name can be re-bound to a new container, an ID cannot"},
            "container_workspace_path": {"plan_value": plan["mounts"]["container_workspace"], "note": "the in-container path Caliper is launched in"},
            "host_mount_source": {"plan_value": plan["mounts"]["host_workspace"], "note": "the WSL-native directory the plan expects to be bind-mounted there; a declaration, not an observation"}}


def _vector(step: str, purpose: str, argv: Sequence[str], *, needs_running: bool, passes_when: str, on_failure: str) -> dict[str, Any]:
    pr.validate_argv(argv)                                     # the same structural rules as every other vector (no shell, no metacharacters)
    return {"step": step, "purpose": purpose, "argv": list(argv), "domain": pr.domain_of(argv), "requires_running_container": needs_running, "executed": False,
            "authorization": AUTH_CLASS, "passes_when": passes_when, "on_failure": on_failure}


def discovery_spec(plan: Mapping[str, Any], declarations: Sequence[Mapping[str, Any]], *, distro: Any) -> dict[str, Any]:
    """The future read-only discovery procedure for THIS plan. Every vector is text; none is executed, and all of them need a separate, explicit authorization."""
    if not (isinstance(distro, str) and _CONTAINER_NAME.fullmatch(distro)):
        raise ValueError("the WSL distribution is required (no default)")
    summary = summarize_declarations(declarations)
    candidates = summary["distinct_container_names"]
    ws, cws = plan["mounts"]["host_workspace"], plan["mounts"]["container_workspace"]
    run_c = plan["mounts"]["run_dir_container"]
    steps: list[dict[str, Any]] = [_vector("D1_list_containers", "which containers exist, their IDs, state and image (read-only listing)", ["docker", "ps", "-a", "--no-trunc", "--format", "{{.ID}} {{.Names}} {{.Status}} {{.Image}}"],
                                           needs_running=False, passes_when="exactly one container is listed under the authorized name", on_failure="stop; the plan's container is not confirmed")]
    for name in candidates:
        if not _CONTAINER_NAME.fullmatch(name):
            raise ValueError(f"declared container name {name!r} is not a valid Docker name")
        steps.append(_vector(f"D2_inspect_{name}", f"full read-only inspection of {name} (ID, mounts, user, image, state); works on a stopped container", ["docker", "inspect", name],
                             needs_running=False, passes_when=f"a bind mount has Destination {cws} and Source {ws}, read-write, and the ID is recorded", on_failure="stop; record the observed mounts"))
    steps += [_vector("D3_workspace_runs_exists", "the host-side runs directory exists inside the distribution", ["wsl.exe", "-d", distro, "--exec", "test", "-d", ws + "/runs"],
                      needs_running=False, passes_when="exit code 0", on_failure="stop; X04 (mkdir without -p) would fail"),
              _vector("D4_workspace_runs_writable_by_wsl_user", "the WSL user can create the run directory", ["wsl.exe", "-d", distro, "--exec", "test", "-w", ws + "/runs"],
                      needs_running=False, passes_when="exit code 0", on_failure="stop; the staging step would fail"),
              _vector("D5_container_can_write_runs", "the container's user can write under the mounted runs directory (uid mapping)", ["docker", "exec", PLACEHOLDER, "test", "-w", cws + "/runs"],
                      needs_running=True, passes_when="exit code 0", on_failure="stop; Caliper could not write the issue log or recorder files"),
              _vector("D6_container_sees_generated_modules", "after a SEPARATELY authorized staging of the run directory, the workload module is readable in the container", ["docker", "exec", PLACEHOLDER, "test", "-r", run_c + "/modules/e1_create_asset_issue_logged_timed.js"],
                      needs_running=True, passes_when="exit code 0", on_failure="stop; the staged files are not visible through the mount")]
    return {"schema": DISCOVERY_SCHEMA, "version": DISCOVERY_VERSION, "status": "SPECIFICATION_ONLY_NOT_EXECUTED", "plan_sha256": plan["plan_sha256"], "run_id": plan["run_id"],
            "declarations": list(declarations), "summary": summary, "concepts": identity_concepts(plan),
            "state_observed_on_2026_11_17": "both containers Exited(255) per CALIPER_BOUNDED_VALIDATION_PROPOSAL_2026-11-17.md section 2.3 (dated; not current; not re-observed here)",
            "steps": steps, "placeholder_rule": f"{PLACEHOLDER} must be replaced by the owner's authorized name, which must equal a container observed in D1/D2; no name is chosen here",
            "start_decision": "D5 and D6 need a running container; starting a stopped container is a state change and is a separate authorization (not part of this read-only discovery)",
            "claims_not_made": ["any container exists", "either name is the right one", "any mount is as declared", "any command works on the target machine"]}


def evaluate_discovery(plan: Mapping[str, Any], authorized_name: Any, inspected: Any, *, observation_class: str) -> dict[str, Any]:
    """Findings for the output of D1/D2 (``inspected``: the parsed ``docker inspect`` JSON list). ``gate_satisfied`` is True only for a REAL runtime observation with no finding."""
    findings: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        findings.append({"code": code, "message": message})

    if observation_class not in (OBSERVATION_REAL, OBSERVATION_SYNTHETIC):
        raise ValueError(f"observation_class must be {OBSERVATION_REAL!r} or {OBSERVATION_SYNTHETIC!r}")
    if not (isinstance(authorized_name, str) and _CONTAINER_NAME.fullmatch(authorized_name)):
        bad("authorized_name_unset", "the owner's authorized container name is required (no default, no guess)")
    elif authorized_name != plan["mounts"]["caliper_container"]:
        bad("plan_container_mismatch", f"authorized {authorized_name!r}, the plan launches in {plan['mounts']['caliper_container']!r}; a different container needs a new plan")
    matches = [c for c in (inspected if isinstance(inspected, list) else []) if isinstance(c, Mapping) and str(c.get("Name", "")).lstrip("/") == authorized_name]
    if len(matches) != 1:
        bad("container_not_unique", f"{len(matches)} inspected containers are named {authorized_name!r}; exactly one is required")
    else:
        c = matches[0]
        if not re.fullmatch(r"[0-9a-f]{64}", str(c.get("Id", ""))):
            bad("container_id_missing", "the container ID must be a 64-hex string and is recorded")
        mounts = [m for m in c.get("Mounts") or [] if isinstance(m, Mapping) and m.get("Destination") == plan["mounts"]["container_workspace"]]
        if len(mounts) != 1:
            bad("workspace_mount_missing", f"{len(mounts)} mounts have Destination {plan['mounts']['container_workspace']!r}")
        else:
            m = mounts[0]
            if m.get("Type") != "bind" or m.get("Source") != plan["mounts"]["host_workspace"]:
                bad("workspace_mount_source_mismatch", f"mount is {m.get('Type')!r} from {m.get('Source')!r}, the plan expects a bind from {plan['mounts']['host_workspace']!r}")
            if m.get("RW") is not True:
                bad("workspace_mount_not_writable", "the workspace mount is not read-write")
    return {"valid": not findings, "findings": findings, "observation_class": observation_class, "gate_satisfied": (not findings) and observation_class == OBSERVATION_REAL,
            "recorded": [{"Id": c.get("Id"), "Name": c.get("Name"), "State": (c.get("State") or {}).get("Status"), "User": (c.get("Config") or {}).get("User")} for c in matches],
            "note": "a SYNTHETIC observation never satisfies a live gate"}


def spec_json(spec: Mapping[str, Any]) -> str:
    return json.dumps(spec, indent=2, sort_keys=True) + "\n"
