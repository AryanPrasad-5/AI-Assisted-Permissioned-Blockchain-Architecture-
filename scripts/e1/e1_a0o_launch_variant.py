"""Variant-isolated DRY-RENDER and preflight for the A0m configuration (task A0o, 2026-11-21). OFFLINE ONLY: it reads and writes files.

It is a thin successor around the A0m orchestrator. ``e1_a0m_dry_render.py``, ``e1_prelive_validation.py`` and the pinned renderer are imported, not edited, so every
historical render keeps its historical behaviour. What this tool adds:

  * the variant is stated explicitly (no default) and is the A0m variant only; a request for the historical A0e variant is refused (its own tool is unchanged);
  * the run id is variant-qualified (``<base>-v3il``), so the container path ``runs/<runId>`` can never equal an A0e path;
  * the output directory is ``<out-root>/a0m-v3il/<render-id>``; an occupied directory (any variant, or unclassified) is refused;
  * a sibling marker ``<render-id>.launch_variant.json`` records the variant, the base and qualified run ids and the selected recorder and workload files;
  * preflight re-derives the variant from the GENERATED benchmark.yaml and the modules directory and compares it with the marker and the manifests.

Nothing here starts Caliper, Docker, WSL, Fabric, Prometheus, a model or a network connection (a test asserts that). ``execute`` is refused, always.

    python scripts/e1/e1_a0o_launch_variant.py render    --variant a0m_recorder_v3_issue_logged --params P --a0m-params Q --channel NAME --out-root DIR --render-id ID
    python scripts/e1/e1_a0o_launch_variant.py preflight  --variant a0m_recorder_v3_issue_logged --run-dir DIR/a0m-v3il/ID [--candidate-manifest M]
    python scripts/e1/e1_a0o_launch_variant.py check-collision --run-dir A --run-dir B
    python scripts/e1/e1_a0o_launch_variant.py execute        (always refused)

Exit codes: 0 pass, 1 a check failed, 2 invalid request (nothing created), 5 execute refused.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
for _entry in (str(HERE), str(REPO / "src")):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

import e1_a0m_dry_render as dr                                    # noqa: E402  (A0m: reused unchanged)
from fabric_perf.controllers import channel_selection as cs        # noqa: E402
from fabric_perf.controllers import launch_variant as lv           # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi        # noqa: E402

TOOL_VERSION = "e1_a0o_launch_variant 0.1 (A0o, 2026-11-21)"
RESULT_SCHEMA = "a0o_variant_preflight_result/1"
EXIT_EXECUTE_REFUSED = 5
PASS, FAIL, NOT_CHECKED = "PASS", "FAIL", "NOT_CHECKED"


def _utc(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(timespec="seconds")


def render(a0e_params_path: Path, a0m_params_path: Path, out_root: Path, render_id: str, *, variant: str | None, channel: str | None,
           repo_root: Path = REPO, now: datetime | None = None) -> dict[str, Any]:
    """Render the A0m configuration into ``<out_root>/a0m-v3il/<render_id>`` under a variant-qualified run id. Nothing is created for a refused request."""
    if variant is None:
        raise lv.LaunchVariantError("variant_missing", "the variant must be stated explicitly; no default is applied")
    if variant == lv.VARIANT_A0E:
        raise lv.LaunchVariantError("variant_not_rendered_here", "the historical A0e variant is prepared by e1_prelive_validation.py (unchanged); this tool renders only "
                                    f"{lv.VARIANT_A0M!r}")
    if variant != lv.VARIANT_A0M:
        raise lv.LaunchVariantError("variant_unknown", f"{variant!r} is not one of {sorted(lv.VARIANTS)}")
    cs.select_experiment_channel(channel, purpose="an A0o variant render", option="--channel")          # explicit, never defaulted; protected names refused
    try:
        base_params = json.loads(Path(a0e_params_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise lv.LaunchVariantError("params_unreadable", f"{a0e_params_path}: {exc}") from exc
    if not isinstance(base_params, dict) or not isinstance(base_params.get("run_id"), str):
        raise lv.LaunchVariantError("params_invalid", "the A0e parameter file must be a JSON object with a run_id")
    base_id = base_params["run_id"]
    qualified = lv.qualified_run_id(base_id, variant)                                                 # refuses over-long and already-qualified ids
    root = lv.variant_directory(out_root, variant, render_id)
    lv.reject_if_occupied(root, variant)
    inputs = root.with_name(root.name + ".inputs")
    if inputs.exists():
        raise lv.LaunchVariantError("inputs_directory_exists", f"{inputs} exists: refusing to reuse a derived-input directory")
    inputs.mkdir(parents=True, exist_ok=False)
    derived = inputs / "a0e_params.qualified.json"
    pi.write_new_json(derived, {**base_params, "run_id": qualified})
    manifest = dr.dry_render(derived, a0m_params_path, root.parent, render_id, channel=channel, repo_root=repo_root, now=now)          # A0m, unchanged
    marker = {"schema": lv.MARKER_SCHEMA, "tool": TOOL_VERSION, "variant": variant, "base_run_id": base_id, "qualified_run_id": qualified, "render_id": render_id,
              "channel": manifest["channel"], "selected_recorder_file": lv.VARIANTS[variant]["recorder"], "selected_workload_file": lv.VARIANTS[variant]["workload"],
              "fallback": "none", "launch_authorizing_directory": manifest["wired_v3_dir"],
              "not_for_launch": ["a0e_base/prepared (the A0e derivation base: recorder v2 + legacy workload)"],
              "a0m_manifest_sha256": pi.sha256_file(root / dr.MANIFEST_NAME), "base_params_sha256": pi.sha256_file(Path(a0e_params_path)),
              "derived_params_sha256": pi.sha256_file(derived), "created_utc": _utc(now), "execution": "NOT_AUTHORIZED",
              "caliper_invoked": False, "fabric_contacted": False, "docker_or_wsl_started": False}
    pi.write_new_json(lv.marker_path(root), marker)
    lv.verify_marker(root, variant)                                                                      # fail now, not at launch
    return {"manifest": manifest, "marker": marker, "root": str(root)}


def _check(checks: list[dict[str, Any]], check_id: str, status: str, detail: Any) -> None:
    checks.append({"id": check_id, "status": status, "detail": detail})


def preflight(run_root: Path, variant: str, *, repo_root: Path = REPO, candidate_manifest: Path | None = None, now: datetime | None = None,
              write: bool = False) -> dict[str, Any]:
    """Offline variant checks plus the unchanged A0m preflight. Never contacts anything; always ``live_ready: false``."""
    root, repo = Path(run_root), Path(repo_root)
    checks: list[dict[str, Any]] = []
    manifest: dict[str, Any] = {}
    try:
        if variant not in lv.VARIANTS:
            raise lv.LaunchVariantError("variant_unknown", f"{variant!r} is not one of {sorted(lv.VARIANTS)}")
        if variant != lv.VARIANT_A0M:
            raise lv.LaunchVariantError("variant_not_checked_here", "this preflight covers the A0m variant; the A0e preflight is e1_prelive_validation.py (unchanged)")
        manifest = json.loads((root / dr.MANIFEST_NAME).read_text(encoding="utf-8"))
        lv.authorize_manifest(manifest, variant)
        _check(checks, "V01_manifest_belongs_to_variant", PASS, manifest.get("schema"))
    except (lv.LaunchVariantError, OSError, ValueError) as exc:
        _check(checks, "V01_manifest_belongs_to_variant", FAIL, str(exc))
        return _finish(checks, root, manifest, now)
    try:
        facts = lv.verify_marker(root, variant)
        _check(checks, "V02_marker_matches_generated_files", PASS, facts)
    except lv.LaunchVariantError as exc:
        _check(checks, "V02_marker_matches_generated_files", FAIL, str(exc))
        facts = {}
    try:
        qualified = facts["run_id"]
        if manifest.get("run_id") != qualified:
            raise lv.LaunchVariantError("manifest_run_id_mismatch", f"the A0m manifest run id {manifest.get('run_id')!r} differs from the qualified id {qualified!r}")
        _check(checks, "V03_run_id_is_qualified", PASS, qualified)
    except (lv.LaunchVariantError, KeyError) as exc:
        _check(checks, "V03_run_id_is_qualified", FAIL, str(exc) if facts else "the marker did not verify")
    try:
        lv.verify_modules(root / manifest["wired_v3_dir"], variant)
        _check(checks, "V04_modules_match_variant", PASS, "recorder v3, issue-logged workload, issue-log writer and workload core present")
    except (lv.LaunchVariantError, KeyError) as exc:
        _check(checks, "V04_modules_match_variant", FAIL, str(exc))
    try:
        cs.select_experiment_channel(manifest["channel"], purpose="an A0o variant preflight", option="channel")
        _check(checks, "V05_channel_not_protected", PASS, "the channel is not a protected reference or earlier experiment channel (it is NOT an approved channel)")
    except (cs.ChannelSelectionError, KeyError) as exc:
        _check(checks, "V05_channel_not_protected", FAIL, str(exc))
    base = dr.preflight(root, repo_root=repo, candidate_manifest=candidate_manifest, write=False, now=now)       # the A0m preflight, unchanged
    _check(checks, "V06_a0m_preflight", {"PASS": PASS, "FAIL": FAIL}.get(base["offline_preflight"], FAIL),
           {"offline_preflight": base["offline_preflight"], "counts": base["counts"], "failed": [c["id"] for c in base["checks"] if c["status"] == "FAIL"]})
    return _finish(checks, root, manifest, now)


def _finish(checks: list[dict[str, Any]], root: Path, manifest: dict[str, Any], now: datetime | None) -> dict[str, Any]:
    statuses = [c["status"] for c in checks]
    return {"schema": RESULT_SCHEMA, "tool": TOOL_VERSION, "run_dir": str(root), "checked_utc": _utc(now), "run_id": manifest.get("run_id"), "channel": manifest.get("channel"),
            "checks": checks, "counts": {s: statuses.count(s) for s in (PASS, FAIL, NOT_CHECKED)}, "offline_preflight": FAIL if FAIL in statuses else PASS, "live_ready": False,
            "statement": "An offline pass shows the generated files select the stated variant and carry a variant-qualified run id. It is not an authorization and shows nothing "
                         "about a live run."}


def refuse_execute() -> int:
    print("REFUSED: the execute phase is not available in this tool. A rendered configuration is not permission to run it; a live run needs a separate, versioned execution "
          "tool and explicit owner authorization for each live step. No flag or environment variable enables it.")
    return EXIT_EXECUTE_REFUSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    r = sub.add_parser("render", help="render the A0m variant under a qualified run id (offline)")
    r.add_argument("--variant", required=True)
    r.add_argument("--params", required=True, type=Path)
    r.add_argument("--a0m-params", required=True, type=Path)
    r.add_argument("--channel", required=True, type=cs.experiment_channel_arg, metavar="CHANNEL")
    r.add_argument("--out-root", required=True, type=Path)
    r.add_argument("--render-id", required=True)
    p = sub.add_parser("preflight", help="variant checks plus the A0m preflight on a rendered directory")
    p.add_argument("--variant", required=True)
    p.add_argument("--run-dir", required=True, type=Path)
    p.add_argument("--candidate-manifest", type=Path)
    c = sub.add_parser("check-collision", help="report directories whose container run paths coincide across variants")
    c.add_argument("--run-dir", action="append", required=True, type=Path)
    sub.add_parser("execute", help="always refused")
    args = parser.parse_args(argv)
    if args.phase == "execute":
        return refuse_execute()
    if args.phase == "check-collision":
        report = lv.check_collision(args.run_dir)
        print(json.dumps(report, indent=2))
        return 0 if report["ok"] else 1
    if args.phase == "render":
        try:
            out = render(args.params, args.a0m_params, args.out_root, args.render_id, variant=args.variant, channel=args.channel)
        except (lv.LaunchVariantError, dr.A0mError, dr.A0mParameterError, cs.ChannelSelectionError, OSError, ValueError, KeyError) as exc:
            print(f"REFUSED: {type(exc).__name__}: {exc}")
            return 2
        print(f"variant render {out['root']}: {out['manifest']['status']}; run id {out['marker']['qualified_run_id']}; Caliper, Fabric, Docker and WSL were not touched")
        return 0
    result = preflight(args.run_dir, args.variant, candidate_manifest=args.candidate_manifest)
    print(json.dumps(result, indent=2))
    return 0 if result["offline_preflight"] == PASS else 1


if __name__ == "__main__":
    sys.exit(main())
