"""Timed-issue-log variant: DRY-RENDER overlay and preflight (task A0p, 2026-11-22). OFFLINE ONLY: it reads and writes files.

Selects the timed issue-log writer (``e1_create_asset_issue_logged_timed.js``) explicitly and never by default. It is a successor wrapper: ``e1_a0m_dry_render.py`` is called unchanged to
render the A0m configuration under a variant-qualified run id (``<base>-v3ilt``, directory ``<out-root>/a0p-v3ilt/<render-id>``); a deterministic OVERLAY beside it
(``<render-id>.timed_overlay/wired_v3_timed/<run>``) swaps in the timed workload and gives every round an ``issueLogTimed`` argument (log path and timing-sidecar path). A sibling marker
``<render-id>.launch_variant.json`` records the selection. See ``fabric_perf.controllers.timed_variant`` for the rules.

Nothing here starts Caliper, Docker, WSL, Fabric, Prometheus, a model or a network connection (a test asserts that). ``execute`` is refused, always.

    python scripts/e1/e1_a0p_launch_variant.py render    --variant a0p_recorder_v3_timed_issue_log --params P --a0m-params Q --channel NAME --out-root DIR --render-id ID
    python scripts/e1/e1_a0p_launch_variant.py preflight  --variant a0p_recorder_v3_timed_issue_log --run-dir DIR/a0p-v3ilt/ID [--candidate-manifest M]
    python scripts/e1/e1_a0p_launch_variant.py execute        (always refused)

Exit codes: 0 pass, 1 a check failed, 2 invalid request (nothing created), 5 execute refused.
"""

from __future__ import annotations

import argparse
import json
import shutil
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
from fabric_perf.controllers import timed_variant as tv            # noqa: E402

TOOL_VERSION = "e1_a0p_launch_variant 0.1 (A0p, 2026-11-22)"
RESULT_SCHEMA = "a0p_variant_preflight_result/1"
EXIT_EXECUTE_REFUSED = 5
PASS, FAIL, NOT_CHECKED = "PASS", "FAIL", "NOT_CHECKED"


def _utc(now: datetime | None = None) -> str:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(timespec="seconds")


def render(a0e_params_path: Path, a0m_params_path: Path, out_root: Path, render_id: str, *, variant: str | None, channel: str | None,
           repo_root: Path = REPO, now: datetime | None = None) -> dict[str, Any]:
    """Render the timed variant. Nothing is created for a refused request."""
    if variant is None:
        raise tv.TimedVariantError("variant_missing", "the variant must be stated explicitly; no default is applied")
    if variant != tv.VARIANT_A0P:
        raise tv.TimedVariantError("variant_not_rendered_here", f"this tool renders only {tv.VARIANT_A0P!r}; {variant!r} has its own tool (A0o: e1_a0o_launch_variant.py, "
                                   "A0e: e1_prelive_validation.py), both unchanged")
    cs.select_experiment_channel(channel, purpose="an A0p timed-variant render", option="--channel")
    try:
        base_params = json.loads(Path(a0e_params_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise tv.TimedVariantError("params_unreadable", f"{a0e_params_path}: {exc}") from exc
    if not isinstance(base_params, dict) or not isinstance(base_params.get("run_id"), str):
        raise tv.TimedVariantError("params_invalid", "the A0e parameter file must be a JSON object with a run_id")
    base_id = base_params["run_id"]
    qualified = tv.qualified_run_id(base_id)
    root = tv.variant_directory(out_root, render_id)
    overlay = tv.overlay_directory(root)
    for taken in (root, overlay, tv.marker_path(root), root.with_name(root.name + ".inputs")):
        if taken.exists():
            raise tv.TimedVariantError("output_occupied", f"{taken} exists: every render gets a new, isolated directory (nothing is reused or overwritten)")
    inputs = root.with_name(root.name + ".inputs")
    inputs.mkdir(parents=True, exist_ok=False)
    derived = inputs / "a0e_params.qualified.json"
    pi.write_new_json(derived, {**base_params, "run_id": qualified})
    manifest = dr.dry_render(derived, a0m_params_path, root.parent, render_id, channel=channel, repo_root=repo_root, now=now)          # A0m, unchanged
    base_wired = root / manifest["wired_v3_dir"]
    wired = overlay / "wired_v3_timed" / base_wired.name
    shutil.copytree(base_wired, wired)
    bench = json.loads((base_wired / "benchmark.yaml").read_text(encoding="utf-8"))
    (wired / "benchmark.yaml").write_text(json.dumps(tv.timed_benchmark(bench), indent=2) + "\n", encoding="utf-8")
    for name, rel in tv.SOURCES.items():
        shutil.copyfile(Path(repo_root) / rel, wired / "modules" / name)
    files = {p.relative_to(overlay).as_posix(): pi.sha256_file(p) for p in sorted(overlay.rglob("*")) if p.is_file()}
    pi.write_new_json(overlay / "a0p_overlay_manifest.json", {"schema": tv.OVERLAY_SCHEMA, "tool": TOOL_VERSION, "base_render": str(root), "files": files,
                                                              "derivation": "timed_benchmark(A0m wired_v3 benchmark.yaml); modules timed workload + timed writer copied from the repository",
                                                              "execution": "NOT_AUTHORIZED"})
    marker = {"schema": tv.MARKER_SCHEMA, "tool": TOOL_VERSION, "variant": tv.VARIANT_A0P, "base_run_id": base_id, "qualified_run_id": qualified, "render_id": render_id,
              "channel": manifest["channel"], "selected_recorder_file": lv.RECORDER_V3, "selected_workload_file": tv.WORKLOAD_TIMED, "selected_writer_file": tv.WRITER_TIMED,
              "issue_log_schema": "e1_issue_log/1", "timing_sidecar_schema": "e1_issue_log_timing/1", "fallback": "none",
              "launch_authorizing_directory": str(wired.relative_to(overlay.parent).as_posix()),
              "not_for_launch": [f"{root.name}/{manifest['wired_v3_dir']} (the untouched A0m render: A0l issue-logged workload)", "a0e_base/prepared (recorder v2 + legacy workload)"],
              "a0m_manifest_sha256": pi.sha256_file(root / dr.MANIFEST_NAME), "created_utc": _utc(now), "execution": "NOT_AUTHORIZED", "caliper_invoked": False,
              "fabric_contacted": False, "docker_or_wsl_started": False}
    pi.write_new_json(tv.marker_path(root), marker)
    tv.verify_overlay(root, overlay, repo_root)                                                         # fail now, not at launch
    return {"manifest": manifest, "marker": marker, "root": str(root), "overlay": str(overlay)}


def _check(checks: list[dict[str, Any]], check_id: str, status: str, detail: Any) -> None:
    checks.append({"id": check_id, "status": status, "detail": detail})


def preflight(run_root: Path, variant: str, *, repo_root: Path = REPO, candidate_manifest: Path | None = None, now: datetime | None = None) -> dict[str, Any]:
    """Offline checks of the overlay plus the unchanged A0m preflight on the render beside it. Never contacts anything; always ``live_ready: false``."""
    root, repo = Path(run_root), Path(repo_root)
    checks: list[dict[str, Any]] = []
    manifest: dict[str, Any] = {}
    try:
        if variant != tv.VARIANT_A0P:
            raise tv.TimedVariantError("variant_unknown", f"{variant!r} is not {tv.VARIANT_A0P!r}; this preflight does not fall back to another variant")
        manifest = json.loads((root / dr.MANIFEST_NAME).read_text(encoding="utf-8"))
        lv.authorize_manifest(manifest, lv.VARIANT_A0M)                         # the base render is an A0m render
        _check(checks, "W01_base_render_is_an_a0m_render", PASS, manifest.get("schema"))
    except (tv.TimedVariantError, lv.LaunchVariantError, OSError, ValueError) as exc:
        _check(checks, "W01_base_render_is_an_a0m_render", FAIL, str(exc))
        return _finish(checks, root, manifest, now)
    facts: dict[str, Any] = {}
    try:
        facts = tv.verify_overlay(root, tv.overlay_directory(root), repo)
        _check(checks, "W02_overlay_matches_marker_and_render", PASS, {"run_id": facts["run_id"], "issue_log_paths": len(facts["issue_log_paths"])})
    except (tv.TimedVariantError, lv.LaunchVariantError, OSError, ValueError, KeyError) as exc:
        _check(checks, "W02_overlay_matches_marker_and_render", FAIL, str(exc))
    try:
        overlay_wired = tv.overlay_directory(root) / "wired_v3_timed" / (root / manifest["wired_v3_dir"]).name
        graph = dr.resolve_requires(overlay_wired / "modules", tv.ENTRY_MODULES)
        _check(checks, "W03_module_requires_resolve", PASS, {"modules": sorted(graph["graph"]), "external_unverifiable_offline": graph["external_unverifiable_offline"]})
    except (dr.A0mError, OSError, KeyError) as exc:
        _check(checks, "W03_module_requires_resolve", FAIL, str(exc))
    try:
        cs.select_experiment_channel(manifest["channel"], purpose="an A0p timed-variant preflight", option="channel")
        _check(checks, "W04_channel_not_protected", PASS, "the channel is not a protected reference or earlier experiment channel (it is NOT an approved channel)")
    except (cs.ChannelSelectionError, KeyError) as exc:
        _check(checks, "W04_channel_not_protected", FAIL, str(exc))
    base = dr.preflight(root, repo_root=repo, candidate_manifest=candidate_manifest, write=False, now=now)       # the A0m preflight on the untouched render, unchanged
    _check(checks, "W05_a0m_preflight_on_untouched_render", {"PASS": PASS, "FAIL": FAIL}.get(base["offline_preflight"], FAIL),
           {"offline_preflight": base["offline_preflight"], "counts": base["counts"], "failed": [c["id"] for c in base["checks"] if c["status"] == "FAIL"]})
    return _finish(checks, root, manifest, now)


def _finish(checks: list[dict[str, Any]], root: Path, manifest: dict[str, Any], now: datetime | None) -> dict[str, Any]:
    statuses = [c["status"] for c in checks]
    return {"schema": RESULT_SCHEMA, "tool": TOOL_VERSION, "run_dir": str(root), "checked_utc": _utc(now), "run_id": manifest.get("run_id"), "channel": manifest.get("channel"),
            "checks": checks, "counts": {s: statuses.count(s) for s in (PASS, FAIL, NOT_CHECKED)}, "offline_preflight": FAIL if FAIL in statuses else PASS, "live_ready": False,
            "statement": "An offline pass shows the overlay selects the timed writer under a variant-qualified run id and matches the untouched A0m render. It is not an authorization "
                         "and shows nothing about a live run."}


def refuse_execute() -> int:
    print("REFUSED: the execute phase is not available in this tool. A rendered configuration is not permission to run it; a live run needs a separate, versioned execution "
          "tool and explicit owner authorization for each live step. No flag or environment variable enables it.")
    return EXIT_EXECUTE_REFUSED


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    r = sub.add_parser("render", help="render the timed variant (offline)")
    r.add_argument("--variant", required=True)
    r.add_argument("--params", required=True, type=Path)
    r.add_argument("--a0m-params", required=True, type=Path)
    r.add_argument("--channel", required=True, type=cs.experiment_channel_arg, metavar="CHANNEL")
    r.add_argument("--out-root", required=True, type=Path)
    r.add_argument("--render-id", required=True)
    p = sub.add_parser("preflight", help="overlay checks plus the A0m preflight on the render beside it")
    p.add_argument("--variant", required=True)
    p.add_argument("--run-dir", required=True, type=Path)
    p.add_argument("--candidate-manifest", type=Path)
    sub.add_parser("execute", help="always refused")
    args = parser.parse_args(argv)
    if args.phase == "execute":
        return refuse_execute()
    if args.phase == "render":
        try:
            out = render(args.params, args.a0m_params, args.out_root, args.render_id, variant=args.variant, channel=args.channel)
        except (tv.TimedVariantError, lv.LaunchVariantError, dr.A0mError, dr.A0mParameterError, cs.ChannelSelectionError, OSError, ValueError, KeyError) as exc:
            print(f"REFUSED: {type(exc).__name__}: {exc}")
            return 2
        print(f"timed variant render {out['root']}: {out['manifest']['status']}; run id {out['marker']['qualified_run_id']}; Caliper, Fabric, Docker and WSL were not touched")
        return 0
    result = preflight(args.run_dir, args.variant, candidate_manifest=args.candidate_manifest)
    print(json.dumps(result, indent=2))
    return 0 if result["offline_preflight"] == PASS else 1


if __name__ == "__main__":
    sys.exit(main())
