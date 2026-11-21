"""Select recorder v2 for ONE rendered Caliper run directory (task A0d, blocker B-wire). Offline: it only reads and writes files.

Why a wrapper. The renderers (``e1_la2_render.py``, ``p4_replay/render.py``) and the run scripts are hash-pinned and name
the v1 recorder ``e1_per_tx_recorder.js`` for the Caliper transaction monitor. v2 (``e1_per_tx_recorder_v2.js``) extends
v1 and needs v1 as a sibling file in the run's ``modules/`` directory. This tool does not change any pinned file. It
takes a run directory that a renderer (or the capture script's template copy) has ALREADY produced and writes a NEW run
directory that is identical except for:

  * ``modules/`` holds both v1 (hash-checked against the pinned value) and v2;
  * the one transaction monitor in ``benchmark.yaml`` names v2 instead of v1 (and, optionally, two v2 live-stream options).

Nothing selects v2 unless this tool is run on purpose. A run directory that was not passed through it keeps v1.

Fail closed. It refuses: a channel that is missing, malformed, protected or an earlier experiment's (the shared A0b
rules); a run whose network configuration does not name exactly that channel; a run in which any protected reference
channel name still appears; a benchmark that does not have exactly one transaction monitor naming v1 (including one that
already names v2); a v1 file that differs from the pinned hash; a target that already exists. It never starts Caliper,
Docker, WSL or Fabric: this module imports no process or network API (a test asserts that).

The result is meant to be inspected before any run: ``recorder_v2_wire_record.json`` lists the files, their SHA-256 and the
exact monitor before and after, and ``status`` is ``RENDERED_OFFLINE_NOT_EXECUTED``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import channel_selection as cs  # noqa: E402  (standard library only)

TOOL_VERSION = "e1_recorder_v2_wire 0.1 (2026-11-21)"
V1_NAME, V2_NAME = "e1_per_tx_recorder.js", "e1_per_tx_recorder_v2.js"
V1_SOURCE, V2_SOURCE = REPO / "scripts" / "e1" / "caliper" / V1_NAME, REPO / "scripts" / "e1" / "caliper" / V2_NAME
# The v1 recorder is hash-pinned in several protected-input manifests; this tool refuses any other v1.
PINNED_V1_SHA256 = "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8"
TEXT_SUFFIXES = {".json", ".yaml", ".yml", ".js"}
LIVE_OPTION_MIN = {"liveFlushRows": 1, "liveFlushIntervalMs": 10}      # the limits v2 itself enforces


class WireError(ValueError):
    """The run directory cannot be wired to v2 safely. Nothing is written."""


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise WireError(f"cannot read {path}: {exc}") from exc


def network_channels(network: dict) -> list[str]:
    return [str(item.get("channelName")) for item in (network.get("channels") or []) if isinstance(item, dict)]


def select_v2_monitor(benchmark: dict, *, run_id: str | None = None, live_options: dict | None = None) -> tuple[dict, dict]:
    """(new benchmark document, monitor-before/after summary). The input is not modified.

    The benchmark must carry exactly one transaction monitor, and it must name the v1 recorder. Anything else is refused,
    so a run is never half-wired and v2 is never selected by accident.
    """
    monitors = ((benchmark.get("monitors") or {}).get("transaction"))
    if not isinstance(monitors, list) or len(monitors) != 1 or not isinstance(monitors[0], dict):
        raise WireError("the benchmark must have exactly one transaction monitor")
    before = monitors[0]
    module = before.get("module")
    if not isinstance(module, str) or not module:
        raise WireError("the transaction monitor names no module")
    base = posixpath.basename(module)
    if base == V2_NAME:
        raise WireError("the transaction monitor already names recorder v2; refusing to wire a run twice")
    if base != V1_NAME:
        raise WireError(f"the transaction monitor names {base!r}, not the v1 recorder {V1_NAME!r}; refusing to guess")
    options = dict(before.get("options") or {})
    if run_id is not None and options.get("runId") != run_id:
        raise WireError(f"the monitor's runId {options.get('runId')!r} differs from the run {run_id!r}")
    extra = dict(live_options or {})
    unknown = sorted(set(extra) - set(LIVE_OPTION_MIN))
    if unknown:
        raise WireError(f"unknown live option(s) {unknown}; allowed: {sorted(LIVE_OPTION_MIN)}")
    for name, value in extra.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < LIVE_OPTION_MIN[name]:
            raise WireError(f"{name} must be an integer >= {LIVE_OPTION_MIN[name]}, got {value!r}")
    after = {**before, "module": posixpath.join(posixpath.dirname(module), V2_NAME), "options": {**options, **extra}}
    new = json.loads(json.dumps(benchmark))
    new["monitors"]["transaction"] = [after]
    return new, {"before": before, "after": after}


def _scan_protected(directory: Path) -> None:
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or path.suffix not in TEXT_SUFFIXES:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for name in sorted(cs.PROTECTED_REFERENCE_CHANNELS):
            if name in text:
                raise cs.ChannelSelectionError(
                    cs.PROTECTED_REMAINS, f"{path} names the protected reference channel {name!r}. Nothing was written.", value=name)


def wire_run(source_run_dir: Path, target_runs_root: Path, *, channel: str, live_options: dict | None = None,
             argv: list[str] | None = None) -> dict:
    """Write ``<target_runs_root>/<run_id>`` = the source run with recorder v2 selected. Returns the record."""
    channel = cs.select_experiment_channel(channel, purpose="a recorder-v2 run", option="--channel")
    source = Path(source_run_dir)
    if not (source / "benchmark.yaml").is_file() or not (source / "networkconfig.yaml").is_file():
        raise WireError(f"{source} is not a rendered run directory (benchmark.yaml and networkconfig.yaml are required)")
    benchmark = _load_json(source / "benchmark.yaml")
    network = _load_json(source / "networkconfig.yaml")
    monitors = (benchmark.get("monitors") or {}).get("transaction")
    if not isinstance(monitors, list) or len(monitors) != 1 or not isinstance(monitors[0], dict):
        raise WireError("the benchmark must have exactly one transaction monitor")
    run_id = (monitors[0].get("options") or {}).get("runId")
    if not isinstance(run_id, str) or not run_id:
        raise WireError("the transaction monitor carries no runId")
    names = network_channels(network)
    if names != [channel]:
        raise cs.ChannelSelectionError(
            cs.MISMATCH, f"the network configuration of {source} names channel(s) {names} but {channel!r} was selected; "
                         f"both must name exactly the same dedicated validation channel. Nothing was written.", value=names)
    _scan_protected(source)
    new_benchmark, monitor_summary = select_v2_monitor(benchmark, run_id=run_id, live_options=live_options)
    if sha256_file(V1_SOURCE) != PINNED_V1_SHA256:
        raise WireError(f"{V1_SOURCE} no longer matches the pinned v1 hash {PINNED_V1_SHA256}; refusing")
    shipped_v1 = source / "modules" / V1_NAME
    if shipped_v1.is_file() and sha256_file(shipped_v1) != PINNED_V1_SHA256:
        raise WireError(f"{shipped_v1} differs from the pinned v1 recorder; refusing")
    target = Path(target_runs_root) / run_id
    if target.exists():
        raise WireError(f"refusing to overwrite {target}")
    shutil.copytree(source, target)
    modules = target / "modules"
    modules.mkdir(exist_ok=True)
    for name, origin in ((V1_NAME, V1_SOURCE), (V2_NAME, V2_SOURCE)):
        shutil.copyfile(origin, modules / name)
        if sha256_file(modules / name) != sha256_file(origin):
            raise WireError(f"the copy of {name} differs from the repository file")
    (target / "benchmark.yaml").write_text(json.dumps(new_benchmark, indent=2) + "\n", encoding="utf-8")
    record = {
        "tool": TOOL_VERSION, "status": "RENDERED_OFFLINE_NOT_EXECUTED", "caliper_invoked": False, "argv": argv,
        "run_id": run_id, "channel": channel,
        "channel_selection": cs.selection_record(channel, role=cs.ROLE_EXPERIMENT, option="--channel"),
        "source_run_dir": str(source), "target_run_dir": str(target),
        "monitor": monitor_summary,
        "files": {"benchmark.yaml": {"source_sha256": sha256_file(source / "benchmark.yaml"),
                                     "target_sha256": sha256_file(target / "benchmark.yaml")},
                  f"modules/{V1_NAME}": {"sha256": sha256_file(modules / V1_NAME), "pinned_sha256": PINNED_V1_SHA256},
                  f"modules/{V2_NAME}": {"sha256": sha256_file(modules / V2_NAME)}},
        "live_stream_files_expected": [f"<outputDir>/<runId>_w<worker>_r<round>_<label>{suffix}"
                                       for suffix in (".live.csv", ".live.submits.csv", ".live.open.json", ".live.final.json")],
        "notes": ["v2 writes its extra live stream beside the v1 outputs; v1's own final files are unchanged.",
                  "Read one source per round (the live stream or the final CSV), never both.",
                  "LIVE_UNTESTED: v2 was never run inside a real Caliper worker."],
    }
    (target / "recorder_v2_wire_record.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    verify_wired(target, channel=channel)
    return record


def verify_wired(target: Path, *, channel: str) -> dict:
    """Re-read what was written and prove v2 is what the run will load. Raises ``WireError`` otherwise."""
    target = Path(target)
    benchmark = _load_json(target / "benchmark.yaml")
    monitors = benchmark["monitors"]["transaction"]
    if len(monitors) != 1 or posixpath.basename(monitors[0]["module"]) != V2_NAME:
        raise WireError("the written benchmark does not select recorder v2")
    for name in (V1_NAME, V2_NAME):
        if not (target / "modules" / name).is_file():
            raise WireError(f"modules/{name} is missing from the wired run")
    if sha256_file(target / "modules" / V1_NAME) != PINNED_V1_SHA256:
        raise WireError("the wired run's v1 recorder is not the pinned file")
    if network_channels(_load_json(target / "networkconfig.yaml")) != [channel]:
        raise WireError("the wired run's network configuration does not name exactly the selected channel")
    _scan_protected(target)
    return {"selected_module": monitors[0]["module"], "options": monitors[0].get("options", {})}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    ap.add_argument("--source-run-dir", required=True, type=Path, help="a run directory already produced by a renderer")
    ap.add_argument("--target-runs-root", required=True, type=Path, help="the new run is written to <root>/<runId> (must not exist)")
    ap.add_argument("--channel", required=True, type=cs.experiment_channel_arg, metavar="CHANNEL",
                    help="the dedicated validation channel; must be the channel the run's network configuration names")
    ap.add_argument("--live-flush-rows", type=int, help="optional v2 option (default: v2's own 50)")
    ap.add_argument("--live-flush-interval-ms", type=int, help="optional v2 option (default: v2's own 500)")
    args = ap.parse_args(argv)
    live = {name: value for name, value in (("liveFlushRows", args.live_flush_rows),
                                            ("liveFlushIntervalMs", args.live_flush_interval_ms)) if value is not None}
    try:
        record = wire_run(args.source_run_dir, args.target_runs_root, channel=args.channel, live_options=live,
                          argv=sys.argv if argv is None else argv)
    except (WireError, cs.ChannelSelectionError, OSError) as err:
        print(f"REFUSED: {err}")
        return 2
    print(f"wired run -> {record['target_run_dir']}; monitor module -> {record['monitor']['after']['module']}; "
          f"{record['status']} (Caliper was not invoked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
