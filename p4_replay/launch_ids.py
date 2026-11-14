"""Identifier namespaces for a dry-run launch and a measured-series launch (Phase 2C, Package A problem 1). OFFLINE: pure data and an offline render; nothing is started.

The conflict. Phase 1's matrix names every run `rp00`..`rp06` and every channel `e1-rpNN-bB-rR`, and Phase 2 runs the dry run and the six measured runs as ONE series. A DRY_RUN_ONLY
launch followed by the measured launch would meet its own leftovers: the Fabric channel `e1-rp00-b10-r0` (a channel cannot be deleted), the run folder in the Caliper workspace (inside
the `wsl_e1` snapshot group, so it is also S1 SOURCE_ADDED drift), and the series root. The orchestrator would either refuse (S8, S11, R5 all fail) or, worse, be tempted to reuse.

Resolution: two launches with disjoint, fixed namespaces, and no identifier is ever shared.

    DRY_RUN          one run     rp90   channel e1-rp90-b10-r0   window D_33000   series root  <root>/p4r-dry-<NN>
    MEASURED_SERIES  six runs    rp01..rp06   channels e1-rp0P-bB-rR (P = order position)   window F_82800   series root  <root>/p4r-meas-<NN>
    retired          rp00 (Phase 1's dry run id): refused in every launch, so a Phase 1 matrix can never be mixed in.

Both fit the Phase 1 channel pattern (`driver.CHANNEL_RE`) and the P1b/E1 naming rules, so every Phase 1 and P1b function that takes a row still accepts them. The measured launch
does not run a dry run: it requires the dry-run launch's recorded evidence (gate R6) instead. Identifiers are consumed by use: a halted launch leaves its channels and folders, the same
identifiers fail S8/S11/R5 on any later attempt, and the namespace has no spare identifier, so a failed measured run cannot be replaced or resumed (a deliberate property).

`stage_launch` renders a launch's runs offline into a NEW staging tree exactly as `driver.stage_series` does (same renderer, same files), minus Phase 1's whole-matrix validation,
which only accepts the single seven-run series.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from . import driver as dr
from . import offsets as of
from . import prereg as pr
from . import render as rd

DRY, MEASURED = "DRY_RUN", "MEASURED_SERIES"
KINDS = (DRY, MEASURED)
DRY_RUN_ID = "rp90"
MEASURED_RUN_IDS = tuple(f"rp{i:02d}" for i in range(1, 7))
RETIRED_RUN_IDS = ("rp00",)
LAUNCH_ID_RE = re.compile(r"^p4r-(dry|meas)-(\d\d)$")
_KIND_OF_PREFIX = {"dry": DRY, "meas": MEASURED}


class LaunchIdError(ValueError):
    pass


def check_launch_id(launch_id: str, kind: str) -> str:
    """The launch id is the series folder name. Its prefix must match the launch kind, so a measured launch cannot be pointed at a dry-run folder or the reverse."""
    m = LAUNCH_ID_RE.match(launch_id or "")
    if not m:
        raise LaunchIdError(f"launch_id {launch_id!r} must look like p4r-dry-01 or p4r-meas-01")
    if _KIND_OF_PREFIX[m.group(1)] != kind:
        raise LaunchIdError(f"launch_id {launch_id!r} belongs to a {_KIND_OF_PREFIX[m.group(1)]} launch, not {kind}")
    return launch_id


def dry_rows() -> list[dr.ReplayRow]:
    return [dr.ReplayRow(0, DRY_RUN_ID, f"e1-{DRY_RUN_ID}-b10-r0", "dry_run", dr.DRY_RUN_WINDOW, "A", 10, dr.TAU, dr.P_BYTES, 0)]


def measured_rows(run_order_seed: int, max_consecutive_same: int = 2) -> list[dr.ReplayRow]:
    """The six measured rows in the seeded order (prereg.run_order, option R2). seq = order position 1..6, run id rp0<position>."""
    rows = []
    for item in pr.run_order(run_order_seed, max_consecutive_same):
        rid = f"rp{item['position']:02d}"
        rows.append(dr.ReplayRow(item["position"], rid, f"e1-{rid}-b{item['B']}-r{item['replicate']}", "measured", dr.MAIN_WINDOW, "A", item["B"], dr.TAU, dr.P_BYTES, item["replicate"]))
    validate_launch_rows(MEASURED, rows)
    return rows


def rows_for(kind: str, run_order_seed: int | None) -> list[dr.ReplayRow]:
    if kind == DRY:
        return dry_rows()
    if kind == MEASURED:
        if run_order_seed is None:
            raise LaunchIdError("a measured launch needs the owner's run-order seed")
        return measured_rows(run_order_seed)
    raise LaunchIdError(f"unknown launch kind {kind!r}")


def validate_launch_rows(kind: str, rows: list[dr.ReplayRow]) -> None:
    """Exactly the namespace of the launch kind; nothing else is accepted. Raises LaunchIdError."""
    p1b = dr.p1b_module()
    if kind not in KINDS:
        raise LaunchIdError(f"unknown launch kind {kind!r}")
    ids = [r.run_id for r in rows]
    if any(i in RETIRED_RUN_IDS for i in ids):
        raise LaunchIdError(f"run id(s) {sorted(set(ids) & set(RETIRED_RUN_IDS))} are retired (Phase 1 dry run); a Phase 1 matrix cannot be mixed into a launch")
    want = (DRY_RUN_ID,) if kind == DRY else MEASURED_RUN_IDS
    if tuple(ids) != want:
        raise LaunchIdError(f"a {kind} launch has exactly the runs {list(want)} in this order, got {ids}")
    if len({r.channel for r in rows}) != len(rows):
        raise LaunchIdError("channels must be unique")
    for r in rows:
        if not dr.CHANNEL_RE.match(r.channel) or r.channel in p1b.EXISTING_CHANNELS or r.channel != f"e1-{r.run_id}-b{r.B}-r{r.replicate}":
            raise LaunchIdError(f"{r.run_id}: channel {r.channel!r} is not the fresh channel of this run")
        if r.B not in (10, 40) or r.tau != dr.TAU or r.P != dr.P_BYTES or r.schedule != "A":
            raise LaunchIdError(f"{r.run_id}: only B in (10, 40), tau 2s, P {dr.P_BYTES}, schedule A are in scope")
    if kind == DRY:
        r = rows[0]
        if (r.kind, r.window, r.B, r.replicate, r.seq) != ("dry_run", dr.DRY_RUN_WINDOW, 10, 0, 0):
            raise LaunchIdError("the dry run is B=10, replicate 0, window D_33000, kind dry_run")
        return
    if [r.seq for r in rows] != [1, 2, 3, 4, 5, 6] or any(r.kind != "measured" or r.window != dr.MAIN_WINDOW for r in rows):
        raise LaunchIdError("measured rows are seq 1..6, kind measured, window F_82800")
    for b in (10, 40):
        if sorted(r.replicate for r in rows if r.B == b) != [1, 2, 3]:
            raise LaunchIdError(f"B={b} needs replicates 1, 2, 3")


def namespaces_disjoint() -> bool:
    return not (set(MEASURED_RUN_IDS) & {DRY_RUN_ID}) and not (set(RETIRED_RUN_IDS) & (set(MEASURED_RUN_IDS) | {DRY_RUN_ID}))


# ----------------------------------------------------------------------------------------------- staging (offline)
def stage_launch(kind: str, rows: list[dr.ReplayRow], env: dict, network_path: str, processed_dir: Path, stage_root: Path, *, archive_manifest: Path | None = None,
                 argv: list[str] | None = None, network_doc: dict | None = None) -> dict:
    """Render a launch's runs into a NEW staging tree: runs/<id>/..., plans/<id>.plan.json, per-run SHA256SUMS, series_manifest.json, replay_matrix.csv, SHA256SUMS.txt.
    The same renderer and file layout as driver.stage_series. Starts nothing. Refuses an existing stage_root."""
    validate_launch_rows(kind, rows)
    stage_root = Path(stage_root)
    if stage_root.exists():
        raise FileExistsError(stage_root)
    (stage_root / "plans").mkdir(parents=True)
    (stage_root / "runs").mkdir()
    identifiers_by_day: dict[str, dict] = {}
    windows: dict[str, tuple] = {}
    series: dict = {"tool": f"p4_replay.launch_ids ({dr.TOOL_VERSION})", "launch_kind": kind, "argv": argv, "status": "STAGED_OFFLINE_NOT_EXECUTED", "runs": {}, "windows": {}}
    for row in rows:
        if row.window not in windows:
            windows[row.window] = of.load_registered_window(processed_dir, row.window)
        window, lineage = windows[row.window]
        identifiers, day_start_ms = None, None
        if archive_manifest is not None:
            import calendar
            from fabric_perf.replay.trace import load_manifest
            y, m_, d = (int(x) for x in window.day.split("-"))
            day_start_ms = calendar.timegm((y, m_, d, 0, 0, 0)) * 1000
            if window.day not in identifiers_by_day:
                entry = load_manifest(archive_manifest)[window.day]
                a, c = int(window.pre_src_index[0]), int(window.measured_src_index[-1]) + 1
                identifiers_by_day[window.day] = of.load_archive_identifiers(entry["source_path"], entry, a, c)
            identifiers = identifiers_by_day[window.day]
        result = rd.render_replay_run(env, network_path, row.run_id, row.channel, row.B, row.tau, row.P, window, stage_root / "plans" / f"{row.run_id}.plan.json",
                                      ws_root=stage_root, identifiers=identifiers, day_start_ms=day_start_ms, lineage=lineage, argv=argv, network_doc=network_doc)
        run_dir = stage_root / "runs" / row.run_id
        (stage_root / "runs" / f"{row.run_id}.SHA256SUMS").write_text(dr.sha256_text_lines(run_dir), encoding="ascii", newline="\n")
        series["runs"][row.run_id] = {"row": row.csv_row(), "render_record_sha256": of.sha256_hex((run_dir / "render_record.json").read_bytes()),
                                      "schedules": result["record"]["schedules"], "tx_total": result["record"]["schedules"]["pre_roll"]["n_tx"] + result["record"]["schedules"]["measured"]["n_tx"]}
        series["windows"][row.window] = lineage
    (stage_root / "replay_matrix.csv").write_bytes(dr.matrix_csv(rows))
    (stage_root / "series_manifest.json").write_text(json.dumps(series, indent=2), encoding="utf-8", newline="\n")
    (stage_root / "SHA256SUMS.txt").write_text(dr.sha256_text_lines(stage_root, {"SHA256SUMS.txt"}), encoding="ascii", newline="\n")
    return series


def stage_sums_sha256(stage_root: Path) -> str:
    return hashlib.sha256((Path(stage_root) / "SHA256SUMS.txt").read_bytes()).hexdigest()
