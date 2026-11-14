"""Upper-bound duration budget for a launch (Phase 2C): how long the keep-awake helper and the continuous clock probe must be allowed to live. OFFLINE, pure arithmetic.

What was wrong with Phase 2: `caliper_check.keep_awake_seconds(n, timeout) = n x (timeout + 900) + 1200` starts the clock at the first run. The helper, however, is started BEFORE
the pre-flight (it is gate S2's evidence), and the pre-flight alone can take over an hour: gate S1 runs `archive_evidence.py verify --check-sources` under a hard 3,600 s limit
(driver `snapshot.gate_s1_replay`, `be.run(..., 3600)`; the third snapshot's verify took about 18 minutes when it was last timed). A helper that expires mid-run ends the
standby protection silently; the fix is a budget that starts where the helper starts and ends where its cleanup ends.

Method: every component is the LARGEST time its own code allows, and the line that sets that limit is named in `basis`. Where a step has no coded limit of its own it gets the
P1b tool limit (`Thresholds.tool_timeout_s`, 120 s) per external call and the number of calls is counted. Nothing is an average and nothing was measured. The sum is a CEILING:
it is the lifetime given to the helper, which is stopped at the end of the launch anyway. If it is wrong it is wrong on the high side, which costs time awake and not data.
It says nothing about how long a launch is expected to take (the P4 draft assumed about 14 minutes per run; that is an assumption, not a bound).

Components, in the order a launch meets them:
  helper start wait; the pre-flight gates (one bound per gate, the sum is checked against the gate list by a test); creation of the series root and the manifest; the
  continuous clock-probe start wait; for each run: the per-run precheck, channel creation, staging, the Caliper run at its hard timeout plus its abort path, configuration
  fetches, sampler stop, metrics, the block fetch (limit scales with the channel height, bounded below), the analysis, the offline evaluation, the clock flush margin;
  the end of the launch: fingerprint of the reference channels, probe stop and clock check, reconciliation, summary, helper cleanup (identity query, kill, re-query); and the
  cleanup margin (P1b's 1,200 s, `caliper_check.SERIES_MARGIN_S`).

Block-height bound used for the block-fetch limit. A block is cut by count (at most tx/B of them), by byte overflow (never at 128-byte payloads against a 10 MiB limit) or by the
2 s timer (at most one per 2 s of the Caliper run, which is bounded by its hard timeout), plus a few configuration blocks: height <= ceil(tx / B) + ceil(timeout / tau) + 16.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from . import caliper_check as cc
from . import driver as dr

NODE_PARSE_TIMEOUT_S = 60.0           # caliper_check.node_parse_records: subprocess.run(..., timeout=60)
KEEPAWAKE_START_WAIT_S = 15.0         # keepawake.KeepAwake wait_s default
PROBE_START_WAIT_S = 3.0              # series.py: be.sleep(3.0) after spawning the probe
TAU_S = 2.0                           # BatchTimeout of every run (driver.TAU)
CONFIG_BLOCK_ALLOWANCE = 16
SYNC_FILE_OPS_S = 60.0                # offline work on local files (hashing, JSON): a generous ceiling, no coded limit exists
OFFLINE_EVAL_S = 120.0                # offline classification of one run (numpy over a few thousand rows)
POST_RUN_MISC_S = 60.0                # directory and JSON writes between steps
RECONCILIATION_S = 120.0              # offline reconciliation over the series' files
CLEANUP_IDENTITY_QUERY_S = 60.0       # keepawake_v2: query, kill (30), query again
CLEANUP_KILL_S = 30.0


@dataclass(frozen=True)
class Component:
    name: str
    seconds: float
    basis: str
    phase: str            # helper | preflight | run | end | margin


def gate_bounds(thr, n_runs: int) -> dict[str, tuple[float, str]]:
    """Upper bound per pre-flight gate with the code that sets it. Keys must equal launch_series.GATE_ORDER_V2 (a test enforces it)."""
    ctrl_http = 5.0
    return {
        "S3": (thr.mem_window_samples * thr.mem_window_interval_s + 60.0 + 30.0, "p1b Backend.host_memory timeout = samples x interval + 60 (60 s window + 60) plus a 30 s WSL read"),
        "S2": (60.0, "p1b Backend.host_power timeout 60"),
        "S4": (30.0, "one disk read, no coded limit: 30"),
        "S5": (30.0 + 5 * 2.0 + 30.0 + 30.0, "p1b gate_s5: docker inspect (30), five /healthz at 2 s, docker ps (30), baseline inspect (30)"),
        "S6": (3 * ctrl_http + 2 * 60.0, "p1b gate_s6: three Prometheus reads at 5 s, two `peer lifecycle queryinstalled` at 60"),
        "R1": (SYNC_FILE_OPS_S, "pins.verify_snapshot_identity: hashes two small files and reads the manifest"),
        "R2": (SYNC_FILE_OPS_S, "offline checks of the matrix, root, chaincode and budget"),
        "R3": (SYNC_FILE_OPS_S * 2 + n_runs * 2 * NODE_PARSE_TIMEOUT_S, "stage re-hash (two passes allowed) plus, per run, two `node` parses at caliper_check.node_parse_records timeout=60"),
        "S7": (2 * (30.0 + 30.0), "p1b fingerprint: two reference channels x (getinfo 30 + fetch config 30)"),
        "S8": (3 * 60.0, "p1b gate_s8: three `osnadmin channel list` at 60"),
        "S9": (600.0, "p1b gate_s9: reads the probe record; runs e1_clock_gate (limit 600) only if clock_gate.json is absent"),
        "R4": (3 * 30.0, "gate_r4: ps, docker top, docker ps at 30 each"),
        "S10": (SYNC_FILE_OPS_S * 2, "hashing the frozen files, the pinned source manifest and the tool list"),
        "S11": (10.0, "two path-existence checks"),
        "R5": (10.0, "path comparisons"),
        "R6": (30.0, "reads the dry-run summary and the pin"),
        "S1": (3600.0, "snapshot.gate_s1_replay: be.run(archive_evidence verify --check-sources, 3600)"),
    }


def per_run_components(thr, caliper_timeout_s: float, B: int, tx_total: int) -> list[tuple[str, float, str]]:
    height = math.ceil(tx_total / B) + math.ceil(caliper_timeout_s / TAU_S) + CONFIG_BLOCK_ALLOWANCE
    fetch_limit = max(thr.fetch_timeout_base_s, 120.0 + thr.fetch_timeout_per_block_s * height)
    return [
        ("precheck", 30 + 10 + 30 + 30 + (2 * 1 + 60) + 60 + 30, "series.replay_precheck: health, inspect (30), disk, WSL memory, host_memory(2,1) (62), host_power (60), ps/wslpath slack"),
        ("channel creation", thr.channel_timeout_s, "Thresholds.channel_timeout_s"),
        ("stage copy, plan, verify, metrics_before", 4 * thr.tool_timeout_s, "four run steps at Thresholds.tool_timeout_s"),
        ("sampler start wait", 3.0, "driver.execute_replay_run: be.sleep(3.0)"),
        ("getinfo + config before", 30.0 + 60.0, "p1b do_getinfo 30; do_fetch_config 30 + decode 30"),
        ("caliper run", caliper_timeout_s, "approved Caliper hard timeout (run_caliper aborts there)"),
        ("caliper abort path", 60.0 + 10.0, "p1b run_caliper: `docker kill` at 60, process wait 10"),
        ("getinfo + config after", 30.0 + 60.0, "as before"),
        ("stop samplers", 120.0, "p1b stop_background timeout_s default 120"),
        ("metrics_after + metrics_summary", 2 * thr.tool_timeout_s, "two run steps at Thresholds.tool_timeout_s"),
        (f"block fetch (height bound {height})", fetch_limit, "driver.execute_replay_run: max(fetch_timeout_base_s, 120 + 1 s x height); height <= ceil(tx/B) + ceil(timeout/tau) + 16"),
        ("analysis", thr.tool_timeout_s, "run step at Thresholds.tool_timeout_s"),
        ("offline evaluation", OFFLINE_EVAL_S, "classification over a few thousand rows; no coded limit: 120"),
        ("clock flush margin", thr.clock_flush_margin_s, "Thresholds.clock_flush_margin_s (probe flushes every 10 s)"),
        ("reference-channel fingerprint (SC10)", 2 * (30.0 + 30.0), "p1b fingerprint: two reference channels x (getinfo 30 + fetch config 30)"),
        ("per-run writes", POST_RUN_MISC_S, "directory and JSON writes: 60"),
    ]


@dataclass(frozen=True)
class DurationBudget:
    kind: str
    components: tuple
    n_runs: int

    @property
    def total_s(self) -> int:
        return int(math.ceil(sum(c.seconds for c in self.components)))

    @property
    def preflight_s(self) -> int:
        return int(math.ceil(sum(c.seconds for c in self.components if c.phase in ("helper", "preflight"))))

    @property
    def probe_s(self) -> int:
        """Lifetime for the continuous clock probe: everything after the pre-flight."""
        return self.total_s - self.preflight_s

    def table(self) -> list[dict]:
        return [{"phase": c.phase, "component": c.name, "seconds": c.seconds, "basis": c.basis} for c in self.components]


def build_budget(kind: str, runs: list[tuple[int, int]], *, caliper_timeout_s: float, gate_ids: tuple[str, ...], thr=None, margin_s: float | None = None) -> DurationBudget:
    """`runs` is [(B, total scheduled transactions of the run incl. pre-roll)] for every run the launch will execute. `gate_ids` is the launch's pre-flight gate list."""
    thr = thr or dr.p1b_module().THR
    margin_s = cc.SERIES_MARGIN_S if margin_s is None else margin_s
    if not runs:
        raise ValueError("a launch has at least one run")
    bounds = gate_bounds(thr, len(runs))
    missing = [g for g in gate_ids if g not in bounds]
    if missing:
        raise ValueError(f"no documented duration bound for gates {missing}")
    comps: list[Component] = [Component("keep-awake helper start wait", KEEPAWAKE_START_WAIT_S, "keepawake.KeepAwake wait_s default", "helper")]
    comps += [Component(f"gate {g}", bounds[g][0], bounds[g][1], "preflight") for g in gate_ids]
    comps += [Component("series root, manifest, probe start wait", SYNC_FILE_OPS_S + PROBE_START_WAIT_S, "create-exclusive writes; series.py be.sleep(3.0)", "preflight")]
    for i, (B, tx) in enumerate(runs, start=1):
        comps += [Component(f"run {i}: {name}", sec, basis, "run") for name, sec, basis in per_run_components(thr, caliper_timeout_s, B, tx)]
    comps += [
        Component("end: fingerprint of the two reference channels", 2 * (30.0 + 30.0), "p1b fingerprint", "end"),
        Component("end: probe flush, stop and clock check", thr.clock_flush_margin_s + 20.0 + SYNC_FILE_OPS_S, "p1b finish_clock_series: flush margin, probe.wait 20, read and check", "end"),
        Component("end: reconciliation and summary", RECONCILIATION_S + 30.0, "series.final_reconciliation (offline) and summary write", "end"),
        Component("end: helper cleanup (identity query, kill, re-query)", 2 * CLEANUP_IDENTITY_QUERY_S + CLEANUP_KILL_S, "keepawake_v2 cleanup", "end"),
        Component("cleanup margin", margin_s, "caliper_check.SERIES_MARGIN_S (P1b's margin)", "margin"),
    ]
    return DurationBudget(kind, tuple(comps), len(runs))
