"""P1b TIER 1 capacity-characterization driver: narrow, owner-gated, fresh throwaway channels only. PROPOSED; nothing here is authorised to run.

Derived from the frozen P1a driver e1_quicklook.py by explicit, listed replacements (see the unified diff in the plan folder); every function
that is not listed in tests/e1/test_e1_quicklook_p1b.py (CHANGED_DEFINITIONS / ADDED_DEFINITIONS) is byte-identical to the frozen one. What changed:
  * matrix: the P1b TIER 1 matrix (10 rows, the ten rungs the frozen P1a ramps skipped: B = 10 and 40 at 15, 20, 25, 30, 40 tx/s), SHA-256 pinned in
    MATRIX_SHA256; Tier 2 (60-200 tx/s) is not in this driver: no matrix hash, rate, approval cap or blocker-free path leads to a rate above 40;
  * classification: the corrected CLIENT_LIMITED / SATURATED rule of erratum E1-QL-ERR-001 (e1_client_classification_v2); the frozen rule is
    still computed and recorded as labels_frozen_rule; a ramp climbs only from a rung that is VALID and HEALTHY;
  * ramp chains: each ramp continues from its P1a rung-1 run through the erratum's pinned derived table; chains are per setting;
  * approval: the approval file must also name the erratum table hash, `tier=1`, `watchdog_mode=bounds_only_tier1` and a max_rate_tps cap (<= 40);
    the erratum document's section 11 must be signed (the owner's adoption is an interlock, not an assumption);
  * B2 (owner decision 2026-11-13): no live backlog watchdog for the bounded Tier 1 stage, on six stated conditions (B2_BOUNDS_CONDITIONS), each pinned by a
    test; a live watchdog / stronger overload monitoring stays an open blocker for any Tier 2 (TIER2_BLOCKERS);
  * B3: a host sampler (e1_host_sampler.py: WSL /proc + Windows typeperf, 1 Hz) runs beside the other two samplers in every run;
  * B4: Caliper gets --caliper-report-path runs/<run id>/report.html, so no launch overwrites another run's report; the default report must stay untouched;
  * gates G-postrun (backlog, transaction validity and failure rate are mandatory evidence), G-evidence (plan 7.8 (E): >= 95 % of the expected 1 Hz
    samples in the measured window, no hole above 5 s, for every sampler stream) and G-report (the per-run report exists and the default one is unchanged);
  * S1: the reference snapshot is the second (post-P1a) archive snapshot.

It never activates a controller, never writes to an existing channel (two read-only fingerprints only), never overwrites a file, and never
retries or resumes a run. Subcommands:

  dry-run    print every command, safety check, stop rule and expected output for all rows; executes nothing
             (--probe-tools additionally runs each tool's own no-side-effect dry-run to prove its arguments parse)
  preflight  evaluate the 11 session gates S1-S11 (read-only) and write preflight_report.json
  run        execute the series; needs --execute, an owner approval file, a signed erratum, and BLOCKERS_OPEN empty
             (exit 0 clean, 3 pre-flight failed, 4 halted or an existing-channel fingerprint changed,
             5 completed but the continuous clock record needs review)
  verdict    re-evaluate one finished run directory offline (no Fabric)
  fingerprint  read-only height and config-block hash of the two reference channels (S7 / end-of-series compare)

Runs inside Ubuntu WSL (the Fabric CLIs use POSIX paths). Standard library plus the sibling E1 tools.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import e1_channel_run as cr                      # noqa: E402
import e1_clock_rate_probe as crp                # noqa: E402
import e1_config_actuator as ca                  # noqa: E402
import e1_la2_render as render                   # noqa: E402
import e1_run_orchestrator as ro                 # noqa: E402
import e1_telemetry_common as tc                 # noqa: E402
import e1_client_classification_v2 as ccv2       # noqa: E402
import e1_host_sampler as hs                     # noqa: E402

TOOL_VERSION = "e1_quicklook_p1b 0.2 (2026-11-13, Tier 1)"

# ---------------------------------------------------------------------------------------------- frozen design
MATRIX_SHA256 = "68b63aa6869145c6d4e3c1b0130b64b8c0c75e0fc2e167aca8d03b62babf8c8d"                 # the Tier 1 matrix: 10 rows, 15-40 tx/s (a prefix of the 26-row design)
ERRATUM_CSV_SHA256 = "3de24fb41a79f0525a6af51e3459ee0bdc6015bbddc35ac74884c83d558a2f9b"          # the derived table of erratum E1-QL-ERR-001 (anchors of the two ramps)
MATRIX_HEADER = "seq,run_id,channel,kind,B,tau,P,rate_tps,replicate,warmup_s,measured_s,ramp_precondition"
ROW_COUNT = 10
ALLOWED_B = (10, 40)
ANCHOR_RUNS = {10: "ql23", 40: "ql17"}                  # the P1a rung-1 runs the two ramps continue from (rate 10)
ALLOWED_RATES = (15, 20, 25, 30, 40)
TIER1_MAX_RATE_TPS = 40                                  # the hard bound of this driver: a rate above it needs stronger overload monitoring first (TIER2_BLOCKERS)
WATCHDOG_MODE = "bounds_only_tier1"                      # the owner's conditional acceptance of 2026-11-13; the approval file must name it
TAU, PREFERRED_MAX_BYTES, WARMUP_S, MEASURED_S = "2s", 10485760, 30, 120
# The ten channel records found in the archived evidence: never touched, never reused as names.
EXISTING_CHANNELS = frozenset({"e1-b001", "e1-b002", "e1-b003", "e1-cfg001", "e1-e00a", "e1-e00a2", "e1-e00b",
                               "e1-e00s", "e1-r001", "e1-r002"})
FINGERPRINT_CHANNELS = ("e1-r002", "e1-cfg001")          # read-only reference channels (S7)
CALIPER = "caliper-benchmark-001"
ORDERERS = ("orderer.example.com", "orderer2.example.com", "orderer3.example.com")
PEERS = ("peer0.org1.example.com", "peer0.org2.example.com")
OPS_PORT = {"orderer.example.com": 9443, "orderer2.example.com": 9446, "orderer3.example.com": 9447,
            "peer0.org1.example.com": 9444, "peer0.org2.example.com": 9445}
PRECONDITION_RE = re.compile(r"^run only if the ramp run at B=(\d+), rate=(\d+) was HEALTHY$")
ANCHOR_RE = re.compile(r"^run only if the P1a run (ql\d\d) \(B=(\d+), rate=(\d+)\) is HEALTHY in the erratum E1-QL-ERR-001 derived table$")
CHANNEL_RE = re.compile(r"^e1-cp(\d\d)-b(\d+)-l(\d+)$")
# Source drift the S1 gate tolerates against the second (post-P1a) snapshot: the owner-approved P1b files created after it was taken.
S1_ALLOWED_DRIFT = frozenset({
    "repo_scripts/e1/e1_client_classification_v2.py", "repo_scripts/e1/e1_p1b_matrix.py", "repo_scripts/e1/e1_quicklook_p1b.py",
    "repo_scripts/e1/e1_host_sampler.py",
    "repo_tests/e1/test_e1_client_classification_v2.py", "repo_tests/e1/test_e1_p1b_matrix.py", "repo_tests/e1/test_e1_quicklook_p1b.py",
    "repo_tests/e1/test_e1_host_sampler.py"})
# Instrumentation the P1b plan requires and that does not exist yet. `run` refuses to start while this is not empty; clearing an entry is a
# reviewed code change that comes with its own tests, never a flag. B3 (host sampler) and B4 (per-run Caliper report path) were implemented and
# tested in v0.2 (owner approval of 2026-11-13). B2 (live backlog watchdog) was accepted by the owner for the bounded Tier 1 stage only, on the
# six conditions below; it stays open for anything above Tier 1.
BLOCKERS_OPEN: tuple = ()
TIER2_BLOCKERS = (
    "B2 live backlog watchdog / stronger overload monitoring: owner decision 2026-11-13, 'Tier 2 remains unauthorized without stronger overload monitoring'",
    "Tier 2 itself: owner decision 2026-11-13, 'conditional on Tier 1 results'; it needs its own matrix, its own approval and a new driver version",
)
# The owner's six conditions for running Tier 1 without a live backlog watchdog (decision of 2026-11-13), each with what enforces it in this code.
# tests/e1/test_e1_quicklook_p1b.py pins every one of them; none is a flag.
B2_BOUNDS_CONDITIONS = (
    ("offered load and run duration strictly bounded",
     "matrix pinned by hash (10 rows, rates 15-40 tx/s, 30 s warm-up + 120 s measured); approval cap max_rate_tps <= 40 (check_approval); ramp_blocked skips any row above "
     "the cap; Caliper hard timeout 300 s (run_caliper)"),
    ("five-second infrastructure health polling remains active",
     "THR.health_poll_s == 5.0 and run_caliper polls the five /healthz endpoints and `docker inspect` every 5 s (run_caliper byte-pinned apart from the report path)"),
    ("existing timeout, container restart, exit and OOM aborts remain active",
     "run_caliper aborts on the 300 s timeout, 2 consecutive health failures, or any restart / replacement / exit / OOM (container_events), then `docker kill caliper-benchmark-001`"),
    ("post-run backlog, transaction validity and ledger reconciliation are mandatory",
     "gates G-postrun (backlog, interior VALID rate, failure rate, ledger validation codes must exist), G-reconcile (VALID) and G-ledger; a missing one is NOT_EVALUATED = INVALID_DATA"),
    ("any infrastructure fault halts the entire series",
     "HALT_VERDICTS = ABORTED_INFRA, INVALID_INFRA, DRIVER_ERROR end the series (run_series); a failed pre-run check, two consecutive INVALID runs and a first run that is not VALID also halt"),
    ("Tier 2 remains unauthorized without stronger overload monitoring",
     "no rate above 40 tx/s exists in the pinned matrix or ALLOWED_RATES, check_approval refuses a cap above TIER1_MAX_RATE_TPS, and TIER2_BLOCKERS lists what is missing"),
)


@dataclass(frozen=True)
class EvidenceThresholds:
    """Plan section 7.8 (E), made operational. Back-tested on the 18 archived P1a runs before adoption: resource sampler >= 98.4 % per container, Prometheus
    scrapes >= 100 % per target, longest hole 1.05 s, so these numbers would have failed none of them."""
    min_ratio: float = 0.95       # samples inside the measured window / expected 1 Hz samples, for every stream
    max_gap_s: float = 5.0        # longest hole inside the window, edges included
    host_max_seconds: int = 420   # guard handed to the host sampler: it can never outlive its run (Caliper hard timeout 300 s plus start-up and tear-down)


EVID = EvidenceThresholds()


@dataclass(frozen=True)
class Thresholds:
    """Every number a gate or label uses, in one place so that the dry-run can print it for review."""
    win_avail_mb_start: float = 1500.0       # owner: do not waive
    win_commit_pct_start: float = 85.0       # owner: do not waive
    win_pages_median_start: float = 100.0    # "Pages/sec <= 100 sustained", operationalised as the median of the window
    wsl_avail_mb_start: float = 3000.0
    win_avail_mb_run: float = 1000.0         # per-run floor (protocol V6: >= 1 GB headroom)
    wsl_avail_mb_run: float = 1000.0
    disk_free_gb_start: float = 15.0
    disk_free_gb_hard: float = 10.0
    mem_window_samples: int = 12             # 12 x 5 s = the 60-s idle window
    mem_window_interval_s: int = 5
    health_poll_s: float = 5.0
    health_consecutive_fail: int = 2
    caliper_timeout_s: float = 300.0
    channel_timeout_s: float = 300.0
    tool_timeout_s: float = 120.0
    fetch_timeout_base_s: float = 300.0
    fetch_timeout_per_block_s: float = 1.0
    pacing_ratio_tol: float = 0.02           # protocol V2
    lateness_p99_ms: float = 5.0             # protocol V2
    event_loop_p99_ms: float = 20.0          # protocol V2
    clock_wall_minus_mono_ms: float = 2.0    # protocol V7
    clock_step_ms: float = 1.0               # A2 gate (a)
    clock_flush_margin_s: float = 12.0       # the pinned probe flushes clock_samples.csv every 200 samples = every 10 s
    clock_gap_max_ms: float = 2000.0         # a longer sampling hole inside a run window = the probe did not watch that window
    failure_rate_max: float = 0.01
    sat_valid_ratio: float = 0.95
    sat_backlog: int = 200
    interior_trim_s: float = 10.0            # interior window = measured round minus the first and last 10 s
    client_event_loop_p99_ms: float = 10.0   # CLIENT_LIMITED label (review M1)
    client_cpu_pct: float = 80.0
    clock_probe_max_age_h: float = 24.0
    consecutive_invalid_stop: int = 2


THR = Thresholds()
INFRA_GATES = ("G-channel", "G-config-stable", "G-caliper", "V6-leader", "V6-containers", "V6-host", "V7-clock")
DATA_GATES = ("G-recorder", "G-reconcile", "G-telemetry", "G-ledger", "G-failures", "G-postrun", "G-evidence", "G-report")
HALT_VERDICTS = ("ABORTED_INFRA", "INVALID_INFRA", "DRIVER_ERROR")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def sha256_file(path: Path) -> str | None:
    p = Path(path)
    if not p.is_file():
        return None
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path: Path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def write_json_new(path: Path, obj) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    return tc.write_json_x(path, obj)


# ---------------------------------------------------------------------------------------------- matrix
@dataclass(frozen=True)
class Row:
    seq: int
    run_id: str
    channel: str
    kind: str
    B: int
    tau: str
    P: int
    rate: int
    replicate: int
    warmup_s: int
    measured_s: int
    precondition: str

    @property
    def cell(self) -> tuple[int, int]:
        return (self.B, self.rate)

    def prev_cell(self) -> tuple[int, int] | None:
        m = PRECONDITION_RE.match(self.precondition) if self.precondition else None
        return (int(m.group(1)), int(m.group(2))) if m else None

    def anchor(self) -> tuple[str, int, int] | None:
        m = ANCHOR_RE.match(self.precondition) if self.precondition else None
        return (m.group(1), int(m.group(2)), int(m.group(3))) if m else None


class MatrixError(ValueError):
    pass


def load_matrix(path: Path, expected_sha: str = MATRIX_SHA256, *, require_count: int | None = ROW_COUNT) -> list[Row]:
    """Load the frozen matrix. The file hash must equal the pinned one; every row is then re-validated."""
    path = Path(path)
    actual = sha256_file(path)
    if actual is None:
        raise MatrixError(f"matrix {path} not found")
    if actual != expected_sha:
        raise MatrixError(f"matrix hash {actual} != frozen {expected_sha}")
    text = path.read_bytes().decode("utf-8")
    if "\r" in text:
        raise MatrixError("matrix contains CR bytes")
    reader = csv.DictReader(text.splitlines())
    if ",".join(reader.fieldnames or []) != MATRIX_HEADER:
        raise MatrixError(f"matrix header {reader.fieldnames} != {MATRIX_HEADER}")
    rows: list[Row] = []
    for d in reader:
        try:
            r = Row(int(d["seq"]), d["run_id"], d["channel"], d["kind"], int(d["B"]), d["tau"], int(d["P"]),
                    int(d["rate_tps"]), int(d["replicate"]), int(d["warmup_s"]), int(d["measured_s"]),
                    d["ramp_precondition"])
        except (KeyError, ValueError) as exc:
            raise MatrixError(f"malformed row {d}: {exc}") from exc
        rows.append(r)
    if require_count is not None and len(rows) != require_count:
        raise MatrixError(f"{len(rows)} rows, expected {require_count}")
    validate_rows(rows)
    return rows


def validate_rows(rows: list[Row]) -> None:
    seen_channels, seen_ids = set(), set()
    last_rate: dict[int, int] = {}
    for i, r in enumerate(rows, start=1):
        if r.seq != i or r.run_id != f"cp{i:02d}":
            raise MatrixError(f"row {i}: seq/run_id out of order ({r.seq}, {r.run_id})")
        if r.kind != "ramp":
            raise MatrixError(f"{r.run_id}: kind {r.kind!r} (the P1b matrix has ramp rows only)")
        if r.B not in ALLOWED_B or r.rate not in ALLOWED_RATES:
            raise MatrixError(f"{r.run_id}: B={r.B} or rate={r.rate} outside the frozen grid")
        if (r.tau, r.P, r.warmup_s, r.measured_s) != (TAU, PREFERRED_MAX_BYTES, WARMUP_S, MEASURED_S):
            raise MatrixError(f"{r.run_id}: tau/P/warm-up/measured differ from the frozen common parameters")
        m = CHANNEL_RE.match(r.channel)
        if not m or int(m.group(1)) != r.seq or int(m.group(2)) != r.B or int(m.group(3)) != r.rate:
            raise MatrixError(f"{r.run_id}: channel {r.channel!r} does not encode seq/B/rate")
        if not cr.CHANNEL_RE.match(r.channel) or not re.match(r"^e1-[A-Za-z0-9_.-]{1,40}$", r.channel):
            raise MatrixError(f"{r.run_id}: channel name {r.channel!r} violates the Fabric or E1 naming rule")
        if r.channel in EXISTING_CHANNELS:
            raise MatrixError(f"{r.run_id}: channel {r.channel!r} is an existing channel")
        if r.channel in seen_channels or r.run_id in seen_ids:
            raise MatrixError(f"{r.run_id}: duplicate channel or run id")
        seen_channels.add(r.channel)
        seen_ids.add(r.run_id)
        if r.B not in last_rate:                                   # first rung of this setting: it continues from its P1a anchor run
            a = r.anchor()
            if a is None or a != (ANCHOR_RUNS[r.B], r.B, 10):
                raise MatrixError(f"{r.run_id}: the first rung of B={r.B} must continue from P1a run {ANCHOR_RUNS[r.B]} (B={r.B}, rate=10); got {r.precondition!r}")
        else:                                                      # later rung: the previous rung of the SAME setting
            if r.prev_cell() != (r.B, last_rate[r.B]) or r.rate <= last_rate[r.B]:
                raise MatrixError(f"{r.run_id}: ramp precondition {r.precondition!r} does not refer to the previous rung of B={r.B}")
        last_rate[r.B] = r.rate


def design_findings(rows: list[Row]) -> list[str]:
    """Observations about the frozen design against the repo's experimental-design rules (reported, never silently fixed)."""
    out = []
    sweep = [r for r in rows if r.kind == "sweep"]
    for a, b in zip(sweep, sweep[1:]):
        if a.cell == b.cell:
            out.append(f"consecutive repeat of cell B={a.B}, rate={a.rate}: {a.run_id} then {b.run_id} "
                       f"(experimental-design skill: 'no consecutive repeats'; the matrix is frozen, so this is reported, "
                       f"not changed; an owner decision is needed)")
    return out


def static_findings() -> list[str]:
    """Properties of the plan and of this driver that the owner should see before approving (reported, not hidden)."""
    return [
        "Failures gate: the plan defines SATURATED to include 'failure rate > 1%' and exempts SATURATED runs from the Failures gate, so as "
        "written the gate could never fail. The driver exempts a run only when overload evidence exists (interior VALID/offered < 0.95 or "
        "in-flight backlog > 200); a failure rate above 1% with sustained throughput is INVALID_DATA (a plant fault, not saturation). The "
        "SATURATED label itself follows the plan unchanged. Veto if you disagree.",
        f"Caliper container: the rendered plan names the stopped E0-0 container 'caliper'; the driver substitutes {CALIPER} (as "
        "e1_existing_channel_pilot.py does), and monitors that container.",
        "No resume or replacement mode: a halted series stays halted (the output root must not exist). A replacement run needs your approval "
        "and a small driver change.",
        "Classification: CLIENT_LIMITED and SATURATED follow the corrected rule of erratum E1-QL-ERR-001 (pacing lateness, A/lambda, Caliper CPU, "
        "failures, backlog, delivery; no event-loop probe). The frozen P1a rule is still computed and stored as labels_frozen_rule for comparison.",
        "Single replicate per rung: a rung can show a candidate capacity or ordering flip but cannot confirm one (Gate H needs the sign reproduced "
        "in every replicate); confirmation replicates are a separate stage with its own authorisation.",
        "B2 (live backlog watchdog) is NOT implemented. The owner accepted bounds-only operation for this Tier 1 stage only (decision of 2026-11-13), on six conditions that are "
        "listed in section E and pinned by tests; every rate above 40 tx/s stays blocked until stronger overload monitoring exists: " + "; ".join(TIER2_BLOCKERS) + ".",
        "B3 (host sampler) and B4 (per-run Caliper report path) are implemented in this version. `run` is still refused while BLOCKERS_OPEN is not empty "
        f"(now {len(BLOCKERS_OPEN)} open). The Caliper flag name was confirmed against the v0.6.0 source and real P1a output, not against the container's own CLI "
        "(no Docker was used); if it were ignored, G-report fails on the first run and the series halts after one run.",
        "Erratum interlock: `run` also refuses unless section 11 of the erratum document is signed with the pinned derived-table hash (the owner's adoption).",
    ]


# ---------------------------------------------------------------------------------------------- context and paths
@dataclass(frozen=True)
class Ctx:
    repo: Path
    e1_home: Path
    root: Path
    net: Path
    env: Path
    caliper_ws: Path
    py: str
    win_repo: str
    win_snapshot: str
    win_python: str
    ref_python: str
    matrix: Path
    fabric_bin: str
    erratum_csv: Path = Path("/mnt/d/PLAN_BLOCK18/p1a_erratum_2026-11-13/derived/p1a_corrected_classification.csv")
    erratum_doc: Path = Path("/mnt/d/PLAN_BLOCK18/p1a_erratum_2026-11-13/ERRATUM_E1-QL-ERR-001_CLIENT_LIMITED_2026-11-13.md")

    @property
    def scripts(self) -> Path:
        return self.repo / "scripts" / "e1"

    def tool(self, name: str) -> str:
        return str(self.scripts / name)

    def rd(self, run_id: str) -> Path:
        return self.root / run_id

    def recorder_dir(self, run_id: str) -> Path:
        return self.caliper_ws / "runs" / run_id / "recorder"

    @staticmethod
    def default() -> "Ctx":
        repo = Path("/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-")
        e1 = Path("/home/mailf18/e1")
        win_repo = r"C:\OJT_SEM3\AI-Assisted-Permissioned-Blockchain-Architecture-"
        return Ctx(repo=repo, e1_home=e1, root=e1 / "p1b-tier1-20261113",
                   net=repo / "fabric-testbed/network/network-e1-recovery.json",
                   env=repo / "fabric-testbed/network/env-e1-caliper-benchmark.json",
                   caliper_ws=e1 / "caliper-ws-benchmark", py=sys.executable or "python3",
                   win_repo=win_repo, win_snapshot=r"D:\PLAN_BLOCK18\evidence_archive\ARCHIVE_20261112T213119Z",
                   # argv[0] of a Windows program started from WSL must be a path Linux can exec (/mnt/c/...); only the
                   # arguments the Windows program itself reads (win_repo, win_snapshot) use Windows-style paths.
                   win_python="/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/.venv/Scripts/python.exe",
                   ref_python="/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/.venv/Scripts/python.exe",
                   matrix=Path("/mnt/d/PLAN_BLOCK18/p1b_tier1_prep_2026-11-13/run_matrix_p1b_tier1.csv"),
                   fabric_bin="/home/mailf18/fabric/fabric-samples/bin")


def tool_env(ctx: Ctx, base: dict | None = None) -> dict:
    env = dict(os.environ if base is None else base)
    env["PATH"] = ctx.fabric_bin + ":" + env.get("PATH", "")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def load_network(ctx: Ctx) -> "cr.Network":
    return cr.Network(json.loads(ctx.net.read_text(encoding="utf-8")))


def peer_env(ctx: Ctx, net: "cr.Network") -> dict:
    return tool_env(ctx, {**os.environ, **net.peer_env(net.orgs()[0])})


def with_wslenv(env: dict, **extra: str) -> dict:
    """Pass plain-string variables from WSL to a Windows process (WSLENV lists the names to forward)."""
    env = dict(env)
    env.update(extra)
    names = [n for n in env.get("WSLENV", "").split(":") if n] + [k for k in extra if k not in env.get("WSLENV", "").split(":")]
    env["WSLENV"] = ":".join(names)
    return env


# ---------------------------------------------------------------------------------------------- command builders
def metrics_targets() -> list[str]:
    return ["orderer1:orderer=http://localhost:9443/metrics", "orderer2:orderer=http://localhost:9446/metrics",
            "orderer3:orderer=http://localhost:9447/metrics", "peer0org1:peer=http://localhost:9444/metrics",
            "peer0org2:peer=http://localhost:9445/metrics"]


def watch_containers(chaincode: list[str] | None = None) -> list[str]:
    return list(ORDERERS) + list(PEERS) + [CALIPER] + list(chaincode or [])


def target_args() -> list[str]:
    return [a for t in metrics_targets() for a in ("--target", t)]


def argv_channel(row: Row, ctx: Ctx) -> list[str]:
    return [ctx.py, "-B", ctx.tool("e1_channel_run.py"), "--network", str(ctx.net), "--channel", row.channel,
            "--run-id", row.run_id, "--max-message-count", str(row.B), "--batch-timeout", row.tau,
            "--preferred-max-bytes", str(row.P), "--run-dir", str(ctx.rd(row.run_id) / "channel"), "--execute"]


def argv_render(row: Row, ctx: Ctx) -> list[str]:
    return [ctx.py, "-B", ctx.tool("e1_la2_render.py"), "run", "--env", str(ctx.env), "--network", str(ctx.net),
            "--run-id", row.run_id, "--channel", row.channel, "--B", str(row.B), "--tau", row.tau, "--P", str(row.P),
            "--duration-s", str(row.measured_s), "--rate", str(row.rate), "--warmup-s", str(row.warmup_s),
            "--workload", "create", "--out-plan", str(ctx.rd(row.run_id) / "plan.json")]


def argv_scrape(row: Row, ctx: Ctx, phase: str, stop_file: Path | None = None) -> list[str]:
    a = [ctx.py, "-B", ctx.tool("e1_metrics_scraper.py"), "scrape", "--phase", phase, "--out-dir",
         str(ctx.rd(row.run_id) / "metrics" / phase), "--interval", "1"]
    if stop_file is not None:
        a += ["--stop-file", str(stop_file)]
    return a + ["--execute"] + target_args()


def argv_resources(row: Row, ctx: Ctx, containers: list[str], stop_file: Path) -> list[str]:
    return [ctx.py, "-B", ctx.tool("e1_resource_sampler.py"), "sample", "--source", "api", "--out-dir",
            str(ctx.rd(row.run_id) / "resources"), "--interval", "1", "--stop-file", str(stop_file), "--execute"] + \
        [a for c in containers for a in ("--container", c)]


def argv_host(row: Row, ctx: Ctx, stop_file: Path) -> list[str]:
    """B3: the host sampler (WSL kernel files + Windows typeperf, 1 Hz). --max-seconds is a guard so that it can never outlive its run."""
    return [ctx.py, "-B", ctx.tool("e1_host_sampler.py"), "sample", "--out-dir", str(ctx.rd(row.run_id) / "host"), "--interval", "1",
            "--stop-file", str(stop_file), "--max-seconds", str(EVID.host_max_seconds), "--execute"]


def argv_summarize(row: Row, ctx: Ctx) -> list[str]:
    rd = ctx.rd(row.run_id)
    return [ctx.py, "-B", ctx.tool("e1_metrics_scraper.py"), "summarize", "--before", str(rd / "metrics/before"),
            "--during", str(rd / "metrics/during"), "--after", str(rd / "metrics/after"), "--channel", row.channel,
            "--fresh-channel", "--expect-traffic", "--out-dir", str(rd / "metrics/summary")]


def argv_fetch(row: Row, ctx: Ctx) -> list[str]:
    return [ctx.py, "-B", ctx.tool("e1_block_fetch.py"), "fetch", "--network", str(ctx.net), "--channel", row.channel,
            "--first", "0", "--max-message-count", str(row.B), "--preferred-max-bytes", str(row.P), "--out-dir",
            str(ctx.rd(row.run_id) / "ledger"), "--execute"]


def recorder_base(row: Row, ctx: Ctx, which: str) -> Path:
    if which == "measured":
        return ctx.recorder_dir(row.run_id) / f"{row.run_id}_w0_r1_m-{row.run_id}"
    return ctx.recorder_dir(row.run_id) / f"{row.run_id}_w0_r0_wu-{row.run_id}"


def argv_analysis(row: Row, ctx: Ctx, expected_n) -> list[str]:
    rd = ctx.rd(row.run_id)
    mb, wb = recorder_base(row, ctx, "measured"), recorder_base(row, ctx, "warmup")
    return [ctx.py, "-B", ctx.tool("e1_benchmark_analyze.py"), "analyze", "--run-id", row.run_id, "--recorder-base",
            str(mb), "--ledger-tx", str(rd / "ledger/ledger/ledger_tx.csv"), "--metrics-summary",
            str(rd / "metrics/summary/metrics_summary.json"), "--out-dir", str(rd / "analysis"), "--expected-n",
            str(expected_n), "--max-message-count", str(row.B), "--preferred-max-bytes", str(row.P),
            "--batch-timeout", row.tau, "--rate-tps", str(row.rate), "--other-round-csv",
            str(wb.with_name(wb.name + ".csv"))]


def argv_getinfo(net: "cr.Network", channel: str) -> list[str]:
    return [net.tool("peer"), "channel", "getinfo", "-c", channel]


def argv_fetch_config(net: "cr.Network", channel: str, pb: Path) -> list[str]:
    return [net.tool("peer"), "channel", "fetch", "config", str(pb), "-c", channel]


def argv_decode(net: "cr.Network", pb: Path, out_json: Path) -> list[str]:
    return [net.tool("configtxlator"), "proto_decode", "--input", str(pb), "--type", "common.Block", "--output", str(out_json)]


def caliper_plan(row: Row, ctx: Ctx, env_doc: dict) -> dict:
    cell = {"B": row.B, "tau": row.tau, "P": row.P, "n": 0, "mode": "txDuration", "rate_tps": float(row.rate),
            "duration_s": row.measured_s}
    return render.run_plan(env_doc, str(ctx.net), row.run_id, row.channel, cell, row.warmup_s, True)


def report_rel(run_id: str) -> str:
    """B4: the workspace-relative path of this run's own Caliper report (Caliper v0.6.0 resolves it against the workspace and creates missing directories)."""
    return f"runs/{run_id}/report.html"


def caliper_argv(plan_raw: dict) -> list[str]:
    """The rendered plan names the E0-0 container 'caliper' (stopped); the benchmark runs in caliper-benchmark-001, exactly
    as e1_existing_channel_pilot.py substitutes it. Anything other than the expected docker-exec shape is refused.
    B4: --caliper-report-path runs/<run id>/report.html is always appended, so that no launch overwrites the workspace's report.html or another run's
    (source: caliper-core v0.6.0 common/config/Config.js:41 'caliper-report-path', :239 command-line arguments become settings; manager/report/report-builder.js:347-358)."""
    argv = list(plan_raw["caliper"]["argv"])
    if argv[:4] != ["docker", "exec", "-w", render.CAL_WS] or argv[4] != plan_raw["containers"]["caliper"]:
        raise ValueError(f"unexpected Caliper argv shape: {argv[:6]}")
    if "--caliper-report-path" in argv:
        raise ValueError("the rendered plan already sets a Caliper report path")
    argv[4] = CALIPER
    return argv + ["--caliper-report-path", report_rel(plan_raw["run_id"])]


def expected_submits(row: Row) -> dict:
    return {"measured": row.rate * row.measured_s, "warmup": row.rate * row.warmup_s}


def expected_regime(row: Row) -> str:
    """Arithmetic expectation only (never a result): count-cut if B < rate*tau, timer-cut if B > rate*tau, boundary if equal; plus the P1a
    batching law: batch wait W = min((B - 1) / rate, tau) and p95 = W + a regime constant (P1a, 18 runs at 10-20 tx/s: count-cut +21..+27 ms,
    boundary -24 ms, timer-cut -52..-68 ms). Above 20 tx/s the law is a hypothesis, and this line is the pre-registered prediction to falsify."""
    fill = row.rate * 2
    w = min((row.B - 1) / row.rate, 2.0)
    if row.B < fill:
        return (f"count-cut expected (B={row.B} < rate*tau={fill}): about {row.B} tx per block, one block every {row.B / row.rate:.2f} s; "
                f"predicted batch wait W = {w:.3f} s, p95 about {w + 0.025:.3f} s (W + 0.025 s; hypothesis above 20 tx/s)")
    if row.B > fill:
        return (f"timer-cut expected (B={row.B} > rate*tau={fill}): about {fill} tx per block, one block every 2 s; "
                f"predicted batch wait W = {w:.3f} s, p95 about {w - 0.060:.3f} s (W - 0.060 s; hypothesis above 20 tx/s)")
    return (f"boundary (B = rate*tau = {fill}): count and timer cuts both occur; predicted batch wait W = {w:.3f} s, "
            f"p95 about {w - 0.024:.3f} s (W - 0.024 s; hypothesis above 20 tx/s)")


# ---------------------------------------------------------------------------------------------- backend (the only place that touches the world)
@dataclass
class CmdResult:
    code: int | None
    stdout: bytes = b""
    stderr: bytes = b""
    started_utc: str = ""
    ended_utc: str = ""
    duration_s: float = 0.0
    timed_out: bool = False


def _ps_encode(script: str) -> str:
    return base64.b64encode(script.encode("utf-16-le")).decode("ascii")


POWER_PS = r"""
Add-Type -AssemblyName System.Windows.Forms
$p = [System.Windows.Forms.SystemInformation]::PowerStatus
$alive = $null
if ($env:QL_HELPER_PID) { $alive = [bool](Get-Process -Id ([int]$env:QL_HELPER_PID) -ErrorAction SilentlyContinue) }
@{ line = [string]$p.PowerLineStatus; battery = [int]($p.BatteryLifePercent * 100); helper_alive = $alive } | ConvertTo-Json -Compress
"""

KEEP_AWAKE_PS = r"""
Add-Type -Namespace Win -Name Power -MemberDefinition '[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint f);'
[void][Win.Power]::SetThreadExecutionState(0x80000001)
Set-Content -Path $env:QL_PID_FILE -Value $PID
Start-Sleep -Seconds ([int]$env:QL_SECONDS)
"""


class Backend:
    """Real world access. Tests substitute a fake with the same methods."""

    def now_utc(self) -> str:
        return utc_now()

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, s: float) -> None:
        time.sleep(s)

    def run(self, argv: list[str], env: dict | None = None, timeout: float | None = None) -> CmdResult:
        t0, started = time.monotonic(), utc_now()
        try:
            p = subprocess.run(argv, env=env, capture_output=True, timeout=timeout, check=False)
            return CmdResult(p.returncode, p.stdout, p.stderr, started, utc_now(), time.monotonic() - t0)
        except subprocess.TimeoutExpired as exc:
            return CmdResult(None, exc.stdout or b"", (exc.stderr or b"") + b"\nTIMEOUT\n", started, utc_now(),
                             time.monotonic() - t0, True)
        except OSError as exc:
            return CmdResult(None, b"", repr(exc).encode(), started, utc_now(), time.monotonic() - t0)

    def spawn(self, argv: list[str], env: dict | None = None, stdout_path: Path | None = None,
              stderr_path: Path | None = None):
        out = stdout_path.open("xb") if stdout_path else subprocess.DEVNULL
        err = stderr_path.open("xb") if stderr_path else subprocess.DEVNULL
        return subprocess.Popen(argv, env=env, stdout=out, stderr=err)

    def http_status(self, url: str, timeout: float = 2.0) -> int | None:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.status
        except urllib.error.HTTPError as exc:
            return exc.code
        except (urllib.error.URLError, OSError, ValueError):
            return None

    def http_text(self, url: str, timeout: float = 5.0) -> str | None:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except (urllib.error.URLError, OSError, ValueError):
            return None

    def host_memory(self, samples: int, interval_s: int) -> dict | None:
        """Windows counters via typeperf.exe (interop): lighter than PowerShell, so the reading disturbs the host less."""
        argv = ["typeperf.exe", r"\Memory\Available MBytes", r"\Memory\% Committed Bytes In Use", r"\Memory\Pages/sec",
                "-si", str(interval_s), "-sc", str(samples)]
        r = self.run(argv, timeout=samples * interval_s + 60)
        if r.code != 0:
            return None
        return parse_typeperf(r.stdout.decode("utf-8", "replace"))

    def host_power(self, helper_pid: int | None = None) -> dict | None:
        env = with_wslenv(dict(os.environ), QL_HELPER_PID=str(helper_pid)) if helper_pid else dict(os.environ)
        r = self.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand", _ps_encode(POWER_PS)],
                     env=env, timeout=60)
        if r.code != 0:
            return None
        try:
            return json.loads(r.stdout.decode("utf-8", "replace").strip().splitlines()[-1])
        except (ValueError, IndexError):
            return None

    def start_keep_awake(self, seconds: int, pid_file_win: str):
        env = with_wslenv(dict(os.environ), QL_SECONDS=str(seconds), QL_PID_FILE=pid_file_win)
        return subprocess.Popen(["powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden",
                                 "-EncodedCommand", _ps_encode(KEEP_AWAKE_PS)], env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def disk_free_gb(self, path: str) -> float:
        return shutil.disk_usage(path).free / 1e9

    def wsl_mem_available_mb(self) -> float:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 1024.0
        return 0.0

    def boot_id(self) -> str:
        try:
            return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        except OSError:
            return ""


def parse_typeperf(text: str) -> dict | None:
    """typeperf CSV -> {'available_mb': [...], 'commit_pct': [...], 'pages_sec': [...]}; rows with a missing value are skipped."""
    out = {"available_mb": [], "commit_pct": [], "pages_sec": [], "rows": 0, "skipped": 0}
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith('"') or line.startswith('"(PDH'):
            continue
        cells = next(csv.reader([line]))
        if len(cells) != 4:
            continue
        try:
            a, c, p = (float(x) for x in cells[1:])
        except ValueError:
            out["skipped"] += 1
            continue
        out["available_mb"].append(a)
        out["commit_pct"].append(c)
        out["pages_sec"].append(p)
        out["rows"] += 1
    return out if out["rows"] else None


# ---------------------------------------------------------------------------------------------- parsers
INFO_RE = re.compile(r"Blockchain info:\s*(\{.*\})")


def parse_getinfo(text: str) -> dict | None:
    m = INFO_RE.search(text)
    try:
        return json.loads(m.group(1)) if m else None
    except ValueError:
        return None


def prom_value(text: str, metric: str, channel: str | None = None) -> float | None:
    pat = re.compile(r"^" + re.escape(metric) + r"\{([^}]*)\}\s+(\S+)\s*$", re.M)
    for labels, val in pat.findall(text or ""):
        if channel is None or f'channel="{channel}"' in labels:
            try:
                return float(val)
            except ValueError:
                return None
    return None


def prom_channels(text: str, metric: str = "consensus_etcdraft_cluster_size") -> set[str]:
    return set(re.findall(r"^" + re.escape(metric) + r'\{[^}]*channel="([^"]+)"', text or "", re.M))


def parse_inspect(text: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    try:
        arr = json.loads(text)
    except ValueError:
        return out
    for d in arr if isinstance(arr, list) else []:
        st = d.get("State", {})
        out[d.get("Name", "").lstrip("/")] = {"id": d.get("Id", ""), "status": st.get("Status"),
                                              "running": bool(st.get("Running")), "oom": bool(st.get("OOMKilled")),
                                              "started_at": st.get("StartedAt"), "restart_count": d.get("RestartCount", 0),
                                              "image": d.get("Config", {}).get("Image")}
    return out


def container_events(now: dict, baseline: dict) -> list[str]:
    """Restart, replacement, exit or OOM against the session baseline (protocol V6)."""
    problems = []
    for name, b in baseline.items():
        c = now.get(name)
        if c is None:
            problems.append(f"{name}: not found")
            continue
        if c["id"] != b["id"]:
            problems.append(f"{name}: container replaced")
        if not c["running"] or c["status"] != "running":
            problems.append(f"{name}: not running ({c['status']})")
        if c["oom"]:
            problems.append(f"{name}: OOM killed")
        if c["started_at"] != b["started_at"] or c["restart_count"] != b["restart_count"]:
            problems.append(f"{name}: restarted (StartedAt {b['started_at']} -> {c['started_at']}, "
                            f"RestartCount {b['restart_count']} -> {c['restart_count']})")
    return problems


# ---------------------------------------------------------------------------------------------- gate evaluators (pure)
@dataclass
class GateResult:
    id: str
    name: str
    status: str                      # PASS | FAIL
    rule: str
    detail: str = ""
    evidence: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"id": self.id, "name": self.name, "status": self.status, "rule": self.rule, "detail": self.detail,
                "evidence": self.evidence}


def eval_memory(win: dict | None, wsl_avail_mb: float, thr: Thresholds = THR) -> GateResult:
    rule = (f"Windows available >= {thr.win_avail_mb_start:.0f} MB at every sample; commit <= {thr.win_commit_pct_start:.0f}% at every "
            f"sample; Pages/sec median <= {thr.win_pages_median_start:.0f} over the {thr.mem_window_samples * thr.mem_window_interval_s}-s "
            f"idle window; WSL MemAvailable >= {thr.wsl_avail_mb_start:.0f} MB")
    if not win or not win.get("available_mb"):
        return GateResult("S3", "host memory headroom", "FAIL", rule, "no Windows counter samples could be read")
    avail_min, commit_max = min(win["available_mb"]), max(win["commit_pct"])
    pages = win["pages_sec"]
    med = statistics.median(pages)
    over = sum(1 for x in pages if x > thr.win_pages_median_start)
    checks = {"available_min": avail_min >= thr.win_avail_mb_start, "commit_max": commit_max <= thr.win_commit_pct_start,
              "pages_median": med <= thr.win_pages_median_start, "wsl": wsl_avail_mb >= thr.wsl_avail_mb_start}
    ev = {"samples": len(pages), "available_mb_min": avail_min, "available_mb_median": statistics.median(win["available_mb"]),
          "commit_pct_max": commit_max, "pages_sec_median": med, "pages_sec_max": max(pages),
          "pages_sec_samples_over_100": over, "wsl_mem_available_mb": round(wsl_avail_mb, 1), "checks": checks}
    status = "PASS" if all(checks.values()) else "FAIL"
    return GateResult("S3", "host memory headroom", status, rule,
                      f"available min {avail_min:.0f} MB, commit max {commit_max:.1f}%, Pages/sec median {med:.1f} "
                      f"(max {max(pages):.0f}; {over}/{len(pages)} samples > 100), WSL {wsl_avail_mb:.0f} MB", ev)


def eval_disk(free_gb: float, thr: Thresholds = THR) -> GateResult:
    rule = f"C: free >= {thr.disk_free_gb_start:.0f} GB to start; hard stop below {thr.disk_free_gb_hard:.0f} GB"
    return GateResult("S4", "disk", "PASS" if free_gb >= thr.disk_free_gb_start else "FAIL", rule,
                      f"C: free {free_gb:.1f} GB", {"free_gb": round(free_gb, 2)})


def v2_pacing(creates_ms: list[float], submitted: int, rate: float, measured_s: float, event_loop_p99_ms: float | None,
              thr: Thresholds = THR) -> dict:
    """Protocol V2: |A/lambda - 1| <= 0.02; p99 create lateness against the schedule <= 5 ms; event-loop p99 <= 20 ms.
    Lateness: create times sorted; the k-th is due at T0 + k/rate, T0 estimated as the smallest (create_k - k/rate)."""
    ratio = submitted / (rate * measured_s) if rate * measured_s else None
    lateness_p99 = None
    if len(creates_ms) >= 2:
        cs = sorted(creates_ms)
        step = 1000.0 / rate
        offs = [c - k * step for k, c in enumerate(cs)]
        t0 = min(offs)
        late = sorted(o - t0 for o in offs)
        lateness_p99 = late[max(0, math.ceil(0.99 * len(late)) - 1)]
    ok = (ratio is not None and abs(ratio - 1) <= thr.pacing_ratio_tol) and \
         (lateness_p99 is not None and lateness_p99 <= thr.lateness_p99_ms) and \
         (event_loop_p99_ms is not None and event_loop_p99_ms <= thr.event_loop_p99_ms)
    return {"submitted": submitted, "ratio_A_over_lambda": ratio, "lateness_p99_ms": lateness_p99,
            "event_loop_p99_ms": event_loop_p99_ms, "pass": bool(ok)}


def interior_valid_rate(meas: list[dict], valid_ids: set[str], measured_s: float, thr: Thresholds = THR) -> dict:
    """VALID commits per second inside the measured round minus the first and last trim seconds."""
    if not meas:
        return {"rate": None, "n": 0}
    t0 = min(float(m["create_ms"]) for m in meas)
    lo, hi = t0 + thr.interior_trim_s * 1000, t0 + (measured_s - thr.interior_trim_s) * 1000
    n = sum(1 for m in meas if m["status"] == "success" and m["tx_id"] in valid_ids and lo <= float(m["final_ms"]) < hi)
    return {"rate": n / ((hi - lo) / 1000.0), "n": n, "window_s": (hi - lo) / 1000.0}


def classify(rate: float, vrate: dict, backlog: int | None, failure_rate: float | None, event_loop_p99: float | None,
             caliper_cpu_mean: float | None, pacing_pass: bool, thr: Thresholds = THR) -> list[str]:
    """Pre-declared labels (plan section 6), applied post hoc from the data."""
    labels = []
    sat = (vrate.get("rate") is not None and vrate["rate"] / rate < thr.sat_valid_ratio) or \
          (backlog is not None and backlog > thr.sat_backlog) or \
          (failure_rate is not None and failure_rate > thr.failure_rate_max)
    if sat:
        labels.append("SATURATED")
    if (event_loop_p99 is not None and event_loop_p99 > thr.client_event_loop_p99_ms) or \
            (caliper_cpu_mean is not None and caliper_cpu_mean >= thr.client_cpu_pct) or not pacing_pass:
        labels.append("CLIENT_LIMITED")
    return labels


# ---------------------------------------------------------------------------------------------- per-run verdict (offline, deterministic)
def _read_csv(path: Path) -> list[dict]:
    with Path(path).open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _cpu_mean_in_window(samples_csv: Path, container: str, lo_ms: float, hi_ms: float) -> float | None:
    if not samples_csv.is_file():
        return None
    vals = []
    for r in _read_csv(samples_csv):
        if r.get("container") != container or r.get("cpu_percent") in (None, ""):
            continue
        t = r.get("read_ns") or r.get("recv_wall_ns") or r.get("t_mid_wall_ns")
        try:
            t_ms = float(t) / 1e6
            if lo_ms <= t_ms <= hi_ms:
                vals.append(float(r["cpu_percent"]))
        except (TypeError, ValueError):
            continue
    return statistics.mean(vals) if vals else None


def evidence_completeness(rd: Path, lo_ms: float, hi_ms: float, evid: EvidenceThresholds = EVID) -> dict:
    """Plan section 7.8 (E): for every 1 Hz stream, the samples inside [lo_ms, hi_ms] against the expected count and the longest hole (edges included).

    Streams: the resource sampler (one per container named in its own manifest), the Prometheus scrapes (one per target named in its manifest, HTTP 200 only),
    the host sampler's WSL stream and its Windows stream. `missing` names a source that is absent, in which case nothing can be concluded."""
    out: dict = {"window_ms": [lo_ms, hi_ms], "streams": {}, "missing": [], "host_required_counters_missing": None}

    def add(name: str, times_ms: list[float]) -> None:
        c = hs.stream_completeness(times_ms, lo_ms, hi_ms)
        c["ok"] = bool(c["ratio"] >= evid.min_ratio and c["max_gap_s"] <= evid.max_gap_s)
        out["streams"][name] = c

    rman, rs = load_json(rd / "resources" / "resource_manifest.json"), rd / "resources" / "resource_samples.csv"
    if rman is None or not rs.is_file() or not rman.get("containers"):
        out["missing"].append("resources")
    else:
        per: dict[str, list[float]] = {c: [] for c in rman["containers"]}
        for r in _read_csv(rs):
            t = hs._f(r.get("read_ns")) or hs._f(r.get("recv_wall_ns"))
            if t is not None and r.get("container") in per:
                per[r["container"]].append(t / 1e6)
        for c, ts in per.items():
            add(f"resources:{c}", ts)
    mman, sc = load_json(rd / "metrics" / "during" / "metrics_manifest.json"), rd / "metrics" / "during" / "scrapes.csv"
    if mman is None or not sc.is_file() or not mman.get("targets"):
        out["missing"].append("scrapes")
    else:
        per = {t["name"]: [] for t in mman["targets"]}
        for r in _read_csv(sc):
            t = hs._f(r.get("t_mid_wall_ns"))
            if t is not None and r.get("http_status") == "200" and r.get("target") in per:
                per[r["target"]].append(t / 1e6)
        for c, ts in per.items():
            add(f"scrapes:{c}", ts)
    hman, wsl, win = load_json(rd / "host" / "host_manifest.json"), rd / "host" / "wsl_samples.csv", rd / "host" / "win_counters.csv"
    if hman is None or not wsl.is_file() or not win.is_file():
        out["missing"].append("host")
    else:
        add("host:wsl", [t / 1e6 for t in (hs._f(r.get("wall_ns")) for r in _read_csv(wsl)) if t is not None])
        add("host:windows", [t / 1e6 for t in (hs._f(r.get("t_wall_ns")) for r in _read_csv(win)) if t is not None])
        out["host_required_counters_missing"] = list(hman.get("windows_missing_required") or []) + \
            ([] if (hman.get("parameters") or {}).get("windows") else ["windows side disabled"])
    return out


def evaluate_run(rd: Path, row: Row, rec_dir: Path, facts: dict, thr: Thresholds = THR) -> dict:
    """Validity gates and labels for one finished run. Reads only files, so it can be re-run offline (`verdict`)."""
    gates: list[GateResult] = []

    def gate(id_: str, name: str, ok: bool | None, rule: str, detail: str = "", ev: dict | None = None) -> None:
        gates.append(GateResult(id_, name, "NOT_EVALUATED" if ok is None else ("PASS" if ok else "FAIL"), rule, detail, ev or {}))

    # channel
    crec = load_json(rd / "channel" / "channel_run_record.json")
    chk = (crec or {}).get("checks", [])
    gate("G-channel", "channel VERIFIED with the requested configuration",
         None if crec is None else (crec.get("status") == "VERIFIED" and bool(chk) and all(c.get("pass") for c in chk)),
         "channel_run_record status VERIFIED; every effective-configuration check passes",
         f"status {(crec or {}).get('status')}, checks {sum(1 for c in chk if c.get('pass'))}/{len(chk)}")
    # configuration unchanged
    cc = load_json(rd / "config_check.json")
    gate("G-config-stable", "channel configuration unchanged across the workload",
         None if cc is None else bool(cc.get("unchanged")),
         "config block hash and active configuration equal before and after", json.dumps({k: cc.get(k) for k in ("unchanged", "reason")}) if cc else "no config_check.json")
    # caliper
    cal = facts.get("caliper") or {}
    aborted = facts.get("aborted")
    gate("G-caliper", "Caliper completed",
         None if not cal else (cal.get("exit_code") == 0 and not cal.get("timed_out") and not aborted),
         "exit 0, no timeout, no abort", f"exit {cal.get('exit_code')}, timed_out {cal.get('timed_out')}, aborted {aborted}")
    # recorder files
    mb, wb = recorder_base(row, Ctx_for(rec_dir), "measured"), recorder_base(row, Ctx_for(rec_dir), "warmup")
    need = [mb.with_name(mb.name + s) for s in (".csv", ".submits.csv", ".manifest.json")] + \
           [wb.with_name(wb.name + s) for s in (".csv", ".submits.csv", ".manifest.json")]
    missing = [p.name for p in need if not p.is_file()]
    failed_markers = [p.name for p in rec_dir.glob("*.FAILED")] if rec_dir.is_dir() else []
    mm = load_json(mb.with_name(mb.name + ".manifest.json")) or {}
    wm = load_json(wb.with_name(wb.name + ".manifest.json")) or {}
    counts = mm.get("counts", {})
    rec_ok = not missing and not failed_markers and counts.get("observer_errors", 1) == 0 and counts.get("duplicate_ids", 1) == 0
    gate("G-recorder", "recorder files for both rounds", None if not cal else rec_ok,
         "six recorder files present, no .FAILED marker, no observer errors, no duplicate ids",
         f"missing {missing}; markers {failed_markers}; observer_errors {counts.get('observer_errors')}; duplicates {counts.get('duplicate_ids')}; "
         f"pending_at_deactivate {counts.get('pending_at_deactivate')}")
    # analysis / reconciliation
    summ = load_json(rd / "analysis" / "summary.json")
    gate("G-reconcile", "recorder-ledger reconciliation VALID",
         None if summ is None else (summ.get("reconciliation", {}).get("verdict") == "VALID"),
         "e1_benchmark_analyze reconciliation verdict VALID (lifecycle transactions declared foreign)",
         f"reconciliation {(summ or {}).get('reconciliation', {}).get('verdict')}, analysis {(summ or {}).get('analysis_verdict')}")
    # telemetry
    ms = load_json(rd / "metrics" / "summary" / "metrics_summary.json")
    rman = load_json(rd / "resources" / "resource_manifest.json")
    tel_ok = None if ms is None or rman is None else (ms.get("status") in tc.USABLE and rman.get("status") in tc.USABLE)
    gate("G-telemetry", "metrics summary and resource sampler usable", tel_ok,
         "both COMPLETE or COMPLETE_WITH_WARNINGS", f"metrics {(ms or {}).get('status')}, resources {(rman or {}).get('status')}")
    bf = load_json(rd / "ledger" / "block_fetch_manifest.json")
    gate("G-ledger", "ledger tables written", None if bf is None else (bf.get("status") in tc.USABLE and bool(bf.get("ledger_tables", {}).get("written"))),
         "block fetch status usable and ledger tables written", f"status {(bf or {}).get('status')}")
    # V6 leader changes and containers
    deltas = {t: d.get("leader_changes_delta") for t, d in (ms or {}).get("derived", {}).items() if d.get("role") == "orderer"}
    gate("V6-leader", "no Raft leader change", None if not deltas or any(v is None for v in deltas.values()) else all(v == 0 for v in deltas.values()),
         "leader_changes_delta == 0 on every orderer", json.dumps(deltas))
    bad = {k: v for k, v in ((rman or {}).get("anomalies", {}).get("error", {}) or {}).items()
           if k in ("container_restarted", "container_replaced", "oom_killed", "container_not_running")}
    gate("V6-containers", "no container restart, replacement, OOM or exit", None if rman is None else not bad,
         "resource sampler container anomalies absent", json.dumps(bad))
    host = facts.get("host", {})
    pre, post = host.get("pre") or {}, host.get("post") or {}
    mem_vals = [v for v in (pre.get("win_available_mb"), post.get("win_available_mb")) if v is not None]
    wsl_vals = [v for v in (pre.get("wsl_available_mb"), post.get("wsl_available_mb")) if v is not None]
    gate("V6-host", "host memory headroom held",
         None if not mem_vals else (min(mem_vals) >= thr.win_avail_mb_run and (not wsl_vals or min(wsl_vals) >= thr.wsl_avail_mb_run)),
         f"Windows available >= {thr.win_avail_mb_run:.0f} MB and WSL >= {thr.wsl_avail_mb_run:.0f} MB before and after",
         f"Windows {mem_vals}, WSL {wsl_vals}")
    # V7 clock
    wmm = [m.get("clock", {}).get("wall_minus_monotonic_ms") for m in (wm, mm) if m]
    gate("V7-clock", "wall minus monotonic over each round", None if not wmm or any(v is None for v in wmm) else all(abs(float(v)) <= thr.clock_wall_minus_mono_ms for v in wmm),
         f"|wall - monotonic| <= {thr.clock_wall_minus_mono_ms} ms over each round (continuous-probe window check runs after the series)", f"{wmm}")

    # measurements, pacing, labels
    meas = _read_csv(rd / "analysis" / "transaction_measurements.csv") if (rd / "analysis" / "transaction_measurements.csv").is_file() else []
    valid_ids = set()
    ltx = rd / "ledger" / "ledger" / "ledger_tx.csv"
    if ltx.is_file():
        valid_ids = {r["tx_id"] for r in _read_csv(ltx) if r.get("header_type") == "3" and r.get("validation_code") == "0"}
    submitted = int(counts.get("submitted", 0) or 0)
    el = (mm.get("event_loop_delay") or {}).get("p99_ms")
    pacing = v2_pacing([float(m["create_ms"]) for m in meas], submitted, row.rate, row.measured_s, el, thr) if meas else \
        {"pass": False, "note": "no measurements"}
    gate("V2-pacing", "client pacing", pacing["pass"] if meas else None,
         f"|A/lambda - 1| <= {thr.pacing_ratio_tol}; p99 lateness <= {thr.lateness_p99_ms} ms; event-loop p99 <= {thr.event_loop_p99_ms} ms",
         json.dumps({k: pacing.get(k) for k in ("ratio_A_over_lambda", "lateness_p99_ms", "event_loop_p99_ms")}), pacing)
    vrate = interior_valid_rate([{**m, "tx_id": m["tx_id"]} for m in meas], valid_ids, row.measured_s, thr) if meas else {"rate": None, "n": 0}
    n_valid_ledger = (summ or {}).get("counts", {}).get("valid_workload_ledger_transactions")
    failure_rate = (1 - n_valid_ledger / submitted) if (submitted and n_valid_ledger is not None) else None
    backlog = (summ or {}).get("metrics", {}).get("max_inflight_backlog")
    t0w, t1w = mm.get("clock", {}).get("wall0_ms"), mm.get("clock", {}).get("wall1_ms")
    cpu = _cpu_mean_in_window(rd / "resources" / "resource_samples.csv", CALIPER, float(t0w), float(t1w)) if t0w and t1w else None
    labels_frozen = classify(float(row.rate), vrate, backlog, failure_rate, el, cpu, bool(pacing["pass"]) if meas else True, thr)
    c2 = ccv2.classify_v2({"rate": float(row.rate), "ratio_A_over_lambda": pacing.get("ratio_A_over_lambda"),
                         "lateness_p99_ms": pacing.get("lateness_p99_ms"), "event_loop_p99_ms": el, "caliper_cpu_mean_pct": cpu,
                         "failure_rate": failure_rate, "max_inflight_backlog": backlog, "interior_valid_rate_tps": vrate.get("rate")})
    labels = c2["labels_v2"]
    overload = (vrate.get("rate") is not None and vrate["rate"] / row.rate < thr.sat_valid_ratio) or \
               (backlog is not None and backlog > thr.sat_backlog)
    fail_ok = None if failure_rate is None else (failure_rate <= thr.failure_rate_max or overload)
    gate("G-failures", "failed or INVALID transactions", fail_ok,
         f"(submitted - VALID in ledger) / submitted <= {thr.failure_rate_max:.2f}, or overload evidence (interior ratio < "
         f"{thr.sat_valid_ratio} or backlog > {thr.sat_backlog}); failures above the limit with sustained throughput are a plant fault, not saturation",
         f"failure rate {failure_rate}, overload evidence {overload}, labels {labels}")
    # owner decision 2026-11-13 (bounds-only Tier 1): post-run backlog, transaction validity and failure rate are mandatory evidence, not optional diagnostics
    post_ok = None if summ is None else bool(backlog is not None and vrate.get("rate") is not None and failure_rate is not None and valid_ids)
    gate("G-postrun", "mandatory post-run evidence: backlog, transaction validity, failure rate", post_ok,
         "max in-flight backlog, the interior VALID rate, the failure rate and the ledger validation codes all exist (a missing one is INVALID_DATA, never a pass)",
         f"backlog {backlog}, interior VALID rate {vrate.get('rate')}, failure rate {failure_rate}, ledger VALID transactions {len(valid_ids)}")
    # plan 7.8 (E) with B3: every 1 Hz stream covers the measured window
    evc = None
    if not (t0w and t1w):
        ev_ok, ev_detail = None, "the measured window is unknown (the recorder manifest has no clock)"
    else:
        evc = evidence_completeness(rd, float(t0w), float(t1w))
        failing_streams = sorted(n for n, c in evc["streams"].items() if not c["ok"])
        ratios, gaps = [c["ratio"] for c in evc["streams"].values()], [c["max_gap_s"] for c in evc["streams"].values()]
        if evc["missing"]:
            ev_ok, ev_detail = None, f"source files missing: {evc['missing']}"
        elif evc["host_required_counters_missing"]:
            ev_ok, ev_detail = False, f"host record lacks required counters: {evc['host_required_counters_missing']}"
        else:
            ev_ok = not failing_streams
            ev_detail = (f"{len(ratios)} streams; worst sample ratio {min(ratios):.3f}; longest hole {max(gaps):.2f} s; "
                         f"failing: {failing_streams or 'none'}")
    gate("G-evidence", "evidence completeness in the measured window", ev_ok,
         f"every sampler stream (resource sampler per container, Prometheus scrapes per target, host WSL and host Windows) has >= {EVID.min_ratio:.0%} of the expected 1 Hz "
         f"samples inside the measured window and no hole above {EVID.max_gap_s:g} s; the host record has every required Windows counter (plan 7.8 (E))",
         ev_detail, {"streams": (evc or {}).get("streams")})
    # B4: the per-run report exists, Caliper said where it wrote it, and the workspace's default report was not touched
    rep = facts.get("report") or {}
    per_run = Path(rec_dir).parent / "report.html"
    cal_out = sorted((rd / "cmd").glob("*_caliper.stdout"))
    said = f"Generated report with path {render.CAL_WS}/{report_rel(row.run_id)}"
    out_text = cal_out[0].read_text(encoding="utf-8", errors="replace") if cal_out else None
    head = per_run.read_bytes()[:4096].decode("utf-8", "replace").lower() if per_run.is_file() else ""
    rep_checks = {"per_run_report_exists_and_is_html": per_run.is_file() and per_run.stat().st_size > 0 and "<html" in head,
                  "caliper_log_names_the_per_run_path": out_text is not None and said in out_text,
                  "default_report_unchanged": rep.get("default_sha256_before") == rep.get("default_sha256_after")}
    gate("G-report", "per-run Caliper report written, default report untouched", None if not cal or not rep else all(rep_checks.values()),
         f"{report_rel(row.run_id)} exists and is HTML; the Caliper log says '{said}'; the workspace report.html has the same SHA-256 before and after the launch",
         json.dumps(rep_checks), {"checks": rep_checks, "facts": rep})

    by_id = {g.id: g for g in gates}
    failed = [g.id for g in gates if g.status == "FAIL"]
    not_eval = [g.id for g in gates if g.status == "NOT_EVALUATED"]
    if aborted:
        verdict = "ABORTED_INFRA"
    elif any(i in failed for i in INFRA_GATES):
        verdict = "INVALID_INFRA"
    elif any(i in failed for i in DATA_GATES) or not_eval:
        verdict = "INVALID_DATA"
    elif "V2-pacing" in failed:
        verdict = "INVALID_SCHEDULE"
    else:
        verdict = "VALID"
    host_summary = None                  # descriptive only: no host figure labels a run (the project has no basis for a host threshold)
    if t0w and t1w and (rd / "host" / "wsl_samples.csv").is_file() and (rd / "host" / "win_counters.csv").is_file():
        host_summary = hs.summarize_window(hs.read_rows(rd / "host" / "wsl_samples.csv"), hs.read_rows(rd / "host" / "win_counters.csv"), float(t0w), float(t1w))
    return {"run_id": row.run_id, "channel": row.channel, "cell": {"B": row.B, "rate_tps": row.rate, "replicate": row.replicate},
            "verdict": verdict, "labels": labels, "labels_frozen_rule": labels_frozen, "class_v2": c2["class_v2"],
            "flags_v2": c2["flags"], "failed_gates": failed, "not_evaluated_gates": not_eval,
            "gates": [g.as_dict() for g in gates],
            "metrics": {"interior_valid_rate_tps": vrate.get("rate"), "interior_valid_n": vrate.get("n"),
                        "failure_rate": failure_rate, "max_inflight_backlog": backlog, "caliper_cpu_mean_pct": cpu,
                        "event_loop_p99_ms": el, "submitted": submitted,
                        "latency_ms_p95_nearest_rank": (summ or {}).get("metrics", {}).get("latency_ms_p95_nearest_rank"),
                        "latency_ms_mean": (summ or {}).get("metrics", {}).get("latency_ms_mean"),
                        "host": host_summary,
                        "evidence": {n: {k: c[k] for k in ("count", "expected", "ratio", "max_gap_s")} for n, c in (evc or {}).get("streams", {}).items()}},
            "evaluated_utc": utc_now(), "tool": TOOL_VERSION}


@dataclass(frozen=True)
class _RecCtx:
    d: Path

    def recorder_dir(self, run_id: str) -> Path:
        return self.d


def Ctx_for(rec_dir: Path) -> "_RecCtx":
    """recorder_base only needs the recorder directory, so a verdict can be rebuilt from files alone."""
    return _RecCtx(rec_dir)


# ---------------------------------------------------------------------------------------------- engine
class StepFailed(RuntimeError):
    def __init__(self, label: str, detail: str, category: str = "infra"):
        super().__init__(f"{label}: {detail}")
        self.label, self.detail, self.category = label, detail, category


class HaltSeries(RuntimeError):
    pass


@dataclass
class Session:
    ctx: Ctx
    be: Backend
    thr: Thresholds = THR
    baseline: dict = field(default_factory=dict)          # container -> inspect facts at session start
    chaincode: list = field(default_factory=list)         # peer-launched chaincode containers found at session start
    helper_pid: int | None = None
    probe: object | None = None                           # the continuous clock probe process (A4), when the series started one
    verdicts: dict = field(default_factory=dict)
    log_path: Path | None = None
    max_rate: int | None = None                           # owner-approved cap on the offered rate (approval file); None = no cap
    anchor_classes: dict = field(default_factory=dict)    # P1a run id -> corrected class from the pinned erratum table

    def log(self, msg: str) -> None:
        line = f"{self.be.now_utc()} {msg}"
        print(line, flush=True)
        if self.log_path is not None:
            with self.log_path.open("a", encoding="utf-8", newline="\n") as fh:
                fh.write(line + "\n")


class RunRecorder:
    """Every command's stdout and stderr go to rd/cmd/NN_label.*; commands.jsonl keeps the ordered provenance."""

    def __init__(self, rd: Path, be: Backend):
        self.rd, self.be, self.n = rd, be, 0
        (rd / "cmd").mkdir(parents=True, exist_ok=False)
        self.fh = (rd / "commands.jsonl").open("x", encoding="utf-8", newline="\n")

    def _stem(self, label: str) -> str:
        self.n += 1
        return f"{self.n:02d}_{label}"

    def _emit(self, rec: dict) -> None:
        self.fh.write(json.dumps(rec, sort_keys=True, default=str) + "\n")
        self.fh.flush()

    def run(self, label: str, argv: list[str], env: dict | None = None, timeout: float | None = None) -> CmdResult:
        stem = self._stem(label)
        res = self.be.run(argv, env, timeout)
        (self.rd / "cmd" / f"{stem}.stdout").write_bytes(res.stdout)
        (self.rd / "cmd" / f"{stem}.stderr").write_bytes(res.stderr)
        self._emit({"label": label, "stem": stem, "argv": argv, "start_utc": res.started_utc, "end_utc": res.ended_utc,
                    "duration_s": round(res.duration_s, 3), "exit_code": res.code, "timed_out": res.timed_out})
        return res

    def spawn(self, label: str, argv: list[str], env: dict | None = None):
        stem = self._stem(label)
        proc = self.be.spawn(argv, env, self.rd / "cmd" / f"{stem}.stdout", self.rd / "cmd" / f"{stem}.stderr")
        self._emit({"label": label, "stem": stem, "argv": argv, "start_utc": self.be.now_utc(), "background": True})
        return proc

    def note(self, label: str, **kw) -> None:
        self._emit({"label": label, "utc": self.be.now_utc(), **kw})

    def close(self) -> None:
        self.fh.close()


def host_snapshot(be: Backend, helper_pid: int | None = None) -> dict:
    mem = be.host_memory(2, 1)
    snap = {"utc": be.now_utc(), "wsl_available_mb": round(be.wsl_mem_available_mb(), 1),
            "disk_free_gb_c": round(be.disk_free_gb("/mnt/c"), 2), "power": be.host_power(helper_pid),
            "win_available_mb": None, "win_commit_pct": None, "win_pages_sec": None}
    if mem:
        snap.update(win_available_mb=mem["available_mb"][-1], win_commit_pct=mem["commit_pct"][-1],
                    win_pages_sec=mem["pages_sec"][-1])
    return snap


def health(be: Backend) -> dict:
    return {n: be.http_status(f"http://127.0.0.1:{p}/healthz") for n, p in OPS_PORT.items()}


def inspect_all(be: Backend, names: list[str]) -> dict:
    r = be.run(["docker", "inspect", *names], None, 30)
    return parse_inspect(r.stdout.decode("utf-8", "replace")) if r.code == 0 else {}


def chaincode_containers(be: Backend) -> list[str]:
    r = be.run(["docker", "ps", "-a", "--filter", "name=dev-peer", "--format", "{{.Names}}"], None, 30)
    return sorted(x.strip() for x in r.stdout.decode("utf-8", "replace").splitlines() if x.strip()) if r.code == 0 else []


def do_getinfo(rec: RunRecorder, net: "cr.Network", channel: str, penv: dict, label: str) -> dict:
    r = rec.run(label, argv_getinfo(net, channel), penv, 30)
    info = parse_getinfo((r.stdout + b"\n" + r.stderr).decode("utf-8", "replace"))
    if r.code != 0 or not info:
        raise StepFailed(label, "no usable channel height")
    return info


def do_fetch_config(rec: RunRecorder, net: "cr.Network", channel: str, penv: dict, label: str, rd: Path) -> dict:
    (rd / "config").mkdir(exist_ok=True)
    pb, js = rd / "config" / f"{label}.pb", rd / "config" / f"{label}.json"
    r1 = rec.run(f"{label}_fetch", argv_fetch_config(net, channel, pb), penv, 30)
    if r1.code != 0:
        raise StepFailed(label, "config fetch failed")
    r2 = rec.run(f"{label}_decode", argv_decode(net, pb, js), penv, 30)
    if r2.code != 0:
        raise StepFailed(label, "config decode failed")
    block = json.loads(js.read_text(encoding="utf-8"))
    active, _policy = ca.config_from_block(block)
    return {"active_config": active.as_dict(), "block_number": ca.block_number(block),
            "block_data_hash": ca.block_data_hash(block), "block_sha256": ca.sha256(pb)}


def stop_background(be: Backend, rec: RunRecorder, handles: list, timeout_s: float = 120.0) -> dict:
    out = {}
    for name, proc, stop_file in handles:
        if not stop_file.exists():
            stop_file.write_text("stop\n", encoding="utf-8")
        deadline = be.monotonic() + timeout_s
        while proc.poll() is None and be.monotonic() < deadline:
            be.sleep(0.5)
        if proc.poll() is None:
            proc.kill()
            out[name] = "killed"
        else:
            out[name] = proc.returncode
        rec.note(f"stop_{name}", result=out[name])
    return out


def run_caliper(s: Session, rec: RunRecorder, plan_raw: dict, env: dict) -> dict:
    """Run Caliper through the plan's own docker-exec argv, polling health every 5 s; abort on infrastructure events."""
    be, thr = s.be, s.thr
    proc = rec.spawn("caliper", caliper_argv(plan_raw), env)
    t0 = be.monotonic()
    next_poll, fails, aborted, timed_out = t0 + thr.health_poll_s, 0, None, False
    while True:
        code = proc.poll()
        if code is not None:
            break
        now = be.monotonic()
        if now - t0 > thr.caliper_timeout_s:
            timed_out, aborted = True, f"hard timeout {thr.caliper_timeout_s:.0f} s"
            break
        if now >= next_poll:
            next_poll = now + thr.health_poll_s
            h = health(be)
            ins = inspect_all(be, list(s.baseline))
            bad = [k for k, v in h.items() if v != 200] + ([] if ins else ["docker-inspect"])
            fails = fails + 1 if bad else 0
            ev = container_events(ins, s.baseline) if ins else []
            if ev:
                aborted = "container event: " + "; ".join(ev)
                break
            if fails >= thr.health_consecutive_fail:
                aborted = f"health failed {fails} consecutive polls: {bad}"
                break
        be.sleep(1.0)
    if aborted:
        try:
            proc.kill()
        except Exception:                                                                 # noqa: BLE001 - best effort
            pass
        k = rec.run("abort_docker_kill", ["docker", "kill", CALIPER], None, 60)
        rec.note("caliper_aborted", reason=aborted, docker_kill_exit=k.code)
        try:
            proc.wait(timeout=10)
        except Exception:                                                                 # noqa: BLE001
            pass
        code = proc.poll()
    rec.note("caliper_end", exit_code=code, aborted=aborted)
    return {"exit_code": code, "timed_out": timed_out, "aborted": aborted, "duration_s": round(be.monotonic() - t0, 3)}


def execute_run(s: Session, row: Row) -> dict:
    """One run, strictly in order. Never retried. Always leaves facts.json, commands.jsonl and run_verdict.json."""
    ctx, be, thr = s.ctx, s.be, s.thr
    rd = ctx.rd(row.run_id)
    rd.mkdir(parents=False, exist_ok=False)
    rec = RunRecorder(rd, be)
    env, net = tool_env(ctx), load_network(ctx)
    penv = peer_env(ctx, net)
    facts: dict = {"run_id": row.run_id, "channel": row.channel, "started_utc": be.now_utc(), "host": {}, "caliper": {},
                   "step_failure": None, "aborted": None}
    handles: list = []
    try:
        facts["host"]["pre"] = host_snapshot(be, s.helper_pid)
        r = rec.run("channel", argv_channel(row, ctx), env, thr.channel_timeout_s)
        crec = load_json(rd / "channel" / "channel_run_record.json")
        if r.code != 0 or not crec or crec.get("status") != "VERIFIED":
            raise StepFailed("channel", f"exit {r.code}, status {(crec or {}).get('status')}")
        r = rec.run("render", argv_render(row, ctx), env, thr.tool_timeout_s)
        if r.code != 0:
            raise StepFailed("render", f"exit {r.code}")
        plan = ro.Plan(json.loads((rd / "plan.json").read_text(encoding="utf-8")))
        if Path(plan.raw["caliper"]["recorder_dir"]) != ctx.recorder_dir(row.run_id):
            raise StepFailed("render", "plan recorder_dir differs from the driver's expectation")
        r = rec.run("metrics_before", argv_scrape(row, ctx, "before"), env, thr.tool_timeout_s)
        if r.code != 0:
            raise StepFailed("metrics_before", f"exit {r.code}")
        stop_m, stop_r, stop_h = rd / "stop_metrics", rd / "stop_resources", rd / "stop_host"
        containers = watch_containers(s.chaincode)       # not plan.containers: that names the stopped E0-0 'caliper' container
        handles = [("metrics", rec.spawn("metrics_during", argv_scrape(row, ctx, "during", stop_m), env), stop_m),
                   ("resources", rec.spawn("resources", argv_resources(row, ctx, containers, stop_r), env), stop_r),
                   ("host", rec.spawn("host", argv_host(row, ctx, stop_h), env), stop_h)]
        be.sleep(3.0)
        for name, proc, _ in handles:
            if proc.poll() is not None:
                raise StepFailed(name, f"background sampler exited early with {proc.poll()}")
        info_b = do_getinfo(rec, net, row.channel, penv, "getinfo_before")
        cfg_b = do_fetch_config(rec, net, row.channel, penv, "config_before", rd)
        report_default, report_run = ctx.caliper_ws / "report.html", ctx.caliper_ws / report_rel(row.run_id)
        facts["report"] = {"default_path": str(report_default), "default_sha256_before": sha256_file(report_default), "per_run_path": str(report_run)}
        cal = run_caliper(s, rec, plan.raw, env)
        facts["report"].update(default_sha256_after=sha256_file(report_default), per_run_sha256=sha256_file(report_run))
        facts["caliper"], facts["aborted"] = cal, cal["aborted"]
        if cal["aborted"]:
            raise StepFailed("caliper", cal["aborted"], "abort")
        if cal["exit_code"] != 0:
            raise StepFailed("caliper", f"exit {cal['exit_code']}")
        info_a = do_getinfo(rec, net, row.channel, penv, "getinfo_after")
        cfg_a = do_fetch_config(rec, net, row.channel, penv, "config_after", rd)
        want = {"max_message_count": row.B, "batch_timeout_ms": cr.tau_to_ms(row.tau), "preferred_max_bytes": row.P}
        same = cfg_a["block_sha256"] == cfg_b["block_sha256"] and cfg_a["active_config"] == cfg_b["active_config"]
        requested = all(cfg_b["active_config"].get(k) == v for k, v in want.items())
        write_json_new(rd / "config_check.json", {
            "unchanged": bool(same and requested), "reason": "" if same and requested else
            f"hash/active-config equal: {same}; equals request: {requested}",
            "before": {"height": info_b.get("height"), **cfg_b}, "after": {"height": info_a.get("height"), **cfg_a},
            "requested": want})
        facts["stop_background"] = stop_background(be, rec, handles)
        handles = []
        r = rec.run("metrics_after", argv_scrape(row, ctx, "after"), env, thr.tool_timeout_s)
        if r.code != 0:
            raise StepFailed("metrics_after", f"exit {r.code}", "infra")
        r = rec.run("metrics_summary", argv_summarize(row, ctx), env, thr.tool_timeout_s)
        if r.code != 0:
            raise StepFailed("metrics_summary", f"exit {r.code}", "data")
        height = int(info_a.get("height", 0) or 0)
        r = rec.run("block_fetch", argv_fetch(row, ctx), env,
                    max(thr.fetch_timeout_base_s, 120 + thr.fetch_timeout_per_block_s * height))
        if r.code != 0:
            raise StepFailed("block_fetch", f"exit {r.code}", "data")
        sub = recorder_base(row, ctx, "measured")
        sub = sub.with_name(sub.name + ".submits.csv")
        try:
            expected_n = sum(int(x["count"]) for x in _read_csv(sub))
        except (OSError, KeyError, ValueError) as exc:
            raise StepFailed("analysis", f"cannot read submit counts: {exc!r}", "data") from exc
        r = rec.run("analysis", argv_analysis(row, ctx, expected_n), env, thr.tool_timeout_s)
        if r.code != 0:
            raise StepFailed("analysis", f"exit {r.code}", "data")
    except StepFailed as exc:
        facts["step_failure"] = {"label": exc.label, "detail": exc.detail, "category": exc.category}
        s.log(f"{row.run_id}: step failed: {exc}")
    finally:
        if handles:
            try:
                facts["stop_background"] = stop_background(be, rec, handles)
            except Exception as exc:                                                       # noqa: BLE001
                facts["stop_background"] = f"error {exc!r}"
        facts["host"]["post"] = host_snapshot(be, s.helper_pid)
        facts["finished_utc"] = be.now_utc()
        rec.close()
        write_json_new(rd / "facts.json", facts)
    v = evaluate_run(rd, row, ctx.recorder_dir(row.run_id), facts, thr)
    sf = facts.get("step_failure")
    if sf:
        v["step_failure"] = sf
        if v["verdict"] not in ("ABORTED_INFRA", "INVALID_INFRA") and sf["category"] in ("infra", "abort"):
            v["verdict"] = "ABORTED_INFRA" if sf["category"] == "abort" else "INVALID_INFRA"
    write_json_new(rd / "run_verdict.json", v)
    return v


def precheck(s: Session, row: Row) -> list[tuple[str, bool, str]]:
    """Safety checks before each run (plan section 6)."""
    ctx, be, thr = s.ctx, s.be, s.thr
    out: list[tuple[str, bool, str]] = []
    out.append(("matrix file hash equals the frozen hash", sha256_file(ctx.matrix) == MATRIX_SHA256, str(ctx.matrix)))
    h = health(be)
    out.append(("five /healthz == 200", all(v == 200 for v in h.values()), json.dumps(h)))
    ins = inspect_all(be, list(s.baseline))
    ev = container_events(ins, s.baseline) if ins else ["docker inspect failed"]
    out.append(("no container restarted, replaced or exited since session start", not ev, "; ".join(ev) or "unchanged"))
    free = be.disk_free_gb("/mnt/c")
    out.append((f"C: free >= {thr.disk_free_gb_hard:.0f} GB", free >= thr.disk_free_gb_hard, f"{free:.1f} GB"))
    wsl = be.wsl_mem_available_mb()
    out.append((f"WSL MemAvailable >= {thr.wsl_avail_mb_run:.0f} MB", wsl >= thr.wsl_avail_mb_run, f"{wsl:.0f} MB"))
    mem = be.host_memory(2, 1)
    wa = mem["available_mb"][-1] if mem else None
    out.append((f"Windows available memory >= {thr.win_avail_mb_run:.0f} MB", wa is not None and wa >= thr.win_avail_mb_run, f"{wa} MB"))
    pw = be.host_power(s.helper_pid)
    out.append(("AC power online", bool(pw) and pw.get("line") == "Online", json.dumps(pw)))
    # A4: the keep-awake request and the continuous clock probe must still be there; a helper that died mid-series is a halt, not a silent loss
    out.append((f"keep-awake helper alive (pid {s.helper_pid})", bool(s.helper_pid) and bool(pw) and pw.get("helper_alive") is True, json.dumps(pw)))
    if s.probe is not None:
        out.append(("continuous clock probe running", s.probe.poll() is None, f"exit {s.probe.poll()}"))
    absent = not ctx.rd(row.run_id).exists() and not (ctx.caliper_ws / "runs" / row.run_id).exists()
    out.append(("run output folders absent", absent, row.run_id))
    return out


def ramp_blocked(s: Session, rows: list[Row], row: Row) -> str | None:
    """P1b ramp rule. A rung runs only if (a) its rate is within the owner-approved cap, and (b) the previous rung of the SAME setting ran, was VALID
    and is HEALTHY under the corrected classification; the first rung of a setting continues from its P1a anchor run, whose class comes from the
    pinned erratum table."""
    if s.max_rate is not None and row.rate > s.max_rate:
        return f"rate {row.rate} tx/s is above the approved cap max_rate_tps={s.max_rate}"
    anchor = row.anchor()
    if anchor is not None:
        run_id = anchor[0]
        cls = s.anchor_classes.get(run_id)
        if cls is None:
            return f"anchor {run_id} has no entry in the pinned erratum table"
        return None if cls == ccv2.HEALTHY else f"anchor {run_id} is {cls} in the erratum table"
    prev_cell = row.prev_cell()
    prev = next((x for x in rows if x.cell == prev_cell), None) if prev_cell else None
    if prev is None:
        return f"{row.run_id} has no ramp precondition"
    v = s.verdicts.get(prev.run_id)
    if v is None:
        return f"{prev.run_id} has no verdict"
    if v["verdict"] == "SKIPPED_PRECONDITION":
        return f"{prev.run_id} was skipped"
    if not ccv2.ramp_may_continue(v["verdict"], v.get("class_v2")):
        return f"{prev.run_id} is {v['verdict']} / {v.get('class_v2')} (labels {v.get('labels')})"
    return None


def run_series(s: Session, rows: list[Row], *, only: set[str] | None = None, from_seq: int = 1) -> list[dict]:
    results: list[dict] = []
    consecutive = 0
    for row in rows:
        if row.seq < from_seq or (only is not None and row.run_id not in only):
            continue
        why = ramp_blocked(s, rows, row)
        if why:
            v = {"run_id": row.run_id, "channel": row.channel, "verdict": "SKIPPED_PRECONDITION", "labels": [], "reason": why}
            s.verdicts[row.run_id] = v
            results.append(v)
            write_json_new(s.ctx.root / "skipped" / f"{row.run_id}.json", v)
            s.log(f"{row.run_id}: skipped, ramp precondition: {why}")
            continue
        checks = precheck(s, row)
        for name, ok, detail in checks:
            s.log(f"{row.run_id}: precheck {'PASS' if ok else 'FAIL'} {name}: {detail}")
        if not all(ok for _, ok, _ in checks):
            raise HaltSeries(f"{row.run_id}: pre-run safety check failed: " + "; ".join(n for n, ok, _ in checks if not ok))
        s.log(f"{row.run_id}: START {row.channel} B={row.B} rate={row.rate} (seq {row.seq})")
        try:
            v = execute_run(s, row)
        except Exception as exc:                                                           # noqa: BLE001
            v = {"run_id": row.run_id, "channel": row.channel, "verdict": "DRIVER_ERROR", "labels": [], "error": repr(exc)}
            s.verdicts[row.run_id] = v
            results.append(v)
            raise HaltSeries(f"{row.run_id}: unexpected driver error {exc!r}") from exc
        s.verdicts[row.run_id] = v
        results.append(v)
        s.log(f"{row.run_id}: END verdict {v['verdict']} labels {v.get('labels')} failed gates {v.get('failed_gates')}")
        if v["verdict"] in HALT_VERDICTS:
            raise HaltSeries(f"{row.run_id}: {v['verdict']}")
        consecutive = consecutive + 1 if v["verdict"].startswith("INVALID") else 0
        if consecutive >= s.thr.consecutive_invalid_stop:
            raise HaltSeries(f"{consecutive} consecutive INVALID runs (driver stop rule)")
        if row.seq == 1 and v["verdict"] != "VALID":
            raise HaltSeries(f"first run {row.run_id} is {v['verdict']}: halting before spending more runs")
    return results


# ---------------------------------------------------------------------------------------------- preflight gates S1-S11
def _drift_lines(text: str) -> list[str]:
    out = []
    for ln in text.splitlines():
        parts = ln.split()
        if len(parts) >= 2 and parts[0].startswith("SOURCE_"):
            out.append(parts[1])
    return out


def gate_s1(ctx: Ctx, be: Backend, evidence: Path | None = None) -> GateResult:
    rule = ("archive_evidence.py verify --check-sources: every archived hash matches; source drift limited to the "
            f"owner-authorised repair and new files {sorted(S1_ALLOWED_DRIFT)}")
    if evidence is not None:
        text = Path(evidence).read_text(encoding="utf-8", errors="replace")
        how = f"evidence file {evidence}"
    else:
        r = be.run([ctx.win_python, ctx.win_repo + r"\scripts\research_audit\archive_evidence.py", "verify",
                    "--snapshot", ctx.win_snapshot, "--check-sources"], None, 3600)
        text, how = (r.stdout + b"\n" + r.stderr).decode("utf-8", "replace"), f"executed, exit {r.code}"
    m = re.search(r"checked=(\d+)\s+mismatched=(\d+)\s+missing=(\d+)\s+unlisted=(\d+)\s+source_drift=(\d+)", text)
    if not m:
        tail = " | ".join(x.strip() for x in text.strip().splitlines()[-3:])[:300]
        return GateResult("S1", "post-P1a snapshot intact", "FAIL", rule, f"no verify summary line ({how}); output tail: {tail or 'none'}")
    checked, mism, miss, unl, drift_n = (int(x) for x in m.groups())
    drift = sorted(set(_drift_lines(text)))
    extra = [d for d in drift if d not in S1_ALLOWED_DRIFT]
    ok = mism == miss == unl == 0 and checked > 0 and not extra
    return GateResult("S1", "post-P1a snapshot intact", "PASS" if ok else "FAIL", rule,
                      f"checked {checked}, mismatched {mism}, missing {miss}, unlisted {unl}, source drift {drift_n}; "
                      f"unexpected drift {extra}", {"drift": drift, "how": how})


def gate_s2(be: Backend, helper_pid: int | None) -> GateResult:
    rule = "AC online and a process-scoped keep-awake request alive (SetThreadExecutionState, no power-plan change)"
    p = be.host_power(helper_pid)
    ok = bool(p) and p.get("line") == "Online" and helper_pid is not None and p.get("helper_alive") is True
    return GateResult("S2", "host power and sleep", "PASS" if ok else "FAIL", rule,
                      f"{json.dumps(p)}; helper pid {helper_pid}", {"power": p})


def gate_s5(ctx: Ctx, be: Backend) -> tuple[GateResult, dict, list[str]]:
    rule = "every orderer, peer and the Caliper container running; five /healthz == 200; images 2.5.16 / 0.6.0; baseline recorded"
    names = watch_containers()
    ins = inspect_all(be, names)
    problems = []
    for n in names:
        c = ins.get(n)
        if c is None or not c["running"]:
            problems.append(f"{n} not running")
            continue
        want = ":0.6.0" if n == CALIPER else ":2.5.16"
        if not str(c.get("image", "")).endswith(want):
            problems.append(f"{n} image {c.get('image')} does not end with {want}")
    h = health(be)
    problems += [f"{k} healthz {v}" for k, v in h.items() if v != 200]
    cc = chaincode_containers(be)
    baseline = inspect_all(be, names + cc)
    if not baseline:
        problems.append("baseline inspect failed")
    return GateResult("S5", "containers and health", "FAIL" if problems else "PASS", rule,
                      "; ".join(problems) or f"{len(names)} containers running, {len(cc)} chaincode containers, health all 200",
                      {"restart_counts": {k: v["restart_count"] for k, v in baseline.items()}, "health": h,
                       "chaincode": cc}), baseline, cc


def gate_s6(ctx: Ctx, be: Backend, net: "cr.Network") -> GateResult:
    rule = "reference channel e1-r002: active_nodes == 3 and cluster_size == 3 on every orderer; the pinned e1asset package installed on both peers"
    problems, ev = [], {}
    for o in net.raw["orderers"]:
        text = be.http_text(o["ops_url"]) or ""
        a, c = prom_value(text, "consensus_etcdraft_active_nodes", "e1-r002"), prom_value(text, "consensus_etcdraft_cluster_size", "e1-r002")
        ev[o["name"]] = {"active_nodes": a, "cluster_size": c}
        if a != 3 or c != 3:
            problems.append(f"{o['name']}: active {a}, cluster {c}")
    pkg = net.raw["chaincode"]["package_id"]
    for p in net.raw["peers"]:
        r = be.run([net.tool("peer"), "lifecycle", "chaincode", "queryinstalled"], tool_env(ctx, {**os.environ, **net.peer_env(p)}), 60)
        has = pkg in (r.stdout + r.stderr).decode("utf-8", "replace")
        ev[p["name"]] = {"package_installed": has}
        if r.code != 0 or not has:
            problems.append(f"{p['name']}: package {pkg[:24]}... not confirmed installed (exit {r.code})")
    return GateResult("S6", "Raft and chaincode state", "FAIL" if problems else "PASS", rule, "; ".join(problems) or "3/3 on all orderers; package installed on both peers", ev)


def fingerprint(ctx: Ctx, be: Backend, net: "cr.Network", out_dir: Path) -> dict:
    """Read-only: height, current block hash and config-block SHA-256 of the two reference channels."""
    out_dir.mkdir(parents=True, exist_ok=True)
    penv, fp = peer_env(ctx, net), {}
    for ch in FINGERPRINT_CHANNELS:
        r = be.run(argv_getinfo(net, ch), penv, 30)
        info = parse_getinfo((r.stdout + b"\n" + r.stderr).decode("utf-8", "replace")) or {}
        pb = out_dir / f"fp_{ch}_{int(time.time())}.pb"
        r2 = be.run(argv_fetch_config(net, ch, pb), penv, 30)
        fp[ch] = {"height": info.get("height"), "currentBlockHash": info.get("currentBlockHash"),
                  "config_block_sha256": sha256_file(pb) if r2.code == 0 else None}
    return fp


def compare_fingerprints(a: dict, b: dict) -> list[str]:
    diffs = []
    for ch in FINGERPRINT_CHANNELS:
        for k in ("height", "currentBlockHash", "config_block_sha256"):
            if a.get(ch, {}).get(k) != b.get(ch, {}).get(k):
                diffs.append(f"{ch}.{k}: {a.get(ch, {}).get(k)} -> {b.get(ch, {}).get(k)}")
    return diffs


def gate_s7(ctx: Ctx, be: Backend, net: "cr.Network", out_dir: Path) -> tuple[GateResult, dict]:
    rule = "read-only fingerprint (height, current block hash, config-block SHA-256) of e1-r002 and e1-cfg001 recorded; compared again at the end of the series"
    fp = fingerprint(ctx, be, net, out_dir / "fingerprint_pre")
    ok = all(fp[ch].get("height") is not None and fp[ch].get("config_block_sha256") for ch in FINGERPRINT_CHANNELS)
    return GateResult("S7", "existing-channel fingerprint", "PASS" if ok else "FAIL", rule, json.dumps(fp), fp), fp


def gate_s8(ctx: Ctx, be: Backend, net: "cr.Network", rows: list[Row]) -> GateResult:
    rule = f"osnadmin channel list on all three orderers: none of the {len(rows)} matrix channel names present"
    names, problems = {}, []
    for o in net.raw["orderers"]:
        argv = [net.tool("osnadmin"), "channel", "list", "-o", o["admin_address"], "--ca-file", net.raw["orderer_ca"],
                "--client-cert", o["admin_client_cert"], "--client-key", o["admin_client_key"]]
        r = be.run(argv, tool_env(ctx), 60)
        status, body = cr.parse_osnadmin((r.stdout + b"\n" + r.stderr).decode("utf-8", "replace"))
        if status != 200 or body is None:
            problems.append(f"{o['name']}: osnadmin list status {status}")
            continue
        listed = sorted(c.get("name") for c in (body.get("channels") or []))
        names[o["name"]] = listed
        clash = sorted(set(listed) & {r_.channel for r_ in rows})
        if clash:
            problems.append(f"{o['name']}: channels already exist: {clash}")
    return GateResult("S8", "no name collision", "FAIL" if problems else "PASS", rule,
                      "; ".join(problems) or f"{len(names)} orderers listed, channel counts {[len(v) for v in names.values()]}", names)


def gate_s9(ctx: Ctx, be: Backend, clock_dir: Path | None, thr: Thresholds = THR) -> GateResult:
    rule = ("30-minute probe (A2): e1_clock_gate verdict PASS with default thresholds; probe duration >= 1800 s; same WSL boot_id as now; "
            f"probe ended within {thr.clock_probe_max_age_h:.0f} h")
    if clock_dir is None:
        return GateResult("S9", "clock pre-flight", "FAIL", rule, "no --clock-dir given")
    clock_dir = Path(clock_dir)
    man = load_json(clock_dir / "probe_manifest.json")
    if man is None:
        return GateResult("S9", "clock pre-flight", "FAIL", rule, f"no probe_manifest.json in {clock_dir}")
    gate = load_json(clock_dir / "clock_gate.json")
    if gate is None:
        r = be.run([ctx.py, "-B", ctx.tool("e1_clock_gate.py"), "--in-dir", str(clock_dir)], None, 600)
        gate = load_json(clock_dir / "clock_gate.json")
        if gate is None:
            return GateResult("S9", "clock pre-flight", "FAIL", rule, f"gate did not write clock_gate.json (exit {r.code})")
    dur = float(man.get("parameters", {}).get("duration_s", 0))
    end_ns = int(man.get("end", {}).get("real", 0))
    age_h = (time.time_ns() - end_ns) / 3.6e12 if end_ns else None
    checks = {"gate_verdict_PASS": gate.get("verdict") == "PASS", "duration_ge_1800": dur >= 1800.0,
              "same_boot": bool(man.get("boot_id")) and man.get("boot_id") == be.boot_id(),
              "fresh": age_h is not None and age_h <= thr.clock_probe_max_age_h}
    return GateResult("S9", "clock pre-flight", "PASS" if all(checks.values()) else "FAIL", rule,
                      f"verdict {gate.get('verdict')}, duration {dur:.0f} s, age {age_h if age_h is None else round(age_h, 2)} h, checks {checks}",
                      {"criteria": gate.get("criteria"), "validity": gate.get("validity"), "checks": checks})


INVOKED_TOOLS = ("e1_quicklook_p1b.py", "e1_client_classification_v2.py", "e1_p1b_matrix.py", "e1_host_sampler.py", "e1_channel_run.py", "e1_la2_render.py", "e1_metrics_scraper.py", "e1_resource_sampler.py",
                 "e1_block_fetch.py", "e1_benchmark_analyze.py", "e1_clock_rate_probe.py", "e1_clock_gate.py",
                 "e1_clock_gate_crosscheck.py", "e1_reconcile.py", "e1_ledger_blocks.py", "e1_telemetry_common.py",
                 "e1_channel_config.py", "e1_run_orchestrator.py", "e1_config_actuator.py", "e1_clock_handshake.py",
                 "e1_log_collector.py")
PINNED_OTHER = ("scripts/e1/caliper/e1_per_tx_recorder.js", "fabric-testbed/caliper/workloads/e1_create_asset.js",
                "fabric-testbed/caliper/workloads/e1_workload_core.js")


def tool_hashes(ctx: Ctx, net: "cr.Network") -> dict:
    out = {f"scripts/e1/{n}": sha256_file(ctx.scripts / n) for n in INVOKED_TOOLS}
    out.update({p: sha256_file(ctx.repo / p) for p in PINNED_OTHER})
    out["run_matrix.csv"] = sha256_file(ctx.matrix)
    out["p1a_corrected_classification.csv"] = sha256_file(ctx.erratum_csv)
    out["erratum_E1-QL-ERR-001_signed_document"] = sha256_file(ctx.erratum_doc)          # recorded, not pinned: its text changes when the owner signs section 11
    out["env-e1-caliper-benchmark.json"] = sha256_file(ctx.env)
    out["network-e1-recovery.json"] = sha256_file(ctx.net)
    out["configtx_base"] = sha256_file(Path(net.raw["configtx_base"]))
    return out


def gate_s10(ctx: Ctx, net: "cr.Network", out_dir: Path) -> GateResult:
    rule = f"every script, workload module, config, the matrix and the erratum table hashed; matrix SHA-256 == {MATRIX_SHA256}; erratum table == {ERRATUM_CSV_SHA256}"
    h = tool_hashes(ctx, net)
    missing = sorted(k for k, v in h.items() if v is None)
    bad_matrix = h.get("run_matrix.csv") != MATRIX_SHA256
    bad_erratum = h.get("p1a_corrected_classification.csv") != ERRATUM_CSV_SHA256
    write_json_new(out_dir / "tool_hashes.json", h)
    return GateResult("S10", "tool and plan pinning", "FAIL" if missing or bad_matrix or bad_erratum else "PASS", rule,
                      f"{len(h)} files hashed; missing {missing}; matrix hash ok: {not bad_matrix}; erratum table hash ok: {not bad_erratum}", {"hashes": h})


def gate_s11(ctx: Ctx, rows: list[Row]) -> GateResult:
    rule = "output roots absent: the series root and every per-run Caliper folder (the driver never reuses or overwrites)"
    present = [str(ctx.root)] if ctx.root.exists() else []
    present += [str(ctx.caliper_ws / "runs" / r.run_id) for r in rows if (ctx.caliper_ws / "runs" / r.run_id).exists()]
    return GateResult("S11", "output roots absent", "FAIL" if present else "PASS", rule, f"present: {present}" if present else f"{ctx.root} and {len(rows)} run folders absent")


def run_preflight(ctx: Ctx, be: Backend, rows: list[Row], out_dir: Path, *, clock_dir: Path | None, helper_pid: int | None,
                  gates: set[str] | None = None, s1_evidence: Path | None = None, thr: Thresholds = THR) -> tuple[list[GateResult], dict]:
    out_dir.mkdir(parents=True, exist_ok=False)
    results: list[GateResult] = []
    ctxinfo: dict = {"baseline": {}, "chaincode": [], "fingerprint": None}
    net = load_network(ctx)

    def want(g: str) -> bool:
        return gates is None or g in gates

    def attempt(gid: str, name: str, fn):
        if not want(gid):
            return
        try:
            res = fn()
        except Exception as exc:                                                           # noqa: BLE001
            res = GateResult(gid, name, "FAIL", "gate raised", repr(exc))
        results.append(res)
        print(f"{res.id:<4}{res.status:<5} {res.name}: {res.detail[:200]}", flush=True)

    # Execution order: the idle-host memory reading first, the heavy archive verification (S1, minutes of disk I/O) last.
    # The report is sorted back into S1..S11 order.
    attempt("S3", "host memory headroom", lambda: eval_memory(be.host_memory(thr.mem_window_samples, thr.mem_window_interval_s),
                                                                be.wsl_mem_available_mb(), thr))
    attempt("S2", "host power and sleep", lambda: gate_s2(be, helper_pid))
    attempt("S4", "disk", lambda: eval_disk(be.disk_free_gb("/mnt/c"), thr))

    def s5():
        res, base, cc = gate_s5(ctx, be)
        ctxinfo["baseline"], ctxinfo["chaincode"] = base, cc
        return res
    attempt("S5", "containers and health", s5)
    attempt("S6", "Raft and chaincode state", lambda: gate_s6(ctx, be, net))

    def s7():
        res, fp = gate_s7(ctx, be, net, out_dir)
        ctxinfo["fingerprint"] = fp
        return res
    attempt("S7", "existing-channel fingerprint", s7)
    attempt("S8", "no name collision", lambda: gate_s8(ctx, be, net, rows))
    attempt("S9", "clock pre-flight", lambda: gate_s9(ctx, be, clock_dir, thr))
    attempt("S10", "tool and plan pinning", lambda: gate_s10(ctx, net, out_dir))
    attempt("S11", "output roots absent", lambda: gate_s11(ctx, rows))
    attempt("S1", "post-P1a snapshot intact", lambda: gate_s1(ctx, be, s1_evidence))
    results.sort(key=lambda g: int(g.id[1:]))
    report = {"tool": TOOL_VERSION, "created_utc": utc_now(), "matrix_sha256": MATRIX_SHA256, "boot_id": be.boot_id(),
              "gates": [g.as_dict() for g in results], "all_pass": bool(results) and all(g.status == "PASS" for g in results),
              "evaluated": sorted(g.id for g in results), "context": ctxinfo, "thresholds": THR.__dict__}
    write_json_new(out_dir / "preflight_report.json", report)
    return results, report


# ---------------------------------------------------------------------------------------------- dry-run rendering
def abbr(ctx: Ctx, text: str) -> str:
    pairs = [(str(ctx.root), "$ROOT"), (str(ctx.caliper_ws), "$CALWS"), (str(ctx.net), "$NET"), (str(ctx.env), "$ENV"),
             (str(ctx.repo), "$R"), (str(ctx.e1_home), "$E"), (ctx.py + " ", "python3 ")]
    text = text.replace(" ".join(target_args()), "$T")
    for a, b in sorted(pairs, key=lambda p: -len(p[0])):
        text = text.replace(a, b)
    return text


def run_listing(ctx: Ctx, row: Row, net: "cr.Network", env_doc: dict | None, chaincode_note: str) -> list[tuple[str, str]]:
    rd = ctx.rd(row.run_id)
    containers = watch_containers()
    c_args = [a for c in containers for a in ("--container", c)]
    pen = "peer env = Org1 admin from $NET (CORE_PEER_LOCALMSPID, ADDRESS, TLS_ROOTCERT_FILE, MSPCONFIGPATH, FABRIC_CFG_PATH)"
    cal = "<docker exec argv from the rendered plan.json, container name replaced by " + CALIPER + ">"
    if env_doc is not None:
        cal = shlex.join(caliper_argv(caliper_plan(row, ctx, env_doc)))
    steps = [
        ("01 channel (create, join 3 orderers + 2 peers, approve + commit e1asset on THIS channel, verify effective B/tau/P)", shlex.join(argv_channel(row, ctx))),
        ("02 render (plan.json + Caliper run folder + module copies with hash check)", shlex.join(argv_render(row, ctx))),
        ("03 metrics before (one scrape of 5 endpoints)", shlex.join(argv_scrape(row, ctx, "before"))),
        ("04a metrics during (background, 1 Hz, stopped by file)", shlex.join(argv_scrape(row, ctx, "during", rd / "stop_metrics")) + "  &"),
        ("04b resources (background, 1 Hz Docker Engine API; 6 plan containers" + chaincode_note + ")", shlex.join(argv_resources(row, ctx, containers, rd / "stop_resources")).replace(" ".join(c_args), "$C") + "  &"),
        ("04c host sampler (B3; background, 1 Hz: the WSL VM's /proc files and one typeperf.exe for the Windows counters; "
         f"--max-seconds {EVID.host_max_seconds} guard; stopped by file)", shlex.join(argv_host(row, ctx, rd / "stop_host")) + "  &"),
        ("05 getinfo before (" + pen + ")", shlex.join(argv_getinfo(net, row.channel))),
        ("06 config before", shlex.join(argv_fetch_config(net, row.channel, rd / "config/config_before.pb")) + "  ;  " + shlex.join(argv_decode(net, rd / "config/config_before.pb", rd / "config/config_before.json"))),
        (f"07 CALIPER (B4: its own report at $CALWS/{report_rel(row.run_id)}; hard timeout 300 s; the abort check polls 5 health endpoints and docker inspect every 5 s)", "timeout 300 " + cal),
        ("08 getinfo after", shlex.join(argv_getinfo(net, row.channel))),
        ("09 config after (hash and active configuration must equal 'before' and the request)", shlex.join(argv_fetch_config(net, row.channel, rd / "config/config_after.pb")) + "  ;  " + shlex.join(argv_decode(net, rd / "config/config_after.pb", rd / "config/config_after.json"))),
        ("10 stop samplers (touch stop files; wait up to 120 s each)", f"touch {rd / 'stop_metrics'} {rd / 'stop_resources'} {rd / 'stop_host'}"),
        ("11 metrics after", shlex.join(argv_scrape(row, ctx, "after"))),
        ("12 metrics summary (fresh-channel semantics)", shlex.join(argv_summarize(row, ctx))),
        ("13 block fetch from block 0 (timeout max(300 s, 120 s + 1 s per block))", shlex.join(argv_fetch(row, ctx))),
        ("14 analysis (expected-n = sum of 'count' in the measured round's submits.csv)", shlex.join(argv_analysis(row, ctx, "<sum of count>"))),
        ("15 verdict (offline, from files only)", f"evaluate_run({row.run_id}) -> {rd / 'run_verdict.json'}"),
    ]
    return [(label, abbr(ctx, cmd)) for label, cmd in steps]


def expected_outputs(ctx: Ctx, row: Row) -> list[str]:
    e = expected_submits(row)
    return [
        f"$ROOT/{row.run_id}/channel/channel_run_record.json  status VERIFIED (exit 0)",
        f"$CALWS/runs/{row.run_id}/recorder/{row.run_id}_w0_r0_wu-{row.run_id}.{{csv,submits.csv,manifest.json}}  about {e['warmup']} submitted (warm-up, 30 s)",
        f"$CALWS/runs/{row.run_id}/recorder/{row.run_id}_w0_r1_m-{row.run_id}.{{csv,submits.csv,manifest.json}}  about {e['measured']} submitted (measured, 120 s); pending_at_deactivate reported",
        f"$ROOT/{row.run_id}/{{metrics/before,metrics/during,metrics/after,metrics/summary/metrics_summary.json}}  COMPLETE or COMPLETE_WITH_WARNINGS",
        f"$ROOT/{row.run_id}/resources/{{resource_samples.csv,resource_manifest.json}}  1 Hz, no restart/replace/OOM/exit",
        f"$ROOT/{row.run_id}/host/{{wsl_samples.csv,win_counters.csv,win_raw.tsv,host_anomalies.csv,host_manifest.json}}  >= 95% of the expected 1 Hz samples in the measured window, no hole above 5 s",
        f"$CALWS/{report_rel(row.run_id)}  Caliper's own report (HTML, evidence only: never a throughput source); $CALWS/report.html (the P1a one) unchanged",
        f"$ROOT/{row.run_id}/ledger/ledger/{{ledger_tx.csv,blocks.csv}} + block_fetch_manifest.json  from block 0",
        f"$ROOT/{row.run_id}/analysis/{{summary.json,reconciliation.json,transaction_measurements.csv}}  analysis_verdict VALID",
        f"$ROOT/{row.run_id}/{{config_check.json,facts.json,commands.jsonl,cmd/*,run_verdict.json}}",
        f"expectation (arithmetic, not a result): {expected_regime(row)}",
    ]


GATE_TABLE = [
    ("S1", "post-P1a snapshot intact", f"archive_evidence.py verify --snapshot <second snapshot> --check-sources; drift limited to the {len(S1_ALLOWED_DRIFT)} owner-approved P1b files"),
    ("S2", "host power and sleep", "AC online; process-scoped SetThreadExecutionState request alive (no power-plan change)"),
    ("S3", "host memory headroom", "Windows available >= 1500 MB and commit <= 85% at every sample; Pages/sec median <= 100 over a 60-s idle window; WSL MemAvailable >= 3000 MB"),
    ("S4", "disk", "C: free >= 15 GB to start; hard stop below 10 GB"),
    ("S5", "containers and health", "orderers, peers and Caliper running; five /healthz == 200; images 2.5.16 / 0.6.0; restart baseline recorded"),
    ("S6", "Raft and chaincode state", "e1-r002 active_nodes == 3 and cluster_size == 3; the pinned e1asset package installed on both peers"),
    ("S7", "existing-channel fingerprint", "read-only height, current block hash, config-block SHA-256 of e1-r002 and e1-cfg001; compared at the end of the series"),
    ("S8", "no name collision", "osnadmin channel list on all three orderers; none of the 10 names present"),
    ("S9", "clock pre-flight", "30-min probe, e1_clock_gate PASS (no 1 ms step, <= 100 ppm, no 3 s window > 333 ppm); same boot; fresh"),
    ("S10", "tool and plan pinning", "SHA-256 of every script, workload module, env/network JSON, configtx base, the matrix and the erratum table; both pinned hashes must match"),
    ("S11", "output roots absent", "series root and all 10 Caliper run folders absent; the driver never overwrites"),
]


def build_dry_run(rows: list[Row], ctx: Ctx, net: "cr.Network", env_doc: dict | None, findings: list[str],
                  probes: list[dict] | None, thr: Thresholds = THR, only: set[str] | None = None) -> str:
    L: list[str] = []
    add = L.append
    add(f"# {TOOL_VERSION}: DRY RUN. Nothing below has been executed; this listing was generated without contacting Fabric or Docker.")
    add(f"generated_utc: {utc_now()}")
    add(f"matrix: {ctx.matrix}  sha256 {sha256_file(ctx.matrix)}  (frozen {MATRIX_SHA256})  rows {len(rows)}")
    add("")
    add("## A. Variables")
    for k, v in (("R", ctx.repo), ("E", ctx.e1_home), ("ROOT", ctx.root), ("CALWS", ctx.caliper_ws), ("NET", ctx.net), ("ENV", ctx.env)):
        add(f"{k}={v}")
    add("T=" + " ".join(target_args()))
    add("C=" + " ".join(a for c in watch_containers() for a in ("--container", c)))
    add("PATH=" + ctx.fabric_bin + ":$PATH   PYTHONDONTWRITEBYTECODE=1")
    add("")
    add("## B. Common parameters (every run)")
    add(f"tau (BatchTimeout)={TAU}  P (PreferredMaxBytes)={PREFERRED_MAX_BYTES}  A (AbsoluteMaxBytes)=base configtx value (not passed)  workload=CreateAsset 128 B  controller=fixed-rate  workers=1")
    add(f"rounds: warm-up {WARMUP_S} s (txDuration) then measured {MEASURED_S} s (txDuration), plus the renderer's 10 s settle; only B and rate vary; Fabric 2.5.16; Caliper 0.6.0")
    add("")
    add("## C. Findings for your decision (reported, nothing changed)")
    for f in (findings or []) + static_findings():
        add(f"- {f}")
    add("")
    add("## D. Session pre-flight gates (all must PASS before run 1; any FAIL stops)")
    for gid, name, rule in GATE_TABLE:
        add(f"{gid:<4}{name:<30} {rule}")
    add("Execution order: S3 first (the 60-s idle-host reading), then S2, S4-S11, and S1 last (it re-hashes the snapshot and the live sources, about 30 GB: expect 15-25 minutes); the report is in S1..S11 order.")
    add("")
    add("## E. Per-run safety checks, abort conditions and stop rules")
    add("Before each run: matrix hash equals frozen; five /healthz == 200; no container restarted/replaced/exited since session start (StartedAt, RestartCount, ID); "
        f"C: free >= {thr.disk_free_gb_hard:.0f} GB; WSL MemAvailable >= {thr.wsl_avail_mb_run:.0f} MB; Windows available >= {thr.win_avail_mb_run:.0f} MB; AC online; "
        "keep-awake helper still alive; continuous clock probe still running; run folders absent; ramp precondition satisfied.")
    add(f"During a run: Caliper hard timeout {thr.caliper_timeout_s:.0f} s; every {thr.health_poll_s:.0f} s the five health endpoints and `docker inspect` of {len(watch_containers())} plus the chaincode containers are polled. "
        f"{thr.health_consecutive_fail} consecutive health failures, or any restart/replace/exit/OOM -> ABORT: kill the docker-exec client, then `docker kill {CALIPER}`; run marked ABORTED_INFRA; series halts.")
    add(f"NO live backlog watchdog exists (B2: the recorder writes only when a round ends). The owner accepted bounds-only operation for THIS Tier 1 stage only (decision of 2026-11-13, "
        f"approval-file line watchdog_mode={WATCHDOG_MODE}), on six conditions, each enforced in code and pinned by a test:")
    for i, (cond, how) in enumerate(B2_BOUNDS_CONDITIONS, start=1):
        add(f"  B2.{i} {cond}: {how}.")
    add(f"  Bounds in numbers: rates {ALLOWED_RATES[0]}-{ALLOWED_RATES[-1]} tx/s (cap {TIER1_MAX_RATE_TPS}), {WARMUP_S} s warm-up + {MEASURED_S} s measured, Caliper hard timeout "
        f"{thr.caliper_timeout_s:.0f} s, rung steps of at most 1.5x (the first rung is 10 -> 15 tx/s). HYPOTHESIS (from the P1a CPU fits, not a measurement): no container comes near its "
        "CPU line below 40 tx/s (predicted Caliper CPU about 21 % at 40 tx/s), so an overload at these rates is not expected; the bounds exist for the case the fits are wrong.")
    add("Series halts (no retry, no replacement without owner approval): ABORTED_INFRA, INVALID_INFRA, DRIVER_ERROR, a failed pre-run check, a changed existing-channel fingerprint at the end, or "
        f"{thr.consecutive_invalid_stop} consecutive INVALID_* runs, or the first run not VALID (driver rules added for safety).")
    add("Continuous clock probe (A4): e1_clock_rate_probe collect (50 ms samples, host reference every 30 s) starts with the series into $ROOT/clock_series; if it has exited "
        "3 s later no run starts. That tool writes its manifest only when its own duration ends, so a probe stopped at the end of the series has samples but no manifest and "
        "'analyze' cannot read it. The driver therefore waits "
        f"{thr.clock_flush_margin_s:.0f} s (the tool flushes every 10 s), stops it, and checks the samples itself: REALTIME-MONOTONIC steps above {thr.clock_step_ms:g} ms and sampling holes above "
        f"{thr.clock_gap_max_ms / 1000:g} s inside each run's [started, finished] window -> clock_series_check.json. It excludes nothing and changes no run verdict (V4 and V7 stay with the per-run gates); "
        "anything but PASS makes the exit code 5.")
    add("After each run: gates G-channel, G-config-stable, G-caliper, G-recorder, G-reconcile, G-telemetry, G-ledger, V6-leader, V6-containers, V6-host, V7-clock, V2-pacing, G-failures, "
        "and the three added in v0.2: G-postrun (backlog, transaction validity and failure rate must exist), G-evidence (plan 7.8 (E): every sampler stream has >= "
        f"{EVID.min_ratio:.0%} of the expected 1 Hz samples in the measured window and no hole above {EVID.max_gap_s:g} s; the host record has every required Windows counter), "
        "G-report (B4: the per-run report exists and the Caliper log names its path; the workspace's default report.html is byte-identical before and after the launch); "
        "verdicts VALID / INVALID_INFRA / INVALID_DATA / INVALID_SCHEDULE / ABORTED_INFRA. Host figures are evidence only: no host threshold labels a run, because the project has no basis for one.")
    t2 = ccv2.THR2
    add(f"Labels (corrected rule of erratum E1-QL-ERR-001, post hoc from data): SATURATED = interior VALID rate / offered < {t2.sat_valid_ratio} OR max in-flight backlog > {t2.sat_backlog} "
        f"OR failure rate > {t2.failure_rate_max:.0%}; CLIENT_LIMITED = pacing failed (|A/lambda - 1| > {t2.pacing_ratio_tol} or p99 lateness > {t2.lateness_p99_ms:.0f} ms) OR "
        f"Caliper container mean CPU >= {t2.client_cpu_pct:.0f}%; HEALTHY = neither. The event-loop probe labels nothing (its 10 ms resolution floor): the V2 validity gate "
        f"(p99 <= {t2.event_loop_p99_v2_ms:.0f} ms) stays and decides INVALID_SCHEDULE only. Missing evidence is UNDETERMINED, never HEALTHY. The frozen P1a rule is also computed "
        "and stored (labels_frozen_rule). Attribution is named only for a measured container at >= 80% CPU; otherwise UNATTRIBUTED.")
    add(f"Operationalisations (stated so you can veto them): interior window = measured round minus the first and last {thr.interior_trim_s:.0f} s; VALID = recorder status success AND ledger validation_code 0; "
        "offered = the nominal rate; failure rate = 1 - (VALID ledger transactions / submitted); CPU mean is over the measured round from the 1 Hz samples; lateness = create time vs the k/rate schedule, "
        "start estimated as the smallest offset; V2 ratio = submitted / (rate x 120 s).")
    add("Ramp rule: a row runs only if its rate is within the approved max_rate_tps cap and the previous rung of the SAME setting ran, was VALID and is HEALTHY; the first rung of each "
        "setting continues from its P1a anchor run (ql23 for B=10, ql17 for B=40), which must be HEALTHY in the pinned erratum table; once a rung is skipped every later rung of that "
        "setting is skipped. The other setting's ramp is independent.")
    add("Additions to the plan, for your review: the two peer-launched chaincode containers are added to the sampler and the watchdog (V6 coverage, per the E0-0 remediation recommendation); "
        "config is fetched before AND after (the gate compares them); the driver-level stop rules above.")
    add("")
    if probes:
        add("## F. Tool argument acceptance (each tool's own no-side-effect mode, run in a throwaway directory)")
        for p in probes:
            add(f"[{p['status']}] {p['name']}: {p['detail']}")
        add("")
    add("## G. Run matrix and full command listing")
    add("seq | run_id | channel | kind | B | rate | rep | precondition")
    for r in rows:
        add(f"{r.seq:>3} | {r.run_id} | {r.channel} | {r.kind} | {r.B} | {r.rate} | {r.replicate} | {r.precondition or '-'}")
    add("")
    chaincode_note = " + the 2 chaincode containers present at session start"
    for r in rows:
        if only is not None and r.run_id not in only:
            continue
        add(f"### {r.run_id}  channel {r.channel}  B={r.B}  tau={r.tau}  P={r.P}  rate={r.rate} tx/s  warm-up {r.warmup_s} s + measured {r.measured_s} s  ({r.kind}, replicate {r.replicate})")
        if r.precondition:
            add(f"precondition: {r.precondition}")
        for label, cmd in run_listing(ctx, r, net, env_doc, chaincode_note):
            add(f"  {label}")
            add(f"      {cmd}")
        add("  expected outputs:")
        for line in expected_outputs(ctx, r):
            add(f"      {line}")
        add("")
    add("## H. After the series")
    add("end-of-series fingerprint of e1-r002 and e1-cfg001 compared with S7 (fingerprint_compare.json); clock_series_check.json (continuous probe: no REALTIME-MONOTONIC step above 1 ms "
        "and no sampling hole above 2 s inside any run window); series_summary.json; then a THIRD hash-verified archive snapshot of $ROOT and the Caliper run folders (a separate go), then the "
        "Gate H note. Nothing is committed or staged. Tier 2 is not part of this series and is not authorised.")
    add("")
    add("## I. Instrumentation, blockers and interlocks (`run` is refused while any entry of BLOCKERS_OPEN exists)")
    add(f"- BLOCKERS_OPEN: {len(BLOCKERS_OPEN)} open" + (": " + "; ".join(BLOCKERS_OPEN) if BLOCKERS_OPEN else " (B3 and B4 are implemented and tested in v0.2)"))
    add("- B3 host sampler: IMPLEMENTED (scripts/e1/e1_host_sampler.py; Fabric-free tests; real smoke runs on this host recorded in the prep folder). It cannot see the Docker bridge network "
        "(the docker-desktop distribution's network namespace is not visible from Ubuntu); in-bridge traffic stays with the per-container counters of the resource sampler.")
    add("- B4 per-run Caliper report path: IMPLEMENTED (--caliper-report-path runs/<run id>/report.html). Source-supported (caliper-core v0.6.0 Config.js:41 and :239, report-builder.js:347-358, "
        "caliper-utils.js:244-254) and consistent with real P1a output ('Generated report with path /hyperledger/caliper/workspace/report.html'); NOT confirmed against the container's own CLI "
        "(no Docker was used). If the flag were ignored, G-report fails on the first run and the series halts after that one run.")
    add("- B2 live backlog watchdog: NOT implemented; accepted for Tier 1 only on the six conditions of section E. Blocked for any higher rate: " + "; ".join(TIER2_BLOCKERS) + ".")
    add("- Erratum interlock: `run` refuses unless section 11 of the erratum document is signed and names the pinned derived-table hash; the approval file must name tier=1 and "
        f"watchdog_mode={WATCHDOG_MODE}; a cap above {TIER1_MAX_RATE_TPS} tx/s is refused.")
    add("- Already collected in every run (kept as is): Docker Engine API stats at 1 Hz for the five Fabric containers, Caliper and the chaincode containers (CPU, memory, network, block I/O, pids); "
        "Prometheus scrapes at 1 Hz of the five operations endpoints (stage histograms: endorsement, broadcast, Raft persist, WAL fsync, block validation and commit, queue lengths, block fill duration); "
        "per-transaction create/final times and submit events from the recorder; the continuous clock probe; before/after host memory, power and disk readings.")
    add("- Optional, needs a separate owner decision because it changes a pinned tool: Node event-loop UTILISATION in the recorder (eventLoopUtilization), not the 10 ms delay probe.")
    return "\n".join(L) + "\n"


def probe_tools(ctx: Ctx, rows: list[Row], net: "cr.Network", env_doc: dict | None) -> list[dict]:
    """Prove that the exact arguments the engine will use are accepted, using only each tool's own no-side-effect mode."""
    import dataclasses
    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="ql_probe_"))
    pctx = dataclasses.replace(ctx, root=tmp / "root")
    row = rows[0]
    out: list[dict] = []

    def rec(name: str, status: str, detail: str) -> None:
        out.append({"name": name, "status": status, "detail": detail})

    be = Backend()
    env = tool_env(ctx)
    cases = [("e1_channel_run.py (no --execute)", [a for a in argv_channel(row, pctx) if a != "--execute"], "dry run: nothing executed"),
             ("e1_metrics_scraper.py scrape before (no --execute)", [a for a in argv_scrape(row, pctx, "before") if a != "--execute"], "dry run"),
             ("e1_metrics_scraper.py scrape during (no --execute)", [a for a in argv_scrape(row, pctx, "during", pctx.rd(row.run_id) / "stop") if a != "--execute"], "dry run"),
             ("e1_resource_sampler.py sample (no --execute)", [a for a in argv_resources(row, pctx, watch_containers(["dev-peer-example"]), pctx.rd(row.run_id) / "stop") if a != "--execute"], "dry run"),
             ("e1_host_sampler.py sample (no --execute)", [a for a in argv_host(row, pctx, pctx.rd(row.run_id) / "stop") if a != "--execute"], "dry run"),
             ("e1_block_fetch.py fetch (no --execute)", [a for a in argv_fetch(row, pctx) if a != "--execute"], "dry run")]
    for name, argv, needle in cases:
        r = be.run(argv, env, 60)
        text = (r.stdout + b"\n" + r.stderr).decode("utf-8", "replace")
        ok = r.code == 0 and needle in text
        rec(name, "OK" if ok else "FAIL", f"exit {r.code}; " + (text.strip().splitlines()[-1][:160] if text.strip() else "no output"))
    # tools without a dry-run mode: the arguments must get past argparse (failure must come later, from the missing inputs)
    late = [("e1_metrics_scraper.py summarize", argv_summarize(row, pctx)),
            ("e1_benchmark_analyze.py analyze", argv_analysis(row, pctx, 1200))]
    for name, argv in late:
        r = be.run(argv, env, 60)
        text = (r.stdout + b"\n" + r.stderr).decode("utf-8", "replace")
        argparse_reject = "usage:" in text and "error:" in text
        rec(name, "FAIL" if argparse_reject else "OK",
            ("argparse rejected the arguments: " if argparse_reject else "arguments accepted; stopped later on the missing inputs, as expected: ")
            + (text.strip().splitlines()[-1][:160] if text.strip() else f"exit {r.code}"))
    if env_doc is not None:
        try:
            plan = caliper_plan(row, pctx, env_doc)
            ro.Plan(plan)
            fixed = caliper_argv(plan)
            ok_argv = fixed[4] == CALIPER and plan["caliper"]["argv"][4] == plan["containers"]["caliper"] != CALIPER
            rec("e1_la2_render.run_plan + e1_run_orchestrator.Plan validation", "OK" if ok_argv else "FAIL",
                f"plan validates; the plan names container '{plan['containers']['caliper']}', the driver substitutes '{CALIPER}': "
                + shlex.join(fixed)[:130])
        except Exception as exc:                                                           # noqa: BLE001
            rec("e1_la2_render.run_plan + e1_run_orchestrator.Plan validation", "FAIL", repr(exc))
    left = [str(p) for p in tmp.rglob("*") if p.is_file()]
    rec("throwaway directory", "OK" if not left else "NOTE", f"{tmp}; files created by the probes: {left or 'none'}")
    return out


# ---------------------------------------------------------------------------------------------- continuous clock record (A4)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_to_ns(text: str) -> int:
    d = datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc) - _EPOCH
    return (d.days * 86400 + d.seconds) * 10**9 + d.microseconds * 1000


def ns_to_utc(ns: int) -> str:
    return (_EPOCH + timedelta(microseconds=ns // 1000)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def read_clock_samples(path: Path) -> tuple[list[dict], int]:
    """(i, real_ns, mono_ns) rows of the pinned probe's clock_samples.csv.

    The probe writes its manifest only when its own --duration ends, so a probe stopped by the driver leaves samples
    without one, and possibly a torn last line: dropped and counted, never guessed."""
    need = ("i", "real_ns", "mono_ns")
    rows: list[dict] = []
    torn = 0
    with Path(path).open(newline="", encoding="utf-8") as fh:
        rd = csv.reader(fh)
        header = next(rd, None) or []
        if any(c not in header for c in need):
            raise ValueError(f"{path}: the header lacks one of {need}")
        ix = [header.index(c) for c in need]
        for rec in rd:
            try:
                if len(rec) != len(header):
                    raise ValueError("torn row")
                rows.append({c: int(rec[k]) for c, k in zip(need, ix)})
            except ValueError:
                torn += 1
    return rows, torn


def run_windows(ctx: Ctx, run_ids: list[str]) -> list[dict]:
    """[started, finished] of every run that wrote facts.json: from the first command (channel creation) to the end of the analysis."""
    out = []
    for rid in run_ids:
        f = load_json(ctx.rd(rid) / "facts.json") or {}
        a, b = f.get("started_utc"), f.get("finished_utc")
        if a and b:
            out.append({"run_id": rid, "start_utc": a, "end_utc": b, "start_ns": utc_to_ns(a), "end_ns": utc_to_ns(b)})
    return sorted(out, key=lambda w: w["start_ns"])


def clock_series_check(samples: list[dict], windows: list[dict], *, step_ms: float = THR.clock_step_ms,
                       max_gap_ms: float = THR.clock_gap_max_ms) -> dict:
    """Pure. A4: the continuous probe's samples against every run window.

    Reports REALTIME-MONOTONIC steps above step_ms (the A2 threshold, found by the probe's own find_steps) and sampling holes. It
    excludes nothing and changes no run verdict: the per-run V4/V7 gates decide validity."""
    base = {"step_threshold_ms": step_ms, "max_gap_ms": max_gap_ms, "samples": len(samples), "run_windows": len(windows)}
    if len(samples) < 2:
        return {**base, "verdict": "NO_DATA"}
    steps = crp.find_steps(samples, int(round(step_ms * 1e6)))
    offs = [x["real_ns"] - x["mono_ns"] for x in samples]
    inside, outside = [], []
    for st in steps:
        lo, hi = sorted((st["real_pre_equiv_ns"], st["real_after_ns"]))
        runs = [w["run_id"] for w in windows if lo <= w["end_ns"] and hi >= w["start_ns"]]
        (inside if runs else outside).append({**st, "runs": runs})
    cover = []
    for w in windows:
        seg = [x for x in samples if w["start_ns"] <= x["real_ns"] <= w["end_ns"]]
        gaps = [b["mono_ns"] - a["mono_ns"] for a, b in zip(seg, seg[1:])]
        c = {"run_id": w["run_id"], "samples": len(seg),
             "lead_ms": round((seg[0]["real_ns"] - w["start_ns"]) / 1e6, 1) if seg else None,
             "tail_ms": round((w["end_ns"] - seg[-1]["real_ns"]) / 1e6, 1) if seg else None,
             "max_gap_ms": round(max(gaps) / 1e6, 1) if gaps else None}
        c["covered"] = bool(len(seg) >= 2 and c["lead_ms"] <= max_gap_ms and c["tail_ms"] <= max_gap_ms
                            and (c["max_gap_ms"] or 0.0) <= max_gap_ms)
        cover.append(c)
    verdict = ("STEP_IN_RUN_WINDOW" if inside else "COVERAGE_GAP" if any(not c["covered"] for c in cover)
               else "NO_RUN_WINDOWS" if not windows else "PASS")
    return {**base, "verdict": verdict, "first_sample_utc": ns_to_utc(samples[0]["real_ns"]),
            "last_sample_utc": ns_to_utc(samples[-1]["real_ns"]),
            "span_s": round((samples[-1]["mono_ns"] - samples[0]["mono_ns"]) / 1e9, 1),
            "max_abs_offset_change_ms": round(max(abs(b - a) for a, b in zip(offs, offs[1:])) / 1e6, 4),
            "steps_total": len(steps), "steps_in_run_windows": inside, "steps_outside_run_windows": outside[:20], "windows": cover}


def finish_clock_series(s: Session, probe, run_ids: list[str]) -> dict:
    """Stop the continuous probe cleanly, then check its samples against the run windows. Never raises: the series result stands."""
    be, thr = s.be, s.thr
    out: dict = {"probe_exit_before_stop": probe.poll()}
    try:
        if probe.poll() is None:
            be.sleep(thr.clock_flush_margin_s)            # the probe flushes its sample file every 10 s; nothing of a run window is lost
            out["stopped_utc"] = be.now_utc()
            probe.terminate()
            try:
                probe.wait(timeout=20)
            except Exception:                                                              # noqa: BLE001
                probe.kill()
        out["probe_returncode"] = probe.poll()
        samples, torn = read_clock_samples(s.ctx.root / "clock_series" / "clock_samples.csv")
        out["torn_rows_dropped"] = torn
        out.update(clock_series_check(samples, run_windows(s.ctx, run_ids), step_ms=thr.clock_step_ms, max_gap_ms=thr.clock_gap_max_ms))
        out["note"] = ("probe_manifest.json is absent by design: the pinned tool writes it only when its --duration ends; the continuous "
                       "record is these samples. A4: evidence kept, no observation excluded, run verdicts unchanged.")
    except Exception as exc:                                                              # noqa: BLE001
        out.update(verdict="CHECK_ERROR", error=repr(exc))
    try:
        write_json_new(s.ctx.root / "clock_series_check.json", out)
    except Exception as exc:                                                              # noqa: BLE001
        out["write_error"] = repr(exc)
    return out


# ---------------------------------------------------------------------------------------------- CLI
def is_linux() -> bool:
    return sys.platform.startswith("linux")


def _wslpath_win(be: Backend, p: Path) -> str:
    r = be.run(["wslpath", "-w", str(p)], None, 10)
    return r.stdout.decode().strip() if r.code == 0 else str(p)


def check_approval(path: Path | None) -> str:
    if path is None or not Path(path).is_file():
        raise SystemExit("refused: --approval-file is required and must exist (it is created only after the owner approves P1b)")
    text = Path(path).read_text(encoding="utf-8")
    if f"matrix_sha256={MATRIX_SHA256}" not in text or "approved_by=owner" not in text:
        raise SystemExit("refused: the approval file must contain 'approved_by=owner' and 'matrix_sha256=<frozen hash>'")
    if f"erratum_csv_sha256={ERRATUM_CSV_SHA256}" not in text:
        raise SystemExit("refused: the approval file must name the erratum table: 'erratum_csv_sha256=<pinned hash>'")
    if not re.search(r"^tier=1\s*$", text, re.M):
        raise SystemExit("refused: the approval file must contain the line 'tier=1' (this driver runs Tier 1 only; Tier 2 is not authorised)")
    if not re.search(rf"^watchdog_mode={WATCHDOG_MODE}\s*$", text, re.M):
        raise SystemExit(f"refused: the approval file must contain the line 'watchdog_mode={WATCHDOG_MODE}' (the owner's conditional acceptance of 2026-11-13 of bounds-only "
                         "operation for the bounded Tier 1 stage: there is no live backlog watchdog)")
    approved_max_rate(text)
    return text


def approved_max_rate(text: str) -> int:
    m = re.search(r"^max_rate_tps=(\d+)\s*$", text, re.M)
    if m and int(m.group(1)) > TIER1_MAX_RATE_TPS:
        raise SystemExit(f"refused: max_rate_tps={m.group(1)} is above the Tier 1 bound of {TIER1_MAX_RATE_TPS} tx/s; Tier 2 needs stronger overload monitoring and its own "
                         "authorisation (owner decision of 2026-11-13)")
    if not m or int(m.group(1)) not in ALLOWED_RATES:
        raise SystemExit("refused: the approval file must contain 'max_rate_tps=<one of the matrix rates>' (it caps the highest offered rate that may run)")
    return int(m.group(1))


ERRATUM_ID = "E1-QL-ERR-001"


def erratum_adoption(text: str, expected_csv_sha256: str | None = None) -> dict:
    """Section 11 of the erratum document, read as lines: erratum_id, derived_csv_sha256, adopted_by, adopted_on. A value in angle brackets is a placeholder,
    not a signature; the derived-table hash must be the pinned one, so a signature cannot be attached to a different table."""
    expected = expected_csv_sha256 or ERRATUM_CSV_SHA256

    def val(key: str) -> str | None:
        m = re.search(rf"^{key}=(.*?)\s*$", text, re.M)
        return m.group(1) if m else None
    got = {k: val(k) for k in ("erratum_id", "derived_csv_sha256", "adopted_by", "adopted_on")}
    problems = []
    if got["erratum_id"] != ERRATUM_ID:
        problems.append(f"erratum_id is {got['erratum_id']!r}, not {ERRATUM_ID!r}")
    if got["derived_csv_sha256"] != expected:
        problems.append("derived_csv_sha256 is not the pinned derived-table hash")
    for k in ("adopted_by", "adopted_on"):
        v = got[k]
        if not v or v.startswith("<") or v.endswith(">"):
            problems.append(f"{k} is not filled in")
    return {"signed": not problems, "problems": problems, **got}


def check_erratum_adopted(path: Path) -> dict:
    """Interlock: the owner's adoption of the erratum (a signed section 11) must exist before any run."""
    p = Path(path)
    if not p.is_file():
        raise SystemExit(f"refused: the erratum document {p} was not found (the owner's adoption of {ERRATUM_ID} is required before a run)")
    a = erratum_adoption(p.read_text(encoding="utf-8"))
    if not a["signed"]:
        raise SystemExit(f"refused: section 11 of the erratum document is not signed ({'; '.join(a['problems'])})")
    return a


def load_anchor_classes(path: Path, expected_sha: str | None = None) -> dict:
    """Corrected class of each P1a run from the erratum's derived table; the file hash must equal the pinned one."""
    expected_sha = expected_sha or ERRATUM_CSV_SHA256
    actual = sha256_file(path)
    if actual is None:
        raise MatrixError(f"erratum table {path} not found")
    if actual != expected_sha:
        raise MatrixError(f"erratum table hash {actual} != pinned {expected_sha}")
    return {r["run_id"]: r["corrected_class"] for r in _read_csv(path)}


def cmd_run(args, ctx: Ctx, be: Backend) -> int:
    rows = load_matrix(ctx.matrix)
    approval = check_approval(args.approval_file)
    if not args.execute:
        print("refused: run needs --execute (use dry-run to review)")
        return 2
    if BLOCKERS_OPEN:
        print("refused: open instrumentation blockers: " + "; ".join(BLOCKERS_OPEN))
        return 2
    adoption = check_erratum_adopted(ctx.erratum_doc)
    if not is_linux():
        print("refused: --execute runs the Linux Fabric CLIs: run inside Ubuntu WSL")
        return 2
    series_s = int(len(rows) * 540 + 1200)
    s = Session(ctx, be, max_rate=approved_max_rate(approval), anchor_classes=load_anchor_classes(ctx.erratum_csv))
    out_dir = Path(args.out_dir)
    ctx.root.parent.mkdir(parents=True, exist_ok=True)
    # keep-awake first, so that the S2 gate can see it
    pid_file = out_dir.parent / f"{out_dir.name}_keep_awake.pid"
    helper = be.start_keep_awake(series_s, _wslpath_win(be, pid_file))
    for _ in range(30):
        be.sleep(0.5)
        if pid_file.is_file():
            break
    s.helper_pid = int(pid_file.read_text().strip()) if pid_file.is_file() else None
    results, report = run_preflight(ctx, be, rows, out_dir, clock_dir=args.clock_dir, helper_pid=s.helper_pid, s1_evidence=args.s1_evidence)
    if not report["all_pass"]:
        print("PREFLIGHT FAILED: nothing was started")
        helper.terminate()
        return 3
    ctx.root.mkdir(parents=True, exist_ok=False)
    s.log_path = ctx.root / "series_events.log"
    s.baseline, s.chaincode = report["context"]["baseline"], report["context"]["chaincode"]
    s.log(f"series start; approval: {approval.strip().splitlines()[0] if approval.strip() else ''}")
    write_json_new(ctx.root / "session_manifest.json", {
        "tool": TOOL_VERSION, "argv": sys.argv, "matrix_sha256": MATRIX_SHA256, "started_utc": utc_now(), "boot_id": be.boot_id(),
        "preflight_report": str(out_dir / "preflight_report.json"), "tool_hashes": tool_hashes(ctx, load_network(ctx)),
        "baseline": s.baseline, "chaincode_containers": s.chaincode, "keep_awake_pid": s.helper_pid,
        "thresholds": THR.__dict__, "design_findings": design_findings(rows), "approval_file": str(args.approval_file),
        "approval_text": approval, "tier": 1, "watchdog_mode": WATCHDOG_MODE, "b2_bounds_conditions": [list(c) for c in B2_BOUNDS_CONDITIONS],
        "tier2_blockers": list(TIER2_BLOCKERS), "evidence_thresholds": EVID.__dict__, "erratum_adoption": adoption})
    probe = be.spawn([ctx.py, "-B", ctx.tool("e1_clock_rate_probe.py"), "collect", "--out-dir", str(ctx.root / "clock_series"),
                      "--duration", str(series_s), "--ref-python", ctx.ref_python], tool_env(ctx), ctx.root / "clock_series.stdout", ctx.root / "clock_series.stderr")
    s.probe = probe
    results_list, halted = [], None
    try:
        be.sleep(3.0)
        if probe.poll() is not None:
            raise HaltSeries(f"the continuous clock probe exited at once (exit {probe.poll()}; see clock_series.stderr): no run was started")
        results_list = run_series(s, rows, only=set(args.only.split(",")) if args.only else None)
    except HaltSeries as exc:
        halted = str(exc)
        s.log(f"SERIES HALTED: {halted}")
    finally:
        try:
            net = load_network(ctx)
            fp_post = fingerprint(ctx, be, net, ctx.root / "fingerprint_post")
            diffs = compare_fingerprints(report["context"]["fingerprint"] or {}, fp_post)
            write_json_new(ctx.root / "fingerprint_compare.json", {"post": fp_post, "differences": diffs})
        except Exception as exc:                              # noqa: BLE001 - the probe and the helper below must still be stopped
            diffs = [f"end-of-series fingerprint failed: {exc!r}"]
        clock = finish_clock_series(s, probe, [x.run_id for x in rows])
        s.log(f"clock series check: {clock.get('verdict')} ({clock.get('samples')} samples, "
              f"{len(clock.get('steps_in_run_windows') or [])} steps inside run windows)")
        helper.terminate()
        if s.helper_pid:                                      # the Windows process may outlive the interop launcher
            be.run(["taskkill.exe", "/PID", str(s.helper_pid), "/F"], None, 30)
        write_json_new(ctx.root / "series_summary.json", {
            "finished_utc": utc_now(), "halted": halted, "fingerprint_differences": diffs,
            "clock_series_check": {k: clock.get(k) for k in ("verdict", "samples", "steps_total", "steps_in_run_windows", "error")},
            "runs": [{"run_id": v["run_id"], "verdict": v["verdict"], "labels": v.get("labels"), "class_v2": v.get("class_v2"),
                      "labels_frozen_rule": v.get("labels_frozen_rule"), "failed_gates": v.get("failed_gates")}
                     for v in s.verdicts.values()]})
    print(f"SERIES {'HALTED: ' + halted if halted else 'COMPLETE'}; fingerprint differences: {diffs or 'none'}; "
          f"clock series check: {clock.get('verdict')}")
    return 4 if (halted or diffs) else (0 if clock.get("verdict") == "PASS" else 5)


def main(argv: list[str] | None = None, be: Backend | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "P1b capacity-characterization driver").splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("dry-run")
    d.add_argument("--matrix", type=Path)
    d.add_argument("--out", type=Path, help="write the listing to this new file as well")
    d.add_argument("--probe-tools", action="store_true")
    d.add_argument("--runs", help="comma-separated run ids to list in full (default all)")
    p = sub.add_parser("preflight")
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--clock-dir", type=Path)
    p.add_argument("--keep-awake-pid", type=int)
    p.add_argument("--gates", help="comma-separated subset, e.g. S2,S4")
    p.add_argument("--s1-evidence", type=Path)
    f = sub.add_parser("fingerprint")
    f.add_argument("--out", required=True, type=Path)
    r = sub.add_parser("run")
    r.add_argument("--execute", action="store_true")
    r.add_argument("--approval-file", type=Path)
    r.add_argument("--out-dir", required=True, type=Path)
    r.add_argument("--clock-dir", type=Path)
    r.add_argument("--only")
    r.add_argument("--s1-evidence", type=Path)
    v = sub.add_parser("verdict")
    v.add_argument("--run-dir", required=True, type=Path)
    args = ap.parse_args(argv)
    ctx, be = Ctx.default(), be or Backend()
    try:
        if args.cmd == "verdict":
            rd = args.run_dir
            facts = load_json(rd / "facts.json") or {}
            row = next(x for x in load_matrix(getattr(args, "matrix", None) or ctx.matrix) if x.run_id == rd.name)
            v_ = evaluate_run(rd, row, ctx.recorder_dir(row.run_id), facts)
            print(json.dumps({k: v_[k] for k in ("run_id", "verdict", "labels", "labels_frozen_rule", "class_v2", "failed_gates", "not_evaluated_gates", "metrics")}, indent=2))
            return 0
        rows = load_matrix(args.matrix or ctx.matrix) if args.cmd == "dry-run" else load_matrix(ctx.matrix)
        if args.cmd == "dry-run":
            if args.matrix:
                ctx = __import__("dataclasses").replace(ctx, matrix=args.matrix)
            net = load_network(ctx)
            env_doc = render.load_env(ctx.env)
            probes = probe_tools(ctx, rows, net, env_doc) if args.probe_tools else None
            text = build_dry_run(rows, ctx, net, env_doc, design_findings(rows), probes,
                                 only=set(args.runs.split(",")) if args.runs else None)
            print(text)
            if args.out:
                args.out.parent.mkdir(parents=True, exist_ok=True)
                with args.out.open("x", encoding="utf-8", newline="\n") as fh:
                    fh.write(text)
                print(f"dry-run listing written to {args.out}  sha256 {sha256_file(args.out)}")
            return 0 if not probes or all(p_["status"] != "FAIL" for p_ in probes) else 1
        if args.cmd == "preflight":
            results, report = run_preflight(ctx, be, rows, args.out_dir, clock_dir=args.clock_dir, helper_pid=args.keep_awake_pid,
                                            gates=set(args.gates.split(",")) if args.gates else None, s1_evidence=args.s1_evidence)
            print(f"PREFLIGHT {'ALL PASS' if report['all_pass'] else 'NOT PASSED'}: " + ", ".join(f"{g.id}={g.status}" for g in results)
                  + f" -> {args.out_dir / 'preflight_report.json'}")
            return 0 if report["all_pass"] else 3
        if args.cmd == "fingerprint":
            fp = fingerprint(ctx, be, load_network(ctx), args.out.parent / (args.out.stem + "_blocks"))
            write_json_new(args.out, fp)
            print(json.dumps(fp, indent=2))
            return 0
        return cmd_run(args, ctx, be)
    except (MatrixError, cr.RequestError, OSError, ValueError, KeyError, StopIteration) as exc:
        print(f"REFUSED: {type(exc).__name__}: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
