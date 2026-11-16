"""Owner approval for the replay series (Phase 2): the Phase 1 approval plus the values Phase 2 found it could not carry. OFFLINE.

Nothing is approved here. This module only decides whether a file the OWNER wrote is complete, coherent and bound to the exact artefacts it names.

Why a new layer instead of `prereg.check_approval` (a Phase 1 file in the S1 snapshot, so it cannot be edited):
  * `check_approval` demands thresholds_status=APPROVED for EVERY live phase, including DRY_RUN_ONLY. But the P4 draft says the numeric fidelity bands are finalised
    AFTER a dry run, from its lag distribution. Under that rule the dry run could never start. Here a DRY_RUN_ONLY approval may carry PROPOSED thresholds (every result is
    then provisional), and a DRY_RUN_THEN_SERIES approval must carry APPROVED ones.
  * it carries no Caliper timeout, host-sampler guard, LP1 threshold, burst limit, stop conditions, continuation policy, snapshot or staged-schedule binding.
The Phase 1 validation itself is reused by calling `check_approval` on the Phase 1 keys, so the two cannot drift apart.

The owner's eight open decisions are listed in DECISIONS. A series that has no valid approval has all of them unresolved, and the orchestrator refuses to start.
"""

from __future__ import annotations

import dataclasses
import hashlib
from dataclasses import dataclass

from . import classify as cl
from . import driver as dr
from . import fidelity as fd
from . import prereg as pr
from . import snapshot as sn

EXTRA_KEYS = ("snapshot_id", "snapshot_sums_sha256", "stage_sums_sha256", "caliper_timeout_s", "host_max_seconds", "lp1_min_gap_ms", "burst_rel_dev_max",
              "stop_conditions_sha256", "continue_after_limited_measured")
CONTINUE_POLICIES = ("halt", "continue")
SAMPLER_MARGIN_S = 120.0
# law gap of arm A (B=40 minus B=10), per mapping, from the STORED table (P4 draft section 3, copied in prereg.LAW_TABLE_P4_DRAFT): arithmetic only
MAPPING_INDEX = {name: i for i, name in enumerate(pr.MAPPINGS)}
LAW_GAP_A_MS = {m: pr.LAW_TABLE_P4_DRAFT[("A", 40)][i] - pr.LAW_TABLE_P4_DRAFT[("A", 10)][i] for m, i in MAPPING_INDEX.items()}

STOP_CONDITIONS = (
    "SC1 any failed pre-flight gate: nothing is started",
    "SC2 no retry and no resume after a halt",
    "SC3 the dry run rp00 must be recorded and classified HEALTHY_REPLAY (HEALTHY_REPLAY_PROVISIONAL in a DRY_RUN_ONLY phase) before any measured run starts, otherwise halt",
    "SC4 ABORTED_INFRA, INVALID_INFRA or DRIVER_ERROR in any run: halt",
    "SC5 two consecutive INVALID runs: halt",
    "SC6 a measured run classified CLOCK_FAILURE, SUBMISSION_FAILURE, INVALID_TRANSACTION or UNDETERMINED_REPLAY: halt",
    "SC7 a measured run classified CLIENT_LIMITED_REPLAY, SCHEDULER_LIMITED_REPLAY or FIDELITY_LIMITED_REPLAY: halt, unless the approval says continue_after_limited_measured=continue",
    "SC8 before every run after the first the per-run checks are repeated (health, container events, memory, disk, power, keep-awake helper, clock probe, run folders absent); a failure halts",
    "SC9 after every run the continuous clock record is checked over that run's window; a step inside it or a coverage gap labels the run CLOCK_FAILURE and halts",
    "SC10 at the end of a series or a halt the reference channels are fingerprinted again; any difference marks the series FAILED_INTEGRITY and no result is claimed",
    "AN1 (analysis, not a stop) if any B=40 run beats B=10 the law is falsified in this regime: report it",
)


def stop_conditions_text() -> str:
    return "\n".join(STOP_CONDITIONS) + "\n"


def stop_conditions_sha256() -> str:
    return hashlib.sha256(stop_conditions_text().encode("ascii")).hexdigest()


DECISIONS = (
    {"id": "D1", "topic": "run-order seed", "keys": ("run_order_seed",), "proposed": "none: the owner chooses; the order is frozen before run 1 (generator: prereg.run_order, option R2 recommended)"},
    {"id": "D2", "topic": "fidelity thresholds and their empirical justification", "keys": ("rel_lag_p99_ms", "rel_lag_max_ms", "missed_schedule_lag_ms", "per_second_count_tv", "drain_deadline_s", "burst_rel_dev_max", "thresholds_status"),
     "proposed": "placeholders 20 ms, 100 ms, 500 ms, 0.02, 10 s and (new) 0.10 burst deviation, no measured basis; the dry run supplies the basis"},
    {"id": "D3", "topic": "law-to-run p95 mapping", "keys": ("law_p95_mapping",), "proposed": "M3 primary with M1 descriptive (hypotheses, not derivations)"},
    {"id": "D4", "topic": "LP1 threshold under the selected mapping", "keys": ("lp1_min_gap_ms",), "proposed": "about 100 ms if M3 (law gap 164 ms); the draft's 600 ms follows from M1, M2 or M4 only"},
    {"id": "D5", "topic": "dry-run workload", "keys": ("dry_run_window",), "proposed": "D_33000 (6,887 transactions, mean 11.5 tx/s, max 45 tx in a second)"},
    {"id": "D6", "topic": "Caliper hard timeout", "keys": ("caliper_timeout_s",), "proposed": "1,200 s (P1b's 300 s would abort every replay)"},
    {"id": "D7", "topic": "host-sampler guard", "keys": ("host_max_seconds",), "proposed": "1,500 s (P1b's 420 s would stop the sampler mid-run)"},
    {"id": "D8", "topic": "final stop conditions", "keys": ("stop_conditions_sha256", "continue_after_limited_measured"), "proposed": "STOP_CONDITIONS (SC1-SC10), halting on a limited measured run"},
)
ADDITIONAL_DECISIONS = (
    {"id": "A1", "topic": "S1 route for the files Phase 2 adds under p4_replay/ (a snapshot group)", "keys": ("s1_route", "s1_evidence_sha256")},
    {"id": "A2", "topic": "approving thresholds after the dry run (DRY_RUN_ONLY may carry PROPOSED thresholds)", "keys": ("live_phase", "thresholds_status")},
    {"id": "A3", "topic": "drain-deadline failure (N5) is reported as a Fabric overload indicator and does not limit the class", "keys": ()},
    {"id": "A4", "topic": "the replacement keep-awake helper (PowerRequest based) is LIVE_UNTESTED; standby is detected through the clock record, not prevented", "keys": ()},
)


class UnresolvedPreregistration(SystemExit):
    """Raised when a series is asked to start with owner decisions still open."""


@dataclass(frozen=True)
class SeriesApproval:
    approval: pr.Approval
    phase: str
    thresholds: cl.ClassThresholds
    caliper_timeout_s: float
    host_max_seconds: float
    lp1_min_gap_ms: int
    continue_after_limited: bool
    stage_sums_sha256: str
    raw: dict

    @property
    def run_order_seed(self) -> int:
        return self.approval.run_order_seed


def decision_register(approval: SeriesApproval | None) -> list[dict]:
    """Every decision with its status. With no valid approval all are OWNER_DECISION_REQUIRED; a valid one resolves D1-D8 (the file cannot validate without them)."""
    status = "RESOLVED_BY_APPROVAL" if approval is not None else "OWNER_DECISION_REQUIRED"
    rows = [{**d, "status": status} for d in DECISIONS]
    rows += [{**d, "status": "OWNER_DECISION_REQUIRED" if approval is None or d["id"] in ("A3", "A4") else "RESOLVED_BY_APPROVAL"} for d in ADDITIONAL_DECISIONS]
    return rows


def require_resolved(approval: SeriesApproval | None) -> SeriesApproval:
    """The orchestrator's gate: no valid approval, no series."""
    if approval is None:
        open_ids = [d["id"] for d in decision_register(None) if d["id"] in {x["id"] for x in DECISIONS}]
        raise UnresolvedPreregistration(f"refused: no valid series approval; unresolved owner decisions: {open_ids}")
    return approval


def check_series_approval(text: str, *, prereg_sha256: str, stage_sums_sha256: str) -> SeriesApproval:
    """Validate an owner-written approval file. Raises prereg.ApprovalRefused on any defect. `stage_sums_sha256` is the hash of the staged tree's SHA256SUMS.txt
    that the caller computed from disk, so the approval is bound to those exact schedule files."""
    kv = pr.parse_kv(text)
    needed = pr.APPROVAL_KEYS + EXTRA_KEYS
    missing = [k for k in needed if k not in kv]
    if missing:
        raise pr.ApprovalRefused(f"refused: the approval file lacks {missing}")
    unknown = sorted(set(kv) - set(needed))
    if unknown:
        raise pr.ApprovalRefused(f"refused: unknown keys {unknown}")
    phase = kv["live_phase"]
    if phase not in pr.LIVE_PHASES:
        raise pr.ApprovalRefused(f"refused: live_phase must be one of {pr.LIVE_PHASES}")
    dry_only_proposed = phase == "DRY_RUN_ONLY" and kv["thresholds_status"] == "PROPOSED"
    base = {k: kv[k] for k in pr.APPROVAL_KEYS}
    if dry_only_proposed:
        base["thresholds_status"] = "APPROVED"                  # validate the rest with the Phase 1 rules; the status is restored below
    approval = pr.check_approval("\n".join(f"{k}={v}" for k, v in base.items()), prereg_sha256=prereg_sha256)
    if dry_only_proposed:
        approval = dataclasses.replace(approval, thresholds=dataclasses.replace(approval.thresholds, status="PROPOSED", approved_by_prereg_sha256=None))
    if kv["dry_run_window"] != dr.DRY_RUN_WINDOW:
        raise pr.ApprovalRefused(f"refused: dry_run_window must be {dr.DRY_RUN_WINDOW} (the confirmed dry-run workload)")
    if kv["snapshot_id"] != sn.SNAPSHOT_ID or kv["snapshot_sums_sha256"].lower() != sn.SNAPSHOT_SUMS_SHA256:
        raise pr.ApprovalRefused(f"refused: the approval must name snapshot {sn.SNAPSHOT_ID} with SHA256SUMS sha256 {sn.SNAPSHOT_SUMS_SHA256}")
    if kv["stage_sums_sha256"].lower() != stage_sums_sha256.lower():
        raise pr.ApprovalRefused("refused: stage_sums_sha256 does not equal the hash of the staged tree's SHA256SUMS.txt (the approved schedules are not the staged ones)")
    try:
        timeout, guard = float(kv["caliper_timeout_s"]), float(kv["host_max_seconds"])
        burst = float(kv["burst_rel_dev_max"])
        lp1 = int(kv["lp1_min_gap_ms"])
    except ValueError as exc:
        raise pr.ApprovalRefused("refused: caliper_timeout_s, host_max_seconds, burst_rel_dev_max and lp1_min_gap_ms must be numbers") from exc
    if timeout < dr.MIN_CALIPER_TIMEOUT_S:
        raise pr.ApprovalRefused(f"refused: caliper_timeout_s {timeout:g} cannot fit a replay (minimum {dr.MIN_CALIPER_TIMEOUT_S:g} s)")
    if guard < timeout + SAMPLER_MARGIN_S:
        raise pr.ApprovalRefused(f"refused: host_max_seconds {guard:g} must outlive the Caliper timeout by {SAMPLER_MARGIN_S:g} s (>= {timeout + SAMPLER_MARGIN_S:g})")
    if not (0 < burst <= 1):
        raise pr.ApprovalRefused("refused: burst_rel_dev_max must be in (0, 1]")
    gap = LAW_GAP_A_MS[approval.mapping]
    if not (0 < lp1 <= gap):
        raise pr.ApprovalRefused(f"refused: lp1_min_gap_ms {lp1} is not attainable under mapping {approval.mapping}: the stored law gap for arm A is {gap} ms "
                                 "(a threshold the law itself cannot meet is incoherent; the draft's 600 ms follows from M1, M2 or M4 only)")
    if kv["stop_conditions_sha256"].lower() != stop_conditions_sha256():
        raise pr.ApprovalRefused("refused: stop_conditions_sha256 is not the hash of the STOP_CONDITIONS text this build enforces")
    if kv["continue_after_limited_measured"] not in CONTINUE_POLICIES:
        raise pr.ApprovalRefused(f"refused: continue_after_limited_measured must be one of {CONTINUE_POLICIES}")
    if approval.thresholds.status == "APPROVED":
        thresholds = cl.approved_class_thresholds(approval.thresholds, burst)
    else:
        thresholds = cl.ClassThresholds(fidelity=approval.thresholds, burst_rel_dev_max=burst, burst_status="PROPOSED")
    return SeriesApproval(approval, phase, thresholds, timeout, guard, lp1, kv["continue_after_limited_measured"] == "continue", kv["stage_sums_sha256"].lower(), kv)


def example_approval_text(*, phase: str, thresholds_status: str, prereg_sha256: str, stage_sums_sha256: str, seed: int = 0) -> str:
    """A COMPLETE example in the right format, for tests and for the owner to copy. Every value is a TEST VALUE and the seed is a throw-away: nothing is approved
    by this function, and no file is written."""
    kv = {"approved_by": "owner", "approved_utc": "2026-11-15T09:30:00Z", "prereg_sha256": prereg_sha256, "window": "F_82800", "dry_run_window": "D_33000", "schedule": "A",
          "configs": "10,40", "replicates": "3", "run_order_seed": str(seed), "law_p95_mapping": "M3_p95ofintervals_1s", "thresholds_status": thresholds_status,
          "rel_lag_p99_ms": "20", "rel_lag_max_ms": "100", "missed_schedule_lag_ms": "500", "per_second_count_tv": "0.02", "drain_deadline_s": "10",
          "s1_route": "SIGNED_DRIFT_UPDATE", "s1_evidence_sha256": "a" * 64, "lp4_resolution": "C1_withdraw_LP4_from_the_A_only_design", "live_phase": phase,
          "snapshot_id": sn.SNAPSHOT_ID, "snapshot_sums_sha256": sn.SNAPSHOT_SUMS_SHA256, "stage_sums_sha256": stage_sums_sha256, "caliper_timeout_s": "1200",
          "host_max_seconds": "1500", "lp1_min_gap_ms": "100", "burst_rel_dev_max": "0.10", "stop_conditions_sha256": stop_conditions_sha256(),
          "continue_after_limited_measured": "halt"}
    return "\n".join(f"{k}={v}" for k, v in kv.items()) + "\n"
