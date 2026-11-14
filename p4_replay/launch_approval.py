"""The two owner-written artifacts a launch needs (Phase 2C): the SERIES APPROVAL (what the experiment is) and the ENTRY AUTHORIZATION (this launch may start). OFFLINE.

Nothing is approved or authorised here. This module only decides whether a file is complete, coherent and bound to the exact artifacts it names. Both files live outside the
repository tree and outside every snapshot group, so naming their hashes creates no circularity (see pins.py).

SERIES APPROVAL v2 replaces Phase 2 `series_approval.check_series_approval` for a Phase 2C launch. Reasons, each tied to an owner decision of Phase 2C:
  * the third snapshot is no longer hard-wired (pins.py);                                                             -> Package A, problem 3
  * the dry run and the measured series are separate launches with their own approval (`launch_kind`);                  -> Package A, problem 1
  * LP1's 0.6 s magnitude threshold is withdrawn; the key `lp1_mode` can only say DIRECTIONAL_ONLY;                      -> decision D4
  * the law mapping is M3, and the approval cannot validate until the owner records that the M3 quantile convention was reviewed (`m3_convention_review=OWNER_REVIEWED`);   -> D3
  * N1, N3, N4 gate, N5 is an overload indicator, N2 and N6 are diagnostics, all measured against the median lag (replay_classify);   -> Package C
  * DRY_RUN may carry PROPOSED thresholds (every result provisional); MEASURED_SERIES needs APPROVED ones, which the owner can only approve after reviewing the dry run.
  The run-order seed is `NOT_USED` for a dry run and the owner's integer for a measured series. No seed is chosen here.

ENTRY AUTHORIZATION binds one launch: its kind and id, the SHA-256 of the series approval, the pins file, the source manifest, the staged schedules, THIS entry module
(so the authorization covers the reviewed code and nothing else), the S1 route, and, for a measured launch, the SHA-256 of the dry-run launch's summary.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from . import driver as dr
from . import launch_ids as li
from . import prereg as pr
from . import replay_classify as rc
from . import series_approval as sa

M3 = "M3_p95ofintervals_1s"
LP4_RESOLUTION = "C1_withdraw_LP4_from_the_A_only_design"
SHA = re.compile(r"^[0-9a-f]{64}$")

STOP_CONDITIONS_V2 = (
    "SC1 any failed pre-flight gate: nothing is started",
    "SC2 no retry, no resume and no replacement: a halted launch stays halted and its identifiers are consumed (a failed measured run is never replaced)",
    "SC3 a measured launch starts only after gate R6 verifies the dry-run launch's recorded evidence: run rp90 classified HEALTHY_REPLAY or HEALTHY_REPLAY_PROVISIONAL, status COMPLETE, "
    "its summary hash named in the authorization, and the baseline snapshot taken after it finished",
    "SC4 ABORTED_INFRA, INVALID_INFRA (a network or configuration failure: container event, health failure, Caliper abort, channel configuration drift) or DRIVER_ERROR in any run: halt",
    "SC5 two consecutive INVALID runs: halt",
    "SC6 a run classified CLOCK_FAILURE, SUBMISSION_FAILURE, INVALID_TRANSACTION or UNDETERMINED_REPLAY: halt",
    "SC7 a run classified CLIENT_LIMITED_REPLAY, SCHEDULER_LIMITED_REPLAY or FIDELITY_LIMITED_REPLAY: halt, unless this is a measured launch and the approval says continue_after_limited_measured=continue",
    "SC8 before every run the per-run checks are repeated (health, container events, memory, disk, power, keep-awake helper, clock probe, run folders absent, staged files); a failure halts",
    "SC9 after every run the continuous clock record is checked over that run's window (steps, coverage, sample density, boot, probe alive); missing or incomplete evidence is CLOCK_FAILURE and halts",
    "SC10 integrity: after every run the reference channels are fingerprinted again; a difference halts the launch before the next run, and the same comparison at the end marks the launch "
    "FAILED_INTEGRITY; no result is claimed from a launch with an integrity failure",
    "SC11 reconciliation: a run whose scheduled, recorded and ledger transactions are not the same set at tx_id level, or that repeats a tx id, is SUBMISSION_FAILURE and halts (SC6); at the end "
    "the whole launch is reconciled and a failure marks it FAILED_INTEGRITY",
    "SC12 the evidence of a failed or halted run is preserved in place: nothing is deleted, overwritten, moved or retried",
    "AN1 (analysis, not a stop) if any B=40 run beats B=10 the law's direction is falsified in this regime: report it",
)


def stop_conditions_sha256_v2() -> str:
    return hashlib.sha256(("\n".join(STOP_CONDITIONS_V2) + "\n").encode("ascii")).hexdigest()


APPROVAL_KEYS_V2 = ("approved_by", "approved_utc", "launch_kind", "prereg_sha256", "window", "dry_run_window", "schedule", "configs", "replicates", "run_order_seed", "law_p95_mapping",
                    "m3_convention_review", "lp1_mode", "lp4_resolution", "thresholds_status", "n1_lag_p99_ms", "n3_missed_ms", "n4_count_tv", "n5_drain_s", "diag_n2_lag_max_ms",
                    "diag_n6_burst_dev", "caliper_timeout_s", "host_max_seconds", "stop_conditions_sha256", "continue_after_limited_measured")
AUTH_KEYS = ("authorized_by", "authorized_utc", "launch_kind", "launch_id", "series_approval_sha256", "pins_sha256", "source_manifest_sha256", "stage_sums_sha256", "entry_sha256",
             "s1_route", "s1_evidence_sha256", "dry_run_summary_sha256")
S1_ROUTES_V2 = ("SNAPSHOT_EXACT", "SIGNED_DRIFT_UPDATE")


class ApprovalRefusedV2(pr.ApprovalRefused):
    pass


@dataclass(frozen=True)
class LaunchApproval:
    kind: str
    thresholds: rc.ReplayThresholds
    caliper_timeout_s: float
    host_max_seconds: float
    continue_after_limited: bool
    run_order_seed: int | None
    mapping: str
    raw: dict
    sha256: str


def _num(kv: dict, key: str, lo: float = 0.0, hi: float | None = None) -> float:
    try:
        v = float(kv[key])
    except ValueError as exc:
        raise ApprovalRefusedV2(f"refused: {key} must be a number") from exc
    if not (v > lo) or (hi is not None and v > hi):
        raise ApprovalRefusedV2(f"refused: {key} must be > {lo}" + (f" and <= {hi}" if hi is not None else ""))
    return v


def check_launch_approval(data: bytes, *, expected_kind: str, prereg_sha256: str) -> LaunchApproval:
    """Validate an owner-written series approval. Raises ApprovalRefusedV2 on any defect. `data` is the file's bytes (its SHA-256 is what the authorization names)."""
    kv = pr.parse_kv(data.decode("utf-8"))
    missing = [k for k in APPROVAL_KEYS_V2 if k not in kv]
    if missing:
        raise ApprovalRefusedV2(f"refused: the approval file lacks {missing}")
    unknown = sorted(set(kv) - set(APPROVAL_KEYS_V2))
    if unknown:
        raise ApprovalRefusedV2(f"refused: unknown keys {unknown}")
    if kv["approved_by"] != "owner":
        raise ApprovalRefusedV2("refused: approved_by must be 'owner'")
    if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", kv["approved_utc"]):
        raise ApprovalRefusedV2("refused: approved_utc must look like 2026-11-15T09:30:00Z")
    kind = kv["launch_kind"]
    if kind != expected_kind or kind not in li.KINDS:
        raise ApprovalRefusedV2(f"refused: this approval is for launch_kind {kind!r}, the launch is {expected_kind!r}")
    if kv["prereg_sha256"].lower() != prereg_sha256.lower():
        raise ApprovalRefusedV2("refused: prereg_sha256 does not equal the hash of the preregistration this build was given")
    if (kv["window"], kv["dry_run_window"], kv["schedule"], kv["configs"], kv["replicates"]) != (dr.MAIN_WINDOW, dr.DRY_RUN_WINDOW, "A", "10,40", "3"):
        raise ApprovalRefusedV2("refused: window F_82800, dry_run_window D_33000, schedule A, configs 10,40, replicates 3 are the only scope")
    if kv["law_p95_mapping"] != M3:
        raise ApprovalRefusedV2(f"refused: law_p95_mapping must be {M3} (owner decision D3)")
    if kv["m3_convention_review"] != "OWNER_REVIEWED":
        raise ApprovalRefusedV2("refused: m3_convention_review must be OWNER_REVIEWED: the M3 quantile convention is read from code and not documented (analysis_primary.M3_CONVENTION), "
                                "and the owner must review it before the preregistration is frozen")
    if kv["lp1_mode"] != "DIRECTIONAL_ONLY":
        raise ApprovalRefusedV2("refused: lp1_mode must be DIRECTIONAL_ONLY (the 0.6 s magnitude threshold was withdrawn, owner decision D4)")
    if kv["lp4_resolution"] != LP4_RESOLUTION:
        raise ApprovalRefusedV2(f"refused: lp4_resolution must be {LP4_RESOLUTION} (LP4 compares arms that this experiment excludes)")
    if kv["thresholds_status"] not in ("PROPOSED", "APPROVED"):
        raise ApprovalRefusedV2("refused: thresholds_status must be PROPOSED or APPROVED")
    if kind == li.MEASURED and kv["thresholds_status"] != "APPROVED":
        raise ApprovalRefusedV2("refused: a measured launch needs APPROVED thresholds (approve them after reviewing the dry run)")
    seed: int | None
    if kind == li.DRY:
        if kv["run_order_seed"] != "NOT_USED":
            raise ApprovalRefusedV2("refused: a dry-run approval carries run_order_seed=NOT_USED (the dry run is never randomised)")
        seed = None
    else:
        if not re.fullmatch(r"\d+", kv["run_order_seed"]):
            raise ApprovalRefusedV2("refused: a measured approval carries the owner's integer run_order_seed")
        seed = int(kv["run_order_seed"])
    base = rc.ReplayThresholds(n1_lag_p99_ms=_num(kv, "n1_lag_p99_ms"), n3_missed_ms=_num(kv, "n3_missed_ms"), n4_count_tv=_num(kv, "n4_count_tv", hi=1.0),
                               n5_drain_s=_num(kv, "n5_drain_s"), n2_lag_max_ms=_num(kv, "diag_n2_lag_max_ms"), n6_burst_dev=_num(kv, "diag_n6_burst_dev", hi=1.0))
    thresholds = rc.approved_replay_thresholds(base, kv["prereg_sha256"].lower()) if kv["thresholds_status"] == "APPROVED" else base
    timeout, guard = _num(kv, "caliper_timeout_s"), _num(kv, "host_max_seconds")
    if timeout < dr.MIN_CALIPER_TIMEOUT_S:
        raise ApprovalRefusedV2(f"refused: caliper_timeout_s {timeout:g} cannot fit a replay (minimum {dr.MIN_CALIPER_TIMEOUT_S:g} s)")
    if guard < timeout + sa.SAMPLER_MARGIN_S:
        raise ApprovalRefusedV2(f"refused: host_max_seconds {guard:g} must outlive the Caliper timeout by {sa.SAMPLER_MARGIN_S:g} s")
    if kv["stop_conditions_sha256"].lower() != stop_conditions_sha256_v2():
        raise ApprovalRefusedV2("refused: stop_conditions_sha256 is not the hash of the STOP_CONDITIONS_V2 text this build enforces")
    if kv["continue_after_limited_measured"] not in sa.CONTINUE_POLICIES:
        raise ApprovalRefusedV2(f"refused: continue_after_limited_measured must be one of {sa.CONTINUE_POLICIES}")
    return LaunchApproval(kind, thresholds, timeout, guard, kv["continue_after_limited_measured"] == "continue", seed, kv["law_p95_mapping"], kv, hashlib.sha256(data).hexdigest())


def example_approval_text(*, kind: str, thresholds_status: str, prereg_sha256: str, seed: int | None = None) -> str:
    """A COMPLETE example in the right format, for tests and for the owner to copy. Every value is a TEST VALUE: nothing is approved by this function, no file is written, and the
    seed is a throw-away (a dry-run example carries NOT_USED)."""
    kv = {"approved_by": "owner", "approved_utc": "2026-11-15T09:30:00Z", "launch_kind": kind, "prereg_sha256": prereg_sha256, "window": "F_82800", "dry_run_window": "D_33000",
          "schedule": "A", "configs": "10,40", "replicates": "3", "run_order_seed": "NOT_USED" if kind == li.DRY else str(7 if seed is None else seed), "law_p95_mapping": M3,
          "m3_convention_review": "OWNER_REVIEWED", "lp1_mode": "DIRECTIONAL_ONLY", "lp4_resolution": LP4_RESOLUTION, "thresholds_status": thresholds_status,
          "n1_lag_p99_ms": "20", "n3_missed_ms": "500", "n4_count_tv": "0.02", "n5_drain_s": "10", "diag_n2_lag_max_ms": "100", "diag_n6_burst_dev": "0.10",
          "caliper_timeout_s": "1200", "host_max_seconds": "1500", "stop_conditions_sha256": stop_conditions_sha256_v2(), "continue_after_limited_measured": "halt"}
    return "\n".join(f"{k}={v}" for k, v in kv.items()) + "\n"


# ----------------------------------------------------------------------------------------------- entry authorization
@dataclass(frozen=True)
class EntryAuthorization:
    kind: str
    launch_id: str
    series_approval_sha256: str
    pins_sha256: str
    source_manifest_sha256: str
    stage_sums_sha256: str
    entry_sha256: str
    s1_route: str
    s1_evidence_sha256: str | None
    dry_run_summary_sha256: str | None
    authorized_utc: str
    raw: dict


def check_entry_authorization(data: bytes) -> EntryAuthorization:
    """Format and internal coherence only. That each named hash equals the artifact on disk is checked by live_entry.verify_entry."""
    kv = pr.parse_kv(data.decode("utf-8"))
    missing = [k for k in AUTH_KEYS if k not in kv]
    if missing:
        raise ApprovalRefusedV2(f"refused: the authorization lacks {missing}")
    unknown = sorted(set(kv) - set(AUTH_KEYS))
    if unknown:
        raise ApprovalRefusedV2(f"refused: unknown keys {unknown}")
    if kv["authorized_by"] != "owner":
        raise ApprovalRefusedV2("refused: authorized_by must be 'owner'")
    if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", kv["authorized_utc"]):
        raise ApprovalRefusedV2("refused: authorized_utc must look like 2026-11-15T09:30:00Z")
    kind = kv["launch_kind"]
    if kind not in li.KINDS:
        raise ApprovalRefusedV2(f"refused: launch_kind must be one of {li.KINDS}")
    try:
        li.check_launch_id(kv["launch_id"], kind)
    except li.LaunchIdError as exc:
        raise ApprovalRefusedV2(f"refused: {exc}") from exc
    for k in ("series_approval_sha256", "pins_sha256", "source_manifest_sha256", "stage_sums_sha256", "entry_sha256"):
        if not SHA.match(kv[k].lower()):
            raise ApprovalRefusedV2(f"refused: {k} must be a SHA-256")
    route = kv["s1_route"]
    if route not in S1_ROUTES_V2:
        raise ApprovalRefusedV2(f"refused: s1_route must be one of {S1_ROUTES_V2}")
    ev = kv["s1_evidence_sha256"]
    if route == "SNAPSHOT_EXACT" and ev != "NONE":
        raise ApprovalRefusedV2("refused: s1_route SNAPSHOT_EXACT allows no drift list: s1_evidence_sha256 must be NONE")
    if route == "SIGNED_DRIFT_UPDATE" and not SHA.match(ev.lower()):
        raise ApprovalRefusedV2("refused: s1_route SIGNED_DRIFT_UPDATE needs the SHA-256 of the signed drift list")
    dry = kv["dry_run_summary_sha256"]
    if kind == li.DRY and dry != "NONE":
        raise ApprovalRefusedV2("refused: a dry-run launch has no earlier dry run: dry_run_summary_sha256 must be NONE")
    if kind == li.MEASURED and not SHA.match(dry.lower()):
        raise ApprovalRefusedV2("refused: a measured launch must name the SHA-256 of the dry-run launch's series_summary.json")
    return EntryAuthorization(kind, kv["launch_id"], kv["series_approval_sha256"].lower(), kv["pins_sha256"].lower(), kv["source_manifest_sha256"].lower(),
                              kv["stage_sums_sha256"].lower(), kv["entry_sha256"].lower(), route, None if ev == "NONE" else ev.lower(), None if dry == "NONE" else dry.lower(),
                              kv["authorized_utc"], kv)


def example_authorization_text(*, kind: str, launch_id: str, **hashes: str) -> str:
    """A COMPLETE example for tests. Hashes are passed in; nothing is authorised by this function."""
    zero = "0" * 64
    kv = {"authorized_by": "owner", "authorized_utc": "2026-11-15T10:00:00Z", "launch_kind": kind, "launch_id": launch_id, "series_approval_sha256": hashes.get("series_approval_sha256", zero),
          "pins_sha256": hashes.get("pins_sha256", zero), "source_manifest_sha256": hashes.get("source_manifest_sha256", zero), "stage_sums_sha256": hashes.get("stage_sums_sha256", zero),
          "entry_sha256": hashes.get("entry_sha256", zero), "s1_route": hashes.get("s1_route", "SNAPSHOT_EXACT"), "s1_evidence_sha256": hashes.get("s1_evidence_sha256", "NONE"),
          "dry_run_summary_sha256": hashes.get("dry_run_summary_sha256", "NONE")}
    return "\n".join(f"{k}={v}" for k, v in kv.items()) + "\n"
