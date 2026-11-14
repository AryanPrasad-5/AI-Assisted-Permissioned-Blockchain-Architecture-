"""Proposed preregistration fields, the run-order generator, and the owner-approval interlock.

NOTHING in PREREG is approved. Every field carries a status:
  CARRIED_FROZEN             taken unchanged from a frozen artifact (source named)
  PROPOSED                   a value or rule proposed here; the owner may change it
  OWNER_DECISION_REQUIRED    no value is proposed or chosen; the owner must decide before any live step

The law table below is STORED in the P4 draft (D:/PLAN_BLOCK18/flashbots_replay_offline_2026-11-14/P4_LIVE_PROTOCOL_DRAFT.md,
section 3) and is copied here as read. It is not recomputed (regenerating law predictions needs separate authorization).
Everything computed from it is closed-form arithmetic on those stored numbers.
"""

from __future__ import annotations

import itertools
import random
import re
from dataclasses import dataclass

from .fidelity import FidelityThresholds, approved_thresholds

# ----------------------------------------------------------------------------------------------- stored law table (P4 draft section 3)
# law-implied run p95 in ms, mean over the window, per (arm, B); four candidate mappings of the same law
MAPPINGS = {
    "M1_mean_1s": "volume-weighted MEAN of per-interval law p95, 1-s intervals",
    "M2_mean_60s": "volume-weighted MEAN of per-interval law p95, 60-s interval means",
    "M3_p95ofintervals_1s": "volume-weighted 95th PERCENTILE of per-interval law p95, 1-s intervals (the draft's 'interval-quantile proxy')",
    "M4_p95ofintervals_60s": "volume-weighted 95th PERCENTILE of per-interval law p95, 60-s interval means",
}
LAW_TABLE_P4_DRAFT = {   # (arm, B) -> (M1, M2, M3, M4) in ms, as stored
    ("A", 10): (861, 892, 1776, 1082), ("A", 40): (1843, 1940, 1940, 1940),
    ("BO", 10): (869, 879, 1525, 1056), ("BO", 40): (1937, 1940, 1940, 1940),
    ("BF", 10): (907, 916, 1525, 1032), ("BF", 40): (1940, 1940, 1940, 1940),
}
LP1_MIN_GAP_MS = 600          # LP1: B=10 run p95 < B=40 run p95 by at least 0.6 s
LP2_BAND_MS = (1700, 2150)    # LP2: B=40 run p95 in [1.70, 2.15] s
LP3_BAND_MS = (700, 2000)     # LP3: B=10 run p95 in [0.70, 2.00] s


def lp_consistency() -> dict:
    """Arithmetic on the stored table: which mapping each draft prediction is consistent with."""
    names = list(MAPPINGS)
    out: dict = {"LP4_A>=BO>=BF_at_B10": {}, "LP1_gap_B40_minus_B10_ms": {}, "LP1_holds_gap_ge_600": {}, "LP2_in_band": {}, "LP3_in_band": {}}
    for i, name in enumerate(names):
        a, bo, bf = (LAW_TABLE_P4_DRAFT[(arm, 10)][i] for arm in ("A", "BO", "BF"))
        out["LP4_A>=BO>=BF_at_B10"][name] = {"values_A_BO_BF": (a, bo, bf), "holds": a >= bo >= bf}
        gaps = {arm: LAW_TABLE_P4_DRAFT[(arm, 40)][i] - LAW_TABLE_P4_DRAFT[(arm, 10)][i] for arm in ("A", "BO", "BF")}
        out["LP1_gap_B40_minus_B10_ms"][name] = gaps
        out["LP1_holds_gap_ge_600"][name] = all(g >= LP1_MIN_GAP_MS for g in gaps.values())
        out["LP2_in_band"][name] = all(LP2_BAND_MS[0] <= LAW_TABLE_P4_DRAFT[(arm, 40)][i] <= LP2_BAND_MS[1] for arm in ("A", "BO", "BF"))
        out["LP3_in_band"][name] = all(LP3_BAND_MS[0] <= LAW_TABLE_P4_DRAFT[(arm, 10)][i] <= LP3_BAND_MS[1] for arm in ("A", "BO", "BF"))
    return out


# ----------------------------------------------------------------------------------------------- the preregistration fields
def proposed_preregistration() -> dict:
    thr = FidelityThresholds()
    return {
        "window": {"status": "CARRIED_FROZEN", "value": {
            "measured": "F_82800: 2026-10-12, UTC second 82,800 (23:00:00Z), 600 s, 6,229 transactions",
            "pre_roll": "60 s of the real preceding trace (UTC seconds 82,740-82,799), unmeasured, own Caliper round",
            "selection": "registered seeded rule, seed 20261114, 143 tiles, 57 eligible, chosen after the frozen F pass",
            "source": "data/processed/flashbots_ms/windows/F_82800/window_record.json"},
            "dry_run_window": {"status": "OWNER_DECISION_REQUIRED", "value": "D_33000 (21 Sep, 6,887 tx, mean 11.5 tx/s) is what the P4 draft names; the owner must confirm it"}},
        "schedule": {"status": "CARRIED_FROZEN", "value": {
            "arm": "A only: the observed millisecond offsets, 1:1 first-seen transaction -> CreateAsset, native time, no compression",
            "excluded": "BO and BF (synthetic Poisson from rates) conflict with the rule that forecasts inform decisions only; they are not part of Phase 1",
            "dispatch": "absolute ms offsets via Caliper replay-rate; one-second bins are analysis only"}},
        "configuration_arms": {"status": "PROPOSED", "value": {
            "B": [10, 40], "tau": "2s", "P_bytes": 10_485_760, "workers": 1, "payload_bytes": 128, "chaincode": "e1asset CreateAsset",
            "channel": "fresh channel per run, e1-rp<NN>-b<B>-r<rep>", "replicates": 3, "measured_runs": 6, "dry_runs": 1,
            "basis": "P1b Tier 1 pattern (tau, P, one worker, one payload, fresh channel); B=40 is the project's static baseline"}},
        "seeds": {"status": "OWNER_DECISION_REQUIRED", "value": {
            "window_selection_seed": 20261114,
            "window_selection_seed_status": "CARRIED_FROZEN",
            "run_order_seed": "NOT CHOSEN (the P4 draft: 'chosen at authorization')"}},
        "fidelity_thresholds": {"status": "PROPOSED", "value": {
            "structural_criteria": ["scheduled count == dispatched count", "dispatch seq complete, no duplicates", "intended offsets equal the schedule",
                                    "recorder rows == scheduled", "no duplicate tx_id, no repeated tx_id across rounds", "no submission failures",
                                    "ledger endorser tx_ids == recorder tx_ids exactly once", "no invalid transactions", "ledger count == scheduled total",
                                    "clock gate PASS"],
            "numeric_criteria_PROPOSED": thr.as_dict(),
            "basis": "NONE measured: placeholders to be replaced from the dry-run lag distribution (P4 draft section 1: bands finalised after a dry run)",
            "label_on_failure": "FIDELITY_LIMITED (not a Fabric result)", "label_on_pass_under_proposed": "FIDELITY_PASS_PROVISIONAL"}},
        "run_order": {"status": "OWNER_DECISION_REQUIRED", "value": {
            "dry_run": "first, never randomised",
            "measured": "6 runs (B in {10, 40} x 3), seeded order frozen before run 1",
            "options": {
                "R1_strict_alternation": "B10,B40,B10,B40,B10,B40 or the reverse; the seed only picks which; no two consecutive runs share a configuration; weakest randomisation (2 orders)",
                "R2_blocked_seeded_permutation": "seeded permutation of the multiset with at most 2 consecutive runs of one configuration (recommended)",
                "R3_free_seeded_permutation": "any of the 20 arrangements, seeded; can put 3 of one configuration in a row, confounding order with configuration"},
            "recommendation": "R2", "generator": "p4_replay.prereg.run_order(seed, max_consecutive_same)"}},
        "law_to_run_p95_mapping": {"status": "OWNER_DECISION_REQUIRED", "value": {
            "run_p95_definition": "p95 of per-transaction latency over all measured transactions of one run, cohort by INTENDED submission time (P4 draft section 5)",
            "candidate_mappings": MAPPINGS, "stored_law_table_ms": {f"{a},{b}": v for (a, b), v in LAW_TABLE_P4_DRAFT.items()},
            "properties": {
                "M1_M2": "a mean of interval p95 values is not a quantile of the pooled latency distribution",
                "M3_M4": "exact for the pooled p95 only if within-interval latency spread is negligible; otherwise a heuristic",
                "rigorous": "the pooled p95 lies between the smallest and largest interval p95 (true for any mixture); too wide to falsify anything"},
            "recommendation": "register M3 (1-s) as primary and M1 as a descriptive secondary; do NOT claim either as derived, both are hypotheses",
            "consequence": "the choice moves the arm-A, B=10 prediction from 861 ms (M1) to 1,776 ms (M3); LP3's band [0.70, 2.00] s spans every mapping and cannot "
                           "discriminate; and LP1's 0.6 s gap is attainable under M1, M2 and M4 but not under M3 (see lp4_lp1_conflict.proposed_corrections C3)"}},
        "lp4_lp1_conflict": {"status": "OWNER_DECISION_REQUIRED", "value": {
            "stored_table_arithmetic": lp_consistency(),
            "finding": "LP4 (A >= BO >= BF at B=10) holds only for the two quantile-proxy columns (M3, M4). The two mean columns (M1, M2) put BF highest "
                       "(861/869/907 ms and 892/879/916 ms). LP1's 0.6 s gap fails on M3 for every arm (164/415/415 ms). Only M4 satisfies both.",
            "proposed_corrections": {
                "C1_withdraw_LP4_from_the_A_only_design": "LP4 compares A with BO and BF; BO and BF are outside Phase 1, so LP4 cannot be tested. Withdraw it from the "
                                                          "reduced design and keep it as a deferred hypothesis.",
                "C2_restate_against_a_named_column_if_BO_BF_return": "State LP4 as holding under M3/M4 only, and state the ordering M1/M2 predict (A < BO < BF at 1-s "
                                                                     "resolution). The observed ordering then discriminates between the two readings of the law.",
                "C3_set_LP1_gap_from_the_chosen_mapping": "A 0.6 s gap follows from M1, M2 and M4 but not from M3 (law gap for A: 164 ms). If M3 is chosen, a gap near 0.1 s "
                                                          "(164 ms minus about the 40 ms registered law tolerance) is the PROPOSED replacement.",
                "recommendation": "C1 and C3 for the A-only design; C2 only if BO/BF are restored. None of this is applied: the P4 draft is not edited."}}},
        "stop_criteria": {"status": "PROPOSED", "value": {
            "carried_from_p1b_driver": ["any failed pre-flight gate: do not start", "no retry, no resume after a halt",
                                        "first run not VALID: halt before spending more runs", "2 consecutive INVALID runs: halt (driver rule)",
                                        "health failure, container event or Caliper hard timeout: abort and kill the Caliper process"],
            "proposed_additions": ["a FIDELITY_LIMITED dry run: halt before any measured run", "a FIDELITY_LIMITED measured run: finish the series only if the owner pre-approves continuing, otherwise halt",
                                   "the keep-awake literal in the P1b helper is ineffective: re-check the clock gate after any standby (carried, P4 draft section 6.3)"],
            "analysis_not_stop": "if any B=40 run beats B=10, the law is falsified in this regime: report it, do not stop"}},
    }


# ----------------------------------------------------------------------------------------------- run order
def run_order(seed: int, max_consecutive_same: int = 2, configs: tuple = (10, 40), replicates: int = 3) -> list[dict]:
    """Deterministic seeded order of the measured runs. A pure scheduling function: no model, no simulation.
    Enumerates every distinct arrangement of the multiset, keeps those with at most `max_consecutive_same` consecutive runs of one
    configuration, and picks one with random.Random(seed). The owner chooses the seed; no seed is chosen here."""
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise ValueError("seed must be an int")
    base = [c for c in configs for _ in range(replicates)]
    arrangements = sorted(set(itertools.permutations(base)))

    def longest(a):
        return max(len(list(g)) for _, g in itertools.groupby(a))

    allowed = [a for a in arrangements if longest(a) <= max_consecutive_same]
    if not allowed:
        raise ValueError("no arrangement satisfies the consecutive-run limit")
    chosen = allowed[random.Random(seed).randrange(len(allowed))]
    seen: dict[int, int] = {}
    out = []
    for position, config in enumerate(chosen, start=1):
        seen[config] = seen.get(config, 0) + 1
        out.append({"position": position, "B": config, "replicate": seen[config]})
    return out


# ----------------------------------------------------------------------------------------------- the owner-approval interlock
class ApprovalRefused(SystemExit):
    """Raised with a message; a SystemExit so a CLI stops with the text."""


APPROVAL_KEYS = ("approved_by", "approved_utc", "prereg_sha256", "window", "dry_run_window", "schedule", "configs", "replicates", "run_order_seed",
                 "law_p95_mapping", "thresholds_status", "rel_lag_p99_ms", "rel_lag_max_ms", "missed_schedule_lag_ms", "per_second_count_tv",
                 "drain_deadline_s", "s1_route", "s1_evidence_sha256", "lp4_resolution", "live_phase")
S1_ROUTES = ("THIRD_SNAPSHOT", "SIGNED_DRIFT_UPDATE")
LIVE_PHASES = ("DRY_RUN_ONLY", "DRY_RUN_THEN_SERIES")


@dataclass(frozen=True)
class Approval:
    values: dict
    thresholds: FidelityThresholds
    run_order_seed: int
    mapping: str


def parse_kv(text: str) -> dict:
    """key=value lines, plain ASCII, no spaces around '='. Duplicate keys are refused."""
    out: dict[str, str] = {}
    for n, line in enumerate(text.splitlines(), start=1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = re.fullmatch(r"([a-z0-9_]+)=(\S.*)", line.rstrip("\r"))
        if not m:
            raise ApprovalRefused(f"refused: line {n} is not 'key=value'")
        if m.group(1) in out:
            raise ApprovalRefused(f"refused: key {m.group(1)!r} repeated on line {n}")
        out[m.group(1)] = m.group(2).strip()
    return out


def check_approval(text: str, *, prereg_sha256: str) -> Approval:
    """Verify an owner approval file against the exact preregistration it approves. Raises ApprovalRefused on any defect."""
    kv = parse_kv(text)
    missing = [k for k in APPROVAL_KEYS if k not in kv]
    if missing:
        raise ApprovalRefused(f"refused: the approval file lacks {missing}")
    unknown = sorted(set(kv) - set(APPROVAL_KEYS))
    if unknown:
        raise ApprovalRefused(f"refused: unknown keys {unknown}")
    if kv["approved_by"] != "owner":
        raise ApprovalRefused("refused: approved_by must be 'owner'")
    if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", kv["approved_utc"]):
        raise ApprovalRefused("refused: approved_utc must look like 2026-11-15T09:30:00Z")
    if kv["prereg_sha256"].lower() != prereg_sha256.lower():
        raise ApprovalRefused("refused: prereg_sha256 does not equal the hash of the preregistration this build was given")
    if kv["window"] != "F_82800" or kv["schedule"] != "A" or kv["configs"] != "10,40":
        raise ApprovalRefused("refused: window must be F_82800, schedule A, configs 10,40 (Phase 1 scope)")
    if kv["thresholds_status"] != "APPROVED":
        raise ApprovalRefused("refused: thresholds_status must be APPROVED; PROPOSED thresholds cannot authorise a live step")
    if kv["s1_route"] not in S1_ROUTES:
        raise ApprovalRefused(f"refused: s1_route must be one of {S1_ROUTES}")
    if not re.fullmatch(r"[0-9a-f]{64}", kv["s1_evidence_sha256"].lower()):
        raise ApprovalRefused("refused: s1_evidence_sha256 must be a SHA-256")
    if kv["law_p95_mapping"] not in MAPPINGS:
        raise ApprovalRefused(f"refused: law_p95_mapping must be one of {list(MAPPINGS)}")
    if kv["live_phase"] not in LIVE_PHASES:
        raise ApprovalRefused(f"refused: live_phase must be one of {LIVE_PHASES}")
    if not re.fullmatch(r"\d+", kv["run_order_seed"]) or not re.fullmatch(r"\d+", kv["replicates"]):
        raise ApprovalRefused("refused: run_order_seed and replicates must be integers")
    if not kv["lp4_resolution"] or kv["lp4_resolution"].upper() == "NONE":
        raise ApprovalRefused("refused: lp4_resolution must name how the LP4/LP1 conflict was resolved")
    try:
        values = {k: float(kv[k]) for k in ("rel_lag_p99_ms", "rel_lag_max_ms", "missed_schedule_lag_ms", "per_second_count_tv", "drain_deadline_s")}
    except ValueError as exc:
        raise ApprovalRefused("refused: every numeric threshold must be a number") from exc
    if any(not (v > 0) for v in values.values()):
        raise ApprovalRefused("refused: every numeric threshold must be positive")
    return Approval(kv, approved_thresholds(values, prereg_sha256.lower()), int(kv["run_order_seed"]), kv["law_p95_mapping"])
