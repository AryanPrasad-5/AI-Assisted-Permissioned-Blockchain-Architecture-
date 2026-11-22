"""Capacity-bracket experiment design, task A0t (2026-11-22). A DESIGN and a schedule builder; it reads no measurement, runs nothing and approves nothing.

Question. A0s found no demonstrated decision headroom: every stored predictive decision is B = 10, and the only measured Fabric evidence (P1b Tier 1: one replicate per cell, 15-40 tx/s,
tau = 2 s, one worker) has B = 10 beating B = 40 at every tested rate. Capacity above 40 tx/s was NOT located. Before any actuator is built, this design asks whether the ordering of
B = 10 and B = 40 ever reverses at higher offered load, with replicates, so that a crossover (or its absence) is a measured statement.

What the design commits to (all PROPOSED; an owner decision record must approve each field, and every field that needs a number the repository cannot supply is ``None``):

* two configurations only (the measured and modelled menu); B = 10 is the static baseline (best measured at every tested rate), B = 40 the historical alternative;
* a sequential escalation ladder above the last measured rate, with NO claimed safe maximum: the ceiling is an owner parameter and escalation stops at the first stop condition;
* randomised order within each (rung, replicate) block, ascending order across rungs, a drift re-check at the anchor rate at the end;
* one fresh dedicated channel per run (the P1b convention), so no configuration update and no actuator is needed;
* primary comparison: paired difference of client p95 latency and of ledger-committed throughput per block; a closed-form signed-rank bound is stated for the replicate count.

Evidence labels. ``model_support`` (HFBTP domain, offline) and ``fabric_evidence`` (measured here) are kept apart: the HFBTP model's workload domain [10, 200] tx/s says nothing about what this
testbed sustains. The design contains no power calculation because there is no variance estimate (one replicate per cell).
"""

from __future__ import annotations

import json
import random
import re
from typing import Any, Mapping, Sequence

from . import channel_selection as cs
from . import config_inspection as ci

DESIGN_SCHEMA = "a0t_capacity_bracket_design/1"
DESIGN_VERSION = "capacity_bracket 0.1 (A0t, 2026-11-22)"
OWNER_REQUIRED = None                                   # every field the owner must decide is None, never a value
STATUS = "PROPOSED_NOT_APPROVED"
CELL_COMMON = {"batch_timeout_ms": 2000, "preferred_max_bytes": 10485760, "absolute_max_bytes": 103809024, "orderers": 3}
BASELINE_B, ALTERNATIVE_B = 10, 40
LAST_MEASURED_RATE_TPS = 40
PROPOSED_RUNGS_TPS = (50, 60, 80, 100, 120)             # PROPOSED escalation rungs; none is claimed safe; rungs above an owner ceiling are not used
STOP_RULES = (
    ("S01", "run_class_not_healthy", "the evaluator's run class (class_v2) is not HEALTHY for either configuration at a rung: stop escalating that configuration; report the rung as a capacity boundary"),
    ("S02", "failure_rate_above_bound", "failed or timed-out transactions exceed the owner bound in the measurement window"),
    ("S03", "delivery_fidelity_below_bound", "generator issuance (issue log) falls below the owner-approved fraction of the offered rate: the load was not delivered"),
    ("S04", "resource_floor", "available memory or another owner-approved resource floor is breached during a run"),
    ("S05", "latency_ceiling", "client p95 exceeds the owner-approved ceiling"),
    ("S06", "configuration_or_clock_gate", "initial configuration differs from the approved cell, the configuration changes, or the clock verdict is not AGREE (A0s stop conditions)"),
    ("S07", "ceiling_reached", "the owner-approved maximum offered rate has been run"),
    ("S08", "interrupt_or_evidence_failure", "an interruption, a recorder or ledger evidence-write failure, or a missing recorder/ledger file: the run is discarded, never repaired or resumed"),
    ("S09", "no_automatic_continuation", "after any stop, no further rung runs without a new owner decision; no retry of a failed run inside the same authorization"),
    ("S10", "owner_review_interval", "the owner reviews after every rung (both configurations) before the next rung is authorized"),
)
METRICS = {
    "primary": [
        {"name": "client_p95_latency_ms", "source": "per-transaction recorder records inside the measurement window, successful transactions only (never Caliper's summary table)"},
        {"name": "ledger_committed_throughput_tps", "source": "ledger follower records (committed transactions in the measurement window / window length); client completion is not ledger commitment"},
    ],
    "secondary": ["client_p50_latency_ms", "client_p99_latency_ms", "failed_transactions", "timed_out_transactions", "delivered_over_offered_ratio", "blocks_per_second",
                  "realised_transactions_per_block", "run_class_v2", "wsl_cpu_busy_pct", "wsl_mem_available_mb", "windows_cpu_utility_pct", "caliper_cpu_pct", "peer_cpu_pct"],
}
REQUIRED_OWNER_PARAMETERS = (
    "max_offered_rate_tps", "initial_escalation_rungs_tps", "replicates_per_cell_initial", "replicates_per_cell_max", "randomisation_seed", "warmup_s", "measurement_window_s", "cooldown_s",
    "failure_rate_bound", "delivery_fidelity_floor", "latency_ceiling_ms", "resource_floor_wsl_mem_available_mb", "practical_threshold_latency_ms", "practical_threshold_throughput_tps",
    "channel_name_family", "series_authorization_mechanism",
)


class DesignInvalid(ValueError):
    """A design or schedule request is not acceptable."""


def minimum_exact_signed_rank_p(n: int) -> float:
    """Smallest attainable two-sided p of the exact Wilcoxon signed-rank test with ``n`` untied non-zero pairs: ``2 / 2**n`` (closed form, no simulation)."""
    if isinstance(n, bool) or not isinstance(n, int) or n < 1:
        raise DesignInvalid("n must be a positive integer")
    return min(1.0, 2.0 / (2 ** n))


def smallest_n_for_rank_p_below(alpha: float) -> int:
    if not (isinstance(alpha, (int, float)) and 0 < alpha < 1):
        raise DesignInvalid("alpha must be in (0, 1)")
    n = 1
    while minimum_exact_signed_rank_p(n) >= alpha:
        n += 1
    return n


def cell_for(block: int) -> dict[str, Any]:
    cell = {"max_message_count": block, **CELL_COMMON}
    return {**cell, "cell_id": ci.cell_identifier(cell)}


def build_design(*, measured_pairs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """The design document. ``measured_pairs`` are the stored P1b pairs (rate, p95_B10_ms, p95_B40_ms), cited and labelled; nothing in this function creates a measurement."""
    cited = [{"rate_tps": p["rate"], "p95_B10_ms": p["p95_B10_ms"], "p95_B40_ms": p["p95_B40_ms"]} for p in measured_pairs]
    return {
        "schema": DESIGN_SCHEMA, "version": DESIGN_VERSION, "status": STATUS, "approved": False, "measured_results": None,
        "question": "Does the ordering of static B = 10 and B = 40 reverse at offered loads above the last measured 40 tx/s, with replicates?",
        "evidence": {
            "fabric_evidence": {"label": "EVIDENCE (measured; one replicate per cell; no noise floor)", "source": "docs/research/e1/a0s_headroom_verification_2026-11-22/headroom_results.json "
                                "(measured_fabric_evidence.pairs, from the P1b Tier 1 evaluator output)", "tau_s": 2, "workers": 1, "rates_tested_tps": [p["rate_tps"] for p in cited],
                                "pairs_cited": cited, "capacity_located": False, "noise_floor": "NOT MEASURED", "highest_measured_rate_tps": LAST_MEASURED_RATE_TPS},
            "model_support": {"label": "model domain only; says nothing about this testbed", "hfbtp_offered_rate_domain_tps": [10, 200], "use": "none: HFBTP extrapolation is not evidence of real Fabric performance"},
            "crossover_support_in_existing_data": "NONE: B = 10 had the lower p95 at every one of the five tested rates; the B40/B10 p95 ratio is about 3.1-4.1 there. No data above 40 tx/s exist.",
            "mechanistic_hypothesis": "HYPOTHESIS (untested): latency here is dominated by batching wait (about B / rate once a size cut dominates), which favours the smaller B; a reversal needs a per-block "
                                      "fixed cost that makes B = 10 (rate / 10 blocks per second) saturate while B = 40 does not. The ladder above 40 tx/s is where block rate for B = 10 grows.",
        },
        "configurations": {"static_baseline": {**cell_for(BASELINE_B), "role": "static baseline: best measured at every tested rate"},
                           "alternative": {**cell_for(ALTERNATIVE_B), "role": "historical static setting; the second menu entry of the controllers"},
                           "other_configurations": [], "other_configurations_reason": "none: no other cell has measured Fabric support at this tau, one worker and one payload (the P1b evaluator lists B = 20 and B = 80 "
                                                                                    "above 20 tx/s as never claimed). Adding one needs a separate decision with its own evidence."},
        "held_constant": {"batch_timeout_ms": 2000, "preferred_max_bytes": 10485760, "absolute_max_bytes": 103809024, "orderers": 3, "workers": 1, "workload": "create (one payload)",
                          "workload_generator": "identical module and arguments in every run (e1_create_asset_issue_logged_timed.js)", "recorder": "e1_per_tx_recorder_v3", "chaincode": "identical",
                          "round_structure": "identical warm-up then measurement round in every run"},
        "offered_load": {"anchor_rate_tps": LAST_MEASURED_RATE_TPS, "anchor_note": "inside the measured range; its replicates give the first noise floor and a check against the P1b single replicates",
                         "proposed_escalation_rungs_tps": list(PROPOSED_RUNGS_TPS), "rung_status": "PROPOSED_ONLY; none is claimed safe or sustainable",
                         "max_offered_rate_tps": OWNER_REQUIRED, "max_rate_note": "the repository cannot establish a safe maximum (capacity above 40 tx/s was not located); the owner sets the ceiling and escalation stops at the first stop rule",
                         "escalation": "sequential: the next rung is authorized only after both configurations at the previous rung completed HEALTHY and the owner reviewed (S10)"},
        "timing": {"warmup_s": OWNER_REQUIRED, "measurement_window_s": OWNER_REQUIRED, "cooldown_s": OWNER_REQUIRED,
                   "reference_values": {"p1b_tier1": "recorded window about 120 s (resource samples 119-120)", "a0s_smoke_proposal": "30 s warm-up + 60 s measurement"},
                   "rule": "identical warm-up, measurement window and cool-down in every run of the series (a different window changes the tail)"},
        "replicates": {"initial_per_cell": 3, "extension_rule": "add replicates only at rungs whose paired-difference interval straddles the practical threshold, up to an owner maximum, never based on the sign of the result",
                       "maximum_per_cell": OWNER_REQUIRED,
                       "rank_test_note": "with n paired blocks the smallest attainable two-sided exact signed-rank p is 2/2**n: n = 3 gives 0.25, n = 5 gives 0.0625, n = 6 gives 0.03125; "
                                         "n >= 6 is needed before any rank-based p below 0.05 is possible",
                       "n_for_rank_p_below_0.05": smallest_n_for_rank_p_below(0.05),
                       "power_calculation": "NONE: there is no variance estimate (one replicate per cell); none is invented"},
        "schedule": {"unit": "block = one replicate of both configurations at one rung", "within_block_order": "randomised by an explicit seed (owner supplied)", "across_rungs": "ascending (sequential escalation)",
                     "seed": OWNER_REQUIRED, "drift_recheck": {"rate_tps": LAST_MEASURED_RATE_TPS, "position": "after the last rung (or after the stop), one block of both configurations", "purpose": "detect environmental drift over the series"},
                     "reset_between_runs": "a fresh dedicated channel per run (the P1b convention): the configuration is fixed at channel creation, so no configuration update and no actuator is used; "
                                           "a cool-down interval (owner value) separates runs; the Caliper container and the network are not restarted between runs unless the owner decides otherwise",
                     "channel_family": OWNER_REQUIRED, "series_authorization": OWNER_REQUIRED,
                     "series_authorization_note": "bounded_execution authorizes ONE run (one channel, cell, run id). A series of N runs needs N single-run authorizations or a new, separately approved series tool; none exists."},
        "metrics": METRICS,
        "analysis": {
            "unit_of_analysis": "paired block difference d = B40 - B10 at one rung (p95 latency in ms; throughput in tx/s)",
            "per_configuration_uncertainty": "mean, sample SD and a t-based 95% interval with n - 1 degrees of freedom; with n = 3 the interval is very wide and normality is unverifiable",
            "between_run_variability": "SD of the replicate values per configuration and rung; the anchor-rate replicates give the first estimate",
            "paired_or_blocked": "paired by block; within-rung order randomised",
            "effect_size": "paired mean difference in native units and the ratio B40/B10 of p95; a standardised paired effect is reported only for n >= 6",
            "practical_significance": {"latency_ms": OWNER_REQUIRED, "throughput_tps": OWNER_REQUIRED,
                                      "note": "the registered P1b SESOI (62.5 ms at 15 tx/s) is a decision threshold, not a noise estimate; the controller utility weights (throughput - 0.01 x latency_ms - backlog) are code defaults, not approved"},
            "decision_relevant_crossover": "at a rung, B = 40 has the larger controller utility (paired mean of d_throughput - 0.01 x d_p95_ms), its Holm-adjusted 95% interval lies above the practical threshold, and the finding is reproduced "
                                           "by an independent replicate set or at the next rung",
            "multiple_comparisons": "confirmatory family = one contrast per rung actually run, Holm-adjusted across rungs; every other comparison is descriptive and labelled so",
            "workload_delivery_fidelity": "delivered/offered from the generator issue log per run; below the owner floor the run is DELIVERY_FAILED, not compared silently",
            "failed_transactions": "counted and reported per run; latency percentiles use successful transactions only; ledger-committed throughput counts committed transactions only; a run above the owner failure bound is DEGRADED and listed, never dropped",
            "saturated_runs": "a run that is not HEALTHY marks a capacity boundary for that configuration; it is neither discarded nor pooled with healthy runs, and no comparison across a saturation boundary is called a crossover",
        },
        "stop_rules": [{"id": i, "name": n, "text": t} for i, n, t in STOP_RULES],
        "stage_gate": {
            "B10_consistently_best": "document 'no adaptive advantage found in {B10, B40} at tau = 2 s up to the highest HEALTHY rung'; widen the menu (for example BatchTimeout) before any live controller comparison",
            "reproducible_crossover": "evaluate whether the reactive and predictive policies could exploit it using stored or replayed workload at the crossover rates; only then consider the actuation mechanism (O10)",
            "uncertainty_too_large": "add replicates (up to the owner maximum) or improve the measurement; build nothing",
            "rates_not_sustained": "report the capacity limit of this testbed (the highest HEALTHY rung) without extrapolation",
        },
        "required_owner_parameters": list(REQUIRED_OWNER_PARAMETERS),
        "claims_not_made": ["a safe maximum offered rate", "that B = 40 becomes better at higher load", "that any controller can exploit a crossover", "any variance, power or capacity number",
                            "any HFBTP-to-Fabric transfer", "that a run count or duration for the series is measured (channel-creation time is unmeasured)"],
    }


def validate_design(design: Mapping[str, Any]) -> list[dict[str, str]]:
    """Problems of a design (an empty list means none): wrong baseline, invented ceiling, missing owner parameters, missing stop rules, claimed approval, invented measurements."""
    problems: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        problems.append({"code": code, "message": message})

    if design.get("schema") != DESIGN_SCHEMA:
        bad("schema", f"schema {design.get('schema')!r}")
    if design.get("status") != STATUS or design.get("approved") is not False:
        bad("approval_claimed", "a design is PROPOSED_NOT_APPROVED until an owner decision record approves it")
    if design.get("measured_results") is not None:
        bad("measured_results_present", "a design contains no measured results of its own")
    configs = design.get("configurations", {})
    base, alt = configs.get("static_baseline", {}), configs.get("alternative", {})
    if base.get("max_message_count") != BASELINE_B or alt.get("max_message_count") != ALTERNATIVE_B:
        bad("baseline", f"baseline must be B = {BASELINE_B} and the alternative B = {ALTERNATIVE_B}")
    if configs.get("other_configurations"):
        bad("unsupported_configuration", "another configuration needs its own measured support and a separate decision")
    for key in ("batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers"):
        if base.get(key) != alt.get(key) or base.get(key) != CELL_COMMON[key]:
            bad("held_constant", f"{key} must be identical in both cells and equal to {CELL_COMMON[key]}")
    load = design.get("offered_load", {})
    if load.get("max_offered_rate_tps") is not None:
        bad("invented_ceiling", "the maximum offered rate is an owner parameter; the design may not set it")
    rungs = load.get("proposed_escalation_rungs_tps", [])
    if not rungs or rungs != sorted(rungs) or rungs[0] <= LAST_MEASURED_RATE_TPS:
        bad("rungs", f"escalation rungs must ascend and lie above the last measured rate ({LAST_MEASURED_RATE_TPS} tx/s)")
    if load.get("anchor_rate_tps") != LAST_MEASURED_RATE_TPS:
        bad("anchor", "the anchor rate must be the last measured rate")
    if (design.get("replicates") or {}).get("initial_per_cell", 0) < 3:
        bad("replicates", "at least three replicates per cell")
    if "NONE" not in str((design.get("replicates") or {}).get("power_calculation", "")):
        bad("invented_power", "no power calculation may be stated without a variance estimate")
    ids = {r["id"] for r in design.get("stop_rules", [])}
    missing = [i for i, _n, _t in STOP_RULES if i not in ids]
    if missing:
        bad("stop_rules", f"missing stop rules {missing}")
    for name in REQUIRED_OWNER_PARAMETERS:
        if name not in design.get("required_owner_parameters", []):
            bad("owner_parameter_missing", name)
    ev = design.get("evidence", {})
    if "fabric_evidence" not in ev or "model_support" not in ev:
        bad("evidence_labels", "model support and Fabric evidence must be separate")
    elif ev["model_support"].get("use") and "not evidence" not in ev["model_support"]["use"]:
        bad("model_as_evidence", "HFBTP model support may not be used as Fabric evidence")
    for pair in (ev.get("fabric_evidence") or {}).get("pairs_cited", []):
        if pair["rate_tps"] > LAST_MEASURED_RATE_TPS:
            bad("evidence_above_measured_range", f"a cited measurement at {pair['rate_tps']} tx/s is above the measured range")
    for p in design.get("analysis", {}).get("practical_significance", {}).items():
        if p[0] in ("latency_ms", "throughput_tps") and p[1] is not None:
            bad("invented_threshold", f"practical threshold {p[0]} is an owner parameter")
    return problems


def build_schedule(design: Mapping[str, Any], *, seed: int, rungs_tps: Sequence[int], replicates: int, channel_family: str) -> dict[str, Any]:
    """An ILLUSTRATIVE or owner-seeded schedule. Every argument is required (no default). ``rungs_tps`` must start with the anchor; the escalation rungs ascend; the last block is the drift re-check.

    The schedule is a list of runs; nothing is authorized. Channel names are checked with the existing validator and must be new (not protected, not historical, not a reserved family).
    """
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise DesignInvalid("seed must be an integer supplied by the owner")
    if isinstance(replicates, bool) or not isinstance(replicates, int) or replicates < 3:
        raise DesignInvalid("at least three replicates per cell")
    rungs = list(rungs_tps)
    if not rungs or rungs[0] != design["offered_load"]["anchor_rate_tps"] or rungs != sorted(set(rungs)):
        raise DesignInvalid("rungs must be strictly ascending and start with the anchor rate")
    if not isinstance(channel_family, str) or not channel_family:
        raise DesignInvalid("channel_family is required")
    if re.match(r"(ql|cp|rp)[0-9]{2}", channel_family):           # the validator matches whole names of those series; a suffix would slip past it, so the family itself is refused
        raise DesignInvalid(f"channel_family {channel_family!r} begins like a reserved earlier series (ql/cp/rp + two digits)")
    rng = random.Random(seed)
    base, alt = design["configurations"]["static_baseline"], design["configurations"]["alternative"]
    runs: list[dict[str, Any]] = []

    def add_block(rate: int, replicate: int, kind: str) -> None:
        order = [base, alt]
        rng.shuffle(order)
        for position, cfg in enumerate(order):
            channel = f"e1-{channel_family}-b{cfg['max_message_count']}-l{rate}-r{replicate}"
            try:
                cs.select_experiment_channel(channel, purpose="a capacity-bracket run", option="channel")
            except cs.ChannelSelectionError as exc:
                raise DesignInvalid(f"channel {channel!r} is not selectable: {exc}") from exc
            runs.append({"sequence": len(runs) + 1, "kind": kind, "rate_tps": rate, "replicate": replicate, "position_in_block": position + 1, "block": cfg["max_message_count"],
                         "cell_id": cfg["cell_id"], "channel": channel})

    for rate in rungs:
        for replicate in range(1, replicates + 1):
            add_block(rate, replicate, "anchor" if rate == rungs[0] else "escalation")
    add_block(rungs[0], replicates + 1, "drift_recheck")
    names = [r["channel"] for r in runs]
    if len(set(names)) != len(names):
        raise DesignInvalid("channel names collide")
    return {"schema": "a0t_capacity_bracket_schedule/1", "status": "ILLUSTRATIVE_NOT_AUTHORIZED", "seed": seed, "rungs_tps": rungs, "replicates_per_cell": replicates,
            "runs": runs, "run_count": len(runs), "note": "no run is authorized; each run needs its own authorization record; the number of runs executed depends on the stop rules"}


def design_json(design: Mapping[str, Any]) -> str:
    return json.dumps(design, indent=2, sort_keys=True) + "\n"
