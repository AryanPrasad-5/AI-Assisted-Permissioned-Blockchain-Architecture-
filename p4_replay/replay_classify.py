"""Replay classification, version 2C. Pure functions over files and rows; nothing is started, nothing is written. OFFLINE_TESTED, LIVE_UNTESTED.

Supersedes `classify.py` (Phase 2, left untouched) for any launch built on the Phase 2C entry. What changed, and only this:

  1. REFERENCE. N1, N3, N4 and the burst deviation are measured against the MEDIAN dispatch lag of the round, not the first dispatch (see replay_fidelity.py for why, and for
     what the median cannot do). The Phase 1 first-dispatch figures are still returned beside them.
  2. GATING. Owner-approved engineering safeguards (PROPOSED values, no empirical basis, none tuned on a measured outcome):
        N1  p99 of |lag - median lag| <= 20 ms                   -> FIDELITY_LIMITED_REPLAY
        N3  no schedule entry undispatched, none beyond 500 ms   -> SCHEDULER_LIMITED_REPLAY
        N4  total variation of per-second counts <= 0.02         -> FIDELITY_LIMITED_REPLAY
        N5  last commit within 10 s of the last dispatch         -> a Fabric OVERLOAD INDICATOR, never a class
     DIAGNOSTICS, reported and never labelling a run:
        N2  maximum |lag - median lag| (reference value 100 ms)
        N6  burst-second count deviation (reference value 0.10)
  3. CLOCK. C1 is no longer a caller-supplied boolean: the clock evidence (clock_evidence.py) is evaluated from the continuous probe record and fails closed.

Classes (strings fixed, as in Phase 2): CLOCK_FAILURE, SUBMISSION_FAILURE, INVALID_TRANSACTION, CLIENT_LIMITED_REPLAY, SCHEDULER_LIMITED_REPLAY, FIDELITY_LIMITED_REPLAY,
UNDETERMINED_REPLAY, HEALTHY_REPLAY (HEALTHY_REPLAY_PROVISIONAL while any threshold is PROPOSED). Missing evidence is never healthy: it gives UNDETERMINED_REPLAY, or
CLOCK_FAILURE where it is clock evidence (owner: clock evidence fails closed).
A healthy class says the schedule was realised and the client was not at its CPU line. It does not say Fabric kept up (`fabric_overload_indicators`), and it is
PROVISIONAL for as long as the thresholds are engineering safeguards without a measured basis, which is every run until the owner approves them with a basis.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path

from . import PACKAGE_VERSION
from . import classify as c1
from . import fidelity as fd
from . import offsets as of
from . import replay_fidelity as rf

CLOCK, SUBMISSION, INVALID_TX = c1.CLOCK, c1.SUBMISSION, c1.INVALID_TX
CLIENT, SCHEDULER, FIDELITY = c1.CLIENT, c1.SCHEDULER, c1.FIDELITY
UNDETERMINED, HEALTHY, HEALTHY_PROVISIONAL = c1.UNDETERMINED, c1.HEALTHY, c1.HEALTHY_PROVISIONAL
PRECEDENCE, CONTINUABLE = c1.PRECEDENCE, c1.CONTINUABLE
SUBMISSION_IDS, INVALID_IDS, SCHEDULER_STRUCT_IDS = c1.SUBMISSION_IDS, c1.INVALID_IDS, {"S0", "S1", "S2", "S3", "S9"}
REPLACED_IDS = {"N1", "N2", "N3", "N4", "C1"}      # computed here (median reference, clock evidence), so Phase 1's values for them are not used
HEALTHY_CLASSES = (HEALTHY, HEALTHY_PROVISIONAL)

GATING = {"N1": FIDELITY, "N3": SCHEDULER, "N4": FIDELITY}
DIAGNOSTIC_ONLY = ("N2", "N6")
INDICATOR_ONLY = ("N5",)

CANNOT_ASSESS = [
    "Client versus scheduler attribution of a dispatch-timing failure is by exclusion only: with no CPU evidence at the 80 % line the failure is labelled "
    "SCHEDULER_LIMITED_REPLAY, but Caliper's replay-rate controller, the Fabric connector, Node and Windows host contention are not separable with this telemetry.",
    "The seq to tx_id join is INFERRED (order of create_ms against dispatch order; ties ambiguous). Key-level ledger reconciliation is NOT AVAILABLE: the ledger list "
    "and the recorder carry the Fabric tx_id, not the CreateAsset key, and the chaincode has no read function.",
    "Time spent inside Caliper between the controller waking and the connector sending is not logged; dispatch_ms is taken inside the workload, create_ms by the recorder.",
    "The event-loop probe has a 10 ms resolution (erratum E1-QL-ERR-001): it is a diagnostic and never labels a run.",
    "There is no live backlog watchdog: in-flight counts are derived after the run from the recorder, and a stall is seen only through the run-level drain figure.",
    "The CPU sampler is 1 Hz: saturation inside one second of a burst is averaged over that second, and a window of the burst second plus two seconds is used.",
    "Windows host contention is recorded by the host sampler but no host figure labels a run (no basis for a host threshold).",
    "The pre-roll to measured turnover gap is not recorded, and pre-roll queue state does not carry into the measured round (Caliper waits for in-flight transactions).",
    "The MEDIAN reference cannot expose a delay of up to 500 ms (N3's limit) that begins right after the first dispatch and covers 99 % or more of the later dispatches (N1 is a "
    "99th percentile, N3 tolerates deviations up to its limit, and that delay becomes the reference); Phase 2's first-dispatch reference would flag it by the luck that the first "
    "dispatch precedes the delay. A shared delay covering 1 % to 99 % of dispatches IS flagged, and so is a larger delay that leaves even one dispatch unaffected. A delay present from "
    "the first dispatch is invisible to every relative-lag reference. The first-dispatch figures, the median lag against the carried 5.5-8.5 ms band and the head/tail drift are "
    "reported beside the gates and gate nothing; a bound on the median lag itself is an OWNER_DECISION_REQUIRED.",
    "N3 is two-sided (|lag - median| > limit), stricter than 'more than 500 ms late' for early dispatches; replay-rate cannot dispatch more than 5 ms early (derived from source).",
    "Every numeric threshold is an engineering safeguard with no empirical basis; HEALTHY_REPLAY_PROVISIONAL until the owner approves them with a stated basis.",
]


@dataclass(frozen=True)
class ReplayThresholds:
    n1_lag_p99_ms: float = 20.0          # PROPOSED (owner recommendation N1), median-referenced, GATES (FIDELITY)
    n3_missed_ms: float = 500.0          # PROPOSED (owner recommendation N3), median-referenced, GATES (SCHEDULER)
    n4_count_tv: float = 0.02            # PROPOSED (owner recommendation N4), GATES (FIDELITY)
    n5_drain_s: float = 10.0             # PROPOSED (owner recommendation N5), OVERLOAD INDICATOR only
    n2_lag_max_ms: float = 100.0         # PROPOSED reference value, DIAGNOSTIC only
    n6_burst_dev: float = 0.10           # PROPOSED reference value, DIAGNOSTIC only
    client_cpu_pct: float = 80.0         # CARRIED_FROZEN: P1b plan section 6
    burst_rate_tps: int = 40             # CARRIED: P4 draft LP5
    clock_wall_minus_mono_ms: float = 2.0   # CARRIED_FROZEN: protocol V7
    status: str = "PROPOSED"             # PROPOSED | APPROVED
    approved_by_prereg_sha256: str | None = None

    def fidelity_thresholds(self) -> fd.FidelityThresholds:
        """The Phase 1 object, for fd.evaluate (structural checks, N5) and for the executor's own fidelity.json (first-dispatch reference, informational only)."""
        return fd.FidelityThresholds(self.n1_lag_p99_ms, self.n2_lag_max_ms, self.n3_missed_ms, self.n4_count_tv, self.n5_drain_s, self.status, self.approved_by_prereg_sha256)

    def as_dict(self) -> dict:
        return {"gating": {"N1_lag_p99_ms": self.n1_lag_p99_ms, "N3_missed_ms": self.n3_missed_ms, "N4_count_tv": self.n4_count_tv},
                "overload_indicator": {"N5_drain_s": self.n5_drain_s}, "diagnostic_only": {"N2_lag_max_ms": self.n2_lag_max_ms, "N6_burst_dev": self.n6_burst_dev},
                "client_cpu_pct": self.client_cpu_pct, "burst_rate_tps": self.burst_rate_tps, "clock_wall_minus_mono_ms": self.clock_wall_minus_mono_ms,
                "reference": rf.REFERENCE, "status": self.status, "approved_by_prereg_sha256": self.approved_by_prereg_sha256}


def approved_replay_thresholds(base: ReplayThresholds, prereg_sha256: str) -> ReplayThresholds:
    """Only the launch-approval loader calls this, with values from an owner-approved file."""
    return replace(base, status="APPROVED", approved_by_prereg_sha256=prereg_sha256)


@dataclass
class ReplayInputs:
    run_id: str
    fidelity: dict                                                   # fd.evaluate output: its structural checks and N5 are used
    clock: dict = field(default_factory=dict)                        # clock_evidence result; {} = not evaluated = fails closed
    lag: dict = field(default_factory=dict)                          # replay_fidelity.lag_metrics (measured round); {} = dispatch evidence missing
    missed: dict = field(default_factory=dict)
    tv: dict = field(default_factory=dict)
    burst: dict | None = None                                        # replay_fidelity.burst_deviation (measured round)
    drift: dict = field(default_factory=dict)
    wall_minus_mono_ms: list = field(default_factory=list)
    cpu: dict = field(default_factory=lambda: {"available": False})
    inflight: dict = field(default_factory=dict)
    event_loop_p99_ms: float | None = None
    dispatch_missing: bool = False


def classify_replay(inp: ReplayInputs, thr: ReplayThresholds | None = None) -> dict:
    thr = thr or ReplayThresholds()
    failed: dict[str, list[dict]] = {k: [] for k in PRECEDENCE}
    missing: list[str] = []
    preroll_flags: list[str] = []
    subtypes: list[str] = []
    overload: dict = {"drain_deadline_exceeded": None, **inp.inflight}
    informational: dict = {"event_loop_p99_ms_diagnostic_only": inp.event_loop_p99_ms, "drain_failures": []}
    for c in inp.fidelity["checks"]:
        if c["ok"] is None or c["id"] in REPLACED_IDS:
            continue
        label = f"{c['id']}{'@' + c['round'] if c['round'] else ''}"
        if c["ok"] is False and str(c["note"]).startswith(fd.EVIDENCE_MISSING):
            missing.append(f"{label}: {c['note']}")
            continue
        cid, rnd = c["id"], c["round"]
        if cid in INDICATOR_ONLY:
            if rnd == "measured":
                overload["drain_deadline_exceeded"] = c["ok"] is False
            if c["ok"] is False:
                informational["drain_failures"].append(label)
            continue
        if c["ok"] is True:
            continue
        if c["kind"] == fd.NUMERIC and rnd == "pre_roll":
            preroll_flags.append(label)
            continue
        if cid in SUBMISSION_IDS:
            failed[SUBMISSION].append(c)
            if cid in ("S7", "S10"):
                subtypes.append("SUBMISSION_FAILURE:RECONCILIATION_MISMATCH")
        elif cid in INVALID_IDS:
            failed[INVALID_TX].append(c)
        elif cid in SCHEDULER_STRUCT_IDS:
            failed[SCHEDULER].append(c)
        else:                                                         # a failing check this mapping does not know must never vanish
            missing.append(f"{label}: failed check with no class mapping")
    # ---- clock evidence (fails closed): from the continuous probe record, never a caller-supplied constant
    clock = inp.clock or {}
    if not clock:
        failed[CLOCK].append({"id": "CS", "name": "clock evidence", "observed": "NOT_EVALUATED"})
        subtypes.append("CLOCK_FAILURE:CLOCK_EVIDENCE_NOT_EVALUATED")
    elif clock.get("ok") is not True:
        failed[CLOCK].append({"id": "CS", "name": "continuous clock record over the run window", "observed": clock.get("verdict"), "expected": "PASS"})
        subtypes.append(f"CLOCK_FAILURE:{clock.get('verdict')}")
    if not inp.wall_minus_mono_ms or any(v is None for v in inp.wall_minus_mono_ms):
        missing.append("wall-minus-monotonic value of every round (recorder manifest)")
    elif any(abs(float(v)) > thr.clock_wall_minus_mono_ms for v in inp.wall_minus_mono_ms):
        failed[CLOCK].append({"id": "V7", "name": "wall minus monotonic over each round", "observed": inp.wall_minus_mono_ms})
        subtypes.append("CLOCK_FAILURE:WALL_MINUS_MONOTONIC")
    # ---- dispatch timing against the MEDIAN reference
    if inp.dispatch_missing:
        missing.append("dispatch log of the measured round (N1, N3, N4)")
    else:
        if inp.lag:
            if inp.lag["abs_p99_ms"] > thr.n1_lag_p99_ms:
                failed[FIDELITY].append({"id": "N1", "name": "p99 |lag - median lag| (ms)", "observed": inp.lag["abs_p99_ms"], "expected": f"<= {thr.n1_lag_p99_ms} ({thr.status})"})
        if inp.missed:
            if inp.missed["missed"] > 0:
                failed[SCHEDULER].append({"id": "N3", "name": "schedule entries undispatched or beyond the limit", "observed": inp.missed["missed"],
                                          "expected": f"0 at {thr.n3_missed_ms} ms ({thr.status})"})
                subtypes.append("SCHEDULER_LIMITED_REPLAY:UNDISPATCHED" if inp.missed["never_dispatched"] else "SCHEDULER_LIMITED_REPLAY:LATE_OR_EARLY_BEYOND_LIMIT")
        if inp.tv:
            if inp.tv["tv_distance"] > thr.n4_count_tv:
                failed[FIDELITY].append({"id": "N4", "name": "per-second count total variation", "observed": inp.tv["tv_distance"], "expected": f"<= {thr.n4_count_tv} ({thr.status})"})
        if not (inp.lag and inp.missed and inp.tv):
            missing.append("median-referenced dispatch metrics (the dispatch log has duplicate or unexpected seq values; see S2)")
    # ---- client evidence: Caliper CPU at the 80 % line, as a round mean or a burst-second mean
    cpu = inp.cpu or {"available": False}
    if not cpu.get("available") or cpu.get("mean_pct") is None:
        missing.append("Caliper container CPU samples over the measured round")
    else:
        if cpu["mean_pct"] >= thr.client_cpu_pct:
            failed[CLIENT].append({"id": "CPU-mean", "observed": cpu["mean_pct"]})
            subtypes.append("CLIENT_LIMITED_REPLAY:CPU_ROUND_MEAN")
        if (inp.burst or {}).get("burst_seconds", 0):
            if cpu.get("burst_mean_pct") is None:
                missing.append("Caliper container CPU samples inside the burst seconds")
            elif cpu["burst_mean_pct"] >= thr.client_cpu_pct:
                failed[CLIENT].append({"id": "CPU-burst", "observed": cpu["burst_mean_pct"]})
                subtypes.append("CLIENT_LIMITED_REPLAY:CPU_BURST_MEAN")
    b = inp.burst
    if b is None or (b.get("applicable") and not b.get("evaluated")):
        if not inp.dispatch_missing:
            missing.append("burst-second realised counts (measured dispatch log)")
    # ---- diagnostics: reported against their reference values, never labelling
    diagnostics = {
        "N2_max_abs_lag_deviation_ms": {"observed": (inp.lag or {}).get("abs_max_ms"), "reference_value": thr.n2_lag_max_ms,
                                        "exceeds_reference": None if not inp.lag else inp.lag["abs_max_ms"] > thr.n2_lag_max_ms, "gates": False},
        "N6_burst_second_count_deviation": {"observed": (b or {}).get("max_rel_dev"), "reference_value": thr.n6_burst_dev, "burst_seconds": (b or {}).get("burst_seconds"),
                                            "exceeds_reference": None if not (b and b.get("evaluated")) else b["max_rel_dev"] > thr.n6_burst_dev, "gates": False},
        "reference": {k: (inp.lag or {}).get(k) for k in ("reference", "median_lag_ms", "first_lag_ms", "reference_gap_ms", "median_inside_carried_offset_band", "carried_offset_band_ms")},
        "vs_first_dispatch_phase1_reference": (inp.lag or {}).get("vs_first"), "drift": inp.drift,
    }
    labels = [k for k in PRECEDENCE if failed[k]]
    attribution = "NONE"
    if SCHEDULER in labels and CLIENT not in labels:
        attribution = "UNATTRIBUTED_BY_EXCLUSION: no client CPU evidence at the line; scheduler, connector and host are not separable"
    elif CLIENT in labels:
        attribution = "CLIENT: CPU evidence at the line" + (" accompanies the dispatch-timing failure" if SCHEDULER in labels else "")
    approved = thr.status == "APPROVED"
    cls = labels[0] if labels else UNDETERMINED if missing else (HEALTHY if approved else HEALTHY_PROVISIONAL)
    return {"run_id": inp.run_id, "class": cls, "labels": labels, "subtypes": sorted(set(subtypes)), "attribution": attribution,
            "usable_as_fabric_result": cls == HEALTHY, "threshold_status": thr.status,
            "label_basis": "APPROVED thresholds" if approved else "PROPOSED thresholds: numeric labels may be placeholder artefacts, structural labels are not",
            "failed": {k: [{kk: vv for kk, vv in c.items() if kk in ("id", "round", "name", "observed", "expected")} for c in v] for k, v in failed.items() if v},
            "missing_evidence": missing, "preroll_timing_flags": preroll_flags, "fabric_overload_indicators": overload, "informational": informational,
            "diagnostics": diagnostics, "thresholds": thr.as_dict(), "clock_evidence": {k: clock.get(k) for k in ("verdict", "ok", "reasons")} if clock else None,
            "fidelity_phase1_verdict": inp.fidelity.get("verdict"), "cannot_assess": CANNOT_ASSESS, "tool": f"p4_replay.replay_classify {PACKAGE_VERSION}"}


def classify_replay_run_dir(run_id: str, stage_run_dir: Path, *, dispatch_dir: Path | None, recorder_dir: Path | None, ledger_csv: Path | None, resources_csv: Path | None,
                            clock_evidence: dict | None, thr: ReplayThresholds | None = None) -> dict:
    """Assemble the inputs from a run's files and classify. Missing files become missing evidence; `clock_evidence` None fails closed."""
    thr = thr or ReplayThresholds()
    tx_rows = of.read_tx_map(Path(stage_run_dir) / "tx_map.csv")
    schedules = {name: [int(x) for x in (Path(stage_run_dir) / f"schedule_{name}.txt").read_text(encoding="ascii").split("\n")] for name in ("pre_roll", "measured")}
    ok = None if not clock_evidence else bool(clock_evidence.get("ok"))
    run = fd.run_input_from_files(run_id, tx_rows, schedules, dispatch_dir=dispatch_dir, recorder_dir=recorder_dir, ledger_csv=ledger_csv, clock_gate_pass=ok)
    fid = fd.evaluate(run, thr.fidelity_thresholds())
    measured = next(r for r in run.rounds if r.name == "measured")
    wall, event_loop = c1._recorder_manifest_facts(recorder_dir, run_id)
    d = measured.dispatch
    lag = missed = tv = drift = {}
    if d:
        seqs = [r["seq"] for r in d]
        if len(set(seqs)) == len(seqs) and all(0 <= s < len(measured.scheduled_offsets_ms) for s in seqs):
            lag = rf.lag_metrics(d)
            missed = rf.missed_schedules(d, len(measured.scheduled_offsets_ms), thr.n3_missed_ms)
            tv = rf.count_tv(d, measured.scheduled_offsets_ms, measured.length_s)
            drift = rf.drift(d)
    burst = rf.burst_deviation(d, measured.scheduled_offsets_ms, measured.length_s, thr.burst_rate_tps) if (d and lag) else (
        None if not d else {"applicable": False, "burst_seconds": 0})
    cpu: dict = {"available": False}
    if d:
        origin, windows = c1.burst_second_windows_ms(d, measured.scheduled_offsets_ms, measured.length_s, thr.burst_rate_tps)
        lo, hi = origin, origin + (max(measured.scheduled_offsets_ms) + 1000.0 + c1.CPU_TAIL_S * 1000.0)
        cpu = c1.cpu_stats(c1.read_cpu_samples(resources_csv), lo, hi, windows)
    inp = ReplayInputs(run_id, fid, clock_evidence or {}, lag, missed, tv, burst, drift, wall, cpu, c1.inflight_profile(measured.recorder), event_loop, dispatch_missing=d is None)
    return classify_replay(inp, thr) | {"fidelity": fid}
