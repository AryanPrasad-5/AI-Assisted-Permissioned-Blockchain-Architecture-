"""Replay-specific run classification (Phase 2). Pure functions over files and rows; nothing is started and nothing is written.

Why P1b's `class_v2` is NOT reused (read in scripts/e1/e1_client_classification_v2.py and e1_quicklook_p1b.evaluate_run):
  * its pacing test is |submitted / (rate x measured_s) - 1| <= 0.02 and p99 create-lateness against `k / rate`: both assume ONE constant rate;
  * its SATURATED clauses compare the interior VALID rate with the offered RATE and a peak in-flight backlog with 200: with a bursty replay the offered rate is not
    constant, and the limit of 200 was set for rates up to 40 tx/s (about 80 in flight at 2 s of batching), whereas a 98 tx/s second with a 2 s batch wait puts about 196
    in flight with no overload at all, so the clause would sit at its limit on a healthy burst;
  * its CPU clause (mean Caliper CPU >= 80 % of one core over the window) averages a burst away.
What IS reused: the 80 % CPU line (P1b plan section 6), the 40 tx/s burst boundary (P4 draft LP5), the 2 ms wall-minus-monotonic bound (protocol V7), and the Phase 1
fidelity checks (`fidelity.evaluate`), whose check ids are mapped to classes below.

Classes (label strings are fixed; a run gets every label that applies and the highest-precedence one as its class):
  CLOCK_FAILURE            clock gate not passed, a clock step or hole inside the run window, or wall-minus-monotonic beyond 2 ms in a round
  SUBMISSION_FAILURE       recorder rows != scheduled, a failed or unverified submission, a repeated transaction id, ledger/recorder mismatch
  INVALID_TRANSACTION      an endorser transaction with a validation code other than VALID
  CLIENT_LIMITED_REPLAY    Caliper container CPU at or above the 80 % line, as a mean over the measured round OR as a mean over the burst seconds
  SCHEDULER_LIMITED_REPLAY a scheduled transaction was never dispatched, was dispatched under a wrong identity or order, or was dispatched more than the
                           missed-schedule lag late; the client shows no CPU evidence (attribution is by exclusion, see `cannot_assess`)
  FIDELITY_LIMITED_REPLAY  every transaction was dispatched, but the realised timing differs from the schedule beyond a lag percentile, a per-second count
                           distance, or a burst-second count deviation
  UNDETERMINED_REPLAY      no limiting label, but evidence that the classification needs is missing: missing evidence is never read as healthy
  HEALTHY_REPLAY           none of the above, all evidence present, thresholds APPROVED. Under PROPOSED thresholds the class is HEALTHY_REPLAY_PROVISIONAL.
A healthy class means the SCHEDULE was realised and the client was not at its limit. It does not mean the ledger holding valid transactions is enough, and it
says nothing about whether Fabric kept up: that is reported separately in `fabric_overload_indicators`.

Every numeric threshold except the CPU line, the burst boundary and the clock bound is PROPOSED with no empirical basis. The burst-deviation limit is new in Phase 2.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field, replace
from pathlib import Path

import numpy as np

from . import PACKAGE_VERSION, ensure_paths
from . import fidelity as fd
from . import offsets as of

ensure_paths()

CLOCK, SUBMISSION, INVALID_TX = "CLOCK_FAILURE", "SUBMISSION_FAILURE", "INVALID_TRANSACTION"
CLIENT, SCHEDULER, FIDELITY = "CLIENT_LIMITED_REPLAY", "SCHEDULER_LIMITED_REPLAY", "FIDELITY_LIMITED_REPLAY"
UNDETERMINED, HEALTHY, HEALTHY_PROVISIONAL = "UNDETERMINED_REPLAY", "HEALTHY_REPLAY", "HEALTHY_REPLAY_PROVISIONAL"
PRECEDENCE = (CLOCK, SUBMISSION, INVALID_TX, CLIENT, SCHEDULER, FIDELITY)
CONTINUABLE = frozenset({CLIENT, SCHEDULER, FIDELITY})       # classes a series may continue past, only if the owner pre-approved it
CLOCK_IDS = {"C1"}
SUBMISSION_IDS = {"S4", "S5", "S5x", "S6", "S7", "S10"}
INVALID_IDS = {"S8"}
SCHEDULER_IDS = {"S0", "S1", "S2", "S3", "S9", "N3"}
FIDELITY_IDS = {"N1", "N2", "N4"}
OVERLOAD_IDS = {"N5"}
CALIPER_CONTAINER = "caliper-benchmark-001"
CPU_TAIL_S = 2                                               # a burst second's CPU window extends 2 s (one BatchTimeout) to cover commit events and 1 Hz alignment

CANNOT_ASSESS = [
    "Client versus scheduler attribution of a dispatch-timing failure is by exclusion only: with no CPU evidence at the 80 % line the failure is labelled "
    "SCHEDULER_LIMITED_REPLAY, but Caliper's replay-rate controller, the Fabric connector, Node and Windows host contention are not separable with this telemetry.",
    "The seq to tx_id join is INFERRED (order of create_ms against dispatch order; ties ambiguous). Key-level ledger reconciliation is NOT AVAILABLE: the ledger list "
    "and the recorder carry the Fabric tx_id, not the CreateAsset key, and the chaincode has no read function.",
    "Time spent inside Caliper between the controller waking and the connector sending is not logged; dispatch_ms is taken inside the workload, create_ms by the recorder.",
    "The event-loop probe has a 10 ms resolution (erratum E1-QL-ERR-001): it is kept as a diagnostic and never labels a run.",
    "There is no live backlog watchdog: in-flight counts are derived after the run from the recorder, and a stall is seen only through the run-level drain figure.",
    "The CPU sampler is 1 Hz: saturation inside one second of a burst is averaged over that second, and a window of the burst second plus two seconds is used.",
    "Windows host contention is recorded by the host sampler but no host figure labels a run (no basis for a host threshold).",
    "The pre-roll to measured turnover gap is not recorded, and pre-roll queue state does not carry into the measured round (Caliper waits for in-flight transactions).",
    "The reference for relative dispatch lag is the first dispatch; one slow first dispatch shifts every later lag, so the lag reference gap to the median is reported.",
]


@dataclass(frozen=True)
class ClassThresholds:
    fidelity: fd.FidelityThresholds = field(default_factory=fd.FidelityThresholds)
    client_cpu_pct: float = 80.0                # CARRIED_FROZEN: P1b plan section 6 (CLIENT_LIMITED clause 2), ThresholdsV2
    burst_rate_tps: int = 40                    # CARRIED: P4 draft LP5, "seconds above 40 tx/s" are the stress probe
    burst_rel_dev_max: float = 0.10             # PROPOSED, NEW IN PHASE 2, no empirical basis: max |realised - scheduled| / scheduled over burst seconds
    clock_wall_minus_mono_ms: float = 2.0       # CARRIED_FROZEN: protocol V7 (P1b THR.clock_wall_minus_mono_ms)
    burst_status: str = "PROPOSED"

    @property
    def status(self) -> str:
        return "APPROVED" if self.fidelity.status == "APPROVED" and self.burst_status == "APPROVED" else "PROPOSED"

    def as_dict(self) -> dict:
        return {"fidelity": self.fidelity.as_dict(), "client_cpu_pct": self.client_cpu_pct, "burst_rate_tps": self.burst_rate_tps,
                "burst_rel_dev_max": self.burst_rel_dev_max, "clock_wall_minus_mono_ms": self.clock_wall_minus_mono_ms, "burst_status": self.burst_status,
                "status": self.status}


def approved_class_thresholds(fidelity: fd.FidelityThresholds, burst_rel_dev_max: float) -> ClassThresholds:
    """Only the series approval loader calls this, with values from an owner-approved file."""
    if fidelity.status != "APPROVED":
        raise ValueError("fidelity thresholds must be APPROVED")
    if not (0 < burst_rel_dev_max <= 1):
        raise ValueError("burst_rel_dev_max must be in (0, 1]")
    return ClassThresholds(fidelity=fidelity, burst_rel_dev_max=float(burst_rel_dev_max), burst_status="APPROVED")


# ----------------------------------------------------------------------------------------------- metrics from rows
def burst_fidelity(dispatch: list[dict] | None, scheduled: list[int], length_s: int, rate_tps: int) -> dict:
    """Per-second realised count against the scheduled count, for the seconds whose scheduled count is at or above `rate_tps`. Realised time is anchored on the
    first dispatch, as in fidelity.realised_count_tv. The whole-run total-variation distance (N4) dilutes a flattened burst: moving 30 of 98 transactions out of
    the peak second changes it by only ~0.005, so this per-burst-second figure is the burst-preservation test."""
    sched = np.asarray(scheduled, dtype=np.int64)
    s_counts = np.bincount(sched // 1000, minlength=length_s)[:length_s]
    burst = np.where(s_counts >= rate_tps)[0]
    out: dict = {"burst_seconds": int(burst.size), "rate_tps": rate_tps, "scheduled_peak": int(s_counts.max()) if s_counts.size else 0}
    if burst.size == 0:
        return {**out, "applicable": False}
    if not dispatch:
        return {**out, "applicable": True, "evaluated": False}
    by_seq = sorted(dispatch, key=lambda r: r["seq"])
    origin = by_seq[0]["dispatch_ms"] - by_seq[0]["intended_offset_ms"]
    realised = np.clip(np.array([r["dispatch_ms"] - origin for r in by_seq], dtype=np.int64), 0, None)
    bins = int(max(length_s, int(realised.max()) // 1000 + 1))
    r_counts = np.bincount(realised // 1000, minlength=bins)[:bins]
    dev = np.abs(r_counts[burst] - s_counts[burst]) / s_counts[burst]
    return {**out, "applicable": True, "evaluated": True, "max_rel_dev": float(dev.max()), "mean_rel_dev": float(dev.mean()),
            "realised_peak_in_scheduled_burst_seconds": int(r_counts[burst].max()), "per_second": [[int(b), int(s_counts[b]), int(r_counts[b])] for b in burst[:50]]}


def burst_second_windows_ms(dispatch: list[dict], scheduled: list[int], length_s: int, rate_tps: int) -> tuple[float, list[tuple[float, float]]]:
    """(round origin in wall ms, [(lo, hi)] wall windows of the burst seconds, each extended by CPU_TAIL_S)."""
    first = min(dispatch, key=lambda r: r["seq"])
    origin = float(first["intended_ms"] - first["intended_offset_ms"])
    s_counts = np.bincount(np.asarray(scheduled, dtype=np.int64) // 1000, minlength=length_s)[:length_s]
    return origin, [(origin + int(s) * 1000.0, origin + (int(s) + 1 + CPU_TAIL_S) * 1000.0) for s in np.where(s_counts >= rate_tps)[0]]


def inflight_profile(recorder: list[dict] | None) -> dict:
    """Peak in-flight transactions (created and not yet final), drain after the last creation: descriptive, from the per-transaction records."""
    if not recorder:
        return {}
    try:
        created = np.array([float(r["create_ms"]) for r in recorder])
        final = np.array([float(r["final_ms"]) for r in recorder])
    except (ValueError, TypeError, KeyError):
        return {}
    events = sorted([(t, 1) for t in created] + [(t, -1) for t in final], key=lambda e: (e[0], e[1]))     # at equal time a finish is processed before a creation
    cur = peak = 0
    peak_t = created.min()
    for t, d in events:
        cur += d
        if cur > peak:
            peak, peak_t = cur, t
    return {"n": len(recorder), "peak_inflight": int(peak), "peak_at_s_from_first_create": float((peak_t - created.min()) / 1000.0),
            "drain_after_last_create_s": float((final.max() - created.max()) / 1000.0), "max_latency_ms": float((final - created).max())}


def read_cpu_samples(path: Path | None, container: str = CALIPER_CONTAINER) -> list[tuple[float, float]] | None:
    """[(wall ms, cpu percent)] of one container from the resource sampler's resource_samples.csv, or None when the file is missing."""
    if path is None or not Path(path).is_file():
        return None
    out = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for r in csv.DictReader(handle):
            if r.get("container") != container or r.get("cpu_percent") in (None, ""):
                continue
            stamp = r.get("read_ns") or r.get("recv_wall_ns") or r.get("t_mid_wall_ns")
            try:
                out.append((float(stamp) / 1e6, float(r["cpu_percent"])))
            except (TypeError, ValueError):
                continue
    return out


def cpu_stats(samples: list[tuple[float, float]] | None, lo_ms: float, hi_ms: float, burst_windows: list[tuple[float, float]]) -> dict:
    if samples is None:
        return {"available": False}
    inside = [c for t, c in samples if lo_ms <= t <= hi_ms]
    in_burst = [c for t, c in samples if any(a <= t < b for a, b in burst_windows)]
    return {"available": True, "mean_pct": float(np.mean(inside)) if inside else None, "samples_in_round": len(inside),
            "burst_mean_pct": float(np.mean(in_burst)) if in_burst else None, "burst_samples": len(in_burst),
            "burst_peak_pct": float(max(in_burst)) if in_burst else None}


# ----------------------------------------------------------------------------------------------- classification
@dataclass
class ClassInputs:
    run_id: str
    fidelity: dict                                   # fidelity.evaluate(...) output
    clock_series_verdict: str | None = None          # p1b clock_series_check verdict for this run's window; None = not available
    wall_minus_mono_ms: list = field(default_factory=list)   # recorder manifest value per round (None = missing)
    cpu: dict = field(default_factory=lambda: {"available": False})
    burst: dict | None = None                        # burst_fidelity() of the MEASURED round
    inflight: dict = field(default_factory=dict)
    event_loop_p99_ms: float | None = None           # diagnostic only
    lag_reference_gap_ms: float | None = None        # |first-dispatch lag - median lag| of the measured round, informational


def classify(inp: ClassInputs, thr: ClassThresholds | None = None) -> dict:
    thr = thr or ClassThresholds()
    failed: dict[str, list[dict]] = {k: [] for k in (CLOCK, SUBMISSION, INVALID_TX, CLIENT, SCHEDULER, FIDELITY)}
    missing: list[str] = []
    preroll_flags: list[str] = []
    overload: dict = {"drain_deadline_exceeded": None, **inp.inflight}
    subtypes: list[str] = []
    informational: dict = {"event_loop_p99_ms_diagnostic_only": inp.event_loop_p99_ms, "lag_reference_gap_ms": inp.lag_reference_gap_ms}
    for c in inp.fidelity["checks"]:
        if c["ok"] is None:
            continue
        label = f"{c['id']}{'@' + c['round'] if c['round'] else ''}"
        if c["ok"] is False and str(c["note"]).startswith(fd.EVIDENCE_MISSING):
            missing.append(f"{label}: {c['note']}")
            continue
        cid, rnd = c["id"], c["round"]
        if cid in OVERLOAD_IDS:
            if rnd == "measured":
                overload["drain_deadline_exceeded"] = c["ok"] is False
            if c["ok"] is False:
                informational.setdefault("drain_failures", []).append(label)
            continue
        if c["ok"] is True:
            continue
        numeric = c["kind"] == fd.NUMERIC
        if numeric and rnd == "pre_roll":
            preroll_flags.append(label)                      # an unmeasured round's timing deviation is recorded, not classed
            continue
        if cid in CLOCK_IDS:
            failed[CLOCK].append(c)
        elif cid in SUBMISSION_IDS:
            failed[SUBMISSION].append(c)
            if cid in ("S7", "S10"):
                subtypes.append("SUBMISSION_FAILURE:RECONCILIATION_MISMATCH")
        elif cid in INVALID_IDS:
            failed[INVALID_TX].append(c)
        elif cid in SCHEDULER_IDS:
            failed[SCHEDULER].append(c)
        elif cid in FIDELITY_IDS:
            failed[FIDELITY].append(c)
        else:                                                # a failing check this mapping does not know must never vanish
            missing.append(f"{label}: failed check with no class mapping")
    # ---- clock evidence beyond the gate: the series check for this run's window and the per-round wall-minus-monotonic
    if inp.clock_series_verdict is None:
        missing.append("clock series check for this run window")
    elif inp.clock_series_verdict != "PASS":
        failed[CLOCK].append({"id": "CS", "name": "continuous clock record over the run window", "observed": inp.clock_series_verdict})
        subtypes.append(f"CLOCK_FAILURE:{inp.clock_series_verdict}")
    if not inp.wall_minus_mono_ms or any(v is None for v in inp.wall_minus_mono_ms):
        missing.append("wall-minus-monotonic value of every round (recorder manifest)")
    elif any(abs(float(v)) > thr.clock_wall_minus_mono_ms for v in inp.wall_minus_mono_ms):
        failed[CLOCK].append({"id": "V7", "name": "wall minus monotonic over each round", "observed": inp.wall_minus_mono_ms})
        subtypes.append("CLOCK_FAILURE:WALL_MINUS_MONOTONIC")
    # ---- client evidence: Caliper CPU at the 80 % line, as a round mean or as a burst-second mean
    cpu = inp.cpu or {"available": False}
    if not cpu.get("available") or cpu.get("mean_pct") is None:
        missing.append("Caliper container CPU samples over the measured round")
    else:
        if cpu["mean_pct"] >= thr.client_cpu_pct:
            failed[CLIENT].append({"id": "CPU-mean", "observed": cpu["mean_pct"]})
            subtypes.append("CLIENT_LIMITED_REPLAY:CPU_ROUND_MEAN")
        burst_n = (inp.burst or {}).get("burst_seconds", 0)
        if burst_n:
            if cpu.get("burst_mean_pct") is None:
                missing.append("Caliper container CPU samples inside the burst seconds")
            elif cpu["burst_mean_pct"] >= thr.client_cpu_pct:
                failed[CLIENT].append({"id": "CPU-burst", "observed": cpu["burst_mean_pct"]})
                subtypes.append("CLIENT_LIMITED_REPLAY:CPU_BURST_MEAN")
    # ---- burst preservation (N6, new in Phase 2)
    b = inp.burst
    if b is None or (b.get("applicable") and not b.get("evaluated")):
        missing.append("burst-second realised counts (measured dispatch log)")
    elif b.get("applicable") and b["max_rel_dev"] > thr.burst_rel_dev_max:
        failed[FIDELITY].append({"id": "N6", "name": "burst-second count deviation", "observed": b["max_rel_dev"], "expected": f"<= {thr.burst_rel_dev_max} ({thr.burst_status})"})
        subtypes.append("FIDELITY_LIMITED_REPLAY:BURST_FLATTENED_OR_SHIFTED")
    labels = [k for k in PRECEDENCE if failed[k]]
    attribution = "NONE"
    if SCHEDULER in labels and CLIENT not in labels:
        attribution = "UNATTRIBUTED_BY_EXCLUSION: no client CPU evidence at the line; scheduler, connector and host are not separable"
    elif SCHEDULER in labels and CLIENT in labels:
        attribution = "CLIENT: CPU evidence at the line accompanies the dispatch-timing failure"
    elif CLIENT in labels:
        attribution = "CLIENT: Caliper container at its CPU line"
    approved = thr.status == "APPROVED"
    if labels:
        cls = labels[0]
    elif missing:
        cls = UNDETERMINED
    else:
        cls = HEALTHY if approved else HEALTHY_PROVISIONAL
    return {"run_id": inp.run_id, "class": cls, "labels": labels, "subtypes": sorted(set(subtypes)), "attribution": attribution,
            "usable_as_fabric_result": cls == HEALTHY, "threshold_status": thr.status,
            "label_basis": "APPROVED thresholds" if approved else "PROPOSED thresholds: numeric labels may be placeholder artefacts, structural labels are not",
            "failed": {k: [{kk: vv for kk, vv in c.items() if kk in ("id", "round", "name", "observed", "expected")} for c in v] for k, v in failed.items() if v},
            "missing_evidence": missing, "preroll_timing_flags": preroll_flags, "fabric_overload_indicators": overload, "informational": informational,
            "thresholds": thr.as_dict(), "fidelity_phase1_verdict": inp.fidelity.get("verdict"), "cannot_assess": CANNOT_ASSESS, "tool": f"p4_replay.classify {PACKAGE_VERSION}"}


# ----------------------------------------------------------------------------------------------- from files
def _recorder_manifest_facts(recorder_dir: Path | None, run_id: str) -> tuple[list, float | None]:
    """([wall-minus-monotonic ms of the pre-roll round, of the measured round], event-loop p99 ms of the measured round), None where a manifest is missing."""
    walls: list = []
    event_loop = None
    for idx, label in ((0, f"wu-{run_id}"), (1, f"m-{run_id}")):
        path = None if recorder_dir is None else Path(recorder_dir) / f"{run_id}_w0_r{idx}_{label}.manifest.json"
        try:
            doc = json.loads(path.read_text(encoding="utf-8")) if path is not None else {}
        except (OSError, ValueError):
            doc = {}
        walls.append((doc.get("clock") or {}).get("wall_minus_monotonic_ms"))
        if idx == 1:
            event_loop = (doc.get("event_loop_delay") or {}).get("p99_ms")
    return walls, event_loop


def classify_run_dir(run_id: str, stage_run_dir: Path, *, dispatch_dir: Path | None, recorder_dir: Path | None, ledger_csv: Path | None,
                     resources_csv: Path | None, clock_gate_pass: bool | None, clock_series_verdict: str | None, thr: ClassThresholds | None = None) -> dict:
    """Assemble the inputs from a run's files (the staged render and what the run left behind) and classify. Missing files become missing evidence."""
    thr = thr or ClassThresholds()
    tx_rows = of.read_tx_map(Path(stage_run_dir) / "tx_map.csv")
    schedules = {name: [int(x) for x in (Path(stage_run_dir) / f"schedule_{name}.txt").read_text(encoding="ascii").split("\n")] for name in ("pre_roll", "measured")}
    run = fd.run_input_from_files(run_id, tx_rows, schedules, dispatch_dir=dispatch_dir, recorder_dir=recorder_dir, ledger_csv=ledger_csv, clock_gate_pass=clock_gate_pass)
    fid = fd.evaluate(run, thr.fidelity)
    measured = next(r for r in run.rounds if r.name == "measured")
    wall, el = _recorder_manifest_facts(recorder_dir, run_id)
    burst = burst_fidelity(measured.dispatch, measured.scheduled_offsets_ms, measured.length_s, thr.burst_rate_tps)
    cpu: dict = {"available": False}
    if measured.dispatch:
        origin, windows = burst_second_windows_ms(measured.dispatch, measured.scheduled_offsets_ms, measured.length_s, thr.burst_rate_tps)
        lo, hi = origin, origin + (max(measured.scheduled_offsets_ms) + 1000.0 + CPU_TAIL_S * 1000.0)
        cpu = cpu_stats(read_cpu_samples(resources_csv), lo, hi, windows)
    gap = None
    if measured.dispatch:
        lag = fd.lag_metrics(measured.dispatch)
        gap = abs(lag["first_lag_ms"] - lag["median_lag_ms"]) if lag else None
    return classify(ClassInputs(run_id, fid, clock_series_verdict, wall, cpu, burst, inflight_profile(measured.recorder), el, gap), thr) | {"fidelity": fid}
