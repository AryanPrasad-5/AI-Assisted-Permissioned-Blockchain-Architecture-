"""Replay fidelity evaluation. Pure functions over files/rows; no process is started and no network is touched.

A replay is only a Fabric result if the schedule was actually realised. A run that fails any criterion below is labelled
FIDELITY_LIMITED and is NOT a Fabric result (P4 draft LP5). Nothing is silently accepted: missing evidence fails its check.

Two kinds of criterion:
  STRUCTURAL  identities that need no threshold (counts equal, no duplicates, nothing missing, no invalid or failed transaction,
              clock gate passed, ledger reconciled at transaction-id level).
  NUMERIC     compared with a threshold. EVERY NUMERIC THRESHOLD HERE IS PROPOSED: it has no measured basis yet, and is finalised by
              the owner after the dry run (P4 draft section 1). A run that passes under PROPOSED thresholds is labelled
              FIDELITY_PASS_PROVISIONAL, never FIDELITY_PASS. FIDELITY_PASS needs thresholds the owner approved.

Reconciliation limits (verified against the files in the P1b run archives, headers and categorical values only):
  * the ledger tx list (ledger_tx.csv) and the recorder (transaction_measurements.csv, <label>.csv) carry the Fabric tx_id, not the
    CreateAsset key; the chaincode e1asset exposes no read or range-query function. Reconciliation is therefore COMPLETE AT
    tx_id LEVEL: recorder tx_ids == ledger endorser tx_ids, all valid, counts equal to the schedule. The seq <-> tx_id join is an
    INFERRED order join (dispatch order against recorder create_ms) with ties reported as ambiguous. KEY-LEVEL RECONCILIATION IS
    NOT AVAILABLE in Phase 1 (it needs block write-set decoding or a chaincode read function: owner decision).
"""

from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass, field, replace
from pathlib import Path

import numpy as np

STRUCTURAL, NUMERIC, INFORMATIONAL = "STRUCTURAL", "NUMERIC", "INFORMATIONAL"
EVIDENCE_MISSING = "EVIDENCE_MISSING"
LIMITED, PASS_PROVISIONAL, PASS_APPROVED = "FIDELITY_LIMITED", "FIDELITY_PASS_PROVISIONAL", "FIDELITY_PASS"
DISPATCH_HEADER = ["seq", "intended_offset_ms", "intended_ms", "dispatch_ms", "lag_ms"]
ENDORSER_HEADER_TYPE = "3"      # ledger_tx.csv header_type: 1 = config block, 3 = endorser transaction (observed in the P1b archives)
VALID_CODE = "0"                # ledger_tx.csv validation_code 0 = VALID (observed)
SHORT_GAP_MS = 5                # gaps at or below this are where replay-rate is reported (carried, unverified) to skip its sleep


class EvidenceError(ValueError):
    """An input file is missing or malformed."""


@dataclass(frozen=True)
class FidelityThresholds:
    """PROPOSED defaults. They have NO empirical basis; they are placeholders for the owner to replace after the dry run."""
    rel_lag_p99_ms: float = 20.0          # 99th percentile of |dispatch lag - first dispatch lag|
    rel_lag_max_ms: float = 100.0         # maximum of the same
    missed_schedule_lag_ms: float = 500.0  # a schedule entry is MISSED if never dispatched or |relative lag| exceeds this
    per_second_count_tv: float = 0.02     # total-variation distance between scheduled and realised per-second counts
    drain_deadline_s: float = 10.0        # every transaction committed within this long after the last dispatch of its round
    status: str = "PROPOSED"              # PROPOSED | APPROVED
    approved_by_prereg_sha256: str | None = None

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in ("rel_lag_p99_ms", "rel_lag_max_ms", "missed_schedule_lag_ms", "per_second_count_tv",
                                              "drain_deadline_s", "status", "approved_by_prereg_sha256")}


def approved_thresholds(values: dict, prereg_sha256: str) -> FidelityThresholds:
    """Only the preregistration loader calls this, with values read from an owner-approved, hash-pinned file."""
    allowed = {"rel_lag_p99_ms", "rel_lag_max_ms", "missed_schedule_lag_ms", "per_second_count_tv", "drain_deadline_s"}
    if set(values) != allowed:
        raise ValueError(f"approved thresholds must give exactly {sorted(allowed)}")
    return replace(FidelityThresholds(**{k: float(v) for k, v in values.items()}), status="APPROVED", approved_by_prereg_sha256=prereg_sha256)


@dataclass
class RoundInput:
    name: str                                   # pre_roll | measured
    length_s: int
    scheduled_offsets_ms: list[int]
    expected_keys: list[str]                    # CreateAsset keys in seq order (from tx_map.csv)
    dispatch: list[dict] | None = None          # parsed dispatch log, None = evidence missing
    overflow_rows: int | None = None
    recorder: list[dict] | None = None          # per-tx recorder rows of this round, None = evidence missing


@dataclass
class RunInput:
    run_id: str
    rounds: list[RoundInput]
    ledger: list[dict] | None = None            # ledger_tx.csv rows for the run's channel
    clock_gate_pass: bool | None = None
    evidence_errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------------------------------- readers
def _rows(path: Path, header_must_include: list[str]) -> list[dict]:
    try:
        with Path(path).open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            missing = [c for c in header_must_include if c not in (reader.fieldnames or [])]
            if missing:
                raise EvidenceError(f"{path}: header lacks {missing}")
            return list(reader)
    except OSError as exc:
        raise EvidenceError(f"{path}: {exc}") from exc


def read_dispatch_log(path: Path) -> list[dict]:
    rows = _rows(path, DISPATCH_HEADER)
    out = []
    for i, r in enumerate(rows):
        try:
            out.append({k: int(float(r[k])) for k in DISPATCH_HEADER})
        except (ValueError, TypeError) as exc:
            raise EvidenceError(f"{path}: row {i + 2} is not numeric: {r}") from exc
    return out


def read_overflow_rows(path: Path) -> int:
    return len(_rows(path, ["requested_seq", "observed_ms"]))


def read_recorder_csv(path: Path) -> list[dict]:
    return _rows(path, ["tx_id", "create_ms", "final_ms", "status", "verified"])


def read_ledger_tx_csv(path: Path) -> list[dict]:
    return _rows(path, ["tx_id", "validation_code", "header_type"])


# ---------------------------------------------------------------------------------------------------- metrics
def lag_metrics(dispatch: list[dict]) -> dict:
    """Dispatch lag relative to the first dispatch (P4 SF-2: the logged lag_ms carries a constant offset of a few ms).
    Reported against the first dispatch (the P4 definition) and, for information only, against the median."""
    if not dispatch:
        return {}
    by_seq = sorted(dispatch, key=lambda r: r["seq"])
    lag = np.array([r["dispatch_ms"] - r["intended_ms"] for r in by_seq], dtype=float)
    rel_first = lag - lag[0]
    rel_median = lag - float(np.median(lag))
    return {"n": len(lag), "first_lag_ms": float(lag[0]), "median_lag_ms": float(np.median(lag)),
            "rel_first_abs_p99_ms": float(np.quantile(np.abs(rel_first), 0.99)), "rel_first_abs_max_ms": float(np.abs(rel_first).max()),
            "rel_first_min_ms": float(rel_first.min()), "rel_first_max_ms": float(rel_first.max()),
            "rel_median_abs_p99_ms": float(np.quantile(np.abs(rel_median), 0.99)), "rel_median_abs_max_ms": float(np.abs(rel_median).max()),
            "logged_lag_consistent": bool(all(r["lag_ms"] == r["dispatch_ms"] - r["intended_ms"] for r in by_seq))}


def realised_count_tv(dispatch: list[dict], scheduled: list[int], length_s: int) -> dict:
    """Total-variation distance between scheduled and realised per-second counts. Realised time is anchored so the first dispatch
    lands on its own intended offset (this removes the constant clock offset); late dispatches may fall past the window."""
    if not dispatch or not scheduled:
        return {}
    by_seq = sorted(dispatch, key=lambda r: r["seq"])
    origin = by_seq[0]["dispatch_ms"] - by_seq[0]["intended_offset_ms"]
    realised = np.array([r["dispatch_ms"] - origin for r in by_seq], dtype=np.int64)
    sched = np.asarray(scheduled, dtype=np.int64)
    bins = int(max(length_s, (int(max(realised.max(), sched.max())) // 1000) + 1))
    r_counts = np.bincount(np.clip(realised, 0, None) // 1000, minlength=bins)[:bins]
    s_counts = np.bincount(sched // 1000, minlength=bins)[:bins]
    n = max(len(sched), 1)
    return {"tv_distance": float(0.5 * np.abs(r_counts - s_counts).sum() / n), "seconds_with_difference": int((r_counts != s_counts).sum()),
            "max_abs_second_difference": int(np.abs(r_counts - s_counts).max()), "n_scheduled": int(len(sched)), "n_realised": int(len(realised))}


def short_gap_info(dispatch: list[dict], scheduled: list[int]) -> dict:
    """INFORMATIONAL (P4 SF-3): realised gaps where the schedule asks for a gap of 5 ms or less."""
    if len(dispatch) < 2 or len(scheduled) < 2:
        return {}
    by_seq = sorted(dispatch, key=lambda r: r["seq"])
    sched = np.asarray(scheduled, dtype=np.int64)
    disp = np.array([r["dispatch_ms"] for r in by_seq], dtype=np.int64)
    if len(disp) != len(sched):
        return {}
    gaps_s, gaps_r = np.diff(sched), np.diff(disp)
    mask = gaps_s <= SHORT_GAP_MS
    out = {"pairs_with_scheduled_gap_le_5ms": int(mask.sum()), "share_of_pairs": float(mask.mean())}
    if mask.any():
        out["realised_gap_p99_ms_where_scheduled_le_5ms"] = float(np.quantile(gaps_r[mask], 0.99))
        out["realised_gap_max_ms_where_scheduled_le_5ms"] = float(gaps_r[mask].max())
    return out


def order_join(dispatch: list[dict], recorder: list[dict]) -> dict:
    """INFERRED seq <-> tx_id join: recorder rows sorted by create_ms against dispatch order. Ties in create_ms are ambiguous."""
    if not dispatch or not recorder or len(dispatch) != len(recorder):
        return {"kind": "INFERRED_ORDER_JOIN", "available": False}
    stamps = sorted(int(float(r["create_ms"])) for r in recorder)
    _, counts = np.unique(np.array(stamps, dtype=np.int64), return_counts=True)
    return {"kind": "INFERRED_ORDER_JOIN", "available": True, "ambiguous_tie_groups": int((counts > 1).sum()),
            "transactions_in_ambiguous_ties": int(counts[counts > 1].sum()),
            "note": "seq <-> tx_id is not recorded anywhere; this join assumes one worker serialised creation in dispatch order"}


# ---------------------------------------------------------------------------------------------------- evaluation
def _add(checks: list[dict], cid: str, name: str, kind: str, ok: bool | None, observed, expected, rnd: str | None = None, note: str = "") -> None:
    checks.append({"id": cid, "round": rnd, "name": name, "kind": kind, "ok": ok, "observed": observed, "expected": expected, "note": note})


def _missing(checks, cid, name, kind, rnd=None, what=""):
    _add(checks, cid, name, kind, False, None, None, rnd, f"{EVIDENCE_MISSING}: {what}")


def evaluate(run: RunInput, thr: FidelityThresholds | None = None) -> dict:
    """Evaluate every criterion. Returns checks, metrics, the verdict and the labels. Never raises on missing evidence."""
    thr = thr or FidelityThresholds()
    checks: list[dict] = []
    metrics: dict = {"rounds": {}}
    for rnd in run.rounds:
        n, name = len(rnd.scheduled_offsets_ms), rnd.name
        m: dict = {"scheduled": n}
        metrics["rounds"][name] = m
        if len(rnd.expected_keys) != n:
            _add(checks, "S0", "tx map covers the schedule", STRUCTURAL, False, len(rnd.expected_keys), n, name)
        # ---- dispatch log
        if rnd.dispatch is None:
            for cid, nm, kind in (("S1", "scheduled count == dispatched count", STRUCTURAL), ("S2", "dispatch seq complete, no duplicates", STRUCTURAL),
                                  ("S3", "intended offsets equal the schedule", STRUCTURAL), ("N1", "relative dispatch lag p99", NUMERIC),
                                  ("N2", "relative dispatch lag max", NUMERIC), ("N3", "missed schedules", NUMERIC),
                                  ("N4", "per-second realised count distance", NUMERIC)):
                _missing(checks, cid, nm, kind, name, "dispatch log")
        else:
            d = rnd.dispatch
            seqs = [r["seq"] for r in d]
            dup_seq = sorted(s for s, c in Counter(seqs).items() if c > 1)
            missing_seq = sorted(set(range(n)) - set(seqs))
            extra_seq = sorted(set(seqs) - set(range(n)))
            m.update(dispatched=len(d), duplicate_seq=len(dup_seq), missing_seq=len(missing_seq), unexpected_seq=len(extra_seq),
                     overflow_rows=rnd.overflow_rows)
            _add(checks, "S1", "scheduled count == dispatched count", STRUCTURAL, len(d) == n, len(d), n, name)
            _add(checks, "S2", "dispatch seq complete, no duplicates", STRUCTURAL, not dup_seq and not missing_seq and not extra_seq,
                 {"duplicate": len(dup_seq), "missing": len(missing_seq), "unexpected": len(extra_seq)}, 0, name)
            mismatched = [r["seq"] for r in d if 0 <= r["seq"] < n and r["intended_offset_ms"] != rnd.scheduled_offsets_ms[r["seq"]]]
            _add(checks, "S3", "intended offsets equal the schedule", STRUCTURAL, not mismatched, len(mismatched), 0, name)
            lag = lag_metrics(d) if not (dup_seq or extra_seq) else {}
            m["lag"] = lag
            if lag:
                _add(checks, "N1", "relative dispatch lag p99 (ms)", NUMERIC, lag["rel_first_abs_p99_ms"] <= thr.rel_lag_p99_ms,
                     lag["rel_first_abs_p99_ms"], f"<= {thr.rel_lag_p99_ms} ({thr.status})", name)
                _add(checks, "N2", "relative dispatch lag max (ms)", NUMERIC, lag["rel_first_abs_max_ms"] <= thr.rel_lag_max_ms,
                     lag["rel_first_abs_max_ms"], f"<= {thr.rel_lag_max_ms} ({thr.status})", name)
                by_seq = {r["seq"]: r for r in d}
                first = by_seq.get(0)
                late = 0
                if first is not None:
                    ref = first["dispatch_ms"] - first["intended_ms"]
                    late = sum(1 for r in d if abs((r["dispatch_ms"] - r["intended_ms"]) - ref) > thr.missed_schedule_lag_ms)
                missed = late + len(missing_seq)
                m["missed_schedules"] = missed
                _add(checks, "N3", "missed schedules (never dispatched, or lag beyond threshold)", NUMERIC, missed == 0, missed,
                     f"0 at {thr.missed_schedule_lag_ms} ms ({thr.status})", name)
                tv = realised_count_tv(d, rnd.scheduled_offsets_ms, rnd.length_s)
                m["per_second"] = tv
                _add(checks, "N4", "per-second realised count distance (TV)", NUMERIC, tv["tv_distance"] <= thr.per_second_count_tv, tv["tv_distance"],
                     f"<= {thr.per_second_count_tv} ({thr.status})", name)
                m["short_gaps"] = short_gap_info(d, rnd.scheduled_offsets_ms)
                ordered = sorted(d, key=lambda r: r["seq"])
                inversions = int(sum(1 for a, b in zip(ordered, ordered[1:]) if b["dispatch_ms"] < a["dispatch_ms"]))
                m["dispatch_order_inversions"] = inversions
                _add(checks, "I1", "dispatch order inversions (informational)", INFORMATIONAL, None, inversions, "n/a", name)
                _add(checks, "S9", "logged lag_ms equals dispatch_ms - intended_ms", STRUCTURAL, lag["logged_lag_consistent"], lag["logged_lag_consistent"], True, name)
            else:
                for cid, nm in (("N1", "relative dispatch lag p99"), ("N2", "relative dispatch lag max"), ("N3", "missed schedules"), ("N4", "per-second realised count distance")):
                    _add(checks, cid, nm, NUMERIC, False, None, None, name, "cannot be computed: dispatch log has duplicate or unexpected seq")
        # ---- recorder
        if rnd.recorder is None:
            for cid, nm in (("S4", "recorder rows == scheduled count"), ("S5", "no duplicate tx_id in the recorder"), ("S6", "no submission failures")):
                _missing(checks, cid, nm, STRUCTURAL, name, "per-transaction recorder csv")
            _missing(checks, "N5", "all committed within the drain deadline", NUMERIC, name, "per-transaction recorder csv")
        else:
            rec = rnd.recorder
            ids = [r["tx_id"] for r in rec]
            dup_ids = len(ids) - len(set(ids))
            failed = [r for r in rec if r["status"] != "success" or str(r["verified"]).lower() != "true"]
            m.update(recorder_rows=len(rec), recorder_duplicate_tx_id=dup_ids, submission_failures=len(failed))
            _add(checks, "S4", "recorder rows == scheduled count", STRUCTURAL, len(rec) == n, len(rec), n, name)
            _add(checks, "S5", "no duplicate tx_id in the recorder", STRUCTURAL, dup_ids == 0, dup_ids, 0, name)
            _add(checks, "S6", "no submission failures", STRUCTURAL, not failed, len(failed), 0, name)
            m["order_join"] = order_join(rnd.dispatch or [], rec)
            if rnd.dispatch:
                last_dispatch = max(r["dispatch_ms"] for r in rnd.dispatch)
                try:
                    drain_s = (max(float(r["final_ms"]) for r in rec) - last_dispatch) / 1000.0
                    m["drain_after_last_dispatch_s"] = drain_s
                    _add(checks, "N5", "all committed within the drain deadline (s)", NUMERIC, drain_s <= thr.drain_deadline_s, round(drain_s, 3),
                         f"<= {thr.drain_deadline_s} ({thr.status})", name)
                except (ValueError, TypeError):
                    _missing(checks, "N5", "all committed within the drain deadline", NUMERIC, name, "final_ms not numeric")
            else:
                _missing(checks, "N5", "all committed within the drain deadline", NUMERIC, name, "dispatch log")
    # ---- run level: ledger reconciliation at tx_id level
    recorder_all = [r["tx_id"] for rnd in run.rounds if rnd.recorder is not None for r in rnd.recorder]
    if run.ledger is None or any(rnd.recorder is None for rnd in run.rounds):
        for cid, nm in (("S7", "ledger endorser tx_ids == recorder tx_ids"), ("S8", "no invalid transactions"), ("S10", "ledger endorser count == scheduled total")):
            _missing(checks, cid, nm, STRUCTURAL, None, "ledger_tx.csv or recorder")
    else:
        endorser = [r for r in run.ledger if str(r["header_type"]) == ENDORSER_HEADER_TYPE]
        l_ids = [r["tx_id"] for r in endorser]
        miss, extra = set(recorder_all) - set(l_ids), set(l_ids) - set(recorder_all)
        dup_ledger = len(l_ids) - len(set(l_ids))
        invalid = [r for r in endorser if str(r["validation_code"]) != VALID_CODE]
        total = sum(len(r.scheduled_offsets_ms) for r in run.rounds)
        metrics["ledger"] = {"endorser_tx": len(l_ids), "recorder_tx": len(recorder_all), "missing_in_ledger": len(miss), "unexpected_in_ledger": len(extra),
                             "duplicate_in_ledger": dup_ledger, "invalid": len(invalid), "scheduled_total": total,
                             "reconciliation": "COMPLETE_AT_TX_ID_LEVEL" if not (miss or extra or dup_ledger) else "INCOMPLETE",
                             "key_level_reconciliation": "NOT_AVAILABLE (no key in ledger_tx.csv or recorder; chaincode has no read function)"}
        _add(checks, "S7", "ledger endorser tx_ids == recorder tx_ids (exactly once each)", STRUCTURAL, not (miss or extra or dup_ledger),
             {"missing": len(miss), "unexpected": len(extra), "duplicate": dup_ledger}, 0)
        _add(checks, "S8", "no invalid transactions (validation_code 0)", STRUCTURAL, not invalid, len(invalid), 0)
        _add(checks, "S10", "ledger endorser count == scheduled total", STRUCTURAL, len(l_ids) == total, len(l_ids), total)
    cross_dups = len(recorder_all) - len(set(recorder_all))
    if all(rnd.recorder is not None for rnd in run.rounds):
        _add(checks, "S5x", "no tx_id repeated across rounds (duplicate submission)", STRUCTURAL, cross_dups == 0, cross_dups, 0)
    if run.clock_gate_pass is None:
        _missing(checks, "C1", "clock gate passed", STRUCTURAL, None, "clock gate result")
    else:
        _add(checks, "C1", "clock gate passed", STRUCTURAL, bool(run.clock_gate_pass), bool(run.clock_gate_pass), True)
    failed_checks = [c for c in checks if c["ok"] is False]
    verdict = LIMITED if failed_checks else (PASS_APPROVED if thr.status == "APPROVED" else PASS_PROVISIONAL)
    return {"run_id": run.run_id, "verdict": verdict, "threshold_status": thr.status, "thresholds": thr.as_dict(),
            "usable_as_fabric_result": verdict == PASS_APPROVED, "failed_checks": [f"{c['id']}{'@' + c['round'] if c['round'] else ''}" for c in failed_checks],
            "evidence_errors": list(run.evidence_errors), "checks": checks, "metrics": metrics,
            "not_evaluated_here": ["live scheduling fidelity on Caliper (LIVE_UNTESTED)", "in-flight backlog during the run (no live watchdog in Phase 1)",
                                   "key-level ledger reconciliation (NOT_AVAILABLE)"]}


# ---------------------------------------------------------------------------------------------------- assembling a RunInput from files
def run_input_from_files(run_id: str, tx_map_rows: list[dict], schedules: dict, *, dispatch_dir: Path | None, recorder_dir: Path | None,
                         ledger_csv: Path | None, clock_gate_pass: bool | None, lengths_s: dict | None = None) -> RunInput:
    """Assemble from the rendered tx_map/schedules and the run's files. Any unreadable source becomes None plus an evidence error.
    LIVE_UNTESTED against a real run directory: the file names follow the P1b archive layout and the workload's own log names."""
    lengths = lengths_s or {"pre_roll": 60, "measured": 600}
    labels = {"pre_roll": f"wu-{run_id}", "measured": f"m-{run_id}"}
    rounds_: list[RoundInput] = []
    errors: list[str] = []
    for name in ("pre_roll", "measured"):
        rows = sorted((r for r in tx_map_rows if r["round"] == name), key=lambda r: int(r["seq"]))
        ri = RoundInput(name, lengths[name], [int(v) for v in schedules[name]], [r["create_asset_key"] for r in rows])
        for attr, loader, path in (("dispatch", read_dispatch_log, None if dispatch_dir is None else Path(dispatch_dir) / f"dispatch_{name}.csv"),
                                   ("overflow_rows", read_overflow_rows, None if dispatch_dir is None else Path(dispatch_dir) / f"dispatch_{name}.csv.overflow.csv"),
                                   ("recorder", read_recorder_csv, None if recorder_dir is None else Path(recorder_dir) / f"{run_id}_w0_r{0 if name == 'pre_roll' else 1}_{labels[name]}.csv")):
            if path is None:
                continue
            try:
                setattr(ri, attr, loader(path))
            except EvidenceError as exc:
                errors.append(str(exc))
        rounds_.append(ri)
    ledger = None
    if ledger_csv is not None:
        try:
            ledger = read_ledger_tx_csv(ledger_csv)
        except EvidenceError as exc:
            errors.append(str(exc))
    return RunInput(run_id, rounds_, ledger, clock_gate_pass, errors)
