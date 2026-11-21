"""Abstention accounting for future static / reactive / predictive comparisons (task A0i, 2026-11-21).

Purpose. A shadow run produces one decision row per (frame, controller). Before any later comparison can say what a controller did, it
must say, with an explicit denominator, how many scheduled decision opportunities there were and what became of each. This module
does that from ``stage3_shadow_decision/1`` and ``/2`` rows. It is pure counting: it computes nothing about throughput or latency, loads
no model and reads no file except where a function says so.

Every scheduled (frame, controller) pair lands in EXACTLY ONE category, so the categories add up to the denominator (checked, and a
violation raises instead of being repaired):

* ``valid_recommendation``            the frame was valid and the controller proposed or held a configuration;
* ``shared_invalid_frame``            the shared telemetry frame was rejected (all three controllers share that disposition);
* ``initialization``                  the frame was an initialization frame (shared, expected at start-up);
* ``predictive_history_abstention``   valid frame, predictive controller abstained because its workload history was unavailable,
                                      incomplete, stale or lacked verified coverage (a predictive-only abstention);
* ``forecast_out_of_grid``            valid frame, predictive controller abstained because the forecast lay outside the HFBTP workload
                                      range (refused, never clipped);
* ``controller_specific_failure``     valid frame, a controller abstained for any other reason (kept visible, never merged away);
* ``missing_decision``                a scheduled frame for which this controller has no row at all.

Rules that keep it honest. The denominator is the scheduled frame list the caller passes (never inferred from the rows). A row for an
unscheduled frame, a duplicate (frame, controller) row, an unknown controller or a frame whose controllers disagree on its status
is reported in ``problems`` and makes ``consistent`` false; nothing is silently dropped. Exclusions must be declared with a reason
and are reported in full; the primary rate never uses them. The accounting favours no controller: it has no weights and no preferred
outcome. A recommendation is a SHADOW recommendation: coverage of recommendations is not realised blockchain performance, and
nothing here estimates an improvement.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

ACCOUNTING_SCHEMA = "stage3_abstention_accounting/1"
CONTROLLERS = ("static", "reactive", "predictive")

VALID_RECOMMENDATION = "valid_recommendation"
SHARED_INVALID_FRAME = "shared_invalid_frame"
INITIALIZATION = "initialization"
PREDICTIVE_HISTORY_ABSTENTION = "predictive_history_abstention"
FORECAST_OUT_OF_GRID = "forecast_out_of_grid"
CONTROLLER_SPECIFIC_FAILURE = "controller_specific_failure"
MISSING_DECISION = "missing_decision"
CATEGORIES = (VALID_RECOMMENDATION, SHARED_INVALID_FRAME, INITIALIZATION, PREDICTIVE_HISTORY_ABSTENTION, FORECAST_OUT_OF_GRID,
              CONTROLLER_SPECIFIC_FAILURE, MISSING_DECISION)
CONTROLLER_ABSTENTION_CATEGORIES = (PREDICTIVE_HISTORY_ABSTENTION, FORECAST_OUT_OF_GRID, CONTROLLER_SPECIFIC_FAILURE)

HISTORY_REASON_CODES = ("workload_prefix_incomplete_fail_safe", "arrival_history_missing")
HISTORY_ERROR_PREFIXES = ("forecast_history_", "forecast_input_from_future", "workload_history", "UTC day ")
ROW_SCHEMAS = ("stage3_shadow_decision/1", "stage3_shadow_decision/2")

INTERPRETATION = ("Recommendation coverage is not realised blockchain performance. These are shadow recommendations; no configuration was "
                  "applied and nothing here estimates a throughput or latency improvement.")


class AccountingError(ValueError):
    """The accounting cannot be made honest (a broken denominator, a conservation failure). Raised, never repaired."""


def classify_row(row: Mapping[str, Any]) -> tuple[str, str]:
    """``(category, reason_code)`` for one decision row. The reason code is the row's own: nothing is invented."""
    frame = row["frame"]
    status = frame["status"]
    if status == "initialization":
        return INITIALIZATION, str(frame.get("reason_code") or "initialization")
    if status == "rejected":
        return SHARED_INVALID_FRAME, str(frame.get("reason_code") or "frame_rejected")
    if status != "accepted":
        raise AccountingError(f"frame {frame.get('index')} has unknown status {status!r}")
    outcome = row["outcome"]
    if outcome in ("propose", "hold"):
        return VALID_RECOMMENDATION, outcome
    if outcome != "abstain":
        raise AccountingError(f"frame {frame.get('index')} controller {row.get('controller')} has unknown outcome {outcome!r}")
    reason = str(row.get("reason") or "abstain")
    if row.get("controller") == "predictive":
        evidence = row.get("predictive_evidence") or {}
        failure = evidence.get("failure") or {}
        error = str(failure.get("error") or "")
        if evidence.get("ood_status") == "outside_training_grid" or "outside_training_grid" in error:
            return FORECAST_OUT_OF_GRID, reason
        if reason in HISTORY_REASON_CODES or (failure.get("stage") == "forecast" and error.startswith(HISTORY_ERROR_PREFIXES)):
            return PREDICTIVE_HISTORY_ABSTENTION, reason
    return CONTROLLER_SPECIFIC_FAILURE, reason


def _rate(numerator: int, denominator: int) -> dict[str, Any]:
    return {"numerator": numerator, "denominator": denominator, "rate": (numerator / denominator) if denominator else None}


def _range(stamps: list[str]) -> dict[str, Any]:
    return {"first_utc": min(stamps) if stamps else None, "last_utc": max(stamps) if stamps else None, "frames": len(stamps)}


def _schedule(scheduled_frames: int | Sequence[int]) -> list[int]:
    if isinstance(scheduled_frames, bool):
        raise AccountingError("scheduled_frames must be a count or a list of frame indices")
    if isinstance(scheduled_frames, int):
        if scheduled_frames < 0:
            raise AccountingError("scheduled_frames cannot be negative")
        return list(range(scheduled_frames))
    frames = [int(i) for i in scheduled_frames]
    if len(set(frames)) != len(frames):
        raise AccountingError("scheduled_frames lists a frame twice")
    return sorted(frames)


def account(rows: Iterable[Mapping[str, Any]], *, scheduled_frames: int | Sequence[int], segment: str,
            exclusions: Sequence[Mapping[str, Any]] | None = None, controllers: Sequence[str] = CONTROLLERS) -> dict[str, Any]:
    """Account for every scheduled (frame, controller) pair of ONE experimental segment. See the module docstring for the rules."""
    if not isinstance(segment, str) or not segment.strip():
        raise AccountingError("a segment label is required")
    schedule = _schedule(scheduled_frames)
    scheduled = set(schedule)
    declared: dict[int, str] = {}
    for item in exclusions or ():
        frame, reason = item.get("frame"), item.get("reason")
        if frame not in scheduled:
            raise AccountingError(f"excluded frame {frame!r} is not a scheduled frame")
        if not isinstance(reason, str) or not reason.strip():
            raise AccountingError(f"excluded frame {frame} has no stated reason")
        if frame in declared:
            raise AccountingError(f"frame {frame} is excluded twice")
        declared[frame] = reason
    problems: list[str] = []
    by_key: dict[tuple[int, str], Mapping[str, Any]] = {}
    for row in rows:
        if row.get("schema") not in ROW_SCHEMAS:
            raise AccountingError(f"a row has schema {row.get('schema')!r}, not one of {ROW_SCHEMAS}")
        index, name = row["frame"]["index"], row["controller"]
        if name not in controllers:
            problems.append(f"row for unknown controller {name!r} at frame {index}")
            continue
        if index not in scheduled:
            problems.append(f"row for unscheduled frame {index} (controller {name})")
            continue
        if (index, name) in by_key:
            problems.append(f"duplicate row for frame {index} controller {name}")
            continue
        by_key[(index, name)] = row
    frame_status: dict[int, str] = {}
    frame_stamp: dict[int, str] = {}
    for index in schedule:
        group = [by_key[(index, name)] for name in controllers if (index, name) in by_key]
        statuses = {r["frame"]["status"] for r in group}
        if len(statuses) > 1:
            problems.append(f"frame {index}: controllers disagree on the frame status {sorted(statuses)}")
        unknown = statuses - {"accepted", "rejected", "initialization"}
        if unknown:
            raise AccountingError(f"frame {index} has unknown status {sorted(unknown)}")
        if group:
            frame_status[index] = sorted(statuses)[0]
            frame_stamp[index] = min(r["frame"]["observation_timestamp_utc"] for r in group)
    frames_block = {"scheduled": len(schedule),
                    "valid_shared": sum(1 for s in frame_status.values() if s == "accepted"),
                    "shared_invalid": sum(1 for s in frame_status.values() if s == "rejected"),
                    "initialization": sum(1 for s in frame_status.values() if s == "initialization"),
                    "without_any_row": sum(1 for i in schedule if i not in frame_status)}
    if sum(v for k, v in frames_block.items() if k != "scheduled") != len(schedule):
        raise AccountingError("frame categories do not add up to the scheduled frames")
    result_controllers: dict[str, Any] = {}
    for name in controllers:
        counts = {c: 0 for c in CATEGORIES}
        reasons: dict[str, dict[str, int]] = {c: {} for c in CATEGORIES}
        stamps: dict[str, list[str]] = {c: [] for c in CATEGORIES}
        outcomes = {"propose": 0, "hold": 0}
        for index in schedule:
            row = by_key.get((index, name))
            if row is None:
                category, reason = MISSING_DECISION, "no_decision_record"
            else:
                category, reason = classify_row(row)
                if category == VALID_RECOMMENDATION:
                    outcomes[reason] += 1
            counts[category] += 1
            reasons[category][reason] = reasons[category].get(reason, 0) + 1
            if index in frame_stamp:
                stamps[category].append(frame_stamp[index])
        if sum(counts.values()) != len(schedule):
            raise AccountingError(f"controller {name}: categories add up to {sum(counts.values())}, not {len(schedule)}")
        valid_shared = frames_block["valid_shared"]
        specific = sum(counts[c] for c in CONTROLLER_ABSTENTION_CATEGORIES)
        kept = [i for i in schedule if i not in declared]
        valid_kept = sum(1 for i in kept if (i, name) in by_key and classify_row(by_key[(i, name)])[0] == VALID_RECOMMENDATION)
        result_controllers[name] = {
            "total_scheduled_decision_opportunities": len(schedule),
            "valid_shared_frames": valid_shared,
            "shared_invalid_frames": counts[SHARED_INVALID_FRAME],
            "initialization_frames": counts[INITIALIZATION],
            "controller_specific_abstentions": {"total": specific, **{c: counts[c] for c in CONTROLLER_ABSTENTION_CATEGORIES}},
            "valid_recommendations": {"total": counts[VALID_RECOMMENDATION], **outcomes},
            "missing_decisions": counts[MISSING_DECISION],
            "decision_coverage_rate": _rate(counts[VALID_RECOMMENDATION], len(schedule)),
            "coverage_rate_given_valid_shared_frame": _rate(counts[VALID_RECOMMENDATION], valid_shared),
            "coverage_rate_excluding_declared_exclusions": None if not declared else _rate(valid_kept, len(kept)),
            "category_counts": counts, "reason_breakdown": {c: dict(sorted(v.items())) for c, v in reasons.items() if v},
            "timestamp_ranges": {c: _range(v) for c, v in stamps.items() if v},
        }
    return {"schema": ACCOUNTING_SCHEMA, "segment": segment, "denominator": {
                "definition": "scheduled decision opportunities = the scheduled frame list supplied by the caller, per controller",
                "scheduled_frames": len(schedule), "frame_indices": schedule if len(schedule) <= 50 else [schedule[0], "...", schedule[-1]]},
            "exclusions": [{"frame": f, "reason": r} for f, r in sorted(declared.items())],
            "exclusions_note": "declared exclusions never change decision_coverage_rate; they only feed coverage_rate_excluding_declared_exclusions",
            "frames": frames_block, "controllers": result_controllers, "problems": problems, "consistent": not problems,
            "interpretation": INTERPRETATION}


def combine_segments(segments: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Sum the COUNTS of several segment accountings and recompute the rates. The per-segment accountings are returned beside it:
    a pooled figure is descriptive only and a comparison must report the per-segment range."""
    if not segments:
        raise AccountingError("no segments to combine")
    pooled: dict[str, Any] = {}
    for name in CONTROLLERS:
        counts = {c: sum(s["controllers"][name]["category_counts"][c] for s in segments) for c in CATEGORIES}
        scheduled = sum(s["controllers"][name]["total_scheduled_decision_opportunities"] for s in segments)
        if sum(counts.values()) != scheduled:
            raise AccountingError(f"controller {name}: pooled categories do not add up")
        pooled[name] = {"scheduled": scheduled, "category_counts": counts,
                        "decision_coverage_rate": _rate(counts[VALID_RECOMMENDATION], scheduled)}
    return {"schema": ACCOUNTING_SCHEMA + "+pooled", "segments": [s["segment"] for s in segments], "pooled_descriptive": pooled,
            "note": "pooled over segments for description only; report the per-segment accountings, never only the pool",
            "per_segment": [dict(s) for s in segments], "interpretation": INTERPRETATION}


def account_run_directory(run_dir: Path, *, segment: str | None = None, exclusions: Sequence[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """Account for a finished Stage 3 run directory. The denominator is the run's own ``frames_in_fixture`` (the scheduled frames),
    not the number of rows found. Files are only read."""
    run_dir = Path(run_dir)
    summary = json.loads((run_dir / "run_summary.json").read_text(encoding="utf-8"))
    rows = [json.loads(line) for line in (run_dir / "shadow_decisions.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    result = account(rows, scheduled_frames=int(summary["frames_in_fixture"]), segment=segment or run_dir.name, exclusions=exclusions)
    result["run_status"] = summary.get("status")
    if summary.get("status") != "COMPLETED":
        result["problems"].append(f"the run status is {summary.get('status')!r}, not COMPLETED; unprocessed frames count as missing decisions")
        result["consistent"] = False
    return result


def historical_below_grid_proxy(path: Path) -> dict[str, Any]:
    """The stored A0h boundary analysis, read back and LABELLED. It counts stored 60 s EWMA forecasts below the HFBTP 10 tx/s bound.

    It is a historical proxy for predictive abstention and is NOT a measured live predictive-abstention rate: the origins are every 60 s
    of two stored Ethereum days, the 21 Sep segment was used for model selection, and the exact Stage 3 decision path was not run.
    """
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    pooled = doc["pooled_descriptive"]
    segments = [{"segment": s["segment"], "utc_day": s["utc_day"], "role": s["role"], "eligible": s["forecast_stats"]["valid"],
                 "below_grid": s["forecast_stats"]["below_grid"], "inside_grid": s["forecast_stats"]["inside_grid"],
                 "above_grid": s["forecast_stats"]["above_grid"]} for s in doc["segments"]]
    eligible, below = int(pooled["valid"]), int(pooled["below_grid"])
    if sum(s["eligible"] for s in segments) != eligible or sum(s["below_grid"] for s in segments) != below:
        raise AccountingError("the stored boundary analysis does not add up")
    return {"kind": "historical_stored_forecast_below_grid_proxy", "is_measured_live_abstention_rate": False,
            "source_schema": doc.get("schema"), "eligible_forecasts": eligible, "below_grid": below, "inside_grid": int(pooled["inside_grid"]),
            "above_grid": int(pooled["above_grid"]), "below_grid_fraction": below / eligible if eligible else None,
            "segments": segments, "caveats": ["stored origins every 60 s on two Ethereum mempool days, not Fabric",
                                              "the 2026-09-21 segment was used for model selection",
                                              "the Stage 3 decision path was not run over these origins; other abstention causes add to it"]}
