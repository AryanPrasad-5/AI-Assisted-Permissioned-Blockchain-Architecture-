"""Clock evidence from the continuous clock-probe record (Phase 2C). OFFLINE: pure functions over rows and files; nothing is started.

Why this exists. Phase 2 `series.py` passes the literal `clock_gate_pass=True` to the executor and to the classifier. That value says nothing about any clock: the series reaches
that line only if the pre-flight passed, so the structural check C1 ("clock gate passed") could never fail. Here the value is computed from evidence:

  PRE-RUN   `pre_run_clock_ok`   the pre-flight S9 gate result of THIS launch, the probe process alive, its sample file readable, its newest sample fresh, the same WSL boot.
  POST-RUN  `evaluate_clock_evidence`   the probe's samples against THIS run's window: no REALTIME-MONOTONIC step, no coverage hole, enough samples (density), strictly increasing
            monotonic stamps, the same boot as the series start, the probe still alive, and the pre-flight gate PASS.

Fail closed: anything missing, unreadable or incomplete is `ok = False` with a named verdict. There is no verdict in which absent evidence passes.

Reused, not reinvented: step detection and window coverage are P1b's `clock_series_check` (which uses the pinned probe's own `find_steps`); the 0.95 completeness ratio is P1b's
`EvidenceThresholds.min_ratio`; the step and gap limits are P1b's `Thresholds`. The density check is new and uses only the record's own median sampling interval.
"""

from __future__ import annotations

from pathlib import Path

from . import driver as dr

MIN_SAMPLE_RATIO = 0.95      # CARRIED: P1b EvidenceThresholds.min_ratio (samples inside a window / expected samples)
PASS = "PASS"
# every verdict other than PASS fails closed
VERDICTS = ("PASS", "NO_RUN_WINDOW", "NO_DATA", "EVIDENCE_UNREADABLE", "PREFLIGHT_CLOCK_NOT_PASS", "BOOT_CHANGED", "PROBE_NOT_ALIVE", "STEP_IN_RUN_WINDOW",
            "COVERAGE_GAP", "INSUFFICIENT_SAMPLES", "NON_MONOTONIC", "CHECK_ERROR")


def read_samples(path: Path) -> tuple[list[dict], int]:
    """(samples, torn rows dropped) from the probe's clock_samples.csv. Raises OSError or ValueError when the file is missing or its header is wrong."""
    return dr.p1b_module().read_clock_samples(Path(path))


def _median_interval_ns(samples: list[dict]) -> float | None:
    diffs = sorted(b["mono_ns"] - a["mono_ns"] for a, b in zip(samples, samples[1:]) if b["mono_ns"] > a["mono_ns"])
    if not diffs:
        return None
    mid = len(diffs) // 2
    return float(diffs[mid]) if len(diffs) % 2 else (diffs[mid - 1] + diffs[mid]) / 2.0


def evaluate_clock_evidence(samples: list[dict] | None, window: dict | None, *, torn_rows: int = 0, step_ms: float | None = None, max_gap_ms: float | None = None,
                            series_boot_id: str | None, current_boot_id: str | None, preflight_clock_gate_pass: bool | None, probe_alive: bool | None,
                            min_ratio: float = MIN_SAMPLE_RATIO) -> dict:
    """Pure. `window` is one p1b run window ({run_id, start_ns, end_ns, ...}) or None. Returns {ok, verdict, reasons, checks, ...}; `ok` is True only for PASS."""
    p1b = dr.p1b_module()
    step_ms = p1b.THR.clock_step_ms if step_ms is None else step_ms
    max_gap_ms = p1b.THR.clock_gap_max_ms if max_gap_ms is None else max_gap_ms
    checks = {"preflight_clock_gate_pass": preflight_clock_gate_pass is True, "same_boot_as_series_start": bool(series_boot_id) and series_boot_id == current_boot_id,
              "probe_alive": probe_alive is True}
    out: dict = {"checks": checks, "torn_rows_dropped": torn_rows, "min_ratio": min_ratio, "step_threshold_ms": step_ms, "max_gap_ms": max_gap_ms}

    def done(verdict: str, *reasons: str, **extra) -> dict:
        return {**out, **extra, "verdict": verdict, "ok": verdict == PASS, "reasons": list(reasons)}

    if window is None:
        return done("NO_RUN_WINDOW", "the run wrote no start and end time (facts.json), so no window can be checked")
    out["window"] = {k: window.get(k) for k in ("run_id", "start_utc", "end_utc")}
    if not samples or len(samples) < 2:
        return done("NO_DATA", f"{len(samples or [])} probe samples: a window cannot be covered")
    if not checks["preflight_clock_gate_pass"]:
        return done("PREFLIGHT_CLOCK_NOT_PASS", f"pre-flight clock gate result was {preflight_clock_gate_pass!r}")
    if not checks["same_boot_as_series_start"]:
        return done("BOOT_CHANGED", f"boot id {series_boot_id!r} at series start, {current_boot_id!r} now: the WSL clock domain is not the one that was probed")
    if not checks["probe_alive"]:
        return done("PROBE_NOT_ALIVE", "the continuous clock probe process is not running")
    raw = p1b.clock_series_check(samples, [window], step_ms=step_ms, max_gap_ms=max_gap_ms)
    out["p1b_series_check"] = {k: raw.get(k) for k in ("verdict", "samples", "steps_total", "span_s", "max_abs_offset_change_ms", "windows")}
    out["steps_in_run_window"] = raw.get("steps_in_run_windows", [])
    if raw["verdict"] == "STEP_IN_RUN_WINDOW":
        return done("STEP_IN_RUN_WINDOW", f"{len(out['steps_in_run_window'])} REALTIME-MONOTONIC step(s) above {step_ms} ms inside the run window")
    if raw["verdict"] != "PASS":
        return done("COVERAGE_GAP" if raw["verdict"] == "COVERAGE_GAP" else "NO_DATA", f"P1b series check verdict {raw['verdict']}")
    inside = [x for x in samples if window["start_ns"] <= x["real_ns"] <= window["end_ns"]]
    median_ns = _median_interval_ns(samples)
    if median_ns is None:
        return done("NON_MONOTONIC", "the record has no increasing monotonic stamps")
    expected = (window["end_ns"] - window["start_ns"]) / median_ns
    ratio = len(inside) / expected if expected > 0 else 0.0
    out["density"] = {"samples_in_window": len(inside), "expected": round(expected, 1), "ratio": round(ratio, 4), "median_interval_ms": round(median_ns / 1e6, 3)}
    mono = [x["mono_ns"] for x in inside]
    if any(b <= a for a, b in zip(mono, mono[1:])):
        return done("NON_MONOTONIC", "monotonic stamps inside the window do not strictly increase")
    if ratio < min_ratio:
        return done("INSUFFICIENT_SAMPLES", f"{len(inside)} samples inside the window, {expected:.0f} expected at the record's own interval (ratio {ratio:.3f} < {min_ratio})")
    return done(PASS)


def evaluate_from_files(clock_csv: Path, window: dict | None, **kw) -> dict:
    """The same, reading the probe's sample file. An unreadable file is a failed verdict, never an exception and never a pass."""
    try:
        samples, torn = read_samples(clock_csv)
    except (OSError, ValueError) as exc:
        return {"ok": False, "verdict": "EVIDENCE_UNREADABLE", "reasons": [repr(exc)], "checks": {}, "torn_rows_dropped": None}
    try:
        return evaluate_clock_evidence(samples, window, torn_rows=torn, **kw)
    except Exception as exc:                                                          # noqa: BLE001 - fail closed
        return {"ok": False, "verdict": "CHECK_ERROR", "reasons": [repr(exc)], "checks": {}, "torn_rows_dropped": torn}


def pre_run_clock_ok(samples: list[dict] | None, *, now_ns: int, preflight_clock_gate_pass: bool | None, probe_alive: bool | None, series_boot_id: str | None,
                     current_boot_id: str | None, max_age_ms: float | None = None) -> dict:
    """Value for the executor's `clock_gate_pass`: real evidence available BEFORE the run. Freshness allows the probe's own flush interval (P1b clock_flush_margin_s, 12 s),
    because the pinned probe writes its file every 10 s. A step inside the run is judged AFTER the run, over its window."""
    p1b = dr.p1b_module()
    max_age_ms = p1b.THR.clock_flush_margin_s * 1000.0 if max_age_ms is None else max_age_ms
    reasons: list[str] = []
    if preflight_clock_gate_pass is not True:
        reasons.append(f"pre-flight clock gate result {preflight_clock_gate_pass!r}")
    if probe_alive is not True:
        reasons.append("the continuous probe is not running")
    if not series_boot_id or series_boot_id != current_boot_id:
        reasons.append(f"boot id changed ({series_boot_id!r} -> {current_boot_id!r})")
    age_ms = None
    if not samples or len(samples) < 2:
        reasons.append("fewer than 2 probe samples")
    else:
        age_ms = (now_ns - samples[-1]["real_ns"]) / 1e6
        if age_ms > max_age_ms:
            reasons.append(f"newest probe sample is {age_ms:.0f} ms old (limit {max_age_ms:.0f} ms)")
        if age_ms < -max_age_ms:
            reasons.append(f"newest probe sample lies {-age_ms:.0f} ms in the future")
    return {"ok": not reasons, "reasons": reasons, "newest_sample_age_ms": age_ms}
