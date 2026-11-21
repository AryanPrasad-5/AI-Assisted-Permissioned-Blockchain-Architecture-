"""A0h: how often do STORED 60 s arrival-rate forecasts fall outside the HFBTP model's 10-200 tx/s range? (2026-11-21)

Read-only and descriptive. It reads forecast values that already exist on disk, checks each table against the Stage 3 forecast
contract, and counts them with the strict boundary rule of ``fabric_perf.controllers.forecast_boundary``. It runs no forecaster and
no model, creates no forecast, and writes nothing except an optional NEW JSON file named by ``--out``.

Eligibility (all must hold; otherwise the table is listed under ``excluded`` with the reason):

* the forecast column is the frozen EWMA's (``yhat_ewma``) and the table's own frozen selection says EWMA, alpha 0.005, H = 60 s,
  minimum history 600 s (``selection_frozen.json``; the v2 file's SHA-256 must equal the pin in ``offline_replay_inputs.json``);
* every row's target window is exactly 60 s and every origin leaves at least 600 s of history, origins strictly increasing;
* the target is an ARRIVAL RATE (not gas demand) and the table is a forecast table (not observed values, not HFBTP predictions).

    python scripts/e1/e1_a0h_forecast_boundary.py [--out NEW_FILE.json]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import forecast_boundary as fb  # noqa: E402

V2 = REPO / "experiments" / "results" / "flashbots_horizon_benchmark_v2"
FB1 = REPO / "Flashbots" / "results" / "FB-FC-v1"
PINS = REPO / "Integration" / "system_integration" / "offline_replay_inputs.json"
FORECAST_COLUMN, HORIZON_S, MIN_HISTORY_S = "yhat_ewma", 60, 600

# (segment id, path, UTC day, role, which frozen selection describes it)
PRIMARY = (
    ("v2_V_H60", V2 / "select" / "forecasts_V_H60.csv", "2026-09-21", "validation half-day (12:00-24:00Z); used for model selection", "v2"),
    ("v2_F_H60", V2 / "final" / "forecasts_F_H60.csv", "2026-10-12", "held-out day, scored once with the frozen selection", "v2"),
)
CROSSCHECK = (
    ("fbfc1_V_arrival_rate_H60", FB1 / "select" / "forecasts_V_arrival_rate_H60.csv", "2026-09-21", "v2_V_H60"),
    ("fbfc1_F_arrival_rate_H60", FB1 / "final" / "forecasts_F_arrival_rate_H60.csv", "2026-10-12", "v2_F_H60"),
)


def rel(path: Path) -> str:
    """Repository-relative POSIX path (a file outside the repository, e.g. in a test, is named as given)."""
    try:
        return path.relative_to(REPO).as_posix()
    except ValueError:
        return path.as_posix()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_table(path: Path):
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        return list(reader.fieldnames or ()), list(reader)


def contract_problems() -> tuple[list[str], dict]:
    """Does the stored frozen selection say what the Stage 3 forecast contract says? Reads JSON only."""
    problems, facts = [], {}
    pins = json.loads(PINS.read_text(encoding="utf-8"))["files"]["selection"]
    selection_path = REPO / pins["path"]
    facts["v2_selection_sha256"] = sha256(selection_path)
    if facts["v2_selection_sha256"].upper() != pins["sha256"].upper():
        problems.append("v2 selection_frozen.json differs from the pin in offline_replay_inputs.json")
    selection = json.loads(selection_path.read_text(encoding="utf-8"))
    h60 = selection["horizons"]["60"]
    facts["v2_h60"] = {"selected_model": h60["selected_model"], "alpha": h60["alpha"], "min_history_s": selection["min_history_s"],
                       "method": selection["method"]}
    if (h60["selected_model"], h60["alpha"], selection["min_history_s"]) != ("ewma", 0.005, MIN_HISTORY_S):
        problems.append("v2 frozen selection is not EWMA alpha 0.005 with 600 s minimum history")
    fb1 = json.loads((FB1 / "select" / "selection_frozen.json").read_text(encoding="utf-8"))["selection"]["arrival_rate|60"]
    facts["fbfc1_arrival_rate_h60"] = {"selected_model": fb1["selected_model"], "ewma_alpha": fb1["ewma_alpha"], "horizon_s": fb1["horizon_s"],
                                       "target": fb1["target"]}
    if (fb1["selected_model"], fb1["ewma_alpha"], fb1["horizon_s"], fb1["target"]) != ("ewma", 0.005, 60, "arrival_rate"):
        problems.append("FB-FC-v1 arrival_rate H60 selection is not EWMA alpha 0.005")
    return problems, facts


def analyse() -> dict:
    problems, facts = contract_problems()
    report: dict = {"schema": "a0h_forecast_boundary/1", "boundary_rule": "below: <10; inside: 10<=x<=200; above: >200 (tx/s)",
                    "contract": facts, "contract_problems": problems, "segments": [], "cross_checks": [], "excluded": [],
                    "computation_performed": "none: stored values are counted; no forecaster or model was run"}
    if problems:
        report["verdict"] = "BOUNDARY FREQUENCY NOT ESTABLISHED — CONTRACT CHECK FAILED"
        return report
    tables, eligible_values = {}, []
    for segment, path, day, role, _ in PRIMARY:
        header, rows = read_table(path)
        bad = fb.eligibility_problems(header, rows, forecast_column=FORECAST_COLUMN, horizon_s=HORIZON_S, min_origin_s=MIN_HISTORY_S)
        entry = {"segment": segment, "path": rel(path), "sha256": sha256(path), "utc_day": day, "role": role,
                 "rows_in_file": len(rows), "eligibility_problems": bad}
        if bad:
            report["excluded"].append({**entry, "reason": "; ".join(bad)})
            continue
        values = [r[FORECAST_COLUMN] for r in rows]
        eligible_values.extend(values)
        origins = [float(r["origin_s"]) for r in rows]
        tables[segment] = {r["origin_s"]: r[FORECAST_COLUMN] for r in rows}
        entry["forecast_origin_utc_first_last"] = [origins[0], origins[-1]]
        entry["forecast_stats"] = fb.summarize_forecasts(values, origins_s=origins)
        observed = fb.summarize_forecasts([r["y_observed"] for r in rows], origins_s=origins)
        entry["context_observed_60s_mean_rate_NOT_A_FORECAST"] = {k: observed[k] for k in (
            "valid", "below_grid", "below_grid_pct_of_valid", "inside_grid", "above_grid")}
        report["segments"].append(entry)
    # Pooled line: the two days share the frozen EWMA and the 60 s contract; it is descriptive and kept apart from the segments.
    report["pooled_descriptive"] = {"segments": [s["segment"] for s in report["segments"]],
                                    "note": "two different UTC days; descriptive pooling only", **fb.summarize_forecasts(eligible_values)}
    for segment, path, day, partner in CROSSCHECK:
        header, rows = read_table(path)
        bad = fb.eligibility_problems(header, rows, forecast_column=FORECAST_COLUMN, horizon_s=HORIZON_S, min_origin_s=MIN_HISTORY_S)
        partner_table = tables.get(partner, {})
        matched = [(partner_table[r["origin_s"]], r[FORECAST_COLUMN]) for r in rows if r["origin_s"] in partner_table]
        difference = max((abs(float(a) - float(b)) for a, b in matched), default=None)
        report["cross_checks"].append({
            "segment": segment, "path": rel(path), "sha256": sha256(path), "utc_day": day,
            "rows_in_file": len(rows), "eligibility_problems": bad, "partner_segment": partner, "origins_shared_with_partner": len(matched),
            "max_abs_difference_tps_on_shared_origins": difference,
            "treatment": "NOT pooled: same frozen EWMA on the same day, so its values duplicate the partner's where origins overlap"})
    for directory in (V2 / "select", V2 / "final", FB1 / "select", FB1 / "final"):
        for path in sorted(directory.glob("forecasts_*.csv")):
            relative = rel(path)
            used = {s["path"] for s in report["segments"]} | {c["path"] for c in report["cross_checks"]}
            if relative in used:
                continue
            name = path.name
            reason = ("wrong horizon (not 60 s)" if any(f"_H{h}." in name for h in (1, 10, 300)) else
                      "wrong target (gas demand, not arrival rate)" if "gas_demand" in name else "not an eligible forecast table")
            header, rows = read_table(path)
            report["excluded"].append({"path": relative, "rows_in_file": len(rows), "reason": reason})
    other = REPO / "experiments" / "results" / "e1" / "hfbtp_scientific_validity_audit_20261111"
    for path in sorted(other.glob("heldout_*_predictions.csv")):
        report["excluded"].append({"path": rel(path), "rows_in_file": len(read_table(path)[1]),
                                   "reason": "HFBTP held-out performance predictions, not workload forecasts"})
    report["excluded_rows_total"] = sum(item["rows_in_file"] for item in report["excluded"])
    report["eligible_rows_total"] = sum(s["forecast_stats"]["eligible_forecasts"] for s in report["segments"])
    report["verdict"] = ("BOUNDARY FREQUENCY ESTABLISHED FROM STORED FORECASTS (proxy for abstention, not a measured abstention rate)"
                         if report["segments"] else "BOUNDARY FREQUENCY NOT ESTABLISHED — NO ELIGIBLE STORED FORECASTS")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", type=Path, help="write the JSON report to this NEW file (an existing file is never overwritten)")
    args = parser.parse_args(argv)
    report = analyse()
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.out is not None:
        try:
            with args.out.open("x", encoding="utf-8", newline="\n") as stream:
                stream.write(text + "\n")
        except FileExistsError:
            print(f"REFUSED: {args.out} exists")
            return 2
    print(text)
    return 0 if report["segments"] else 1


if __name__ == "__main__":
    sys.exit(main())
