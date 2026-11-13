#!/usr/bin/env python3
"""Corrected CLIENT_LIMITED / SATURATED classification for the P1a quick-look runs (erratum E1-QL-ERR-001, prepared 2026-11-13).

Why this module exists
  The frozen driver scripts/e1/e1_quicklook.py labels a run CLIENT_LIMITED when the recorder's event-loop p99 exceeds 10 ms
  (Thresholds.client_event_loop_p99_ms, classify()). The recorder samples that loop with monitorEventLoopDelay({ resolution: 10 })
  and Node records the whole tick interval, so the value cannot fall below the 10 ms resolution: the median was 10.13-10.15 ms and the
  p99 11.30-11.57 ms in all 18 executed runs, at 10 and at 20 tx/s alike. That clause is true for every run whatever the load, so it
  carries no information about the client, and it ended both ramps at their first rung.

What it does
  It never edits a record. It derives a second classification NEXT TO the original one, from the same raw files, with the same
  offline evaluator the driver uses (e1_quicklook.evaluate_run), and it adds no new number: every threshold below is a number the
  frozen plan (section 6) or protocol (V2) already contained. The only change is that the event-loop probe no longer labels a run:
  neither through clause 1 (p99 > 10 ms) nor through the event-loop component of the V2 pacing gate (p99 > 20 ms), which the frozen
  classify() also turned into a CLIENT_LIMITED label. The event-loop figures stay in the record as diagnostics. The V2 validity gate
  (p99 <= 20 ms) is untouched and still decides INVALID_SCHEDULE, which stops a ramp exactly as before; it is never read as proof of
  client saturation. No executed P1a run came near 20 ms (largest p99 11.57 ms), so that second difference changes no P1a label.

Rule v2
  CLIENT_LIMITED_V2 = actual pacing failed  (|submitted / (rate x measured seconds) - 1| > 0.02, or p99 create lateness > 5 ms)
                      OR Caliper container mean CPU >= 80 % of one core
  SATURATED_V2      = interior VALID rate / offered < 0.95  OR  peak in-flight backlog > 200  OR  failure rate > 1 %   (unchanged)
  HEALTHY           = neither label.  A ramp may climb only from a rung that is VALID and HEALTHY.
  Evidence that is missing is never read as healthy: the class is UNDETERMINED.
  Attribution is separate and conservative: a component is named only when its measured container CPU is at or above the 80 % line;
  saturation without such a component is UNATTRIBUTED. Fabric is never named without a measurement.

Usage (inside Ubuntu WSL; read-only on the evidence; writes only two NEW files and refuses to overwrite):
  python3 -B e1_client_classification_v2.py derive --out-csv <new csv> --out-json <new json>
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

TOOL_VERSION = "e1_client_classification_v2 0.1 (2026-11-13)"
ERRATUM_ID = "E1-QL-ERR-001"


@dataclass(frozen=True)
class ThresholdsV2:
    """Every number is one the frozen P1a plan (section 6) or protocol (V2) already contained. None is new, none is tuned."""
    pacing_ratio_tol: float = 0.02        # protocol V2: |A/lambda - 1|
    lateness_p99_ms: float = 5.0          # protocol V2: p99 create lateness against the schedule
    event_loop_p99_v2_ms: float = 20.0    # protocol V2 validity gate: a diagnostic here, never a saturation criterion
    client_cpu_pct: float = 80.0          # plan section 6, CLIENT_LIMITED clause 2 (unchanged)
    failure_rate_max: float = 0.01        # plan section 6, SATURATED (unchanged)
    sat_valid_ratio: float = 0.95         # plan section 6, SATURATED (unchanged)
    sat_backlog: int = 200                # plan section 6, SATURATED (unchanged)
    # Deliberately absent: client_event_loop_p99_ms = 10.0, the frozen clause 1 that this erratum withdraws as a label.


THR2 = ThresholdsV2()
HEALTHY, CLIENT_LIMITED, SATURATED, UNDETERMINED = "HEALTHY", "CLIENT_LIMITED", "SATURATED", "UNDETERMINED"


def _le(a, b):
    return None if a is None else a <= b


def _lt(a, b):
    return None if a is None else a < b


def _ge(a, b):
    return None if a is None else a >= b


def classify_v2(m: dict, thr: ThresholdsV2 = THR2) -> dict:
    """m: rate, ratio_A_over_lambda, lateness_p99_ms, event_loop_p99_ms, caliper_cpu_mean_pct, failure_rate, max_inflight_backlog,
    interior_valid_rate_tps (any may be None). Returns the evidence flags, the labels and the class."""
    rate = m.get("rate")
    ratio, late = m.get("ratio_A_over_lambda"), m.get("lateness_p99_ms")
    cpu, fail, backlog, vrate = m.get("caliper_cpu_mean_pct"), m.get("failure_rate"), m.get("max_inflight_backlog"), m.get("interior_valid_rate_tps")
    delivery = (vrate / rate) if (vrate is not None and rate) else None
    f = {
        "pacing_ratio_ok": None if ratio is None else abs(ratio - 1.0) <= thr.pacing_ratio_tol,
        "lateness_ok": _le(late, thr.lateness_p99_ms),
        "el_v2_gate_ok": _le(m.get("event_loop_p99_ms"), thr.event_loop_p99_v2_ms),   # diagnostic only
        "client_cpu_ok": _lt(cpu, thr.client_cpu_pct),
        "delivery_ratio": delivery,
        "delivery_ok": _ge(delivery, thr.sat_valid_ratio),
        "backlog_ok": _le(backlog, thr.sat_backlog),
        "failures_ok": _le(fail, thr.failure_rate_max),
    }
    f["pacing_ok"] = None if (f["pacing_ratio_ok"] is None or f["lateness_ok"] is None) else (f["pacing_ratio_ok"] and f["lateness_ok"])
    f["v2_gate_pass"] = None if (f["pacing_ok"] is None or f["el_v2_gate_ok"] is None) else (f["pacing_ok"] and f["el_v2_gate_ok"])
    missing = sorted(k for k in ("pacing_ratio_ok", "lateness_ok", "client_cpu_ok", "delivery_ok", "backlog_ok", "failures_ok") if f[k] is None)
    labels: list[str] = []
    if f["pacing_ok"] is False or f["client_cpu_ok"] is False:
        labels.append(CLIENT_LIMITED)
    if f["delivery_ok"] is False or f["backlog_ok"] is False or f["failures_ok"] is False:
        labels.append(SATURATED)
    if missing and not labels:
        cls = UNDETERMINED
    elif not labels:
        cls = HEALTHY
    else:
        cls = "+".join(labels)
    return {"flags": f, "labels_v2": labels, "class_v2": cls, "missing_evidence": missing}


def ramp_may_continue(verdict: str | None, cls: str | None) -> bool:
    """A ramp climbs only from a rung that ran, was VALID and is HEALTHY (no label, no missing evidence)."""
    return verdict == "VALID" and cls == HEALTHY


def attribute(labels: list[str], component_cpu_pct: dict[str, float] | None, thr: ThresholdsV2 = THR2) -> dict:
    """Name a component only on measured evidence: its mean CPU over the measured window at or above the 80 % line."""
    at_limit = sorted(c for c, v in (component_cpu_pct or {}).items() if v is not None and v >= thr.client_cpu_pct)
    if not labels:
        return {"cpu_at_limit_components": [], "attribution": "NONE"}
    if component_cpu_pct is None:
        return {"cpu_at_limit_components": [], "attribution": "UNATTRIBUTED: no component measurements"}
    if CLIENT_LIMITED in labels and SATURATED in labels:
        return {"cpu_at_limit_components": at_limit, "attribution": "AMBIGUOUS: client pacing or CPU limit and a saturation symptom in the same run"}
    if CLIENT_LIMITED in labels:
        return {"cpu_at_limit_components": at_limit, "attribution": "CLIENT: pacing failed or the Caliper container is at its CPU line"}
    non_client = [c for c in at_limit if not c.startswith("caliper")]
    if non_client:
        return {"cpu_at_limit_components": at_limit, "attribution": "AT_CPU_LIMIT: " + ", ".join(non_client)}
    return {"cpu_at_limit_components": at_limit, "attribution": "UNATTRIBUTED: saturation symptom but no measured component at its CPU line"}


CSV_COLUMNS = [
    "run_id", "seq", "channel", "kind", "B", "rate_tps", "replicate", "status",
    "original_verdict", "original_labels", "original_labels_reproduced_by_driver_code",
    "original_el_p50_ms", "original_el_p99_ms", "original_clause1_el_gt_10ms",
    "pacing_ratio", "pacing_ratio_ok", "lateness_p99_ms", "lateness_ok", "el_p99_ms_v2_gate_diagnostic", "el_v2_gate_ok", "v2_gate_pass",
    "caliper_cpu_mean_pct", "client_cpu_ok",
    "interior_valid_rate_tps", "delivery_ratio", "delivery_ok", "max_inflight_backlog", "backlog_ok", "failure_rate", "failures_ok",
    "corrected_labels", "corrected_class", "cpu_at_limit_components", "attribution",
    "ramp_may_continue_original", "ramp_may_continue_corrected", "note"]


def _fmt(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, list):
        return ";".join(v)
    if isinstance(v, float):
        return repr(round(v, 9))
    return str(v)


def build_row(row, verdict_recorded: dict | None, skipped_record: dict | None, recomputed: dict | None, el_stats: dict | None,
              component_cpu: dict | None, thr: ThresholdsV2 = THR2) -> dict:
    """One row of the derived table. `row` is a matrix row (attributes seq, run_id, channel, kind, B, rate, replicate); `recomputed` is the
    output of the driver's own offline evaluator on the same files; nothing here reads or writes the evidence."""
    base = {"run_id": row.run_id, "seq": row.seq, "channel": row.channel, "kind": row.kind, "B": row.B, "rate_tps": row.rate,
            "replicate": row.replicate}
    if recomputed is None:
        why = (skipped_record or {}).get("reason", "")
        base.update(status="SKIPPED", original_verdict=(skipped_record or {}).get("verdict", ""), original_labels="",
                    corrected_class="NOT_RUN", attribution="NONE", ramp_may_continue_original=False,
                    ramp_may_continue_corrected="", note=f"not run; original skip reason: {why}")
        return {c: _fmt(base.get(c)) for c in CSV_COLUMNS}
    met = recomputed.get("metrics", {})
    gate = next((g for g in recomputed.get("gates", []) if g.get("id") == "V2-pacing"), {})
    ev = gate.get("evidence", {})
    inp = {"rate": float(row.rate), "ratio_A_over_lambda": ev.get("ratio_A_over_lambda"), "lateness_p99_ms": ev.get("lateness_p99_ms"),
           "event_loop_p99_ms": met.get("event_loop_p99_ms"), "caliper_cpu_mean_pct": met.get("caliper_cpu_mean_pct"),
           "failure_rate": met.get("failure_rate"), "max_inflight_backlog": met.get("max_inflight_backlog"),
           "interior_valid_rate_tps": met.get("interior_valid_rate_tps")}
    c = classify_v2(inp, thr)
    f = c["flags"]
    att = attribute(c["labels_v2"], component_cpu, thr)
    orig_labels = (verdict_recorded or {}).get("labels", [])
    el = el_stats or {}
    base.update(
        status="EXECUTED", original_verdict=(verdict_recorded or {}).get("verdict", ""), original_labels=orig_labels,
        original_labels_reproduced_by_driver_code=(sorted(recomputed.get("labels", [])) == sorted(orig_labels)
                                                   and recomputed.get("verdict") == (verdict_recorded or {}).get("verdict")),
        original_el_p50_ms=el.get("p50_ms"), original_el_p99_ms=el.get("p99_ms"),
        original_clause1_el_gt_10ms=None if el.get("p99_ms") is None else el["p99_ms"] > 10.0,
        pacing_ratio=inp["ratio_A_over_lambda"], pacing_ratio_ok=f["pacing_ratio_ok"], lateness_p99_ms=inp["lateness_p99_ms"],
        lateness_ok=f["lateness_ok"], el_p99_ms_v2_gate_diagnostic=inp["event_loop_p99_ms"], el_v2_gate_ok=f["el_v2_gate_ok"],
        v2_gate_pass=f["v2_gate_pass"], caliper_cpu_mean_pct=inp["caliper_cpu_mean_pct"], client_cpu_ok=f["client_cpu_ok"],
        interior_valid_rate_tps=inp["interior_valid_rate_tps"], delivery_ratio=f["delivery_ratio"], delivery_ok=f["delivery_ok"],
        max_inflight_backlog=inp["max_inflight_backlog"], backlog_ok=f["backlog_ok"], failure_rate=inp["failure_rate"],
        failures_ok=f["failures_ok"], corrected_labels=c["labels_v2"], corrected_class=c["class_v2"],
        cpu_at_limit_components=att["cpu_at_limit_components"], attribution=att["attribution"],
        ramp_may_continue_original=ramp_may_continue((verdict_recorded or {}).get("verdict"), "+".join(orig_labels) if orig_labels else HEALTHY),
        ramp_may_continue_corrected=ramp_may_continue(recomputed.get("verdict"), c["class_v2"]),
        note="; ".join(filter(None, ["missing evidence: " + ",".join(c["missing_evidence"]) if c["missing_evidence"] else "",
                                     "" if att["attribution"] in ("NONE",) else att["attribution"]])))
    return {col: _fmt(base.get(col)) for col in CSV_COLUMNS}


def write_new(path: Path, text: str) -> str:
    """Create a new file (never overwrite) and return its SHA-256."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "x", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def csv_text(rows: list[dict]) -> str:
    import io
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def sha256_file(p: Path) -> str | None:
    p = Path(p)
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None


def derive(args) -> int:
    """Re-evaluate every row of the frozen matrix offline with the driver's own evaluator and write the derived table."""
    here = Path(__file__).resolve().parent
    sys.path.insert(0, str(here))
    import e1_quicklook as ql                                   # the frozen driver, imported read-only

    ctx = ql.Ctx.default()
    if args.series_root:
        ctx = __import__("dataclasses").replace(ctx, root=Path(args.series_root))
    if args.matrix:
        ctx = __import__("dataclasses").replace(ctx, matrix=Path(args.matrix))
    rows = ql.load_matrix(ctx.matrix)                           # hash-pinned: refuses any other matrix
    out_rows, provenance = [], []
    for r in rows:
        rd = ctx.rd(r.run_id)
        vfile = rd / "run_verdict.json"
        sfile = ctx.root / "skipped" / f"{r.run_id}.json"
        if vfile.is_file():
            recorded = json.loads(vfile.read_text(encoding="utf-8"))
            facts = ql.load_json(rd / "facts.json") or {}
            recomputed = ql.evaluate_run(rd, r, ctx.recorder_dir(r.run_id), facts)
            mb = ql.recorder_base(r, ctx, "measured")
            mm = ql.load_json(mb.with_name(mb.name + ".manifest.json")) or {}
            el_stats = mm.get("event_loop_delay")
            comp = {}
            samples = rd / "resources" / "resource_samples.csv"
            t0, t1 = (mm.get("clock") or {}).get("wall0_ms"), (mm.get("clock") or {}).get("wall1_ms")
            if samples.is_file() and t0 and t1:
                names = sorted({x.get("container") for x in ql._read_csv(samples) if x.get("container")})
                comp = {n: ql._cpu_mean_in_window(samples, n, float(t0), float(t1)) for n in names}
            out_rows.append(build_row(r, recorded, None, recomputed, el_stats, comp or None))
            provenance.append({"run_id": r.run_id, "run_verdict_sha256": sha256_file(vfile), "facts_sha256": sha256_file(rd / "facts.json"),
                               "recorder_manifest_sha256": sha256_file(mb.with_name(mb.name + ".manifest.json")),
                               "analysis_summary_sha256": sha256_file(rd / "analysis" / "summary.json"),
                               "resource_samples_sha256": sha256_file(samples)})
        else:
            skipped = json.loads(sfile.read_text(encoding="utf-8")) if sfile.is_file() else None
            out_rows.append(build_row(r, None, skipped, None, None, None))
            provenance.append({"run_id": r.run_id, "skipped_record_sha256": sha256_file(sfile)})
    text = csv_text(out_rows)
    csv_sha = write_new(Path(args.out_csv), text)
    executed = [x for x in out_rows if x["status"] == "EXECUTED"]
    meta = {
        "tool": TOOL_VERSION, "erratum": ERRATUM_ID, "created_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "driver_e1_quicklook_sha256": sha256_file(here / "e1_quicklook.py"), "matrix": str(ctx.matrix),
        "matrix_sha256": sha256_file(ctx.matrix), "series_root": str(ctx.root), "thresholds_v2": asdict(THR2),
        "withdrawn_clause": {"name": "client_event_loop_p99_ms", "value": ql.THR.client_event_loop_p99_ms,
                             "reason": "monitorEventLoopDelay resolution 10 ms: the recorded p99 cannot be below 10 ms"},
        "counts": {"rows": len(out_rows), "executed": len(executed), "skipped": len(out_rows) - len(executed),
                   "original_client_limited": sum(1 for x in executed if "CLIENT_LIMITED" in x["original_labels"]),
                   "corrected_healthy": sum(1 for x in executed if x["corrected_class"] == HEALTHY),
                   "original_labels_reproduced": sum(1 for x in executed if x["original_labels_reproduced_by_driver_code"] == "true"),
                   "ramp_may_continue_original": sum(1 for x in executed if x["ramp_may_continue_original"] == "true"),
                   "ramp_may_continue_corrected": sum(1 for x in executed if x["ramp_may_continue_corrected"] == "true")},
        "csv_sha256": csv_sha, "inputs": provenance}
    write_new(Path(args.out_json), json.dumps(meta, indent=2, sort_keys=True) + "\n")
    print(json.dumps(meta["counts"], sort_keys=True))
    print("csv sha256", csv_sha)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").strip().splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("derive", help="derive the corrected classification of the 28 frozen rows (read-only on the evidence)")
    d.add_argument("--out-csv", required=True)
    d.add_argument("--out-json", required=True)
    d.add_argument("--series-root")
    d.add_argument("--matrix")
    args = ap.parse_args(argv)
    return derive(args)


if __name__ == "__main__":
    raise SystemExit(main())
