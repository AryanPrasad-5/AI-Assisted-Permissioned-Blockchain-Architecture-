"""Independent re-derivation of the predictive_live decisions (stdlib; imports NO project forecasting, predictor or controller code).

Given the staged arm.json, the run's decision records and the run's own Caliper schedule files, it recomputes for every decision:
  - the per-second arrival history (from the schedule files the run actually replayed, not from the stage's stored counts),
  - the frozen EWMA forecast (own recurrence, alpha 0.005) from warm-up + rounds 0..d only,
  - each candidate's predicted latency (own log-rate linear interpolation of the staged X0 surface points) and delivered fraction,
  - each candidate's utility (throughput - 0.01/ms * latency - 1.0 * backlog) and the argmax,
and compares them with what the controller logged. A mismatch means the controller used something other than the declared inputs.
"""

from __future__ import annotations

import math
from pathlib import Path

ALPHA = 0.005
LAT_PEN, BACKLOG_PEN, DEADBAND = 0.01, 1.0, 0.95


def schedule_counts(ws_dir: Path, name: str, length_ms: int = 60_000) -> list[int]:
    text = (Path(ws_dir) / f"schedule_{name}.txt").read_text(encoding="ascii")
    offs = [int(x) for x in text.split("\n") if x.strip()]
    out = [0] * (length_ms // 1000)
    for v in offs:
        out[v // 1000] += 1
    return out


def ewma(hist: list[float]) -> float:
    e = float(hist[0])
    for x in hist[1:]:
        e = ALPHA * float(x) + (1.0 - ALPHA) * e
    return e


def interp(points: list[dict], b: int, rate: float, key: str) -> float:
    pts = sorted(((float(p["rate_tps"]), float(p[key])) for p in points if int(p["B"]) == b))
    if rate < pts[0][0] - 1e-9 or rate > pts[-1][0] + 1e-9:
        raise ValueError("outside surface")
    xs = [math.log(r) for r, _ in pts]
    x = math.log(max(rate, pts[0][0]))
    i = next((j for j in range(1, len(xs)) if xs[j] >= x), len(xs) - 1)
    w = 0.0 if xs[i] == xs[i - 1] else min(max((x - xs[i - 1]) / (xs[i] - xs[i - 1]), 0.0), 1.0)
    return pts[i - 1][1] + w * (pts[i][1] - pts[i - 1][1])


def check_predictive_live(arm: dict, decisions: list[dict], ws_dir: Path) -> list[dict]:
    checks: list[dict] = []

    def add(name, ok, detail=""):
        checks.append({"check": name, "pass": bool(ok), "detail": detail})

    pl = arm["predictive_live"]
    warm = [float(v) for v in pl["warmup_counts"]]
    staged = {int(k): v for k, v in pl["round_counts"].items()}
    recount = {r["index"]: schedule_counts(ws_dir, r["name"], r["length_ms"]) for r in arm["rounds"]}
    add("staged per-second counts == recount from the run's own schedule files", all(recount[i] == staged[i] for i in recount),
        f"{sum(1 for i in recount if recount[i] != staged[i])} rounds differ")
    add("warm-up history is 600 s", len(warm) == 600, str(len(warm)))
    pts = pl["surface_points"]
    bad_f, bad_h, bad_u, bad_l, bad_c = [], [], [], [], []
    abst = []
    for d in decisions:
        i = d["decision_index"]
        meta = d.get("controller_metadata") or {}
        if d.get("abstained"):
            abst.append({"decision": i, "reason": d.get("reason"), "error": meta.get("error")})
        f = meta.get("forecast_tps")
        if f is None:
            continue
        hist = warm + [float(v) for r in range(0, i + 1) for v in recount[r]]
        if abs(ewma(hist) - float(f)) > 1e-9:
            bad_f.append(i)
        if meta.get("history_seconds_used") != len(hist):
            bad_h.append(i)
        cands = meta.get("candidate_utilities") or []
        utils = {}
        for c in cands:
            b = int(c["config"]["max_message_count"])
            try:
                lat = interp(pts, b, float(f), "mean_ms")
                frac = min(1.0, interp(pts, b, float(f), "delivered_fraction"))
            except ValueError:
                bad_l.append((i, b))
                continue
            frac = 1.0 if frac >= DEADBAND else frac
            thr = float(f) * frac
            if abs(lat - float(c["latency_ms"])) > 1e-6 or abs(thr - float(c["throughput_tps"])) > 1e-6:
                bad_l.append((i, b))
            u = thr - LAT_PEN * lat - BACKLOG_PEN * max(0.0, float(f) - thr)
            utils[b] = u
            if abs(u - float(c["utility"])) > 1e-6:
                bad_u.append((i, b))
        if utils and d.get("changed"):
            best = max(sorted(utils), key=lambda b: (utils[b], -b))
            if int(d["target_B"]) != best or not utils[best] > utils.get(int(d["current_B"]), -1e18):
                bad_c.append(i)
        if utils and not d.get("changed") and not d.get("abstained"):
            best = max(utils.values())
            if utils.get(int(d["current_B"]), -1e18) < best - 1e-9 and meta.get("hold_cause") not in ("cooldown", "insufficient_margin"):
                bad_c.append(i)
    add("every logged forecast == independent EWMA of warm-up + rounds <= d only", not bad_f, f"decisions {bad_f}")
    add("history length used == 600 + 60 * (d + 1) seconds", not bad_h, f"decisions {bad_h}")
    add("every candidate latency/throughput == independent interpolation of the staged X0 surface", not bad_l, str(bad_l[:6]))
    add("every candidate utility == independent recomputation", not bad_u, str(bad_u[:6]))
    add("every switch is the utility argmax and an improvement; every other hold has a stated cause", not bad_c, f"decisions {bad_c}")
    add("abstentions listed (informational)", True, str(abst))
    return checks
