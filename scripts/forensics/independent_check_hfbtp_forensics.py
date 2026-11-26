"""Independent recomputation of the pre-registered HFBTP forensic verdict inputs.

Purpose
    A second code path for the headline numbers behind H1, H2 (fixed-n parts),
    H3 and H5 in
    docs/research/forensics/hfbtp_forensic_validation_report_2026-11-09.md,
    compared value by value against the stored
    docs/research/forensics/results_2026-11-09/run_01/results.json.

Independence
    Written from the pre-registration text (prereg v0.1 section 1-3 and the
    v0.2 amendment), not from scripts/forensics/hfbtp_allrow_forensics.py.
    It imports nothing from that script, from scripts/construct_validity or
    from src/fabric_perf, and uses the Python standard library only: the csv
    parser, float(), and exact integer arithmetic for the timer-class floors
    (c is held as an integer number of 0.5 ms steps, so floor(c*S) has no
    floating-point ties). Shared inputs with the generator: the pinned CSV and
    the pre-registration text. A shared misreading of the prereg is therefore
    not excluded (this is a T4-compute check of the arithmetic, not of the
    hypotheses).

Computation class
    Descriptive HFBTP analysis under root PROJECT_RULES.md, Computation boundary:
    reads HFBTP.csv and the stored results.json, prints to the console and
    writes nothing. It evaluates only pre-registered fixed values (n = 1000 and
    the prereg v0.1 section 5 sensitivity set). It does not run the
    n = 100..5000 profile scan, the P6 cap-switch scan (H4) or P8/H6.

Usage (from the repository root)
    .venv/Scripts/python.exe -B -I scripts/forensics/independent_check_hfbtp_forensics.py

Exit codes
    0  every compared value agrees within tolerance
    1  at least one compared value disagrees (printed as MISMATCH)
    2  input integrity failure (hash pin) or missing input
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DATA = REPO / "data" / "raw" / "hfbtp" / "HFBTP.csv"
STORED = REPO / "docs" / "research" / "forensics" / "results_2026-11-09" / "run_01" / "results.json"

PIN_SHA256 = "ce295d6451787fefcfe2101579238cda4bede8539bc302e6a482aa4e5782bd5e"
PIN_MD5 = "e9275c49a2d7dc1f2a4015b6d8b28482"

N_PRIMARY = 1000
N_SENSITIVITY = (900, 950, 990, 995, 999, 1000, 1001, 1005, 1010, 1050, 1100)
NBOUND_AT = (900, 999, 1000, 1001, 1010, 1050, 1100, 2000)

# c range of amendment v0.2, [2.001, 2.035] s, in 0.5 ms steps (c = j / 2000 s).
C_STEPS = range(4002, 4071)

ROUND_T = 0.05  # T and A printed to 0.1
ROUND_MAX = 0.005  # Max latency printed to 0.01
RESIDUE_ALLOWANCE = 2
JITTER_ALLOWANCE_S = 0.1

TOL = 1e-6  # stored shares and medians are rounded to 6 decimals
BANDS = (("20-45", 20, 45), ("50-95", 50, 95), ("100-145", 100, 145), ("150-200", 150, 200))

COLUMNS = {
    "S": "Set Transaction Arrival Rate",
    "A": "Actual Transaction Arrival Rate",
    "B": "Block Size",
    "O": "Orderers",
    "T": "Throughput",
    "avg": "Avg Latency",
    "min": "Min Latency",
    "max": "Max Latency",
}


# ----------------------------------------------------------------- inputs
def file_hashes(path: Path) -> tuple[str, str]:
    data = path.read_bytes()
    return hashlib.sha256(data).hexdigest(), hashlib.md5(data).hexdigest()


def load_rows(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        header = [h.strip() for h in reader.fieldnames or []]
        missing = [c for c in COLUMNS.values() if c not in header]
        if missing:
            raise SystemExit(f"missing columns {missing}")
        rows = []
        for raw in reader:
            rec = {k.strip(): v for k, v in raw.items()}
            row = {key: float(rec[col]) for key, col in COLUMNS.items()}
            if row["B"] != int(row["B"]) or row["O"] != int(row["O"]) or row["S"] != int(row["S"]):
                raise SystemExit(f"non-integer key in row {rec}")
            row["B"], row["O"], row["S"] = int(row["B"]), int(row["O"]), int(row["S"])
            rows.append(row)
    return rows


def band(s: int) -> str | None:
    for name, lo, hi in BANDS:
        if lo <= s <= hi:
            return name
    return None


# ------------------------------------------------------- per-row formulas
def drain(n: int, t: float, a: float) -> float:
    """D = W - R = n(1/T - 1/A) (prereg v0.1 section 1)."""
    return n * (1.0 / t - 1.0 / a)


def spacing(n: int, a: float) -> float:
    """Observed create spacing s = R/(n-1) = n/((n-1)A)."""
    return n / ((n - 1) * a)


def rhat(n: int, t: float, a: float, mx: float) -> float:
    return 1.0 + (mx - drain(n, t, a)) / spacing(n, a)


def corners(n: int, row: dict) -> tuple[float, float, float, float]:
    """(rhat_lo, rhat_hi, D_lo, D_hi) over the 8 rounding corners."""
    rh, dd = [], []
    for t in (row["T"] - ROUND_T, row["T"] + ROUND_T):
        for a in (row["A"] - ROUND_T, row["A"] + ROUND_T):
            for mx in (row["max"] - ROUND_MAX, row["max"] + ROUND_MAX):
                rh.append(rhat(n, t, a, mx))
                dd.append(drain(n, t, a))
    return min(rh), max(rh), min(dd), max(dd)


def pacing_ok(row: dict) -> bool:
    ratio = row["A"] / row["S"]
    return 0.98 <= ratio <= 1.02


def count_eligible(row: dict) -> bool:
    # S >= 50, B <= 115, (B-1)/S <= 1.0 s  (integer form: B - 1 <= S)
    return row["S"] >= 50 and row["B"] <= 115 and row["B"] - 1 <= row["S"]


def p4_eligible(row: dict) -> bool:
    # 20 <= S <= 55 and B >= floor(2.035 S) + 6  (exact: 2035*S // 1000)
    s = row["S"]
    return 20 <= s <= 55 and row["B"] >= (2035 * s) // 1000 + 6


def p5_eligible(row: dict) -> bool:
    # S >= 20 and (B >= 410 or (B-1)/S >= 2.5 s)  (integer form: 2(B-1) >= 5S)
    return row["S"] >= 20 and (row["B"] >= 410 or 2 * (row["B"] - 1) >= 5 * row["S"])


def timer_class(n: int, s_level: int, j: int) -> tuple[int, int, int]:
    """(m, k, q) for c = j/2000 s and scheduled spacing 1/S (amendment v0.2)."""
    m = (j * s_level) // 2000 + 1
    k = -(-n // m)
    q = n - (k - 1) * m
    return m, k, q


def quantile7(values: list[float], p: float) -> float:
    """Linear-interpolation quantile (Hyndman-Fan type 7, numpy default)."""
    xs = sorted(values)
    h = (len(xs) - 1) * p
    lo = math.floor(h)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (h - lo) * (xs[hi] - xs[lo])


def ols_slope(x: list[float], y: list[float]) -> tuple[float, float]:
    mx, my = statistics.fmean(x), statistics.fmean(y)
    sxx = sum((xi - mx) ** 2 for xi in x)
    sxy = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    slope = sxy / sxx
    return slope, my - slope * mx


def share(flags: list[bool]) -> float:
    return sum(flags) / len(flags) if flags else float("nan")


# --------------------------------------------------------------- analyses
def count_block(rows: list[dict], n: int) -> dict:
    """P1 (residue), P2 (tail contrast), P3 (residual time), L-hat and K-hat."""
    pos, zero = [], []
    for row in rows:
        if not (pacing_ok(row) and count_eligible(row)):
            continue
        r = n % row["B"]
        rlo, rhi, _, _ = corners(n, row)
        s = spacing(n, row["A"])
        d = drain(n, row["T"], row["A"])
        rec = {"row": row, "r": r, "rlo": rlo, "rhi": rhi, "s": s, "D": d,
               "rhat": rhat(n, row["T"], row["A"], row["max"])}
        if r > 0:
            rec["agree"] = rlo - RESIDUE_ALLOWANCE <= r <= rhi + RESIDUE_ALLOWANCE
            rec["agree1"] = rlo - 1 <= r <= rhi + 1
            rec["u"] = (r - 1) * s
            rec["K"] = d + rec["u"]
            # Chance: residue uniform on 1..B-1, share of those inside the agreement interval.
            lo = max(1, math.ceil(rlo - RESIDUE_ALLOWANCE))
            hi = min(row["B"] - 1, math.floor(rhi + RESIDUE_ALLOWANCE))
            rec["chance_1_to_Bm1"] = max(0, hi - lo + 1) / (row["B"] - 1)
            lo0 = max(0, math.ceil(rlo - RESIDUE_ALLOWANCE))
            hi0 = min(row["B"] - 1, math.floor(rhi + RESIDUE_ALLOWANCE))
            rec["chance_0_to_Bm1"] = max(0, hi0 - lo0 + 1) / row["B"]
            pos.append(rec)
        else:
            b = row["B"]
            rec["agree"] = rlo - RESIDUE_ALLOWANCE - JITTER_ALLOWANCE_S / s <= b <= rhi + RESIDUE_ALLOWANCE
            zero.append(rec)

    out = {"eligible": len(pos) + len(zero), "n_pos": len(pos), "n_zero": len(zero)}
    out["p1_pos"] = share([p["agree"] for p in pos])
    out["p1_zero"] = share([z["agree"] for z in zero])
    out["combined"] = share([p["agree"] for p in pos] + [z["agree"] for z in zero])
    if n != N_PRIMARY:
        return out

    out["p1_pos_allow1"] = share([p["agree1"] for p in pos])
    out["median_signed_err"] = statistics.median(p["rhat"] - p["r"] for p in pos)
    out["chance_1_to_Bm1"] = statistics.fmean(p["chance_1_to_Bm1"] for p in pos)
    out["chance_0_to_Bm1"] = statistics.fmean(p["chance_0_to_Bm1"] for p in pos)
    out["p1_pos_by_O"] = {o: share([p["agree"] for p in pos if p["row"]["O"] == o]) for o in (3, 5, 7, 9)}
    out["p1_pos_by_band"] = {bn: share([p["agree"] for p in pos if band(p["row"]["S"]) == bn])
                             for bn in ("50-95", "100-145", "150-200")}
    out["n_zero_by_O"] = {o: sum(1 for z in zero if z["row"]["O"] == o) for o in (3, 5, 7, 9)}

    d_pos = statistics.median(p["D"] for p in pos)
    d_zero = statistics.median(z["D"] for z in zero)
    out["median_D_pos"], out["median_D_zero"], out["contrast_D"] = d_pos, d_zero, d_pos - d_zero
    out["TA_pos"] = statistics.median(p["row"]["T"] / p["row"]["A"] for p in pos)
    out["TA_zero"] = statistics.median(z["row"]["T"] / z["row"]["A"] for z in zero)
    out["contrast_by_O"] = {
        o: statistics.median(p["D"] for p in pos if p["row"]["O"] == o)
        - statistics.median(z["D"] for z in zero if z["row"]["O"] == o)
        for o in (3, 5, 7, 9)
    }

    slope, intercept = ols_slope([p["u"] for p in pos], [p["D"] for p in pos])
    ks = [p["K"] for p in pos]
    out["slope"], out["intercept"] = slope, intercept
    out["K_median"] = statistics.median(ks)
    out["K_iqr"] = quantile7(ks, 0.75) - quantile7(ks, 0.25)
    out["max_minus_K"] = statistics.median(p["row"]["max"] - p["K"] for p in pos)
    out["c_hat"] = out["K_median"] - d_zero
    return out


def level_L(rows: list[dict], n: int) -> dict[int, float]:
    """L-hat(level): median D of count-eligible r = 0 rows; S < 50 uses pooled S 50-60."""
    by_level: dict[int, list[float]] = {}
    for row in rows:
        if pacing_ok(row) and count_eligible(row) and n % row["B"] == 0:
            by_level.setdefault(row["S"], []).append(drain(n, row["T"], row["A"]))
    lhat: dict = {s: statistics.median(v) for s, v in by_level.items()}
    pooled = [d for s, v in by_level.items() if 50 <= s <= 60 for d in v]
    # No r = 0 row exists when no block size divides n (e.g. n = 995, 999, 1001, since every
    # HFBTP block size is a multiple of 5). L-hat, and hence T_cyc, is then undefined.
    lhat["pooled"] = statistics.median(pooled) if pooled else None
    return lhat


def l_for(lhat: dict, s: int) -> float | None:
    return lhat.get(s) if s >= 50 else lhat["pooled"]


def union_share(intervals: list[tuple[float, float]], lo_a: float, hi_a: float) -> float:
    """Length of the union of intervals inside [lo_a, hi_a], as a share of that window (max 1)."""
    pieces = sorted((max(lo_a, a), min(hi_a, b)) for a, b in intervals if min(hi_a, b) > max(lo_a, a))
    covered, cur_lo, cur_hi = 0.0, None, None
    for a, b in pieces:
        if cur_hi is None or a > cur_hi:
            if cur_hi is not None:
                covered += cur_hi - cur_lo
            cur_lo, cur_hi = a, b
        else:
            cur_hi = max(cur_hi, b)
    if cur_hi is not None:
        covered += cur_hi - cur_lo
    return min(1.0, covered / (hi_a - lo_a))


def p4_block(rows: list[dict], n: int) -> dict:
    """P4: low-load cycle residue, membership over the c range (amendment v0.2)."""
    flags, chance_min_m, chance_as_run, by_o, by_b = [], [], [], {}, {}
    for row in rows:
        if not (pacing_ok(row) and p4_eligible(row)):
            continue
        classes = {timer_class(n, row["S"], j) for j in C_STEPS}
        rlo, rhi, _, _ = corners(n, row)
        s = spacing(n, row["A"])
        lo_bound = rlo - RESIDUE_ALLOWANCE - JITTER_ALLOWANCE_S / s
        hi_bound = rhi + RESIDUE_ALLOWANCE
        member = any(lo_bound <= q <= hi_bound for (_, _, q) in classes)
        flags.append(member)
        by_o.setdefault(row["O"], []).append(member)
        by_b.setdefault(band(row["S"]), []).append(member)
        lo, ms = max(1, math.ceil(lo_bound)), sorted({c[0] for c in classes})
        # Reading 1 (independent): residue uniform on 1..m for the smallest attainable m.
        chance_min_m.append(max(0, min(ms[0], math.floor(hi_bound)) - lo + 1) / ms[0])
        # Reading 2 (as run, taken from the generator after comparison): p = width / m_max over
        # 1..m_max, then 1 - (1 - p)^(number of attainable m values).
        width = max(0, min(ms[-1], math.floor(hi_bound)) - lo + 1)
        chance_as_run.append(1 - (1 - min(1.0, width / ms[-1])) ** len(ms))
    return {
        "rows": len(flags),
        "share": share(flags),
        "by_O": {o: share(v) for o, v in sorted(by_o.items())},
        "by_band": {b: share(v) for b, v in sorted(by_b.items())},
        "chance_min_m": statistics.fmean(chance_min_m) if flags else float("nan"),
        "chance_as_run": statistics.fmean(chance_as_run) if flags else float("nan"),
    }


def p5_block(rows: list[dict], n: int, lhat: dict, detail: bool) -> dict:
    """P5: cycle throughput membership over the c range (amendment v0.2), two readings.

    Amendment v0.2 says a row is a member if it "matches any (m, k) class attainable for c
    in the range". That does not say which T_cyc represents a class.
      any_c   (independent reading): T within tol_T of T_cyc(c) for any c on the 0.5 ms grid.
      as_run  (generator's reading, scripts/forensics/hfbtp_allrow_forensics.py:371-403,
               identified after the first comparison): classes keyed by k only; each class is
               represented by T_cyc at the median grid point of that k.
    """
    res = {key: {"flags": [], "rel": [], "chance": [], "by_O": {}, "by_band": {}} for key in ("any_c", "as_run")}
    undefined = 0
    for row in rows:
        if not (pacing_ok(row) and p5_eligible(row)):
            continue
        s_obs = spacing(n, row["A"])
        big_l = l_for(lhat, row["S"])
        if big_l is None:
            undefined += 1
            continue
        grid, by_k = [], {}
        for j in C_STEPS:
            m, k, _ = timer_class(n, row["S"], j)
            t_cyc = n / ((k - 1) * m * s_obs + j / 2000.0 + big_l)
            grid.append((t_cyc, max(1.0, t_cyc * t_cyc * 0.15 / n)))
            by_k.setdefault(k, []).append(len(grid) - 1)
        reps = [grid[idx[len(idx) // 2]] for _, idx in sorted(by_k.items())]
        t = row["T"]
        for key, preds in (("any_c", grid), ("as_run", reps)):
            r = res[key]
            member = any(abs(t - tc) <= tol for tc, tol in preds)
            r["flags"].append(member)
            r["by_O"].setdefault(row["O"], []).append(member)
            r["by_band"].setdefault(band(row["S"]), []).append(member)
            if detail:
                nearest = min(preds, key=lambda pr: abs(t - pr[0]))[0]
                r["rel"].append((t - nearest) / nearest)
                r["chance"].append(union_share([(tc - tol, tc + tol) for tc, tol in preds],
                                               0.5 * row["A"], row["A"]))
    out = {"undefined_rows": undefined}
    for key, r in res.items():
        o = {"rows": len(r["flags"]), "share": share(r["flags"]),
             "by_O": {k: share(v) for k, v in sorted(r["by_O"].items())},
             "by_band": {k: share(v) for k, v in sorted(r["by_band"].items())}}
        if detail and r["flags"]:
            o["median_rel_err"] = statistics.median(r["rel"])
            o["mean_chance"] = statistics.fmean(r["chance"])
        out[key] = o
    return out


def p7_block(rows: list[dict]) -> dict:
    """P7: medians of Avg and Max for B >= 410, S >= 170 minus S = 150 (pacing gate applied)."""
    big = [r for r in rows if pacing_ok(r) and r["B"] >= 410]
    base = [r for r in big if r["S"] == 150]
    high = [r for r in big if r["S"] >= 170]
    avg_150, avg_hi = statistics.median(r["avg"] for r in base), statistics.median(r["avg"] for r in high)
    max_150, max_hi = statistics.median(r["max"] for r in base), statistics.median(r["max"] for r in high)
    return {"avg_150": avg_150, "avg_170_200": avg_hi, "max_150": max_150, "max_170_200": max_hi,
            "delta_avg": avg_hi - avg_150, "delta_max": max_hi - max_150}


def nbound(rows: list[dict], n: int) -> float:
    """Share of all rows with D_lo > Max + 0.005 (lenient corner)."""
    hits = 0
    for row in rows:
        d_lo = n * (1.0 / (row["T"] + ROUND_T) - 1.0 / (row["A"] - ROUND_T))
        hits += d_lo > row["max"] + ROUND_MAX
    return hits / len(rows)


# ------------------------------------------------------- decision rules
def h1_verdict(p1: dict, profile_max_stored: float) -> str:
    strata = list(p1["p1_pos_by_O"].values()) + list(p1["p1_pos_by_band"].values())
    support = (p1["p1_pos"] >= 0.80 and min(strata) >= 0.70 and -1.10 <= p1["slope"] <= -0.90
               and -1 <= p1["median_signed_err"] <= 1)
    # Falsified if any: profile max < 0.50; slope outside [-1.30, -0.70] at n = 1000 and at the
    # profile-best n (stored profile-best n = 1000, so one slope covers both); P2 contrast < 0.5 s.
    falsified = (profile_max_stored < 0.50 or not (-1.30 <= p1["slope"] <= -0.70)
                 or p1["contrast_D"] < 0.5)
    return "SUPPORTED" if support else "FALSIFIED" if falsified else "INCONCLUSIVE"


def h3_falsified(p4_share: float, p5_share: float) -> bool:
    # Amendment v0.2: FALSIFIED if P4 < 0.50 or P5 < 0.50 (or median relative error beyond 5%).
    return p4_share < 0.50 or p5_share < 0.50


def h5_verdict(p7: dict) -> str:
    if p7["delta_avg"] < 0.15 and p7["delta_max"] < 0.30:
        return "AGAINST_SUSTAINED_BACKLOG"
    if p7["delta_avg"] > 0.40:
        return "CONSISTENT_WITH_BACKLOG"
    return "INCONCLUSIVE"


# ------------------------------------------------------------ comparison
class Ledger:
    def __init__(self) -> None:
        self.items: list[tuple[str, object, object, str]] = []

    def num(self, name: str, stored, mine, tol: float = TOL) -> None:
        if stored is None:
            self.items.append((name, "not stored", mine, "NOT COMPARED"))
            return
        ok = abs(float(stored) - float(mine)) <= tol
        self.items.append((name, stored, mine, "MATCH" if ok else "MISMATCH"))

    def exact(self, name: str, stored, mine) -> None:
        self.items.append((name, stored, mine, "MATCH" if stored == mine else "MISMATCH"))

    def info(self, name: str, stored, mine, note: str) -> None:
        self.items.append((name, stored, mine, note))

    def report(self) -> int:
        width = max(len(i[0]) for i in self.items)
        for name, stored, mine, status in self.items:
            m = f"{mine:.6f}" if isinstance(mine, float) else str(mine)
            print(f"{name:<{width}}  stored={stored!s:<28} recomputed={m:<28} {status}")
        mism = sum(1 for i in self.items if i[3] == "MISMATCH")
        match = sum(1 for i in self.items if i[3] == "MATCH")
        sens = sum(1 for i in self.items if i[3] == "SENSITIVITY")
        other = len(self.items) - mism - match - sens
        print(f"\nSUMMARY: {match} MATCH, {mism} MISMATCH, {sens} SENSITIVITY (alternative reading, "
              f"not compared), {other} not evaluable / not recomputed / informational")
        return 1 if mism else 0


def main() -> int:
    if not DATA.exists() or not STORED.exists():
        print(f"missing input: {DATA if not DATA.exists() else STORED}")
        return 2
    sha, md5 = file_hashes(DATA)
    print(f"HFBTP.csv sha256={sha} md5={md5}")
    if sha != PIN_SHA256 or md5 != PIN_MD5:
        print("INTEGRITY FAILURE: HFBTP.csv does not match the pinned hashes")
        return 2
    stored = json.loads(STORED.read_text(encoding="utf-8"))
    rows = load_rows(DATA)
    led = Ledger()
    led.exact("rows", 24687, len(rows))
    led.exact("pacing_excluded", stored["all_rows"]["by_regime"]["pacing_excluded"]["rows"],
              sum(1 for r in rows if not pacing_ok(r)))

    # ---- P1, P2, P3 at n = 1000
    c = count_block(rows, N_PRIMARY)
    sp1, sp2, sp3 = stored["P1"], stored["P2"], stored["P3"]
    led.exact("P1.eligible_rows", sp1["eligible_rows"], c["eligible"])
    led.exact("P1.r_pos_rows", sp1["r_pos_rows"], c["n_pos"])
    led.exact("P1.r_zero_rows", sp1["r_zero_rows"], c["n_zero"])
    led.exact("P1.r_zero_rows_by_O", {int(k): v["n"] for k, v in sp1["r_zero"]["by_orderers"].items()},
              c["n_zero_by_O"])
    led.num("P1.r_pos.share", sp1["r_pos"]["overall"]["share"], c["p1_pos"])
    led.num("P1.r_zero.share", sp1["r_zero"]["overall"]["share"], c["p1_zero"])
    led.num("P1.combined_share", sp1["combined_share"], c["combined"])
    led.num("P1.allowance_1_share_r_pos", sp1["allowance_1_share_r_pos"], c["p1_pos_allow1"])
    led.num("P1.median_signed_error_r_pos", sp1["median_signed_error_r_pos"], c["median_signed_err"])
    for o in (3, 5, 7, 9):
        led.num(f"P1.r_pos.by_O[{o}]", sp1["r_pos"]["by_orderers"][str(o)]["share"], c["p1_pos_by_O"][o])
    for bn in ("50-95", "100-145", "150-200"):
        led.num(f"P1.r_pos.by_band[{bn}]", sp1["r_pos"]["by_band"][bn]["share"], c["p1_pos_by_band"][bn])
    chance_stored = sp1["mean_chance_r_pos"]
    led.num("P1.mean_chance (uniform 1..B-1)", chance_stored, c["chance_1_to_Bm1"])
    led.info("P1.mean_chance (uniform 0..B-1, alt.)", chance_stored, c["chance_0_to_Bm1"], "INFO")
    led.num("P2.median_D_r_pos", sp2["median_D_r_pos"], c["median_D_pos"])
    led.num("P2.median_D_r_zero", sp2["median_D_r_zero"], c["median_D_zero"])
    led.num("P2.contrast_D", sp2["contrast_D"], c["contrast_D"])
    led.num("P2.median_T_over_A_r_pos", sp2["median_T_over_A_r_pos"], c["TA_pos"])
    led.num("P2.median_T_over_A_r_zero", sp2["median_T_over_A_r_zero"], c["TA_zero"])
    for o in (3, 5, 7, 9):
        led.num(f"P2.contrast_D.by_O[{o}]", sp2["by_orderers"][str(o)]["contrast_D"], c["contrast_by_O"][o])
    led.num("P3.slope_D_on_u", sp3["slope_D_on_u"], c["slope"])
    led.num("P3.intercept", sp3["intercept"], c["intercept"])
    led.num("P3.K_median", sp3["K_median"], c["K_median"])
    led.num("P3.K_iqr", sp3["K_iqr"], c["K_iqr"])
    led.num("P3.max_minus_K_median", sp3["max_minus_K_median"], c["max_minus_K"])
    led.num("level_parameters.c_hat", stored["level_parameters"]["c_hat"], c["c_hat"])

    # ---- L-hat by level
    lhat = level_L(rows, N_PRIMARY)
    stored_l = stored["level_parameters"]["L_hat_by_level"]
    worst = max(abs(lhat[int(float(k))] - v) for k, v in stored_l.items())
    led.num(f"L_hat_by_level (max |diff| over {len(stored_l)} levels)", 0.0, worst)
    led.num("L_hat_pooled_S50_60", stored["level_parameters"]["L_hat_pooled_S50_60_for_S_below_50"], lhat["pooled"])

    # ---- P1 n-sensitivity (pre-registered set)
    for n in N_SENSITIVITY:
        cn = count_block(rows, n)
        st = sp1["n_sensitivity"][str(n)]
        led.exact(f"P1.n_sens[{n}].r_pos_rows", st["r_pos_rows"], cn["n_pos"])
        led.num(f"P1.n_sens[{n}].r_pos_share", st["r_pos_share"], cn["p1_pos"])
        led.num(f"P1.n_sens[{n}].combined_share", st["combined_share"], cn["combined"])

    # ---- N-bound
    for n in NBOUND_AT:
        led.num(f"N-bound[{n}]", stored["n_profile"]["nbound_at"][str(n)], nbound(rows, n))

    # ---- P4 and P5 at n = 1000
    sp4, sp5 = stored["P4"], stored["P5"]
    p4 = p4_block(rows, N_PRIMARY)
    led.exact("P4.rows", sp4["rows"], p4["rows"])
    led.num("P4.membership", sp4["membership"]["overall"]["share"], p4["share"])
    for o in (3, 5, 7, 9):
        led.num(f"P4.membership.by_O[{o}]", sp4["membership"]["by_orderers"][str(o)]["share"], p4["by_O"][o])
    for bn in ("20-45", "50-95"):
        led.num(f"P4.membership.by_band[{bn}]", sp4["membership"]["by_band"][bn]["share"], p4["by_band"][bn])
    led.num("P4.mean_chance [as-run reading]", sp4["mean_chance"], p4["chance_as_run"])
    led.info("P4.mean_chance [smallest-m reading]", sp4["mean_chance"], p4["chance_min_m"], "SENSITIVITY")

    p5 = p5_block(rows, N_PRIMARY, lhat, detail=True)
    run, alt = p5["as_run"], p5["any_c"]
    led.exact("P5.rows", sp5["rows"], run["rows"])
    led.num("P5.membership [as-run]", sp5["membership"]["overall"]["share"], run["share"])
    for o in (3, 5, 7, 9):
        led.num(f"P5.membership.by_O[{o}] [as-run]", sp5["membership"]["by_orderers"][str(o)]["share"], run["by_O"][o])
    for bn in ("20-45", "50-95", "100-145", "150-200"):
        led.num(f"P5.membership.by_band[{bn}] [as-run]", sp5["membership"]["by_band"][bn]["share"], run["by_band"][bn])
    led.num("P5.median_rel_err [as-run]", sp5["median_rel_err"], run["median_rel_err"])
    led.num("P5.mean_chance [as-run]", sp5["mean_chance"], run["mean_chance"])
    led.info("P5.membership [any-c reading]", sp5["membership"]["overall"]["share"], alt["share"], "SENSITIVITY")
    for o in (3, 5, 7, 9):
        led.info(f"P5.membership.by_O[{o}] [any-c]", sp5["membership"]["by_orderers"][str(o)]["share"], alt["by_O"][o], "SENSITIVITY")
    for bn in ("20-45", "50-95", "100-145", "150-200"):
        led.info(f"P5.membership.by_band[{bn}] [any-c]", sp5["membership"]["by_band"][bn]["share"], alt["by_band"][bn], "SENSITIVITY")
    led.info("P5.median_rel_err [any-c]", sp5["median_rel_err"], alt["median_rel_err"], "SENSITIVITY")
    led.info("P5.mean_chance [any-c]", sp5["mean_chance"], alt["mean_chance"], "SENSITIVITY")

    # ---- P4 / P5 n-sensitivity and H3 at every pre-registered n
    p4_sens = sp4.get("n_sensitivity", {})
    p5_sens = sp5.get("n_sensitivity", {})
    h3_all = {"as_run": True, "any_c": True}
    for n in N_SENSITIVITY:
        lh = level_L(rows, n)
        a4 = p4_block(rows, n)["share"]
        b5 = p5_block(rows, n, lh, detail=False)
        led.num(f"P4.n_sens[{n}]", p4_sens.get(str(n)), a4)
        if b5["as_run"]["rows"] == 0 and b5["undefined_rows"] > 0:
            # L-hat undefined at this n: P5 is not evaluable. Stored value is shown for the record.
            led.info(f"P5.n_sens[{n}]", p5_sens.get(str(n)), "undefined (no r=0 rows, L-hat undefined)",
                     "NOT EVALUABLE (stored 0.0 is not a measured 0% membership)")
            for key in h3_all:
                h3_all[key] &= a4 < 0.50
        else:
            led.num(f"P5.n_sens[{n}] [as-run]", p5_sens.get(str(n)), b5["as_run"]["share"])
            led.info(f"P5.n_sens[{n}] [any-c]", p5_sens.get(str(n)), b5["any_c"]["share"], "SENSITIVITY")
            for key in h3_all:
                h3_all[key] &= h3_falsified(a4, b5[key]["share"])
    for key, val in h3_all.items():
        led.info(f"H3 falsified at every pre-registered n [{key}]", "claimed (FE-2)", val,
                 "MATCH" if val else "MISMATCH")

    # ---- P7
    sp7 = stored["P7"]
    p7 = p7_block(rows)
    for key in ("avg_150", "avg_170_200", "max_150", "max_170_200", "delta_avg", "delta_max"):
        led.num(f"P7.{key}", sp7[key], p7[key])

    # ---- verdicts (recomputed from the values above)
    v = stored["verdicts"]
    led.exact("verdict H1", v["H1_small_block_final_partial_block"],
              h1_verdict(c, stored["n_profile"]["best_share"]))
    led.exact("verdict H3 [as-run P5]", v["H3_timer_cycles"],
              "FALSIFIED" if h3_falsified(p4["share"], run["share"]) else "NOT FALSIFIED")
    led.exact("verdict H3 [any-c P5]", v["H3_timer_cycles"],
              "FALSIFIED" if h3_falsified(p4["share"], alt["share"]) else "NOT FALSIFIED")
    led.exact("verdict H5", v["H5_backlog"], h5_verdict(p7))
    led.info("verdict H2 (fixed-n parts only)", v["H2_n_1000"],
             f"profile(1000)={c['combined']:.6f}, N-bound(1000)={nbound(rows, 1000):.6f}",
             "PARTIAL (full n-profile scan not recomputed)")
    led.info("verdict H4 / H6", f"{v['H4_cap_120_129']} / {v['H6_averaging']}", "-",
             "NOT RECOMPUTED (outside this check)")
    return led.report()


if __name__ == "__main__":
    sys.exit(main())
