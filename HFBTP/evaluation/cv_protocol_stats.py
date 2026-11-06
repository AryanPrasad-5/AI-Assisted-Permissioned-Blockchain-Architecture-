"""Monte Carlo and interval calculations for protocol design. Uses no project data."""
from __future__ import annotations

import math

import numpy as np
from scipy import stats

SEED = 20261106


def warmup_gate_false_alarm(reps=400_000, threshold=0.95, phi=0.0, seed=SEED) -> float:
    """Per-run probability that 4 stable quarter means give |r| >= threshold with the index."""
    rng = np.random.default_rng(seed)
    shocks = rng.normal(size=(reps, 4))
    series = shocks.copy()
    for step in range(1, 4):
        series[:, step] = phi * series[:, step - 1] + shocks[:, step]
    x = np.arange(4) - 1.5
    centred = series - series.mean(axis=1, keepdims=True)
    r = (centred * x).sum(axis=1) / np.sqrt((centred**2).sum(axis=1) * (x**2).sum())
    return float((np.abs(r) >= threshold).mean())


def sd_interval(n: int) -> tuple[float, float]:
    return (math.sqrt((n - 1) / stats.chi2.ppf(0.975, n - 1)), math.sqrt((n - 1) / stats.chi2.ppf(0.025, n - 1)))


def wilcoxon_power(n: int, effect: float, alpha: float, reps=1500, seed=SEED) -> float:
    rng = np.random.default_rng(seed)
    draws = rng.normal(effect, 1, size=(reps, n))
    return float(np.mean([stats.wilcoxon(row, mode="exact").pvalue < alpha for row in draws]))


def minimum_wilcoxon_p(n: int) -> float:
    return 2 / 2**n


def screening_slots(points=12, reps=8, trials=3000, seed=SEED) -> dict:
    rng = np.random.default_rng(seed)
    total = points * reps
    found = []
    for _ in range(trials):
        while True:
            schedule = rng.permutation(np.repeat(np.arange(points), reps))
            for _attempt in range(200):
                clash = np.where(schedule[1:] == schedule[:-1])[0]
                if clash.size == 0:
                    break
                a, b = clash[0] + 1, rng.integers(total)
                schedule[a], schedule[b] = schedule[b], schedule[a]
            if (schedule[1:] != schedule[:-1]).all():
                break
        seen = set()
        for slot, value in enumerate(schedule):
            seen.add(int(value))
            if len(seen) == points:
                found.append(slot + 1)
                break
    values = np.array(found)
    return {"median": float(np.median(values)), "p5": float(np.percentile(values, 5)),
            "p95": float(np.percentile(values, 95)),
            "share_within_18_30": float(((values >= 18) & (values <= 30)).mean())}


def summary() -> dict:
    per_run = warmup_gate_false_alarm()
    return {
        "warmup_gate_false_alarm_per_run": per_run,
        "warmup_gate_pass_96_stable_runs": (1 - per_run) ** 96,
        "warmup_gate_false_alarm_ar1_phi_0_5": warmup_gate_false_alarm(reps=200_000, phi=0.5),
        "sd_interval_n3": sd_interval(3),
        "sd_interval_n5": sd_interval(5),
        "sd_interval_n8": sd_interval(8),
        "screening_slots": screening_slots(),
        "wilcoxon_power_e1_n5": {"minimum_p": minimum_wilcoxon_p(5)},
        "wilcoxon_power_n10_d1_5_alpha_0_0167": wilcoxon_power(10, 1.5, 0.0167),
    }
