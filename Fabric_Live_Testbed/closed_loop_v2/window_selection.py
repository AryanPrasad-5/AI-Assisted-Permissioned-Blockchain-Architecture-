"""Deterministic window ranking from HISTORICAL ARRIVALS ONLY (numpy; Windows). No controller output, no Fabric measurement and no X0
result enters this module, so the ranking can be frozen before any measurement.

Candidates: every 600-s window aligned on 600 s in the protocol day, with 660 s of history before it (10 warm-up minutes + 1 pre-roll
minute), excluding the F_82800 window already used by the cl01-cl12 series. Eligible iff every 60-s mean rate >= 5 tx/s and
R_robust >= 1.5. Score = R_robust (second-largest / second-smallest of the ten 60-s means: robust to a single-minute spike);
ties go to the earliest start. The top FALLBACK_TOP_N form the fallback list used by protocol.select_k_over_ranked_windows.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from . import REPO_ROOT
from .protocol import WINDOW, robust_ratio

ARRIVALS = REPO_ROOT / "data" / "processed" / "flashbots_ms" / f"arrivals_{WINDOW['day']}_ms.npy"


def per_second_counts(arrivals_ms: np.ndarray) -> np.ndarray:
    a = np.asarray(arrivals_ms, dtype=np.int64)
    if a.min() < 0 or a.max() >= 86_400_000:
        raise ValueError("arrival offsets must lie in one UTC day")
    return np.bincount(a // 1000, minlength=86_400).astype(np.int64)


def minute_rates(counts_s: np.ndarray) -> np.ndarray:
    return counts_s.reshape(-1, 60).mean(axis=1)


def rank_windows(counts_s: np.ndarray) -> list[dict]:
    m = minute_rates(counts_s)
    L, step = WINDOW["length_s"], WINDOW["alignment_s"]
    need_before = (WINDOW["warmup_min"] + WINDOW["preroll_min"]) * 60
    out = []
    for start_s in range(0, 86_400 - L + 1, step):
        if start_s < need_before or start_s in WINDOW["exclude_start_s"]:
            continue
        i0 = start_s // 60
        seg = [float(v) for v in m[i0:i0 + L // 60]]
        if len(seg) != L // 60 or min(seg) < WINDOW["min_segment_rate_tps"]:
            continue
        r = robust_ratio(seg)
        if r < WINDOW["min_robust_ratio"]:
            continue
        pre = float(m[i0 - 1])
        warm = [float(v) for v in m[i0 - 1 - WINDOW["warmup_min"]:i0 - 1]]
        out.append({"start_s": start_s, "score_R_robust": r, "segment_rates_tps": seg, "preroll_rate_tps": pre,
                    "warmup_rates_tps": warm, "min_rate_tps": min(seg), "max_rate_tps": max(seg)})
    out.sort(key=lambda c: (-c["score_R_robust"], c["start_s"]))
    for rank, c in enumerate(out, 1):
        c["rank"] = rank
    return out


def load_ranking(path: Path = ARRIVALS) -> tuple[list[dict], dict]:
    raw = Path(path).read_bytes()
    arr = np.load(path)
    ranked = rank_windows(per_second_counts(arr))
    return ranked, {"arrivals_path": str(path), "arrivals_sha256": hashlib.sha256(raw).hexdigest(), "n_arrivals": int(arr.size),
                    "n_eligible_windows": len(ranked)}


if __name__ == "__main__":
    ranked, src = load_ranking()
    print(json.dumps({**src, "top10": ranked[:10]}, indent=1))
