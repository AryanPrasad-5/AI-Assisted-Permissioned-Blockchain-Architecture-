"""Generate a seeded E1 run order. Plans runs; starts nothing.

Usage: python scripts/e1/e1_schedule.py --design core --out schedule.csv
Freeze the file before run 1. Never regenerate after any run has started.
"""
from __future__ import annotations

import argparse
import csv
from itertools import product
from pathlib import Path

import numpy as np

SEED = 20261106
DESIGNS = {"core": (40, 45), "extended": (40, 45, 500)}


def build(rates=(160, 170), blocks=(40, 45), reps=5, seed=SEED) -> list[dict]:
    cells = list(product(("txNumber", "txDuration"), rates, blocks))
    labels = np.repeat(np.arange(len(cells)), reps)
    rng = np.random.default_rng(seed)
    while True:
        order = rng.permutation(labels)
        if (order[1:] != order[:-1]).all():
            break
    counts = {}
    rows = []
    for slot, label in enumerate(order, start=1):
        cell = int(label)
        counts[cell] = counts.get(cell, 0) + 1
        mode, rate, block = cells[cell]
        rows.append({"slot": slot, "mode": mode, "rate": rate, "block": block, "rep": counts[cell]})
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--design", choices=DESIGNS, default="core")
    parser.add_argument("--low", type=int, default=160)
    parser.add_argument("--high", type=int, default=170)
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    rows = build((args.low, args.high), DESIGNS[args.design], seed=args.seed)
    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} slots to {args.out}")


if __name__ == "__main__":
    main()
