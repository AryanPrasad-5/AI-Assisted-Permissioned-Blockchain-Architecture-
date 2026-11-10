"""Audit completed MaxMessageCount pilot artifacts without contacting Fabric."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


RUNS = [
    "p10r002b", "p20r002", "p30r002", "p40r002", "p50r002",
    "p10c001", "p20c001", "p30c001", "p40c001", "p50c001",
]


def audit_run(root: Path, run: str) -> dict:
    directory = root / run
    record = json.loads((directory / "pilot_run_record.json").read_text(encoding="utf-8"))
    blocks = list(csv.DictReader((directory / "ledger" / "ledger" / "blocks.csv").open(newline="", encoding="utf-8")))
    txs = list(csv.DictReader((directory / "ledger" / "ledger" / "ledger_tx.csv").open(newline="", encoding="utf-8")))
    by_block: dict[int, list[dict]] = {}
    for tx in txs:
        if tx.get("header_type") == "3":
            by_block.setdefault(int(tx["block"]), []).append(tx)
    block_audit = []
    for block in blocks:
        rows = by_block.get(int(block["number"]), [])
        timestamps = [int(row["ch_ts_ms"]) for row in rows if row.get("ch_ts_ms")]
        block_audit.append({
            "block_number": int(block["number"]),
            "transaction_count": int(block["tx_count"]),
            "inferred_cut_cause": block["inferred_cut_cause"],
            "first_transaction_timestamp_ms": min(timestamps) if timestamps else None,
            "last_transaction_timestamp_ms": max(timestamps) if timestamps else None,
            "transaction_timestamp_span_ms": max(timestamps) - min(timestamps) if timestamps else None,
            "timestamp_basis": "Fabric ChannelHeader transaction timestamps; not orderer block timestamp",
        })
    return {
        "run_id": run,
        "channel": record["channel"],
        "cell": record["cell"],
        "status": record["status"],
        "before": record["before"],
        "after": record["after"],
        "counts": record["analysis"]["counts"],
        "metrics": record["analysis"]["metrics"],
        "blocks": block_audit,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = {"tool": "e1_pilot_audit 0.1", "runs": [audit_run(args.root, run) for run in RUNS]}
    if args.out.exists():
        raise SystemExit(f"refusing to overwrite {args.out}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"runs": len(result["runs"]), "out": str(args.out)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
