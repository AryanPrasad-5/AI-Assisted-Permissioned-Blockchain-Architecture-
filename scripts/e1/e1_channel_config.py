"""Verify a channel's batch settings from its decoded config block (LA-2(d); E0-0 "Channel config").

Status: written 2026-11-09, tested on synthetic JSON only. JSON paths follow configtxlator's
proto-JSON for common.Block / common.Config (Fabric "update a channel configuration" layout);
confirm on a real decoded block in E-a before relying on a PASS.

Input: the JSON written by
  peer channel fetch config cfg.block -c <ch> ...      (or the channel's genesis block)
  configtxlator proto_decode --type common.Block --input cfg.block --output cfg.json
Either the whole block or just its `config` object may be given.

Checks requested values against the ledger, so a configuration file is never taken as proof:
  MaxMessageCount = B, BatchTimeout = tau (Go duration string, compared in ms),
  PreferredMaxBytes = P, optionally AbsoluteMaxBytes, consensus type and consenter count.

Usage:
  python scripts/e1/e1_channel_config.py cfg.json --max-message-count 45 --batch-timeout 2s
         --preferred-max-bytes 10485760 [--absolute-max-bytes N] [--consensus etcdraft] [--consenters 3]
Exit codes: 0 all PASS, 1 a mismatch, 2 unreadable input or missing field.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

_UNITS_MS = {"ns": 1e-6, "us": 1e-3, "µs": 1e-3, "ms": 1.0, "s": 1000.0, "m": 60_000.0, "h": 3_600_000.0}


def duration_ms(text: str) -> float:
    """Go time.Duration string ("2s", "500ms", "1m30s", "1.5s") to milliseconds."""
    parts = re.findall(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)", text.strip())
    if not parts or "".join(n + u for n, u in parts) != text.strip():
        raise ValueError(f"not a Go duration: {text!r}")
    return sum(float(n) * _UNITS_MS[u] for n, u in parts)


def orderer_values(doc: dict) -> dict:
    config = doc
    if "data" in doc:  # whole common.Block
        config = doc["data"]["data"][0]["payload"]["data"]["config"]
    values = config["channel_group"]["groups"]["Orderer"]["values"]
    batch = values["BatchSize"]["value"]
    consensus = values.get("ConsensusType", {}).get("value", {})
    consenters = consensus.get("metadata", {}).get("consenters") if isinstance(consensus.get("metadata"), dict) else None
    return {
        "max_message_count": int(batch["max_message_count"]),
        "preferred_max_bytes": int(batch["preferred_max_bytes"]),
        "absolute_max_bytes": int(batch["absolute_max_bytes"]),
        "batch_timeout_ms": duration_ms(values["BatchTimeout"]["value"]["timeout"]),
        "batch_timeout_raw": values["BatchTimeout"]["value"]["timeout"],
        "consensus_type": consensus.get("type"),
        "consenters": len(consenters) if consenters is not None else None,
    }


def verify(found: dict, *, max_message_count: int, batch_timeout: str, preferred_max_bytes: int,
           absolute_max_bytes: int | None = None, consensus: str | None = None,
           consenters: int | None = None) -> list[tuple[str, object, object, bool]]:
    rows = [
        ("MaxMessageCount", max_message_count, found["max_message_count"],
         found["max_message_count"] == max_message_count),
        ("BatchTimeout_ms", duration_ms(batch_timeout), found["batch_timeout_ms"],
         abs(found["batch_timeout_ms"] - duration_ms(batch_timeout)) < 1e-9),
        ("PreferredMaxBytes", preferred_max_bytes, found["preferred_max_bytes"],
         found["preferred_max_bytes"] == preferred_max_bytes),
        ("PreferredMaxBytes <= AbsoluteMaxBytes", True, found["preferred_max_bytes"] <= found["absolute_max_bytes"],
         found["preferred_max_bytes"] <= found["absolute_max_bytes"]),
    ]
    if absolute_max_bytes is not None:
        rows.append(("AbsoluteMaxBytes", absolute_max_bytes, found["absolute_max_bytes"],
                     found["absolute_max_bytes"] == absolute_max_bytes))
    if consensus is not None:
        rows.append(("ConsensusType", consensus, found["consensus_type"], found["consensus_type"] == consensus))
    if consenters is not None:
        rows.append(("Consenters", consenters, found["consenters"], found["consenters"] == consenters))
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("config_json", type=Path)
    ap.add_argument("--max-message-count", required=True, type=int)
    ap.add_argument("--batch-timeout", required=True)
    ap.add_argument("--preferred-max-bytes", required=True, type=int)
    ap.add_argument("--absolute-max-bytes", type=int)
    ap.add_argument("--consensus")
    ap.add_argument("--consenters", type=int)
    args = ap.parse_args(argv)
    try:
        found = orderer_values(json.loads(args.config_json.read_text(encoding="utf-8")))
        rows = verify(found, max_message_count=args.max_message_count, batch_timeout=args.batch_timeout,
                      preferred_max_bytes=args.preferred_max_bytes, absolute_max_bytes=args.absolute_max_bytes,
                      consensus=args.consensus, consenters=args.consenters)
    except (OSError, ValueError, KeyError, IndexError, TypeError) as err:
        print(f"UNREADABLE OR INCOMPLETE CONFIG: {err!r}")
        return 2
    for name, want, got, ok in rows:
        print(f"{name:<38} requested={want!s:<12} ledger={got!s:<12} {'PASS' if ok else 'FAIL'}")
    ok = all(r[3] for r in rows)
    print("CHANNEL CONFIG " + ("MATCHES" if ok else "DOES NOT MATCH") + f" (BatchTimeout raw {found['batch_timeout_raw']!r})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
