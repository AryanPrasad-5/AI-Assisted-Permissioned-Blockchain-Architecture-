                                                                                                       

                                                                                               
                                                                                                
                                  

                                                                                                 
                                                                                            
                                                                                             
                                                                                                   
                                                                                        
                                                                                           
                                                                                  

      
                                                                                         
                                                                  
                                                                                                
                                                                               
   
from __future__ import annotations

import argparse
import csv
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from e1_reconcile import infer_cut_causes              

TRANSACTIONS_FILTER = 2                             
HEADER_TYPES = {0: "MESSAGE", 1: "CONFIG", 2: "CONFIG_UPDATE", 3: "ENDORSER_TRANSACTION",
                4: "ORDERER_TRANSACTION", 5: "DELIVER_SEEK_INFO", 6: "CHAINCODE_PACKAGE"}
TEN_MIB = 10 * 1024 * 1024
E1_MAX_B = 45


                                                                                   
def _varint(buf: bytes, pos: int) -> tuple[int, int]:
    result, shift = 0, 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint")
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def fields(buf: bytes) -> dict[int, list]:
                                                                                            
    out: dict[int, list] = {}
    pos = 0
    while pos < len(buf):
        tag, pos = _varint(buf, pos)
        num, wire = tag >> 3, tag & 7
        if wire == 0:
            val, pos = _varint(buf, pos)
        elif wire == 1:
            val, pos = int.from_bytes(buf[pos:pos + 8], "little"), pos + 8
        elif wire == 2:
            length, pos = _varint(buf, pos)
            if pos + length > len(buf):
                raise ValueError("truncated length-delimited field")
            val, pos = buf[pos:pos + length], pos + length
        elif wire == 5:
            val, pos = int.from_bytes(buf[pos:pos + 4], "little"), pos + 4
        else:
            raise ValueError(f"unsupported wire type {wire}")
        if pos > len(buf):
            raise ValueError("truncated fixed-width field")
        out.setdefault(num, []).append(val)
    return out


def _one(f: dict, num: int, default=None):
    vals = f.get(num)
    return vals[-1] if vals else default


                                                                              
def parse_block(raw: bytes) -> dict:
    block = fields(raw)
    header = fields(_one(block, 1, b""))
    data = fields(_one(block, 2, b""))
    metadata = fields(_one(block, 3, b""))
    meta_entries = metadata.get(1, [])
    flags = meta_entries[TRANSACTIONS_FILTER] if len(meta_entries) > TRANSACTIONS_FILTER else b""
    number = _one(header, 1, 0)
    txs = []
    for i, env_raw in enumerate(data.get(1, [])):
        env = fields(env_raw)
        payload, signature = _one(env, 1, b""), _one(env, 2, b"")
        chdr = fields(_one(fields(_one(fields(payload), 1, b"")), 1, b""))
        ts = fields(_one(chdr, 3, b""))
        seconds, nanos = _one(ts, 1, 0), _one(ts, 2, 0)
        tx_id = _one(chdr, 5, b"")
        txs.append({
            "tx_id": tx_id.decode("utf-8") if isinstance(tx_id, bytes) else "",
            "block": number,
            "index": i,
            "header_type": _one(chdr, 1, -1),
            "validation_code": flags[i] if i < len(flags) else "",
            "env_bytes": len(payload) + len(signature),
            "channel_id": (_one(chdr, 4, b"") or b"").decode("utf-8"),
            "ch_ts_ms": seconds * 1000 + nanos // 1_000_000 if seconds else "",
        })
    return {"number": number, "txs": txs, "flags_len": len(flags)}


def read_blocks(paths: list[Path]) -> list[dict]:
    blocks = [parse_block(p.read_bytes()) for p in paths]
    blocks.sort(key=lambda b: b["number"])
    numbers = [b["number"] for b in blocks]
    if len(set(numbers)) != len(numbers):
        raise ValueError("duplicate block numbers")
    for b in blocks:
        if b["flags_len"] != len(b["txs"]):
            raise ValueError(f"block {b['number']}: TRANSACTIONS_FILTER length {b['flags_len']} != {len(b['txs'])} txs")
    return blocks


def tables(blocks: list[dict], max_message_count: int, preferred_max_bytes: int) -> tuple[list[dict], list[dict]]:
    ledger = [tx for b in blocks for tx in b["txs"]]
    causes = infer_cut_causes(ledger, max_message_count, preferred_max_bytes)
    rows = []
    for b in blocks:
        txs = b["txs"]
        rows.append({
            "number": b["number"], "tx_count": len(txs), "bytes_sum": sum(t["env_bytes"] for t in txs),
            "first_tx_id": txs[0]["tx_id"] if txs else "", "last_tx_id": txs[-1]["tx_id"] if txs else "",
            "header_type": HEADER_TYPES.get(txs[0]["header_type"], txs[0]["header_type"]) if txs else "",
            "inferred_cut_cause": causes.get(b["number"], "n/a (config or non-endorser block)"),
        })
    return ledger, rows


def size_summary(ledger: list[dict]) -> dict:
    sizes = sorted(t["env_bytes"] for t in ledger if t["header_type"] == 3)
    if not sizes:
        return {"envelopes": 0}
    p99 = sizes[min(len(sizes) - 1, int(round(0.99 * (len(sizes) - 1))))]
    x_max = sizes[-1]
    return {"envelopes": len(sizes), "min": sizes[0], "median": statistics.median(sizes), "p99": p99,
            "max": x_max, "stop_rule_45_x_max_gt_10MiB": E1_MAX_B * x_max > TEN_MIB,
            "enough_for_E0_1_size_estimate": len(sizes) >= 1000}


def _write(path: Path, rows: list[dict]) -> None:
    with path.open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--blocks-dir", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--max-message-count", required=True, type=int)
    ap.add_argument("--preferred-max-bytes", required=True, type=int)
    ap.add_argument("--summary", action="store_true")
    args = ap.parse_args(argv)
    paths = sorted(args.blocks_dir.glob("*.block"))
    if not paths:
        print(f"no *.block files in {args.blocks_dir}")
        return 2
    try:
        blocks = read_blocks(paths)
    except ValueError as err:
        print(f"UNREADABLE BLOCK: {err}")
        return 2
    ledger, block_rows = tables(blocks, args.max_message_count, args.preferred_max_bytes)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    try:
        _write(args.out_dir / "ledger_tx.csv", ledger)
        _write(args.out_dir / "blocks.csv", block_rows)
    except FileExistsError as err:
        print(f"refusing to overwrite: {err.filename}")
        return 2
    print(f"blocks={len(blocks)} transactions={len(ledger)} -> {args.out_dir}")
    if args.summary:
        for k, v in size_summary(ledger).items():
            print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
