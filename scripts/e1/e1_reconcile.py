                                                                                             

                                                                                    
                                                                                                             
                                                                                            
                                                                                                 

                                         
                                                                             
                                                                                                     
                                        
                                                                                  
                                                                  
                                                                                          
                                                                                                  
                                                                                                 
                                                                                             
                                                                                                   

                                                                                   
                                                                                                
                                     
                                                                                              
                                                                                                  
                                                                                  
                                                                                             
                                                                                                  
                                                                                                   
                                        
                                                                                    
                                                                                  
                                                                                    
                                                                                             
                                                                                                
                                                                                             
                                                                                            
                                            
                                                                                                
                                                          

                                                                                                   
                                                                                                 
                                                                                             
                                                                                               
                       

      
                                                                                               
                                                                                     
                                                                    
   
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

VALID = 0
HEADER_TYPE_ENDORSER = 3                                          
                                                                                               
                                                          
VALIDATION_CODE_NAMES = {
    0: "VALID", 1: "NIL_ENVELOPE", 2: "BAD_PAYLOAD", 3: "BAD_COMMON_HEADER", 4: "BAD_CREATOR_SIGNATURE",
    5: "INVALID_ENDORSER_TRANSACTION", 6: "INVALID_CONFIG_TRANSACTION", 7: "UNSUPPORTED_TX_PAYLOAD",
    8: "BAD_PROPOSAL_TXID", 9: "DUPLICATE_TXID", 10: "ENDORSEMENT_POLICY_FAILURE",
    11: "MVCC_READ_CONFLICT", 12: "PHANTOM_READ_CONFLICT", 13: "UNKNOWN_TX_TYPE",
    14: "TARGET_CHAIN_NOT_FOUND", 15: "MARSHAL_TX_ERROR", 16: "NIL_TXACTION", 17: "EXPIRED_CHAINCODE",
    18: "CHAINCODE_VERSION_CONFLICT", 19: "BAD_HEADER_EXTENSION", 20: "BAD_CHANNEL_HEADER",
    21: "BAD_RESPONSE_PAYLOAD", 22: "BAD_RWSET", 23: "ILLEGAL_WRITESET", 24: "INVALID_WRITESET",
    25: "INVALID_CHAINCODE", 254: "NOT_VALIDATED", 255: "INVALID_OTHER_REASON",
}
PASS, FAIL, NOT_EVALUATED = "PASS", "FAIL", "NOT EVALUATED"


@dataclass
class Expectations:
    n: int
    max_message_count: int
    preferred_max_bytes: int
    event_strategy: str | None = None
    invoke_timeout_s: float | None = None
    binding: str | None = None
    clock_offset_bound_ms: float = 1.0
    other_round_tx_ids: set[str] = field(default_factory=set)
    expect_all_success: bool = True
    expect_block_pattern: bool = False

    @classmethod
    def from_json(cls, path: Path) -> "Expectations":
        raw = json.loads(path.read_text(encoding="utf-8"))
        others: set[str] = set(raw.pop("other_round_tx_ids", []))
        for extra in raw.pop("other_round_csvs", []):
            others |= {r["tx_id"] for r in read_csv(Path(extra)) if r["tx_id"]}
        return cls(other_round_tx_ids=others, **raw)


@dataclass
class Check:
    check_id: str
    status: str
    detail: str


def read_csv(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def code_name(code: int) -> str:
    return VALIDATION_CODE_NAMES.get(code, f"CODE_{code}")


                                                                               
def infer_cut_causes(ledger: list[dict], max_message_count: int, preferred_max_bytes: int) -> dict[int, str]:
                                                                                                
    by_block: dict[int, list[dict]] = {}
    for tx in ledger:
        by_block.setdefault(int(tx["block"]), []).append(tx)
    blocks = sorted(b for b, txs in by_block.items()
                    if all(int(t["header_type"]) == HEADER_TYPE_ENDORSER for t in txs))
    causes = {}
    for i, b in enumerate(blocks):
        txs = sorted(by_block[b], key=lambda t: int(t["index"]))
        sizes = [int(t["env_bytes"]) for t in txs]
        nxt = None
        if i + 1 < len(blocks) and blocks[i + 1] == b + 1:
            nxt = int(min(by_block[b + 1], key=lambda t: int(t["index"]))["env_bytes"])
        if len(sizes) == 1 and sizes[0] > preferred_max_bytes:
            causes[b] = "isolated"
        elif len(sizes) == max_message_count:
            causes[b] = "count"
        elif nxt is not None and sum(sizes) + nxt > preferred_max_bytes:
            causes[b] = "byte_overflow"
        else:
            causes[b] = "timer"
    return causes


                                                                                   
def reconcile(records: list[dict], submits: list[dict] | None, manifest: dict, ledger: list[dict],
              exp: Expectations, peer_commit: list[dict] | None = None,
              blocks: list[dict] | None = None) -> list[Check]:
    checks: list[Check] = []
    counts = manifest.get("counts", {})
    rows = len(records)
    ids = [r["tx_id"] for r in records]
    dup_ids = sorted(k for k, v in Counter(i for i in ids if i).items() if v > 1)

               
    submitted, finished = counts.get("submitted"), counts.get("finished")
    pending = counts.get("pending_at_deactivate")
    submit_sum = sum(int(s["count"]) for s in submits) if submits is not None else None
    ok = (submitted == finished == rows == exp.n and pending == 0
          and (submit_sum is None or submit_sum == submitted))
    checks.append(Check("V1-counts", PASS if ok else FAIL,
                        f"N={exp.n} submitted={submitted} finished={finished} rows={rows} "
                        f"pending_at_deactivate={pending} submit_events_sum={submit_sum}"))

                   
    reported_dups = counts.get("duplicate_ids")
    ok = not dup_ids and reported_dups == 0
    checks.append(Check("V1-duplicates", PASS if ok else FAIL,
                        f"recomputed={len(dup_ids)} manifest={reported_dups} examples={dup_ids[:3]}"))

            
    empty = sum(1 for i in ids if not i)
    no_id = counts.get("no_id_records")
    checks.append(Check("V1-ids", PASS if (empty == 0 and no_id == 0) else FAIL,
                        f"empty_tx_id_rows={empty} manifest_no_id_records={no_id}"))

                  
    ledger_end = [t for t in ledger if int(t["header_type"]) == HEADER_TYPE_ENDORSER]
    by_id: dict[str, list[dict]] = {}
    for t in ledger_end:
        by_id.setdefault(t["tx_id"], []).append(t)
    round_ids = {i for i in ids if i}

                
    bad = []
    for r in records:
        if r["status"] != "success":
            continue
        entries = by_id.get(r["tx_id"], [])
        if not entries:
            bad.append(f"{r['tx_id']}:absent")
        elif not any(int(e["validation_code"]) == VALID for e in entries):
            bad.append(f"{r['tx_id']}:{code_name(int(entries[0]['validation_code']))}")
    n_success = sum(1 for r in records if r["status"] == "success")
    checks.append(Check("V3-success", PASS if not bad else FAIL,
                        f"success={n_success} not_valid_in_ledger={len(bad)} examples={bad[:3]}"))

                                  
    classes: Counter = Counter()
    contradictions = []
    for r in records:
        if r["status"] == "success":
            continue
        entries = by_id.get(r["tx_id"], []) if r["tx_id"] else []
        if not entries:
            classes["not_ordered"] += 1
        elif any(int(e["validation_code"]) == VALID for e in entries):
            contradictions.append(r["tx_id"])
        else:
            classes[f"committed_invalid:{code_name(int(entries[0]['validation_code']))}"] += 1
    checks.append(Check("V3-failures", PASS if not contradictions else FAIL,
                        f"failed={sum(classes.values()) + len(contradictions)} classes={dict(sorted(classes.items()))} "
                        f"failed_but_VALID_in_ledger={contradictions[:3]}"))

                
    foreign = sorted({t["tx_id"] for t in ledger_end} - round_ids - exp.other_round_tx_ids)
    checks.append(Check("V3-foreign", PASS if not foreign else FAIL,
                        f"foreign_ledger_tx={len(foreign)} examples={foreign[:3]}"))

                     
    in_ledger = [t for t in ledger_end if t["tx_id"] in round_ids]
    reached = n_success + sum(v for k, v in classes.items() if k.startswith("committed_invalid"))
    ok = len(in_ledger) == reached and (not exp.expect_all_success or len(in_ledger) == exp.n)
    checks.append(Check("V3-ledger-count", PASS if ok else FAIL,
                        f"ledger_round_tx={len(in_ledger)} reached_ordering={reached} N={exp.n} "
                        f"expect_all_success={exp.expect_all_success}"))

              
    neg = [r["tx_id"] for r in records if float(r["create_ms"]) > float(r["final_ms"])]
    stamps = [float(s["t_submit_evt_ms"]) for s in sorted(submits or [], key=lambda s: int(s["seq_submit"]))]
    non_mono = sum(1 for a, b in zip(stamps, stamps[1:]) if b < a)
    checks.append(Check("V4-order", PASS if not neg and non_mono == 0 else FAIL,
                        f"create_after_final={len(neg)} submit_stamp_decreases={non_mono}"
                        + ("" if submits is not None else " (submits file not given)")))

                     
    if peer_commit is None:
        checks.append(Check("V4-cross-source", NOT_EVALUATED, "peer_commit not given"))
    else:
                                                                                              
        dup_blocks = sorted(b for b, k in Counter(int(p["block"]) for p in peer_commit).items() if k > 1)
        commit_ms = {int(p["block"]): float(p["commit_ms"]) for p in peer_commit}
        block_of = {t["tx_id"]: int(t["block"]) for t in in_ledger}
        early, missing = [], 0
        for r in records:
            b = block_of.get(r["tx_id"])
            if b is None or r["status"] != "success":
                continue
            if b not in commit_ms:
                missing += 1
            elif float(r["final_ms"]) < commit_ms[b] - exp.clock_offset_bound_ms:
                early.append(r["tx_id"])
        ok = not early and missing == 0 and not dup_blocks
        checks.append(Check("V4-cross-source", PASS if ok else FAIL,
                            f"finished_before_commit={len(early)} blocks_without_commit_time={missing} "
                            f"duplicate_commit_rows={len(dup_blocks)} offset_bound_ms={exp.clock_offset_bound_ms}"))

                        
    cfg = manifest.get("config_in_force", {})
    wanted = {"event_strategy": exp.event_strategy, "invoke_timeout_s": exp.invoke_timeout_s, "binding": exp.binding}
    diffs = {k: (cfg.get(k), v) for k, v in wanted.items() if v is not None and cfg.get(k) != v}
    if all(v is None for v in wanted.values()):
        checks.append(Check("V7-config", NOT_EVALUATED, "no protocol values given"))
    else:
        checks.append(Check("V7-config", PASS if not diffs else FAIL, f"mismatches(manifest, protocol)={diffs}"))

                                       
    causes = infer_cut_causes(ledger, exp.max_message_count, exp.preferred_max_bytes)
    round_blocks = sorted({int(t["block"]) for t in in_ledger})
    if exp.expect_block_pattern:
        b_max, r = exp.max_message_count, exp.n % exp.max_message_count
        sizes = [sum(1 for t in in_ledger if int(t["block"]) == b) for b in round_blocks]
        want_sizes = [b_max] * (exp.n // b_max) + ([r] if r else [])
        want_causes = ["count"] * (exp.n // b_max) + (["timer"] if r else [])
        got_causes = [causes.get(b) for b in round_blocks]
        ok = sizes == want_sizes and got_causes == want_causes
        checks.append(Check("E0-1-blocks", PASS if ok else FAIL,
                            f"blocks={len(round_blocks)} sizes_tail={sizes[-3:]} want_tail={want_sizes[-3:]} "
                            f"causes_tail={got_causes[-3:]} want_tail={want_causes[-3:]}"))
    else:
        checks.append(Check("E0-1-blocks", NOT_EVALUATED, "expect_block_pattern is false"))
    if blocks is None or not blocks or "inferred_cut_cause" not in blocks[0]:
        checks.append(Check("E0-1-causes", NOT_EVALUATED, "blocks CSV with inferred_cut_cause not given"))
    else:
        reader = {int(b["number"]): b["inferred_cut_cause"] for b in blocks if int(b["number"]) in causes}
        diff = {b: (c, causes[b]) for b, c in reader.items() if c != causes[b]}
        checks.append(Check("E0-1-causes", PASS if not diff else FAIL,
                            f"blocks_compared={len(reader)} disagreements(reader, recomputed)={dict(list(diff.items())[:3])}"))
    return checks


def verdict(checks: list[Check]) -> str:
    return "INVALID" if any(c.status == FAIL for c in checks) else "VALID"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run", required=True, type=Path, help="recorder base path (without .csv)")
    ap.add_argument("--ledger-tx", required=True, type=Path)
    ap.add_argument("--expect", required=True, type=Path)
    ap.add_argument("--peer-commit", type=Path)
    ap.add_argument("--blocks", type=Path)
    ap.add_argument("--gate-out", type=Path, help="write the gate result as JSON (refuses to overwrite)")
    args = ap.parse_args(argv)
    base = args.run
    try:
        records = read_csv(base.with_name(base.name + ".csv"))
        sub_path = base.with_name(base.name + ".submits.csv")
        submits = read_csv(sub_path) if sub_path.exists() else None
        manifest = json.loads(base.with_name(base.name + ".manifest.json").read_text(encoding="utf-8"))
        ledger = read_csv(args.ledger_tx)
        exp = Expectations.from_json(args.expect)
        peer = read_csv(args.peer_commit) if args.peer_commit else None
        blocks = read_csv(args.blocks) if args.blocks else None
    except (OSError, ValueError, KeyError, TypeError) as err:
        print(f"UNREADABLE INPUT: {err}")
        return 2
    checks = reconcile(records, submits, manifest, ledger, exp, peer, blocks)
    for c in checks:
        print(f"{c.check_id:<16} {c.status:<14} {c.detail}")
    result = verdict(checks)
    print(f"RUN {result}")
    if args.gate_out:
        if args.gate_out.exists():
            print(f"refusing to overwrite {args.gate_out}")
            return 2
        args.gate_out.write_text(json.dumps({"verdict": result, "checks": [c.__dict__ for c in checks]},
                                            indent=2), encoding="utf-8")
    return 0 if result == "VALID" else 1


if __name__ == "__main__":
    sys.exit(main())
