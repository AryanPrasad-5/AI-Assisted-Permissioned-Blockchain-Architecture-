"""Per-run analysis (stdlib). Every number comes from files written during the run:
  recorder round CSVs   per-tx create/final (Caliper clock) -> latency, success/failure, per-round throughput
  dispatch_rNN.csv      intended vs actual dispatch time (replay fidelity, diagnostic)
  ledger/ledger_tx.csv  the committed ledger (block, validation code) -> reconciliation and per-block fill
  config_blocks.json    MaxMessageCount decoded from every CONFIG block on the ledger (independent of the actuator)
Latency percentiles are nearest rank over successful transactions; unit ms.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

from .telemetry import TelemetryError, nearest_rank, read_round, round_base

CLOCK_SUSPECT_MS = 50.0
DISPATCH_LAG_FLAG_P99_MS = 100.0


def _stats(values: list[float]) -> dict:
    v = sorted(values)
    if not v:
        return {"n": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None}
    return {"n": len(v), "mean": sum(v) / len(v), "p50": nearest_rank(v, 0.5), "p95": nearest_rank(v, 0.95),
            "p99": nearest_rank(v, 0.99), "max": v[-1]}


def decode_config_blocks(ledger_dir: Path, channel: str, run, configtxlator: str) -> list[dict]:
    """Decode every CONFIG block (header_type 1) the block fetch stored. `run(argv)` returns an object with .code."""
    rows = list(csv.DictReader((Path(ledger_dir) / "ledger" / "ledger_tx.csv").open(encoding="utf-8")))
    numbers = sorted({int(r["block"]) for r in rows if str(r["header_type"]) == "1"})
    out_dir = Path(ledger_dir) / "config_decoded"
    out_dir.mkdir(exist_ok=False)
    result = []
    for n in numbers:
        files = sorted((Path(ledger_dir) / "blocks").glob(f"{channel}_{n:06d}_a*.block"))
        if not files:
            result.append({"block": n, "error": "block file missing"})
            continue
        js = out_dir / f"{n:06d}.json"
        r = run([configtxlator, "proto_decode", "--input", str(files[-1]), "--type", "common.Block", "--output", str(js)])
        try:
            doc = json.loads(js.read_text(encoding="utf-8"))
            values = doc["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]
            result.append({"block": n, "max_message_count": int(values["BatchSize"]["value"]["max_message_count"]),
                           "preferred_max_bytes": int(values["BatchSize"]["value"]["preferred_max_bytes"]),
                           "batch_timeout": values["BatchTimeout"]["value"]["timeout"], "decode_exit": r.code,
                           "source": "peer channel fetch <n> + configtxlator proto_decode (block fetch files)"})
        except (OSError, KeyError, IndexError, TypeError, ValueError) as exc:
            result.append({"block": n, "error": f"decode failed: {exc!r}", "decode_exit": r.code})
    return result


_LIFECYCLE_TXID = re.compile(r"txid \[([0-9a-f]{64})\] committed with status \(VALID\)")


def lifecycle_tx_ids(channel_dir: Path) -> dict[str, str]:
    """Chaincode-lifecycle transactions committed by the channel step (approveformyorg x2, commit), read from that step's
    own peer CLI logs (cmd/NN_approve_*.stderr, cmd/NN_commit_*.stderr). These are endorser transactions on the run's
    channel that Caliper never submitted, so they are excluded from application reconciliation (cs01 finding,
    2026-11-22). Returns tx_id -> log file name."""
    out: dict[str, str] = {}
    cmd = Path(channel_dir) / "cmd"
    if not cmd.is_dir():
        return out
    for p in sorted(cmd.glob("*.stderr")):
        if "_approve_" not in p.name and "_commit_" not in p.name:
            continue
        for tx in _LIFECYCLE_TXID.findall(p.read_text(encoding="utf-8", errors="replace")):
            out.setdefault(tx, p.name)
    return out


def _dispatch(run_ws: Path, name: str) -> dict:
    p = Path(run_ws) / f"dispatch_{name}.csv"
    if not p.exists():
        return {"present": False}
    lags = [float(r["lag_ms"]) for r in csv.DictReader(p.open(encoding="utf-8"))]
    over = Path(f"{p}.overflow.csv")
    overflow = max(0, len(over.read_text(encoding="utf-8").splitlines()) - 1) if over.exists() else None
    return {"present": True, "dispatched": len(lags), "lag_ms": _stats(lags), "overflow_callbacks": overflow}


def analyze_run(arm: dict, run_ws: Path, ledger_dir: Path, cfg_blocks: list[dict], control: dict,
                setup_tx: dict[str, str] | None = None) -> dict:
    """`setup_tx`: tx_id -> source of the channel step's lifecycle transactions (see lifecycle_tx_ids); only those exact
    tx_ids are left out of application reconciliation, and any other unexplained ledger transaction is still a problem."""
    setup_tx = setup_tx or {}
    run_id = arm["run_id"]
    flags, problems = [], []
    rounds_out, measured_lat, all_success_ids, all_failed = [], [], {}, 0
    total_success = total_wall_ms = total_sched = 0
    for r in arm["rounds"]:
        base = round_base(Path(run_ws) / "recorder", run_id, r["index"], r["label"])
        try:
            obs, rows = read_round(base, r["index"], r["label"])
        except TelemetryError as exc:
            problems.append(f"round {r['index']}: {exc}")
            rounds_out.append({"round": r["index"], "error": str(exc)})
            continue
        lat = [float(int(float(x["final_ms"])) - int(float(x["create_ms"]))) for x in rows if x["status"] == "success"]
        for x in rows:
            if x["status"] == "success":
                all_success_ids[x["tx_id"]] = r["index"]
        wall_ms = obs.wall1_ms - obs.wall0_ms
        d = _dispatch(run_ws, r["name"])
        entry = {"round": r["index"], "label": r["label"], "measured": r["measured"], "segment": r["segment"],
                 "scheduled": r["n_tx"], "rows": obs.rows, "success": obs.success, "failed": obs.failed, "other": obs.other,
                 "pending_at_deactivate": obs.pending_at_deactivate, "latency_ms": _stats(lat), "round_wall_ms": wall_ms,
                 "offered_tps": r["n_tx"] / (r["length_ms"] / 1000), "throughput_tps_over_round_wall": obs.success / (wall_ms / 1000) if wall_ms > 0 else None,
                 "wall_minus_monotonic_ms": obs.wall_minus_monotonic_ms, "dispatch": d}
        if obs.rows != r["n_tx"]:
            problems.append(f"round {r['index']}: {obs.rows} recorder rows for {r['n_tx']} scheduled")
        if d.get("present") and d["dispatched"] != r["n_tx"]:
            problems.append(f"round {r['index']}: {d['dispatched']} dispatched for {r['n_tx']} scheduled")
        if obs.wall_minus_monotonic_ms is not None and abs(obs.wall_minus_monotonic_ms) > CLOCK_SUSPECT_MS:
            flags.append(f"CLOCK_SUSPECT round {r['index']} ({obs.wall_minus_monotonic_ms:.1f} ms)")
        if d.get("present") and d["lag_ms"]["p99"] is not None and d["lag_ms"]["p99"] > DISPATCH_LAG_FLAG_P99_MS:
            flags.append(f"DISPATCH_LAG round {r['index']} p99 {d['lag_ms']['p99']:.0f} ms")
        if r["measured"]:
            measured_lat += lat
            total_success += obs.success
            all_failed += obs.failed
            total_wall_ms += wall_ms
            total_sched += r["n_tx"]
        rounds_out.append(entry)

    # ledger reconciliation and per-block configuration in force
    ledger = list(csv.DictReader((Path(ledger_dir) / "ledger" / "ledger_tx.csv").open(encoding="utf-8")))
    endorser = [x for x in ledger if str(x["header_type"]) == "3"]
    setup_rows = [x for x in endorser if x["tx_id"] in setup_tx]
    app = [x for x in endorser if x["tx_id"] not in setup_tx]
    valid = {x["tx_id"]: int(x["block"]) for x in app if str(x["validation_code"]) == "0"}
    invalid = [x["tx_id"] for x in app if str(x["validation_code"]) != "0"]
    missing = [t for t in all_success_ids if t not in valid]
    extra = [t for t in valid if t not in all_success_ids]
    setup_invalid = [x["tx_id"] for x in setup_rows if str(x["validation_code"]) != "0"]
    recon = {"recorder_success": len(all_success_ids), "ledger_valid_endorser": len(valid), "ledger_invalid_endorser": len(invalid),
             "success_missing_from_ledger": len(missing), "ledger_valid_not_in_recorder": len(extra),
             "channel_setup_lifecycle_tx": [{"tx_id": x["tx_id"], "block": int(x["block"]), "validation_code": x["validation_code"],
                                             "source": setup_tx[x["tx_id"]]} for x in setup_rows],
             "channel_setup_lifecycle_tx_not_on_ledger": sorted(set(setup_tx) - {x["tx_id"] for x in setup_rows}),
             "complete": not missing and not extra}
    if setup_invalid:
        problems.append(f"{len(setup_invalid)} channel-setup lifecycle transactions are not VALID on the ledger")
    if not recon["complete"]:
        problems.append(f"reconciliation: {len(missing)} recorder successes not on the ledger, {len(extra)} ledger tx not in the recorder")
    cfg_ok = [c for c in cfg_blocks if "max_message_count" in c]
    if len(cfg_ok) != len(cfg_blocks) or not cfg_ok or cfg_ok[0]["block"] != 0:
        problems.append("configuration blocks could not all be decoded (or genesis missing)")
    regime = sorted((c["block"], c["max_message_count"]) for c in cfg_ok)

    def b_at(block: int) -> int | None:
        cur = None
        for n, b in regime:
            if n < block:
                cur = b
        return cur

    per_block: dict[int, int] = {}
    for x in endorser:
        per_block[int(x["block"])] = per_block.get(int(x["block"]), 0) + 1
    blocks_by_b: dict[int, list[int]] = {}
    over_limit = []
    for n, cnt in sorted(per_block.items()):
        b = b_at(n)
        blocks_by_b.setdefault(b, []).append(cnt)
        if b is not None and cnt > b:
            over_limit.append({"block": n, "tx": cnt, "B_in_force": b})
    if over_limit:
        problems.append(f"{len(over_limit)} blocks hold more transactions than the MaxMessageCount in force")
    block_evidence = {str(b): {"blocks": len(v), "mean_fill": sum(v) / len(v), "max_fill": max(v), "blocks_with_more_than_10": sum(1 for c in v if c > 10)}
                      for b, v in blocks_by_b.items()}
    # every applied transition must appear as a config block with its target, after the actuator's before-block
    transitions = control.get("transitions", [])
    for t in transitions:
        if t.get("success"):
            hit = [n for n, b in regime if b == t["to_B"] and (t.get("before_block_number") is None or n > int(t["before_block_number"]))]
            t["ledger_config_block"] = hit[0] if hit else None
            if not hit:
                problems.append(f"transition at decision {t['decision_index']} to B={t['to_B']} has no matching config block on the ledger")
    # per segment: share of its transactions committed under each B
    tx_round = all_success_ids
    seg_b: dict[int, dict[str, int]] = {}
    for tx, rnd in tx_round.items():
        if tx in valid:
            b = b_at(valid[tx])
            seg_b.setdefault(rnd, {}).setdefault(str(b), 0)
            seg_b[rnd][str(b)] += 1
    for e in rounds_out:
        if "round" in e and e["round"] in seg_b:
            e["committed_under_B"] = seg_b[e["round"]]
    if all_failed:
        flags.append(f"FAILED_TRANSACTIONS {all_failed}")
    run_level = {"scheduled_measured": total_sched, "success": total_success, "failed": all_failed,
                 "latency_ms": _stats(measured_lat), "sum_round_wall_s": total_wall_ms / 1000,
                 "throughput_tps_over_round_walls": total_success / (total_wall_ms / 1000) if total_wall_ms else None,
                 "offered_tps": total_sched / (sum(r["length_ms"] for r in arm["rounds"] if r["measured"]) / 1000)}
    verdict = "INVALID_INCOMPLETE" if problems else ("VALID_WITH_FLAGS" if flags else "VALID")
    return {"run_id": run_id, "arm": arm["arm_label"], "verdict": verdict, "problems": problems, "flags": flags,
            "run_level_measured": run_level, "rounds": rounds_out, "reconciliation": recon,
            "configuration_regimes": [{"from_after_block": n, "max_message_count": b} for n, b in regime],
            "block_evidence_by_B": block_evidence, "blocks_over_limit": over_limit[:20],
            "decisions": control.get("decisions", []), "transitions": transitions}
