"""Block fetch (LA-3; spec file 03 section 7 "Ledger reader"; E1 v0.2 protocol section 9 step 6).

Status: written 2026-11-09, tested with a fake peer CLI and synthetic protobuf blocks only. Never
run against Fabric. The block-hash rules below are from Fabric v2.5 protoutil/blockutils.go as read
during planning (BlockHeaderHash = SHA-256 of the ASN.1 DER SEQUENCE {INTEGER number, OCTET STRING
previous_hash, OCTET STRING data_hash}; BlockDataHash = SHA-256 of the concatenated envelopes).
E0-0 must confirm both on real blocks before INCONSISTENT verdicts from them are relied on.

Symbols: B = MaxMessageCount (block size in transactions), P = PreferredMaxBytes (bytes); both are
only passed through to the cut-cause rule of e1_reconcile.infer_cut_causes.

fetch   peer channel getinfo -c CH, then `peer channel fetch <n> <file> -c CH` from the first org's PEER
        (deliver service; no -o: orderer-served blocks carry no validation codes, E0-0 check 7) for every n
        in FIRST..LAST (default LAST = height - 1). Each attempt writes its own file, so nothing is ever
        rewritten. Prints the plan; runs only with --execute.
verify  the same checks on a directory of already fetched *.block files (no Fabric needed).

Inputs   --network network.json (the e1_channel_run.py description: orderers[0], orderer_ca, the
         first peer org's admin env, optional bin_dir), --channel, --first, --last, B, P.
Outputs  (new --out-dir; refuses an existing one)
  blocks/<channel>_<n:06d>_a<k>.block   raw bytes of every fetch attempt (verify: not copied)
  cmd/<label>.stdout|.stderr            CLI output of every command
  block_fetch_records.csv               one row per requested block: canonical file, SHA-256, bytes,
                                        attempts, decoded number, tx count, hashes, checks, status
  block_anomalies.csv                   every problem, with file and detail
  ledger/ledger_tx.csv, ledger/blocks.csv   e1_ledger_blocks tables, written ONLY when the range is
                                        COMPLETE or COMPLETE_WITH_WARNINGS (input to e1_reconcile.py
                                        --ledger-tx/--blocks and e1_log_collector.py --blocks-csv)
  block_fetch_manifest.json             request, getinfo, provenance, counts, status, output hashes

Checks
  completeness  height_unknown, range_beyond_height, fetch_failed, block_file_empty,
                block_unparseable, range_gap (verify mode: a number in FIRST..LAST has no file)
  consistency   number_mismatch, data_hash_mismatch, chain_break, tip_hash_mismatch,
                flags_length_mismatch, channel_mismatch, conflicting_duplicate
  warnings      exact_duplicate, duplicate_tx_id (Fabric keeps both; the later one carries
                DUPLICATE_TXID and the reconciler judges it), retry_succeeded, chain_anchor_unverified
                (FIRST > 0: the first block's previous_hash cannot be checked), unexpected_file

Usage
  python scripts/e1/e1_block_fetch.py fetch --network network.json --channel e1-r001 --first 0
         --max-message-count 45 --preferred-max-bytes 10485760 --out-dir runs/r001/ledger [--last N] [--execute]
  python scripts/e1/e1_block_fetch.py verify --blocks-dir DIR --channel e1-r001 --first 0 --last 21
         --max-message-count 10 --preferred-max-bytes 524288 --out-dir DIR2
Standard library only.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import posixpath
import re
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_ledger_blocks as lb  # noqa: E402
import e1_telemetry_common as tc  # noqa: E402

TOOL_VERSION = "e1_block_fetch 0.1 (2026-11-09)"
COMPLETENESS = {"height_unknown", "range_beyond_height", "fetch_failed", "block_file_empty", "block_unparseable",
                "range_gap"}
CONSISTENCY = {"number_mismatch", "data_hash_mismatch", "chain_break", "tip_hash_mismatch", "flags_length_mismatch",
               "channel_mismatch", "conflicting_duplicate"}
WARNINGS = {"exact_duplicate", "duplicate_tx_id", "retry_succeeded", "chain_anchor_unverified", "unexpected_file"}
RECORD_COLS = ["number", "status", "file", "sha256", "bytes", "attempts", "fetched_utc", "decoded_number", "tx_count",
               "previous_hash_hex", "data_hash_hex", "header_hash_hex", "data_hash_ok", "chain_ok", "detail"]
LEDGER_TX_COLS = ["tx_id", "block", "index", "header_type", "validation_code", "env_bytes", "channel_id", "ch_ts_ms"]
BLOCKS_COLS = ["number", "tx_count", "bytes_sum", "first_tx_id", "last_tx_id", "header_type", "inferred_cut_cause"]
_GETINFO = re.compile(r"Blockchain info:\s*(\{.*\})")
_CHANNEL = re.compile(r"^[a-z][a-z0-9.-]{0,248}$")


# ------------------------------------------------------------------ block hashing (protoutil)
def _der_len(n: int) -> bytes:
    if n < 0x80:
        return bytes([n])
    b = n.to_bytes((n.bit_length() + 7) // 8, "big")
    return bytes([0x80 | len(b)]) + b


def _der_uint(v: int) -> bytes:
    b = v.to_bytes(max(1, (v.bit_length() + 7) // 8), "big")
    if b[0] & 0x80:
        b = b"\x00" + b
    return b"\x02" + _der_len(len(b)) + b


def _der_octets(b: bytes) -> bytes:
    return b"\x04" + _der_len(len(b)) + b


def header_hash(number: int, previous_hash: bytes, data_hash: bytes) -> bytes:
    body = _der_uint(number) + _der_octets(previous_hash) + _der_octets(data_hash)
    return hashlib.sha256(b"\x30" + _der_len(len(body)) + body).digest()


def block_facts(raw: bytes) -> dict:
    """Header, data hash and parsed transactions of one block. ValueError if it does not parse."""
    if not raw:
        raise ValueError("empty file")
    top = lb.fields(raw)
    hdr = lb.fields(lb._one(top, 1, b""))
    data_entries = lb.fields(lb._one(top, 2, b"")).get(1, [])
    number = lb._one(hdr, 1, 0)
    prev, dhash = lb._one(hdr, 2, b""), lb._one(hdr, 3, b"")
    if not isinstance(prev, bytes) or not isinstance(dhash, bytes) or not isinstance(number, int):
        raise ValueError("block header fields have the wrong wire types")
    parsed = lb.parse_block(raw)
    return {"number": number, "previous_hash": prev, "data_hash": dhash,
            "computed_data_hash": hashlib.sha256(b"".join(data_entries)).digest(),
            "header_hash": header_hash(number, prev, dhash), "parsed": parsed}


# ------------------------------------------------------------------ verification core
def verify_range(entries: dict[int, list[dict]], first: int, last: int, channel: str,
                 anomalies: tc.Anomalies, tip: dict | None = None) -> tuple[list[dict], list[dict]]:
    """entries: requested number -> candidate files [{file, raw, attempts, fetched_utc}] in arrival order.
    Returns (record rows, parsed blocks in number order for the ledger tables)."""
    records, parsed_blocks, facts_by_n = [], [], {}
    for n in range(first, last + 1):
        cands = entries.get(n, [])
        rec = {"number": n, "attempts": sum(c.get("attempts", 1) for c in cands) if cands else 0}
        good = [c for c in cands if c["raw"]]
        if not cands:
            rec.update(status="missing", detail="no file")
            records.append(rec)
            continue
        if not good:
            anomalies.add("block_file_empty", n, "block file is empty", cands[-1]["file"])
            rec.update(status="missing", file=cands[-1]["file"], detail="empty file")
            records.append(rec)
            continue
        canon = good[0]
        for other in good[1:]:
            same = other["raw"] == canon["raw"]
            anomalies.add("exact_duplicate" if same else "conflicting_duplicate", n,
                          f"{other['file']} {'equals' if same else 'differs from'} {canon['file']}", other["file"])
        rec.update(file=canon["file"], sha256=tc.sha256_bytes(canon["raw"]), bytes=len(canon["raw"]),
                   fetched_utc=canon.get("fetched_utc", ""))
        try:
            f = block_facts(canon["raw"])
        except ValueError as err:
            anomalies.add("block_unparseable", n, str(err), canon["file"])
            rec.update(status="malformed", detail=str(err))
            records.append(rec)
            continue
        p = f["parsed"]
        rec.update(decoded_number=f["number"], tx_count=len(p["txs"]), previous_hash_hex=f["previous_hash"].hex(),
                   data_hash_hex=f["data_hash"].hex(), header_hash_hex=f["header_hash"].hex(),
                   data_hash_ok=f["computed_data_hash"] == f["data_hash"])
        problems = []
        if f["number"] != n:
            anomalies.add("number_mismatch", n, f"file decodes to block {f['number']}", canon["file"])
            problems.append("number_mismatch")
        if not rec["data_hash_ok"]:
            anomalies.add("data_hash_mismatch", n, f"header {f['data_hash'].hex()} != computed "
                                                   f"{f['computed_data_hash'].hex()}", canon["file"])
            problems.append("data_hash_mismatch")
        if p["flags_len"] != len(p["txs"]):
            anomalies.add("flags_length_mismatch", n, f"TRANSACTIONS_FILTER {p['flags_len']} != {len(p['txs'])} txs",
                          canon["file"])
            problems.append("flags_length_mismatch")
        foreign = sorted({t["channel_id"] for t in p["txs"]} - {channel})
        if foreign:
            anomalies.add("channel_mismatch", n, f"envelopes of channel(s) {foreign}", canon["file"])
            problems.append("channel_mismatch")
        prev = facts_by_n.get(n - 1)
        if n == 0:
            rec["chain_ok"] = f["previous_hash"] == b""
        elif prev is not None:
            rec["chain_ok"] = f["previous_hash"] == prev["header_hash"]
        else:
            rec["chain_ok"] = None
            if n == first:
                anomalies.add("chain_anchor_unverified", n, "previous block not in the requested range")
        if rec["chain_ok"] is False:
            anomalies.add("chain_break", n, f"previous_hash {f['previous_hash'].hex()} != header hash of block {n - 1}",
                          canon["file"])
            problems.append("chain_break")
        facts_by_n[n] = f
        rec.update(status="ok" if not problems else "inconsistent", detail=";".join(problems))
        records.append(rec)
        if not problems:
            parsed_blocks.append(p)
    if tip is not None and last in facts_by_n:
        want = tip.get("currentBlockHash")
        if want is not None:
            try:
                want_b = base64.b64decode(want, validate=True)
            except (ValueError, TypeError):
                want_b = None
            if want_b != facts_by_n[last]["header_hash"]:
                anomalies.add("tip_hash_mismatch", last, f"getinfo currentBlockHash {want!r} != header hash "
                                                         f"{facts_by_n[last]['header_hash'].hex()}")
    ids = Counter(t["tx_id"] for b in parsed_blocks for t in b["txs"] if t["tx_id"])
    for tx_id, k in sorted(ids.items()):
        if k > 1:
            anomalies.add("duplicate_tx_id", tx_id, f"{k} ledger positions (later ones should carry DUPLICATE_TXID)")
    return records, parsed_blocks


def write_outputs(out_dir: Path, records: list[dict], blocks: list[dict], anomalies: tc.Anomalies,
                  max_message_count: int, preferred_max_bytes: int, manifest: dict) -> dict:
    status = anomalies.status()
    outputs = {"block_fetch_records.csv": tc.write_csv_x(out_dir / "block_fetch_records.csv", RECORD_COLS, records),
               "block_anomalies.csv": tc.write_csv_x(out_dir / "block_anomalies.csv", tc.ANOMALY_COLS, anomalies.rows())}
    if status in tc.USABLE:
        ledger, block_rows = lb.tables(blocks, max_message_count, preferred_max_bytes)
        (out_dir / "ledger").mkdir()
        outputs["ledger/ledger_tx.csv"] = tc.write_csv_x(out_dir / "ledger" / "ledger_tx.csv", LEDGER_TX_COLS, ledger)
        outputs["ledger/blocks.csv"] = tc.write_csv_x(out_dir / "ledger" / "blocks.csv", BLOCKS_COLS, block_rows)
        manifest["ledger_tables"] = {"written": True, "transactions": len(ledger), "blocks": len(block_rows)}
    else:
        manifest["ledger_tables"] = {"written": False, "reason": f"status {status}: an incomplete or inconsistent "
                                                                 "range is never handed to the reconciler"}
    manifest.update(status=status, anomalies=anomalies.counts(),
                    counts={"requested": len(records), "ok": sum(r.get("status") == "ok" for r in records),
                            "missing": sum(r.get("status") == "missing" for r in records),
                            "malformed": sum(r.get("status") == "malformed" for r in records),
                            "inconsistent": sum(r.get("status") == "inconsistent" for r in records)},
                    outputs_sha256=outputs)
    tc.write_json_x(out_dir / "block_fetch_manifest.json", manifest)
    return manifest


def _anoms() -> tc.Anomalies:
    return tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)


def _base_manifest(argv, channel, first, last, b, p) -> dict:
    return {"tool": TOOL_VERSION, "script": tc.provenance(__file__),
            "ledger_reader": tc.provenance(lb.__file__),
            "argv": argv, "request": {"channel": channel, "first": first, "last": last,
                                      "B_max_message_count": b, "P_preferred_max_bytes": p}}


# ------------------------------------------------------------------ verify mode
def run_verify(blocks_dir: Path, channel: str, first: int, last: int, out_dir: Path, max_message_count: int,
               preferred_max_bytes: int, argv: list[str] | None = None) -> dict:
    """Verify already fetched block files. Files are assigned to numbers by their decoded header."""
    anomalies = _anoms()
    entries: dict[int, list[dict]] = {}
    for path in sorted(blocks_dir.glob("*.block")):
        raw = path.read_bytes()
        try:
            n = block_facts(raw)["number"]
        except ValueError as err:
            m = re.search(r"_(\d+)(?:_a\d+)?\.block$", path.name)
            if not m:
                anomalies.add("unexpected_file", path.name, f"unparseable and number not in the name: {err}", str(path))
                continue
            n = int(m.group(1))
        if n < first or n > last:
            anomalies.add("unexpected_file", path.name, f"block {n} outside {first}..{last}", str(path))
            continue
        entries.setdefault(n, []).append({"file": str(path), "raw": raw})
    for n in range(first, last + 1):
        if n not in entries:
            anomalies.add("range_gap", n, f"no file decodes to block {n}")
    records, blocks = verify_range(entries, first, last, channel, anomalies)
    out_dir.mkdir(parents=True, exist_ok=False)
    m = _base_manifest(argv, channel, first, last, max_message_count, preferred_max_bytes)
    m.update(mode="verify", blocks_dir=str(blocks_dir))
    return write_outputs(out_dir, records, blocks, anomalies, max_message_count, preferred_max_bytes, m)


# ------------------------------------------------------------------ fetch mode
NetRunner = Callable[[list[str], dict], "tuple[int | None, bytes, bytes]"]


def getinfo_argv(net, channel: str) -> list[str]:
    return [net.tool("peer"), "channel", "getinfo", "-c", channel]


def fetch_argv(net, channel: str, n: int, path: str) -> list[str]:
    """From the PEER's deliver service (no -o), with the first org's peer environment. Orderer-served blocks
    carry an empty TRANSACTIONS_FILTER (only the peer's committer writes validation codes): E0-0 check 7,
    2026-11-09, blocks 0 and 1 of e1-e00a2 fetched both ways, identical header and data hashes."""
    return [net.tool("peer"), "channel", "fetch", str(n), path, "-c", channel]


def parse_getinfo(text: str) -> dict | None:
    m = _GETINFO.search(text)
    if not m:
        return None
    try:
        info = json.loads(m.group(1))
        int(info["height"])
        return info
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None


def run_fetch(net, channel: str, first: int, last: int | None, out_dir: Path, max_message_count: int,
              preferred_max_bytes: int, runner: NetRunner, *, attempts: int = 3, retry_sleep_s: float = 1.0,
              sleep: Callable[[float], None] = time.sleep, wall_ns: Callable[[], int] = time.time_ns,
              argv: list[str] | None = None) -> dict:
    if not _CHANNEL.match(channel) or first < 0 or (last is not None and last < first) or attempts < 1:
        raise ValueError("invalid channel name, range or attempts")
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "blocks").mkdir()
    (out_dir / "cmd").mkdir()
    env = net.peer_env(net.orgs()[0])
    anomalies = _anoms()
    steps = []

    def call(label: str, cmd: list[str]) -> tuple[int | None, bytes, bytes]:
        t0 = wall_ns()
        try:
            code, out, err = runner(cmd, env)
        except (OSError, subprocess.SubprocessError) as exc:
            code, out, err = None, b"", repr(exc).encode()
        tc.write_bytes_x(out_dir / "cmd" / f"{label}.stdout", out)
        tc.write_bytes_x(out_dir / "cmd" / f"{label}.stderr", err)
        steps.append({"label": label, "argv": cmd, "start_utc": tc.utc_iso(t0), "end_utc": tc.utc_iso(wall_ns()),
                      "exit_code": code, "stdout_sha256": tc.sha256_bytes(out), "stderr_sha256": tc.sha256_bytes(err)})
        return code, out, err

    code, out, err = call("getinfo", getinfo_argv(net, channel))
    info = parse_getinfo((out + b"\n" + err).decode("utf-8", "replace")) if code == 0 else None
    if info is None:
        anomalies.add("height_unknown", channel, f"getinfo exit {code}; no 'Blockchain info' JSON with a height")
    height = int(info["height"]) if info else None
    if last is None:
        last = (height - 1) if height else first - 1
    if height is not None and last >= height:
        anomalies.add("range_beyond_height", channel, f"last {last} >= height {height}")
    entries: dict[int, list[dict]] = {}
    for n in range(first, last + 1):
        for k in range(1, attempts + 1):
            rel = f"blocks/{channel}_{n:06d}_a{k}.block"
            path = out_dir / rel
            code, _, err = call(f"fetch_{n:06d}_a{k}", fetch_argv(net, channel, n, path.resolve().as_posix()))
            if code == 0 and path.exists() and path.stat().st_size > 0:
                entries[n] = [{"file": rel, "raw": path.read_bytes(), "attempts": k,
                               "fetched_utc": steps[-1]["end_utc"]}]
                if k > 1:
                    anomalies.add("retry_succeeded", n, f"attempt {k} of {attempts}")
                break
            if k < attempts:
                sleep(retry_sleep_s)
        else:
            left = path.exists()
            anomalies.add("fetch_failed", n, f"{attempts} attempts; last exit {code}; "
                                             f"{err.decode('utf-8', 'replace').strip()[-200:]}"
                          + ("; partial file kept" if left else ""), rel if left else "")
    records, blocks = verify_range(entries, first, last, channel, anomalies,
                                   tip=info if (info and height is not None and last == height - 1) else None)
    m = _base_manifest(argv, channel, first, last, max_message_count, preferred_max_bytes)
    m.update(mode="fetch", getinfo=info, height=height, attempts_per_block=attempts, steps=steps,
             network_sha256=tc.sha256_bytes(json.dumps(net.raw, sort_keys=True).encode()),
             timestamp_note="fetched_utc and step times are host wall clock (time.time_ns), not the container clock")
    return write_outputs(out_dir, records, blocks, anomalies, max_message_count, preferred_max_bytes, m)


# ------------------------------------------------------------------ CLI
def _net_subprocess_runner(argv: list[str], env: dict) -> tuple[int | None, bytes, bytes]:
    import os
    proc =subprocess.run(argv, env={**os.environ, **env}, capture_output=True, timeout=300, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def main(argv: list[str] | None = None, runner: NetRunner | None = None, platform_name: str = sys.platform) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("fetch", "verify"):
        p = sub.add_parser(name)
        p.add_argument("--channel", required=True)
        p.add_argument("--first", type=int, default=0)
        p.add_argument("--max-message-count", required=True, type=int, help="B")
        p.add_argument("--preferred-max-bytes", required=True, type=int, help="P, bytes")
        p.add_argument("--out-dir", required=True, type=Path)
    f = sub.choices["fetch"]
    f.add_argument("--network", required=True, type=Path)
    f.add_argument("--last", type=int)
    f.add_argument("--attempts", type=int, default=3)
    f.add_argument("--execute", action="store_true", help="run the peer CLI (needs Fabric authorization)")
    v = sub.choices["verify"]
    v.add_argument("--blocks-dir", required=True, type=Path)
    v.add_argument("--last", required=True, type=int)
    args = ap.parse_args(argv)
    if args.out_dir.exists():
        print(f"refusing to overwrite {args.out_dir}")
        return 2
    try:
        if args.cmd == "verify":
            m = run_verify(args.blocks_dir, args.channel, args.first, args.last, args.out_dir,
                           args.max_message_count, args.preferred_max_bytes, argv=sys.argv if argv is None else argv)
        else:
            from e1_channel_run import Network, RequestError
            try:
                net = Network(json.loads(args.network.read_text(encoding="utf-8")))
            except RequestError as err:
                raise ValueError(str(err)) from err
            if not args.execute:
                print("PLAN " + " ".join(getinfo_argv(net, args.channel)))
                last = "height-1" if args.last is None else str(args.last)
                print("PLAN " + " ".join(fetch_argv(net, args.channel, "<n>",
                                                    posixpath.join(args.out_dir.as_posix(), "blocks", "<file>")))
                      + f"   for n in {args.first}..{last}, up to {args.attempts} attempts each")
                print("dry run: nothing executed or written (add --execute once Fabric use is authorized)")
                return 0
            if runner is None and not platform_name.startswith("linux"):
                raise ValueError(f"--execute runs the Linux peer CLI with POSIX paths: run it inside the Ubuntu WSL "
                                 f"distro (platform here: {platform_name})")
            m = run_fetch(net, args.channel, args.first, args.last, args.out_dir, args.max_message_count,
                          args.preferred_max_bytes, runner or _net_subprocess_runner, attempts=args.attempts,
                          argv=sys.argv if argv is None else argv)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as err:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {err!r}")
        return 2
    c = m["counts"]
    print(f"requested={c['requested']} ok={c['ok']} missing={c['missing']} malformed={c['malformed']} "
          f"inconsistent={c['inconsistent']} ledger_tables_written={m['ledger_tables']['written']}")
    for sev in ("error", "warning"):
        for k, n in sorted(m["anomalies"][sev].items()):
            print(f"  {sev:<7} {k:<24} {n}")
    print(f"BLOCKS {m['status']} -> {args.out_dir}")
    return tc.EXIT[m["status"]]


if __name__ == "__main__":
    sys.exit(main())
