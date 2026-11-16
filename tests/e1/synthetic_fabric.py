"""Deterministic synthetic Fabric/Docker/HTTP doubles for the LA-3 telemetry tests.

Synthetic only: nothing here is Fabric, Docker or Caliper output, and no E0 result comes from it.
Blocks are hash-chained with the rules implemented in scripts/e1/e1_block_fetch.py (protoutil as
read during planning), so a self-consistent chain passes and any tampering is visible.
"""
from __future__ import annotations

import base64
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_block_fetch as bf  # noqa: E402

CHANNEL = "e1-r001"
T0_NS = 1_700_000_000_000_000_000          # 2023-11-14T22:13:20Z, the fake_caliper.js epoch


# ------------------------------------------------------------------ protobuf encoding
def varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def fld(num: int, value) -> bytes:
    """proto3: zero ints and empty bytes are omitted, as the Go marshaller does."""
    if isinstance(value, int):
        return b"" if value == 0 else varint(num << 3) + varint(value)
    return b"" if value == b"" else varint((num << 3) | 2) + varint(len(value)) + value


def envelope(tx_id: str, header_type: int = 3, channel: str = CHANNEL, body_len: int = 2500,
             ts_ms: int = 1_700_000_000_250) -> bytes:
    ts = fld(1, ts_ms // 1000) + fld(2, (ts_ms % 1000) * 1_000_000)
    chdr = fld(1, header_type) + fld(3, ts) + fld(4, channel.encode()) + fld(5, tx_id.encode())
    payload = fld(1, fld(1, chdr) + fld(2, b"sighdr")) + fld(2, b"x" * body_len)
    return fld(1, payload) + fld(2, b"s" * 70)


def block_bytes(number: int, prev: bytes, envs: list[bytes], codes: list[int], data_hash: bytes | None = None) -> bytes:
    dh = hashlib.sha256(b"".join(envs)).digest() if data_hash is None else data_hash
    header = fld(1, number) + fld(2, prev) + fld(3, dh)
    data = b"".join(varint((1 << 3) | 2) + varint(len(e)) + e for e in envs)
    meta = b"".join(varint((1 << 3) | 2) + varint(len(m)) + m for m in [b"", b"", bytes(codes), b"", b""])
    return fld(1, header) + fld(2, data) + fld(3, meta)


def chain(groups: list[list[dict]], channel: str = CHANNEL) -> list[bytes]:
    """groups[n] = transactions of block n: dicts with tx_id, header_type (3), code (0), body_len, ts_ms."""
    prev, out = b"", []
    for n, txs in enumerate(groups):
        envs = [envelope(t["tx_id"], t.get("header_type", 3), t.get("channel", channel), t.get("body_len", 2500),
                         t.get("ts_ms", 1_700_000_000_250)) for t in txs]
        dh = hashlib.sha256(b"".join(envs)).digest()
        out.append(block_bytes(n, prev, envs, [t.get("code", 0) for t in txs], dh))
        prev = bf.header_hash(n, prev, dh)
    return out


def tip_hash_b64(blocks: list[bytes]) -> str:
    f = bf.block_facts(blocks[-1])
    return base64.b64encode(f["header_hash"]).decode()


def ledger_groups(tx_ids: list[str], b: int, ts_ms: list[int] | None = None) -> list[list[dict]]:
    """Block 0 = one config transaction; then count cuts of B; the remainder in a final (timer) block."""
    groups = [[{"tx_id": "cfg0", "header_type": 1, "body_len": 9000}]]
    for i in range(0, len(tx_ids), b):
        groups.append([{"tx_id": t, "ts_ms": (ts_ms[i + j] if ts_ms else 1_700_000_000_250)}
                       for j, t in enumerate(tx_ids[i:i + b])])
    return groups


# ------------------------------------------------------------------ network description
def network(tmp: Path) -> dict:
    files = {}
    for name in ("configtx.yaml", "orderer-ca.pem", "o1.crt", "o1.key", "p1-tls.pem", "p2-tls.pem"):
        p = tmp / "netfiles" / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("synthetic\n", encoding="utf-8")
        files[name] = str(p)
    for d in ("p1-msp", "p2-msp"):
        (tmp / "netfiles" / d).mkdir(parents=True, exist_ok=True)
    return {
        "configtx_base": files["configtx.yaml"], "profile": "E1Channel", "fabric_cfg_path": str(tmp / "netfiles"),
        "orderer_ca": files["orderer-ca.pem"], "bin_dir": "/opt/fabric/bin",
        "orderers": [{"name": "orderer1.example.com", "address": "orderer1.example.com:7050",
                      "tls_hostname": "orderer1.example.com", "admin_address": "orderer1.example.com:7053",
                      "admin_client_cert": files["o1.crt"], "admin_client_key": files["o1.key"]}],
        "peers": [{"name": "peer0.org1.example.com", "msp_id": "Org1MSP", "address": "peer0.org1.example.com:7051",
                   "tls_root_cert": files["p1-tls.pem"], "admin_msp_dir": str(tmp / "netfiles" / "p1-msp")},
                  {"name": "peer0.org2.example.com", "msp_id": "Org2MSP", "address": "peer0.org2.example.com:9051",
                   "tls_root_cert": files["p2-tls.pem"], "admin_msp_dir": str(tmp / "netfiles" / "p2-msp")}],
        "chaincode": {"name": "e1asset", "version": "1.0", "sequence": 1, "package_id": "e1asset_1.0:" + "ab" * 32},
    }


class FakePeer:
    """peer channel getinfo / fetch. fail[n] = number of failing attempts before success (-1 = always);
    serve[n] = bytes served for n instead of the chain's block (tampering, wrong block, empty)."""

    def __init__(self, blocks: list[bytes], fail: dict | None = None, serve: dict | None = None,
                 tip_b64: str | None = None, getinfo_ok: bool = True, height: int | None = None):
        self.blocks, self.fail, self.serve = blocks, dict(fail or {}), dict(serve or {})
        self.tip = tip_b64 if tip_b64 is not None else tip_hash_b64(blocks)
        self.getinfo_ok, self.height = getinfo_ok, height if height is not None else len(blocks)
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str], env: dict) -> tuple[int, bytes, bytes]:
        self.calls.append(argv)
        assert env["CORE_PEER_LOCALMSPID"] == "Org1MSP"
        if argv[1:3] == ["channel", "getinfo"]:
            if not self.getinfo_ok:
                return 1, b"", b"Error: channel not found\n"
            info = {"height": self.height, "currentBlockHash": self.tip, "previousBlockHash": ""}
            return 0, ("Blockchain info: " + json.dumps(info) + "\n").encode(), b""
        if argv[1:3] == ["channel", "fetch"]:
            # blocks must come from the peer (validation codes), never from an orderer (E0-0 check 7)
            assert "-o" not in argv and "--tls" not in argv, argv
            n, path = int(argv[3]), Path(argv[4])
            left = self.fail.get(n, 0)
            if left != 0:
                self.fail[n] = left - 1 if left > 0 else -1
                path.write_bytes(b"\x0a\x05part")      # a partial file is left behind, as a crash might
                return 1, b"", b"Error: rpc error: code = Unavailable\n"
            if n >= len(self.blocks) and n not in self.serve:
                return 1, b"", b"Error: can't read the block: NOT_FOUND\n"
            path.write_bytes(self.serve.get(n, self.blocks[n] if n < len(self.blocks) else b""))
            return 0, b"", f"Received block: {n}\n".encode()
        return 127, b"", f"unexpected command {argv}".encode()


# ------------------------------------------------------------------ fake clocks
class Clock:
    """Wall and monotonic clocks in ns. step() advances both; jump() moves only the wall clock."""

    def __init__(self, wall_ns: int = T0_NS, mono_ns: int = 5_000_000_000):
        self.wall, self.mono = wall_ns, mono_ns

    def wall_ns(self) -> int:
        return self.wall

    def mono_ns(self) -> int:
        return self.mono

    def step(self, ns: int) -> None:
        self.wall += ns
        self.mono += ns

    def sleep(self, s: float) -> None:
        self.step(int(round(s * 1e9)))

    def jump(self, ns: int) -> None:
        self.wall += ns


# ------------------------------------------------------------------ Prometheus text
def exposition(series: dict, types: dict | None = None) -> str:
    """series: {(name, (('k','v'),...)): value} -> text exposition format 0.0.4."""
    lines, seen = [], set()
    for (name, labels), value in series.items():
        base = name.rsplit("_", 1)[0] if name.endswith(("_bucket", "_sum", "_count")) else name
        fam = base if types and base in types else name
        if types and fam in types and fam not in seen:
            lines.append(f"# HELP {fam} synthetic")
            lines.append(f"# TYPE {fam} {types[fam]}")
            seen.add(fam)
        lab = "{" + ",".join(f'{k}="{v}"' for k, v in labels) + "}" if labels else ""
        lines.append(f"{name}{lab} {value}")
    return "\n".join(lines) + "\n"


ORDERER_TYPES = {"blockcutter_block_fill_duration": "histogram", "broadcast_processed_count": "counter",
                 "consensus_etcdraft_is_leader": "gauge", "consensus_etcdraft_leader_changes": "counter",
                 "consensus_etcdraft_committed_block_number": "gauge"}
PEER_TYPES = {"ledger_blockchain_height": "gauge", "ledger_block_processing_time": "histogram",
              "ledger_blockstorage_commit_time": "histogram", "ledger_statedb_commit_time": "histogram"}


def orderer_metrics(k: int, channel: str = CHANNEL, leader_changes: int = 0, leader: int = 1) -> str:
    ch = (("channel", channel),)
    s = {("blockcutter_block_fill_duration_bucket", ch + (("le", "+Inf"),)): k,
         ("blockcutter_block_fill_duration_sum", ch): round(0.05 * k, 6),
         ("blockcutter_block_fill_duration_count", ch): k,
         ("broadcast_processed_count", ch + (("status", "SUCCESS"), ("type", "ENDORSER_TRANSACTION"))): 10 * k,
         ("consensus_etcdraft_is_leader", ch): leader,
         ("consensus_etcdraft_leader_changes", ch): leader_changes,
         ("consensus_etcdraft_committed_block_number", ch): k}
    return exposition(s, ORDERER_TYPES)


def peer_metrics(k: int, channel: str = CHANNEL) -> str:
    ch = (("channel", channel),)
    s = {("ledger_blockchain_height", ch): k + 1}
    for h in ("ledger_block_processing_time", "ledger_blockstorage_commit_time", "ledger_statedb_commit_time"):
        s[(h + "_bucket", ch + (("le", "+Inf"),))] = k
        s[(h + "_sum", ch)] = round(0.01 * k, 6)
        s[(h + "_count", ch)] = k
    return exposition(s, PEER_TYPES)


# ------------------------------------------------------------------ Fabric log lines (format of tests/e1/test_e1_log_collector.py)
def _stamp(ms: int) -> str:
    import datetime as dt
    d = dt.datetime.fromtimestamp(ms // 1000, tz=dt.timezone.utc)
    return d.strftime("%Y-%m-%d %H:%M:%S") + f".{ms % 1000:03d}"


def _docker(ms: int, lag_ns: int = 250_000) -> str:
    import datetime as dt
    ns = ms * 1_000_000 + lag_ns
    d = dt.datetime.fromtimestamp(ns // 1_000_000_000, tz=dt.timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns % 1_000_000_000:09d}Z "


def orderer_log_line(block: int, ms: int, channel: str = CHANNEL, node: int = 1, lag_ns: int = 250_000) -> str:
    """lag_ns = docker receive time minus Fabric header time; negative = a clock inversion."""
    return (_docker(ms, lag_ns) + f"{_stamp(ms)} UTC 0045 INFO [orderer.consensus.etcdraft] writeBlock -> "
            f"Writing block [{block}] (Raft index: {block + 4}) to ledger channel={channel} node={node}")


def peer_log_line(block: int, ms: int, ntx: int, channel: str = CHANNEL, lag_ns: int = 400_000) -> str:
    return (_docker(ms, lag_ns) + f"{_stamp(ms)} UTC 0045 INFO [kvledger] commit -> [{channel}] Committed block "
            f"[{block}] with {ntx} transaction(s) in 25ms (state_validation=2ms block_and_pvtdata_commit=15ms "
            f"state_commit=7ms) commitHash=[47dc540c94ceb704a23875c11273e16bb0b8a87aed84de911f2133568115f254]")


BOOT_ID = "3f2c9a1e-5b7d-4e8f-9a0b-1c2d3e4f5a6b"


def identity_output(boot_id: str = BOOT_ID, release: str = "6.6.87.2-microsoft-standard-WSL2",
                    time_ns: str = "time:[4026531834]") -> bytes:
    """What e1_clock_handshake.IDENTITY_PROBE prints inside a container."""
    return f"{boot_id}\n{release}\n{time_ns}\n".encode()


# ------------------------------------------------------------------ docker doubles
def stats_line(name: str, cid: str | None = None, cpu: str = "12.50%", mem: str = "105.3MiB / 7.637GiB",
               memp: str = "1.35%", net: str = "1.2MB / 845kB", blk: str = "0B / 4.1MB", pids: str = "21") -> str:
    return json.dumps({"BlockIO": blk, "CPUPerc": cpu, "Container": (cid or name)[:12], "ID": cid or ("c" * 64),
                       "MemPerc": memp, "MemUsage": mem, "Name": name, "NetIO": net, "PIDs": pids})


def inspect_doc(name: str, cid: str | None = None, started: str = "2023-11-14T22:00:00.000000000Z",
                restarts: int = 0, oom: bool = False, status: str = "running") -> dict:
    return {"Id": cid or ("c" * 64), "Name": "/" + name, "RestartCount": restarts,
            "State": {"Status": status, "Running": status == "running", "OOMKilled": oom, "StartedAt": started}}
