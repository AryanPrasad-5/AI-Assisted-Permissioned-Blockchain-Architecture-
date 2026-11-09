"""Channel per run (E1 v0.2 protocol section 9, OPT-2 B; LA-2): create a fresh channel carrying the
requested (B, tau, P), join orderers and peers, commit the chaincode definition, then record and
verify the effective configuration from the decoded config block.

Status: written 2026-11-09, tested with a fake command runner only. Never executed against Fabric.
Command syntax follows the Fabric v2.5 CLI (configtxgen, osnadmin, peer, configtxlator) as used by
fabric-samples test-network; E0-0 must confirm it on the pinned binaries before a VERIFIED record is
relied on.

Symbols (as in the E1 protocol and e1_channel_config.py):
  B    Orderer.BatchSize.MaxMessageCount     block size in transactions   (--max-message-count)
  tau  Orderer.BatchTimeout                  Go duration, whole ms        (--batch-timeout)
  P    Orderer.BatchSize.PreferredMaxBytes   bytes                        (--preferred-max-bytes)
  A    Orderer.BatchSize.AbsoluteMaxBytes    bytes; default keeps the base configtx value

Sequence (each step's argv, exit code, timing and output hashes are recorded):
  1  render     copy the base configtx.yaml with B, tau, P (and A if given) set in the
                &OrdererDefaults section; relative MSPDir/cert paths made absolute. Fails if the
                profile overrides batch settings or does not inherit *OrdererDefaults.
  2  configtxgen -outputBlock genesis.block
  3  decode genesis; GATE: genesis must carry the requested values, else stop before any join
  4  osnadmin channel join (each orderer, expect HTTP 201); osnadmin channel list until active
  5  peer channel join (each peer); peer channel getinfo (height >= 1)
  6  approveformyorg (one peer per org), checkcommitreadiness, commit, querycommitted
  7  peer channel fetch config from the orderer; decode; verify the EFFECTIVE configuration:
     B, tau, P, P <= A, consensus type, consenter count, application orgs, anchor peers (optional),
     config block number, and that its data hash equals the generated genesis block's
Status / exit code:
  VERIFIED          0   effective configuration equals the request
  CONFIG_MISMATCH   1   a decoded value differs from the request (genesis or effective)
  INVALID_REQUEST   2   bad input, missing file or existing run dir; nothing executed
  INCOMPLETE_SETUP  3   a setup command failed; later steps not run
  UNVERIFIABLE      4   the configuration could not be fetched, decoded or read
The record channel_run_record.json is written for every status except INVALID_REQUEST.

Usage (dry run prints the plan and writes nothing; --execute needs Fabric authorization):
  python scripts/e1/e1_channel_run.py --network network.json --channel e1-r001 --run-id r001
         --max-message-count 45 --batch-timeout 2s --preferred-max-bytes 10485760
         --run-dir runs/r001/channel [--absolute-max-bytes N] [--execute]
Standard library only.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import posixpath
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_channel_config as ccfg  # noqa: E402

TOOL_VERSION = "e1_channel_run 0.1 (2026-11-09)"
UINT32_MAX = 2**32 - 1
TAU_MIN_MS, TAU_MAX_MS = 1, 60_000          # script sanity bound, not a Fabric limit
CHANNEL_RE = re.compile(r"^[a-z][a-z0-9.-]{0,248}$")   # Fabric: lowercase alnum . -, letter first, < 250
RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
PACKAGE_ID_RE = re.compile(r"^[A-Za-z0-9_.+-]+:[0-9a-f]{64}$")
BATCH_KEYS = ("BatchTimeout", "MaxMessageCount", "AbsoluteMaxBytes", "PreferredMaxBytes")
PATH_KEYS = ("MSPDir", "ClientTLSCert", "ServerTLSCert", "Identity")
_TAU = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")  # used with .match(s, pos): anchored at pos
_TAU_NS = {"ns": 1, "us": 1_000, "µs": 1_000, "ms": 1_000_000, "s": 1_000_000_000, "m": 60_000_000_000,
           "h": 3_600_000_000_000}
_BYTES = re.compile(r"^\s*(\d+)\s*(?:([KkMmGg])[Bb]?)?\s*$")

VERIFIED, CONFIG_MISMATCH, INVALID_REQUEST = "VERIFIED", "CONFIG_MISMATCH", "INVALID_REQUEST"
INCOMPLETE_SETUP, UNVERIFIABLE = "INCOMPLETE_SETUP", "UNVERIFIABLE"
EXIT = {VERIFIED: 0, CONFIG_MISMATCH: 1, INVALID_REQUEST: 2, INCOMPLETE_SETUP: 3, UNVERIFIABLE: 4}


class RequestError(ValueError):
    """Invalid request or network description: nothing is executed."""


class ConfigtxError(RequestError):
    """The base configtx.yaml cannot be rendered safely."""


# ------------------------------------------------------------------ request
def tau_to_ms(text: str) -> int:
    """Go duration string to whole milliseconds, exactly. Rejects sub-ms remainders and signs."""
    s, total, pos = text.strip(), Decimal(0), 0
    if not s:
        raise RequestError("BatchTimeout is empty")
    while pos < len(s):
        m = _TAU.match(s, pos)
        if not m:
            raise RequestError(f"BatchTimeout {text!r} is not a Go duration such as 2s or 500ms")
        try:
            total += Decimal(m.group(1)) * _TAU_NS[m.group(2)]
        except InvalidOperation as err:
            raise RequestError(f"BatchTimeout {text!r}: {err}") from err
        pos = m.end()
    if total % 1_000_000:
        raise RequestError(f"BatchTimeout {text!r} is not a whole number of milliseconds")
    return int(total // 1_000_000)


def canonical_tau(ms: int) -> str:
    return f"{ms // 1000}s" if ms % 1000 == 0 else f"{ms}ms"


def parse_byte_size(text: str) -> int:
    """configtx byte size ("512 KB", "99 MB", "524288") to bytes; K/M/G are powers of 1024."""
    m = _BYTES.match(str(text))
    if not m:
        raise ConfigtxError(f"byte size {text!r} not understood")
    return int(m.group(1)) * {None: 1, "k": 1024, "m": 1024**2, "g": 1024**3}[m.group(2) and m.group(2).lower()]


def _strict_int(name: str, v) -> int:
    if isinstance(v, bool) or not isinstance(v, int):
        raise RequestError(f"{name} must be an integer, got {v!r}")
    return v


@dataclass
class Request:
    channel: str
    run_id: str
    max_message_count: int
    batch_timeout: str
    preferred_max_bytes: int
    absolute_max_bytes: int | None = None
    tau_ms: int = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.channel, str) or not CHANNEL_RE.match(self.channel):
            raise RequestError(f"channel {self.channel!r}: lowercase letters, digits, '.', '-'; starts with a letter; < 250")
        if not isinstance(self.run_id, str) or not RUN_ID_RE.match(self.run_id):
            raise RequestError(f"run id {self.run_id!r} must match {RUN_ID_RE.pattern}")
        b = _strict_int("MaxMessageCount (B)", self.max_message_count)
        if not 1 <= b <= UINT32_MAX:
            raise RequestError(f"MaxMessageCount (B) {b} outside 1..{UINT32_MAX}")
        p = _strict_int("PreferredMaxBytes (P)", self.preferred_max_bytes)
        if not 1 <= p <= UINT32_MAX:
            raise RequestError(f"PreferredMaxBytes (P) {p} outside 1..{UINT32_MAX}")
        if self.absolute_max_bytes is not None:
            a = _strict_int("AbsoluteMaxBytes (A)", self.absolute_max_bytes)
            if not 1 <= a <= UINT32_MAX:
                raise RequestError(f"AbsoluteMaxBytes (A) {a} outside 1..{UINT32_MAX}")
            if p > a:
                raise RequestError(f"PreferredMaxBytes (P) {p} > AbsoluteMaxBytes (A) {a}")
        if not isinstance(self.batch_timeout, str):
            raise RequestError(f"BatchTimeout must be a string, got {self.batch_timeout!r}")
        self.tau_ms = tau_to_ms(self.batch_timeout)
        if not TAU_MIN_MS <= self.tau_ms <= TAU_MAX_MS:
            raise RequestError(f"BatchTimeout {self.tau_ms} ms outside the script bound {TAU_MIN_MS}..{TAU_MAX_MS} ms")

    def as_record(self) -> dict:
        return {"channel": self.channel, "run_id": self.run_id, "B_max_message_count": self.max_message_count,
                "tau_batch_timeout": self.batch_timeout, "tau_ms": self.tau_ms,
                "tau_written": canonical_tau(self.tau_ms), "P_preferred_max_bytes": self.preferred_max_bytes,
                "A_absolute_max_bytes": self.absolute_max_bytes}


# ------------------------------------------------------------------ configtx rendering
_KEY_LINE = re.compile(r"^(?P<indent>\s*)(?P<dash>-\s+)?(?P<key>[A-Za-z_][\w.-]*):(?P<sp>\s*)(?P<value>[^#]*?)"
                       r"(?P<comment>\s+#.*)?$")


def _code(line: str) -> str:
    """Line without a trailing comment, for key detection."""
    stripped = line.lstrip()
    return "" if stripped.startswith("#") else line


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _block_end(lines: list[str], start: int, parent_indent: int) -> int:
    """First index after `start` whose code line is indented <= parent_indent."""
    for i in range(start + 1, len(lines)):
        if _code(lines[i]).strip() and _indent(lines[i]) <= parent_indent:
            return i
    return len(lines)


def _find_key(lines: list[str], lo: int, hi: int, key: str) -> list[int]:
    out = []
    for i in range(lo, hi):
        m = _KEY_LINE.match(_code(lines[i]))
        if m and m.group("key") == key:
            out.append(i)
    return out


def render_configtx(base_text: str, base_dir: str, profile: str, req: Request) -> tuple[str, list[dict], dict]:
    """Return (rendered text, changed lines, base batch values). Raises ConfigtxError."""
    newline = "\r\n" if "\r\n" in base_text else "\n"
    lines = base_text.split(newline)
    anchors = [i for i, l in enumerate(lines) if re.match(r"^Orderer:\s*&OrdererDefaults\s*(#.*)?$", l)]
    if len(anchors) != 1:
        raise ConfigtxError(f"expected one top-level 'Orderer: &OrdererDefaults' line, found {len(anchors)}")
    o_start = anchors[0]
    o_end = _block_end(lines, o_start, 0)

    # every batch key exactly once, and only inside &OrdererDefaults
    where: dict[str, int] = {}
    for key in BATCH_KEYS + ("BatchSize",):
        inside = _find_key(lines, o_start + 1, o_end, key)
        outside = _find_key(lines, 0, o_start, key) + _find_key(lines, o_end, len(lines), key)
        if len(inside) != 1:
            raise ConfigtxError(f"{key} appears {len(inside)} times in &OrdererDefaults (need exactly 1)")
        if outside:
            raise ConfigtxError(f"{key} also set outside &OrdererDefaults (line {outside[0] + 1}): an override "
                                "would make the requested value unverifiable from the file")
        where[key] = inside[0]
    bs = where["BatchSize"]
    for key in ("MaxMessageCount", "AbsoluteMaxBytes", "PreferredMaxBytes"):
        if not (bs < where[key] < _block_end(lines, bs, _indent(lines[bs]))):
            raise ConfigtxError(f"{key} is not nested under BatchSize")

    # the profile must exist and inherit the defaults
    prof = [i for i, l in enumerate(lines) if re.match(r"^Profiles:\s*(#.*)?$", l)]
    if len(prof) != 1:
        raise ConfigtxError("expected one top-level 'Profiles:' section")
    p_end = _block_end(lines, prof[0], 0)
    hits = [i for i in range(prof[0] + 1, p_end)
            if re.match(rf"^\s+{re.escape(profile)}:\s*(#.*)?$", lines[i])]
    if len(hits) != 1:
        raise ConfigtxError(f"profile {profile!r} found {len(hits)} times under Profiles")
    pr_end = _block_end(lines, hits[0], _indent(lines[hits[0]]))
    if not any("*OrdererDefaults" in _code(lines[i]) for i in range(hits[0] + 1, pr_end)):
        raise ConfigtxError(f"profile {profile!r} does not inherit *OrdererDefaults")

    base_vals = {k: _KEY_LINE.match(lines[where[k]]).group("value").strip() for k in BATCH_KEYS}
    base_a = parse_byte_size(base_vals["AbsoluteMaxBytes"])
    a = req.absolute_max_bytes if req.absolute_max_bytes is not None else base_a
    if req.preferred_max_bytes > a:
        raise ConfigtxError(f"PreferredMaxBytes (P) {req.preferred_max_bytes} > AbsoluteMaxBytes {a}")
    new_vals = {"BatchTimeout": canonical_tau(req.tau_ms), "MaxMessageCount": str(req.max_message_count),
                "PreferredMaxBytes": str(req.preferred_max_bytes)}
    if req.absolute_max_bytes is not None:
        new_vals["AbsoluteMaxBytes"] = str(req.absolute_max_bytes)

    changes = []

    def _set(i: int, key: str, value: str, why: str) -> None:
        m = _KEY_LINE.match(lines[i])
        new = f"{m.group('indent')}{m.group('dash') or ''}{key}: {value}{m.group('comment') or ''}"
        if new != lines[i]:
            changes.append({"line": i + 1, "key": key, "reason": why, "before": lines[i], "after": new})
            lines[i] = new

    for key, value in new_vals.items():
        _set(where[key], key, value, "requested")
    for i, line in enumerate(lines):
        m = _KEY_LINE.match(_code(line))
        if not m or m.group("key") not in PATH_KEYS:
            continue
        raw = m.group("value").strip()
        quote = raw[0] if raw[:1] in "'\"" and raw[-1:] == raw[:1] and len(raw) > 1 else ""
        path = raw[1:-1] if quote else raw
        if path and not posixpath.isabs(path):
            _set(i, m.group("key"), f"{quote}{posixpath.normpath(posixpath.join(base_dir, path))}{quote}",
                 "relative path made absolute (configtxgen resolves it against the base file's directory)")
    rendered = newline.join(lines)

    # self-check: the rendered file now states exactly the requested values
    for key, value in new_vals.items():
        got = _KEY_LINE.match(lines[where[key]]).group("value").strip()
        if got != value:
            raise ConfigtxError(f"render self-check failed for {key}: {got!r} != {value!r}")
    return rendered, changes, {**base_vals, "AbsoluteMaxBytes_bytes": base_a}


# ------------------------------------------------------------------ network description
def _need(d: dict, key: str, ctx: str):
    if key not in d or d[key] in (None, ""):
        raise RequestError(f"network {ctx}: missing {key!r}")
    return d[key]


@dataclass
class Network:
    raw: dict

    def __post_init__(self) -> None:
        r = self.raw
        for k in ("configtx_base", "profile", "fabric_cfg_path", "orderer_ca", "orderers", "peers", "chaincode"):
            _need(r, k, "top level")
        if not r["orderers"] or not r["peers"]:
            raise RequestError("network: need at least one orderer and one peer")
        for o in r["orderers"]:
            for k in ("name", "address", "tls_hostname", "admin_address", "admin_client_cert", "admin_client_key"):
                _need(o, k, f"orderer {o.get('name')}")
        for p in r["peers"]:
            for k in ("name", "msp_id", "address", "tls_root_cert", "admin_msp_dir"):
                _need(p, k, f"peer {p.get('name')}")
        names = [x["name"] for x in r["orderers"] + r["peers"]]
        if len(set(names)) != len(names):
            raise RequestError("network: duplicate orderer/peer names")
        cc = r["chaincode"]
        for k in ("name", "version", "sequence", "package_id"):
            _need(cc, k, "chaincode")
        if not PACKAGE_ID_RE.match(str(cc["package_id"])):
            raise RequestError(f"chaincode package_id {cc['package_id']!r} is not <label>:<sha256 hex>")
        _strict_int("chaincode sequence", cc["sequence"])
        rel = r.get("configtx_relative_to")
        if rel is not None and (not isinstance(rel, str) or not rel.startswith("/")):
            raise RequestError(f"configtx_relative_to {rel!r} must be an absolute POSIX directory")
        if r.get("expect", {}).get("raft_leader_wait"):
            bad = [o["name"] for o in r["orderers"] if not str(o.get("ops_url", "")).startswith("http")]
            if bad:
                raise RequestError(f"expect.raft_leader_wait needs an ops_url (operations /metrics) for {bad}")

    def relative_base(self, base_path: Path) -> str:
        """Directory against which relative MSPDir/cert paths of the base configtx are resolved: the
        optional configtx_relative_to (e.g. fabric-samples test-network/configtx, so that a configtx kept
        in the repo reuses the samples' ../organizations layout), else the base file's own directory."""
        return self.raw.get("configtx_relative_to") or posixpath.dirname(base_path.resolve().as_posix())

    def files(self) -> list[str]:
        r = self.raw
        out = [r["configtx_base"], r["orderer_ca"]]
        out += [o[k] for o in r["orderers"] for k in ("admin_client_cert", "admin_client_key")]
        out += [p[k] for p in r["peers"] for k in ("tls_root_cert", "admin_msp_dir")]
        return out

    def tool(self, name: str) -> str:
        return posixpath.join(self.raw["bin_dir"], name) if self.raw.get("bin_dir") else name

    def orgs(self) -> list[dict]:
        """First peer of each MSP ID, in file order."""
        seen, out = set(), []
        for p in self.raw["peers"]:
            if p["msp_id"] not in seen:
                seen.add(p["msp_id"])
                out.append(p)
        return out

    def peer_env(self, p: dict) -> dict:
        return {"CORE_PEER_TLS_ENABLED": "true", "CORE_PEER_LOCALMSPID": p["msp_id"],
                "CORE_PEER_TLS_ROOTCERT_FILE": p["tls_root_cert"], "CORE_PEER_MSPCONFIGPATH": p["admin_msp_dir"],
                "CORE_PEER_ADDRESS": p["address"], "FABRIC_CFG_PATH": self.raw["fabric_cfg_path"]}


# ------------------------------------------------------------------ plan
@dataclass
class Step:
    label: str
    argv: list[str]
    env: dict = field(default_factory=dict)
    check: str = "exit0"          # how the output is validated
    on_fail: str = INCOMPLETE_SETUP
    attempts: int = 1
    retry_sleep_s: float = 0.0
    target: str = ""


def plan(req: Request, net: Network, run_dir: str) -> list[Step]:
    r, ch, cc = net.raw, req.channel, net.raw["chaincode"]
    j = posixpath.join
    gen, gen_json = j(run_dir, "genesis.block"), j(run_dir, "genesis.json")
    cfg, cfg_json = j(run_dir, "config_block.pb"), j(run_dir, "config_block.json")
    o0 = r["orderers"][0]
    tls = ["-o", o0["address"], "--ordererTLSHostnameOverride", o0["tls_hostname"], "--tls", "--cafile", r["orderer_ca"]]
    env0 = net.peer_env(net.orgs()[0])
    steps = [
        Step("configtxgen", [net.tool("configtxgen"), "-configPath", j(run_dir, "configtx"), "-profile", r["profile"],
                             "-outputBlock", gen, "-channelID", ch]),
        Step("decode_genesis", [net.tool("configtxlator"), "proto_decode", "--input", gen, "--type", "common.Block",
                                "--output", gen_json], check="genesis_gate", on_fail=UNVERIFIABLE),
    ]
    for o in r["orderers"]:
        admin = ["-o", o["admin_address"], "--ca-file", r["orderer_ca"], "--client-cert", o["admin_client_cert"],
                 "--client-key", o["admin_client_key"]]
        steps.append(Step(f"osnadmin_join:{o['name']}", [net.tool("osnadmin"), "channel", "join", "--channelID", ch,
                                                         "--config-block", gen] + admin, check="osn_join", target=o["name"]))
    for o in r["orderers"]:
        admin = ["-o", o["admin_address"], "--ca-file", r["orderer_ca"], "--client-cert", o["admin_client_cert"],
                 "--client-key", o["admin_client_key"]]
        steps.append(Step(f"osnadmin_list:{o['name']}", [net.tool("osnadmin"), "channel", "list", "--channelID", ch] + admin,
                          check="osn_active", attempts=int(r.get("poll_attempts", 10)),
                          retry_sleep_s=float(r.get("poll_sleep_s", 1.0)), target=o["name"]))
    for p in r["peers"]:
        steps.append(Step(f"peer_join:{p['name']}", [net.tool("peer"), "channel", "join", "-b", gen], env=net.peer_env(p),
                          attempts=int(r.get("peer_join_attempts", 3)), retry_sleep_s=float(r.get("peer_join_sleep_s", 2.0)),
                          target=p["name"]))
    for p in r["peers"]:
        steps.append(Step(f"peer_getinfo:{p['name']}", [net.tool("peer"), "channel", "getinfo", "-c", ch],
                          env=net.peer_env(p), check="getinfo", attempts=int(r.get("poll_attempts", 10)),
                          retry_sleep_s=float(r.get("poll_sleep_s", 1.0)), target=p["name"]))
    if r.get("expect", {}).get("raft_leader_wait"):
        # E0-0 (2026-11-09): "active" is not "has a Raft leader"; a broadcast sent before the election is
        # rejected (SERVICE_UNAVAILABLE). Poll the orderers' consensus_etcdraft_is_leader for this channel.
        steps.append(Step("raft_leader", ["internal:raft_leader", ch], check="raft_leader",
                          attempts=int(r.get("leader_attempts", 30)), retry_sleep_s=float(r.get("leader_sleep_s", 1.0))))
    lc = [net.tool("peer"), "lifecycle", "chaincode"]
    ident = ["--channelID", ch, "--name", cc["name"], "--version", str(cc["version"]), "--sequence", str(cc["sequence"])]
    for p in net.orgs():
        steps.append(Step(f"approve:{p['msp_id']}", lc + ["approveformyorg"] + tls + ident + ["--package-id", cc["package_id"]],
                          env=net.peer_env(p), target=p["msp_id"]))
    steps.append(Step("checkcommitreadiness", lc + ["checkcommitreadiness"] + ident + ["--output", "json"], env=env0,
                      check="approvals", attempts=int(r.get("poll_attempts", 10)), retry_sleep_s=float(r.get("poll_sleep_s", 1.0))))
    endorsers = [a for p in net.orgs() for a in ("--peerAddresses", p["address"], "--tlsRootCertFiles", p["tls_root_cert"])]
    steps.append(Step("commit", lc + ["commit"] + tls + ident + endorsers, env=env0))
    steps.append(Step("querycommitted", lc + ["querycommitted", "--channelID", ch, "--name", cc["name"], "--output", "json"],
                      env=env0, check="committed", attempts=int(r.get("poll_attempts", 10)),
                      retry_sleep_s=float(r.get("poll_sleep_s", 1.0))))
    steps.append(Step("fetch_config", [net.tool("peer"), "channel", "fetch", "config", cfg, "-c", ch] + tls, env=env0,
                      on_fail=UNVERIFIABLE))
    steps.append(Step("decode_config", [net.tool("configtxlator"), "proto_decode", "--input", cfg, "--type", "common.Block",
                                        "--output", cfg_json], check="effective", on_fail=UNVERIFIABLE))
    return steps


# ------------------------------------------------------------------ output checks
def parse_osnadmin(text: str) -> tuple[int | None, dict | None]:
    m = re.search(r"^Status:\s*(\d{3})\s*$", text, re.M)
    if not m:
        return None, None
    body = text[m.end():].strip()
    try:
        return int(m.group(1)), (json.loads(body) if body else None)
    except json.JSONDecodeError:
        return int(m.group(1)), None


def _load_json_file(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def block_facts(doc: dict) -> dict:
    hdr = doc["header"]
    return {"header_number": int(hdr.get("number", 0) or 0), "data_hash": hdr["data_hash"]}


def app_orgs(doc: dict) -> dict[str, dict]:
    """Application org group -> {msp_id, anchor_peers}."""
    config = doc["data"]["data"][0]["payload"]["data"]["config"]
    groups = config["channel_group"]["groups"]["Application"]["groups"]
    out = {}
    for name, g in groups.items():
        vals = g.get("values", {})
        out[name] = {"msp_id": vals["MSP"]["value"]["config"]["name"],
                     "anchor_peers": vals.get("AnchorPeers", {}).get("value", {}).get("anchor_peers", [])}
    return out


def verify_config(doc: dict, req: Request, net: Network, absolute_max_bytes: int) -> list[dict]:
    """Checks of a decoded config block against the request. KeyError etc. mean UNVERIFIABLE.
    absolute_max_bytes is A as requested, or the base configtx value when A was not requested."""
    exp = net.raw.get("expect", {})
    found = ccfg.orderer_values(doc)
    rows = ccfg.verify(found, max_message_count=req.max_message_count, batch_timeout=canonical_tau(req.tau_ms),
                       preferred_max_bytes=req.preferred_max_bytes, absolute_max_bytes=absolute_max_bytes,
                       consensus=exp.get("consensus"), consenters=exp.get("consenters"))
    checks = [{"check": n, "requested": w, "decoded": g, "pass": bool(ok)} for n, w, g, ok in rows]
    checks.append({"check": "BatchTimeout_raw", "requested": canonical_tau(req.tau_ms),
                   "decoded": found["batch_timeout_raw"], "pass": True, "note": "Go Duration.String(); compared in ms above"})
    orgs = app_orgs(doc)
    have = sorted(o["msp_id"] for o in orgs.values())
    want = sorted({p["msp_id"] for p in net.raw["peers"]})
    checks.append({"check": "ApplicationOrgs", "requested": want, "decoded": have, "pass": set(want) <= set(have)})
    if exp.get("anchor_peers_required"):
        missing = sorted(o["msp_id"] for o in orgs.values() if not o["anchor_peers"])
        checks.append({"check": "AnchorPeers", "requested": "every application org", "decoded":
                       {o["msp_id"]: o["anchor_peers"] for o in orgs.values()}, "pass": not missing})
    return checks


# ------------------------------------------------------------------ execution
Runner = Callable[[list[str], dict], "tuple[int, bytes, bytes]"]


def subprocess_runner(argv: list[str], env: dict) -> tuple[int, bytes, bytes]:
    proc = subprocess.run(argv, env={**os.environ, **env}, capture_output=True, timeout=600, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def _http_text(url: str, timeout: float = 3.0) -> str:
    import urllib.request
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def leader_probe(net: "Network", channel: str, fetch: Callable[[str], str] = _http_text) -> dict:
    """consensus_etcdraft_is_leader{channel=<channel>} per orderer: a number, None (series absent) or 'error: …'."""
    pat = re.compile(rf'^consensus_etcdraft_is_leader\{{channel="{re.escape(channel)}"\}} (\S+)$', re.M)
    out = {}
    for o in net.raw["orderers"]:
        try:
            m = pat.search(fetch(o["ops_url"]))
            out[o["name"]] = float(m.group(1)) if m else None
        except (OSError, ValueError, KeyError) as exc:
            out[o["name"]] = f"error: {exc!r}"
    return out


def _utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def _sha(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def run(req: Request, net: Network, run_dir: Path, runner: Runner, *, sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic, now: Callable[[], str] = _utc,
        argv: list[str] | None = None, check_files: bool = True,
        probe: Callable[["Network", str], dict] = leader_probe) -> dict:
    """Execute the plan. Returns the record (also written to run_dir/channel_run_record.json).

    Raises RequestError before anything is created when the request, the network description, a
    referenced file or the run directory is not acceptable."""
    if run_dir.exists():
        raise RequestError(f"run dir {run_dir} exists; refusing to reuse or overwrite")
    if net.raw.get("bootstrap"):
        raise RequestError("bootstrap network description (placeholder package id): for e1_chaincode_deploy.py only")
    if check_files:
        absent = [f for f in net.files() if not Path(f).exists()]
        if absent:
            raise RequestError(f"files named in the network description do not exist: {absent}")
    base_path = Path(net.raw["configtx_base"])
    base_bytes = base_path.read_bytes()
    base_dir = net.relative_base(base_path)
    rendered, changes, base_vals = render_configtx(base_bytes.decode("utf-8"), base_dir, net.raw["profile"], req)
    run_dir_posix = run_dir.resolve().as_posix()
    steps = plan(req, net, run_dir_posix)

    run_dir.mkdir(parents=True)
    (run_dir / "configtx").mkdir()
    (run_dir / "cmd").mkdir()
    cfgtx = run_dir / "configtx" / "configtx.yaml"
    cfgtx.write_bytes(rendered.encode("utf-8"))
    record: dict = {
        "tool": TOOL_VERSION, "script_sha256": _sha(Path(__file__)),
        "verifier": {"path": "scripts/e1/e1_channel_config.py", "sha256": _sha(Path(ccfg.__file__))},
        "argv": argv, "created_utc": now(), "request": req.as_record(),
        "network": {"sha256": hashlib.sha256(json.dumps(net.raw, sort_keys=True).encode()).hexdigest(),
                    "description": net.raw},
        "configtx": {"base_path": str(base_path), "base_sha256": hashlib.sha256(base_bytes).hexdigest(),
                     "base_batch_values": base_vals, "rendered_path": str(cfgtx), "rendered_sha256": _sha(cfgtx),
                     "profile": net.raw["profile"], "changes": changes},
        "steps": [], "genesis": None, "effective": None, "checks": [], "participation": {}, "peers": {},
        "chaincode": None, "status": None, "failed_step": None, "detail": "",
    }
    t_first = monotonic()
    status = VERIFIED
    try:
        for k, step in enumerate(steps):
            ok, detail = False, ""
            for attempt in range(1, step.attempts + 1):
                t0, ts0 = monotonic(), now()
                try:
                    if step.check == "raft_leader":
                        code, out, err = 0, json.dumps(probe(net, req.channel), sort_keys=True).encode(), b""
                    else:
                        code, out, err = runner(step.argv, step.env)
                except (OSError, subprocess.SubprocessError) as exc:
                    code, out, err = None, b"", repr(exc).encode()
                t1 = monotonic()
                stem = f"{k:02d}_{step.label.replace(':', '_')}_a{attempt}"
                (run_dir / "cmd" / f"{stem}.stdout").write_bytes(out)
                (run_dir / "cmd" / f"{stem}.stderr").write_bytes(err)
                ok, detail, status_if_bad = _check(step, code, out, err, run_dir, req, net, record)
                record["steps"].append({
                    "index": k, "label": step.label, "attempt": attempt, "argv": step.argv, "env": step.env,
                    "start_utc": ts0, "duration_s": round(t1 - t0, 6), "exit_code": code, "ok": ok, "detail": detail,
                    "stdout": {"file": f"cmd/{stem}.stdout", "sha256": hashlib.sha256(out).hexdigest()},
                    "stderr": {"file": f"cmd/{stem}.stderr", "sha256": hashlib.sha256(err).hexdigest()}})
                if step.check == "raft_leader" and record.get("raft_leader"):
                    record["raft_leader"]["attempt"] = attempt
                if ok or status_if_bad == CONFIG_MISMATCH:
                    break
                if attempt < step.attempts:
                    sleep(step.retry_sleep_s)
            if not ok:
                status = status_if_bad
                record["failed_step"], record["detail"] = step.label, detail
                break
    except Exception as exc:  # recorded, then re-raised: an internal error is never reported as VERIFIED
        status, record["failed_step"], record["detail"] = UNVERIFIABLE, "internal_error", repr(exc)
        raise
    finally:
        record["timing"] = {"setup_s": round(monotonic() - t_first, 6),
                            "note": "first command start to last command end (E0-1 reset cost input)"}
        record["status"] = status if record["failed_step"] or status != VERIFIED else _final(record)
        record["finished_utc"] = now()
        with (run_dir / "channel_run_record.json").open("x", encoding="utf-8", newline="\n") as fh:
            json.dump(record, fh, indent=2, default=str)
            fh.write("\n")
    return record


def _final(record: dict) -> str:
    if record["effective"] is None:
        record["detail"] = "plan ended without an effective-configuration check"
        return UNVERIFIABLE
    return VERIFIED if all(c["pass"] for c in record["checks"]) else CONFIG_MISMATCH


def _check(step: Step, code, out: bytes, err: bytes, run_dir: Path, req: Request, net: Network,
           record: dict) -> tuple[bool, str, str]:
    """Validate one command. Returns (ok, detail, status to use if not ok)."""
    if code != 0:
        return False, f"exit code {code}: {err.decode('utf-8', 'replace').strip()[-400:]}", step.on_fail
    text = out.decode("utf-8", "replace")
    both = text + "\n" + err.decode("utf-8", "replace")
    if step.check == "exit0":
        return True, "exit 0", step.on_fail
    if step.check == "osn_join":
        st, body = parse_osnadmin(both)
        if st != 201 or not body:
            return False, f"osnadmin join: HTTP {st}, body {body!r} (want 201)", step.on_fail
        record["participation"][step.target] = {"join": body}
        return True, f"HTTP 201 {body.get('consensusRelation')}/{body.get('status')}", step.on_fail
    if step.check == "osn_active":
        st, body = parse_osnadmin(both)
        good = st == 200 and body and body.get("status") == "active" and body.get("consensusRelation") == "consenter"
        record["participation"].setdefault(step.target, {})["list"] = body
        return bool(good), f"HTTP {st} {body!r}", step.on_fail
    if step.check == "getinfo":
        m = re.search(r"Blockchain info:\s*(\{.*\})", both)
        try:
            info = json.loads(m.group(1)) if m else None
            good = bool(info) and int(info.get("height", 0)) >= 1
        except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
            info, good = None, False
        record["peers"][step.target] = info
        return good, f"getinfo {info!r}", step.on_fail
    if step.check == "raft_leader":
        try:
            values = json.loads(text)
        except json.JSONDecodeError:
            return False, f"leader probe output not understood: {text[:200]!r}", step.on_fail
        numeric = all(isinstance(v, (int, float)) for v in values.values())
        leaders = sorted(k for k, v in values.items() if v == 1)
        good = bool(values) and numeric and len(leaders) == 1
        record["raft_leader"] = {"leader": leaders[0] if good else None, "values": values}
        return good, (f"leader {leaders[0]}" if good else f"no single Raft leader yet: {values}"), step.on_fail
    if step.check == "approvals":
        try:
            approvals = json.loads(text)["approvals"]
        except (json.JSONDecodeError, KeyError, TypeError):
            return False, f"checkcommitreadiness output not understood: {text[:200]!r}", step.on_fail
        want = {p["msp_id"] for p in net.orgs()}
        good = want <= set(approvals) and all(approvals[m] is True for m in want)
        return good, f"approvals {approvals}", step.on_fail
    if step.check == "committed":
        cc = net.raw["chaincode"]
        try:
            q = json.loads(text)
            good = (int(q["sequence"]) == int(cc["sequence"]) and str(q["version"]) == str(cc["version"])
                    and all(q.get("approvals", {}).get(p["msp_id"]) is True for p in net.orgs()))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            return False, f"querycommitted output not understood: {text[:200]!r}", step.on_fail
        record["chaincode"] = q
        return good, f"committed {q}", step.on_fail
    if step.check in ("genesis_gate", "effective"):
        name = "genesis.json" if step.check == "genesis_gate" else "config_block.json"
        path = run_dir / name
        a_expected = (req.absolute_max_bytes if req.absolute_max_bytes is not None
                      else record["configtx"]["base_batch_values"]["AbsoluteMaxBytes_bytes"])
        try:
            doc = _load_json_file(path)
            facts = block_facts(doc)
            checks = verify_config(doc, req, net, a_expected)
            found = ccfg.orderer_values(doc)
        except (OSError, ValueError, KeyError, IndexError, TypeError) as exc:
            return False, f"{name} cannot be read or lacks a required field: {exc!r}", UNVERIFIABLE
        info = {"decoded_path": str(path), "decoded_sha256": _sha(path), **facts, "orderer_values": found,
                "application_orgs": app_orgs(doc)}
        blk = path.with_name("genesis.block" if step.check == "genesis_gate" else "config_block.pb")
        info["block_path"], info["block_sha256"] = str(blk), _sha(blk)
        if step.check == "genesis_gate":
            record["genesis"] = info
            bad = [c["check"] for c in checks if not c["pass"]]
            if bad:
                record["checks"] = [{**c, "source": "genesis"} for c in checks]
                return False, f"genesis block does not carry the request ({bad}); no orderer or peer joined", \
                    CONFIG_MISMATCH
            return True, "genesis carries the requested values", UNVERIFIABLE
        record["effective"] = info
        gen = record["genesis"] or {}
        want_num = int(net.raw.get("expect", {}).get("config_block_number", 0))
        checks.append({"check": "ConfigBlockNumber", "requested": want_num, "decoded": facts["header_number"],
                       "pass": facts["header_number"] == want_num})
        checks.append({"check": "ConfigBlockIsGeneratedGenesis", "requested": gen.get("data_hash"),
                       "decoded": facts["data_hash"],
                       "pass": want_num != 0 or facts["data_hash"] == gen.get("data_hash")})
        record["checks"] = [{**c, "source": "orderer config block"} for c in checks]
        bad = [c["check"] for c in checks if not c["pass"]]
        if bad:
            return False, f"effective configuration differs from the request: {bad}", CONFIG_MISMATCH
        return True, "effective configuration equals the request", UNVERIFIABLE
    raise AssertionError(f"unknown check {step.check}")


# ------------------------------------------------------------------ CLI
def main(argv: list[str] | None = None, runner: Runner | None = None, platform_name: str = sys.platform,
         **run_kw) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--network", required=True, type=Path)
    ap.add_argument("--channel", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--max-message-count", required=True, type=int, help="B")
    ap.add_argument("--batch-timeout", required=True, help="tau, Go duration")
    ap.add_argument("--preferred-max-bytes", required=True, type=int, help="P")
    ap.add_argument("--absolute-max-bytes", type=int, help="A (default: keep the base configtx value)")
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--execute", action="store_true", help="run the commands (needs Fabric authorization)")
    args = ap.parse_args(argv)
    try:
        req = Request(args.channel, args.run_id, args.max_message_count, args.batch_timeout,
                      args.preferred_max_bytes, args.absolute_max_bytes)
        net = Network(json.loads(args.network.read_text(encoding="utf-8")))
        if not args.execute:
            base_path = Path(net.raw["configtx_base"])
            if not base_path.exists():
                raise RequestError(f"configtx_base {base_path} does not exist")
            _, changes, base_vals = render_configtx(base_path.read_text(encoding="utf-8"),
                                                    net.relative_base(base_path),
                                                    net.raw["profile"], req)
            print(json.dumps({"request": req.as_record(), "base_batch_values": base_vals,
                              "configtx_changes": changes}, indent=2))
            for s in plan(req, net, args.run_dir.resolve().as_posix()):
                print("PLAN", s.label, " ".join(s.argv))
            print("dry run: nothing executed or written (add --execute once Fabric use is authorized)")
            return 0
        if runner is None and not platform_name.startswith("linux"):
            raise RequestError(f"--execute runs the Linux Fabric CLIs with POSIX paths: run it inside the Ubuntu "
                               f"WSL distro (platform here: {platform_name})")
        record = run(req, net, args.run_dir, runner or subprocess_runner,
                     argv=sys.argv if argv is None else argv, **run_kw)
    except (RequestError, OSError, json.JSONDecodeError, UnicodeDecodeError) as err:
        print(f"{INVALID_REQUEST}: {err}")
        return EXIT[INVALID_REQUEST]
    for c in record["checks"]:
        print(f"{c['check']:<38} requested={c['requested']!s:<14} decoded={c['decoded']!s:<14} "
              f"{'PASS' if c['pass'] else 'FAIL'}")
    print(f"CHANNEL {record['status']}" + (f" at {record['failed_step']}: {record['detail']}" if record['failed_step'] else "")
          + f" -> {args.run_dir / 'channel_run_record.json'}")
    return EXIT[record["status"]]


if __name__ == "__main__":
    sys.exit(main())
