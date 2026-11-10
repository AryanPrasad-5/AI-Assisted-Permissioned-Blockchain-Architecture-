                                                                                                     

                                                                                                
                                           
                                                                                                 
                                                          
                                                                                                     
                                                                                                          
                                                                                                    
                                                                                                  
                                                                                              
                    

     
                                                                                                    
                                                                                                      
                                                                 
                                                                                                   
                                                                      
                                                                                                  
                                                                                      
                                                                                                      
                                                                                           

                              
                                                                                     
                                                                                                 
                                                            
                                                                                               
                                                                                                   
                                  

                                                                                              
                                                                                                   

                               
                                                                                          
                                                                                          
                                                                                          
                                                                                  
                                                                                           
                                                                                     
                                                                                          

     
                                                                                                
                                                                                                 
                                                               
                                                                                
                                                                            
                                                                                              
                                                                              
                                                                                   
   
from __future__ import annotations

import argparse
import calendar
import csv
import datetime
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

TOOL_VERSION = "e1_log_collector 0.1 (2026-11-09)"
ORDERER_MARKER = "Writing block"
PEER_MARKER = "Committed block"

_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_DOCKER_TS = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})(?: |$)")
_FABRIC_HEAD = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3}) (?P<tz>[A-Za-z]{1,5}|[+-]\d{4}) (?P<rest>.*)$")
_REST_NEW = re.compile(r"^(?P<id>[0-9a-f]{3,})\s+(?P<level>[A-Z]{4,5})\s+\[(?P<module>[^\]]*)\]\s+\S+\s+->\s+(?P<msg>.*)$")
_REST_OLD = re.compile(r"^\[(?P<module>[^\]]*)\]\s+\S+\s+->\s+(?P<level>[A-Z]{4,5})\s+(?P<id>[0-9a-f]{3,})\s+(?P<msg>.*)$")
_ORDERER_MSG = re.compile(r"Writing block \[(?P<block>\d+)\] \(Raft index: (?P<raft>\d+)\) to ledger(?P<tail>.*)$")
_PEER_MSG = re.compile(
    r"(?:\[(?P<channel>[^\]]+)\] )?Committed block \[(?P<block>\d+)\] with (?P<ntx>\d+) transaction\(s\) "
    r"in (?P<dur>\d+)ms"
    r"(?: \(state_validation=(?P<sv>\d+)ms block_and_pvtdata_commit=(?P<bc>\d+)ms state_commit=(?P<sc>\d+)ms\))?"
    r"(?: commitHash=\[(?P<hash>[0-9a-fA-F]*)\])?(?P<tail>.*)$")
_FIELD = re.compile(r"(?:^|\s)(channel|node)=(\S+)")
_CONTAINER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")

COMPLETENESS_ERRORS = {"missing_event", "malformed_event_line", "fabric_ts_unusable", "channel_unattributed",
                       "input_empty", "no_in_scope_events"}
CONSISTENCY_ERRORS = {"conflicting_duplicate", "ts_order_violation", "log_order_violation", "commit_before_write",
                      "tx_count_mismatch_ledger", "peer_tx_count_disagreement", "event_in_unexpected_role"}
WARNINGS = {"exact_duplicate", "docker_ts_missing", "ts_docker_before_fabric", "ts_docker_lag", "decode_replaced"}


                                                                               
def _epoch(y: str, mo: str, d: str, h: str, mi: str, s: str) -> int:
                                                                                                            
    fields = (int(y), int(mo), int(d), int(h), int(mi), int(s))
    datetime.datetime(*fields)                         
    return calendar.timegm(fields + (0, 0, 0))


def parse_docker_ts(line: str) -> tuple[str, int | None, str]:
                                                                                               
    m = _DOCKER_TS.match(line)
    if not m:
        return "", None, line
    y, mo, d, h, mi, s, frac, tz = m.groups()
    try:
        secs = _epoch(y, mo, d, h, mi, s)
    except (ValueError, OverflowError):
        return "", None, line
    if tz != "Z":
        sign = 1 if tz[0] == "+" else -1
        secs -= sign * (int(tz[1:3]) * 3600 + int(tz[4:6]) * 60)
    ns = secs * 1_000_000_000 + int((frac or "0").ljust(9, "0"))
    text = line[:m.end()].rstrip(" ")
    return text, ns, line[m.end():]


@dataclass
class Header:
    ts_text: str
    ms: int | None
    tz: str
    level: str
    module: str
    msg: str


def parse_fabric_header(text: str) -> Header | None:
                                                                                          
    m = _FABRIC_HEAD.match(text)
    if not m:
        return None
    rest = m.group("rest")
    r = _REST_NEW.match(rest) or _REST_OLD.match(rest)
    if not r:
        return None
    y, mo, d, h, mi, s, ms = m.groups()[:7]
    tz = m.group("tz")
    try:
        secs = _epoch(y, mo, d, h, mi, s)
    except (ValueError, OverflowError):
        return None
    epoch_ms: int | None
    if tz in ("UTC", "GMT", "Z"):
        epoch_ms = secs * 1000 + int(ms)
    elif tz[0] in "+-" and tz[1:].isdigit():
        sign = 1 if tz[0] == "+" else -1
        epoch_ms = (secs - sign * (int(tz[1:3]) * 3600 + int(tz[3:5]) * 60)) * 1000 + int(ms)
    else:
        epoch_ms = None                                                               
    ts_text = text[:m.start("rest")].rstrip(" ")
    return Header(ts_text, epoch_ms, tz, r.group("level"), r.group("module"), r.group("msg"))


                                                                            
@dataclass
class Event:
    kind: str                                             
    container: str
    container_role: str                                      
    source_file: str
    stream: str
    line_no: int
    raw_line: str
    channel: str
    block: int
    fabric_ts: str = ""
    fabric_ms: int | None = None
    docker_ts: str = ""
    docker_ns: int | None = None
    level: str = ""
    module: str = ""
    raft_index: int | None = None
    node: str = ""
    tx_count: int | None = None
    commit_duration_ms: int | None = None
    state_validation_ms: int | None = None
    block_and_pvtdata_commit_ms: int | None = None
    state_commit_ms: int | None = None
    commit_hash: str = ""
    in_scope: bool = False
    canonical: bool = True
    dup_kind: str = ""
    dup_of: str = ""
    flags: list[str] = field(default_factory=list)

    @property
    def ref(self) -> str:
        return f"{self.source_file}:{self.line_no}"

    @property
    def content(self) -> tuple:
                                                                                       
        return (self.fabric_ms, self.raft_index, self.node, self.tx_count, self.commit_duration_ms,
                self.state_validation_ms, self.block_and_pvtdata_commit_ms, self.state_commit_ms, self.commit_hash)


@dataclass
class Anomaly:
    severity: str
    kind: str
    container: str = ""
    channel: str = ""
    block: int | str = ""
    source_file: str = ""
    line_no: int | str = ""
    detail: str = ""
    raw_line: str = ""


@dataclass
class LogInput:
    container: str
    role: str                        
    path: Path

    @property
    def stream(self) -> str:
        name = self.path.name
        return "stderr" if name.endswith(".stderr.log") else "stdout" if name.endswith(".stdout.log") else "unknown"


def _sev(kind: str) -> str:
    return "warning" if kind in WARNINGS else "error"


def _anom(out: list[Anomaly], kind: str, **kw) -> None:
    out.append(Anomaly(_sev(kind), kind, **kw))


                                                                            
def parse_line(text: str, inp: LogInput, line_no: int, raw: str) -> tuple[Event | None, list[tuple[str, str]]]:
                                                                                                     
    problems: list[tuple[str, str]] = []
    clean = _ANSI.sub("", text)
    docker_text, docker_ns, rest = parse_docker_ts(clean)
    if docker_ns is None:
        problems.append(("docker_ts_missing", "no docker --timestamps prefix"))
    header = parse_fabric_header(rest)
    msg = header.msg if header else rest
    is_orderer = ORDERER_MARKER in msg
    m = _ORDERER_MSG.search(msg) if is_orderer else _PEER_MSG.search(msg)
    if not m:
        return None, [("malformed_event_line", "event marker present but the message does not parse")]
    fields_text = m.group("tail") or ""
    fields = dict(_FIELD.findall(fields_text))
    ev = Event(kind="orderer_write" if is_orderer else "peer_commit", container=inp.container,
               container_role=inp.role, source_file=str(inp.path), stream=inp.stream, line_no=line_no,
               raw_line=raw, channel="", block=int(m.group("block")), docker_ts=docker_text, docker_ns=docker_ns)
    if is_orderer:
        ev.raft_index = int(m.group("raft"))
        ev.node = fields.get("node", "")
        ev.channel = fields.get("channel", "")
    else:
        ev.channel = m.group("channel") or fields.get("channel", "")
        ev.tx_count = int(m.group("ntx"))
        ev.commit_duration_ms = int(m.group("dur"))
        for attr, grp in (("state_validation_ms", "sv"), ("block_and_pvtdata_commit_ms", "bc"),
                          ("state_commit_ms", "sc")):
            if m.group(grp) is not None:
                setattr(ev, attr, int(m.group(grp)))
        ev.commit_hash = m.group("hash") or ""
    if header is None:
        problems.append(("fabric_ts_unusable", "Fabric log header does not parse"))
    else:
        ev.fabric_ts, ev.fabric_ms, ev.level, ev.module = header.ts_text, header.ms, header.level, header.module
        if header.ms is None:
            problems.append(("fabric_ts_unusable", f"time zone {header.tz!r} not converted (UTC or +hhmm only)"))
    if not ev.channel:
        problems.append(("channel_unattributed", "no channel in the message or fields"))
    expected_role = "orderer" if is_orderer else "peer"
    if inp.role != expected_role:
        problems.append(("event_in_unexpected_role", f"{ev.kind} event in a {inp.role} log"))
    return ev, problems


def read_lines(path: Path) -> tuple[list[tuple[str, str, bool]], dict]:
                                                                                       
    data = path.read_bytes()
    parts = data.split(b"\n")
    if parts and parts[-1] == b"":
        parts.pop()
    lines = []
    for b in parts:
        if b.endswith(b"\r"):
            b = b[:-1]
        text = b.decode("utf-8", errors="replace")
        replaced = "�" in text and "�".encode() not in b
        lines.append((text, text, replaced))
    facts = {"path": str(path), "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    return lines, facts


def parse_inputs(inputs: list[LogInput], anomalies: list[Anomaly]) -> tuple[list[Event], list[dict]]:
    events: list[Event] = []
    accounting = []
    for inp in inputs:
        lines, facts = read_lines(inp.path)
        acc = dict(container=inp.container, role=inp.role, stream=inp.stream, **facts, lines_total=len(lines),
                   lines_blank=0, lines_event=0, lines_malformed_event=0, lines_other=0,
                   lines_without_docker_ts=0, lines_with_decode_replacement=0)
        if not lines:
            _anom(anomalies, "input_empty", container=inp.container, source_file=str(inp.path),
                  detail="log file has no lines")
        for i, (text, raw, replaced) in enumerate(lines, start=1):
            acc["lines_with_decode_replacement"] += replaced
            if not text.strip():
                acc["lines_blank"] += 1
                continue
            if parse_docker_ts(_ANSI.sub("", text))[1] is None:
                acc["lines_without_docker_ts"] += 1
            if ORDERER_MARKER not in text and PEER_MARKER not in text:
                acc["lines_other"] += 1
                continue
            ev, problems = parse_line(text, inp, i, raw)
            if replaced:
                problems.append(("decode_replaced", "invalid UTF-8 replaced with U+FFFD"))
            if ev is None:
                acc["lines_malformed_event"] += 1
            else:
                acc["lines_event"] += 1
                events.append(ev)
            for kind, detail in problems:
                if ev is not None:
                    ev.flags.append(kind)
                _anom(anomalies, kind, container=inp.container, channel=ev.channel if ev else "",
                      block=ev.block if ev else "", source_file=str(inp.path), line_no=i, detail=detail, raw_line=raw)
        if acc["lines_total"] != (acc["lines_blank"] + acc["lines_event"] + acc["lines_malformed_event"]
                                  + acc["lines_other"]):
            raise RuntimeError(f"line accounting broken for {inp.path}")
        accounting.append(acc)
    return events, accounting


                                                                             
def mark_duplicates(events: list[Event], anomalies: list[Anomaly]) -> None:
    first: dict[tuple, Event] = {}
    for ev in events:
        key = (ev.kind, ev.container, ev.channel, ev.block)
        prior = first.get(key)
        if prior is None:
            first[key] = ev
            continue
        ev.canonical = False
        ev.dup_of = prior.ref
        same_text = _ANSI.sub("", ev.raw_line) == _ANSI.sub("", prior.raw_line)
        ev.dup_kind = "exact" if same_text or (ev.content == prior.content and ev.docker_ns == prior.docker_ns) \
            else "conflicting"
        kind = "exact_duplicate" if ev.dup_kind == "exact" else "conflicting_duplicate"
        ev.flags.append(kind)
        _anom(anomalies, kind, container=ev.container, channel=ev.channel, block=ev.block, source_file=ev.source_file,
              line_no=ev.line_no, detail=f"duplicate of {prior.ref}", raw_line=ev.raw_line)


def expected_blocks(events: list[Event], blocks_csv: list[dict] | None, expect: tuple[int, int] | None
                    ) -> tuple[list[int], str, dict[int, int]]:
    ledger_tx: dict[int, int] = {}
    if blocks_csv is not None:
        for row in blocks_csv:
            ledger_tx[int(row["number"])] = int(row["tx_count"])
        return sorted(ledger_tx), "blocks_csv", ledger_tx
    if expect is not None:
        return list(range(expect[0], expect[1] + 1)), "expect_blocks", ledger_tx
    seen = sorted({e.block for e in events if e.in_scope})
    return (list(range(seen[0], seen[-1] + 1)) if seen else []), "observed_range", ledger_tx


def analyse(events: list[Event], inputs: list[LogInput], channel: str, blocks: list[int], ledger_tx: dict[int, int],
            anomalies: list[Anomaly], offset_bound_ms: float, ts_lag_ms: float) -> list[dict]:
    canon = [e for e in events if e.canonical and e.in_scope]
    containers = {"orderer_write": sorted({i.container for i in inputs if i.role == "orderer"}),
                  "peer_commit": sorted({i.container for i in inputs if i.role == "peer"})}
    by_key = {(e.kind, e.container, e.block): e for e in canon}

                    
    for kind, names in containers.items():
        for name in names:
            for b in blocks:
                if (kind, name, b) not in by_key:
                    _anom(anomalies, "missing_event", container=name, channel=channel, block=b,
                          detail=f"no {kind} event for block {b}")

                                                                                   
    for kind, names in containers.items():
        for name in names:
            evs = sorted((e for e in canon if e.kind == kind and e.container == name), key=lambda e: e.block)
            for a, b in zip(evs, evs[1:]):
                if a.fabric_ms is not None and b.fabric_ms is not None and b.fabric_ms < a.fabric_ms:
                    _anom(anomalies, "ts_order_violation", container=name, channel=channel, block=b.block,
                          source_file=b.source_file, line_no=b.line_no,
                          detail=f"block {b.block} at {b.fabric_ms} ms is earlier than block {a.block} at {a.fabric_ms} ms",
                          raw_line=b.raw_line)
            by_file: dict[str, list[Event]] = {}
            for e in evs:
                by_file.setdefault(e.source_file, []).append(e)
            for f_evs in by_file.values():
                f_evs.sort(key=lambda e: e.line_no)
                for a, b in zip(f_evs, f_evs[1:]):
                    if b.block < a.block:
                        _anom(anomalies, "log_order_violation", container=name, channel=channel, block=b.block,
                              source_file=b.source_file, line_no=b.line_no,
                              detail=f"block {b.block} logged after block {a.block} (line {a.line_no})",
                              raw_line=b.raw_line)

                                                               
    for e in canon:
        if e.docker_ns is None or e.fabric_ms is None:
            continue
        delta_ms = (e.docker_ns - e.fabric_ms * 1_000_000) / 1e6
        if delta_ms < -1.0:
            kind = "ts_docker_before_fabric"
        elif delta_ms > ts_lag_ms:
            kind = "ts_docker_lag"
        else:
            continue
        e.flags.append(kind)
        _anom(anomalies, kind, container=e.container, channel=channel, block=e.block, source_file=e.source_file,
              line_no=e.line_no, detail=f"docker - fabric = {delta_ms:.3f} ms", raw_line=e.raw_line)

                                    
    rows = []
    expected_set = set(blocks)
    all_blocks = sorted(expected_set | {e.block for e in canon})
    for b in all_blocks:
        ow = [by_key[("orderer_write", n, b)] for n in containers["orderer_write"] if ("orderer_write", n, b) in by_key]
        pc = [by_key[("peer_commit", n, b)] for n in containers["peer_commit"] if ("peer_commit", n, b) in by_key]
        ow_ms = [e.fabric_ms for e in ow if e.fabric_ms is not None]
        flags = []
        first_write = min(ow_ms) if ow_ms else None
        for p in pc:
            if first_write is not None and p.fabric_ms is not None and p.fabric_ms < first_write - offset_bound_ms:
                flags.append("commit_before_write")
                _anom(anomalies, "commit_before_write", container=p.container, channel=channel, block=b,
                      source_file=p.source_file, line_no=p.line_no,
                      detail=f"peer commit {p.fabric_ms} ms < first orderer write {first_write} ms - {offset_bound_ms} ms",
                      raw_line=p.raw_line)
        counts = {p.tx_count for p in pc}
        if len(counts) > 1:
            flags.append("peer_tx_count_disagreement")
            _anom(anomalies, "peer_tx_count_disagreement", channel=channel, block=b,
                  detail="; ".join(f"{p.container}={p.tx_count}" for p in pc))
        if b in ledger_tx:
            for p in pc:
                if p.tx_count != ledger_tx[b]:
                    flags.append("tx_count_mismatch_ledger")
                    _anom(anomalies, "tx_count_mismatch_ledger", container=p.container, channel=channel, block=b,
                          source_file=p.source_file, line_no=p.line_no,
                          detail=f"log {p.tx_count} tx, ledger {ledger_tx[b]} tx", raw_line=p.raw_line)
        rows.append({
            "block": b, "expected": b in expected_set,
            "orderers_expected": len(containers["orderer_write"]), "orderers_logged": len(ow),
            "orderers_missing": ";".join(n for n in containers["orderer_write"] if ("orderer_write", n, b) not in by_key),
            "orderer_first_write_ms": first_write if first_write is not None else "",
            "orderer_last_write_ms": max(ow_ms) if ow_ms else "",
            "peers_expected": len(containers["peer_commit"]), "peers_logged": len(pc),
            "peers_missing": ";".join(n for n in containers["peer_commit"] if ("peer_commit", n, b) not in by_key),
            "peer_commit_ms": ";".join(f"{p.container}={p.fabric_ms if p.fabric_ms is not None else ''}" for p in pc),
            "peer_tx_counts": ";".join(f"{p.container}={p.tx_count}" for p in pc),
            "ledger_tx_count": ledger_tx.get(b, ""),
            "flags": ";".join(sorted(set(flags))),
        })
    return rows


def status_of(anomalies: list[Anomaly]) -> str:
    kinds = {a.kind for a in anomalies}
    if kinds & COMPLETENESS_ERRORS:
        return "INCOMPLETE"
    if kinds & CONSISTENCY_ERRORS:
        return "INCONSISTENT"
    if kinds:
        return "COMPLETE_WITH_WARNINGS"
    return "COMPLETE"


                                                                           
ORDERER_COLS = ["container", "container_role", "channel", "in_scope", "block", "raft_index", "node", "fabric_ts",
                "fabric_ms", "docker_ts", "docker_ns", "level", "module", "source_file", "stream", "line_no",
                "canonical", "dup_kind", "dup_of", "flags", "raw_line"]
PEER_COLS = ["container", "container_role", "channel", "in_scope", "block", "tx_count", "commit_duration_ms",
             "state_validation_ms", "block_and_pvtdata_commit_ms", "state_commit_ms", "commit_hash", "fabric_ts",
             "fabric_ms", "docker_ts", "docker_ns", "level", "module", "source_file", "stream", "line_no",
             "canonical", "dup_kind", "dup_of", "flags", "raw_line"]
ANOMALY_COLS = ["severity", "kind", "container", "channel", "block", "source_file", "line_no", "detail", "raw_line"]
SUMMARY_COLS = ["block", "expected", "orderers_expected", "orderers_logged", "orderers_missing",
                "orderer_first_write_ms", "orderer_last_write_ms", "peers_expected", "peers_logged", "peers_missing",
                "peer_commit_ms", "peer_tx_counts", "ledger_tx_count", "flags"]


def _cell(v):
    if v is None:
        return ""
    if isinstance(v, list):
        return ";".join(v)
    return v


def _write_csv(path: Path, cols: list[str], rows: list[dict]) -> str:
    with path.open("x", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({c: _cell(r.get(c)) for c in cols})
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_parse(inputs: list[LogInput], channel: str, out_dir: Path, *, blocks_csv: list[dict] | None = None,
              expect: tuple[int, int] | None = None, reconcile_peer: str | None = None,
              offset_bound_ms: float = 1.0, ts_lag_ms: float = 1000.0, argv: list[str] | None = None) -> dict:
                                                                                                           
    anomalies: list[Anomaly] = []
    events, accounting = parse_inputs(inputs, anomalies)
    for e in events:
        e.in_scope = e.channel == channel
    mark_duplicates(events, anomalies)
    blocks, source, ledger_tx = expected_blocks(events, blocks_csv, expect)
    if not any(e.in_scope for e in events):
        _anom(anomalies, "no_in_scope_events", channel=channel, detail=f"no event for channel {channel!r}")
    summary = analyse(events, inputs, channel, blocks, ledger_tx, anomalies, offset_bound_ms, ts_lag_ms)
    status = status_of(anomalies)

    out_dir.mkdir(parents=True, exist_ok=False)
    outputs = {
        "orderer_write.csv": _write_csv(out_dir / "orderer_write.csv", ORDERER_COLS,
                                        [e.__dict__ for e in events if e.kind == "orderer_write"]),
        "peer_commit.csv": _write_csv(out_dir / "peer_commit.csv", PEER_COLS,
                                      [e.__dict__ for e in events if e.kind == "peer_commit"]),
        "log_anomalies.csv": _write_csv(out_dir / "log_anomalies.csv", ANOMALY_COLS, [a.__dict__ for a in anomalies]),
        "block_summary.csv": _write_csv(out_dir / "block_summary.csv", SUMMARY_COLS, summary),
    }
    if reconcile_peer is not None:
        rows = sorted(({"block": e.block, "commit_ms": e.fabric_ms} for e in events
                       if e.kind == "peer_commit" and e.container == reconcile_peer and e.canonical and e.in_scope
                       and e.fabric_ms is not None), key=lambda r: r["block"])
        outputs["peer_commit_reconcile.csv"] = _write_csv(out_dir / "peer_commit_reconcile.csv",
                                                          ["block", "commit_ms"], rows)
    by_kind: dict[str, dict[str, int]] = {"error": {}, "warning": {}}
    for a in anomalies:
        by_kind[a.severity][a.kind] = by_kind[a.severity].get(a.kind, 0) + 1
    per_channel: dict[str, dict[str, int]] = {}
    for e in events:
        c = per_channel.setdefault(e.channel or "<unattributed>", {"orderer_write": 0, "peer_commit": 0})
        c[e.kind] += 1
    manifest = {
        "tool": TOOL_VERSION,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "argv": argv,
        "channel": channel,
        "status": status,
        "inputs": accounting,
        "expected_blocks": {"source": source, "first": blocks[0] if blocks else None,
                            "last": blocks[-1] if blocks else None, "count": len(blocks)},
        "events": {"orderer_write": sum(e.kind == "orderer_write" for e in events),
                   "peer_commit": sum(e.kind == "peer_commit" for e in events),
                   "in_scope_canonical": sum(e.in_scope and e.canonical for e in events),
                   "duplicates": sum(not e.canonical for e in events), "per_channel": per_channel},
        "anomalies": by_kind,
        "reconcile_peer": reconcile_peer,
        "parameters": {"offset_bound_ms": offset_bound_ms, "ts_lag_ms": ts_lag_ms},
        "timestamp_note": "fabric_ms = Fabric header (container clock, ms, truncated); docker_ns = daemon receive time",
        "outputs_sha256": outputs,
    }
    with (out_dir / "log_collector_manifest.json").open("x", encoding="utf-8", newline="\n") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    return manifest


                                                                            
Runner = Callable[[list[str]], "tuple[int, bytes, bytes]"]


def subprocess_runner(argv: list[str]) -> tuple[int, bytes, bytes]:
    proc = subprocess.run(argv, capture_output=True, timeout=300, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def capture_commands(containers: list[str], since: str, until: str) -> list[list[str]]:
    for c in containers:
        if not _CONTAINER.match(c):
            raise ValueError(f"invalid container name {c!r}")
    if len(set(containers)) != len(containers):
        raise ValueError("duplicate container names")
    t0, t1 = parse_docker_ts(since)[1], parse_docker_ts(until)[1]
    if t0 is None or t1 is None or parse_docker_ts(since)[2] or parse_docker_ts(until)[2]:
        raise ValueError("--since/--until must be RFC 3339 timestamps, e.g. 2026-11-11T10:00:00Z")
    if t1 <= t0:
        raise ValueError("--until must be later than --since")
    return [["docker", "logs", "--timestamps", "--since", since, "--until", until, c] for c in containers]


def run_capture(containers: list[str], since: str, until: str, out_dir: Path, runner: Runner) -> dict:
                                                                                                 
    cmds = capture_commands(containers, since, until)
    out_dir.mkdir(parents=True, exist_ok=False)
    records = []
    for c, argv in zip(containers, cmds):
        try:
            code, out, err = runner(argv)
        except (OSError, subprocess.SubprocessError) as exc:
            records.append({"container": c, "argv": argv, "exit_code": None, "error": repr(exc)})
            continue
        rec = {"container": c, "argv": argv, "exit_code": code}
        for stream, data in (("stdout", out), ("stderr", err)):
            p = out_dir / f"{c}.{stream}.log"
            with p.open("xb") as fh:
                fh.write(data)
            rec[stream] = {"file": p.name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                           "lines": data.count(b"\n") + (1 if data and not data.endswith(b"\n") else 0)}
        records.append(rec)
    ok = all(r.get("exit_code") == 0 for r in records)
    manifest = {"tool": TOOL_VERSION, "since": since, "until": until, "status": "OK" if ok else "FAILED",
                "captures": records}
    with (out_dir / "capture_manifest.json").open("x", encoding="utf-8", newline="\n") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    return manifest


                                                                        
def _named(values: list[str], role: str) -> list[LogInput]:
    out = []
    for v in values or []:
        name, sep, path = v.partition("=")
        if not sep or not name or not path or not _CONTAINER.match(name):
            raise ValueError(f"--{role}-log wants NAME=PATH, got {v!r}")
        out.append(LogInput(name, role, Path(path)))
    return out


def _range(text: str) -> tuple[int, int]:
    a, sep, b = text.partition(":")
    if not sep or not a.isdigit() or not b.isdigit() or int(b) < int(a):
        raise ValueError(f"--expect-blocks wants FIRST:LAST with FIRST <= LAST, got {text!r}")
    return int(a), int(b)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    cap = sub.add_parser("capture")
    cap.add_argument("--container", action="append", required=True)
    cap.add_argument("--since", required=True)
    cap.add_argument("--until", required=True)
    cap.add_argument("--out-dir", required=True, type=Path)
    cap.add_argument("--execute", action="store_true", help="run docker logs (needs Docker authorization)")
    par = sub.add_parser("parse")
    par.add_argument("--channel", required=True)
    par.add_argument("--orderer-log", action="append", default=[])
    par.add_argument("--peer-log", action="append", default=[])
    par.add_argument("--out-dir", required=True, type=Path)
    grp = par.add_mutually_exclusive_group()
    grp.add_argument("--blocks-csv", type=Path)
    grp.add_argument("--expect-blocks")
    par.add_argument("--reconcile-peer")
    par.add_argument("--offset-bound-ms", type=float, default=1.0)
    par.add_argument("--ts-lag-ms", type=float, default=1000.0)
    args = ap.parse_args(argv)

    if args.cmd == "capture":
        try:
            cmds = capture_commands(args.container, args.since, args.until)
        except ValueError as err:
            print(f"INVALID REQUEST: {err}")
            return 2
        if not args.execute:
            for c in cmds:
                print("PLAN " + " ".join(c))
            print("dry run: nothing executed (add --execute once Docker use is authorized)")
            return 0
        try:
            m = run_capture(args.container, args.since, args.until, args.out_dir, subprocess_runner)
        except FileExistsError:
            print(f"refusing to overwrite {args.out_dir}")
            return 2
        print(f"CAPTURE {m['status']} -> {args.out_dir}")
        return 0 if m["status"] == "OK" else 1

    try:
        inputs = _named(args.orderer_log, "orderer") + _named(args.peer_log, "peer")
        if not inputs:
            raise ValueError("give at least one --orderer-log or --peer-log")
        expect = _range(args.expect_blocks) if args.expect_blocks else None
        blocks_csv = None
        if args.blocks_csv:
            with args.blocks_csv.open(newline="", encoding="utf-8") as fh:
                blocks_csv = list(csv.DictReader(fh))
        if args.reconcile_peer and args.reconcile_peer not in {i.container for i in inputs if i.role == "peer"}:
            raise ValueError(f"--reconcile-peer {args.reconcile_peer!r} is not a --peer-log container")
        if args.out_dir.exists():
            print(f"refusing to overwrite {args.out_dir}")
            return 2
        manifest = run_parse(inputs, args.channel, args.out_dir, blocks_csv=blocks_csv, expect=expect,
                             reconcile_peer=args.reconcile_peer, offset_bound_ms=args.offset_bound_ms,
                             ts_lag_ms=args.ts_lag_ms, argv=sys.argv if argv is None else argv)
    except (OSError, ValueError, KeyError) as err:
        print(f"UNREADABLE INPUT: {err!r}")
        return 2
    ev = manifest["events"]
    print(f"orderer_write={ev['orderer_write']} peer_commit={ev['peer_commit']} "
          f"in_scope_canonical={ev['in_scope_canonical']} duplicates={ev['duplicates']}")
    for sev in ("error", "warning"):
        for k, n in sorted(manifest["anomalies"][sev].items()):
            print(f"  {sev:<7} {k:<28} {n}")
    print(f"LOGS {manifest['status']} -> {args.out_dir}")
    return 0 if manifest["status"] in ("COMPLETE", "COMPLETE_WITH_WARNINGS") else 1


if __name__ == "__main__":
    sys.exit(main())
