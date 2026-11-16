"""Static guard for the channel batch symbols (owner ruling 2026-11-09; E1 protocol convention):
B = MaxMessageCount (count), tau = BatchTimeout (Go duration), P = PreferredMaxBytes (bytes).
P is never a message count and B is never a byte size. Reads files only."""
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "e1"))

import e1_channel_run as cr  # noqa: E402
import e1_telemetry_common as tc  # noqa: E402

SCANNED = [("scripts/e1", "*.py"), ("scripts/e1/caliper", "*.js"), ("tests/e1", "*.py"), ("tests/e1/js", "*.js"),
           ("Fabric_Live_Testbed/network/chaincode/e1asset", "*.md"), ("Fabric_Live_Testbed/network/chaincode/e1asset", "*.go"),
           ("Fabric_Live_Testbed/network/chaincode/e1asset/core", "*.go"), ("docs/research/e1", "*.md"),
           ("docs/research/e1/proposed_configs", "*")]
FORBIDDEN = [
    (re.compile(r"\bP\b\s*(?:=|:|is|means)\s*[^\n.;]{0,30}(?i:message.?count|max.?message|messages\b)"), "P as a count"),
    (re.compile(r"(?i:message.?count|maxmessagecount|max(?:imum)? message)[^\n]{0,25}\(\s*P\s*\)"), "count labelled (P)"),
    (re.compile(r"(?i:preferred.?max.?bytes)[^\n]{0,25}\(\s*B\s*\)"), "bytes labelled (B)"),
    (re.compile(r"\bB\s*=\s*(?i:preferred.?max.?bytes)"), "B as bytes"),
    (re.compile(r"(?i)preferred.?max.?message.?count"), "nonexistent Fabric field"),
]


def files():
    me = Path(__file__).resolve()
    for d, pat in SCANNED:
        for p in sorted((REPO / d).glob(pat)):
            if p.is_file() and p.resolve() != me:
                yield p


def test_symbol_table_is_the_protocol_convention():
    assert tc.SYMBOLS["B"][0] == "Orderer.BatchSize.MaxMessageCount"
    assert tc.SYMBOLS["tau"][0] == "Orderer.BatchTimeout"
    assert tc.SYMBOLS["P"] == ("Orderer.BatchSize.PreferredMaxBytes", "bytes")
    assert tc.SYMBOLS["A"] == ("Orderer.BatchSize.AbsoluteMaxBytes", "bytes")


def test_channel_request_records_use_the_symbols():
    rec = cr.Request("e1-r001", "r001", 45, "2s", 10_485_760).as_record()
    assert rec["B_max_message_count"] == 45 and rec["P_preferred_max_bytes"] == 10_485_760
    assert rec["tau_batch_timeout"] == "2s"


@pytest.mark.parametrize("bad", ["P = max message count", "maximum message count (P)", "PreferredMaxBytes (B)",
                                 "B = PreferredMaxBytes", "PreferredMaxMessageCount: 10"])
def test_guard_detects_each_wrong_usage(bad):
    assert any(rx.search(bad) for rx, _ in FORBIDDEN)


@pytest.mark.parametrize("good", ["P = PreferredMaxBytes", "B = MaxMessageCount", "MaxMessageCount (B)",
                                  "PreferredMaxBytes (P) 524288 > AbsoluteMaxBytes"])
def test_guard_accepts_the_convention(good):
    assert not any(rx.search(good) for rx, _ in FORBIDDEN)


def test_no_file_reinterprets_the_symbols():
    hits = []
    scanned = 0
    for p in files():
        scanned += 1
        for no, line in enumerate(p.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
            for rx, what in FORBIDDEN:
                if rx.search(line):
                    hits.append(f"{p.relative_to(REPO)}:{no}: {what}: {line.strip()[:100]}")
    assert scanned > 30
    assert not hits, "\n".join(hits)
