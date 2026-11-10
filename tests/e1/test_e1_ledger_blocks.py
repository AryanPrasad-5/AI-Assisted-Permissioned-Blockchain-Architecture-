"""Synthetic protobuf blocks for the ledger reader core. Not Fabric output; not E0 results.

The encoder below writes the same wire format as fabric-protos for the fields the reader uses.
Validation on a real fetched block is still required (E-a / E0-0).
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_ledger_blocks as lb  # noqa: E402


def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def fld(num, value):
    if isinstance(value, int):
        return varint(num << 3) + varint(value)
    return varint((num << 3) | 2) + varint(len(value)) + value


def envelope(tx_id, header_type=3, body=b"x" * 2500, sig=b"s" * 70, seconds=1_700_000_000, nanos=250_000_000):
    ts = fld(1, seconds) + fld(2, nanos)
    chdr = fld(1, header_type) + fld(3, ts) + fld(4, b"e1ch") + fld(5, tx_id.encode())
    payload = fld(1, fld(1, chdr) + fld(2, b"sighdr")) + fld(2, body)
    return fld(1, payload) + fld(2, sig), len(payload) + len(sig)


def block(number, envs, codes):
    header = fld(1, number) + fld(2, b"p" * 32) + fld(3, b"d" * 32)
    data = b"".join(fld(1, e) for e in envs)
    meta = fld(1, b"") + fld(1, b"") + fld(1, bytes(codes)) + fld(1, b"") + fld(1, b"")
    return fld(1, header) + fld(2, data) + fld(3, meta)


def test_parse_block_fields_and_envelope_bytes():
    e1, size1 = envelope("txA")
    e2, size2 = envelope("txB", body=b"y" * 4000)
    parsed = lb.parse_block(block(7, [e1, e2], [0, 11]))
    assert parsed["number"] == 7
    a, b = parsed["txs"]
    assert (a["tx_id"], a["index"], a["header_type"], a["validation_code"], a["env_bytes"]) == ("txA", 0, 3, 0, size1)
    assert (b["tx_id"], b["validation_code"], b["env_bytes"]) == ("txB", 11, size2)
    assert a["channel_id"] == "e1ch" and a["ch_ts_ms"] == 1_700_000_000_250


def test_tables_infer_count_then_timer_for_n205_b10(tmp_path):
    raw = [block(0, [envelope("cfg", header_type=1)[0]], [0])]
    for blk in range(1, 22):
        n = 10 if blk <= 20 else 5
        raw.append(block(blk, [envelope(f"t{blk}_{i}")[0] for i in range(n)], [0] * n))
    for i, r in enumerate(raw):
        (tmp_path / f"e1ch_{i}.block").write_bytes(r)
    out = tmp_path / "out"
    assert lb.main(["--blocks-dir", str(tmp_path), "--out-dir", str(out), "--max-message-count", "10",
                    "--preferred-max-bytes", "524288", "--summary"]) == 0
    rows = (out / "blocks.csv").read_text().splitlines()
    assert rows[1].startswith("0,1,") and "CONFIG" in rows[1]
    assert rows[2].endswith(",count") and rows[-1].endswith(",timer")
    assert len((out / "ledger_tx.csv").read_text().splitlines()) == 1 + 1 + 205
    # Refuses to overwrite existing outputs.
    assert lb.main(["--blocks-dir", str(tmp_path), "--out-dir", str(out), "--max-message-count", "10",
                    "--preferred-max-bytes", "524288"]) == 2


def test_size_summary_and_stop_rule():
    small = [{"header_type": 3, "env_bytes": 3000}] * 1000
    s = lb.size_summary(small)
    assert s["max"] == 3000 and not s["stop_rule_45_x_max_gt_10MiB"] and s["enough_for_E0_1_size_estimate"]
    big = small + [{"header_type": 3, "env_bytes": 233_017}]  # 45 * 233,017 > 10,485,760
    assert lb.size_summary(big)["stop_rule_45_x_max_gt_10MiB"]
    edge = small + [{"header_type": 3, "env_bytes": 233_016}]
    assert not lb.size_summary(edge)["stop_rule_45_x_max_gt_10MiB"]


def test_truncated_or_inconsistent_blocks_are_rejected(tmp_path):
    e1, _ = envelope("txA")
    with pytest.raises(ValueError, match="truncated"):
        lb.parse_block(block(1, [e1], [0])[:-3])
    short_filter = tmp_path / "a.block"
    short_filter.write_bytes(block(2, [e1, e1], [0]))  # TRANSACTIONS_FILTER shorter than the tx list
    with pytest.raises(ValueError, match="TRANSACTIONS_FILTER"):
        lb.read_blocks([short_filter])
    dup1, dup2 = tmp_path / "b.block", tmp_path / "c.block"
    dup1.write_bytes(block(3, [e1], [0]))
    dup2.write_bytes(block(3, [e1], [0]))
    with pytest.raises(ValueError, match="duplicate"):
        lb.read_blocks([dup1, dup2])
