"""Block fetch: synthetic hash-chained blocks and a fake peer CLI. Not Fabric output; not E0 results."""
import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_block_fetch as bf  # noqa: E402
import e1_channel_run as cr  # noqa: E402
import e1_log_collector as lc  # noqa: E402
import e1_reconcile as rc  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

B, P = 10, 524_288


def ids(n):
    return [f"tx{i:04d}" for i in range(n)]


def blocks_for(n=205, b=B):
    return sf.chain(sf.ledger_groups(ids(n), b))


def fetch(tmp_path, peer, first=0, last=None, attempts=3, name="out"):
    net = cr.Network(sf.network(tmp_path))
    return bf.run_fetch(net, sf.CHANNEL, first, last, tmp_path / name, B, P, peer, attempts=attempts,
                        sleep=lambda s: None, wall_ns=sf.Clock().wall_ns)


def kinds(m):
    return {**m["anomalies"]["error"], **m["anomalies"]["warning"]}


def rows(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# ------------------------------------------------------------------ hashing primitives
def test_der_encoding_vectors():
    assert bf._der_uint(0) == b"\x02\x01\x00"
    assert bf._der_uint(127) == b"\x02\x01\x7f"
    assert bf._der_uint(128) == b"\x02\x02\x00\x80"
    assert bf._der_uint(256) == b"\x02\x02\x01\x00"
    assert bf._der_octets(b"") == b"\x04\x00"
    assert bf._der_len(127) == b"\x7f" and bf._der_len(128) == b"\x81\x80" and bf._der_len(300) == b"\x82\x01\x2c"


def test_header_hash_depends_on_every_field():
    h = bf.header_hash(5, b"p" * 32, b"d" * 32)
    assert len(h) == 32 and h == bf.header_hash(5, b"p" * 32, b"d" * 32)
    assert len({h, bf.header_hash(6, b"p" * 32, b"d" * 32), bf.header_hash(5, b"q" * 32, b"d" * 32),
                bf.header_hash(5, b"p" * 32, b"e" * 32)}) == 4


# ------------------------------------------------------------------ fetch: positive
def test_complete_range_writes_reconciler_tables(tmp_path):
    blocks = blocks_for()
    peer = sf.FakePeer(blocks)
    m = fetch(tmp_path, peer)
    assert m["status"] == "COMPLETE" and m["height"] == 22 and m["counts"]["ok"] == 22
    assert m["ledger_tables"] == {"written": True, "transactions": 206, "blocks": 22}
    recs = rows(tmp_path / "out" / "block_fetch_records.csv")
    assert all(r["status"] == "ok" and r["data_hash_ok"] == "true" and r["chain_ok"] == "true" for r in recs)
    causes = [r["inferred_cut_cause"] for r in rows(tmp_path / "out" / "ledger" / "blocks.csv")]
    assert causes[1:] == ["count"] * 20 + ["timer"]
    assert m["request"]["B_max_message_count"] == B and m["request"]["P_preferred_max_bytes"] == P
    # every command is recorded with hashes; every attempt has its own file (nothing rewritten)
    assert len(m["steps"]) == 23 and all(s["exit_code"] == 0 for s in m["steps"])
    assert len(list((tmp_path / "out" / "blocks").glob("*.block"))) == 22
    assert (tmp_path / "out" / "ledger" / "ledger_tx.csv").read_bytes().count(b"\r") == 0


def test_retry_success_is_a_warning(tmp_path):
    m = fetch(tmp_path, sf.FakePeer(blocks_for(), fail={3: 2}))
    assert m["status"] == "COMPLETE_WITH_WARNINGS" and kinds(m) == {"retry_succeeded": 1}
    assert len(list((tmp_path / "out" / "blocks").glob("e1-r001_000003_a*.block"))) == 3  # failed attempts kept


def test_partial_range_from_first_flags_unverified_anchor(tmp_path):
    m = fetch(tmp_path, sf.FakePeer(blocks_for()), first=5, last=9)
    assert m["status"] == "COMPLETE_WITH_WARNINGS" and kinds(m) == {"chain_anchor_unverified": 1}


# ------------------------------------------------------------------ fetch: negative
def test_missing_block_blocks_the_ledger_tables(tmp_path):
    m = fetch(tmp_path, sf.FakePeer(blocks_for(), fail={7: -1}))
    assert m["status"] == "INCOMPLETE" and kinds(m)["fetch_failed"] == 1
    assert m["ledger_tables"]["written"] is False and not (tmp_path / "out" / "ledger").exists()
    rec = [r for r in rows(tmp_path / "out" / "block_fetch_records.csv") if r["number"] == "7"][0]
    assert rec["status"] == "missing"
    assert (tmp_path / "out" / "blocks" / "e1-r001_000007_a3.block").exists()  # partial file kept


@pytest.mark.parametrize("served,kind", [(b"\xff\xff\xff", "block_unparseable"), (b"", "fetch_failed")])
def test_malformed_or_empty_block_is_incomplete(tmp_path, served, kind):
    # exit 0 with an empty file counts as a failed attempt; an unparseable file reaches the decoder
    m = fetch(tmp_path, sf.FakePeer(blocks_for(), serve={4: served}), attempts=1)
    assert m["status"] == "INCOMPLETE" and kinds(m) == {kind: 1}
    assert m["ledger_tables"]["written"] is False


def test_tampered_envelope_breaks_the_data_hash(tmp_path):
    blocks = blocks_for()
    i = blocks[6].index(b"tx0055")
    tampered = blocks[6][:i] + b"tx0099" + blocks[6][i + 6:]
    m = fetch(tmp_path, sf.FakePeer(blocks, serve={6: tampered}))
    assert m["status"] == "INCONSISTENT" and kinds(m) == {"data_hash_mismatch": 1}
    assert m["ledger_tables"]["written"] is False


def test_block_from_another_chain_breaks_the_chain(tmp_path):
    other = sf.chain(sf.ledger_groups([f"zz{i:04d}" for i in range(205)], B))
    m = fetch(tmp_path, sf.FakePeer(blocks_for(), serve={9: other[9]}))
    assert m["status"] == "INCONSISTENT"
    # block 9 itself is self-consistent but does not link to 8, and 10 no longer links to 9
    assert kinds(m)["chain_break"] == 2


def test_wrong_block_number_and_tip_mismatch(tmp_path):
    blocks = blocks_for()
    m = fetch(tmp_path, sf.FakePeer(blocks, serve={12: blocks[13]}))
    assert m["status"] == "INCONSISTENT" and kinds(m)["number_mismatch"] == 1
    m2 = fetch(tmp_path, sf.FakePeer(blocks, tip_b64="AAAA"), name="out2")
    assert m2["status"] == "INCONSISTENT" and kinds(m2) == {"tip_hash_mismatch": 1}


def test_getinfo_failure_and_range_beyond_height(tmp_path):
    m = fetch(tmp_path, sf.FakePeer(blocks_for(), getinfo_ok=False), last=3)
    assert m["status"] == "INCOMPLETE" and "height_unknown" in kinds(m)
    m2 = fetch(tmp_path, sf.FakePeer(blocks_for()), last=30, name="out2")
    assert m2["status"] == "INCOMPLETE" and "range_beyond_height" in kinds(m2) and "fetch_failed" in kinds(m2)


def test_foreign_channel_envelope_is_inconsistent(tmp_path):
    groups = sf.ledger_groups(ids(20), B)
    groups[2][3]["channel"] = "other-channel"
    m = fetch(tmp_path, sf.FakePeer(sf.chain(groups)))
    assert m["status"] == "INCONSISTENT" and kinds(m) == {"channel_mismatch": 1}


def test_duplicate_tx_id_in_ledger_is_kept_and_flagged(tmp_path):
    groups = sf.ledger_groups(ids(20), B)
    groups[2][0] = {"tx_id": "tx0003", "code": 9}
    m = fetch(tmp_path, sf.FakePeer(sf.chain(groups)))
    assert m["status"] == "COMPLETE_WITH_WARNINGS" and kinds(m) == {"duplicate_tx_id": 1}
    ledger = rows(tmp_path / "out" / "ledger" / "ledger_tx.csv")
    assert [r["validation_code"] for r in ledger if r["tx_id"] == "tx0003"] == ["0", "9"]


# ------------------------------------------------------------------ verify mode
def write_blocks(d: Path, blocks, names=None):
    d.mkdir()
    for i, b in enumerate(blocks):
        (d / (names[i] if names else f"b_{i:06d}.block")).write_bytes(b)


def test_verify_mode_duplicates_gaps_and_strays(tmp_path):
    blocks = blocks_for(50)
    write_blocks(tmp_path / "ok", blocks)
    assert bf.run_verify(tmp_path / "ok", sf.CHANNEL, 0, 5, tmp_path / "v1", B, P)["status"] == "COMPLETE"
    d = tmp_path / "dup"
    write_blocks(d, blocks)
    (d / "copy_000002.block").write_bytes(blocks[2])
    m = bf.run_verify(d, sf.CHANNEL, 0, 5, tmp_path / "v2", B, P)
    assert m["status"] == "COMPLETE_WITH_WARNINGS" and kinds(m) == {"exact_duplicate": 1}
    other = sf.chain(sf.ledger_groups([f"zz{i:04d}" for i in range(50)], B))
    (d / "zz_000003.block").write_bytes(other[3])
    m = bf.run_verify(d, sf.CHANNEL, 0, 5, tmp_path / "v3", B, P)
    assert m["status"] == "INCONSISTENT" and kinds(m)["conflicting_duplicate"] == 1
    g = tmp_path / "gap"
    write_blocks(g, [b for i, b in enumerate(blocks) if i != 3])
    m = bf.run_verify(g, sf.CHANNEL, 0, 5, tmp_path / "v4", B, P)
    assert m["status"] == "INCOMPLETE" and kinds(m)["range_gap"] == 1
    e = tmp_path / "empty"
    write_blocks(e, blocks[:6])
    (e / "b_000004.block").write_bytes(b"")
    m = bf.run_verify(e, sf.CHANNEL, 0, 5, tmp_path / "v5", B, P)
    assert m["status"] == "INCOMPLETE" and kinds(m)["block_file_empty"] == 1
    (e / "stray_000009.block").write_bytes(blocks_for(205)[9])
    m = bf.run_verify(e, sf.CHANNEL, 0, 5, tmp_path / "v6", B, P)
    assert kinds(m)["unexpected_file"] == 1


# ------------------------------------------------------------------ CLI guards
def test_cli_refuses_existing_out_dir_and_dry_run_writes_nothing(tmp_path, capsys):
    net = tmp_path / "network.json"
    net.write_text(json.dumps(sf.network(tmp_path)), encoding="utf-8")
    (tmp_path / "exists").mkdir()
    base = ["fetch", "--network", str(net), "--channel", sf.CHANNEL, "--max-message-count", "10",
            "--preferred-max-bytes", str(P)]
    assert bf.main(base + ["--out-dir", str(tmp_path / "exists")]) == 2
    assert bf.main(base + ["--out-dir", str(tmp_path / "dry")]) == 0
    assert "dry run" in capsys.readouterr().out and not (tmp_path / "dry").exists()
    peer = sf.FakePeer(blocks_for(30))
    assert bf.main(base + ["--out-dir", str(tmp_path / "run"), "--execute"], runner=peer) == 0
    peer = sf.FakePeer(blocks_for(30), fail={2: -1})
    assert bf.main(base + ["--out-dir", str(tmp_path / "run2"), "--execute"], runner=peer) == 1


# ------------------------------------------------------------------ integration
def test_fetched_ledger_reconciles_and_feeds_the_log_collector(tmp_path):
    n = 205
    tx = ids(n)
    m = fetch(tmp_path, sf.FakePeer(blocks_for(n)))
    assert m["status"] == "COMPLETE"
    led = tmp_path / "out" / "ledger"
    base = tmp_path / "rec" / "r001_w0_r1_m-r001"
    base.parent.mkdir()
    with base.with_name(base.name + ".csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["tx_id", "seq_finish", "create_ms", "final_ms", "status", "verified", "err", "obs_hr_ns",
                    "worker", "round", "label"])
        for i, t in enumerate(tx):
            w.writerow([t, i, 1000 + 100 * i, 1450 + 100 * i, "success", "false", "", i, 0, 1, "m-r001"])
    base.with_name(base.name + ".manifest.json").write_text(json.dumps(
        {"counts": {"submitted": n, "finished": n, "pending_at_deactivate": 0, "duplicate_ids": 0,
                    "no_id_records": 0}}), encoding="utf-8")
    (tmp_path / "expect.json").write_text(json.dumps({"n": n, "max_message_count": B, "preferred_max_bytes": P,
                                                      "expect_block_pattern": True}), encoding="utf-8")
    code = rc.main(["--run", str(base), "--ledger-tx", str(led / "ledger_tx.csv"), "--expect",
                    str(tmp_path / "expect.json"), "--blocks", str(led / "blocks.csv")])
    assert code == 0
    # the log collector takes blocks.csv as the expected-block list and compares tx counts
    blocks_csv = rows(led / "blocks.csv")
    exp, source, ledger_tx = lc.expected_blocks([], blocks_csv, None)
    assert source == "blocks_csv" and exp == list(range(22)) and ledger_tx[21] == 5
