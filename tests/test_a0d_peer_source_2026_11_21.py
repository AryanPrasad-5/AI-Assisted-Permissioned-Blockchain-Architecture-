"""Task A0d (2026-11-21): the peer-CLI ledger source and the follower command, against a MOCKED peer.

The mocked peer is ``tests/e1/synthetic_fabric.FakePeer``: it answers ``peer channel getinfo`` / ``fetch`` with synthetic,
hash-chained protobuf blocks that the REAL decoder in ``e1_ledger_blocks`` / ``e1_block_fetch`` parses. That exercises the
decoding, the validation-flag mapping, the hash-chain and data-hash checks and the command construction. It does not show
that a real Fabric peer answers the same way: no peer, Docker, WSL or network is contacted, and the follower was never run
with ``--execute`` against a real peer.
"""

import json
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts" / "e1"))
sys.path.insert(0, str(REPO / "tests" / "e1"))

import e1_channel_run as cr  # noqa: E402
import e1_ledger_follow as follow  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

from fabric_perf.controllers import channel_selection as cs  # noqa: E402
from fabric_perf.controllers.ledger_evidence import (  # noqa: E402
    KIND_FAILURE,
    LedgerCollector,
    LedgerReadError,
    read_observation_journal,
    windowed_committed_rate,
)

CH = "e1-offline.a0d"


def groups(per_block, *, codes=None, channel=CH):
    """Block 0 = a configuration envelope (as in a real channel); then one block per entry of ``per_block``."""
    out = [[{"tx_id": "cfg0", "header_type": 1, "channel": channel}]]
    n = 0
    for count in per_block:
        out.append([{"tx_id": f"tx{n + i:04d}", "channel": channel, "code": (codes or {}).get(n + i, 0)} for i in range(count)])
        n += count
    return out


def net(tmp_path):
    return cr.Network(sf.network(tmp_path))


class Ticker:
    """A real wall clock that never returns the same millisecond twice (the follower stamps real time)."""

    def __call__(self, seconds):
        time.sleep(0.004)


def source_for(tmp_path, blocks, **peer_kwargs):
    peer = sf.FakePeer(blocks, **peer_kwargs)
    return follow.PeerCliLedgerSource(net(tmp_path), CH, peer, tmp_path / "raw"), peer


# ============================================================ decoding and command construction
def test_the_source_reads_height_and_decodes_blocks_with_the_real_decoder(tmp_path):
    blocks = sf.chain(groups([3, 2]), channel=CH)
    source, peer = source_for(tmp_path, blocks)
    assert source.height() == 3
    block = source.block(1)
    assert block.number == 1 and len(block.txs) == 3 and {t.header_type for t in block.txs} == {3}
    assert all(t.validation_code == 0 and t.channel_id == CH for t in block.txs)
    assert block.data_hash_ok is True and block.flags_len == 3 and len(block.header_hash_hex) == 64
    config = source.block(0)
    assert [t.header_type for t in config.txs] == [1]
    argv = [c for c in peer.calls if c[1:3] == ["channel", "fetch"]]
    assert all("-o" not in c and "--tls" not in c for c in argv)                                  # a peer is read, never an orderer


def test_validation_codes_come_from_the_peers_flags_not_from_the_envelope(tmp_path):
    blocks = sf.chain(groups([3], codes={1: 11}), channel=CH)
    source, _ = source_for(tmp_path, blocks)
    assert [t.validation_code for t in source.block(1).txs] == [0, 11, 0]


def test_a_failed_height_or_block_read_is_a_read_error_not_a_crash(tmp_path):
    blocks = sf.chain(groups([2]), channel=CH)
    down, _ = source_for(tmp_path / "a", blocks, getinfo_ok=False)
    with pytest.raises(LedgerReadError, match="getinfo"):
        down.height()
    flaky, _ = source_for(tmp_path / "b", blocks, fail={1: -1})
    with pytest.raises(LedgerReadError, match="fetch"):
        flaky.block(1)
    empty, _ = source_for(tmp_path / "c", blocks, serve={1: b""})
    with pytest.raises(LedgerReadError):
        empty.block(1)
    garbage, _ = source_for(tmp_path / "d", blocks, serve={1: b"\x0a\xff\xff\xff"})
    with pytest.raises(LedgerReadError, match="does not decode"):
        garbage.block(1)


def test_a_runner_that_raises_is_a_failed_read(tmp_path):
    def exploding(argv, env):
        raise OSError("docker not running")
    source = follow.PeerCliLedgerSource(net(tmp_path), CH, exploding, tmp_path / "raw")
    with pytest.raises(LedgerReadError):
        source.height()


# ============================================================ the collector over the decoded ledger
def follow_run(tmp_path, blocks, steps, **peer_kwargs):
    """Poll once per entry of ``steps`` (the peer's height after that poll's growth); returns collector and journal rows."""
    peer = sf.FakePeer(blocks, height=steps[0], **peer_kwargs)
    source = follow.PeerCliLedgerSource(net(tmp_path), CH, peer, tmp_path / "raw")
    collector = LedgerCollector(source, channel_id=CH, out_dir=tmp_path / "journal", collector_run_id="peer-test")
    for height in steps:
        peer.height = height
        time.sleep(0.004)
        collector.poll()
    return collector, read_observation_journal(collector.paths["observations"]), peer


def test_committed_counts_over_real_decoded_blocks_exclude_the_config_envelope_and_the_invalid_one(tmp_path):
    blocks = sf.chain(groups([3, 4], codes={5: 11}), channel=CH)                                   # tx5 is in the second block, invalid
    collector, journal, _ = follow_run(tmp_path, blocks, [1, 3])                                   # baseline at height 1 (config block excluded)
    last = journal[-1]
    assert (last.kind, last.blocks, last.tx_total, last.endorser, last.valid, last.invalid, last.other) == ("interval", 2, 7, 7, 6, 1, 0)
    rate = windowed_committed_rate(journal, now=last.observed, window_s=60)
    assert rate.value == pytest.approx(6 / last.interval_s)


def test_a_baseline_before_the_config_block_counts_it_as_other_not_as_throughput(tmp_path):
    blocks = sf.chain(groups([2]), channel=CH)
    _, journal, _ = follow_run(tmp_path, blocks, [0, 2])
    assert (journal[-1].other, journal[-1].valid, journal[-1].endorser) == (1, 2, 2)


def test_a_tampered_block_halts_the_journal_and_writes_no_evidence_from_it(tmp_path):
    good = sf.chain(groups([2, 2]), channel=CH)
    tampered = list(good)
    tampered[2] = sf.block_bytes(2, b"\x00" * 32, [sf.envelope("tx9999", channel=CH)], [0], data_hash=b"\x11" * 32)
    collector, journal, _ = follow_run(tmp_path, tampered, [1, 3])
    assert journal[-1].kind == KIND_FAILURE and journal[-1].status == "inconsistent" and collector.halted
    assert collector.paths["tx"].read_text(encoding="utf-8").count("\n") == 1                      # header only


def test_an_envelope_of_a_foreign_channel_in_a_real_block_halts(tmp_path):
    blocks = sf.chain([[{"tx_id": "cfg0", "header_type": 1, "channel": CH}], [{"tx_id": "x", "channel": "e1-foreign"}]], channel=CH)
    collector, journal, _ = follow_run(tmp_path, blocks, [1, 2])
    assert journal[-1].status == "inconsistent" and "channel_mismatch" in journal[-1].detail


def test_a_block_that_cannot_be_fetched_yet_is_retried_and_counted_once(tmp_path):
    blocks = sf.chain(groups([3, 2]), channel=CH)
    collector, journal, peer = follow_run(tmp_path, blocks, [1, 3, 3], fail={2: 1})                # block 2 fails once, then succeeds
    assert [r.status for r in journal] == ["ok", "fetch_incomplete", "ok"]
    assert (journal[-1].valid, journal[-1].blocks, journal[-1].prev_seq) == (5, 2, 1)
    ids = [r.split(",")[0] for r in collector.paths["tx"].read_text(encoding="utf-8").splitlines()[1:]]
    assert len(ids) == len(set(ids)) == 5


# ============================================================ the follower command
def argv(tmp_path, *extra, channel=CH):
    path = tmp_path / "network.json"
    path.write_text(json.dumps(sf.network(tmp_path)), encoding="utf-8")
    return ["--channel", channel, "--network", str(path), "--out-dir", str(tmp_path / "journal"), "--collector-run-id", "f1",
            "--max-polls", "3", "--poll-interval-s", "0.01", *extra]


def test_without_execute_the_follower_only_prints_a_plan_and_touches_nothing(tmp_path, capsys):
    calls = []
    code = follow.main(argv(tmp_path), runner=lambda a, e: (calls.append(a), (1, b"", b""))[1])
    out = capsys.readouterr().out
    assert code == 0 and "dry run" in out and "PLAN" in out and calls == [] and not (tmp_path / "journal").exists()


def test_execute_on_a_non_linux_host_is_refused_before_anything_is_created(tmp_path, capsys):
    code = follow.main(argv(tmp_path, "--execute"), platform_name="win32")
    assert code == 2 and "WSL" in capsys.readouterr().out and not (tmp_path / "journal").exists()


def test_the_follower_runs_end_to_end_against_the_mocked_peer_and_reports_a_summary(tmp_path, capsys):
    blocks = sf.chain(groups([3, 2, 4]), channel=CH)
    peer = sf.FakePeer(blocks, height=1)
    grow = iter([1, 3, 4])
    original = peer.__call__

    def runner(a, e):
        if a[1:3] == ["channel", "getinfo"]:
            peer.height = next(grow, 4)
        return original(a, e)
    code = follow.main(argv(tmp_path, "--execute"), runner=runner, platform_name="linux", sleep=Ticker())
    summary = json.loads(capsys.readouterr().out)
    assert code == 0 and summary["polls"] == 3 and summary["halted"] is False and summary["by_status"] == {"ok": 3}
    journal = read_observation_journal(tmp_path / "journal" / "ledger_observations.csv")
    assert [r.kind for r in journal] == ["baseline", "interval", "interval"]
    assert [r.valid for r in journal] == [None, 5, 4]                                               # baseline at height 1; blocks 1-2, then block 3


def test_the_follower_resumes_a_journal_and_the_first_new_row_carries_no_rate(tmp_path, capsys):
    blocks = sf.chain(groups([3, 2, 4]), channel=CH)
    heights = {"v": 1}
    peer = sf.FakePeer(blocks, height=1)
    original = peer.__call__

    def runner(a, e):
        if a[1:3] == ["channel", "getinfo"]:
            peer.height = heights["v"]
        return original(a, e)
    assert follow.main(argv(tmp_path, "--execute"), runner=runner, platform_name="linux", sleep=Ticker()) == 0
    capsys.readouterr()
    heights["v"] = 4
    args = argv(tmp_path, "--execute", "--resume")
    assert follow.main(args, runner=runner, platform_name="linux", sleep=Ticker()) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["recovery"]["resumed"] is True
    journal = read_observation_journal(tmp_path / "journal" / "ledger_observations.csv")
    first_new = journal[3]
    assert first_new.kind == "resume" and first_new.rate_valid is False and first_new.valid == 9


@pytest.mark.parametrize("channel", sorted(cs.PROTECTED_REFERENCE_CHANNELS) + ["", "BAD", "e1-"])
def test_the_follower_refuses_protected_and_malformed_channels_before_reading_anything(tmp_path, channel, capsys):
    with pytest.raises(SystemExit) as exit_info:
        follow.main(argv(tmp_path, "--execute", channel=channel), runner=lambda a, e: (1, b"", b""), platform_name="linux")
    assert exit_info.value.code == 2 and not (tmp_path / "journal").exists()


def test_the_follower_has_no_default_channel_or_bounds():
    import ast
    tree = ast.parse((REPO / "scripts" / "e1" / "e1_ledger_follow.py").read_text(encoding="utf-8"))
    declared = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "add_argument":
            flag = next((a.value for a in node.args if isinstance(a, ast.Constant)), None)
            declared[flag] = {k.arg: k.value for k in node.keywords}
    for flag in ("--channel", "--network", "--out-dir", "--collector-run-id", "--max-polls", "--poll-interval-s"):
        assert declared[flag]["required"].value is True and "default" not in declared[flag], flag
    assert declared["--channel"]["type"].attr == "experiment_channel_arg"


def test_a_halted_follower_exits_with_its_own_status(tmp_path, capsys):
    good = sf.chain(groups([2, 2]), channel=CH)
    bad = list(good)
    bad[1] = sf.block_bytes(1, b"\x00" * 32, [sf.envelope("tx1", channel=CH)], [0], data_hash=b"\x22" * 32)
    peer = sf.FakePeer(bad, height=1)
    original = peer.__call__
    seq = iter([1, 3, 3])

    def runner(a, e):
        if a[1:3] == ["channel", "getinfo"]:
            peer.height = next(seq, 3)
        return original(a, e)
    code = follow.main(argv(tmp_path, "--execute"), runner=runner, platform_name="linux", sleep=Ticker())
    assert code == 3 and json.loads(capsys.readouterr().out)["halted"] is True


def test_a_real_block_whose_data_hash_is_wrong_but_whose_chain_link_is_right_is_rejected(tmp_path):
    import e1_block_fetch as bf
    good = sf.chain(groups([2, 2]), channel=CH)
    previous = bf.block_facts(good[1])["header_hash"]                                              # block 2 links correctly to block 1
    tampered = list(good)
    tampered[2] = sf.block_bytes(2, previous, [sf.envelope("tx0100", channel=CH)], [0], data_hash=b"\x33" * 32)
    collector, journal, _ = follow_run(tmp_path, tampered, [1, 3])
    assert journal[-1].status == "inconsistent" and "data_hash_mismatch" in journal[-1].detail and collector.halted


def test_a_block_with_fewer_validation_flags_than_envelopes_yields_no_code_never_valid(tmp_path):
    import e1_block_fetch as bf
    first = sf.chain(groups([2]), channel=CH)
    previous = bf.block_facts(first[0])["header_hash"]
    envelopes = [sf.envelope("tx7000", channel=CH), sf.envelope("tx7001", channel=CH)]
    short = sf.block_bytes(1, previous, envelopes, [0])                                             # two envelopes, one flag
    source, _ = source_for(tmp_path, [first[0], short])
    block = source.block(1)
    assert [t.validation_code for t in block.txs] == [0, None] and block.flags_len == 1
    collector, journal, _ = follow_run(tmp_path / "run", [first[0], short], [1, 2])
    assert journal[-1].status == "inconsistent" and "flags_length_mismatch" in journal[-1].detail
