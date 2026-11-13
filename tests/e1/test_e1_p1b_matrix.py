"""Fabric-free tests for scripts/e1/e1_p1b_matrix.py (the P1b capacity-characterization matrix generator). Synthetic: nothing here is a result.

Scope since the owner decision of 2026-11-13 ("TIER 1 FIRST"): the generator's default output is TIER 1 ONLY (ten rows, 15-40 tx/s). The 26-row design of the
same date (Tier 2 up to 200 tx/s, NOT authorised) is still reproducible with max_rate=200, and its original hash is pinned here so that a silent change is noticed.
"""
import csv
import hashlib
import io
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "e1"))

import e1_p1b_matrix as mx  # noqa: E402

CH = re.compile(r"^e1-cp(\d\d)-b(\d+)-l(\d+)$")
P1A_CHANNELS = {f"e1-ql{i:02d}" for i in range(1, 29)}
FULL_DESIGN_SHA256 = "e73f0a362273317e32cb774ec2e289860eaffe342fb871473f65c6cd26ed8053"        # the 26-row design as first published on 2026-11-13


def parsed(max_rate=mx.TIER1_MAX_RATE):
    return list(csv.DictReader(io.StringIO(mx.matrix_text(max_rate))))


def test_generator_is_deterministic_and_the_header_is_the_frozen_one():
    assert mx.matrix_bytes() == mx.matrix_bytes() and mx.matrix_text().splitlines()[0] == mx.HEADER
    assert mx.HEADER == "seq,run_id,channel,kind,B,tau,P,rate_tps,replicate,warmup_s,measured_s,ramp_precondition"
    assert b"\r" not in mx.matrix_bytes() and hashlib.sha256(mx.matrix_bytes()).hexdigest() == mx.matrix_sha256()


def test_the_default_is_tier_one_only_and_has_no_row_above_the_bound():
    rows = parsed()
    assert len(rows) == 10 and max(int(r["rate_tps"]) for r in rows) == mx.TIER1_MAX_RATE == 40
    assert sorted({int(r["rate_tps"]) for r in rows}) == [15, 20, 25, 30, 40] and mx.RATES == (15, 20, 25, 30, 40)
    assert all(mx.tier(int(r["rate_tps"])) == 1 for r in rows)


def test_the_full_26_row_design_still_reproduces_its_original_hash_and_tier_one_is_an_exact_prefix_of_it():
    full = mx.matrix_text(200)
    assert hashlib.sha256(full.encode("utf-8")).hexdigest() == FULL_DESIGN_SHA256 == mx.FULL_DESIGN_SHA256
    assert len(parsed(200)) == 26
    tier1_lines, full_lines = mx.matrix_text().splitlines(), full.splitlines()
    assert tier1_lines == full_lines[:len(tier1_lines)] and len(tier1_lines) == 11                 # header + ten rows, byte for byte the same rows
    assert mx.matrix_sha256() != FULL_DESIGN_SHA256


def test_a_max_rate_that_is_not_a_rung_is_refused():
    for bad in (0, 10, 45, 201):
        with pytest.raises(ValueError, match="not a rung"):
            mx.rows(bad)


def test_shape_two_chains_five_rungs_rung_major():
    rows = parsed()
    assert len(rows) == 10 == len(mx.CHAINS) * len(mx.RATES)
    assert [r["run_id"] for r in rows] == [f"cp{i:02d}" for i in range(1, 11)] and [int(r["seq"]) for r in rows] == list(range(1, 11))
    for k, rate in enumerate(mx.RATES):
        pair = rows[2 * k: 2 * k + 2]
        assert {int(r["rate_tps"]) for r in pair} == {rate} and sorted(int(r["B"]) for r in pair) == [10, 40]     # both settings adjacent in time


def test_every_row_has_the_frozen_common_parameters_and_one_replicate():
    for r in parsed(200):
        assert (r["tau"], r["P"], r["warmup_s"], r["measured_s"], r["replicate"], r["kind"]) == ("2s", "10485760", "30", "120", "1", "ramp")


def test_channels_encode_seq_b_rate_and_never_collide_with_p1a_or_each_other():
    rows = parsed(200)
    seen = set()
    for r in rows:
        m = CH.match(r["channel"])
        assert m and int(m.group(1)) == int(r["seq"]) and int(m.group(2)) == int(r["B"]) and int(m.group(3)) == int(r["rate_tps"])
        assert re.match(r"^e1-[A-Za-z0-9_.-]{1,40}$", r["channel"]) and r["channel"] not in seen
        assert not any(r["channel"].startswith(p + "-") or r["channel"] == p for p in P1A_CHANNELS)
        seen.add(r["channel"])


def test_the_ten_skipped_p1a_rungs_are_tier_one_and_the_extension_is_tier_two():
    rows = parsed(200)
    t1 = [r for r in rows if mx.tier(int(r["rate_tps"])) == 1]
    assert len(t1) == 10 and sorted({int(r["rate_tps"]) for r in t1}) == [15, 20, 25, 30, 40]
    assert [r["run_id"] for r in t1] == [f"cp{i:02d}" for i in range(1, 11)]
    t2 = [r for r in rows if mx.tier(int(r["rate_tps"])) == 2]
    assert len(t2) == 16 and min(int(r["rate_tps"]) for r in t2) == 60 and max(int(r["rate_tps"]) for r in t2) == 200
    assert parsed() == t1                                                                      # the default matrix is exactly the Tier 1 rows


def test_rates_ascend_within_each_chain_and_preconditions_name_the_previous_rung_of_the_same_setting():
    rows = parsed()
    for B in mx.CHAINS:
        chain = [r for r in rows if int(r["B"]) == B]
        assert [int(r["rate_tps"]) for r in chain] == list(mx.RATES)
        anchor, rate = mx.ANCHORS[B]
        assert chain[0]["ramp_precondition"] == f"run only if the P1a run {anchor} (B={B}, rate={rate}) is HEALTHY in the erratum E1-QL-ERR-001 derived table"
        for prev, cur in zip(chain, chain[1:]):
            assert cur["ramp_precondition"] == f"run only if the ramp run at B={B}, rate={prev['rate_tps']} was HEALTHY"


def test_anchors_are_the_p1a_rung_one_runs_of_the_same_setting():
    assert mx.ANCHORS == {10: ("ql23", 10), 40: ("ql17", 10)}


def test_seeded_order_is_frozen():
    """The coin that orders the pair at each rate is part of the frozen matrix: pin the first pairs so a change is noticed."""
    first = [(int(r["B"]), int(r["rate_tps"])) for r in parsed()[:6]]
    assert sorted(first[0:2]) == [(10, 15), (40, 15)] and sorted(first[2:4]) == [(10, 20), (40, 20)] and sorted(first[4:6]) == [(10, 25), (40, 25)]
    assert first == [(40, 15), (10, 15), (40, 20), (10, 20), (40, 25), (10, 25)]                 # the coin's actual outcome for seed 20261113
    again = [(int(r["B"]), int(r["rate_tps"])) for r in parsed()[:6]]
    assert first == again


def test_cli_writes_once_defaults_to_tier_one_and_refuses_to_overwrite(tmp_path):
    out = tmp_path / "m.csv"
    assert mx.main(["--out", str(out)]) == 0 and out.read_bytes() == mx.matrix_bytes() and len(parsed()) == 10
    with pytest.raises(FileExistsError):
        mx.main(["--out", str(out)])
    full = tmp_path / "full.csv"
    assert mx.main(["--out", str(full), "--max-rate-tps", "200"]) == 0 and hashlib.sha256(full.read_bytes()).hexdigest() == FULL_DESIGN_SHA256
