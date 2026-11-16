"""Hand-derived cases. Independent of real data."""
from fractions import Fraction

import pytest

from cv_block_cutter import legacy_float_round, simulate_round, stair_boundaries


def test_divisor_block_has_no_timer_tail():
    outcome = simulate_round(200, 40, 1000)
    assert outcome.blocks == 25
    assert outcome.round_end == pytest.approx(999 / 200)


def test_non_divisor_block_waits_for_timer():
    # 22 full blocks hold 990 tx; the last 10 start at 990/200 and wait 2 s.
    outcome = simulate_round(200, 45, 1000)
    assert outcome.blocks == 23
    assert outcome.round_end == pytest.approx(990 / 200 + 2)


def test_timer_only_block_steps_on_two_second_cycles():
    # With window 2*rate inclusive, each block holds 2*rate+1 tx.
    slow = simulate_round(160, 800, 1000)
    fast = simulate_round(170, 800, 1000)
    assert slow.blocks == 4
    assert fast.blocks == 3
    assert slow.round_end - fast.round_end > 1.5


def test_size_cut_blocks_have_no_stair_between_160_and_170():
    low = simulate_round(160, 45, 1000).round_end
    high = simulate_round(170, 45, 1000).round_end
    assert low - high == pytest.approx(990 / 160 - 990 / 170)
    assert low - high < 0.4


def test_wait_summary_for_single_block():
    outcome = simulate_round(10, 1000, 5, Fraction(2))
    assert outcome.blocks == 1
    assert outcome.max_wait == pytest.approx(2.0)
    assert outcome.min_wait == pytest.approx(1.6)
    assert outcome.mean_wait == pytest.approx(1.8)


def test_overflow_does_not_reset_surviving_timer():
    surviving = simulate_round(100, 1000, 300, byte_cap=150, timer_survives_overflow=True)
    resetting = simulate_round(100, 1000, 300, byte_cap=150, timer_survives_overflow=False)
    # Surviving: cuts at 1.5 (overflow), 2.0 (original timer), then a new timer ends at 4.01.
    assert (surviving.blocks, surviving.round_end) == (3, pytest.approx(4.01))
    # Resetting: cut at 1.5, then a fresh timer from tx 150 ends at 3.5.
    assert (resetting.blocks, resetting.round_end) == (2, pytest.approx(3.5))


def test_tie_rule_changes_only_tied_transaction():
    inclusive = simulate_round(10, 1000, 21, tie_inclusive=True)
    exclusive = simulate_round(10, 1000, 21, tie_inclusive=False)
    assert inclusive.blocks == 1
    assert exclusive.blocks == 2


def test_rejects_bad_inputs():
    with pytest.raises(ValueError):
        simulate_round(0, 10)


def test_stair_boundaries_for_reported_values():
    assert stair_boundaries(1000, 2.0)[2:5] == pytest.approx([1000 / 6, 125.0, 100.0])


def test_legacy_function_is_preserved_for_reproduction():
    assert legacy_float_round(200.0, 45)[4] == 23
