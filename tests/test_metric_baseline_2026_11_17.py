"""Owner decision D3 (2026-11-17): block-fill counter baselines.

An absent series is never converted to zero. A delta exists only when both readings are valid and
comparable, and block creation is checked independently against the ledger height delta.
"""

import pytest

from fabric_perf.controllers import BaselineState, DeltaStatus, PrometheusTelemetryAdapter
from fabric_perf.controllers.metric_baseline import (
    CrossCheck,
    HistogramObservation,
    assess_delta,
    cross_check_block_count,
    observe_histogram,
)
from fabric_perf.controllers.telemetry import parse_prometheus_text

CH = "e1-new"
S = BaselineState


def fill(count, total=None, channel=CH):
    total = 2.0 * count if total is None else total
    return (f'blockcutter_block_fill_duration_bucket{{channel="{channel}",le="+Inf"}} {count}\n'
            f'blockcutter_block_fill_duration_sum{{channel="{channel}"}} {total}\n'
            f'blockcutter_block_fill_duration_count{{channel="{channel}"}} {count}\n')


LEADER = f'consensus_etcdraft_is_leader{{channel="{CH}"}} 1\n'


def reading(body):
    return observe_histogram(parse_prometheus_text(body), channel=CH)


# ----------------------------------------------------------------- the six states
@pytest.mark.parametrize("body, state", [
    ("go_goroutines 7\n", S.FAMILY_ABSENT),
    (LEADER, S.FAMILY_ABSENT),
    (fill(3, channel="other"), S.SERIES_ABSENT),
    (fill(0, 0.0), S.PRESENT_ZERO),
    (fill(3), S.PRESENT_NONZERO),
    (f'blockcutter_block_fill_duration_count{{channel="{CH}"}} 3\n', S.MALFORMED),              # no _sum
    (f'blockcutter_block_fill_duration_sum{{channel="{CH}"}} 3\n', S.MALFORMED),                # no _count
    (fill(3) + f'blockcutter_block_fill_duration_count{{channel="{CH}"}} 4\n', S.MALFORMED),    # duplicate _count
    (fill(-1, 1.0), S.MALFORMED),
    (fill(2.5, 5.0), S.MALFORMED),
    (fill(0, 1.0), S.MALFORMED),                                                               # sum without observations
])
def test_each_reading_is_classified_into_exactly_one_state(body, state):
    assert reading(body).state is state


def test_valid_states_carry_values_and_invalid_states_never_do():
    nonzero = reading(fill(4))
    assert (nonzero.count, nonzero.total, nonzero.valid) == (4.0, 8.0, True)
    zero = reading(fill(0, 0.0))
    assert (zero.count, zero.total, zero.valid) == (0.0, 0.0, True)
    for body in ("go_goroutines 1\n", fill(1, channel="other"), "x"):
        text = body if body != "x" else f'blockcutter_block_fill_duration_count{{channel="{CH}"}} 1\n'
        absent = reading(text)
        assert not absent.valid and absent.count is None and absent.total is None
    with pytest.raises(ValueError):
        HistogramObservation(S.SCRAPE_FAILED, count=1.0, total=2.0)
    with pytest.raises(ValueError):
        HistogramObservation(S.PRESENT_ZERO, count=1.0, total=2.0)
    with pytest.raises(ValueError):
        HistogramObservation(S.PRESENT_NONZERO, count=float("nan"), total=1.0)


# ------------------------------------------------------------------ delta rules
@pytest.mark.parametrize("baseline_state, reason", [
    (S.FAMILY_ABSENT, "baseline_family_absent"), (S.SERIES_ABSENT, "baseline_series_absent"),
    (S.SCRAPE_FAILED, "baseline_scrape_failed"), (S.MALFORMED, "baseline_malformed")])
def test_an_invalid_baseline_never_becomes_a_zero_baseline(baseline_state, reason):
    before = HistogramObservation(baseline_state, detail="x")
    after = reading(fill(8))                                       # the stored-run shape: 8 blocks, baseline absent
    delta = assess_delta(before, after)
    assert delta.status is DeltaStatus.UNAVAILABLE and delta.reason == reason
    assert delta.count_delta is None and delta.sum_delta is None and delta.mean is None   # not 8, and not 0
    document = delta.as_dict()
    assert document["final_absolute"]["count"] == 8.0                                    # reported, labelled as absolute
    assert "not a delta" in document["final_absolute"]["note"]


@pytest.mark.parametrize("final_state, reason", [
    (S.FAMILY_ABSENT, "final_family_absent"), (S.SERIES_ABSENT, "final_series_absent"),
    (S.SCRAPE_FAILED, "final_scrape_failed"), (S.MALFORMED, "final_malformed")])
def test_an_invalid_final_reading_gives_no_delta(final_state, reason):
    delta = assess_delta(reading(fill(2)), HistogramObservation(final_state))
    assert delta.status is DeltaStatus.UNAVAILABLE and delta.reason == reason and delta.count_delta is None


def test_no_earlier_observation_is_its_own_reason():
    delta = assess_delta(None, reading(fill(2)))
    assert delta.reason == "no_earlier_observation" and delta.baseline_state is None and delta.count_delta is None


def test_valid_readings_give_a_delta_including_a_valid_zero_baseline():
    delta = assess_delta(reading(fill(0, 0.0)), reading(fill(4)))
    assert delta.status is DeltaStatus.AVAILABLE and (delta.count_delta, delta.sum_delta, delta.mean) == (4.0, 8.0, 2.0)
    assert delta.baseline_state is S.PRESENT_ZERO and delta.final_state is S.PRESENT_NONZERO
    quiet = assess_delta(reading(fill(3)), reading(fill(3)))
    assert quiet.status is DeltaStatus.AVAILABLE and quiet.count_delta == 0.0 and quiet.mean is None   # no block cut: no mean


def test_counter_reset_is_unavailable_in_both_forms():
    assert assess_delta(reading(fill(5)), reading(fill(1))).reason == "counter_reset"
    assert assess_delta(reading(fill(5)), reading(fill(5, 7.0))).reason == "counter_reset"          # the sum went backwards
    seen = assess_delta(reading(fill(3)), reading(fill(4)), reset_seen=True)                          # looks fine, a reset happened between
    assert seen.status is DeltaStatus.UNAVAILABLE and seen.reason == "counter_reset_observed_between_readings"


def test_series_never_observed_on_either_side_is_not_a_failure_and_not_a_zero():
    delta = assess_delta(HistogramObservation(S.FAMILY_ABSENT), HistogramObservation(S.SERIES_ABSENT))
    assert delta.never_observed and delta.status is DeltaStatus.UNAVAILABLE and delta.count_delta is None


# ------------------------------------------- counter delta versus ledger height delta
def test_cross_check_consistent_when_every_cutter_has_a_valid_delta():
    leader = assess_delta(reading(fill(3)), reading(fill(11)))                       # 8 blocks cut by the leader
    follower = assess_delta(HistogramObservation(S.FAMILY_ABSENT), HistogramObservation(S.FAMILY_ABSENT))
    result = cross_check_block_count({"orderer3": leader, "orderer1": follower}, ledger_height_before=114,
                                     ledger_height_after=122)
    assert result.verdict is CrossCheck.CONSISTENT and result.blocks_by_ledger == 8 and result.blocks_by_counter == 8
    assert result.contributing_targets == ("orderer3",)


def test_cross_check_flags_disagreement_without_correcting_either_number():
    leader = assess_delta(reading(fill(3)), reading(fill(4)))                        # counter says 1
    result = cross_check_block_count({"orderer3": leader}, ledger_height_before=10, ledger_height_after=15)
    assert result.verdict is CrossCheck.INCONSISTENT and result.blocks_by_ledger == 5 and result.blocks_by_counter == 1
    assert result.reason == "counter_total_1_differs_from_ledger_delta_5"


def test_cross_check_is_not_comparable_when_a_cutter_has_no_valid_delta_and_ledger_stays_authoritative():
    absent_leader = assess_delta(HistogramObservation(S.FAMILY_ABSENT), reading(fill(8)))
    result = cross_check_block_count({"orderer3": absent_leader}, ledger_height_before=114, ledger_height_after=122)
    assert result.verdict is CrossCheck.NOT_COMPARABLE and result.blocks_by_ledger == 8
    assert result.blocks_by_counter is None and result.unresolved_targets == ("orderer3",)
    assert "baseline_family_absent" in result.reason


def test_cross_check_with_no_series_anywhere_but_new_blocks_is_inconsistent():
    nothing = assess_delta(HistogramObservation(S.FAMILY_ABSENT), HistogramObservation(S.FAMILY_ABSENT))
    result = cross_check_block_count({"orderer1": nothing}, ledger_height_before=0, ledger_height_after=3)
    assert result.verdict is CrossCheck.INCONSISTENT and result.blocks_by_counter == 0
    assert cross_check_block_count({"orderer1": nothing}, ledger_height_before=3,
                                   ledger_height_after=3).verdict is CrossCheck.CONSISTENT


@pytest.mark.parametrize("before, after, reason", [(-1, 5, "ledger_height_invalid"), (True, 5, "ledger_height_invalid"),
                                                   (5.0, 6, "ledger_height_invalid"), (9, 8, "ledger_height_decreased")])
def test_cross_check_rejects_unusable_ledger_heights(before, after, reason):
    result = cross_check_block_count({}, ledger_height_before=before, ledger_height_after=after)
    assert result.verdict is CrossCheck.NOT_COMPARABLE and result.reason == reason and result.blocks_by_ledger is None


# --------------------------------------------------------------- live adapter
class Script:
    """Serve one scripted body (or error) per sample and endpoint; ``collect`` advances the sample."""

    def __init__(self, samples, endpoints=None):
        self.samples, self.k = samples, 0
        self.adapter = PrometheusTelemetryAdapter(endpoints or {"orderer1": "http://orderer1/metrics"}, fetch=self)

    def __call__(self, url, timeout):
        item = self.samples[self.k][url.split("//")[1].split("/")[0]]
        if isinstance(item, BaseException):
            raise item
        return item

    def collect(self):
        snapshot = self.adapter.collect(CH, timestamp_utc=f"2026-11-17T12:00:{self.k:02d}.000+00:00")
        self.k += 1
        return snapshot


def block_fill(snapshot):
    metric = snapshot.metric("block_fill_duration_s")
    return metric.value, metric.missing_reason


def test_first_sample_has_no_baseline_and_a_valid_pair_gives_the_mean():
    script = Script([{"orderer1": LEADER + fill(1)}, {"orderer1": LEADER + fill(2)}])
    first, second = script.collect(), script.collect()
    assert block_fill(first)[0] is None and "no_earlier_observation" in block_fill(first)[1]
    assert block_fill(second) == (2.0, None)
    assert second.metric("block_fill_duration_s").sampling_interval_s is not None


def test_an_absent_baseline_is_unavailable_until_two_valid_readings_exist():
    script = Script([{"orderer1": LEADER}, {"orderer1": LEADER + fill(3)}, {"orderer1": LEADER + fill(5)}])
    first, second, third = script.collect(), script.collect(), script.collect()
    assert block_fill(second)[0] is None and "baseline_family_absent" in block_fill(second)[1]   # not 3, not 6/3
    assert second.health["block_fill"]["orderer1"]["reading"]["state"] == "present_nonzero"
    assert block_fill(third) == (2.0, None)
    assert first.health["block_fill"]["orderer1"]["reading"]["state"] == "family_absent"


def test_a_series_absent_for_this_channel_is_not_the_same_as_an_absent_family():
    script = Script([{"orderer1": LEADER + fill(3, channel="other")}, {"orderer1": LEADER + fill(5)}])
    script.collect()
    second = script.collect()
    assert block_fill(second)[0] is None and "baseline_series_absent" in block_fill(second)[1]


def test_a_zero_baseline_that_was_actually_observed_is_a_valid_baseline():
    script = Script([{"orderer1": LEADER + fill(0, 0.0)}, {"orderer1": LEADER + fill(2)}])
    script.collect()
    assert block_fill(script.collect()) == (2.0, None)


def test_counter_reset_gives_no_delta_and_the_next_pair_is_valid_again():
    script = Script([{"orderer1": LEADER + fill(5)}, {"orderer1": LEADER + fill(1)}, {"orderer1": LEADER + fill(3)}])
    script.collect()
    reset = script.collect()
    assert block_fill(reset)[0] is None and "counter_reset" in block_fill(reset)[1]
    assert block_fill(script.collect()) == (2.0, None)


def test_scrape_failure_between_readings_never_spans_the_gap():
    script = Script([{"orderer1": LEADER + fill(1)}, {"orderer1": TimeoutError("timed out")},
                     {"orderer1": LEADER + fill(3)}, {"orderer1": LEADER + fill(4)}])
    script.collect()
    failed = script.collect()
    assert failed.metric("orderer_peer_health").value == 0.0
    assert failed.health["block_fill"]["orderer1"]["reading"]["state"] == "scrape_failed"
    assert block_fill(failed)[0] is None and "final_scrape_failed" in block_fill(failed)[1]
    after_gap = script.collect()
    assert block_fill(after_gap)[0] is None and "baseline_scrape_failed" in block_fill(after_gap)[1]   # not (6-2)/(3-1)
    assert block_fill(script.collect()) == (2.0, None)


def test_malformed_scrape_is_its_own_state():
    script = Script([{"orderer1": LEADER + fill(1)}, {"orderer1": "block{ nope\n"}, {"orderer1": LEADER + fill(3)}])
    script.collect()
    bad = script.collect()
    assert bad.health["block_fill"]["orderer1"]["reading"]["state"] == "malformed"
    assert "final_malformed" in block_fill(bad)[1]
    assert "baseline_malformed" in block_fill(script.collect())[1]


THREE = {"orderer1": "http://orderer1/metrics", "orderer2": "http://orderer2/metrics", "orderer3": "http://orderer3/metrics"}


def test_followers_that_never_cut_do_not_block_the_leaders_delta():
    quiet = LEADER
    script = Script([{"orderer1": quiet, "orderer2": LEADER + fill(1), "orderer3": quiet},
                     {"orderer1": quiet, "orderer2": LEADER + fill(2), "orderer3": quiet}], THREE)
    first, second = script.collect(), script.collect()
    assert block_fill(first)[0] is None
    assert block_fill(second) == (2.0, None)                       # followers: absent before and after, nothing to difference
    assert second.health["block_fill"]["orderer1"]["delta"]["reason"] == "baseline_family_absent"


def test_a_new_cutter_makes_the_aggregate_unavailable_until_it_has_a_baseline():
    quiet = LEADER
    script = Script([{"orderer1": quiet, "orderer2": LEADER + fill(2), "orderer3": quiet},
                     {"orderer1": quiet, "orderer2": LEADER + fill(3), "orderer3": LEADER + fill(1)},     # leadership moved
                     {"orderer1": quiet, "orderer2": LEADER + fill(3), "orderer3": LEADER + fill(2)}], THREE)
    script.collect()
    moved = script.collect()
    assert block_fill(moved)[0] is None and "orderer3 (baseline_family_absent)" in block_fill(moved)[1]
    assert block_fill(script.collect()) == (2.0, None)


def test_legacy_adapter_surface_is_unchanged():
    script = Script([{"orderer1": LEADER + fill(1)}])
    snapshot = script.collect()
    assert snapshot.metric("block_fill_duration_s").unit == "s"
    assert snapshot.metric("block_fill_duration_s").source.startswith("Fabric Prometheus")
    assert snapshot.health["endpoint_errors"] == {}
