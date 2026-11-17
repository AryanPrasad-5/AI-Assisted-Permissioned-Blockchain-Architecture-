"""Owner decision D3 (2026-11-17), file summarizer: --strict-baseline.

An absent baseline is reported unavailable, never as zero; --fresh-channel is refused in this mode;
block creation is cross-checked against the independently observed ledger height delta.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_metrics_scraper as ms              # noqa: E402
import synthetic_fabric as sf                # noqa: E402

CH = "e1-new"
T3 = [{"name": f"orderer{i}", "role": "orderer", "url": f"http://o{i}:9443/metrics"} for i in (1, 2, 3)]
U1, U2, U3 = (f"http://o{i}:9443/metrics" for i in (1, 2, 3))


def orderer_text(leader, cut=None, other_cut=None):
    c = (("channel", CH),)
    s = {("consensus_etcdraft_is_leader", c): leader, ("consensus_etcdraft_leader_changes", c): 0,
         ("consensus_etcdraft_committed_block_number", c): 4}
    if cut is not None:
        s[("blockcutter_block_fill_duration_bucket", c + (("le", "+Inf"),))] = cut
        s[("blockcutter_block_fill_duration_sum", c)] = round(2.0 * cut, 6)
        s[("blockcutter_block_fill_duration_count", c)] = cut
    if other_cut is not None:
        o = (("channel", "e1-other"),)
        s[("blockcutter_block_fill_duration_bucket", o + (("le", "+Inf"),))] = other_cut
        s[("blockcutter_block_fill_duration_sum", o)] = round(2.0 * other_cut, 6)
        s[("blockcutter_block_fill_duration_count", o)] = other_cut
    return sf.exposition(s, sf.ORDERER_TYPES)


CLOCKS: dict[str, "sf.Clock"] = {}


def scrape(tmp_path, phase, bodies, name=None, **kw):
    # One clock per test, advanced before every phase, as one real scraping process would see it.
    # (A fresh clock per phase would give the before and after scrapes the same timestamps, and the
    # time-ordered reset check could not tell which reading came first.)
    clock = CLOCKS.setdefault(str(tmp_path), sf.Clock())
    clock.step(1_000_000_000)

    def fetch(url, timeout):
        item = bodies[url]
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, tuple):
            return item
        return 200, item.encode()

    return ms.run_scrape(T3, phase, tmp_path / (name or phase), fetch=fetch, wall_ns=clock.wall_ns,
                         mono_ns=clock.mono_ns, sleep=clock.sleep, **kw)


def run(tmp_path, before, after, *, during=None, heights=None, **kw):
    scrape(tmp_path, "before", before)                      # chronological order, as in a real run
    if during is not None:
        scrape(tmp_path, "during", during, max_rounds=2, interval_s=1.0)
    scrape(tmp_path, "after", after)
    lo, hi = heights if heights else (None, None)
    return ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "summary",
                        during=(tmp_path / "during") if during is not None else None, channel=CH,
                        strict_baseline=True, ledger_height_before=lo, ledger_height_after=hi, **kw)


QUIET = {U1: orderer_text(0), U3: orderer_text(0)}


def with_leader(cut, **extra):
    return {**QUIET, U2: orderer_text(1, cut=cut), **extra}


def test_valid_baseline_is_a_delta_and_agrees_with_the_ledger(tmp_path):
    s = run(tmp_path, with_leader(3), with_leader(11), heights=(10, 18))
    assert s["baseline_policy"].startswith("strict") and s["block_fill_delta_status"] == "available"
    leader = s["block_fill_baseline"]["orderer2"]
    assert leader["delta"]["status"] == "available" and leader["delta"]["count_delta"] == 8
    assert leader["before"]["state"] == "present_nonzero" and leader["after"]["state"] == "present_nonzero"
    assert s["block_fill_baseline"]["orderer1"]["before"]["state"] == "family_absent"      # follower never cut
    assert s["block_fill_delta_unavailable_for"] == []
    check = s["ledger_cross_check"]
    assert (check["verdict"], check["blocks_by_ledger"], check["blocks_by_counter"]) == ("CONSISTENT", 8, 8)


def test_absent_baseline_is_unavailable_never_zero_and_the_ledger_stays_authoritative(tmp_path):
    # The shape of the stored ft01r002 cell: nothing before, 8 blocks cut after, ledger 114 -> 122.
    s = run(tmp_path, {**QUIET, U2: orderer_text(1)}, with_leader(8), heights=(114, 122))
    leader = s["block_fill_baseline"]["orderer2"]
    assert leader["before"]["state"] == "family_absent"
    assert leader["delta"]["status"] == "unavailable" and leader["delta"]["reason"] == "baseline_family_absent"
    assert leader["delta"]["count_delta"] is None and leader["delta"]["mean"] is None
    assert leader["delta"]["final_absolute"]["count"] == 8.0                                # shown, labelled absolute
    assert s["block_fill_delta_status"] == "unavailable" and s["block_fill_delta_unavailable_for"] == ["orderer2"]
    check = s["ledger_cross_check"]
    assert check["verdict"] == "NOT_COMPARABLE" and check["blocks_by_ledger"] == 8 and check["blocks_by_counter"] is None
    assert s["status"] == "INCOMPLETE"                                                     # the existing completeness rule still applies


def test_strict_mode_and_fresh_channel_cannot_be_combined(tmp_path):
    scrape(tmp_path, "before", {**QUIET, U2: orderer_text(1)})
    scrape(tmp_path, "after", with_leader(8))
    with pytest.raises(ValueError, match="never read as zero"):
        ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s", channel=CH, strict_baseline=True,
                     fresh_channel=True)
    legacy = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "legacy", channel=CH, fresh_channel=True)
    assert {r["before_source"] for r in ms.tc.read_csv(tmp_path / "legacy" / "metric_deltas.csv")
            if r["metric"].endswith("_count") and "blockcutter" in r["metric"]} == {"assumed_zero"}
    assert "baseline_policy" not in legacy and "ledger_cross_check" not in legacy      # default mode unchanged


@pytest.mark.parametrize("before_item, reason", [
    (TimeoutError("timed out"), "baseline_scrape_failed"),
    ((503, b"busy"), "baseline_scrape_failed"),
    ((200, b"block{ nope\n"), "baseline_malformed"),
    ((200, b"\xff\xfe"), "baseline_malformed"),
    ((200, b"   \n"), "baseline_malformed"),
])
def test_a_failed_or_malformed_baseline_scrape_gives_no_delta(tmp_path, before_item, reason):
    s = run(tmp_path, {**QUIET, U2: before_item}, with_leader(8), heights=(10, 18))
    leader = s["block_fill_baseline"]["orderer2"]
    assert leader["delta"]["reason"] == reason and leader["delta"]["count_delta"] is None
    assert s["ledger_cross_check"]["verdict"] == "NOT_COMPARABLE"


def test_a_failed_final_scrape_gives_no_delta_even_if_earlier_scrapes_succeeded(tmp_path):
    s = run(tmp_path, with_leader(3), {**QUIET, U2: ConnectionRefusedError("refused")},
            during=with_leader(6), heights=(10, 18))
    delta = s["block_fill_baseline"]["orderer2"]["delta"]
    assert delta["reason"] == "final_scrape_failed" and delta["count_delta"] is None


def test_series_for_another_channel_only_is_series_absent(tmp_path):
    s = run(tmp_path, {**QUIET, U2: orderer_text(1, other_cut=5)}, with_leader(8))
    leader = s["block_fill_baseline"]["orderer2"]
    assert leader["before"]["state"] == "series_absent" and leader["delta"]["reason"] == "baseline_series_absent"


def test_an_observed_zero_baseline_is_a_valid_baseline(tmp_path):
    s = run(tmp_path, with_leader(0), with_leader(4), heights=(5, 9))
    leader = s["block_fill_baseline"]["orderer2"]
    assert leader["before"]["state"] == "present_zero" and leader["delta"]["count_delta"] == 4
    assert s["ledger_cross_check"]["verdict"] == "CONSISTENT"


def test_counter_reset_between_the_two_readings_is_detected_even_when_the_pair_looks_valid(tmp_path):
    # before 3, during 6, after 4: 4 >= 3 looks like a delta of 1, but the counter fell from 6 to 4.
    s = run(tmp_path, with_leader(3), with_leader(4), during=with_leader(6), heights=(10, 11))
    leader = s["block_fill_baseline"]["orderer2"]
    assert leader["reset_seen"] is True
    assert leader["delta"]["reason"] == "counter_reset_observed_between_readings" and leader["delta"]["count_delta"] is None
    assert s["ledger_cross_check"]["verdict"] == "NOT_COMPARABLE"


def test_a_decrease_between_before_and_after_is_a_reset(tmp_path):
    s = run(tmp_path, with_leader(5), with_leader(2))
    leader = s["block_fill_baseline"]["orderer2"]
    assert leader["reset_seen"] is True and leader["delta"]["count_delta"] is None


def test_disagreement_with_the_ledger_is_reported_not_corrected(tmp_path):
    s = run(tmp_path, with_leader(3), with_leader(4), heights=(10, 15))
    check = s["ledger_cross_check"]
    assert check["verdict"] == "INCONSISTENT" and check["blocks_by_ledger"] == 5 and check["blocks_by_counter"] == 1


def test_without_ledger_heights_the_cross_check_says_so(tmp_path):
    s = run(tmp_path, with_leader(3), with_leader(4))
    assert s["ledger_cross_check"]["verdict"] == "NOT_COMPARABLE"
    assert s["ledger_cross_check"]["reason"] == "no independent ledger heights supplied"


def test_argument_validation(tmp_path):
    scrape(tmp_path, "before", with_leader(3))
    scrape(tmp_path, "after", with_leader(4))
    b, a = tmp_path / "before", tmp_path / "after"
    with pytest.raises(ValueError, match="needs a channel"):
        ms.summarize(b, a, tmp_path / "s1", strict_baseline=True)
    with pytest.raises(ValueError, match="both ledger heights or neither"):
        ms.summarize(b, a, tmp_path / "s2", channel=CH, strict_baseline=True, ledger_height_before=1)
    with pytest.raises(ValueError, match="strict baseline mode only"):
        ms.summarize(b, a, tmp_path / "s3", channel=CH, ledger_height_before=1, ledger_height_after=2)


def test_cli_refuses_fresh_channel_in_strict_mode_and_reports_the_cross_check(tmp_path, capsys):
    scrape(tmp_path, "before", with_leader(3))
    scrape(tmp_path, "after", with_leader(11))
    base = ["summarize", "--before", str(tmp_path / "before"), "--after", str(tmp_path / "after"), "--channel", CH]
    with pytest.raises(SystemExit) as refused:
        ms.main([*base, "--strict-baseline", "--fresh-channel", "--out-dir", str(tmp_path / "o1")])
    assert refused.value.code == 2 and not (tmp_path / "o1").exists()
    rc = ms.main([*base, "--strict-baseline", "--ledger-height-before", "10", "--ledger-height-after", "18",
                  "--out-dir", str(tmp_path / "o2")])
    out = capsys.readouterr().out
    assert "block-fill delta available; ledger cross-check CONSISTENT" in out and isinstance(rc, int)
