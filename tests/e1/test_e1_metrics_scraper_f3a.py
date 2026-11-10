"""Metrics scraper 0.2 (E0-0 remediation F3a, 2026-11-09): per-target versus any-of-role expected families,
and identification of the receiving and block-cutting orderers. Synthetic exposition text shaped like the
real E0-0 check 8 output (channel e1-e00s): orderer1 received the 3 setup transactions from the peer CLI,
orderer3 received the run's transaction from the gateway, orderer2 (the Raft leader) cut the block and
never received a client Broadcast, so it has no broadcast_processed_count family at all."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_metrics_scraper as ms  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

CH = "e1-e00s"
T3 = [{"name": f"orderer{i}", "role": "orderer", "url": f"http://o{i}:9443/metrics"} for i in (1, 2, 3)]
OLD_EXPECTED_0_1 = {"orderer": ["blockcutter_block_fill_duration", "broadcast_processed_count",
                                "consensus_etcdraft_is_leader", "consensus_etcdraft_leader_changes",
                                "consensus_etcdraft_committed_block_number"], "peer": [], "other": []}


def orderer_text(leader: int, broadcast: dict | None = None, cut: int | None = None, per_target: bool = True) -> str:
    c = (("channel", CH),)
    s = {}
    if per_target:
        s.update({("consensus_etcdraft_is_leader", c): leader, ("consensus_etcdraft_leader_changes", c): 0,
                  ("consensus_etcdraft_committed_block_number", c): 4})
    for status, n in (broadcast or {}).items():
        s[("broadcast_processed_count", c + (("status", status), ("type", "ENDORSER_TRANSACTION")))] = n
    if cut is not None:
        s[("blockcutter_block_fill_duration_bucket", c + (("le", "+Inf"),))] = cut
        s[("blockcutter_block_fill_duration_sum", c)] = round(2.0 * cut, 6)
        s[("blockcutter_block_fill_duration_count", c)] = cut
    return sf.exposition(s, sf.ORDERER_TYPES)


BEFORE = {"http://o1:9443/metrics": orderer_text(0, {"SUCCESS": 3}),
          "http://o2:9443/metrics": orderer_text(1, cut=3),
          "http://o3:9443/metrics": orderer_text(0)}
AFTER = {"http://o1:9443/metrics": orderer_text(0, {"SUCCESS": 3}),
         "http://o2:9443/metrics": orderer_text(1, cut=4),
         "http://o3:9443/metrics": orderer_text(0, {"SUCCESS": 1})}


def scrape(tmp_path, phase, bodies, name=None, **kw):
    clock = sf.Clock()
    return ms.run_scrape(T3, phase, tmp_path / (name or phase), fetch=lambda u, t: (200, bodies[u].encode()),
                         wall_ns=clock.wall_ns, mono_ns=clock.mono_ns, sleep=clock.sleep, **kw)


def kinds(m):
    return {**m["anomalies"]["error"], **m["anomalies"]["warning"]}


# ------------------------------------------------------------------ positive
def test_e0_0_shape_is_complete_under_the_any_of_role_rule(tmp_path):
    m = scrape(tmp_path, "after", AFTER)
    assert m["status"] == "COMPLETE"
    assert m["expected_any_holders"] == {"orderer:blockcutter_block_fill_duration": ["orderer2"],
                                         "orderer:broadcast_processed_count": ["orderer1", "orderer3"]}


def test_the_0_1_rule_reproduces_the_e0_0_failure(tmp_path):
    """Regression record of the defect: the per-target rule of 0.1 flags orderer2 and orderer1/orderer3."""
    m = scrape(tmp_path, "after", AFTER, expected=OLD_EXPECTED_0_1, expected_any={})
    bad = [a for a in ms.tc.read_csv(tmp_path / "after" / "metrics_anomalies.csv")
           if a["kind"] == "expected_metric_absent"]
    assert m["status"] == "INCOMPLETE"
    assert ("orderer2", "broadcast_processed_count") in {(a["subject"], a["detail"]) for a in bad}


def test_summary_identifies_receiver_and_cutter(tmp_path):
    scrape(tmp_path, "before", BEFORE)
    scrape(tmp_path, "after", AFTER)
    s = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s", channel=CH, fresh_channel=True,
                     expect_traffic=True)
    assert s["status"] == "COMPLETE"
    assert s["traffic"]["broadcast_receivers"] == {"orderer3": 1.0}         # not orderer1 (setup only, delta 0)
    assert s["traffic"]["block_cutters"] == {"orderer2": 1.0}
    assert s["traffic"]["leaders_after"] == ["orderer2"]
    assert s["derived"]["orderer3"]["broadcast_success_delta"] == 1.0
    assert s["derived"]["orderer2"]["blocks_cut_delta"] == 1.0 and s["derived"]["orderer2"]["fill_duration_mean_s"] == 2.0


# ------------------------------------------------------------------ negative (missing data is still detected)
def test_any_family_absent_on_every_orderer_is_incomplete_after_and_a_warning_before(tmp_path):
    none = {u: orderer_text(1 if "o2" in u else 0) for u in AFTER}          # no broadcast, no blockcutter anywhere
    m = scrape(tmp_path, "after", none)
    assert m["status"] == "INCOMPLETE" and kinds(m)["expected_metric_absent"] == 2
    rows = [a for a in ms.tc.read_csv(tmp_path / "after" / "metrics_anomalies.csv")]
    assert {a["subject"] for a in rows} == {"any orderer"}
    b = scrape(tmp_path, "before", none, name="b")
    assert b["status"] == "COMPLETE_WITH_WARNINGS" and kinds(b)["expected_metric_absent_early"] == 2


def test_per_target_family_missing_on_one_orderer_is_still_incomplete(tmp_path):
    bodies = dict(AFTER)
    bodies["http://o3:9443/metrics"] = orderer_text(0, {"SUCCESS": 1}, per_target=False)
    m = scrape(tmp_path, "after", bodies)
    bad = [a for a in ms.tc.read_csv(tmp_path / "after" / "metrics_anomalies.csv")]
    assert m["status"] == "INCOMPLETE"
    assert {(a["subject"], a["detail"]) for a in bad} == {("orderer3", f) for f in ms.DEFAULT_EXPECTED["orderer"]}


def test_failed_target_is_not_hidden_by_the_any_rule(tmp_path):
    clock = sf.Clock()

    def fetch(u, t):
        if "o3" in u:
            raise ConnectionRefusedError("refused")
        return 200, AFTER[u].encode()
    m = ms.run_scrape(T3, "after", tmp_path / "a", fetch=fetch, wall_ns=clock.wall_ns, mono_ns=clock.mono_ns,
                      sleep=clock.sleep)
    assert m["status"] == "INCOMPLETE" and kinds(m)["scrape_failed"] == 1 and kinds(m)["no_successful_scrape"] == 1


def test_expect_traffic_without_deltas_is_incomplete(tmp_path):
    scrape(tmp_path, "before", AFTER)
    scrape(tmp_path, "after", AFTER, name="after")
    s = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s", channel=CH, fresh_channel=True,
                     expect_traffic=True)
    assert s["status"] == "INCOMPLETE"
    assert kinds(s)["broadcast_receiver_unidentified"] == 1 and kinds(s)["block_cutter_unidentified"] == 1
    s2 = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s2", channel=CH, fresh_channel=True)
    assert s2["status"] == "COMPLETE"                                         # opt-in: unchanged without the flag


def test_cutter_not_leader_and_rejected_broadcast_are_warnings(tmp_path):
    after = dict(AFTER)
    after["http://o1:9443/metrics"] = orderer_text(1, {"SUCCESS": 3, "SERVICE_UNAVAILABLE": 1})
    after["http://o2:9443/metrics"] = orderer_text(0, cut=4)
    before = dict(BEFORE)
    before["http://o2:9443/metrics"] = orderer_text(0, cut=3)
    scrape(tmp_path, "before", before)
    scrape(tmp_path, "after", after)
    s = ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s", channel=CH, fresh_channel=True,
                     expect_traffic=True)
    assert s["status"] == "COMPLETE_WITH_WARNINGS"
    assert kinds(s) == {"cutter_not_leader": 1, "broadcast_not_success": 1}
    assert s["traffic"]["broadcast_by_target_status"]["orderer1"] == {"SERVICE_UNAVAILABLE": 1.0}


def test_expect_traffic_needs_a_channel(tmp_path):
    scrape(tmp_path, "before", BEFORE)
    scrape(tmp_path, "after", AFTER)
    with pytest.raises(ValueError):
        ms.summarize(tmp_path / "before", tmp_path / "after", tmp_path / "s", expect_traffic=True)
    assert ms.main(["summarize", "--before", str(tmp_path / "before"), "--after", str(tmp_path / "after"),
                    "--out-dir", str(tmp_path / "s3"), "--expect-traffic"]) == 2
