"""Raft-leader wait in the channel-per-run tool (added after E0-0 check 6a, 2026-11-09: the approve broadcast was
sent 0.4 s after the joins and rejected 'no Raft leader'; the leader was elected 0.7 s later). Synthetic probes only."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_channel_run as cr  # noqa: E402
from test_e1_channel_run import FakeFabric, execute, labels, make_network  # noqa: E402

# the real metric lines seen on orderer2 in E0-0 (2026-11-09)
REAL = ('# TYPE consensus_etcdraft_is_leader gauge\n'
        'consensus_etcdraft_is_leader{channel="e1-e00a"} 1\n'
        'consensus_etcdraft_leader_changes{channel="e1-e00a"} 1\n')


def leader_net(tmp, **extra):
    net = make_network(tmp / "net", expect={"consensus": "etcdraft", "consenters": 3, "raft_leader_wait": True},
                       leader_attempts=5, leader_sleep_s=1.0, **extra)
    for i, o in enumerate(net["orderers"]):
        o["ops_url"] = f"http://localhost:{9443 + i}/metrics"
    return net


class Probe:
    """Returns the given per-call answers (dicts orderer -> value), the last one repeated."""

    def __init__(self, *answers):
        self.answers, self.calls = list(answers), 0

    def __call__(self, net, channel):
        self.calls += 1
        return self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]


NONE = {"orderer0": 0.0, "orderer1": 0.0, "orderer2": 0.0}
ONE = {"orderer0": 0.0, "orderer1": 1.0, "orderer2": 0.0}


def test_waits_for_exactly_one_leader_before_approving(tmp_path):
    probe = Probe(NONE, {"orderer0": None, "orderer1": 0.0, "orderer2": 0.0}, ONE)
    rec, fake, sleeps = execute(tmp_path, net=leader_net(tmp_path), probe=probe)
    assert rec["status"] == "VERIFIED" and probe.calls == 3
    assert rec["raft_leader"] == {"leader": "orderer1", "values": ONE, "attempt": 3}
    lab = labels(rec)
    assert lab.index("raft_leader") < lab.index("approve:Org1MSP")
    assert lab.index("raft_leader") > max(i for i, x in enumerate(lab) if x.startswith("osnadmin_list"))
    assert not any(a[0] == "internal:raft_leader" for a, _ in fake.calls)    # never sent to a CLI


@pytest.mark.parametrize("bad", [NONE, {"orderer0": 1.0, "orderer1": 1.0, "orderer2": 0.0},
                                 {"orderer0": 1.0, "orderer1": "error: refused", "orderer2": 0.0},
                                 {"orderer0": 1.0, "orderer1": None, "orderer2": 0.0}])
def test_no_single_leader_stops_before_any_approval(tmp_path, bad):
    rec, fake, sleeps = execute(tmp_path, net=leader_net(tmp_path), probe=Probe(bad))
    assert rec["status"] == "INCOMPLETE_SETUP" and rec["failed_step"] == "raft_leader"
    assert not any(a[1:4] == ["lifecycle", "chaincode", "approveformyorg"] for a, _ in fake.calls)
    assert sum(1 for s in rec["steps"] if s["label"] == "raft_leader") == 5 and sleeps.count(1.0) >= 4


def test_flag_requires_ops_urls_and_is_off_by_default(tmp_path):
    net = leader_net(tmp_path)
    del net["orderers"][2]["ops_url"]
    with pytest.raises(cr.RequestError, match="ops_url"):
        cr.Network(net)
    rec, _, _ = execute(tmp_path / "plain")                     # no flag: unchanged behaviour, no leader step
    assert "raft_leader" not in labels(rec) and rec["status"] == "VERIFIED"


def test_leader_probe_parses_the_real_metric_format(tmp_path):
    net = cr.Network(leader_net(tmp_path))
    pages = {"http://localhost:9443/metrics": REAL.replace("} 1\nconsensus_etcdraft_leader", "} 0\nconsensus_etcdraft_leader"),
             "http://localhost:9444/metrics": REAL,
             "http://localhost:9445/metrics": "# nothing for this channel\n"}

    def fetch(url):
        if url not in pages:
            raise OSError("refused")
        return pages[url]
    assert cr.leader_probe(net, "e1-e00a", fetch) == {"orderer0": 0.0, "orderer1": 1.0, "orderer2": None}
    assert cr.leader_probe(net, "e1-other", fetch) == {"orderer0": None, "orderer1": None, "orderer2": None}
    pages.pop("http://localhost:9445/metrics")
    assert cr.leader_probe(net, "e1-e00a", fetch)["orderer2"].startswith("error:")
