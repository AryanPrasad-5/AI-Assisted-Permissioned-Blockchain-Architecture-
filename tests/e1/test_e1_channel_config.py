"""Synthetic configtxlator-style JSON for the channel-config verifier. Not a real decoded block."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_channel_config as cc  # noqa: E402


def config(b=45, timeout="2s", p=10_485_760, absolute=103_809_024, consenters=3):
    orderer_values = {
        "BatchSize": {"mod_policy": "Admins", "value": {"max_message_count": b, "absolute_max_bytes": absolute,
                                                        "preferred_max_bytes": p}},
        "BatchTimeout": {"mod_policy": "Admins", "value": {"timeout": timeout}},
        "ConsensusType": {"mod_policy": "Admins", "value": {
            "type": "etcdraft", "state": "STATE_NORMAL",
            "metadata": {"consenters": [{"host": f"orderer{i}.example.com", "port": 7050} for i in range(consenters)],
                         "options": {"tick_interval": "500ms"}}}},
    }
    cfg = {"channel_group": {"groups": {"Orderer": {"values": orderer_values}}}}
    return {"data": {"data": [{"payload": {"data": {"config": cfg}}}]}}


@pytest.mark.parametrize("text,ms", [("2s", 2000), ("500ms", 500), ("1.5s", 1500), ("1m30s", 90_000), ("250us", 0.25)])
def test_go_durations(text, ms):
    assert cc.duration_ms(text) == pytest.approx(ms)


@pytest.mark.parametrize("bad", ["2", "2 s", "s2", "2sec", ""])
def test_bad_durations_rejected(bad):
    with pytest.raises(ValueError):
        cc.duration_ms(bad)


def test_three_e1_cells_match_whole_block_and_config_only():
    for b, tau, p in [(40, "2s", 10_485_760), (45, "2s", 10_485_760), (500, "4s", 524_288)]:
        doc = config(b=b, timeout=tau, p=p)
        for d in (doc, doc["data"]["data"][0]["payload"]["data"]["config"]):
            rows = cc.verify(cc.orderer_values(d), max_message_count=b, batch_timeout=tau, preferred_max_bytes=p,
                             consensus="etcdraft", consenters=3)
            assert all(r[3] for r in rows), rows


def test_512_kb_means_524288_bytes():
    rows = cc.verify(cc.orderer_values(config(p=524_288)), max_message_count=45, batch_timeout="2s",
                     preferred_max_bytes=512 * 1024)
    assert all(r[3] for r in rows)


def test_mismatches_are_reported(tmp_path):
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(config(b=10, timeout="2.5s")), encoding="utf-8")
    assert cc.main([str(path), "--max-message-count", "45", "--batch-timeout", "2s",
                    "--preferred-max-bytes", "10485760"]) == 1
    assert cc.main([str(path), "--max-message-count", "10", "--batch-timeout", "2500ms",
                    "--preferred-max-bytes", "10485760", "--consenters", "1"]) == 1
    assert cc.main([str(path), "--max-message-count", "10", "--batch-timeout", "2500ms",
                    "--preferred-max-bytes", "10485760", "--consenters", "3"]) == 0


def test_missing_field_is_unreadable(tmp_path):
    doc = config()
    del doc["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchTimeout"]
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert cc.main([str(path), "--max-message-count", "45", "--batch-timeout", "2s",
                    "--preferred-max-bytes", "10485760"]) == 2
