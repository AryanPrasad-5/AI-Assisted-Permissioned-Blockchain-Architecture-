"""Offline tests of the replay renderer: it writes files, proves them against the window, and never executes anything."""

import json
import shutil

import pytest
import yaml

from p4_replay import offsets as of
from p4_replay import render as rd
from p4_replay.tests.conftest import ENV, NETWORK_PATH, PKG, START_S

NETWORK_DOC = rd.lr.network(ENV, PKG)     # the existing renderer's real network description; lets ro.Plan validate offline


def render(tmp_path, window, env, run_id="rp01", channel="e1-rp01-b10-r1", B=10, name="ws"):
    return rd.render_replay_run(env, NETWORK_PATH, run_id, channel, B, "2s", 10_485_760, window, tmp_path / f"{name}_plan.json",
                                ws_root=tmp_path / name, day_start_ms=1_791_763_200_000, network_doc=NETWORK_DOC)


def bench(result):
    return json.loads((result["dir"] / "benchmark.yaml").read_text(encoding="utf-8"))


def test_two_rounds_with_replay_rate_and_exact_counts(tmp_path, window, env):
    r = render(tmp_path, window, env)
    b = bench(r)
    pre, meas = b["test"]["rounds"]
    assert b["test"]["workers"] == {"number": 1}
    assert (pre["label"], meas["label"]) == ("wu-rp01", "m-rp01")                  # the P1b warm-up/measured slots: recorder naming unchanged
    for rnd, offs in ((pre, window.pre_ms), (meas, window.measured_ms)):
        assert rnd["rateControl"]["type"] == "replay-rate"                          # never fixed-rate: no uniform distribution of counts
        assert rnd["rateControl"]["opts"]["inputFormat"] == "TEXT"
        assert rnd["txNumber"] == len(offs)                                         # scheduled count == txNumber
        assert rnd["workload"]["arguments"]["scheduleMs"] == offs.tolist()          # absolute ms offsets, ties and all
        assert rnd["workload"]["arguments"]["rateControlled"] is True
        assert rnd["workload"]["module"].endswith("/modules/e1_trace_replay.js")
    assert "fixed-rate" not in (r["dir"] / "benchmark.yaml").read_text(encoding="utf-8")


def test_schedule_files_equal_the_window_and_arguments(tmp_path, window, env):
    r = render(tmp_path, window, env)
    for name, offs in (("pre_roll", window.pre_ms), ("measured", window.measured_ms)):
        body = (r["dir"] / f"schedule_{name}.txt").read_bytes()
        assert b"\r" not in body and not body.endswith(b"\n")
        assert [int(x) for x in body.decode("ascii").split("\n")] == offs.tolist()
        assert r["record"]["schedules"][name]["sha256"] == of.sha256_hex(body)
    assert r["record"]["verified"]["measured"]["ok"] and r["record"]["verified"]["pre_roll"]["ok"]


def test_benchmark_is_valid_json_and_yaml(tmp_path, window, env):
    r = render(tmp_path, window, env)
    text = (r["dir"] / "benchmark.yaml").read_text(encoding="utf-8")
    assert yaml.safe_load(text) == json.loads(text)                                 # parses as YAML and as JSON, identically
    for name in ("networkconfig.yaml", "ccp-org1.json"):
        assert json.loads((r["dir"] / name).read_text(encoding="utf-8"))


def test_plan_validates_and_names_the_two_rounds(tmp_path, window, env):
    r = render(tmp_path, window, env)
    cal = r["plan"]["caliper"]
    assert (cal["warmup_round"], cal["measured_round"]) == (0, 1)
    assert (cal["warmup_label"], cal["measured_label"]) == ("wu-rp01", "m-rp01")
    assert r["plan"]["cell"]["mode"] == "txNumber" and r["plan"]["cell"]["n"] == window.measured_ms.size
    assert r["plan"]["expect"]["expect_block_pattern"] is False                     # the block-pattern expectation is for constant rates
    assert json.loads((tmp_path / "ws_plan.json").read_text(encoding="utf-8")) == r["plan"]


def test_render_record_is_complete_and_honest(tmp_path, window, env):
    rec = render(tmp_path, window, env)["record"]
    assert rec["status"] == "RENDERED_OFFLINE_NOT_EXECUTED"
    assert rec["window"] == {"id": "T_SYN", "day": "2026-10-12", "start_s": START_S, "length_s": 600, "pre_roll_s": 60, "schedule": "A"}
    assert any("LIVE_UNTESTED" in n for n in rec["notes"]) and any("settleSeconds" in n for n in rec["notes"])
    assert rec["identifiers"].startswith("NOT_LOADED")
    assert set(rec["modules"]) == {"e1_per_tx_recorder.js", "e1_workload_core.js", "e1_trace_replay.js"}
    assert "delivered" not in json.dumps(rec).lower().replace("delivered_status", "")      # offered is never confused with delivered


def test_module_copies_equal_the_repo(tmp_path, window, env):
    r = render(tmp_path, window, env)
    for name, info in r["record"]["modules"].items():
        assert (r["dir"] / "modules" / name).read_bytes() == (rd.REPO_ROOT / info["source"]).read_bytes()


def test_render_is_deterministic(tmp_path, window, env):
    a, b = render(tmp_path, window, env, name="a"), render(tmp_path, window, env, name="b")
    for f in ("benchmark.yaml", "networkconfig.yaml", "ccp-org1.json", "schedule_pre_roll.txt", "schedule_measured.txt", "tx_map.csv"):
        assert (a["dir"] / f).read_bytes() == (b["dir"] / f).read_bytes(), f


def test_render_refuses_to_overwrite(tmp_path, window, env):
    render(tmp_path, window, env)
    with pytest.raises(FileExistsError):
        render(tmp_path, window, env)                                               # same run id, same workspace


@pytest.mark.parametrize("run_id,channel", [("bad id!", "e1-rp01-b10-r1"), ("rp01", "not-e1"), ("rp01", "e1-" + "x" * 50)])
def test_bad_names_are_refused(tmp_path, window, env, run_id, channel):
    with pytest.raises(ValueError):
        render(tmp_path, window, env, run_id=run_id, channel=channel)


def test_tx_map_is_written_with_every_transaction(tmp_path, window, env):
    r = render(tmp_path, window, env)
    rows = of.read_tx_map(r["dir"] / "tx_map.csv")
    assert len(rows) == window.pre_ms.size + window.measured_ms.size
    assert "\r" not in (r["dir"] / "tx_map.csv").read_text(encoding="utf-8")
    assert rows[0].keys() == set(of.TX_MAP_HEADER) or list(rows[0].keys()) == of.TX_MAP_HEADER
    assert r["record"]["files"]["tx_map.csv"] == of.sha256_hex((r["dir"] / "tx_map.csv").read_bytes())


def test_a_tampered_render_is_caught_by_verify(tmp_path, window, env):
    r = render(tmp_path, window, env)
    path = r["dir"] / "schedule_measured.txt"
    path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n", 1))                  # CRLF sneaks in
    with pytest.raises(of.ScheduleError):
        rd.verify_rendered(r["dir"], window, "rp01")
