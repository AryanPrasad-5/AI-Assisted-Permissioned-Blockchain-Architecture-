"""Offline validation of rendered replay runs against Caliper v0.6.0's replay-rate semantics (SOURCE_READ), without starting Caliper.

What passing here means: the files satisfy the rules Caliper's TEXT parser and replay-rate controller are known to apply (source read on 2026-11-14, line numbers approximate),
and the verbatim parser expression, run under node, agrees with a Python transcription on the real rendered files.
What it does NOT mean: that Caliper accepted or timed them. Real replay-rate timing, the round turnover gap, the connector's submit accounting and Fabric under bursts are
CALIPER_LIVE_UNVALIDATED."""

import json
import shutil

import numpy as np
import pytest

from p4_replay import REPO_ROOT, caliper_check as cc, driver as dr, offsets as of, render as rd
from p4_replay.tests.conftest import ENV, NETWORK_PATH, PKG

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")
NETWORK_DOC = rd.lr.network(ENV, PKG)


def rendered(tmp_path, window, env, sub="ws"):
    return rd.render_replay_run(env, NETWORK_PATH, "rp01", "e1-rp01-b10-r1", 10, "2s", 10_485_760, window, tmp_path / f"plan_{sub}.json", ws_root=tmp_path / sub, network_doc=NETWORK_DOC)


# ----------------------------------------------------------------------------------------------- the parser transcription
@pytest.mark.parametrize("text,expected", [("12", 12), (" 7", 7), ("1.5", 1), ("12abc", 12), ("", None), ("abc", None), ("-3", -3), ("+4", 4), ("0x1f", 31), ("5\r", 5), ("\n", None)])
def test_js_parse_int_matches_javascript(text, expected):
    assert cc.js_parse_int(text) == expected


@needs_node
def test_the_python_transcription_agrees_with_the_verbatim_javascript_expression(tmp_path):
    bodies = [b"0\n5\n5\n1204\n599724", b"0\r\n5\r\n7", b"0\n5\n", b"0\n\n5", b"1.5\n2.9\n3abc", b"0x10\n20", b"\n"]
    for i, body in enumerate(bodies):
        path = tmp_path / f"s{i}.txt"
        path.write_bytes(body)
        assert cc.node_parse_records(path, NODE) == cc.parse_text_records(body), body


def test_text_parse_facts_that_matter_for_the_schedule_files():
    assert cc.parse_text_records(b"0\n5\n9") == [0, 5, 9]
    assert cc.parse_text_records(b"0\n5\n9\n")[-1] is None                                  # a trailing newline adds a NaN record (never reached while txNumber equals the count)
    assert cc.parse_text_records(b"0\n\n9")[1] is None                                      # an empty line inside is NaN: dispatched with NO sleep, i.e. at the wrong time
    assert cc.parse_text_records(b"0\r\n5\r\n9") == [0, 5, 9]                              # CRLF is harmless (the legacy P1-P5 form)


def test_replay_rate_options_written_by_the_renderer_are_only_keys_caliper_reads():
    assert set(rd.REPLAY_RATE_OPTS) | {"pathTemplate"} <= cc.CALIPER_OPTION_KEYS_READ and "logChunkSize" not in rd.REPLAY_RATE_OPTS


# ----------------------------------------------------------------------------------------------- the controller delay rule
def test_ideal_controller_is_never_late_and_early_by_at_most_5_ms():
    offsets = [0, 0, 5, 1000, 1004, 1004, 1009, 1100, 1103, 1106, 1109, 5000, 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010, 5011]
    t = cc.model_dispatch_times(offsets)
    err = t - np.array(offsets)
    assert err.max() <= 0 and -cc.SKIP_SLEEP_AT_OR_BELOW_MS <= err.min() < 0                # early by at most 5 ms, and really early where a short sleep is skipped
    #   (derived from replayRate.js:135, not measured on a running Caliper)


def test_a_slow_client_is_never_compensated_by_the_controller():
    t = cc.model_dispatch_times([1000] * 200, call_cost_ms=2.0)                             # 200 arrivals due at the same instant, 2 ms of client work each
    assert t[0] == 1000.0 and t[-1] == pytest.approx(1000.0 + 199 * 2.0)                    # no catch-up: lateness accumulates (replayRate.js has none)


def test_facts_are_labelled_and_the_unread_ones_are_named():
    assert {f["status"] for f in cc.FACTS} == {"SOURCE_READ", "UNREAD"}
    assert {f["id"] for f in cc.FACTS if f["status"] == "UNREAD"} == {"U1", "U2", "U3"}
    assert all(f["source"] and f["consequence"] for f in cc.FACTS) and cc.CALIPER_STATUS == "CALIPER_LIVE_UNVALIDATED"


# ----------------------------------------------------------------------------------------------- round-completion and timeout semantics
def test_round_semantics_say_the_600_s_is_not_a_caliper_timer():
    s = cc.round_semantics(599_724, 59_999)
    assert s["ms_between_last_offset_and_window_end"] == 276 and s["offered_load_span_s"] == pytest.approx(599.724)
    assert "no 600 s timer" in s["the_600_s_is"] and "no timeout" in s["caliper_round_ends_when"] and "NOT exactly 600 s" in s["observation_period"]
    assert "INTENDED" in s["analysis_cohort_rule"] and "none for queue state" in s["pre_roll_state_carryover"]


def test_run_length_budget_for_the_proposed_limits_and_for_p1b_defaults():
    ok = cc.run_length_budget(59_999, 599_724, 1200.0, 1500.0)
    assert ok["ok"] and ok["timeout_margin_s"] > 300 and ok["checks"]["p1b_defaults_would_abort"] is True
    assert not cc.run_length_budget(59_999, 599_724, 300.0, 420.0)["ok"]                    # P1b's 300 s and 420 s would abort every replay
    assert not cc.run_length_budget(59_999, 599_724, 1200.0, 1250.0)["ok"]                  # the sampler guard must outlive the Caliper timeout by 120 s
    assert not cc.run_length_budget(59_999, 599_724, 700.0, 1500.0)["ok"]                   # below the expected wall time
    assert not cc.run_length_budget(59_999, 599_724, 1200.0, 1500.0, sampler_margin_s=400.0)["ok"]


def test_keep_awake_duration_comes_from_the_budget_not_the_p1b_literal():
    assert cc.keep_awake_seconds(7, 1200.0) == 7 * (1200 + 900) + 1200
    expected = cc.expected_caliper_seconds(59_999, 599_724)
    assert 540 * 7 + 1200 < 7 * expected                                                    # P1b's formula would end before seven replays could finish
    assert cc.keep_awake_seconds(7, 1200.0) > 7 * expected


# ----------------------------------------------------------------------------------------------- a rendered run folder
def test_a_rendered_run_passes_every_offline_check_and_is_still_caliper_live_unvalidated(tmp_path, window, env):
    r = rendered(tmp_path, window, env)
    res = cc.validate_rendered_run(r["dir"], use_node=NODE is not None)
    assert res["ok"], res["failed"]
    ids = {c["id"] for c in res["checks"]}
    assert {"V1", "V3", "V6", "V14", "V15", "V2@pre_roll", "V2@measured", "V4@measured", "V5@measured", "V7@measured", "V8a@measured", "V8b@measured", "V8c@pre_roll",
            "V9@measured", "V10@measured", "V11@measured", "V12@measured", "V13@measured"} <= ids
    assert res["caliper_status"] == "CALIPER_LIVE_UNVALIDATED" and {"U1", "U2", "U3"} <= set(res["not_validated"]) and "real replay-rate timing" in res["not_validated"]
    if NODE:
        assert any(c["id"] == "V4n@measured" and c["ok"] for c in res["checks"])


def mutate(tmp_path, window, env, fn):
    r = rendered(tmp_path, window, env, "orig")
    mut = tmp_path / "mut"
    shutil.copytree(r["dir"], mut)
    fn(mut)
    return mut


def edit_bench(path, fn):
    p = path / "benchmark.yaml"
    doc = json.loads(p.read_text(encoding="utf-8"))
    fn(doc)
    p.write_text(json.dumps(doc), encoding="utf-8")


def edit_txmap(path, fn):
    import csv
    p = path / "tx_map.csv"
    with p.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    fn(rows)
    with p.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=of.TX_MAP_HEADER, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def sched(path, name):
    return path / f"schedule_{name}.txt"


MUTANTS = {
    "trailing_newline": (lambda p: sched(p, "measured").write_bytes(sched(p, "measured").read_bytes() + b"\n"), {"V5@measured"}),
    "crlf": (lambda p: sched(p, "measured").write_bytes(sched(p, "measured").read_bytes().replace(b"\n", b"\r\n")), {"V5@measured"}),
    "blank_line_inside": (lambda p: sched(p, "measured").write_bytes(sched(p, "measured").read_bytes().replace(b"\n", b"\n\n", 3)), {"V4@measured"}),
    "txnumber_plus_one": (lambda p: edit_bench(p, lambda d: d["test"]["rounds"][1].update(txNumber=d["test"]["rounds"][1]["txNumber"] + 1)), {"V4@measured", "V5@measured", "V7@measured"}),
    "file_differs_from_args": (lambda p: sched(p, "measured").write_bytes(sched(p, "measured").read_bytes().replace(b"1204", b"1205", 1)), {"V8a@measured", "V8b@measured"}),
    "txmap_offset_changed": (lambda p: edit_txmap(p, lambda rows: rows[10].update(intended_offset_ms="1")), {"V10@measured", "V12@measured", "V8b@measured", "V8c@measured", "V9@measured"}),
    "round_placeholder": (lambda p: edit_bench(p, lambda d: d["test"]["rounds"][1]["rateControl"]["opts"].update(pathTemplate="/x/<R>/schedule_measured.txt")), {"V2@measured"}),
    "unread_option_key": (lambda p: edit_bench(p, lambda d: d["test"]["rounds"][0]["rateControl"]["opts"].update(logChunkSize=100)), {"V2@pre_roll"}),
    "fixed_rate_controller": (lambda p: edit_bench(p, lambda d: d["test"]["rounds"][1]["rateControl"].update(type="fixed-rate")), {"V2@measured"}),
    "two_workers": (lambda p: edit_bench(p, lambda d: d["test"]["workers"].update(number=2)), {"V3"}),
    "wrong_label": (lambda p: edit_bench(p, lambda d: d["test"]["rounds"][0].update(label="m-rp01")), {"V1"}),
    "not_create_asset_mode": (lambda p: edit_bench(p, lambda d: d["test"]["rounds"][1]["workload"]["arguments"].update(mode="rmw")), {"V13@measured"}),
    "wrong_key": (lambda p: edit_txmap(p, lambda rows: rows[3].update(create_asset_key="rp01_1_9999999")), {"V7@pre_roll"}),
    "tie_broken_in_source": (lambda p: edit_txmap(p, lambda rows: [r.update(day_offset_ms=str(int(r["day_offset_ms"]) + 1)) for r in rows if r["round"] == "measured" and r["seq"] == "1"]), {"V9@measured"}),
    "empty_second_filled": (lambda p: edit_txmap(p, lambda rows: [r.update(window_offset_ms="150000") for r in rows if r["round"] == "measured" and r["seq"] == "30"]), {"V11@measured", "V12@measured", "V8c@measured"}),
    "preroll_equals_measured_file": (lambda p: sched(p, "pre_roll").write_bytes(sched(p, "measured").read_bytes()), {"V15", "V5@pre_roll", "V6", "V8a@pre_roll", "V8b@pre_roll"}),
}


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_every_mutation_of_a_rendered_run_is_detected_by_its_named_check(tmp_path, window, env, name):
    fn, expected = MUTANTS[name]
    res = cc.validate_rendered_run(mutate(tmp_path, window, env, fn), use_node=False)
    assert not res["ok"] and set(res["failed"]) == expected, (name, res["failed"])           # EXACTLY the named checks: no more, no fewer


def test_an_offset_at_the_end_of_the_window_is_detected(tmp_path, window, env):
    def push(p):
        body = sched(p, "measured").read_bytes().decode().split("\n")
        body[-1] = "600000"
        sched(p, "measured").write_bytes("\n".join(body).encode())
        edit_bench(p, lambda d: d["test"]["rounds"][1]["workload"]["arguments"].update(scheduleMs=[int(x) for x in body]))
    res = cc.validate_rendered_run(mutate(tmp_path, window, env, push), use_node=False)
    assert {"V6", "V14"} <= set(res["failed"])                                              # the window is half-open: 600,000 ms is outside it


@pytest.mark.parametrize("timeout,guard", [(600.0, 1500.0), (1200.0, 1250.0), (300.0, 420.0)])
def test_a_timeout_or_guard_that_does_not_fit_the_replay_fails_v15(tmp_path, window, env, timeout, guard):
    r = rendered(tmp_path, window, env)
    res = cc.validate_rendered_run(r["dir"], caliper_timeout_s=timeout, host_max_seconds=guard, use_node=False)
    assert "V15" in res["failed"] and res["budget"]["ok"] is False


# ----------------------------------------------------------------------------------------------- the real registered windows (read-only; skipped if absent)
REAL = REPO_ROOT / "data" / "processed" / "flashbots_ms"


@pytest.mark.skipif(not (REAL / "flashbots_ms_manifest.json").exists(), reason="derived Flashbots trace not present")
@pytest.mark.parametrize("wid,n_tx,pre_n", [("F_82800", 6229, 548), ("D_33000", 6887, 603)])
def test_the_real_windows_render_to_files_that_pass_every_offline_caliper_check(tmp_path, wid, n_tx, pre_n):
    window, lineage = of.load_registered_window(REAL, wid)
    r = rd.render_replay_run(dict(ENV), NETWORK_PATH, "rp01", "e1-rp01-b10-r1", 10, "2s", 10_485_760, window, tmp_path / "plan.json", ws_root=tmp_path / "ws", network_doc=NETWORK_DOC,
                             lineage=lineage)
    res = cc.validate_rendered_run(r["dir"], use_node=NODE is not None)
    assert res["ok"], [c for c in res["checks"] if not c["ok"]]
    assert lineage["equal_to_p1_p5_schedule"] and window.measured_ms.size == n_tx and window.pre_ms.size == pre_n
    measured = next(c for c in res["checks"] if c["id"] == "V12@measured")
    if wid == "F_82800":
        assert measured["observed"]["peak_per_second"] == 98 and res["semantics"]["last_scheduled_offset_ms"] == 599_724           # 98 tx/s peak; last arrival ~599.724 s
        assert res["semantics"]["ms_between_last_offset_and_window_end"] == 276
    assert res["caliper_status"] == "CALIPER_LIVE_UNVALIDATED"
