"""X1 stage, owner-authorization gate, whole runs through the UNCHANGED runner against the fake world, derived verifier and non-interference
with the frozen X0 / cl01-cl12 package. No process, no Fabric."""
import hashlib
import json
import runpy
import sys
from pathlib import Path

import pytest

from Fabric_Live_Testbed.closed_loop import runner as v1_runner
from Fabric_Live_Testbed.closed_loop.stage_verify import verify_stage_tree
from Fabric_Live_Testbed.closed_loop.tests import test_runner_fake as fake
from Fabric_Live_Testbed.closed_loop.tests.test_runner_fake import FakeWorld, auth_file
from Fabric_Live_Testbed.closed_loop_v2 import runner_v2 as rn
from Fabric_Live_Testbed.closed_loop_x1 import decide_x1 as D
from Fabric_Live_Testbed.closed_loop_x1 import derive_verify, protocol_x1 as X, stage_x1 as ST
from Fabric_Live_Testbed.closed_loop_x1.tests.worlds import RANKED

PKG = Path(__file__).resolve().parents[1]
GATE = PKG / "ops" / "check_auth_x1.py"
V2 = PKG.parent / "closed_loop_v2"


@pytest.fixture(scope="module")
def x1(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setattr(X, "RUNG_RATES_TPS", (10, 20, 40))
    root = tmp_path_factory.mktemp("x1") / "s"
    m = ST.stage_x1(root, protocol_sha256="f" * 64, source_freeze_sha256="e" * 64)
    mp.undo()
    return root, m


# ------------------------------------------------------------------------------------------------ run order
def test_run_order_is_deterministic_block_randomized_and_never_repeats_a_configuration_back_to_back():
    s = ST.run_specs()
    assert s == ST.run_specs() and [x["pos"] for x in s] == list(range(9))
    for rep in (1, 2, 3):
        blk = s[3 * (rep - 1):3 * rep]
        assert sorted(x["B"] for x in blk) == [2, 5, 10] and {x["rep"] for x in blk} == {rep}   # any prefix of 6 = 2 replicates of each
    assert all(a["B"] != b["B"] for a, b in zip(s, s[1:]))
    assert all(sorted(x["rung_order"]) == sorted(X.RUNG_RATES_TPS) for x in s)
    assert len({tuple(x["rung_order"]) for x in s}) == 9                      # every run has its own rung order
    assert ST.run_specs(seed=1) != s


def test_no_configuration_runs_twice_in_a_row_for_any_seed():
    for seed in range(60):
        s = ST.run_specs(seed=seed)
        assert all(a["B"] != b["B"] for a, b in zip(s, s[1:])), seed


# ------------------------------------------------------------------------------------------------ stage
def test_stage_is_hashed_pins_protocol_and_source_freeze_and_has_the_nine_runs(x1):
    root, m = x1
    assert verify_stage_tree(root) == m["stage_sha256"]
    assert m["kind"] == "X1" and m["protocol_sha256"] == "f" * 64 and m["source_freeze_sha256"] == "e" * 64
    assert [r["run_id"] for r in m["runs"]] == [f"cb0{i}" for i in range(1, 10)] and [r["channel"] for r in m["runs"]] == [f"e1-cb0{i}" for i in range(1, 10)]
    assert sorted((r["B"], r["replicate"]) for r in m["runs"]) == [(b, k) for b in (2, 5, 10) for k in (1, 2, 3)]
    arm = json.loads((root / "cb01" / "arm.json").read_text())
    assert arm["arm"] == "static" and arm["menu_B"] == [2, 5, 10] and arm["initial_B"] in (2, 5, 10) and arm["decision_times_utc"] == []
    assert arm["batch_timeout"] == "2s" and arm["protocol"] == X.PROTOCOL_ID and arm["rounds"][0]["n_tx"] == 20 * 60
    assert [r["n_tx"] / 60 for r in arm["rounds"][1:]] == [float(v) for v in m["runs"][0]["rung_order_tps"]]
    assert set(arm["x0"]["rate_of_round"].values()) == {10.0, 20.0, 40.0} and arm["x1"]["B"] == arm["initial_B"]
    with pytest.raises(FileExistsError):
        ST.stage_x1(root)


def test_full_stage_uses_the_frozen_rungs_and_transaction_budget(tmp_path):
    m = ST.stage_x1(tmp_path / "s")
    assert m["rungs_tps"] == list(X.RUNG_RATES_TPS) and all(r["n_tx"] == 20 * 60 + 60 * sum(X.RUNG_RATES_TPS) for r in m["runs"])
    assert sum(r["n_tx"] for r in m["runs"]) == 9 * 18600


# ------------------------------------------------------------------------------------------------ authorization gate
def gate(monkeypatch, capsys, auth, stage):
    monkeypatch.setattr(sys, "argv", ["check_auth_x1.py", str(auth), str(stage)])
    with pytest.raises(SystemExit) as e:
        runpy.run_path(str(GATE), run_name="__main__")
    return e.value.code, capsys.readouterr().out


def write_auth(tmp_path, root, m, **over):
    runs = over.pop("runs", ",".join(r["run_id"] for r in m["runs"])).split(",")
    chans = over.pop("broadcast_channels", ",".join(r["channel"] for r in m["runs"])).split(",")
    return auth_file(tmp_path, over.pop("stage_sha256", hashlib.sha256((root / "SHA256SUMS.txt").read_bytes()).hexdigest()), runs, chans,
                     **{"max_broadcasts_per_run": "0", "native_verification": "required", "broadcast_targets": "10", **over})


def test_exact_authorization_is_accepted(x1, tmp_path, monkeypatch, capsys):
    root, m = x1
    code, out = gate(monkeypatch, capsys, write_auth(tmp_path, root, m), root)
    assert code == 0 and "X1_AUTH ACCEPTED" in out


@pytest.mark.parametrize("over,needle", [
    ({"max_broadcasts_per_run": "1"}, "max_broadcasts_per_run=0"),
    ({"broadcast_targets": "10,40"}, "broadcast_targets must be 10"),
    ({"native_verification": "waived"}, "native_verification must be required"),
    ({"expires_utc": "2020-01-01T00:00:00Z"}, "REFUSED"),
    ({"stage_sha256": "0" * 64}, "stage SHA256SUMS"),
    ({"runs": "cb01,cb02,cb03"}, "authorized runs"),
    ({"runs": "cb01,cb02,cb03,cb04,cb05,cb06,cb07,cb08,cb09,cx01"}, "authorized runs"),
    ({"broadcast_channels": "e1-cb01,e1-cb02"}, "channels differ"),
])
def test_wrong_authorizations_are_refused(x1, tmp_path, monkeypatch, capsys, over, needle):
    root, m = x1
    code, out = gate(monkeypatch, capsys, write_auth(tmp_path, root, m, **over), root)
    assert code == 1 and needle in out


def test_an_authorization_naming_earlier_experiment_runs_is_refused_even_if_the_stage_were_edited(x1, tmp_path, monkeypatch, capsys):
    root, m = x1
    bad = {**m, "runs": [{**r, "run_id": "cx01"} if i == 0 else r for i, r in enumerate(m["runs"])]}
    stage = tmp_path / "evil"
    stage.mkdir()
    (stage / "STAGE_MANIFEST.json").write_text(json.dumps(bad))
    (stage / "SHA256SUMS.txt").write_bytes((root / "SHA256SUMS.txt").read_bytes())
    code, out = gate(monkeypatch, capsys, write_auth(tmp_path, stage, bad), stage)
    assert code == 1 and "earlier experiment" in out


def test_x0_stage_kind_is_refused(x1, tmp_path, monkeypatch, capsys):
    root, m = x1
    stage = tmp_path / "other"
    stage.mkdir()
    (stage / "STAGE_MANIFEST.json").write_text(json.dumps({**m, "kind": "X0"}))
    (stage / "SHA256SUMS.txt").write_bytes((root / "SHA256SUMS.txt").read_bytes())
    code, out = gate(monkeypatch, capsys, write_auth(tmp_path, stage, m), stage)
    assert code == 1 and "is not X1" in out


def test_absent_authorization_file_is_refused_cleanly(x1, tmp_path, monkeypatch, capsys):
    code, out = gate(monkeypatch, capsys, tmp_path / "AUTH_X1.txt", x1[0])
    assert code == 1 and "owner authorization file absent" in out


# ------------------------------------------------------------------------------------------------ whole runs through the unchanged runner
class WorldX1(FakeWorld):
    def __init__(self, ctx, arm, **kw):
        super().__init__(ctx, arm, **kw)
        self.B = int(arm["initial_B"])
        self.events = [("config", self.B)]


def test_all_nine_runs_execute_through_the_unchanged_runner_and_feed_the_decision(x1, tmp_path, monkeypatch):
    root, m = x1
    monkeypatch.setitem(fake.LAT, 2, 200)
    monkeypatch.setitem(fake.LAT, 5, 300)
    auth = write_auth(tmp_path, root, m)
    evidence = tmp_path / "evidence"
    rows = []
    for run in m["runs"]:
        wd = tmp_path / run["run_id"]
        wd.mkdir()
        ctx = v1_runner.Ctx(root, run["run_id"], auth, wd / "out", Path("/net.json"), wd / "cws", Path("/msp"), python="python3")
        arm = json.loads((root / run["run_id"] / "arm.json").read_text())
        world = WorldX1(ctx, arm)
        res = rn.execute(ctx, world)
        assert res["verdict"] == "VALID", (run["run_id"], res.get("step_failure"))
        assert not any("apply" in c for c in world.calls)                      # static: no actuation at all
        assert json.loads((ctx.rd / "analysis.json").read_text())["configuration_regimes"] == [{"from_after_block": 0, "max_message_count": run["B"]}]
        dst = evidence / run["run_id"] / run["run_id"]
        dst.mkdir(parents=True)
        (dst / "analysis.json").write_bytes((ctx.rd / "analysis.json").read_bytes())
    rows = D.load_rows(root, evidence)
    assert len(rows) == 9 * 3 and {r["B"] for r in rows} == {2, 5, 10} and all(X.classify_round(r) == "OK" for r in rows)
    d = X.decide(rows, RANKED)
    # the fake plant has constant latency per B (200/300/520 ms) and never saturates: B=2 is decisively best at every rung
    assert d["rungs"]["r_max"] == 40.0 and d["rungs"]["regime_configs"] == [2]
    assert d["status"] == "STOP_NO_HEADROOM" and d["reason"] == "NO_ORDERING_REVERSAL"
    out = tmp_path / "decision"
    D.write_outputs(d, out)
    assert (out / "x1_decision.json").exists() and (out / "rung_table.csv").read_text().splitlines()[0].startswith("B,rate_tps")
    with pytest.raises(FileExistsError):
        D.write_outputs(d, out)


def test_missing_run_evidence_contributes_no_rows(x1, tmp_path):
    assert D.load_rows(x1[0], tmp_path) == []


def test_lpc_ranking_file_must_match_its_pinned_hash(tmp_path):
    p = tmp_path / "p.json"
    p.write_text(json.dumps({"window_ranking": []}))
    with pytest.raises(ValueError):
        D.load_ranking(p)


LPC_JSON = Path("D:/PLAN_BLOCK18/live_predictive_control_2026-11-25/protocol/PROTOCOL_LPC_v1.json")


@pytest.mark.skipif(not LPC_JSON.exists(), reason="frozen LPC-v1 protocol file not on this machine")
def test_real_frozen_ranking_loads_and_has_the_14_eligible_windows():
    ranked, sha = D.load_ranking(LPC_JSON)
    assert sha == X.LPC_PROTOCOL_SHA256 and len(ranked) == 14 and ranked[0]["start_s"] == 20400 and ranked[1]["start_s"] == 25200


# ------------------------------------------------------------------------------------------------ verifier derivation and non-interference
def test_verify_x1_is_verify_v2_with_exactly_the_declared_substitutions():
    src = (V2 / "ops" / "verify_v2.py").read_bytes().decode("utf-8")
    assert (PKG / "ops" / "verify_x1.py").read_bytes().decode("utf-8") == derive_verify.derive(src)
    a, b = src.splitlines(), (PKG / "ops" / "verify_x1.py").read_text().splitlines()
    assert len(a) == len(b) and sum(1 for x, y in zip(a, b) if x != y) == len(derive_verify.SUBSTITUTIONS) == 4
    with pytest.raises(ValueError):
        derive_verify.derive(src + src)                                          # a substitution must match exactly once


def test_x1_scripts_have_lf_endings_and_no_baked_in_owner_values():
    for p in list((PKG / "ops").iterdir()) + list(PKG.glob("*.py")):
        raw = p.read_bytes()
        assert b"\r" not in raw, p
    for p in (PKG / "ops").glob("*.sh"):
        t = p.read_text()
        assert "approved_by=owner" not in t and "AUTH_X1.txt" in t or p.name != "launch_x1.sh"


FROZEN_V2 = Path("D:/PLAN_BLOCK18/live_predictive_control_2026-11-25/source_hashes_frozen_v2.txt")


@pytest.mark.skipif(not FROZEN_V2.exists(), reason="frozen v2 source list not on this machine")
def test_x1_does_not_alter_any_frozen_v1_or_v2_source():
    repo = PKG.parents[1]
    checked = 0
    for line in FROZEN_V2.read_text().splitlines():
        digest, rel = line.split("  ", 1)
        if rel.startswith(("Fabric_Live_Testbed/closed_loop/", "Fabric_Live_Testbed/closed_loop_v2/")):
            assert hashlib.sha256((repo / rel).read_bytes()).hexdigest() == digest, rel
            checked += 1
    assert checked >= 30


def test_launcher_gates_are_present_in_order():
    t = (PKG / "ops" / "launch_x1.sh").read_text()
    order = ['check_auth_x1.py" "$AUTH"', "GATE FAIL: previous run", "GATE FAIL: containers", "GATE FAIL: caliper not idle",
             "GATE FAIL: authorization expires", "GATE FAIL: source changed", "GATE FAIL: stage does not pin", 'echo "ALL GATES PASS"',
             "runner_v2 --stage", 'verify_x1.py" "$R"', 'continue_check_v2.py" "$R"']
    idx = [t.index(s) for s in order]
    assert idx == sorted(idx)
    assert 'cb0[1-9]' in t and t.count("--execute") == 1


def test_channel_requests_for_every_x1_run_validate_with_the_unchanged_channel_tool():
    """Offline construction only (no process): the channel tool accepts B=2 and B=5 and the run ids / channel names of X1."""
    import e1_channel_run as cr
    for b in X.CONFIGS:
        for i in range(1, 10):
            req = cr.Request(f"e1-cb{i:02d}", f"cb{i:02d}", b, "2s", 10_485_760)
            assert req.max_message_count == b
    with pytest.raises(cr.RequestError):
        cr.Request("e1-cb01", "cb01", 0, "2s", 10_485_760)


def test_frozen_actuator_would_refuse_a_b2_or_b5_broadcast_so_x1_authorizes_none():
    """Documents the constraint found while building X1: targets below 10 are outside the frozen actuator's approved range."""
    import e1_config_actuator as act
    assert (act.DEFAULT_MIN_B, act.DEFAULT_MAX_B) == (10, 800)


def test_go_decision_serializes_to_json_and_csv(tmp_path):
    from Fabric_Live_Testbed.closed_loop_x1.tests.worlds import world
    d = X.decide(world(), RANKED)
    assert d["status"] == "GO_LIVE_DESIGN"
    D.write_outputs(d, tmp_path / "o")
    back = json.loads((tmp_path / "o" / "x1_decision.json").read_text())
    assert back["status"] == "GO_LIVE_DESIGN" and back["menu"] == [2, 5] and back["selected_window"]["start_s"] == 25200


def test_whole_day_natural_minutes_are_read_from_the_arrival_array_and_reported_descriptively(tmp_path):
    import numpy as np
    arr = np.arange(0, 86_400_000, 100, dtype=np.int64)                  # 10 arrivals per second all day
    f = tmp_path / "a.npy"
    np.save(f, arr)
    rates = D.day_minute_rates(f)
    assert len(rates) == 1440 and all(abs(r - 10.0) < 1e-9 for r in rates)
    from Fabric_Live_Testbed.closed_loop_x1.tests.worlds import world
    d = X.decide(world(), RANKED, {"whole_day": rates})
    ev = d["descriptive_workloads"]["whole_day"]
    assert ev["coverage"] == 1.0 and ev["tuned_static_B"] == 2 and ev["headroom_oracle"] == pytest.approx(0.0, abs=1e-9)


@pytest.mark.skipif(not LPC_JSON.exists(), reason="frozen LPC-v1 protocol file not on this machine")
def test_decide_cli_with_no_evidence_reports_not_established(x1, tmp_path, capsys):
    out = tmp_path / "decision"
    assert D.main(["--stage", str(x1[0]), "--evidence", str(tmp_path), "--lpc-protocol", str(LPC_JSON), "--out", str(out)]) == 0
    doc = json.loads((out / "x1_decision.json").read_text())
    assert doc["status"] == "STOP_NOT_ESTABLISHED" and doc["inputs"]["runs_with_rows"] == [] and doc["inputs"]["lpc_protocol_sha256"] == X.LPC_PROTOCOL_SHA256
