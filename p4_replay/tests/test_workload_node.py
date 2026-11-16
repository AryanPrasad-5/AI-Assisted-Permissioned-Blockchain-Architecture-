"""Caliper workload validation WITHOUT Caliper: the real e1_trace_replay.js and e1_workload_core.js are exercised under node with a fake
Caliper base class, a virtual clock and an in-memory filesystem, using the exact round arguments the renderer wrote.

What this proves: the JavaScript parses, accepts the rendered arguments, rejects tampered ones, maps every scheduled arrival to one
CreateAsset with the key the Python side predicts, logs intended and actual dispatch time, and does not turn a trailing callback
into a submission. What it does NOT prove: Caliper's own parsing of the schedule file, replay-rate timing, or anything on Fabric (LIVE_UNTESTED).
"""

import csv
import io
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from p4_replay import REPO_ROOT, offsets as of, render as rd
from p4_replay.tests.conftest import ENV, NETWORK_PATH, PKG

HARNESS = Path(__file__).parent / "node" / "harness.js"
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node is not installed")
NETWORK_DOC = rd.lr.network(ENV, PKG)


def rendered(tmp_path, window, env):
    return rd.render_replay_run(env, NETWORK_PATH, "rp01", "e1-rp01-b10-r1", 10, "2s", 10_485_760, window, tmp_path / "plan.json", ws_root=tmp_path / "ws",
                                network_doc=NETWORK_DOC)


def run_harness(tmp_path, spec: dict) -> dict:
    spec_path = tmp_path / f"spec_{abs(hash(json.dumps(spec, sort_keys=True))) % 10**8}.json"
    spec_path.write_text(json.dumps(spec), encoding="utf-8")
    proc = subprocess.run([NODE, str(HARNESS), str(REPO_ROOT), str(spec_path)], capture_output=True, text=True, timeout=120, check=False)
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def round_args(result, index):
    return json.loads((result["dir"] / "benchmark.yaml").read_text(encoding="utf-8"))["test"]["rounds"][index]["workload"]["arguments"]


@pytest.mark.parametrize("path", ["fabric-testbed/caliper/workloads/e1_trace_replay.js", "fabric-testbed/caliper/workloads/e1_workload_core.js",
                                  "scripts/e1/caliper/e1_per_tx_recorder.js"])
def test_workload_modules_parse(path):
    proc = subprocess.run([NODE, "--check", str(REPO_ROOT / path)], capture_output=True, text=True, timeout=60, check=False)
    assert proc.returncode == 0, proc.stderr


def test_rendered_copies_of_the_modules_parse(tmp_path, window, env):
    r = rendered(tmp_path, window, env)
    for f in (r["dir"] / "modules").glob("*.js"):
        assert subprocess.run([NODE, "--check", str(f)], capture_output=True, timeout=60, check=False).returncode == 0, f.name


@pytest.mark.parametrize("mode", ["controller", "self"])
@pytest.mark.parametrize("index,name", [(0, "pre_roll"), (1, "measured")])
def test_every_arrival_becomes_one_create_asset_with_the_predicted_key(tmp_path, window, env, mode, index, name):
    r = rendered(tmp_path, window, env)
    args = round_args(r, index)
    if mode == "self":
        args = {**args, "rateControlled": False}                                    # let the module's own gate sleep on the virtual clock
    out = run_harness(tmp_path, {"mode": mode, "roundIndex": index, "arguments": args})
    assert out["ok"], out["error"]
    tx_rows = [x for x in of.read_tx_map(r["dir"] / "tx_map.csv") if x["round"] == name]
    assert len(out["requests"]) == len(tx_rows) == len(args["scheduleMs"])          # nothing extra, nothing missing: the trailing callback was swallowed
    assert [q["args"][0] for q in out["requests"]] == [x["create_asset_key"] for x in tx_rows]
    assert all(q["fn"] == "CreateAsset" and q["readOnly"] is False and q["args"][1] == "v" * 128 for q in out["requests"])
    log = list(csv.DictReader(io.StringIO(out["dispatchLog"])))
    assert list(log[0]) == ["seq", "intended_offset_ms", "intended_ms", "dispatch_ms", "lag_ms"]
    assert [int(x["seq"]) for x in log] == list(range(len(tx_rows)))                # no duplicates, no gaps
    assert [int(x["intended_offset_ms"]) for x in log] == args["scheduleMs"]        # intended time is the schedule, millisecond for millisecond
    assert all(int(x["lag_ms"]) == 0 for x in log)                                  # perfect virtual scheduling: intended == actual
    overflow = list(csv.DictReader(io.StringIO(out["overflowLog"])))
    assert len(overflow) == 1 and int(overflow[0]["requested_seq"]) == len(tx_rows)   # the extra callback is logged, not submitted


def test_python_key_equals_javascript_key(tmp_path, window, env):
    r = rendered(tmp_path, window, env)
    args = round_args(r, 1)
    out = run_harness(tmp_path, {"mode": "controller", "roundIndex": 1, "arguments": args})
    assert [q["args"][0] for q in out["requests"][:5]] == [of.tx_key("rp01", 1, i) for i in range(5)]


@pytest.mark.parametrize("tamper,expect", [
    ({"scheduleMs": [5, 4, 9]}, "chronological"),                                   # decreasing offsets
    ({"scheduleMs": [-1, 4, 9]}, "non-negative"),                                   # negative offset
    ({"scheduleMs": []}, "non-empty"),
    ({"dispatchLog": "relative/dispatch.csv"}, "absolute"),                         # relative log path
    ({"runId": "bad id!"}, "bad runId"),
    ({"valueBytes": 0}, "valueBytes"),
])
def test_tampered_round_arguments_are_rejected(tmp_path, window, env, tamper, expect):
    r = rendered(tmp_path, window, env)
    out = run_harness(tmp_path, {"mode": "validate", "roundIndex": 1, "arguments": {**round_args(r, 1), **tamper}})
    assert not out["ok"] and expect in out["error"]


def test_two_workers_are_rejected(tmp_path, window, env):
    r = rendered(tmp_path, window, env)
    out = run_harness(tmp_path, {"mode": "validate", "roundIndex": 1, "totalWorkers": 2, "arguments": round_args(r, 1)})
    assert not out["ok"] and "exactly 1 worker" in out["error"]
