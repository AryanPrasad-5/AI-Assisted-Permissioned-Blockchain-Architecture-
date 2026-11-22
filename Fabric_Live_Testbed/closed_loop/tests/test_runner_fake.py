"""Whole closed-loop run against a fake world (no process, no network): stage -> runner -> control loop -> actuator -> ledger
-> analysis. The fake plant reproduces the measured batching behaviour qualitatively: mean latency ~520 ms at B=10 and
~1030 ms at B=40 for ~10 tx/s, so the uncalibrated R0 rule switches and R1 does not."""
import csv
import hashlib
import json
from pathlib import Path

import pytest

from Fabric_Live_Testbed.closed_loop import runner as rn
from Fabric_Live_Testbed.closed_loop.stage import stage
from Fabric_Live_Testbed.closed_loop.telemetry import round_base

CHANNEL_FIELDS = ["tx_id", "block", "index", "header_type", "validation_code", "env_bytes", "channel_id", "ch_ts_ms"]
LAT = {10: 520, 40: 1030}


def cfg(b):
    return {"max_message_count": b, "batch_timeout_ms": 2000, "preferred_max_bytes": 10485760, "absolute_max_bytes": 103809024, "orderers": 3}


class FakeCaliper:
    def __init__(self, world):
        self.w, self.next = world, 0

    def poll(self):
        rounds = self.w.arm["rounds"]
        if self.next < len(rounds):
            self.w.emit_round(rounds[self.next])
            self.next += 1
            return None
        return 0

    def kill(self):
        self.w.killed = True


class FakeWorld:
    def __init__(self, ctx, arm, *, fail_actuation=False, drop_round=None, stray_tx=False):
        self.ctx, self.arm, self.B, self.t = ctx, arm, 10, 1_800_000_000_000
        self.events, self.calls, self.killed = [("config", 10)], [], False
        self.fail_actuation, self.drop_round, self.stray_tx = fail_actuation, drop_round, stray_tx

    # clocks
    def now_ms(self):
        self.t += 7
        return self.t

    def monotonic(self):
        return self.t / 1000

    def sleep(self, s):
        self.t += int(s * 1000)

    def mem_available_mb(self):
        return 4000.0

    def disk_free_gb(self, p):
        return 50.0

    def spawn(self, argv, out, err):
        assert argv == self.arm["caliper_argv"]
        out.write_bytes(b"")
        err.write_bytes(b"")
        return FakeCaliper(self)

    def emit_round(self, r):
        if r["index"] == self.drop_round:
            return
        run_id = self.arm["run_id"]
        base = round_base(self.ctx.recorder_dir, run_id, r["index"], r["label"])
        base.parent.mkdir(parents=True, exist_ok=True)
        lines, disp = ["tx_id,seq_finish,create_ms,final_ms,status,verified,err,obs_hr_ns,worker,round,label"], ["seq,intended_offset_ms,intended_ms,dispatch_ms,lag_ms"]
        wall0 = self.t
        for seq in range(r["n_tx"]):
            c = wall0 + seq * 10
            tx = f"{run_id}-{r['index']}-{seq}"
            lines.append(f"{tx},{seq},{c},{c + LAT[self.B]},success,true,,0,0,{r['index']},{r['label']}")
            disp.append(f"{seq},{seq * 10},{c},{c + 1},1")
            self.events.append(("tx", tx))
        self.t = wall0 + r["n_tx"] * 10 + 2000
        body = ("\n".join(lines) + "\n").encode()
        Path(f"{base}.csv").write_bytes(body)
        Path(f"{base}.submits.csv").write_bytes(b"seq_submit,t_submit_evt_ms,count\n")
        Path(f"{base}.manifest.json").write_text(json.dumps({
            "round": r["index"], "label": r["label"], "files": {"csv_sha256": hashlib.sha256(body).hexdigest()},
            "counts": {"rows": r["n_tx"], "pending_at_deactivate": 0},
            "clock": {"wall0_ms": wall0, "wall1_ms": self.t, "wall_minus_monotonic_ms": 0.3}}))
        (self.ctx.run_ws / f"dispatch_{r['name']}.csv").write_text("\n".join(disp) + "\n")

    def build_ledger(self, out_dir: Path):
        (out_dir / "blocks").mkdir(parents=True)
        (out_dir / "ledger").mkdir()
        rows, block, cur, b = [], 0, [], 10
        ch = self.arm["channel"]

        def flush():
            nonlocal block, cur
            if cur:
                block += 1
                rows.extend({"tx_id": t, "block": block, "index": i, "header_type": 3, "validation_code": 0, "env_bytes": 3000,
                             "channel_id": ch, "ch_ts_ms": 0} for i, t in enumerate(cur))
                cur = []
        if self.stray_tx:
            self.events.append(("tx", "f" * 64))          # an endorser tx nobody can explain
        for kind, val in self.events:
            if kind == "lifecycle":                       # as on Fabric: each lifecycle tx is cut into its own block
                flush()
                cur.append(val)
                flush()
                continue
            if kind == "config":
                flush()
                if rows or block:
                    block += 1
                rows.append({"tx_id": f"cfg{block}", "block": block, "index": 0, "header_type": 1, "validation_code": 0,
                             "env_bytes": 9000, "channel_id": ch, "ch_ts_ms": 0})
                (out_dir / "blocks" / f"{ch}_{block:06d}_a1.block").write_bytes(f"B={val}".encode())
                b = val
            else:
                cur.append(val)
                if len(cur) >= b:
                    flush()
        flush()
        with (out_dir / "ledger" / "ledger_tx.csv").open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=CHANNEL_FIELDS)
            w.writeheader()
            w.writerows(rows)

    def run(self, argv, timeout_s, env=None):
        argv = [str(a) for a in argv]
        self.calls.append(argv)
        ok = lambda out=b"": rn.Cmd(0, out, b"", 0.01)  # noqa: E731
        if argv[:2] == ["docker", "inspect"]:
            return ok("\n".join(f"/{n} true 0 2026-11-23T00:00:00Z" for n in argv[4:]).encode())
        if any(a.endswith("e1_channel_run.py") for a in argv):
            rd = Path(argv[argv.index("--run-dir") + 1])
            rd.mkdir(parents=True)
            (rd / "channel_run_record.json").write_text(json.dumps({"status": "VERIFIED"}))
            # the real channel step commits the chaincode definition: approve x2 + commit, logged by the peer CLI
            (rd / "cmd").mkdir()
            for i, name in enumerate(("13_approve_Org1MSP_a1", "14_approve_Org2MSP_a1", "16_commit_a1")):
                tx = f"{i + 1:x}" * 64
                (rd / "cmd" / f"{name}.stderr").write_text(
                    f"2026-11-22 16:54:34.628 UTC 0001 INFO [chaincodeCmd] ClientWait -> txid [{tx}] committed with status (VALID) at localhost:7051\n")
                self.events.append(("lifecycle", tx))
            return ok()
        if any(a.endswith("e1_config_actuator.py") for a in argv):
            assert "--broadcast-authorization" in argv or "inspect" in argv
            if "inspect" in argv:
                return ok(json.dumps({"active_config": cfg(self.B)}).encode())
            target = int(argv[argv.index("--target-b") + 1])
            assert argv[argv.index("--approved-target") + 1] == str(target) and "--execute" in argv
            if self.fail_actuation:
                return rn.Cmd(1, b"", b"SafetyError: simulated", 0.5)
            self.B = target
            self.events.append(("config", target))
            return ok(json.dumps({"success": True, "verification_checks": {"independent_verification": True},
                                  "after_config": cfg(target), "before_block_number": 0, "after_block_number": 99,
                                  "attempt_id": "update-x", "commit_latency_ms": 800.0}).encode())
        if any(a.endswith("e1_block_fetch.py") for a in argv):
            self.build_ledger(Path(argv[argv.index("--out-dir") + 1]))
            return ok()
        if argv[0].endswith("configtxlator"):
            b = int(Path(argv[argv.index("--input") + 1]).read_text().split("=")[1])
            doc = {"data": {"data": [{"payload": {"data": {"config": {"channel_group": {"groups": {"Orderer": {"values": {
                "BatchSize": {"value": {"max_message_count": b, "preferred_max_bytes": 10485760}},
                "BatchTimeout": {"value": {"timeout": "2s"}}}}}}}}}}]}}
            Path(argv[argv.index("--output") + 1]).write_text(json.dumps(doc))
            return ok()
        return ok()


@pytest.fixture(scope="module")
def staged(tmp_path_factory):
    root = tmp_path_factory.mktemp("stage") / "s"
    m = stage(root, seed=7, arms=[("static", None), ("reactive", "R1"), ("reactive", "R0"), ("predictive", None), ("scripted", "10/40/10")],
              replicates=1, first_seq=1, segments=3)
    return root, m


def auth_file(tmp_path, stage_sha, runs, channels, **over):
    kv = {"approved_by": "owner", "approved_utc": "2026-11-01T00:00:00Z", "expires_utc": "2099-01-01T00:00:00Z",
          "stage_sha256": stage_sha, "runs": ",".join(runs), "broadcast_channels": ",".join(channels), "broadcast_targets": "10,40",
          "max_broadcasts_per_run": "10"}
    kv.update(over)
    p = tmp_path / "AUTH.txt"
    p.write_bytes("".join(f"{k}={v}\n" for k, v in kv.items()).encode("ascii"))
    return p


def run_arm(tmp_path, staged, label, auth_over=None, **world_kw):
    root, m = staged
    run = next(r for r in m["runs"] if r["arm_label"] == label)
    auth = auth_file(tmp_path, m["stage_sha256"], [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]],
                     **(auth_over or {}))
    ctx = rn.Ctx(root, run["run_id"], auth, tmp_path / "out", Path("/net.json"), tmp_path / "cws", Path("/msp"), python="python3")
    arm = json.loads((root / run["run_id"] / "arm.json").read_text())
    world = FakeWorld(ctx, arm, **world_kw)
    return rn.execute(ctx, world), world, ctx


def test_static_run_is_valid_and_never_actuates(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "static")
    assert res["verdict"] == "VALID", res.get("step_failure")
    assert not any("apply" in c for c in world.calls)
    a = json.loads((ctx.rd / "analysis.json").read_text())
    assert a["reconciliation"]["complete"] and a["run_level_measured"]["failed"] == 0
    assert [d["target_B"] for d in a["decisions"]] == [10, 10, 10]
    assert a["configuration_regimes"] == [{"from_after_block": 0, "max_message_count": 10}]


def test_reactive_r1_holds_b10(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "reactive-R1")
    assert res["verdict"] == "VALID" and res["control"]["transitions"] == []


def test_reactive_r0_switches_live_and_the_ledger_shows_b40_blocks(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "reactive-R0")
    assert res["verdict"] == "VALID", res.get("step_failure")
    t = res["control"]["transitions"]
    assert len(t) == 1 and t[0]["decision_index"] == 1 and t[0]["to_B"] == 40 and t[0]["success"]
    a = json.loads((ctx.rd / "analysis.json").read_text())
    assert a["block_evidence_by_B"]["40"]["blocks_with_more_than_10"] > 0
    assert a["transitions"][0]["ledger_config_block"] is not None
    assert res["final_config"]["max_message_count"] == 40
    seg2 = next(r for r in a["rounds"] if r["round"] == 2)
    assert seg2["committed_under_B"] == {"40": seg2["success"]}
    assert seg2["latency_ms"]["mean"] == 1030


def test_predictive_applies_stored_b10(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "predictive")
    assert res["verdict"] == "VALID"
    assert [d["target_B"] for d in res["control"]["decisions"]] == [10, 10, 10]
    assert [d["abstained"] for d in res["control"]["decisions"]] == [True, False, False]


def test_scripted_actuation_check_switches_twice(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "scripted-ACTUATION_CHECK")
    assert res["verdict"] == "VALID", res.get("step_failure")
    assert [(t["from_B"], t["to_B"]) for t in res["control"]["transitions"]] == [(10, 40), (40, 10)]


def test_channel_setup_lifecycle_transactions_are_reported_not_counted_as_unreconciled(tmp_path, staged):
    """cs01 (2026-11-22): the ledger's 3 extra endorser tx were the channel step's approve x2 + commit (blocks 1-3)."""
    res, world, ctx = run_arm(tmp_path, staged, "static")
    a = json.loads((ctx.rd / "analysis.json").read_text())
    r = a["reconciliation"]
    assert res["verdict"] == "VALID" and r["complete"] and r["ledger_valid_not_in_recorder"] == 0
    assert [(x["block"], x["source"]) for x in r["channel_setup_lifecycle_tx"]] == [
        (1, "13_approve_Org1MSP_a1.stderr"), (2, "14_approve_Org2MSP_a1.stderr"), (3, "16_commit_a1.stderr")]
    assert r["channel_setup_lifecycle_tx_not_on_ledger"] == []


def test_an_unexplained_ledger_transaction_still_invalidates_the_run(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "static", stray_tx=True)
    a = json.loads((ctx.rd / "analysis.json").read_text())
    assert res["verdict"] == "INVALID_INCOMPLETE"
    assert a["reconciliation"]["ledger_valid_not_in_recorder"] == 1 and len(a["reconciliation"]["channel_setup_lifecycle_tx"]) == 3


def test_lifecycle_tx_ids_reads_only_approve_and_commit_logs(tmp_path):
    from Fabric_Live_Testbed.closed_loop.analyze import lifecycle_tx_ids
    (tmp_path / "cmd").mkdir()
    line = "INFO [chaincodeCmd] ClientWait -> txid [{}] committed with status (VALID) at localhost:{}\n"
    (tmp_path / "cmd" / "13_approve_Org1MSP_a1.stderr").write_text(line.format("a" * 64, 7051))
    (tmp_path / "cmd" / "16_commit_a1.stderr").write_text(line.format("b" * 64, 7051) + line.format("b" * 64, 9051))
    (tmp_path / "cmd" / "08_peer_join_x_a1.stderr").write_text(line.format("c" * 64, 7051))
    (tmp_path / "cmd" / "14_approve_Org2MSP_a1.stderr").write_text("txid [" + "d" * 64 + "] committed with status (ENDORSEMENT_POLICY_FAILURE)\n")
    assert lifecycle_tx_ids(tmp_path) == {"a" * 64: "13_approve_Org1MSP_a1.stderr", "b" * 64: "16_commit_a1.stderr"}
    assert lifecycle_tx_ids(tmp_path / "absent") == {}


def test_failed_actuation_stops_the_workload_and_fails_the_run(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "scripted-ACTUATION_CHECK", fail_actuation=True)
    assert res["verdict"] == "FAILED_ACTUATION" and res["step_failure"]["step"] == "actuation"
    assert world.killed                                   # Caliper is stopped; no further rounds are submitted
    t = res["control"]["transitions"]
    assert len(t) == 1 and not t[0]["success"]
    assert sum(1 for c in world.calls if "apply" in c) == 1
    assert not any(any(a.endswith("e1_block_fetch.py") for a in c) for c in world.calls)
    labels = [json.loads(x).get("label") for x in (ctx.rd / "events.jsonl").read_text().splitlines()]
    assert "inspect_after_failed_actuation" in labels and (ctx.rd / "run_result.json").exists()


def test_decision_is_logged_before_the_actuator_runs(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "scripted-ACTUATION_CHECK")
    ev = [json.loads(x) for x in (ctx.rd / "events.jsonl").read_text().splitlines()]
    for target in (40, 10):
        pre = next(i for i, e in enumerate(ev) if e["kind"] == "decision_before_actuation" and e["target_B"] == target
                   and e["changed"])
        cmd = next(i for i, e in enumerate(ev) if e["kind"] == "command" and e["label"].endswith(f"_B{target}"))
        assert pre < cmd


def test_broadcast_limit_refuses_the_extra_update_and_stops(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "scripted-ACTUATION_CHECK", auth_over={"max_broadcasts_per_run": "1"})
    assert res["verdict"] == "FAILED_ACTUATION" and "max_broadcasts_per_run=1" in res["step_failure"]["detail"]
    assert sum(1 for c in world.calls if "apply" in c) == 1
    assert res["control"]["decisions"][-1]["actuation"] == "REFUSED_BROADCAST_LIMIT"


@pytest.mark.parametrize("value", ["", "-1", "21", "two"])
def test_bad_broadcast_limit_is_refused(tmp_path, staged, value):
    root, m = staged
    run = m["runs"][0]
    a = auth_file(tmp_path, m["stage_sha256"], [run["run_id"]], [run["channel"]], max_broadcasts_per_run=value)
    ctx = rn.Ctx(root, run["run_id"], a, tmp_path / "o", Path("/n"), tmp_path / "c", Path("/m"))
    with pytest.raises(rn.RunRefused):
        rn.execute(ctx, None)


@pytest.mark.parametrize("channel", ["e1-cfg001", "e1-r002", "e1-b001", "e1-rp01-b10-r1"])
def test_protected_or_historical_channel_in_arm_is_refused(tmp_path, staged, channel):
    import shutil
    root, m = staged
    copy = tmp_path / "copy"
    shutil.copytree(root, copy)
    run = m["runs"][0]
    p = copy / run["run_id"] / "arm.json"
    doc = json.loads(p.read_text())
    doc["channel"] = channel
    p.write_bytes((json.dumps(doc, indent=2, sort_keys=True) + "\n").encode())
    sums = copy / "SHA256SUMS.txt"
    lines = [ln if not ln.endswith(f"  {run['run_id']}/arm.json") else f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {run['run_id']}/arm.json"
             for ln in sums.read_text().splitlines()]
    sums.write_bytes(("\n".join(lines) + "\n").encode())
    sha = hashlib.sha256(sums.read_bytes()).hexdigest()
    a = auth_file(tmp_path, sha, [run["run_id"]], ["e1-cs01"])
    ctx = rn.Ctx(copy, run["run_id"], a, tmp_path / "o", Path("/n"), tmp_path / "c", Path("/m"))
    with pytest.raises(rn.RunRefused, match="channel"):
        rn.execute(ctx, None)
    assert not (tmp_path / "o").exists()


def test_missing_round_manifest_fails_the_run(tmp_path, staged):
    res, world, ctx = run_arm(tmp_path, staged, "static", drop_round=2)
    assert res["verdict"] == "FAILED_RECORDER"


def test_compare_builds_tables_and_figures_from_fake_runs(tmp_path, staged):
    from Fabric_Live_Testbed.closed_loop.compare import build
    root, m = staged
    out_root = tmp_path / "out"
    for label in ("static", "reactive-R1", "predictive", "reactive-R0", "scripted-ACTUATION_CHECK"):
        run = next(r for r in m["runs"] if r["arm_label"] == label)
        auth = auth_file(tmp_path, m["stage_sha256"], [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]])
        ctx = rn.Ctx(root, run["run_id"], auth, out_root, Path("/net.json"), tmp_path / "cws", Path("/msp"), python="python3")
        rn.execute(ctx, FakeWorld(ctx, json.loads((root / run["run_id"] / "arm.json").read_text())))
    info = build(out_root, tmp_path / "cmp")
    assert info["runs"] == 5 and info["comparable"] == 4 and len(info["figures"]) == 2
    rows = list(csv.DictReader((tmp_path / "cmp" / "arm_summary.csv").open()))
    by = {r["arm"]: r for r in rows}
    assert set(by) == {"static", "reactive-R1", "predictive", "reactive-R0"}
    assert float(by["static"]["latency_mean_ms_mean"]) == 520 and float(by["reactive-R0"]["transitions_applied_mean"]) == 1
    sums = (tmp_path / "cmp" / "SHA256SUMS.txt").read_text().splitlines()
    assert len(sums) == 6


def test_refusals_without_valid_authorization(tmp_path, staged):
    root, m = staged
    run = m["runs"][0]
    ctx = lambda auth: rn.Ctx(root, run["run_id"], auth, tmp_path / "o", Path("/n"), tmp_path / "c", Path("/m"))  # noqa: E731
    with pytest.raises(rn.RunRefused, match="not authorized"):
        rn.execute(ctx(auth_file(tmp_path, m["stage_sha256"], ["zz99"], [run["channel"]])), None)
    with pytest.raises(rn.RunRefused, match="differs from the authorized stage"):
        rn.execute(ctx(auth_file(tmp_path, "0" * 64, [run["run_id"]], [run["channel"]])), None)
    with pytest.raises(rn.RunRefused, match="approved_by=owner"):
        rn.execute(ctx(auth_file(tmp_path, m["stage_sha256"], [run["run_id"]], [run["channel"]], approved_by="assistant")), None)
    assert not (tmp_path / "o").exists()


def test_tampered_stage_is_refused(tmp_path, staged):
    root, m = staged
    import shutil
    copy = tmp_path / "copy"
    shutil.copytree(root, copy)
    p = copy / m["runs"][0]["run_id"] / "workspace" / "schedule_r01.txt"
    p.write_bytes(p.read_bytes() + b"\n1")
    ctx = rn.Ctx(copy, m["runs"][0]["run_id"], tmp_path / "a", tmp_path / "o", Path("/n"), tmp_path / "c", Path("/m"))
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        rn.execute(ctx, None)
