"""Derived driver, offline: matrix, staging end to end on a synthetic processed tree, the step list, S1 evaluation, the live-path interlock,
and the per-run executor's ordering and halting logic against a RECORDING FAKE backend (no Docker, WSL, Fabric or Caliper).

The executor's body is LIVE_UNTESTED against a real backend; what is proven here is order, halting, refusals and verdicts only."""

import csv
import hashlib
import json
import shutil
from pathlib import PurePosixPath

import numpy as np
import pytest

from p4_replay import REPO_ROOT, driver as dr, fidelity as fd, offsets as of, render as rd
from p4_replay.tests.conftest import ENV, NETWORK_PATH, PKG, START_S, synthetic_day

p1b = dr.p1b_module()
NETWORK_DOC = rd.lr.network(ENV, PKG)


# ----------------------------------------------------------------------------------------------- the frozen driver is imported, not edited
def test_the_frozen_p1b_driver_is_the_pinned_file():
    path = REPO_ROOT / "scripts" / "e1" / "e1_quicklook_p1b.py"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == dr.P1B_DRIVER_SHA256


def test_a_changed_p1b_driver_is_refused(monkeypatch):
    monkeypatch.setattr(dr, "_P1B", None)
    monkeypatch.setattr(dr, "P1B_DRIVER_SHA256", "0" * 64)
    with pytest.raises(RuntimeError, match="not the frozen file"):
        dr.p1b_module()


# ----------------------------------------------------------------------------------------------- matrix
def test_matrix_is_one_dry_run_then_six_seeded_measured_runs():
    rows = dr.build_matrix(7)
    assert [r.kind for r in rows] == ["dry_run"] + ["measured"] * 6
    assert (rows[0].window, rows[0].B) == ("D_33000", 10)
    assert {r.window for r in rows[1:]} == {"F_82800"} and {r.schedule for r in rows} == {"A"}
    assert sorted((r.B, r.replicate) for r in rows[1:]) == [(10, 1), (10, 2), (10, 3), (40, 1), (40, 2), (40, 3)]
    assert dr.build_matrix(7) == rows                                                     # same seed, same order
    assert all(dr.CHANNEL_RE.match(r.channel) and r.channel not in p1b.EXISTING_CHANNELS for r in rows)


def test_matrix_csv_is_lf_ascii_and_stable():
    a = dr.matrix_csv(dr.build_matrix(7))
    assert a == dr.matrix_csv(dr.build_matrix(7)) and b"\r" not in a and a.splitlines()[0] == b"seq,run_id,channel,kind,window,schedule,B,tau,P,replicate"


@pytest.mark.parametrize("mutate", [
    lambda rows: rows[:-1],                                                             # a replicate missing
    lambda rows: rows[::-1],                                                            # not in seq order
    lambda rows: [*rows[:-1], dr.ReplayRow(rows[-1].seq, rows[-1].run_id, rows[1].channel, "measured", "F_82800", "A", 10, "2s", 10_485_760, 3)],   # duplicate channel
    lambda rows: [*rows[:-1], dr.ReplayRow(rows[-1].seq, rows[-1].run_id, "e1-b001", "measured", "F_82800", "A", 40, "2s", 10_485_760, 3)],        # an existing channel
    lambda rows: [*rows[:-1], dr.ReplayRow(rows[-1].seq, rows[-1].run_id, rows[-1].channel, "measured", "F_82800", "A", 45, "2s", 10_485_760, 3)],  # B outside scope
    lambda rows: [*rows[:-1], dr.ReplayRow(rows[-1].seq, rows[-1].run_id, rows[-1].channel, "measured", "F_82800", "BO", 40, "2s", 10_485_760, 3)],  # another arm
])
def test_matrix_validation_rejects(mutate):
    with pytest.raises(dr.MatrixError):
        dr.validate_matrix(mutate(dr.build_matrix(7)))


# ----------------------------------------------------------------------------------------------- the run-length trap
def test_p1b_default_thresholds_would_kill_a_replay_and_are_refused():
    assert p1b.THR.caliper_timeout_s == 300.0 and p1b.EVID.host_max_seconds == 420          # the trap, as found in the frozen driver
    assert p1b.THR.caliper_timeout_s < dr.MIN_CALIPER_TIMEOUT_S == 780.0                    # 60 + 600 s of load + 120 s floor
    thr = dr.replay_thresholds()
    assert thr.caliper_timeout_s == dr.CALIPER_TIMEOUT_S_PROPOSED >= dr.MIN_CALIPER_TIMEOUT_S
    for field in ("win_avail_mb_start", "health_poll_s", "consecutive_invalid_stop", "channel_timeout_s"):
        assert getattr(thr, field) == getattr(p1b.THR, field)                               # only the run-length limit changed


def test_host_sampler_argv_is_corrected_from_the_p1b_value():
    ctx = dr.posix_ctx("S")
    s = dr.shim(dr.build_matrix(7)[1], 10.4)
    stock = p1b.argv_host(s, ctx, PurePosixPath("/x/stop"))
    assert stock[stock.index("--max-seconds") + 1] == "420"
    fixed = dr.sampler_argvs(s, ctx, ["c"], PurePosixPath("/x"))["host"]
    assert fixed[fixed.index("--max-seconds") + 1] == str(dr.HOST_MAX_SECONDS_PROPOSED) and int(fixed[fixed.index("--max-seconds") + 1]) > 660


# ----------------------------------------------------------------------------------------------- steps and listing
def test_step_order_and_reuse_of_p1b_builders():
    rows = dr.build_matrix(7)
    ctx = dr.posix_ctx("S")
    steps = dr.run_steps(rows[1], ctx, PurePosixPath("/mnt/d/stage"), 10.4)
    assert [s["label"] for s in steps] == ["channel", "stage_copy", "stage_plan", "stage_verify", "metrics_before", "samplers", "getinfo_before", "config_before",
                                           "caliper", "getinfo_after", "config_after", "stop_samplers", "metrics_after", "metrics_summary", "block_fetch", "analysis",
                                           "fidelity"]
    channel = next(s for s in steps if s["label"] == "channel")
    assert channel["argv"] == p1b.argv_channel(dr.shim(rows[1], 10.4), ctx)                  # the frozen builder, unchanged
    assert next(s for s in steps if s["label"] == "stage_copy")["argv"][:2] == ["cp", "-r"]
    assert "/home/mailf18/e1/" not in str(ctx.root)                                          # the series root is outside the wsl_e1 snapshot group
    assert all("/" in a for s in steps if "argv" in s for a in s["argv"][:3] if a not in ("cp", "-r", "sh", "-c", "python3", "-B"))


def test_listing_prints_text_and_executes_nothing(capsys):
    assert dr.main(["listing", "--run-order-seed", "7"]) == 0
    out = capsys.readouterr().out
    assert "LISTING ONLY" in out and "LIVE_UNTESTED" in out and out.count("== rp") == 7


# ----------------------------------------------------------------------------------------------- the live interlock
def test_run_command_is_refused():
    with pytest.raises(dr.LiveExecutionRefused, match="closed in Phase 1"):
        dr.main(["run"])


def test_live_flag_is_off_and_a_real_backend_is_refused_before_any_call(tmp_path):
    assert dr.LIVE_EXECUTION_ENABLED is False
    backend = object.__new__(p1b.Backend)                                                    # a real Backend, never initialised: any use would fail loudly
    ctx = p1b.Ctx(repo=tmp_path, e1_home=tmp_path, root=tmp_path / "series", net=tmp_path / "n.json", env=tmp_path / "e.json", caliper_ws=tmp_path / "ws",
                  py="python3", win_repo="", win_snapshot="", win_python="", ref_python="", matrix=tmp_path / "m.csv", fabric_bin="/x")
    s = p1b.Session(ctx=ctx, be=backend, thr=dr.replay_thresholds())
    with pytest.raises(dr.LiveExecutionRefused, match="not enabled in this build"):
        dr.execute_replay_run(s, dr.build_matrix(7)[1], PurePosixPath("/stage"), 10.4)
    assert not (tmp_path / "series").exists()                                                # nothing was created


def test_p1b_default_thresholds_are_refused_by_the_executor(tmp_path):
    fake = RecBackend(ledger_src=tmp_path / "ledger_src.csv")
    ctx = fake_ctx(tmp_path)
    s = p1b.Session(ctx=ctx, be=fake)                                                        # THR default: 300 s Caliper timeout
    with pytest.raises(ValueError, match="cannot fit"):
        dr.execute_replay_run(s, dr.build_matrix(7)[1], tmp_path / "stage", 10.4)
    assert fake.calls == []


# ----------------------------------------------------------------------------------------------- S1
ALLOWED = ["repo_scripts/e1/e1_quicklook_p1b.py", "repo_tests/e1/test_e1_quicklook_p1b.py"]


def verify_text(events, checked=100, drift=None):
    return "\n".join(events) + f"\nverify: checked={checked} mismatched=0 missing=0 unlisted=0 source_drift={len(events) if drift is None else drift}\n"


def test_s1_fails_on_unexpected_added_files_and_passes_when_exactly_covered():
    ev = [f"SOURCE_ADDED {p}" for p in ALLOWED] + ["SOURCE_ADDED repo_src/fabric_perf/replay/trace.py", "SOURCE_ADDED repo_data/processed/flashbots_ms/x.npy"]
    r = dr.evaluate_s1(verify_text(ev), allowed_exact=frozenset(ALLOWED))
    assert r["verdict"] == "FAIL" and r["unexpected_count"] == 2
    ok = dr.evaluate_s1(verify_text(ev), allowed_exact=frozenset(ALLOWED), allowed_prefixes=("repo_src/fabric_perf/replay/", "repo_data/processed/flashbots_ms/"))
    assert ok["verdict"] == "PASS" and ok["unexpected_count"] == 0


def test_s1_never_allows_changed_or_removed_files_even_under_an_allowed_prefix():
    for kind in ("SOURCE_CHANGED", "SOURCE_REMOVED"):
        r = dr.evaluate_s1(verify_text([f"{kind} repo_src/fabric_perf/replay/trace.py"]), allowed_prefixes=("repo_src/",))
        assert r["verdict"] == "FAIL" and r["unexpected_count"] == 1


def test_s1_fails_on_missing_summary_integrity_errors_or_a_miscount():
    assert dr.evaluate_s1("nothing")["verdict"] == "FAIL"
    assert dr.evaluate_s1("verify: checked=10 mismatched=1 missing=0 unlisted=0 source_drift=0")["verdict"] == "FAIL"
    assert dr.evaluate_s1("verify: checked=0 mismatched=0 missing=0 unlisted=0 source_drift=0")["verdict"] == "FAIL"
    assert dr.evaluate_s1(verify_text(["SOURCE_ADDED a/b"], drift=3), allowed_exact=frozenset({"a/b"}))["verdict"] == "FAIL"   # fewer events than the tool counted


# ----------------------------------------------------------------------------------------------- staging end to end (synthetic processed tree)
def make_processed(tmp_path):
    day = synthetic_day()
    root = tmp_path / "processed"
    (root / "windows").mkdir(parents=True)
    manifest = {"days": {}}
    for date in ("2026-09-21", "2026-10-12"):
        np.save(root / f"arrivals_{date}_ms.npy", day)
        manifest["days"][date] = {"array": f"arrivals_{date}_ms.npy", "array_sha256": of.sha256_hex((root / f"arrivals_{date}_ms.npy").read_bytes()).upper()}
    (root / "flashbots_ms_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    for wid, date in (("F_82800", "2026-10-12"), ("D_33000", "2026-09-21")):
        w = of.build_window(day, wid, date, START_S, 600, 60)
        rec = {"day": date, "start_s": START_S, "length_s": 600, "pre_roll_s": 60, "selected_tile": {"n_tx": int(w.measured_ms.size)},
               "schedule_sha256": {"A": of.sha256_hex(of.schedule_bytes(w.measured_ms, b"\r\n")).upper()}}
        (root / "windows" / wid).mkdir()
        (root / "windows" / wid / "window_record.json").write_text(json.dumps(rec), encoding="utf-8")
    return root


def test_stage_series_end_to_end_and_verify(tmp_path):
    root = make_processed(tmp_path)
    series = dr.stage_series(dr.build_matrix(7), dict(ENV), NETWORK_PATH, root, tmp_path / "stage", network_doc=NETWORK_DOC)
    assert sorted(series["runs"]) == [f"rp{i:02d}" for i in range(7)] and set(series["windows"]) == {"F_82800", "D_33000"}
    assert all(w["equal_to_p1_p5_schedule"] for w in series["windows"].values())             # lineage to the P1-P5 schedule hash
    assert dr.verify_stage(tmp_path / "stage")["ok"]
    sums = (tmp_path / "stage" / "SHA256SUMS.txt").read_bytes()
    assert b"\r" not in sums and sums.endswith(b"\n")
    for line in sums.decode().splitlines():                                                  # sha256sum -c format
        digest, sep, rel = line.partition("  ")
        assert sep and len(digest) == 64 and (tmp_path / "stage" / rel).is_file()
    with pytest.raises(FileExistsError):
        dr.stage_series(dr.build_matrix(7), dict(ENV), NETWORK_PATH, root, tmp_path / "stage", network_doc=NETWORK_DOC)    # never overwrites


def test_verify_stage_catches_tampering_and_strays(tmp_path):
    root = make_processed(tmp_path)
    dr.stage_series(dr.build_matrix(7), dict(ENV), NETWORK_PATH, root, tmp_path / "stage", network_doc=NETWORK_DOC)
    target = tmp_path / "stage" / "runs" / "rp03" / "schedule_measured.txt"
    target.write_bytes(target.read_bytes() + b"\n999")
    (tmp_path / "stage" / "stray.txt").write_text("x")
    result = dr.verify_stage(tmp_path / "stage")
    assert not result["ok"] and "runs/rp03/schedule_measured.txt" in result["mismatched_or_missing"] and "stray.txt" in result["unlisted"]


def test_a_window_record_that_disagrees_with_the_trace_is_refused(tmp_path):
    root = make_processed(tmp_path)
    path = root / "windows" / "F_82800" / "window_record.json"
    rec = json.loads(path.read_text())
    rec["schedule_sha256"]["A"] = "0" * 64
    path.write_text(json.dumps(rec))
    with pytest.raises(Exception, match="approach-A hash"):
        of.load_registered_window(root, "F_82800")


# ----------------------------------------------------------------------------------------------- the executor against a recording fake
class FakeProc:
    returncode = None

    def poll(self):
        return None

    def kill(self):
        self.returncode = -9

    terminate = kill

    def wait(self, timeout=None):
        return self.returncode


class RecBackend:
    """Duck-typed Backend that records and emulates only the file effects later steps read. It is NOT a p1b.Backend."""

    def __init__(self, fail_if=None, ledger_src=None):
        self.t, self.calls, self.spawns, self.fail_if, self.ledger_src = 100.0, [], [], fail_if, ledger_src

    def now_utc(self):
        return "2026-11-15T00:00:00.000Z"

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.t += s

    def run(self, argv, env=None, timeout=None):
        self.calls.append(list(argv))
        if self.fail_if and self.fail_if(argv):
            return p1b.CmdResult(code=1)
        if "--run-dir" in argv:
            d = self.__class__.dir_of(argv[argv.index("--run-dir") + 1])
            d.mkdir(parents=True, exist_ok=True)
            (d / "channel_run_record.json").write_text(json.dumps({"status": "VERIFIED"}), encoding="utf-8")
        if any(str(a).endswith("e1_block_fetch.py") for a in argv) and self.ledger_src is not None:
            out = self.dir_of(argv[argv.index("--out-dir") + 1]) / "ledger"        # what the real block_fetch leaves behind
            out.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self.ledger_src, out / "ledger_tx.csv")
        if argv[0] == "cp" and argv[1] == "-r":
            shutil.copytree(argv[2], argv[3], dirs_exist_ok=True)
        elif argv[0] == "cp":
            shutil.copyfile(argv[1], argv[2])
        return p1b.CmdResult(code=0)

    @staticmethod
    def dir_of(p):
        from pathlib import Path
        return Path(p)

    def spawn(self, argv, env=None, stdout_path=None, stderr_path=None):
        self.spawns.append(list(argv))
        return FakeProc()


def fake_ctx(tmp_path):
    (tmp_path / "series").mkdir(exist_ok=True)
    return p1b.Ctx(repo=REPO_ROOT, e1_home=tmp_path, root=tmp_path / "series", net=tmp_path / "network.json", env=tmp_path / "env.json", caliper_ws=tmp_path / "ws",
                   py="python3", win_repo="", win_snapshot="", win_python="", ref_python="", matrix=tmp_path / "m.csv", fabric_bin="/x")


def write_rows(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)


def prepare(tmp_path, window, *, drop_dispatch=False):
    """Stage one run and pre-create the files a real run would leave (perfect timing), in the places the executor reads."""
    (tmp_path / "network.json").write_text(json.dumps(NETWORK_DOC), encoding="utf-8")
    ctx = fake_ctx(tmp_path)
    row = dr.build_matrix(7)[1]
    stage = tmp_path / "stage"
    (stage / "plans").mkdir(parents=True)
    r = rd.render_replay_run(dict(ENV), str(tmp_path / "network.json"), row.run_id, row.channel, row.B, row.tau, row.P, window,
                             stage / "plans" / f"{row.run_id}.plan.json", ws_root=stage)
    ws_run, rec_dir = ctx.caliper_ws / "runs" / row.run_id, ctx.recorder_dir(row.run_id)
    ledger = [{"tx_id": "cfg", "validation_code": "0", "header_type": "1"}]
    for idx, name in enumerate(("pre_roll", "measured")):
        offs = [int(x) for x in (r["dir"] / f"schedule_{name}.txt").read_text().split("\n")]
        t0 = 1_700_000_000_000 + idx * 100_000
        disp = [[i, o, t0 + o, t0 + o + 7, 7] for i, o in enumerate(offs)]
        if drop_dispatch and name == "measured":
            disp.pop()
        write_rows(ws_run / f"dispatch_{name}.csv", fd.DISPATCH_HEADER, disp)
        write_rows(ws_run / f"dispatch_{name}.csv.overflow.csv", ["requested_seq", "observed_ms"], [[len(offs), t0]])
        recs = [[f"t{idx}{i:04d}", i, t0 + o + 7, t0 + o + 1507, "success", "true", "", 0, 0, idx, ""] for i, o in enumerate(offs)]
        write_rows(rec_dir / f"{row.run_id}_w0_r{idx}_{'wu' if idx == 0 else 'm'}-{row.run_id}.csv",
                   ["tx_id", "seq_finish", "create_ms", "final_ms", "status", "verified", "err", "obs_hr_ns", "worker", "round", "label"], recs)
        ledger += [{"tx_id": x[0], "validation_code": "0", "header_type": "3"} for x in recs]
        if name == "measured":
            write_rows(rec_dir / f"{row.run_id}_w0_r1_m-{row.run_id}.submits.csv", ["seq_submit", "t_submit_evt_ms", "count"], [[0, t0, len(offs)]])
    write_rows(tmp_path / "ledger_src.csv", ["tx_id", "block", "index", "header_type", "validation_code", "env_bytes", "channel_id", "ch_ts_ms"],
               [[x["tx_id"], 0, 0, x["header_type"], x["validation_code"], 0, row.channel, 0] for x in ledger])
    return ctx, row, stage


def helpers(row, caliper=None):
    cfg = {"active_config": {"max_message_count": row.B, "batch_timeout_ms": 2000, "preferred_max_bytes": row.P}, "block_sha256": "x"}
    return {"host_snapshot": lambda be, pid=None: {"stub": True}, "load_network": lambda ctx: object(), "peer_env": lambda ctx, net: {}, "tool_env": lambda ctx: {},
            "getinfo": lambda rec, net, ch, penv, label: {"height": 12}, "fetch_config": lambda rec, net, ch, penv, label, rd_: cfg,
            "stop_background": lambda be, rec, handles, timeout_s=120.0: {n: 0 for n, _, _ in handles},
            "run_caliper": caliper or (lambda s, rec, plan_raw, env: {"exit_code": 0, "timed_out": False, "aborted": None, "duration_s": 1.0})}


EXPECTED_DONE = ["channel", "stage_copy", "stage_plan", "stage_verify", "metrics_before", "samplers", "getinfo_before", "config_before", "caliper", "getinfo_after",
                 "config_after", "stop_samplers", "metrics_after", "metrics_summary", "block_fetch", "analysis"]


def labels(ctx, row):
    return [json.loads(x)["label"] for x in (ctx.rd(row.run_id) / "commands.jsonl").read_text().splitlines()]


def test_executor_runs_every_step_in_order_and_records_a_provisional_verdict(tmp_path, window):
    ctx, row, stage = prepare(tmp_path, window)
    be = RecBackend(ledger_src=tmp_path / "ledger_src.csv")
    s = p1b.Session(ctx=ctx, be=be, thr=dr.replay_thresholds())
    v = dr.execute_replay_run(s, row, stage, 10.4, helpers=helpers(row), clock_gate_pass=True)
    assert v["verdict"] == "REPLAY_RECORDED" and v["fidelity"] == "FIDELITY_PASS_PROVISIONAL" and v["failed_checks"] == []
    assert v["steps_done"] == EXPECTED_DONE
    assert labels(ctx, row) == ["channel", "stage_copy", "stage_plan", "stage_verify", "metrics_before", "metrics_during", "resources", "host", "metrics_after",
                                "metrics_summary", "block_fetch", "analysis"]
    for name in ("facts.json", "run_verdict.json", "fidelity.json", "commands.jsonl"):
        assert (ctx.rd(row.run_id) / name).is_file()
    host = next(c for c in be.spawns if "e1_host_sampler.py" in " ".join(c))
    assert host[host.index("--max-seconds") + 1] == str(dr.HOST_MAX_SECONDS_PROPOSED)         # the corrected guard reached the real command line
    assert not any(c[0] in ("docker", "wsl", "wsl.exe") for c in be.calls + be.spawns)        # nothing here can reach Docker


def test_executor_labels_a_bad_replay_FIDELITY_LIMITED_not_accepted(tmp_path, window):
    ctx, row, stage = prepare(tmp_path, window, drop_dispatch=True)
    v = dr.execute_replay_run(p1b.Session(ctx=ctx, be=RecBackend(ledger_src=tmp_path / "ledger_src.csv"), thr=dr.replay_thresholds()), row, stage, 10.4, helpers=helpers(row), clock_gate_pass=True)
    assert v["verdict"] == "REPLAY_RECORDED" and v["fidelity"] == "FIDELITY_LIMITED" and "S1@measured" in v["failed_checks"]


def test_executor_without_clock_evidence_is_limited(tmp_path, window):
    ctx, row, stage = prepare(tmp_path, window)
    v = dr.execute_replay_run(p1b.Session(ctx=ctx, be=RecBackend(ledger_src=tmp_path / "ledger_src.csv"), thr=dr.replay_thresholds()), row, stage, 10.4, helpers=helpers(row))
    assert v["fidelity"] == "FIDELITY_LIMITED" and v["failed_checks"] == ["C1"]


def test_executor_halts_at_the_first_failed_step(tmp_path, window):
    ctx, row, stage = prepare(tmp_path, window)
    be = RecBackend(fail_if=lambda argv: any("e1_channel_run.py" in a for a in argv))
    v = dr.execute_replay_run(p1b.Session(ctx=ctx, be=be, thr=dr.replay_thresholds()), row, stage, 10.4, helpers=helpers(row), clock_gate_pass=True)
    assert v["verdict"] == "INVALID_INFRA" and v["steps_done"] == ["channel"] and v["step_failure"]["label"] == "channel"
    assert len(be.calls) == 1 and not be.spawns                                              # nothing after the failed channel step ran


def test_executor_reports_a_caliper_abort_as_aborted_infra_and_stops_samplers(tmp_path, window):
    ctx, row, stage = prepare(tmp_path, window)
    aborted = lambda s, rec, plan_raw, env: {"exit_code": None, "timed_out": False, "aborted": "container event: restart", "duration_s": 5.0}   # noqa: E731
    be = RecBackend(ledger_src=tmp_path / "ledger_src.csv")
    v = dr.execute_replay_run(p1b.Session(ctx=ctx, be=be, thr=dr.replay_thresholds()), row, stage, 10.4, helpers=helpers(row, aborted), clock_gate_pass=True)
    assert v["verdict"] == "ABORTED_INFRA" and "caliper" in v["steps_done"] and "analysis" not in v["steps_done"]
    facts = json.loads((ctx.rd(row.run_id) / "facts.json").read_text())
    assert facts["stop_background"] == {"metrics": 0, "resources": 0, "host": 0}              # the samplers were stopped on the failure path


# ----------------------------------------------------------------------------------------------- real derived data (read-only; skipped if absent)
REAL = REPO_ROOT / "data" / "processed" / "flashbots_ms"


@pytest.mark.skipif(not (REAL / "flashbots_ms_manifest.json").exists(), reason="derived Flashbots trace not present")
def test_real_registered_windows_rebuild_to_the_p1_p5_schedule():
    for wid, n_tx in (("F_82800", 6229), ("D_33000", 6887)):
        w, lineage = of.load_registered_window(REAL, wid)
        assert w.measured_ms.size == n_tx and lineage["equal_to_p1_p5_schedule"] is True
        assert w.measured_ms.max() < 600_000 and w.pre_ms.size > 0 and w.pre_ms.max() < 60_000
        rows = of.build_tx_map("rp99", w)
        assert len(rows) == w.pre_ms.size + w.measured_ms.size and len({r["create_asset_key"] for r in rows}) == len(rows)
