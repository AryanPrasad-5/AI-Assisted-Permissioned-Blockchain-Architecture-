"""Clock handshake: fake `docker exec` against a fake host clock. Not Docker output; not E0 results."""
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_clock_handshake as ch  # noqa: E402
import e1_reconcile as rc  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

C = ["caliper", "peer0.org1.example.com", "orderer1.example.com"]


class FakeExec:
    """Each docker exec takes rtt_ns; the container reads its clock (host wall + offset) halfway through.
    fmt[c]: ns | ms | s | busybox | garbage; fail[c] = set of call indices that fail; step[c] = {call: ns}
    applied to that container's clock from that call on."""

    def __init__(self, clock, rtt_ns=400_000, offsets=None, fmt=None, fail=None, step=None):
        self.clock, self.rtt = clock, rtt_ns
        self.offsets, self.fmt = dict(offsets or {}), dict(fmt or {})
        self.fail, self.step = dict(fail or {}), dict(step or {})
        self.n = {}

    def __call__(self, argv):
        assert argv[:2] == ["docker", "exec"]
        c = argv[2]
        i = self.n.get(c, 0)
        self.n[c] = i + 1
        self.clock.step(self.rtt // 2)
        extra = sum(v for k, v in self.step.get(c, {}).items() if i >= k)
        remote = self.clock.wall + self.offsets.get(c, 0) + extra
        self.clock.step(self.rtt - self.rtt // 2)
        if i in self.fail.get(c, set()):
            return 1, b"", b"Error response from daemon: container is not running"
        f = self.fmt.get(c, "ns")
        out = {"ns": str(remote), "ms": str(remote // 1_000_000), "s": str(remote // 10**9),
               "busybox": f"{remote // 10**9}%N", "garbage": "Thu Nov  9 12:00:00 UTC 2026"}[f]
        return 0, (out + "\n").encode(), b""


def hs(tmp_path, fake, clock, name="h", containers=C, **kw):
    return ch.run_handshake(containers, kw.pop("phase", "start"), tmp_path / name, fake, wall_ns=clock.wall_ns,
                            mono_ns=clock.mono_ns, sleep=clock.sleep, **kw)


def kinds(r):
    return {**r["anomalies"]["error"], **r["anomalies"]["warning"]}


def test_parse_remote_units_and_rejections():
    assert ch.parse_remote("1700000000123456789\n") == (1_700_000_000_123_456_789, 1)
    assert ch.parse_remote("1700000000123") == (1_700_000_000_123_000_000, 1_000_000)
    assert ch.parse_remote("1700000000") == (1_700_000_000 * 10**9, 10**9)
    for bad, kind in (("1700000000%N", "no_subsecond_support"), ("1700000000N", "no_subsecond_support"),
                      ("17000000001", "malformed_output"), ("", "malformed_output"), ("12:00", "malformed_output")):
        with pytest.raises(ValueError, match=kind):
            ch.parse_remote(bad)


def test_shared_clock_recovered_within_the_round_trip_bound(tmp_path):
    clock = sf.Clock()
    r = hs(tmp_path, FakeExec(clock, rtt_ns=400_000), clock)
    assert r["status"] == "COMPLETE"
    for c in C:
        v = r["containers"][c]
        assert v["status"] == "MEASURED" and v["samples_ok"] == 5
        assert abs(v["offset_ns"]) <= v["uncertainty_ns"] == 200_001     # true offset 0 inside the interval
    p = ch.pair_bound(r["containers"], "caliper", "peer0.org1.example.com")
    assert p["same_clock_consistent"] and p["within_protocol_bound"] and p["bound_ms"] == pytest.approx(0.400002)


def test_known_offset_is_recovered_and_slow_exec_cannot_demonstrate_1ms(tmp_path):
    clock = sf.Clock()
    r = hs(tmp_path, FakeExec(clock, rtt_ns=40_000_000, offsets={"caliper": 7_000_000}), clock)
    cal = r["containers"]["caliper"]
    assert abs(cal["offset_ns"] - 7_000_000) <= cal["uncertainty_ns"]
    p = ch.pair_bound(r["containers"], "caliper", "peer0.org1.example.com")
    assert p["within_protocol_bound"] is False and p["bound_ms"] > 40        # honest: the bound is the round trip


def test_ms_probe_and_busybox_without_nanoseconds(tmp_path):
    clock = sf.Clock()
    r = hs(tmp_path, FakeExec(clock, fmt={"caliper": "ms", "orderer1.example.com": "busybox"}), clock,
           probes={"caliper": ["node", "-e", "process.stdout.write(String(Date.now()))"]})
    assert r["containers"]["caliper"]["resolution_ns"] == 1_000_000
    assert r["containers"]["orderer1.example.com"]["status"] == "UNMEASURED"
    k = kinds(r)
    assert r["status"] == "INCOMPLETE" and k["no_subsecond_support"] == 5 and k["container_unmeasured"] == 1
    rows = ch.tc.read_csv(tmp_path / "h" / "clock_samples.csv")
    assert [x["probe"] for x in rows if x["container"] == "caliper"][0].startswith("node -e")


def test_seconds_only_is_a_warning_but_widens_the_bound(tmp_path):
    clock = sf.Clock()
    r = hs(tmp_path, FakeExec(clock, fmt={"caliper": "s"}), clock)
    assert r["status"] == "COMPLETE_WITH_WARNINGS" and kinds(r) == {"coarse_resolution": 5}
    assert r["containers"]["caliper"]["uncertainty_ns"] >= 500_000_000


def test_failures_garbage_steps_and_implausible_times(tmp_path):
    clock = sf.Clock()
    r = hs(tmp_path, FakeExec(clock, fail={"caliper": {0, 1}}, fmt={"orderer1.example.com": "garbage"}), clock)
    k = kinds(r)
    assert k["probe_failed"] == 2 and k["malformed_output"] == 5 and r["status"] == "INCOMPLETE"
    assert r["containers"]["caliper"]["samples_ok"] == 3
    clock2 = sf.Clock()
    # rounds are ~151 ms apart per container (3 probes x (0.4 ms + 50 ms gap)); a -500 ms step makes stamps decrease
    r2 = hs(tmp_path, FakeExec(clock2, step={"caliper": {2: -500_000_000}}), clock2, name="h2")
    assert r2["status"] == "INCONSISTENT"
    assert kinds(r2)["remote_clock_step"] == 1 and kinds(r2)["offset_inconsistent"] >= 1
    clock3 = sf.Clock()
    r3 = hs(tmp_path, FakeExec(clock3, offsets={"caliper": 3 * 86_400 * 10**9}), clock3, name="h3")
    assert kinds(r3)["implausible_remote_time"] == 5


def test_host_clock_step_is_detected(tmp_path):
    class Jumpy(FakeExec):
        def __call__(self, argv):
            out = super().__call__(argv)
            if self.n[argv[2]] == 2:
                self.clock.jump(10_000_000)
            return out
    clock = sf.Clock()
    r = hs(tmp_path, Jumpy(clock), clock, containers=["caliper"])
    assert r["status"] == "INCONSISTENT" and kinds(r)["host_clock_step"] >= 1


def test_drift_between_start_and_end(tmp_path):
    clock = sf.Clock()
    fake = FakeExec(clock, step={"peer0.org1.example.com": {5: 9_000_000}})
    s = hs(tmp_path, fake, clock, name="s", phase="start")
    clock.step(150 * 10**9)
    e = hs(tmp_path, fake, clock, name="e", phase="end")
    d = ch.drift(s, e, tmp_path / "drift.json")
    assert d["status"] == "INCONSISTENT" and d["anomalies"]["error"] == {"drift_exceeds_tolerance": 1}
    assert d["containers"]["caliper"]["excess_beyond_uncertainty_ms"] == 0.0
    assert d["containers"]["peer0.org1.example.com"]["delta_ns"] == pytest.approx(9_000_000, abs=500_000)
    with pytest.raises(FileExistsError):
        ch.drift(s, e, tmp_path / "drift.json")


def test_bound_feeds_reconciler_v4_cross_source(tmp_path):
    clock = sf.Clock()
    r = hs(tmp_path, FakeExec(clock, rtt_ns=400_000), clock)
    bound = ch.pair_bound(r["containers"], "caliper", "peer0.org1.example.com")["bound_ms"]
    recs = [{"tx_id": f"t{i}", "status": "success", "create_ms": "100", "final_ms": "1000.0"} for i in range(3)]
    ledger = [{"tx_id": f"t{i}", "block": 1, "index": i, "header_type": 3, "validation_code": 0, "env_bytes": 1}
              for i in range(3)]
    manifest = {"counts": {"submitted": 3, "finished": 3, "pending_at_deactivate": 0, "duplicate_ids": 0,
                           "no_id_records": 0}}
    exp = rc.Expectations(n=3, max_message_count=10, preferred_max_bytes=524288, clock_offset_bound_ms=bound)

    def v4(commit_ms):
        checks = rc.reconcile(recs, None, manifest, ledger, exp, [{"block": "1", "commit_ms": str(commit_ms)}])
        return next(c for c in checks if c.check_id == "V4-cross-source").status
    assert v4(1000.0 + bound * 0.99) == "PASS"      # commit logged just after finish, inside the measured bound
    assert v4(1000.0 + bound * 1.01) == "FAIL"      # beyond the bound: a finish before its block committed


def test_cli(tmp_path, capsys):
    assert ch.main(["handshake", "--container", "caliper", "--phase", "start", "--out-dir", str(tmp_path / "d")]) == 0
    assert "dry run" in capsys.readouterr().out and not (tmp_path / "d").exists()
    assert ch.main(["handshake", "--container", "a", "--container", "a", "--phase", "start", "--out-dir",
                    str(tmp_path / "x"), "--execute"]) == 2
    # the CLI uses the real host clocks, so the fake container must read the real clock too (shared clock)
    real = lambda argv: (0, f"{time.time_ns()}\n".encode(), b"")  # noqa: E731
    assert ch.main(["handshake", "--container", "caliper", "--container", "peer0.org1.example.com", "--phase", "start",
                    "--rounds", "2", "--out-dir", str(tmp_path / "ok"), "--execute"], runner=real) == 0
    hsj = tmp_path / "ok" / "clock_handshake.json"
    assert ch.main(["bound", "--handshake", str(hsj), "--client", "caliper", "--peer", "peer0.org1.example.com"]) in (0, 1)
    assert json.loads(capsys.readouterr().out.split("CLOCK")[-1].split("\n", 1)[1])["a"] == "caliper"


# ------------------------------------------------------------------ E0-0 clock domain: identity and log-lag
class FakeIdentity:
    """docker exec <c> IDENTITY_PROBE. boots[c] overrides the boot_id; out[c] overrides the whole output;
    fail = containers whose exec fails."""

    def __init__(self, boots=None, out=None, fail=(), tns=None):
        self.boots, self.out, self.fail, self.tns = dict(boots or {}), dict(out or {}), set(fail), dict(tns or {})

    def __call__(self, argv):
        assert argv[:2] == ["docker", "exec"] and argv[3:] == ch.IDENTITY_PROBE
        c = argv[2]
        if c in self.fail:
            return 1, b"", b"Error response from daemon: No such container"
        if c in self.out:
            return 0, self.out[c], b""
        return 0, sf.identity_output(self.boots.get(c, sf.BOOT_ID), time_ns=self.tns.get(c, "time:[4026531834]")), b""


LOCAL_SAME = {"status": "OK", "boot_id": sf.BOOT_ID, "kernel_release": "6.6", "time_ns": "time:[4026531834]"}


def ident(tmp_path, fake, name="i", local=LOCAL_SAME):
    return ch.run_identity(C, tmp_path / name, fake, local=lambda: local)


def test_parse_identity():
    assert ch.parse_identity(sf.identity_output().decode())["boot_id"] == sf.BOOT_ID
    for bad in ("x\ny\n", "NOT-A-UUID\n6.6\nnone\n", f"{sf.BOOT_ID}\n\nnone\n", f"{sf.BOOT_ID.upper()}\n6.6\nnone\n"):
        with pytest.raises(ValueError):
            ch.parse_identity(bad)


def test_one_kernel_clock_is_demonstrated_and_host_membership_recorded(tmp_path):
    r = ident(tmp_path, FakeIdentity())
    assert r["verdict"] == "SAME_KERNEL_CLOCK" and r["criterion"]["status"] == "DEMONSTRATED"
    assert r["host_in_container_clock_domain"] is True and r["status"] == "COMPLETE"
    assert (tmp_path / "i" / "raw" / "caliper.stdout").read_bytes() == sf.identity_output()
    r2 = ident(tmp_path, FakeIdentity(), name="i2", local={"status": "NOT_LINUX", "platform": "win32"})
    assert r2["verdict"] == "SAME_KERNEL_CLOCK" and r2["host_in_container_clock_domain"] is None
    assert kinds(r2) == {"host_outside_clock_domain": 1}


def test_two_kernels_unreadable_members_and_time_namespaces(tmp_path):
    r = ident(tmp_path, FakeIdentity(boots={"caliper": "0" * 8 + sf.BOOT_ID[8:]}))
    assert r["verdict"] == "DIFFERENT_KERNEL_CLOCKS" and r["criterion"]["status"] == "NOT DEMONSTRATED"
    assert r["status"] == "INCONSISTENT" and "different_kernel_clock" in kinds(r)
    r2 = ident(tmp_path, FakeIdentity(fail={"orderer1.example.com"}), name="i2")
    assert r2["verdict"] == "UNVERIFIABLE" and r2["status"] == "INCOMPLETE"
    r3 = ident(tmp_path, FakeIdentity(out={"caliper": b"cat: can't open '/proc/sys/kernel/random/boot_id'\n"}),
               name="i3")
    assert r3["verdict"] == "UNVERIFIABLE" and r3["members"]["caliper"]["status"] == "MALFORMED"
    r4 = ident(tmp_path, FakeIdentity(tns={"caliper": "time:[4026532999]"}), name="i4")
    assert r4["verdict"] == "SAME_KERNEL_CLOCK" and "time_namespace_differs" in kinds(r4)


def _ev(container, fabric_ms, docker_ns, canonical="true"):
    return {"container": container, "fabric_ms": str(fabric_ms), "docker_ns": str(docker_ns), "canonical": canonical}


def test_log_lag_falsification():
    ok = [_ev("o1", 1000, 1000 * 10**6 + 250_000), _ev("o1", 1001, 1001 * 10**6), _ev("p1", 1002, 1002 * 10**6 + 9)]
    r = ch.log_lag(ok)
    assert r["status"] == "PASS" and r["containers"]["o1"]["min_lag_ms"] == 0.0 and r["inversions"] == 0
    bad = ok + [_ev("p1", 1003, 1003 * 10**6 - 1)]       # daemon 1 ns before the Fabric stamp: impossible on one clock
    assert ch.log_lag(bad)["status"] == "FAIL" and ch.log_lag(bad)["containers"]["p1"]["inversions"] == 1
    assert ch.log_lag([_ev("o1", 5, 1, canonical="false"), {"container": "o1", "fabric_ms": "", "docker_ns": "1",
                                                            "canonical": "true"}])["status"] == "NOT EVALUATED"


def test_log_lag_on_real_log_collector_output(tmp_path):
    import e1_log_collector as lc
    d = tmp_path / "logs"
    d.mkdir()
    (d / "o1.stderr.log").write_text("\n".join(sf.orderer_log_line(b, 1_700_000_000_000 + 100 * b)
                                               for b in range(1, 6)) + "\n", encoding="utf-8")
    (d / "o2.stderr.log").write_text("\n".join(sf.orderer_log_line(b, 1_700_000_000_000 + 100 * b, lag_ns=-400_000)
                                               for b in range(1, 6)) + "\n", encoding="utf-8")
    lc.run_parse([lc.LogInput("o1", "orderer", d / "o1.stderr.log"), lc.LogInput("o2", "orderer", d / "o2.stderr.log")],
                 sf.CHANNEL, tmp_path / "parsed")
    r = ch.log_lag(ch.tc.read_csv(tmp_path / "parsed" / "orderer_write.csv"))
    assert r["containers"]["o1"]["inversions"] == 0 and r["containers"]["o2"]["inversions"] == 5
    assert r["status"] == "FAIL"


def test_domain_gate(tmp_path):
    same = ident(tmp_path, FakeIdentity(), name="a")
    same2 = ident(tmp_path, FakeIdentity(), name="b")
    lag_ok = ch.log_lag([_ev("o1", 1, 10**6 + 5)])
    assert ch.domain_gate([same, same2], lag_ok)[0] == "PASS"
    assert ch.domain_gate([same], None) == ("PASS", ch.domain_gate([same], None)[1])
    assert "log-lag not evaluated" in ch.domain_gate([same], None)[1]
    assert ch.domain_gate([], lag_ok)[0] == "NOT EVALUATED"
    rebooted = ident(tmp_path, FakeIdentity(boots={c: "1" * 8 + sf.BOOT_ID[8:] for c in C}), name="c")
    assert ch.domain_gate([same, rebooted], lag_ok)[0] == "FAIL"           # start and end on different boots
    assert ch.domain_gate([same, ident(tmp_path, FakeIdentity(fail={"caliper"}), name="d")], lag_ok)[0] == "FAIL"
    assert ch.domain_gate([same], ch.log_lag([_ev("o1", 1, 10)]))[0] == "FAIL"


def test_identity_cli(tmp_path, capsys):
    assert ch.main(["identity", "--container", "caliper", "--out-dir", str(tmp_path / "d")]) == 0
    assert "dry run" in capsys.readouterr().out and not (tmp_path / "d").exists()
    assert ch.main(["identity", "--container", "caliper", "--out-dir", str(tmp_path / "x"), "--execute"],
                   runner=FakeIdentity()) == 0
    assert ch.main(["identity", "--container", "caliper", "--out-dir", str(tmp_path / "x"), "--execute"],
                   runner=FakeIdentity()) == 2
