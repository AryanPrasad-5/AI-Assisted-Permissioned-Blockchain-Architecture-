"""Synthetic checks of the E0/E1 run reconciliation. These do not represent E0 or E1 results."""
import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_reconcile as rc  # noqa: E402

ENV_BYTES = 3_000
P_512K = 524_288


def synth_run(n=200, b=10, lam=10.0, warmup=0, fail_every=0, mvcc=(), env_bytes=ENV_BYTES):
    """A synthetic, internally consistent run: recorder rows, submits, manifest, ledger, peer commits.

    Blocks are cut by count every b transactions; the final partial block (n mod b) is cut by the
    timer. fail_every=k makes every k-th transaction an endorsement failure (never ordered). mvcc
    lists indices committed with MVCC_READ_CONFLICT (failed in Caliper, present in the ledger).
    """
    ledger, records, submits, commits = [], [], [], {}
    ledger.append({"tx_id": "cfg0", "block": 0, "index": 0, "header_type": 1, "validation_code": 0,
                   "env_bytes": 9000})
    block, idx = 1, 0
    for w in range(warmup):  # warm-up round, its own count-cut blocks
        ledger.append({"tx_id": f"wu{w:04d}", "block": block, "index": idx, "header_type": 3,
                       "validation_code": 0, "env_bytes": env_bytes})
        idx += 1
        if idx == b:
            block, idx = block + 1, 0
    if idx:
        block, idx = block + 1, 0
    t0 = 1_700_000_000_000.0
    for i in range(n):
        tx_id = f"tx{i:04d}"
        create = t0 + i * 1000.0 / lam
        submits.append({"seq_submit": i, "t_submit_evt_ms": create, "count": 1})
        if fail_every and (i + 1) % fail_every == 0:
            records.append({"tx_id": tx_id, "seq_finish": i, "create_ms": create, "final_ms": create + 40,
                            "status": "failed", "verified": "false", "err": "endorsement failure",
                            "obs_hr_ns": 0, "worker": 0, "round": 1, "label": "m-run"})
            continue
        code = 11 if i in mvcc else 0
        ledger.append({"tx_id": tx_id, "block": block, "index": idx, "header_type": 3,
                       "validation_code": code, "env_bytes": env_bytes})
        records.append({"tx_id": tx_id, "seq_finish": i, "create_ms": create, "final_ms": None,
                        "status": "failed" if code else "success", "verified": "false", "err": "",
                        "obs_hr_ns": 0, "worker": 0, "round": 1, "label": "m-run", "_block": block})
        idx += 1
        if idx == b:
            block, idx = block + 1, 0
    # Commit time of each block: 300 ms after its last create; finals 20 ms after commit.
    last_create: dict[int, float] = {}
    for r in records:
        if "_block" in r:
            last_create[r["_block"]] = max(last_create.get(r["_block"], 0.0), r["create_ms"])
    for blk, t in last_create.items():
        commits[blk] = t + 300.0
    for r in records:
        if "_block" in r:
            r["final_ms"] = commits[r.pop("_block")] + 20.0
    manifest = {
        "counts": {"submitted": n, "finished": n, "pending_at_deactivate": 0, "no_id_records": 0,
                   "duplicate_ids": 0, "success": sum(r["status"] == "success" for r in records),
                   "failed": sum(r["status"] == "failed" for r in records), "array_events": 0},
        "config_in_force": {"event_strategy": "gateway_peer_commit_status", "invoke_timeout_s": 60,
                            "binding": "fabric:2.5"},
    }
    peer = [{"block": k, "commit_ms": v} for k, v in sorted(commits.items())]
    return records, submits, manifest, ledger, peer


def expect(n=200, b=10, **kw):
    base = dict(n=n, max_message_count=b, preferred_max_bytes=P_512K,
                event_strategy="gateway_peer_commit_status", invoke_timeout_s=60, binding="fabric:2.5",
                expect_block_pattern=True)
    base.update(kw)
    return rc.Expectations(**base)


def run(records, submits, manifest, ledger, peer, exp, blocks=None):
    checks = rc.reconcile(records, submits, manifest, ledger, exp, peer, blocks)
    return {c.check_id: c for c in checks}, rc.verdict(checks)


def stringify(rows):
    return [{k: ("" if v is None else str(v)) for k, v in r.items()} for r in rows]


# ---------------------------------------------------------------- clean runs
def test_clean_200_tx_run_reconciles():
    by_id, v = run(*synth_run(), expect())
    assert v == "VALID", {k: c.detail for k, c in by_id.items() if c.status != "PASS"}
    not_pass = {k: c.status for k, c in by_id.items() if c.status != "PASS"}
    assert not_pass == {"E0-1-causes": "NOT EVALUATED"}  # no ledger-reader blocks CSV supplied
    assert "blocks=20" in by_id["E0-1-blocks"].detail


def test_n205_final_block_is_timer_cut_with_five():
    by_id, v = run(*synth_run(n=205), expect(n=205))
    assert v == "VALID"
    assert "sizes_tail=[10, 10, 5]" in by_id["E0-1-blocks"].detail
    assert "'count', 'timer'" in by_id["E0-1-blocks"].detail


def test_csv_strings_reconcile_like_numbers():
    rec, sub, man, led, peer = synth_run()
    _, v = run(stringify(rec), stringify(sub), man, stringify(led), stringify(peer), expect())
    assert v == "VALID"


# ---------------------------------------------------------------- V1 traceable outcome
def test_missing_record_fails_counts():
    rec, sub, man, led, peer = synth_run()
    by_id, v = run(rec[:-1], sub, man, led, peer, expect())
    assert v == "INVALID" and by_id["V1-counts"].status == "FAIL"


def test_duplicate_tx_id_fails():
    rec, sub, man, led, peer = synth_run()
    rec[5] = dict(rec[5], tx_id=rec[4]["tx_id"])
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V1-duplicates"].status == "FAIL"


def test_error_path_truncation_is_detected():
    """E0-2c: the worker throws mid-round, skips the drain, and in-flight results are dropped."""
    rec, sub, man, led, peer = synth_run()
    man["counts"].update(finished=150, pending_at_deactivate=50)
    by_id, v = run(rec[:150], sub, man, led, peer, expect())
    assert v == "INVALID" and by_id["V1-counts"].status == "FAIL"
    assert "pending_at_deactivate=50" in by_id["V1-counts"].detail


def test_no_id_record_fails():
    rec, sub, man, led, peer = synth_run()
    man["counts"]["no_id_records"] = 1
    rec[-1] = dict(rec[-1], tx_id="", status="failed")
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V1-ids"].status == "FAIL"


def test_submit_event_sum_must_equal_submitted():
    rec, sub, man, led, peer = synth_run()
    by_id, _ = run(rec, sub[:-1], man, led, peer, expect())
    assert by_id["V1-counts"].status == "FAIL"


# ---------------------------------------------------------------- V3 ledger reconciliation
def test_success_missing_from_ledger_fails():
    rec, sub, man, led, peer = synth_run()
    led = [t for t in led if t["tx_id"] != "tx0007"]
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V3-success"].status == "FAIL"


def test_success_with_invalid_code_fails():
    rec, sub, man, led, peer = synth_run()
    led = [dict(t, validation_code=11) if t["tx_id"] == "tx0007" else t for t in led]
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V3-success"].status == "FAIL" and "MVCC_READ_CONFLICT" in by_id["V3-success"].detail


def test_foreign_ledger_tx_fails_but_declared_warmup_passes():
    rec, sub, man, led, peer = synth_run(warmup=20)
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V3-foreign"].status == "FAIL"
    warm = {f"wu{i:04d}" for i in range(20)}
    by_id, v = run(rec, sub, man, led, peer, expect(other_round_tx_ids=warm))
    assert v == "VALID", {k: c.detail for k, c in by_id.items() if c.status == "FAIL"}


def test_endorsement_failures_are_not_ordered_e0_2a():
    """E0-2a: every 25th call fails at endorsement (8 of 200); round completes; ledger holds 192."""
    rec, sub, man, led, peer = synth_run(fail_every=25)
    by_id, v = run(rec, sub, man, led, peer, expect(expect_all_success=False, expect_block_pattern=False))
    assert v == "VALID", {k: c.detail for k, c in by_id.items() if c.status == "FAIL"}
    assert "'not_ordered': 8" in by_id["V3-failures"].detail
    assert "ledger_round_tx=192" in by_id["V3-ledger-count"].detail


def test_mvcc_failures_are_found_in_ledger_e0_2b():
    rec, sub, man, led, peer = synth_run(mvcc={3, 4, 5})
    by_id, v = run(rec, sub, man, led, peer, expect(expect_all_success=False))
    assert v == "VALID"
    assert "'committed_invalid:MVCC_READ_CONFLICT': 3" in by_id["V3-failures"].detail


def test_failed_record_that_is_valid_in_ledger_fails():
    rec, sub, man, led, peer = synth_run()
    rec[3] = dict(rec[3], status="failed")
    by_id, _ = run(rec, sub, man, led, peer, expect(expect_all_success=False))
    assert by_id["V3-failures"].status == "FAIL"


def test_ledger_count_must_equal_n_when_all_should_succeed():
    rec, sub, man, led, peer = synth_run(fail_every=25)
    by_id, _ = run(rec, sub, man, led, peer, expect(expect_block_pattern=False))
    assert by_id["V3-ledger-count"].status == "FAIL"


# ---------------------------------------------------------------- V4 ordering
def test_create_after_final_fails():
    rec, sub, man, led, peer = synth_run()
    rec[0] = dict(rec[0], final_ms=rec[0]["create_ms"] - 1)
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V4-order"].status == "FAIL"


def test_decreasing_submit_stamp_fails():
    rec, sub, man, led, peer = synth_run()
    sub[10] = dict(sub[10], t_submit_evt_ms=sub[9]["t_submit_evt_ms"] - 5)
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V4-order"].status == "FAIL"


def test_finish_before_peer_commit_fails_cross_source():
    rec, sub, man, led, peer = synth_run()
    peer = [dict(p, commit_ms=p["commit_ms"] + 100) if p["block"] == 3 else p for p in peer]
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V4-cross-source"].status == "FAIL"


def test_cross_source_not_evaluated_without_peer_log():
    rec, sub, man, led, _ = synth_run()
    by_id, v = run(rec, sub, man, led, None, expect())
    assert by_id["V4-cross-source"].status == "NOT EVALUATED" and v == "VALID"


# ---------------------------------------------------------------- V7 config in force
def test_config_mismatch_fails():
    rec, sub, man, led, peer = synth_run()
    man["config_in_force"]["invoke_timeout_s"] = 30
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["V7-config"].status == "FAIL"


# ---------------------------------------------------------------- cut causes
def test_cut_cause_rule_count_timer_byte_isolated():
    led = [{"tx_id": "c", "block": 0, "index": 0, "header_type": 1, "validation_code": 0, "env_bytes": 1}]
    def add(block, sizes):
        for i, s in enumerate(sizes):
            led.append({"tx_id": f"b{block}_{i}", "block": block, "index": i, "header_type": 3,
                        "validation_code": 0, "env_bytes": s})
    add(1, [100] * 10)          # count (B = 10)
    add(2, [300, 300, 300])     # 900 + next 300 > 1000: byte overflow
    add(3, [300, 300])          # 600 + next 2000 > 1000: cut before an isolated envelope
    add(4, [2000])              # isolated (> P)
    add(5, [100, 100])          # last block: timer
    causes = rc.infer_cut_causes(led, max_message_count=10, preferred_max_bytes=1000)
    assert causes == {1: "count", 2: "byte_overflow", 3: "byte_overflow", 4: "isolated", 5: "timer"}


def test_ledger_reader_cut_cause_disagreement_fails():
    rec, sub, man, led, peer = synth_run(n=205)
    blocks = [{"number": b, "inferred_cut_cause": "count"} for b in range(1, 22)]
    by_id, _ = run(rec, sub, man, led, peer, expect(n=205), blocks=blocks)
    assert by_id["E0-1-causes"].status == "FAIL"
    blocks[-1]["inferred_cut_cause"] = "timer"
    by_id, _ = run(rec, sub, man, led, peer, expect(n=205), blocks=blocks)
    assert by_id["E0-1-causes"].status == "PASS"


def test_unexpected_block_sizes_fail_pattern():
    rec, sub, man, led, peer = synth_run()
    led = [dict(t, block=t["block"] + 1) if t["tx_id"] == "tx0199" else t for t in led]  # split final block
    by_id, _ = run(rec, sub, man, led, peer, expect())
    assert by_id["E0-1-blocks"].status == "FAIL"


# ---------------------------------------------------------------- command line
def _write(path, rows):
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def test_cli_end_to_end(tmp_path):
    rec, sub, man, led, peer = synth_run()
    base = tmp_path / "RUN1_w0_r1_m-RUN1"
    _write(base.with_name(base.name + ".csv"), rec)
    _write(base.with_name(base.name + ".submits.csv"), sub)
    base.with_name(base.name + ".manifest.json").write_text(json.dumps(man), encoding="utf-8")
    _write(tmp_path / "ledger_tx.csv", led)
    _write(tmp_path / "peer_commit.csv", peer)
    exp = dict(n=200, max_message_count=10, preferred_max_bytes=P_512K, event_strategy="gateway_peer_commit_status",
               invoke_timeout_s=60, binding="fabric:2.5", expect_block_pattern=True)
    (tmp_path / "expect.json").write_text(json.dumps(exp), encoding="utf-8")
    args = ["--run", str(base), "--ledger-tx", str(tmp_path / "ledger_tx.csv"), "--expect", str(tmp_path / "expect.json"),
            "--peer-commit", str(tmp_path / "peer_commit.csv"), "--gate-out", str(tmp_path / "gate.json")]
    assert rc.main(args) == 0
    assert json.loads((tmp_path / "gate.json").read_text())["verdict"] == "VALID"
    assert rc.main(args) == 2  # refuses to overwrite an existing gate file


def test_cli_unreadable_input(tmp_path):
    assert rc.main(["--run", str(tmp_path / "missing"), "--ledger-tx", str(tmp_path / "x.csv"),
                    "--expect", str(tmp_path / "e.json")]) == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
