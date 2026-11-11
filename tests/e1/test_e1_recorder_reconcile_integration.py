                                                                                          

                                                                                                   
                                                                                                 
                                                                                                   
                                                         
   
import csv
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "e1"))

import e1_reconcile as rc              

NODE = shutil.which("node")


@pytest.mark.skipif(NODE is None, reason="Node.js not on PATH: recorder contract NOT VERIFIED")
@pytest.mark.parametrize("n,batch_every", [(200, 0), (205, 25)])
def test_recorder_output_reconciles(tmp_path, n, batch_every):
    out = tmp_path / "rec"
    subprocess.run([NODE, str(REPO / "tests" / "e1" / "js" / "fake_caliper.js"), str(out), "RUNI", str(n),
                    str(batch_every)], check=True, capture_output=True, text=True)
    base = out / "RUNI_w0_r1_m-RUNI"
    with base.with_name(base.name + ".csv").open(newline="", encoding="utf-8") as fh:
        records = list(csv.DictReader(fh))
    assert len(records) == n
    ordered = sorted(records, key=lambda r: (float(r["create_ms"]), int(r["seq_finish"])))
    ledger = [{"tx_id": "cfg", "block": 0, "index": 0, "header_type": 1, "validation_code": 0, "env_bytes": 9000}]
    ledger += [{"tx_id": r["tx_id"], "block": 1 + i // 10, "index": i % 10, "header_type": 3,
                "validation_code": 0, "env_bytes": 3000} for i, r in enumerate(ordered)]
    with (tmp_path / "ledger_tx.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(ledger[0]))
        w.writeheader()
        w.writerows(ledger)
    (tmp_path / "expect.json").write_text(
        '{"n": %d, "max_message_count": 10, "preferred_max_bytes": 524288, "event_strategy": "msp_any", '
        '"invoke_timeout_s": 60, "binding": "fabric:2.5", "expect_block_pattern": true}' % n, encoding="utf-8")
    code = rc.main(["--run", str(base), "--ledger-tx", str(tmp_path / "ledger_tx.csv"),
                    "--expect", str(tmp_path / "expect.json"), "--gate-out", str(tmp_path / "gate.json")])
    assert code == 0, (tmp_path / "gate.json").read_text() if (tmp_path / "gate.json").exists() else "no gate"
