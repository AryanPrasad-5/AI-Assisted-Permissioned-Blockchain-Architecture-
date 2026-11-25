"""The machine-readable protocol is generated from the code the experiment runs, deterministically, with predictions fixed before any data."""
import json

import pytest

from Fabric_Live_Testbed.closed_loop_x1 import freeze_protocol_x1 as F
from Fabric_Live_Testbed.closed_loop_x1 import protocol_x1 as X
from Fabric_Live_Testbed.closed_loop_x1 import stage_x1 as ST


def test_protocol_document_is_deterministic_and_carries_every_constant_and_the_run_order():
    d1, d2 = F.build("a  b\nc  d\n"), F.build("a  b\nc  d\n")
    assert json.dumps(d1, sort_keys=True) == json.dumps(d2, sort_keys=True)
    assert d1["constants"]["CONFIGS"] == (2, 5, 10) and d1["constants"]["LPC_PROTOCOL_SHA256"] == X.LPC_PROTOCOL_SHA256
    assert d1["run_specs"] == ST.run_specs() and d1["source_freeze_files"] == 2
    assert F.build("x  y\n")["source_freeze_sha256"] != d1["source_freeze_sha256"]


def test_predictions_follow_the_stored_x0_fit_and_name_their_falsifiers():
    p = F.predictions()
    assert p["H1_latency_model"]["predicted_p95_ms"]["5"]["10"] == 474.0 and p["H1_latency_model"]["predicted_p95_ms"]["2"]["20"] == 114.0
    dev = F.latency_model()["relative_deviation_measured_over_model"]["10"]
    assert max(abs(v) for v in dev.values()) < 0.031                     # B=10 stored X0 values are reproduced by the model within 3.1%
    assert all("falsified_if" in p[k] for k in ("H1_latency_model", "H2_saturation", "H3_decision")) and "HYPOTHESES" in p["label"]


def test_main_writes_once_and_refuses_to_overwrite(tmp_path, capsys):
    f = tmp_path / "src.txt"
    f.write_text("a  b\n")
    assert F.main(["--source-freeze-file", str(f), "--out", str(tmp_path / "o")]) == 0
    assert (tmp_path / "o" / "PROTOCOL_X1_v1.json").exists() and "sha256" in capsys.readouterr().out
    with pytest.raises(FileExistsError):
        F.main(["--source-freeze-file", str(f), "--out", str(tmp_path / "o")])
