import hashlib
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
for p in (ROOT, ROOT / "src", ROOT / "scripts" / "e1"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from Fabric_Live_Testbed.closed_loop_m2 import freeze_protocol_m2 as F  # noqa: E402
from Fabric_Live_Testbed.closed_loop_m2 import protocol_m2 as P  # noqa: E402
from Fabric_Live_Testbed.closed_loop_m2.tests.test_protocol_m2 import RANKED, x1_doc  # noqa: E402


def test_protocol_twin_is_written_once_hashed_and_carries_the_pin_and_the_design(tmp_path):
    design = P.design_from_x1(x1_doc(), RANKED)
    doc = F.build_doc(design, {"x1_decision_sha256": "a" * 64})
    out = tmp_path / "p" / "PROTOCOL_M2_v1.json"
    digest = F.write_doc(out, doc)
    assert digest == hashlib.sha256(out.read_bytes()).hexdigest()
    twin = json.loads(out.read_text())
    assert twin["protocol"]["protocol"] == P.PROTOCOL_ID and twin["protocol"]["menu_B"] == [2, 5, 10]
    assert twin["actuator_range_policy"]["frozen_actuator_sha256"] and twin["actuator_range_policy"]["menu_B"] == [2, 5, 10]
    assert twin["derived_design"]["window_start_s"] == 20400 and twin["derived_design"]["reactive"]["default_B"] == 2
    assert len(twin["derived_design"]["surface_points"]) == 30
    with pytest.raises(FileExistsError):
        F.write_doc(out, doc)
    assert F.write_doc(tmp_path / "again.json", doc) == digest                          # deterministic bytes
