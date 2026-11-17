"""Owner decisions D2 and D5 (2026-11-17).

D2: the warm-up round and the measured round stay distinguishable and separately counted.
D5: three separate research stages, each with its own acceptance criteria and authorization;
    no controller can change live Fabric configuration; reports label observed, replayed and predicted values.
"""

import ast
import csv
import json
import sys
from collections import Counter, defaultdict
from dataclasses import replace
from pathlib import Path

import pytest

from fabric_perf.controllers import (
    FabricConfig,
    IntegratedControllerRunner,
    PredictiveController,
    ReactiveController,
    StaticController,
    TelemetryFrame,
)
from fabric_perf.controllers.base import ControllerState, PerformancePrediction
from fabric_perf.controllers.harness import CallableActuator, RecordingActuator, ShadowActuator, VerifiedCallableActuator
from fabric_perf.controllers.live_integration import IntegrationSafetyError
from fabric_perf.controllers.research_stages import (
    STAGE_CONTRACTS,
    EvidenceClass,
    ResearchStage,
    StageAuthorization,
    StageAuthorizationError,
    StageViolation,
    assert_may_emit,
    assert_no_live_actuation,
    require_authorization,
)

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts" / "e1"))
import e1_la2_render as lr  # noqa: E402

S1, S2, S3 = ResearchStage.CALIPER_VALIDATION, ResearchStage.OFFLINE_REPLAY, ResearchStage.LIVE_SHADOW
CONFIG = FabricConfig(max_message_count=45)
ENV = {"test_network": "/home/aryan/fabric/fabric-samples/test-network", "fabric_bin": "/home/aryan/fabric/bin",
       "fabric_cfg": "/home/aryan/fabric/fabric-samples/config", "repo": "/mnt/c/OJT_SEM3/repo",
       "caliper_workspace": "/home/aryan/e1/caliper-ws", "chaincode": {"name": "e1asset", "version": "1.0", "sequence": 1},
       "protocol": {"event_strategy": "msp_any", "invoke_timeout_s": 60, "binding": "fabric:2.5"}}
SNAPSHOT = REPO / "experiments/results/e1/phase5_2_throughput_semantics_20261111/source_snapshots/e1_raw/ft01r002"
RUN_ID = "cbv-new-20261118T0900Z"


# ------------------------------------------------------ D2: warm-up and measured rounds
def rounds_for(warmup_s, duration_s=10):
    files = lr.caliper_files(ENV, RUN_ID, "e1-validation", None, 10.0, warmup_s, "create", duration_s)
    return files["benchmark.yaml"]["test"]["rounds"]


def test_the_rendered_benchmark_has_a_distinct_warmup_round_and_measured_round():
    warmup, measured = rounds_for(5)
    assert (warmup["label"], measured["label"]) == (f"wu-{RUN_ID}", f"m-{RUN_ID}")
    assert (warmup["txDuration"], measured["txDuration"]) == (5, 10)
    assert warmup["rateControl"] == measured["rateControl"] == {"type": "fixed-rate", "opts": {"tps": 10.0}}
    assert warmup["workload"]["arguments"]["settleSeconds"] == 0
    assert measured["workload"]["arguments"]["settleSeconds"] == 10      # a quiet period longer than the 2 s batch timer
    assert warmup["workload"]["arguments"]["runId"] == measured["workload"]["arguments"]["runId"] == RUN_ID


def test_the_plan_records_which_round_is_the_measured_one():
    cell = {"B": 45, "tau": "2s", "P": 10485760, "n": 0, "mode": "txDuration", "rate_tps": 10.0, "duration_s": 10}
    plan = lr.run_plan(ENV, "/mnt/x/network.json", RUN_ID, "e1-validation", cell, 5, True)["caliper"]
    assert (plan["warmup_round"], plan["measured_round"]) == (0, 1)
    assert (plan["warmup_label"], plan["measured_label"]) == (f"wu-{RUN_ID}", f"m-{RUN_ID}")
    none = lr.run_plan(ENV, "/mnt/x/network.json", RUN_ID, "e1-validation", cell, 0, True)["caliper"]
    assert none["warmup_label"] is None and none["measured_round"] == 0
    assert [r["label"] for r in rounds_for(0)] == [f"m-{RUN_ID}"]


def test_the_recorder_writes_one_file_set_per_round_and_stamps_round_and_label_on_every_row():
    source = (REPO / "scripts" / "e1" / "caliper" / "e1_per_tx_recorder.js").read_text(encoding="utf-8")
    assert "_r${this.round}_${label}" in source                          # one file set per round, named by label
    assert "'obs_hr_ns', 'worker', 'round', 'label'" in source           # columns of every recorder row
    assert "this._reset(roundIndex, roundLabel)" in source               # counters restart for each round
    assert "round: this.round, label: this.label" in source              # and the manifest names its round


@pytest.mark.skipif(not SNAPSHOT.exists(), reason="stored ft01r002 snapshot not present")
def test_stored_run_blocks_partition_cleanly_into_warmup_and_measured_by_transaction_id():
    recorder = list(csv.DictReader((SNAPSHOT / "recorder.csv").open(newline="", encoding="utf-8")))
    ledger = list(csv.DictReader((SNAPSHOT / "ledger.csv").open(newline="", encoding="utf-8")))
    manifest = json.loads((SNAPSHOT / "manifest.json").read_text(encoding="utf-8"))
    assert {(r["round"], r["label"]) for r in recorder} == {("1", "m-ft01r002")}      # only the measured round here
    assert manifest["round"] == 1 and manifest["label"] == "m-ft01r002" and manifest["counts"]["submitted"] == 101
    measured_ids = {r["tx_id"] for r in recorder}
    assert len(ledger) == 152 and len(measured_ids) == 101                              # 51 warm-up + 101 measured
    phases = defaultdict(Counter)
    for row in ledger:
        phases[int(row["block"])]["measured" if row["tx_id"] in measured_ids else "warmup"] += 1
    assert all(len(counts) == 1 for counts in phases.values())                          # no block mixes the two phases
    warmup_blocks = {b: dict(c) for b, c in phases.items() if "warmup" in c}
    measured_blocks = {b: dict(c) for b, c in phases.items() if "measured" in c}
    assert sum(c["warmup"] for c in warmup_blocks.values()) == 51 and len(warmup_blocks) == 3
    assert sum(c["measured"] for c in measured_blocks.values()) == 101 and len(measured_blocks) == 5
    assert max(warmup_blocks) < min(measured_blocks)                                    # the warm-up ends before the measured round


# --------------------------------------------------------------- D5: the stage register
def test_there_are_exactly_three_stages_each_with_its_own_acceptance_criteria():
    assert list(STAGE_CONTRACTS) == [S1, S2, S3]
    ids = [criterion_id for c in STAGE_CONTRACTS.values() for criterion_id, _ in c.acceptance]
    assert len(ids) == len(set(ids)) and len(ids) >= 15
    for stage, contract in STAGE_CONTRACTS.items():
        assert contract.stage is stage and len(contract.acceptance) >= 5
        assert all(text.strip() and criterion_id.startswith(f"S{list(STAGE_CONTRACTS).index(stage) + 1}-")
                   for criterion_id, text in contract.acceptance)


def test_no_stage_authorizes_the_next_and_the_authorizations_are_distinct():
    assert not any(c.authorizes_next_stage for c in STAGE_CONTRACTS.values())
    phrases = [c.authorization_phrase.lower() for c in STAGE_CONTRACTS.values()]
    assert len(set(phrases)) == 3 and not any(a in b for a in phrases for b in phrases if a is not b)


def test_stage_1_runs_no_controller_and_the_later_stages_allow_only_non_live_actuators():
    assert STAGE_CONTRACTS[S1].runs_controllers is False and STAGE_CONTRACTS[S1].allowed_actuators == frozenset()
    assert STAGE_CONTRACTS[S2].allowed_actuators == {"ShadowActuator", "RecordingActuator"}
    assert STAGE_CONTRACTS[S3].allowed_actuators == {"ShadowActuator"}
    for contract in STAGE_CONTRACTS.values():
        assert not {"CallableActuator", "VerifiedCallableActuator"} & contract.allowed_actuators


def test_each_stage_can_only_label_what_it_can_truthfully_produce():
    assert_may_emit(S1, EvidenceClass.OBSERVED)
    for refused in (EvidenceClass.REPLAYED, EvidenceClass.PREDICTED, EvidenceClass.PROPOSED):
        with pytest.raises(StageViolation):
            assert_may_emit(S1, refused)
    for allowed in (EvidenceClass.REPLAYED, EvidenceClass.PREDICTED, EvidenceClass.PROPOSED):
        assert_may_emit(S2, allowed)
    with pytest.raises(StageViolation, match="cannot produce observed"):
        assert_may_emit(S2, EvidenceClass.OBSERVED)                       # a replay cannot create an observation
    assert_may_emit(S3, EvidenceClass.OBSERVED)
    with pytest.raises(StageViolation, match="cannot produce replayed"):
        assert_may_emit(S3, EvidenceClass.REPLAYED)                       # live shadow is not a replay


def test_authorization_is_per_stage_and_never_carries_over():
    phrases = {s: STAGE_CONTRACTS[s].authorization_phrase for s in ResearchStage}
    first = StageAuthorization(S1, f"{phrases[S1]} on the dedicated channel.", "2026-11-18T09:00:00Z")
    assert require_authorization(S1, [first]) is first
    for later in (S2, S3):
        with pytest.raises(StageAuthorizationError, match="does not carry over"):
            require_authorization(later, [first])
    wrong_label = StageAuthorization(S2, f"{phrases[S1]}.", "2026-11-18T09:00:00Z")      # text for stage 1 filed under 2
    with pytest.raises(StageAuthorizationError):
        require_authorization(S2, [wrong_label])
    with pytest.raises(StageAuthorizationError):
        require_authorization(S1, [])


def test_one_message_cannot_authorize_several_stages_and_stamps_must_be_dated():
    both = f"{STAGE_CONTRACTS[S1].authorization_phrase} and {STAGE_CONTRACTS[S2].authorization_phrase}."
    with pytest.raises(StageAuthorizationError, match="name one stage"):
        StageAuthorization(S1, both, "2026-11-18T09:00:00Z")
    for bad in ("tomorrow", "2026-11-18T09:00:00", ""):
        with pytest.raises(StageAuthorizationError):
            StageAuthorization(S1, STAGE_CONTRACTS[S1].authorization_phrase, bad)


# ------------------------------------------- D5: no controller can change live Fabric
def noop(_config):
    return {}


def test_the_register_matches_what_the_offline_replay_already_accepts_and_refuses():
    for ok in (ShadowActuator(CONFIG), RecordingActuator(CONFIG)):
        assert_no_live_actuation(S2, [ok])
    live = [CallableActuator(CONFIG, noop), VerifiedCallableActuator(CONFIG, noop, verify_fn=lambda *_: True)]
    for stage in (S2, S3):
        with pytest.raises(StageViolation, match="No controller may change live Fabric"):
            assert_no_live_actuation(stage, live)
    with pytest.raises(StageViolation):
        assert_no_live_actuation(S3, [RecordingActuator(CONFIG)])           # stage 3 is shadow only
    with pytest.raises(StageViolation):
        assert_no_live_actuation(S1, [ShadowActuator(CONFIG)])              # stage 1 runs no controller at all
    # The replay's own guards agree: it refuses a live-enabled runner and any non built-in actuator.
    runner = IntegratedControllerRunner({"static": StaticController(CONFIG)}, CONFIG, run_id="unit", enable_live=True)
    with pytest.raises(IntegrationSafetyError, match="live-enabled"):
        runner.run_offline([{}], source_kind="fabric")
    with pytest.raises(ValueError, match="built-in shadow/recording actuator"):
        IntegratedControllerRunner({"static": StaticController(CONFIG)}, CONFIG, run_id="unit").run_offline(
            [{}], source_kind="fabric", recording_actuators={"static": CallableActuator(CONFIG, noop)})


class FakeModel:
    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


class FakeForecaster:
    def predict(self, history, horizon):
        return 10.0


def frame(index, *, source_age=0.1, latency=100.0):
    stamp = f"2026-11-17T12:00:{index:02d}Z"
    state = ControllerState(timestamp_utc=stamp, current_config=CONFIG, observed_latency_ms=latency,
                            observed_throughput_tps=10.0, observed_arrival_rate_tps=10.0, queue_depth=0.0,
                            telemetry_age_s=0.5, arrival_history_tps=(10.0,), arrival_history_timestamps_utc=(stamp,))
    return TelemetryFrame("r", index, stamp, 1000 + index, state, source_timestamps_utc={"caliper": stamp},
                          freshness_seconds={"caliper": source_age})


def controllers():
    return {"static": StaticController(CONFIG),
            "reactive": ReactiveController(CONFIG, congested_config=replace(CONFIG, max_message_count=40),
                                           latency_high_ms=50, latency_low_ms=10, consecutive_samples=1, cooldown_samples=0),
            "predictive": PredictiveController(FakeModel(), CONFIG, arrival_forecaster=FakeForecaster(),
                                               candidate_configs=[CONFIG, replace(CONFIG, max_message_count=40)])}


def test_abstentions_never_reach_an_actuator_and_proposals_are_never_applied(monkeypatch):
    calls = []
    original = ShadowActuator.apply

    def spy(self, config):
        calls.append(config)
        return original(self, config)

    monkeypatch.setattr(ShadowActuator, "apply", spy)
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    rejected = runner.run([frame(0, source_age=9.0), frame(1, source_age=9.0)])
    assert rejected["abstained_frame_count"] == 2 and rejected["record_count"] == 6
    assert calls == [] and all(r["application"] is None for r in rejected["records"])      # nothing offered to any actuator
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    proposed = runner.run([frame(0), frame(1, source_age=9.0), frame(2)])
    assert calls, "a high-latency frame makes the reactive controller propose a change"
    assert all(r["application"]["error"] == "shadow_mode_no_submission" and r["application"]["accepted"] is False
               for r in proposed["records"] if r["application"])
    assert all(r["configuration_state"]["proposal_applied"] is False for r in proposed["records"])
    assert proposed["live_submission_enabled"] is False and proposed["mode"] == "shadow"
    assert all(type(a).__name__ == "ShadowActuator" for a in runner.actuators.values())


NEW_MODULES = ("telemetry_errors", "telemetry_freshness", "metric_baseline", "config_evidence", "research_stages")
FORBIDDEN_IMPORTS = {"subprocess", "shutil", "requests", "docker", "os", "ctypes", "multiprocessing", "asyncio",
                     "urllib.request", "e1_config_actuator", "fabric_perf.controllers.harness"}
FORBIDDEN_NAMES = {"VerifiedCallableActuator", "CallableActuator", "urlopen", "Popen", "system", "ShadowActuator"}


def imported_and_called(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports, names = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(("." * node.level) + (node.module or ""))
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
    return imports, names


@pytest.mark.parametrize("module", NEW_MODULES)
def test_the_new_modules_cannot_reach_fabric_docker_the_network_or_an_actuator(module):
    imports, names = imported_and_called(REPO / "src" / "fabric_perf" / "controllers" / f"{module}.py")
    assert not imports & FORBIDDEN_IMPORTS, imports & FORBIDDEN_IMPORTS
    assert not names & FORBIDDEN_NAMES, names & FORBIDDEN_NAMES


PROTECTED_CHANNELS = ("e1-r002", "e1-cfg001")


def protected_channel_defaults(path):
    """argparse ``default=`` values and function-parameter defaults that name a protected reference channel."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return []
    hits = []
    for node in ast.walk(tree):
        candidates = []
        if isinstance(node, ast.keyword) and node.arg == "default":
            candidates.append(node.value)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            candidates.extend([*node.args.defaults, *[d for d in node.args.kw_defaults if d is not None]])
        for value in candidates:
            if isinstance(value, ast.Constant) and isinstance(value.value, str) and any(
                    name in value.value for name in PROTECTED_CHANNELS):
                hits.append((str(path.relative_to(REPO)), getattr(value, "lineno", 0), value.value))
    return hits


# Frozen, SHA-256-pinned evidence copies are never edited, so a default in one of them is historical, not live.
FROZEN_EVIDENCE = "Integration/system_integration/observability_recovery_2026-11-17"


def test_no_script_or_module_silently_defaults_to_a_protected_reference_channel():
    """Owner decision D1: e1-r002 and e1-cfg001 stay untouched, so nothing may fall back to them."""
    hits = []
    for root in ("src", "scripts/e1", "Integration", "Fabric_Live_Testbed", "p4_replay"):
        for path in sorted((REPO / root).rglob("*.py")):
            if "__pycache__" in path.parts or path.relative_to(REPO).as_posix().startswith(FROZEN_EVIDENCE):
                continue
            hits.extend(protected_channel_defaults(path))
    assert hits == [], hits
    # Where a channel is derived rather than supplied, it comes from the run id, never from a constant.
    render = (REPO / "scripts" / "e1" / "e1_la2_render.py").read_text(encoding="utf-8")
    assert 'channel = channel or f"e1-{run_id.lower()}"' in render
    for script in ("scripts/e1/build_contemporaneous_transaction_evidence.py",
                   "Fabric_Live_Testbed/controllers/run_production_shadow_validation.py",
                   "Integration/system_integration/collect_live_telemetry.py"):
        text = (REPO / script).read_text(encoding="utf-8")
        assert '"--channel"' in text and "required=True" in text.split('"--channel"', 1)[1].split(")", 1)[0]


def test_the_one_excluded_script_really_is_pinned_evidence():
    evidence = REPO / FROZEN_EVIDENCE
    if not evidence.exists():
        pytest.skip("recovery evidence directory not present")
    pinned = (evidence / "SHA256SUMS.txt").read_text(encoding="utf-8")
    flagged = {p.relative_to(evidence).as_posix() for p in evidence.rglob("*.py") if protected_channel_defaults(p)}
    assert flagged == {"scripts/bounded_scrape_continuity.py"}
    assert all(f"./{name}" in pinned for name in flagged)               # listed in the evidence manifest, hence frozen


@pytest.mark.parametrize("path", [REPO / "scripts/e1/build_contemporaneous_transaction_evidence.py",
                                  REPO / "Integration/system_integration/run_integrated_controller.py"])
def test_the_modified_scripts_do_not_import_the_live_actuator(path):
    imports, names = imported_and_called(path)
    assert "e1_config_actuator" not in imports and not names & {"VerifiedCallableActuator", "CallableActuator", "Popen"}
    assert "subprocess" not in imports
