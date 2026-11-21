"""Task A0e (2026-11-21), Workstream B: the single operator-parameter contract. Offline; no file is read except the template."""

import ast
import copy
import json
import math
from pathlib import Path

import pytest

from tests.a0e_support import CHANNEL, DROP, GOOD_PARAMS, REPO, params
from fabric_perf.controllers import channel_selection as cs
from fabric_perf.controllers import prelive_contract as pc

TEMPLATE = REPO / "Integration" / "system_integration" / "a0e_2026-11-21" / "prelive_params.template.json"


def issues_of(doc) -> list[pc.ParameterIssue]:
    with pytest.raises(pc.PreliveParameterError) as caught:
        pc.validate_operator_parameters(doc)
    return caught.value.issues


def codes_for(doc, field) -> set[str]:
    return {i.code for i in issues_of(doc) if i.field == field}


def edited(path: str, value):
    """GOOD_PARAMS with one nested value replaced (or removed when value is Ellipsis)."""
    doc = copy.deepcopy(GOOD_PARAMS)
    node = doc
    *parents, leaf = path.split(".")
    for part in parents:
        node = node[part]
    if value is Ellipsis:
        node.pop(leaf)
    else:
        node[leaf] = value
    return doc


# ---------------------------------------------------------------- the good set
def test_the_good_parameters_pass_and_round_trip():
    validated = pc.validate_operator_parameters(GOOD_PARAMS)
    assert validated.values["channel"] == CHANNEL and validated.values["stage3"]["max_consecutive_abstentions"] == 3
    again = pc.validate_operator_parameters(validated.values)            # the stored copy must validate again unchanged
    assert dict(again.values) == dict(validated.values)
    assert validated.window_margin_s == pytest.approx(3.0) and validated.planned_round_s == 5.0 and validated.collector_span_s == 19.0
    doc = validated.as_dict()
    assert "no scientific justification" in doc["basis"] and doc["derived"]["initialization"] == "bounded_by_operator"


def test_nothing_in_the_contract_has_a_default_value():
    """Removing any one required key is an error that names the key (no silent fill-in)."""
    required = [(section, key) for section, keys in pc.SECTION_REQUIRED.items() for key in keys]
    assert len(required) >= 15
    for section, key in required:
        assert "missing" in codes_for(edited(f"{section}.{key}", Ellipsis), f"{section}.{key}"), (section, key)
    for top in pc.REQUIRED_TOP:
        if top != "schema":
            assert "missing" in codes_for(params(**{top: DROP}), top), top


def test_the_template_file_is_rejected_because_every_value_is_unset():
    template = json.loads(TEMPLATE.read_text(encoding="utf-8"))
    found = issues_of(template)
    assert len(found) >= 15                                              # it lists every problem, not only the first
    assert {"missing", "wrong_type"} & {i.code for i in found}
    assert all(value is None or isinstance(value, (dict, list, str)) for value in template.values())
    assert "__OPERATOR" in TEMPLATE.read_text(encoding="utf-8")          # the placeholders say whose decision each value is


def test_all_problems_are_reported_together_not_one_at_a_time():
    doc = params(channel="e1-r002")
    doc["ledger_collector"]["poll_interval_s"] = 0
    doc["stage3"]["max_consecutive_abstentions"] = 0
    fields = {i.field for i in issues_of(doc)}
    assert {"channel", "ledger_collector.poll_interval_s", "stage3.max_consecutive_abstentions"} <= fields


def test_a_non_object_root_and_unknown_keys_are_refused():
    assert issues_of([1, 2])[0].code == "wrong_type"
    doc = params()
    doc["surprise"] = 1
    assert "unknown_key" in codes_for(doc, "surprise")
    doc = params()
    doc["cell"]["extra"] = 1
    assert "unknown_key" in codes_for(doc, "cell.extra")
    assert "schema" in codes_for(params(schema="prelive_params/2"), "schema")


# ---------------------------------------------------------------- numeric rules (parametrised)
NUMERIC = [("ledger_collector.poll_interval_s", False), ("telemetry_adapter.window_s", False), ("cell.rate_tps", False),
           ("ledger_collector.max_polls", True), ("cell.max_message_count", True), ("cell.preferred_max_bytes", True),
           ("cell.duration_s", True)]
BAD_NUMBERS = [("zero", 0, {"not_positive"}), ("negative", -1, {"not_positive"}), ("nan", float("nan"), {"nonfinite"}),
               ("inf", float("inf"), {"nonfinite"}), ("true", True, {"wrong_type"}), ("string", "5", {"wrong_type"}),
               ("none", None, {"missing"}), ("list", [1], {"wrong_type"})]


@pytest.mark.parametrize("path,integer", NUMERIC)
@pytest.mark.parametrize("label,value,expected", BAD_NUMBERS)
def test_every_numeric_parameter_refuses_unusable_values(path, integer, label, value, expected):
    doc = edited(path, value)
    if path == "cell.duration_s" and value is None:
        doc["cell"]["tx_count"] = None                                   # None means "not given" here, which the xor rule reports
        assert "exactly_one_required" in codes_for(doc, "cell.duration_s/cell.tx_count")
        return
    assert codes_for(doc, path) & expected, (path, label)


@pytest.mark.parametrize("path", [p for p, integer in NUMERIC if integer])
def test_integer_parameters_refuse_a_fractional_value(path):
    assert "not_integer" in codes_for(edited(path, 2.5), path)


def test_float_parameters_accept_small_positive_values_and_integers_in_float_form():
    doc = edited("ledger_collector.poll_interval_s", 0.001)
    doc["telemetry_adapter"]["window_s"] = 0.002
    doc["recorder"] = {}
    doc["ledger_collector"]["max_polls"] = 100000
    assert pc.validate_operator_parameters(doc).window_margin_s == pytest.approx(0.001)
    doc = edited("ledger_collector.max_polls", 20.0)
    assert pc.validate_operator_parameters(doc).values["ledger_collector"]["max_polls"] == 20.0


def test_warmup_may_be_zero_but_not_negative():
    assert pc.validate_operator_parameters(edited("cell.warmup_s", 0)).planned_round_s == 5.0
    assert "negative" in codes_for(edited("cell.warmup_s", -1), "cell.warmup_s")


# ---------------------------------------------------------------- window / poll ordering and units
def test_adapter_window_must_be_strictly_longer_than_the_poll_interval():
    for window in (1.0, 0.5):                                            # equal and shorter
        doc = edited("telemetry_adapter.window_s", window)
        assert "window_not_longer_than_poll" in codes_for(doc, "telemetry_adapter.window_s")
    just = edited("telemetry_adapter.window_s", 1.0000001)
    assert pc.validate_operator_parameters(just).window_margin_s == pytest.approx(1e-7)


def test_the_window_margin_is_reported_not_judged():
    margin = pc.validate_operator_parameters(edited("telemetry_adapter.window_s", 1.5)).as_dict()["derived"]
    assert margin["window_margin_s"] == pytest.approx(0.5) and "not judged" in margin["window_margin_note"]


@pytest.mark.parametrize("section,key,expected", [
    ("telemetry_adapter", "window_ms", "window_s"), ("telemetry_adapter", "window", "window_s"),
    ("ledger_collector", "poll_interval_ms", "poll_interval_s"), ("ledger_collector", "poll_interval", "poll_interval_s"),
    ("cell", "duration_ms", "duration_s"), ("cell", "warmup_ms", "warmup_s"), ("recorder", "live_flush_interval_s", "live_flush_interval_ms")])
def test_a_key_with_the_wrong_unit_is_refused_and_names_the_right_one(section, key, expected):
    doc = params()
    doc[section][key] = 1000
    found = [i for i in issues_of(doc) if i.field == f"{section}.{key}"]
    assert found and found[0].code == "unit_mismatch" and expected in found[0].message and "nothing is converted" in found[0].message


def test_a_value_typed_in_milliseconds_into_a_seconds_field_is_caught_by_the_unit_slip_guard():
    doc = edited("ledger_collector.poll_interval_s", 5000)
    doc["telemetry_adapter"]["window_s"] = 6000
    found = issues_of(doc)
    assert {"unit_slip_suspected"} == {i.code for i in found if i.field in ("ledger_collector.poll_interval_s", "telemetry_adapter.window_s")}
    assert all("Nothing is converted" in i.message for i in found if i.code == "unit_slip_suspected")
    at_ceiling = edited("ledger_collector.poll_interval_s", pc.UNIT_SLIP_CEILINGS["poll_interval_s"])
    at_ceiling["telemetry_adapter"]["window_s"] = pc.UNIT_SLIP_CEILINGS["window_s"]
    at_ceiling["ledger_collector"]["max_polls"] = 2
    at_ceiling["cell"]["duration_s"] = 1
    assert pc.validate_operator_parameters(at_ceiling).window_margin_s > 0           # the ceiling itself is allowed


def test_the_recorder_flush_must_be_shorter_than_the_window_and_respect_v2_minimums():
    assert "flush_not_shorter_than_window" in codes_for(edited("recorder.live_flush_interval_ms", 4000), "recorder.live_flush_interval_ms")
    assert "flush_not_shorter_than_window" in codes_for(edited("recorder.live_flush_interval_ms", 4001), "recorder.live_flush_interval_ms")
    assert "below_minimum" in codes_for(edited("recorder.live_flush_interval_ms", 9), "recorder.live_flush_interval_ms")
    assert "below_minimum" in codes_for(edited("recorder.live_flush_rows", 0), "recorder.live_flush_rows") or \
        "not_positive" in codes_for(edited("recorder.live_flush_rows", 0), "recorder.live_flush_rows")
    assert pc.validate_operator_parameters(edited("recorder.live_flush_interval_ms", 10)).values["recorder"]["live_flush_interval_ms"] == 10
    doc = params()
    doc["recorder"] = {}
    assert pc.validate_operator_parameters(doc).values["recorder"] == {}                # the live options are optional
    doc = params(recorder=None)
    assert pc.validate_operator_parameters(doc).values["recorder"] == {}


def test_the_collector_must_outlast_the_planned_round():
    doc = edited("ledger_collector.max_polls", 5)                                       # (5-1)*1.0 = 4 s < 5 s round
    assert "collector_stops_before_round_ends" in codes_for(doc, "ledger_collector.max_polls")
    ok = edited("ledger_collector.max_polls", 6)                                        # exactly 5 s
    assert pc.validate_operator_parameters(ok).collector_span_s == 5.0
    warm = edited("cell.warmup_s", 20)                                                  # 20 + 5 + 10 settle = 35 s
    assert "collector_stops_before_round_ends" in codes_for(warm, "ledger_collector.max_polls")
    warm["ledger_collector"]["max_polls"] = 36                                          # (36-1)*1.0 = 35 s, exactly the round
    assert pc.validate_operator_parameters(warm).planned_round_s == 35.0


def test_a_run_length_is_never_assumed():
    both = edited("cell.tx_count", 10)
    assert "exactly_one_required" in codes_for(both, "cell.duration_s/cell.tx_count")
    neither = edited("cell.duration_s", None)
    assert "exactly_one_required" in codes_for(neither, "cell.duration_s/cell.tx_count")
    count = edited("cell.duration_s", None)
    count["cell"]["tx_count"] = 20
    assert pc.validate_operator_parameters(count).planned_round_s == pytest.approx(20 / 4.0)
    assert "wrong_type" in codes_for(edited("cell.workload", 3), "cell.workload") or "not_allowed" in codes_for(edited("cell.workload", 3), "cell.workload")
    assert "not_allowed" in codes_for(edited("cell.workload", "burst"), "cell.workload")


# ---------------------------------------------------------------- Stage 3 limits are separate
@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "3", None, [3]])
def test_the_abstention_limit_is_validated_by_the_stage3_contract(bad):
    assert "invalid" in codes_for(edited("stage3.max_consecutive_abstentions", bad), "stage3.max_consecutive_abstentions")


@pytest.mark.parametrize("bad", [0, -2, True, 2.5, "2"])
def test_the_initialization_limit_is_validated_separately(bad):
    doc = edited("stage3.max_initialization_frames", bad)
    assert codes_for(doc, "stage3.max_initialization_frames") == {"invalid"}
    assert not codes_for(doc, "stage3.max_consecutive_abstentions")                      # the other limit is fine and not blamed


def test_both_limits_bad_gives_two_separate_issues():
    doc = edited("stage3.max_initialization_frames", 0)
    doc["stage3"]["max_consecutive_abstentions"] = 0
    fields = {i.field for i in issues_of(doc)}
    assert {"stage3.max_consecutive_abstentions", "stage3.max_initialization_frames"} <= fields


def test_initialization_may_be_disabled_only_by_an_explicit_null():
    disabled = pc.validate_operator_parameters(edited("stage3.max_initialization_frames", None))
    assert disabled.values["stage3"]["max_initialization_frames"] is None and disabled.as_dict()["derived"]["initialization"] == "disabled_by_operator"
    assert "missing" in codes_for(edited("stage3.max_initialization_frames", Ellipsis), "stage3.max_initialization_frames")


# ---------------------------------------------------------------- channel
@pytest.mark.parametrize("value,reason", [(None, cs.MISSING), ("", cs.EMPTY), ("   ", cs.EMPTY), (5, cs.WRONG_TYPE),
                                          ("e1-r002", cs.PROTECTED), ("E1-CFG001", cs.PROTECTED), (" e1-cfg001 ", cs.PROTECTED),
                                          ("e1-b001", cs.HISTORICAL), ("e1-ql01-b40-l1", cs.HISTORICAL), ("e1-cp01-b45-l2", cs.HISTORICAL),
                                          ("e1-rp01-b40-r1", cs.HISTORICAL), ("mychannel", cs.MALFORMED), ("e1-", cs.MALFORMED),
                                          ("e1-UPPER", cs.MALFORMED), ("e1-" + "x" * 41, cs.MALFORMED), ("e1-ok\n", cs.MALFORMED)])
def test_the_channel_is_vetted_by_the_a0b_rules(value, reason):
    found = [i for i in issues_of(edited("channel", value)) if i.field == "channel"]
    assert found and found[0].code == reason


def test_a_missing_channel_key_has_no_default_and_no_fallback_suggestion():
    found = [i for i in issues_of(params(channel=DROP)) if i.field == "channel"]
    assert found and found[0].code == cs.MISSING
    text = " ".join(i.message for i in found)
    for protected in cs.PROTECTED_REFERENCE_CHANNELS:
        assert protected not in text


@pytest.mark.parametrize("value", ["e1-r002", "e1-cfg001", "e1-b001", "e1-ql01-b40-l1"])
def test_refusal_messages_never_recommend_another_protected_or_old_channel(value):
    message = " ".join(i.message for i in issues_of(edited("channel", value)) if i.field == "channel")
    named = [name for name in (*cs.PROTECTED_REFERENCE_CHANNELS, *cs.HISTORICAL_EVIDENCE_CHANNELS) if name in message]
    assert len(named) <= 1 and (not named or named[0] == value)                          # only the refused name itself appears


def test_the_channel_is_not_a_fabric_block_configuration_parameter():
    doc = params()
    doc["cell"]["channel"] = CHANNEL
    found = [i for i in issues_of(doc) if i.field == "cell.channel"]
    assert found and found[0].code == "channel_in_block_configuration" and "top-level 'channel'" in found[0].message


def test_a_run_id_may_not_smuggle_in_a_protected_channel_name():
    assert "protected_name_in_run_id" in codes_for(edited("run_id", "e1-r002-run"), "run_id")
    assert "protected_name_in_run_id" in codes_for(edited("run_id", "E1-CFG001"), "run_id")
    for suffix in ("r002", "cfg001", "R002", "CFG001"):                                  # the renderer prefixes 'e1-': these become protected names
        assert "protected_name_in_run_id" in codes_for(edited("run_id", suffix), "run_id"), suffix
    # the recorder-v2 wrapper scans generated files for the protected names as SUBSTRINGS, so 'e1-r0021' would be refused later;
    # the contract mirrors that rule and refuses it first
    for suffix in ("r0021", "cfg0010"):
        assert "protected_name_in_run_id" in codes_for(edited("run_id", suffix), "run_id"), suffix
    for fine in ("a0e-smoke-001", "r001", "cfg002", "x"):
        assert pc.validate_operator_parameters(edited("run_id", fine)).values["run_id"] == fine
    assert "malformed" in codes_for(edited("run_id", "has space"), "run_id")
    assert "malformed" in codes_for(edited("run_id", "x" * 41), "run_id")


# ---------------------------------------------------------------- telemetry sources
@pytest.mark.parametrize("value,code", [(None, "missing"), ([], "missing"), ("orderer1=http://h:1/m", "missing"),
                                        (["noequals"], "malformed"), (["a=ftp://h/m"], "malformed"), ([5], "malformed"),
                                        (["a=http://h:1/m", "a=http://h:2/m"], "duplicate")])
def test_prometheus_endpoints_must_be_declared_and_well_formed(value, code):
    found = {i.code for i in issues_of(edited("telemetry_sources.prometheus_endpoints", value))
             if i.field.startswith("telemetry_sources.prometheus_endpoints")}
    assert code in found


def test_the_latency_mode_is_one_of_the_adapters_modes():
    assert "not_allowed" in codes_for(edited("telemetry_adapter.recorder_latency", "guess"), "telemetry_adapter.recorder_latency")
    for mode in pc.RECORDER_LATENCY_MODES:
        assert pc.validate_operator_parameters(edited("telemetry_adapter.recorder_latency", mode))


def test_the_contract_modes_equal_the_adapters_modes():
    from fabric_perf.controllers.telemetry import ContinuousCaliperLedgerAdapter
    assert pc.RECORDER_LATENCY_MODES == ContinuousCaliperLedgerAdapter.RECORDER_LATENCY_MODES


def test_the_contract_module_reads_no_environment_and_starts_no_process():
    tree = ast.parse((REPO / "src/fabric_perf/controllers/prelive_contract.py").read_text(encoding="utf-8"))
    names = {n.names[0].name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import)} | \
            {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    assert not names & {"os", "subprocess", "socket", "urllib", "http", "pathlib", "shutil"}

