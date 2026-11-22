"""Task A0b (2026-11-20): explicit, validated channel selection for the channel-bound tooling.

Offline only. An autouse fixture makes any attempt to start a process or open a socket or URL fail the test, so
nothing here can reach Fabric, Docker, WSL, Caliper or the network. Where a tool would run an external command, the
test substitutes a fake that writes fixture files.

Owner decision D1 (2026-11-17): ``e1-r002`` and ``e1-cfg001`` stay untouched reference channels; the next validation
runs on a new dedicated channel whose name is not yet approved. These tests therefore never name an approved
channel: acceptance is rule based, and the names used below are arbitrary examples.
"""

import argparse
import ast
import gzip
import hashlib
import importlib.util
import json
import re
import socket
import subprocess
import sys
import types
import urllib.request
from pathlib import Path

import pytest

from fabric_perf.controllers import channel_selection as cs

REPO = Path(__file__).resolve().parents[1]
E1 = REPO / "scripts" / "e1"
sys.path.insert(0, str(E1))

PROTECTED = ("e1-r002", "e1-cfg001")
FROZEN_DIR = REPO / "Integration" / "system_integration" / "observability_recovery_2026-11-17"

# The 38 channels found on the network by the read-only survey of 2026-11-17 (Integration/.../04_fabric_readonly_survey.json).
SURVEYED = (
    ["e1-b001", "e1-b002", "e1-b003", "e1-cfg001", "e1-e00a", "e1-e00a2", "e1-e00b", "e1-e00s", "e1-r001", "e1-r002"]
    + [f"e1-cp{i:02d}-b{b}-l{l}" for i, (b, l) in enumerate(
        [(40, 15), (10, 15), (40, 20), (10, 20), (40, 25), (10, 25), (10, 30), (40, 30), (40, 40), (10, 40)], start=1)]
    + [f"e1-ql{i:02d}-b{b}-l{l}" for i, b, l in
       [(1, 20, 10), (2, 10, 10), (3, 20, 10), (4, 40, 20), (5, 80, 10), (6, 10, 20), (7, 20, 20), (8, 40, 20), (9, 10, 10),
        (10, 40, 10), (11, 80, 20), (12, 80, 20), (13, 10, 20), (14, 40, 10), (15, 80, 10), (16, 20, 20), (17, 40, 10),
        (23, 10, 10)]]
)


@pytest.fixture(autouse=True)
def no_external_access(monkeypatch):
    """Any process start, socket connection or URL open fails the test."""
    def refuse(*_args, **_kwargs):
        raise AssertionError("an external process or network access was attempted")
    for name in ("run", "Popen", "call", "check_call", "check_output"):
        monkeypatch.setattr(subprocess, name, refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(urllib.request, "urlopen", refuse)


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def refusal(call, *args, **kwargs):
    with pytest.raises(cs.ChannelSelectionError) as caught:
        call(*args, **kwargs)
    return caught.value


# =============================================================== A. the module's rules
DEDICATED = ["e1-new", "e1-val001", "e1-val.2026-11", "e1-0", "e1-x-y.z", "e1-" + "a" * 40]


@pytest.mark.parametrize("name", DEDICATED)
def test_a_dedicated_channel_is_accepted_and_returned_unchanged(name):
    assert cs.select_experiment_channel(name) == name
    assert cs.select_experiment_channel(name, purpose="a pilot run", option="--validation-channel") == name
    assert cs.select_observed_channel(name) == name


def test_acceptance_is_rule_based_not_a_list_of_approved_names():
    names = [f"e1-v{i:04d}" for i in range(500)] + [f"e1-x{c}{d}" for c in "abcdef" for d in "0123456789"]
    assert all(cs.select_experiment_channel(name) == name for name in names)
    source = (REPO / "src" / "fabric_perf" / "controllers" / "channel_selection.py").read_text(encoding="utf-8")
    assert "e1-new" not in source and "e1-val" not in source           # no future name is hard-coded


def test_a_missing_channel_is_refused_and_no_default_exists():
    error = refusal(cs.select_experiment_channel, None)
    assert error.reason == cs.MISSING and "no default channel exists" in str(error)
    assert refusal(cs.select_observed_channel, None).reason == cs.MISSING


@pytest.mark.parametrize("value", ["", " ", "\t", "\n", "  \r\n  "])
def test_an_empty_or_blank_channel_is_refused(value):
    assert refusal(cs.select_experiment_channel, value).reason == cs.EMPTY
    assert refusal(cs.select_observed_channel, value).reason == cs.EMPTY


@pytest.mark.parametrize("value", [0, 1, 1.5, True, b"e1-new", ["e1-new"], ("e1-new",), {"channel": "e1-new"}, object()])
def test_a_non_string_channel_is_refused(value):
    assert refusal(cs.select_experiment_channel, value).reason == cs.WRONG_TYPE


MALFORMED = ["e1-", "e1", "e1_new", "E1-new", "e1-New", "e1-a_b", "e1-a b", " e1-new", "e1-new ", "\te1-new", "e1-new\n",
             "e1-new\r", "e1-new\x00", "val-001", "mychannel", "e1-" + "a" * 41, "e1-é", "e1-a/b", "e1-a\\b", "e1-a;b",
             "e1-$(x)", "../e1-x", "e1-new?", "e1-new#", "e1-*", "http://e1-new", "e1-new,e1-other", "e1-new e1-other",
             "e1-ｎｅｗ"]


@pytest.mark.parametrize("value", MALFORMED)
def test_a_malformed_channel_is_refused_and_never_repaired(value):
    error = refusal(cs.select_experiment_channel, value)
    assert error.reason == cs.MALFORMED
    assert "e1-" in str(error) and "lower-case" in str(error)


@pytest.mark.parametrize("value", ["e1-new\n", "e1-new\r\n", " e1-new", "E1-new", "e1_new", "1-new", "e1-é"])
def test_the_observation_role_also_rejects_malformed_names(value):
    assert refusal(cs.select_observed_channel, value).reason == cs.MALFORMED


@pytest.mark.parametrize("value", ["e1-r002", "e1-cfg001", "E1-R002", "E1-CFG001", " e1-cfg001", "e1-r002 ", "e1-cfg001\n", "E1-CfG001 "])
def test_the_protected_reference_channels_are_refused_in_every_spelling(value):
    error = refusal(cs.select_experiment_channel, value, purpose="a pilot run", option="--validation-channel")
    assert error.reason == cs.PROTECTED
    text = str(error)
    assert "protected reference channel" in text and "cannot be selected" in text and "dedicated validation channel" in text
    other = "e1-cfg001" if "r002" in value.lower() else "e1-r002"
    assert other not in text                                       # only the offending value is named


def test_no_protected_channel_is_the_result_of_any_call_without_a_channel():
    for value in (None, "", " "):
        with pytest.raises(cs.ChannelSelectionError):
            cs.select_experiment_channel(value)
    assert cs.is_protected_reference_channel("e1-new") is False and cs.is_protected_reference_channel(None) is False
    assert cs.is_protected_reference_channel("E1-R002") and cs.is_protected_reference_channel(" e1-cfg001 ")


@pytest.mark.parametrize("name", [n for n in SURVEYED if n not in PROTECTED] + ["e1-rp01-b10-r1", "e1-rp90-b40-r0", "E1-QL01-B20-L10"])
def test_every_channel_of_an_earlier_experiment_is_refused(name):
    assert refusal(cs.select_experiment_channel, name).reason == cs.HISTORICAL


def test_the_survey_has_38_channels_and_each_is_either_protected_or_historical():
    assert len(SURVEYED) == 38 and len(set(SURVEYED)) == 38
    for name in SURVEYED:
        assert refusal(cs.select_experiment_channel, name).reason in {cs.PROTECTED, cs.HISTORICAL}
    survey = FROZEN_DIR / "raw" / "04_fabric_readonly_survey.json"
    if survey.exists():                                                # cross-check against the frozen evidence itself
        found = set(re.findall(r"\be1-[a-z0-9.\-]+", survey.read_text(encoding="utf-8")))
        assert found == set(SURVEYED)


def test_the_observation_role_may_read_a_protected_channel_but_never_a_missing_one():
    for name in PROTECTED:
        assert cs.select_observed_channel(name) == name
    assert cs.select_observed_channel("e1-ql01-b20-l10") == "e1-ql01-b20-l10"
    assert refusal(cs.select_observed_channel, None).reason == cs.MISSING


def test_a_recorded_channel_must_equal_the_selection():
    assert cs.require_same_channel("e1-new", "e1-new", source="a plan") == "e1-new"
    for recorded in ("e1-other", None, "", "E1-NEW"):
        error = refusal(cs.require_same_channel, "e1-new", recorded, source="the plan file")
        assert error.reason == cs.MISMATCH and "the plan file" in str(error)


ALL_REFUSALS = ([None, "", " ", 0] + MALFORMED[:6] + list(PROTECTED) + ["e1-b001", "e1-ql01-b20-l10"])


@pytest.mark.parametrize("value", ALL_REFUSALS)
def test_no_refusal_message_suggests_a_protected_channel_as_a_fallback(value):
    text = str(refusal(cs.select_experiment_channel, value, purpose="an experiment", option="--channel")).lower()
    for phrase in ("fall back", "fallback", "use e1-r002", "use e1-cfg001", "e1-r002 or", "or e1-cfg001", "instead use e1-"):
        assert phrase not in text
    if value not in PROTECTED:
        assert not any(name in text for name in PROTECTED)          # a refusal never names a protected channel unless it is the value


def test_the_argparse_types_give_a_clean_error_and_the_expected_value(capsys):
    parser = argparse.ArgumentParser(prog="t")
    parser.add_argument("--channel", required=True, type=cs.experiment_channel_arg)
    assert parser.parse_args(["--channel", "e1-new"]).channel == "e1-new"
    for argv, fragment in (([], "required"), (["--channel", ""], "is empty"), (["--channel", "e1-r002"], "protected reference channel"),
                           (["--channel", "E1 x"], "not a valid E1 channel name"), (["--channel", "e1-ql02-b10-l10"], "earlier experiment|naming family")):
        with pytest.raises(SystemExit) as caught:
            parser.parse_args(argv)
        assert caught.value.code == 2 and re.search(fragment, capsys.readouterr().err)
    observed = argparse.ArgumentParser(prog="t")
    observed.add_argument("--channel", required=True, type=cs.observed_channel_arg)
    assert observed.parse_args(["--channel", "e1-cfg001"]).channel == "e1-cfg001"
    with pytest.raises(SystemExit):
        observed.parse_args(["--channel", "E1-x"])


def test_the_command_line_vetting_entry_point(capsys):
    assert cs.main(["--channel", "e1-new"]) == 0
    assert json.loads(capsys.readouterr().out) == {"valid": True, "role": "experiment", "channel": "e1-new", "policy": cs.POLICY_VERSION}
    assert cs.main(["--channel", "e1-r002"]) == 2
    verdict = json.loads(capsys.readouterr().out)
    assert verdict["valid"] is False and verdict["reason"] == cs.PROTECTED
    assert cs.main(["--channel", "e1-r002", "--role", "observation"]) == 0


def test_the_selection_record_says_how_the_channel_was_chosen():
    record = cs.selection_record("e1-new", role=cs.ROLE_EXPERIMENT, option="--channel")
    assert record == {"channel": "e1-new", "role": "experiment", "selected_by": "--channel", "explicit": True,
                      "policy": cs.POLICY_VERSION, "protected_reference_channels_refused": True}
    assert cs.selection_record("e1-x", role=cs.ROLE_OBSERVATION, option="--channel")["protected_reference_channels_refused"] is False


# ------------------------------------------- the module agrees with the tools it replaces
def literal_assignment(path: Path, name: str):
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            value = node.value
            if isinstance(value, ast.Call) and getattr(value.func, "id", "") == "frozenset":
                value = value.args[0]
            return ast.literal_eval(value)
    raise AssertionError(f"{name} not found in {path}")


def test_the_fabric_pattern_is_the_one_channel_run_enforces_and_the_e1_pattern_narrows_it():
    import e1_channel_run as cr
    assert cr.CHANNEL_RE.pattern == "^" + cs.FABRIC_CHANNEL_PATTERN + "$"
    fabric = re.compile(cs.FABRIC_CHANNEL_PATTERN)
    for name in DEDICATED + [f"e1-{c}" for c in "a0.-9z"] + ["e1-" + "z" * 40]:
        assert re.fullmatch(cs.E1_CHANNEL_PATTERN, name) and fabric.fullmatch(name)
    # the project convention the renderer and drivers use is wider; the module narrows it to what Fabric accepts
    assert re.match(r"^e1-[A-Za-z0-9_.-]{1,40}$", "e1-Foo_bar") and not cr.CHANNEL_RE.match("e1-Foo_bar")
    assert not re.fullmatch(cs.E1_CHANNEL_PATTERN, "e1-Foo_bar")


def test_the_protected_set_equals_the_guard_tests_constant():
    guard = literal_assignment(REPO / "tests" / "test_live_shadow_stage_separation_2026_11_17.py", "PROTECTED_CHANNELS")
    assert set(guard) == set(cs.PROTECTED_REFERENCE_CHANNELS) == set(PROTECTED)


def test_the_refused_names_cover_exactly_what_the_p1b_tool_calls_existing():
    existing = literal_assignment(E1 / "e1_quicklook_p1b.py", "EXISTING_CHANNELS")
    assert set(existing) == set(cs.PROTECTED_REFERENCE_CHANNELS) | set(cs.HISTORICAL_EVIDENCE_CHANNELS)
    assert not set(cs.PROTECTED_REFERENCE_CHANNELS) & set(cs.HISTORICAL_EVIDENCE_CHANNELS)


def test_the_reserved_families_refuse_every_name_the_series_drivers_can_generate():
    ql = re.compile(r"^e1-ql(\d\d)-b(\d+)-l(\d+)$")
    cp = re.compile(r"^e1-cp(\d\d)-b(\d+)-l(\d+)$")
    rp = re.compile(r"^e1-rp(\d\d)-b(\d+)-r(\d+)$")
    for pattern, template in ((ql, "e1-ql{i:02d}-b{b}-l{l}"), (cp, "e1-cp{i:02d}-b{b}-l{l}"), (rp, "e1-rp{i:02d}-b{b}-r{l}")):
        for i in range(100):
            for b in (10, 40, 45, 80):
                name = template.format(i=i, b=b, l=10)
                assert pattern.match(name) and refusal(cs.select_experiment_channel, name).reason == cs.HISTORICAL
    # the families used above are the patterns recorded in the drivers' own source files
    for path, text in ((E1 / "e1_quicklook.py", r"e1-ql(\d\d)-b(\d+)-l(\d+)"), (E1 / "e1_quicklook_p1b.py", r"e1-cp(\d\d)-b(\d+)-l(\d+)"),
                       (REPO / "p4_replay" / "driver.py", r"e1-rp(\d\d)-b(\d+)-r(\d+)")):
        assert text in path.read_text(encoding="utf-8"), path.name


ALLOWED_IMPORTS = {"__future__", "argparse", "json", "re", "sys", "typing"}
FORBIDDEN_NAMES = {"open", "exec", "eval", "system", "Popen", "urlopen", "environ", "getenv", "socket", "subprocess", "shutil"}


def test_the_module_is_pure_standard_library_and_cannot_reach_anything():
    tree = ast.parse((REPO / "src" / "fabric_perf" / "controllers" / "channel_selection.py").read_text(encoding="utf-8"))
    imports = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imports |= {(node.module or "").split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert imports <= ALLOWED_IMPORTS, imports
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)} | {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    assert not names & FORBIDDEN_NAMES, names & FORBIDDEN_NAMES


# ===================================================== B. the tools, one by one (offline)
ENV = {"test_network": "/home/aryan/fabric/fabric-samples/test-network", "fabric_bin": "/home/aryan/fabric/bin",
       "fabric_cfg": "/home/aryan/fabric/fabric-samples/config", "repo": "/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-",
       "caliper_workspace": "/home/aryan/e1/caliper-ws", "chaincode": {"name": "e1asset", "version": "1.0", "sequence": 1},
       "protocol": {"event_strategy": "msp_any", "invoke_timeout_s": 60, "binding": "fabric:2.5"}}


def render_plan(tmp_path, run_id, channel, name="plan.json"):
    """A real plan from the (pinned, unmodified) renderer; writes only under tmp_path."""
    import e1_la2_render as lr
    network = tmp_path / "network.json"
    if not network.exists():
        network.write_text(json.dumps(lr.network(ENV, "e1asset_1.0:" + "ab" * 32)), encoding="utf-8")
    plan = tmp_path / name
    lr.render_run(ENV, str(network), run_id, 45, "2s", 10_485_760, 60, 10.0, 0, "create", plan, repo_root=REPO,
                  ws_root=tmp_path / f"ws-{run_id}-{name}", channel=channel)
    return plan


def exit_of(main, argv, capsys):
    with pytest.raises(SystemExit) as caught:
        main(argv)
    return caught.value.code, capsys.readouterr().err


# ------------------------------------------------------------------ the pilot runner
@pytest.fixture()
def pilot():
    import e1_existing_channel_pilot as module
    return module


def test_the_pilot_requires_an_explicit_channel(pilot, tmp_path, capsys):
    code, err = exit_of(pilot.main, ["--plan", str(tmp_path / "plan.json"), "--run-dir", str(tmp_path / "run")], capsys)
    assert code == 2 and "--validation-channel" in err and "required" in err


BAD_FOR_EXPERIMENTS = [("e1-r002", "protected reference channel"), ("e1-cfg001", "protected reference channel"),
                       ("E1-CFG001", "protected reference channel"), ("e1-b001", "earlier experiment"),
                       ("e1-ql05-b80-l10", "naming family"), ("", "is empty"), (" ", "is empty"),
                       ("e1-NEW", "not a valid E1 channel name"), ("val-001", "not a valid E1 channel name"),
                       ("e1-new\n", "not a valid E1 channel name")]


@pytest.mark.parametrize("value, fragment", BAD_FOR_EXPERIMENTS)
@pytest.mark.parametrize("flag", ["--validation-channel", "--channel"])
def test_the_pilot_refuses_a_bad_channel_before_it_even_reads_the_plan(pilot, tmp_path, capsys, flag, value, fragment):
    missing_plan = tmp_path / "does-not-exist.json"                      # reading it would raise FileNotFoundError, not exit 2
    code, err = exit_of(pilot.main, ["--plan", str(missing_plan), flag, value, "--run-dir", str(tmp_path / "run")], capsys)
    assert code == 2 and fragment in err
    assert not (tmp_path / "run").exists()


def test_the_pilot_refuses_a_plan_rendered_for_another_channel(pilot, tmp_path, capsys):
    plan = render_plan(tmp_path, "pa", "e1-pilot-a")
    code, err = exit_of(pilot.main, ["--plan", str(plan), "--validation-channel", "e1-pilot-b", "--run-dir", str(tmp_path / "run")], capsys)
    assert code == 2 and "the channel recorded in the plan file" in err and "'e1-pilot-a'" in err
    assert not (tmp_path / "run").exists()


@pytest.mark.parametrize("flag", ["--validation-channel", "--channel"])
def test_the_pilot_dry_run_accepts_the_matching_dedicated_channel_and_does_nothing_else(pilot, tmp_path, capsys, flag):
    plan = render_plan(tmp_path, "pc", "e1-pilot-c")
    assert pilot.main(["--plan", str(plan), flag, "e1-pilot-c", "--run-dir", str(tmp_path / "run")]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["channel"] == "e1-pilot-c" and printed["note"] == "no channel setup or workload executed"
    assert printed["channel_selection"] == cs.selection_record("e1-pilot-c", role=cs.ROLE_EXPERIMENT, option="--validation-channel")
    assert not (tmp_path / "run").exists()                               # a dry run creates nothing


@pytest.mark.parametrize("run_id", ["r002", "cfg001"])
def test_a_protected_channel_derived_by_the_pinned_renderer_is_caught_by_the_pilot(pilot, tmp_path, capsys, run_id):
    """e1_la2_render.py (pinned) derives e1-<run id> when no channel is given, so --run-id r002 yields a protected name."""
    plan = render_plan(tmp_path, run_id, None, name=f"derived-{run_id}.json")
    derived = json.loads(plan.read_text(encoding="utf-8"))["channel"]
    assert derived == f"e1-{run_id}" and cs.is_protected_reference_channel(derived)       # the renderer's hazard, demonstrated
    code, err = exit_of(pilot.main, ["--plan", str(plan), "--validation-channel", derived, "--run-dir", str(tmp_path / "r1")], capsys)
    assert code == 2 and "protected reference channel" in err
    code, err = exit_of(pilot.main, ["--plan", str(plan), "--validation-channel", "e1-pilot-d", "--run-dir", str(tmp_path / "r2")], capsys)
    assert code == 2 and "the channel recorded in the plan file" in err
    assert not (tmp_path / "r1").exists() and not (tmp_path / "r2").exists()


def test_the_pilot_has_no_allowlist_left_and_still_has_no_module_docstring(pilot):
    assert not hasattr(pilot, "ALLOWED_CHANNELS")
    assert not (pilot.__doc__ or "").strip()                              # tests/e1/test_cli_help_smoke.py depends on this
    assert list(protected_sites(E1 / "e1_existing_channel_pilot.py")) == []


# ------------------------------------------------------------- the synchronized shadow replay
@pytest.fixture()
def replay():
    return load_module(E1 / "synchronized_shadow_replay.py", "shadow_replay_a0b")


def replay_argv(tmp_path, channel, b="45", **extra):
    series = tmp_path / "series.csv.gz"
    if not series.exists():
        with gzip.open(series, "wt", newline="", encoding="utf-8") as fh:
            fh.write("timestamp_utc,arrival_count,frequency_seconds,date_utc\n")
            fh.write("2026-09-21T00:00:00+00:00,2,1,2026-09-21\n2026-09-21T00:00:01+00:00,1,1,2026-09-21\n")
    network = tmp_path / "network.json"
    network.write_text("{}", encoding="utf-8")
    return ["--series", str(series), "--network", str(network), "--run-id", extra.get("run_id", "a0b-1"), "--channel", channel,
            "--expected-max-message-count", b, "--duration-bins", "2", "--out-root", str(tmp_path / "out"),
            "--workspace", str(tmp_path / "ws")]


def test_the_shadow_replay_requires_the_channel_and_a_separate_expected_block_size(replay, tmp_path, capsys):
    code, err = exit_of(replay.main, ["--run-id", "t"], capsys)
    assert code == 2 and re.search(r"required:.*--channel.*--expected-max-message-count", " ".join(err.split()))
    argv = replay_argv(tmp_path, "e1-new")                                 # everything but the expected block size
    del argv[argv.index("--expected-max-message-count"):argv.index("--expected-max-message-count") + 2]
    code, err = exit_of(replay.main, argv, capsys)
    assert code == 2 and re.search(r"required: --expected-max-message-count", " ".join(err.split()))
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("value, fragment", BAD_FOR_EXPERIMENTS)
def test_the_shadow_replay_refuses_a_bad_channel_before_writing_anything(replay, tmp_path, capsys, value, fragment):
    code, err = exit_of(replay.main, replay_argv(tmp_path, value), capsys)
    assert code == 2 and fragment in err
    assert not (tmp_path / "out").exists() and not (tmp_path / "ws").exists()


def test_the_shadow_replay_refuses_a_non_positive_expected_block_size(replay, tmp_path, capsys):
    code, err = exit_of(replay.main, replay_argv(tmp_path, "e1-new", b="0"), capsys)
    assert code == 2 and "positive" in err and not (tmp_path / "out").exists()


def test_the_shadow_replay_prepares_a_dedicated_channel_without_any_table_of_channels(replay, tmp_path, capsys):
    assert replay.main(replay_argv(tmp_path, "e1-new")) == 0                # prepare mode: files only, nothing executed
    manifest = json.loads((tmp_path / "out" / "a0b-1" / "replay_manifest.json").read_text(encoding="utf-8"))
    assert manifest["channel"] == "e1-new" and manifest["expected_max_message_count"] == 45
    assert manifest["channel_selection"]["explicit"] is True and manifest["live_configuration_submission"] is False
    network = json.loads((tmp_path / "ws" / "runs" / "a0b-1" / "networkconfig.yaml").read_text(encoding="utf-8"))
    assert [c["channelName"] for c in network["channels"]] == ["e1-new"]
    assert not hasattr(replay, "CHANNELS")
    assert list(protected_sites(E1 / "synchronized_shadow_replay.py")) == []           # only prose, never a channel constant


# ------------------------------------------------------------------ the capture runner
@pytest.fixture()
def capture():
    return load_module(E1 / "run_contemporaneous_capture.py", "capture_a0b")


def capture_argv(tmp_path, channel):
    return ["--run-id", "a0b-cap", "--channel", channel, "--max-message-count", "45", "--network", str(tmp_path / "absent-network.json"),
            "--out-dir", str(tmp_path / "out"), "--caliper-run", str(tmp_path / "caliper-run")]


@pytest.mark.parametrize("value, fragment", BAD_FOR_EXPERIMENTS)
def test_the_capture_runner_refuses_a_bad_channel_at_argument_parsing(capture, tmp_path, capsys, value, fragment):
    code, err = exit_of(capture.main, capture_argv(tmp_path, value), capsys)
    assert code == 2 and fragment in err
    assert not (tmp_path / "out").exists() and not (tmp_path / "caliper-run").exists()


def test_the_capture_runner_accepts_a_dedicated_channel_and_stops_at_the_first_real_operation(capture, tmp_path):
    with pytest.raises(FileNotFoundError):                                # reading the (absent) network file: validation had passed
        capture.main(capture_argv(tmp_path, "e1-new"))
    assert not (tmp_path / "out").exists() and not (tmp_path / "caliper-run").exists()


def write_template(root: Path, *, network_channel="e1-cfg001", extra=None):
    """A stand-in for the stored template run: JSON written into .yaml files, as the real tools do."""
    root.mkdir(parents=True)
    benchmark = {"test": {"name": "e1-q22c001", "workers": {"number": 1}, "rounds": [
        {"label": "wu-q22c001", "txDuration": 30, "rateControl": {"type": "fixed-rate", "opts": {"tps": 10}},
         "workload": {"module": "x.js", "arguments": {"runId": "q22c001", "settleSeconds": 5}}},
        {"label": "m-q22c001", "txDuration": 120, "rateControl": {"type": "fixed-rate", "opts": {"tps": 10}},
         "workload": {"module": "x.js", "arguments": {"runId": "q22c001", "settleSeconds": 10}}}]},
        "monitors": {"transaction": [{"module": "r.js", "options": {"runId": "q22c001", "outputDir": "o"}}]}}
    (root / "benchmark.yaml").write_text(json.dumps(benchmark), encoding="utf-8")
    (root / "networkconfig.yaml").write_text(json.dumps({"channels": [{"channelName": network_channel, "contracts": [{"id": "e1asset"}]}]}), encoding="utf-8")
    (root / "modules").mkdir()
    (root / "modules" / "e1_create_asset.js").write_text("// workload\n" + (extra or ""), encoding="utf-8")
    return root


def test_the_template_rewrite_names_exactly_the_selected_channel(capture, tmp_path):
    template = write_template(tmp_path / "template")
    capture.copy_run_template(template, tmp_path / "run", "a0b-cap", "e1-new", "/ws")
    network = json.loads((tmp_path / "run" / "networkconfig.yaml").read_text(encoding="utf-8"))
    assert [c["channelName"] for c in network["channels"]] == ["e1-new"]
    assert not any(any(p in f.read_text(encoding="utf-8") for p in PROTECTED) for f in (tmp_path / "run").rglob("*") if f.is_file())


def test_a_template_without_the_placeholder_is_refused_instead_of_silently_keeping_its_own_channel(capture, tmp_path):
    template = write_template(tmp_path / "template", network_channel="e1-some-other-channel")
    error = refusal(capture.copy_run_template, template, tmp_path / "run", "a0b-cap", "e1-new", "/ws")
    assert error.reason == cs.TEMPLATE_NOT_REWRITTEN and "Nothing was run" in str(error)


def test_a_template_that_still_names_a_protected_channel_after_the_rewrite_is_refused(capture, tmp_path):
    template = write_template(tmp_path / "template", extra="const other = 'e1-r002';\n")
    assert refusal(capture.copy_run_template, template, tmp_path / "run", "a0b-cap", "e1-new", "/ws").reason == cs.PROTECTED_REMAINS


def test_the_shadow_replay_takes_the_expected_block_size_from_its_own_argument(replay):
    tree = ast.parse((E1 / "synchronized_shadow_replay.py").read_text(encoding="utf-8"))
    execute = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "execute")
    assigns = [n for n in ast.walk(execute) if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "expected_b" for t in n.targets)]
    assert len(assigns) == 1 and ast.unparse(assigns[0].value) == "args.expected_max_message_count"


def test_the_pilot_records_how_the_channel_was_selected_in_its_dry_run_and_in_its_run_record():
    assert (E1 / "e1_existing_channel_pilot.py").read_text(encoding="utf-8").count('"channel_selection"') == 2


@pytest.mark.parametrize("value", ["e1-r002", "e1-cfg001", "", None, "E1-X"])
def test_the_template_copy_itself_refuses_a_bad_channel_before_copying(capture, tmp_path, value):
    template = write_template(tmp_path / "template")
    with pytest.raises(cs.ChannelSelectionError):
        capture.copy_run_template(template, tmp_path / "run", "a0b-cap", value, "/ws")
    assert not (tmp_path / "run").exists()


# ----------------------------------------------------------------- the final-comparison wrapper
@pytest.fixture()
def comparison(monkeypatch):
    module = load_module(E1 / "e1_final_comparison.py", "final_comparison_a0b")
    calls = []

    def stop(spec):
        calls.append(spec)
        raise RuntimeError("stub: the first live call was reached")
    monkeypatch.setattr(module, "set_all", stop)
    return module, calls


def comparison_argv(tmp_path, plan):
    return ["--plan", str(plan), "--run-root", str(tmp_path / "runs"), "--observation-root", str(tmp_path / "obs"),
            "--summary", str(tmp_path / "summary.json")]


@pytest.mark.parametrize("channel", ["e1-r002", "e1-cfg001", "e1-b002", "E1-X", "", None])
def test_the_final_comparison_refuses_a_plan_for_a_bad_channel_before_toggling_anything(comparison, tmp_path, capsys, channel):
    module, calls = comparison
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"run_id": "x", "channel": channel}), encoding="utf-8")
    code, err = exit_of(module.main, comparison_argv(tmp_path, plan), capsys)
    assert code == 2 and "final-comparison run" in err
    assert calls == [] and not (tmp_path / "runs").exists() and not (tmp_path / "obs").exists()      # nothing was toggled or created


def test_the_final_comparison_refuses_an_unreadable_plan(comparison, tmp_path, capsys):
    module, calls = comparison
    code, err = exit_of(module.main, comparison_argv(tmp_path, tmp_path / "absent.json"), capsys)
    assert code == 2 and "cannot read plan" in err and calls == []


def test_the_final_comparison_reaches_its_first_live_call_only_for_a_dedicated_channel(comparison, tmp_path):
    module, calls = comparison
    plan = render_plan(tmp_path, "fc", "e1-compare-a")
    with pytest.raises(RuntimeError, match="first live call was reached"):
        module.main(comparison_argv(tmp_path, plan))
    assert calls[0] == module.DEBUG_SPEC                                  # the gate passed; the stub stopped the run there


def test_the_final_comparison_starts_the_pilot_with_the_explicit_channel():
    source = (E1 / "e1_final_comparison.py").read_text(encoding="utf-8")
    assert '"--validation-channel", channel' in source


# -------------------------------------------------------------------- the independent verifier
@pytest.fixture()
def verifier():
    import e1_independent_config_verify as module
    return module


def config_block(channel, *, b=45, batch_version="1", orderer_version="0", timeout_version="0", timeout="2s", consenters=3):
    """A decoded CONFIG block with the keys the verifier reads (a fixture, not a real Fabric block)."""
    admins = {"Admins": {"policy": {"type": 3, "value": {"rule": "MAJORITY"}}}}
    values = {
        "BatchSize": {"version": batch_version, "mod_policy": "Admins",
                      "value": {"max_message_count": b, "absolute_max_bytes": 103809024, "preferred_max_bytes": 10485760}},
        "BatchTimeout": {"version": timeout_version, "mod_policy": "Admins", "value": {"timeout": timeout}},
        "ConsensusType": {"version": "0", "mod_policy": "Admins",
                          "value": {"type": "etcdraft", "metadata": {"consenters": [{} for _ in range(consenters)]}}},
    }
    groups = {"Orderer": {"version": orderer_version, "mod_policy": "Admins", "policies": admins, "values": values},
              "Application": {"groups": {"Org1MSP": {}, "Org2MSP": {}}}}
    config = {"sequence": "3", "channel_group": {"version": "0", "groups": groups}}
    payload = {"header": {"channel_header": {"channel_id": channel, "timestamp": "2026-11-20T00:00:00Z"}}, "data": {"config": config}}
    return {"header": {"number": "12", "data_hash": "abc"}, "data": {"data": [{"payload": payload}]}}


def install_fake_peer(monkeypatch, blocks):
    """Stand-in for `peer channel fetch config` and `configtxlator proto_decode`; records every command."""
    calls = []

    def fake_run(argv, capture_output=True, env=None, timeout=None, check=False, **_):
        calls.append(list(argv))
        if argv[1:4] == ["channel", "fetch", "config"]:
            channel = argv[argv.index("-c") + 1]
            Path(argv[4]).write_bytes(b"pb:" + channel.encode())
        elif argv[1] == "proto_decode":
            channel = Path(argv[argv.index("--input") + 1]).read_bytes()[3:].decode()
            Path(argv[argv.index("--output") + 1]).write_text(json.dumps(blocks[channel]), encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, b"", b"")
    monkeypatch.setattr(subprocess, "run", fake_run)
    return calls


def verifier_argv(tmp_path, *extra):
    msp = tmp_path / "org" / "peerOrganizations" / "org1.example.com" / "users" / "Admin" / "msp"
    return ["--out-dir", str(tmp_path / "evidence"), "--peer", "peer", "--configtxlator", "configtxlator",
            "--fabric-cfg", str(tmp_path), "--orderer-ca", str(tmp_path / "ca.pem"), "--org1-admin-msp", str(msp), *extra]


def test_the_verifier_has_no_silent_default_channel(verifier, tmp_path, capsys):
    code, err = exit_of(verifier.main, verifier_argv(tmp_path), capsys)
    assert code == 2 and "no channel was selected" in err
    assert not (tmp_path / "evidence").exists()


@pytest.mark.parametrize("value, fragment", BAD_FOR_EXPERIMENTS)
def test_the_verifier_refuses_a_bad_channel_before_any_command(verifier, tmp_path, capsys, value, fragment):
    code, err = exit_of(verifier.main, verifier_argv(tmp_path, "--channel", value, "--expect-max-message-count", "45"), capsys)
    assert code == 2 and fragment in err and not (tmp_path / "evidence").exists()


@pytest.mark.parametrize("extra, fragment", [
    (["--channel", "e1-new"], "verifies nothing without an expected value"),
    (["--channel", "e1-new", "--expect-max-message-count", "45", "--before-block", "x.json"], "historical read-only mode"),
    (["--channel", "e1-new", "--historical-cfg001-r002-verification"], "cannot be combined"),
    (["--historical-cfg001-r002-verification", "--expect-max-message-count", "45"], "apply only to --channel"),
])
def test_the_verifier_modes_cannot_be_mixed_and_a_check_needs_an_expectation(verifier, tmp_path, capsys, extra, fragment):
    code, err = exit_of(verifier.main, verifier_argv(tmp_path, *extra), capsys)
    assert code == 2 and fragment in err and not (tmp_path / "evidence").exists()


def test_the_verifier_checks_an_explicit_channel_against_the_expected_values(verifier, tmp_path, monkeypatch, capsys):
    calls = install_fake_peer(monkeypatch, {"e1-new": config_block("e1-new", b=45)})
    argv = verifier_argv(tmp_path, "--channel", "e1-new", "--expect-max-message-count", "45", "--expect-batch-timeout-ms", "2000",
                         "--expect-preferred-max-bytes", "10485760", "--expect-orderers", "3")
    assert verifier.main(argv) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "explicit_channel" and result["read_only"] is True and result["all_checks_pass"] is True
    assert result["channel_selection"]["explicit"] is True and set(result["checks"].values()) == {True}
    fetched = [c[c.index("-c") + 1] for c in calls if c[1:4] == ["channel", "fetch", "config"]]
    assert fetched == ["e1-new"]                                          # exactly the selected channel, nothing else
    assert (Path(result["evidence_dir"]) / "independent_verification.json").is_file()


def test_the_verifier_reports_a_mismatch_with_an_expectation_as_a_failure(verifier, tmp_path, monkeypatch, capsys):
    install_fake_peer(monkeypatch, {"e1-new": config_block("e1-new", b=45)})
    assert verifier.main(verifier_argv(tmp_path, "--channel", "e1-new", "--expect-max-message-count", "40")) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["checks"]["max_message_count_equals_expected"] is False and result["all_checks_pass"] is False


def test_the_verifier_fails_when_the_fetched_block_belongs_to_another_channel(verifier, tmp_path, monkeypatch, capsys):
    install_fake_peer(monkeypatch, {"e1-new": config_block("e1-someone-else", b=45)})
    assert verifier.main(verifier_argv(tmp_path, "--channel", "e1-new", "--expect-max-message-count", "45")) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["checks"]["config_block_names_the_requested_channel"] is False and result["all_checks_pass"] is False


def test_the_verifier_records_a_malformed_decoded_block_as_a_failure(verifier, tmp_path, monkeypatch, capsys):
    install_fake_peer(monkeypatch, {"e1-new": {"data": {"data": [{}]}}})
    assert verifier.main(verifier_argv(tmp_path, "--channel", "e1-new", "--expect-max-message-count", "45")) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["all_checks_pass"] is False and "error" in result


def test_the_historical_mode_still_reproduces_the_two_channel_verification_exactly(verifier, tmp_path, monkeypatch, capsys):
    blocks = {"e1-cfg001": config_block("e1-cfg001", b=40), "e1-r002": config_block("e1-r002", b=45, batch_version="3")}
    calls = install_fake_peer(monkeypatch, blocks)
    assert verifier.main(verifier_argv(tmp_path, "--historical-cfg001-r002-verification")) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["tool"] == "e1_independent_config_verify 0.2"          # unchanged tool string
    assert set(result) >= {"e1_cfg001", "e1_r002", "transition", "checks", "all_checks_pass"} and "mode" not in result
    fetched = [c[c.index("-c") + 1] for c in calls if c[1:4] == ["channel", "fetch", "config"]]
    assert fetched == ["e1-cfg001", "e1-r002"]                            # the historical order and set, nothing else
    assert result["checks"]["target_B_is_40"] and result["checks"]["e1_r002_still_45"] and result["all_checks_pass"]


# ---------------------------------------------------------------- the production shadow validation CLI
@pytest.fixture()
def shadow_cli(monkeypatch):
    module = load_module(REPO / "Fabric_Live_Testbed" / "controllers" / "run_production_shadow_validation.py", "shadow_cli_a0b")

    class Reached(Exception):
        pass

    def provider(*_args, **_kwargs):
        raise Reached("the provider would be constructed here")
    monkeypatch.setattr(module, "ProductionTelemetryProvider", provider)
    monkeypatch.setattr(module, "PrometheusTelemetryAdapter", lambda *a, **k: object())
    module.Reached = Reached
    return module


def shadow_argv(tmp_path, channel):
    return ["--run-id", "a0b", "--channel", channel, "--out", str(tmp_path / "frames.json")]


@pytest.mark.parametrize("value, fragment", BAD_FOR_EXPERIMENTS)
def test_the_shadow_validation_cli_refuses_a_bad_channel_before_building_any_provider(shadow_cli, tmp_path, capsys, value, fragment):
    code, err = exit_of(shadow_cli.main, shadow_argv(tmp_path, value), capsys)
    assert code == 2 and fragment in err and not (tmp_path / "frames.json").exists()


def test_the_shadow_validation_cli_reaches_the_provider_for_a_dedicated_channel(shadow_cli, tmp_path):
    with pytest.raises(shadow_cli.Reached):
        shadow_cli.main(shadow_argv(tmp_path, "e1-new"))


# --------------------------------------- the two read-only tools (observation role, explicit and validated)
@pytest.fixture()
def builder():
    return load_module(E1 / "build_contemporaneous_transaction_evidence.py", "builder_a0b")


def builder_argv(tmp_path, channel):
    names = ("recorder.csv", "submits.csv", "manifest.json", "ledger_tx.csv", "observations.jsonl")
    flags = ("--recorder-csv", "--submits-csv", "--manifest", "--ledger-tx", "--ledger-observations")
    argv = ["--run-id", "t", "--channel", channel, "--out-dir", str(tmp_path / "out")]
    for flag, name in zip(flags, names):
        argv += [flag, str(tmp_path / name)]
    return argv


@pytest.mark.parametrize("value", ["", " ", "E1-x", "e1-a_b", "e1-new\n", "mychannel!"])
def test_the_evidence_builder_refuses_an_empty_or_malformed_channel_before_reading_anything(builder, tmp_path, capsys, value):
    code, err = exit_of(builder.main, builder_argv(tmp_path, value), capsys)
    assert code == 2 and re.search("is empty|not a valid Fabric channel name", err) and not (tmp_path / "out").exists()


@pytest.mark.parametrize("channel", ["e1-new", "e1-cfg001", "e1-r002"])
def test_the_evidence_builder_still_accepts_any_explicit_channel_so_historical_evidence_can_be_rebuilt(builder, tmp_path, channel):
    with pytest.raises(FileNotFoundError):                                # the first real operation: reading the recorder CSV
        builder.main(builder_argv(tmp_path, channel))
    assert not (tmp_path / "out").exists()


def test_the_existing_historical_builder_regression_test_is_still_present():
    tree = ast.parse((REPO / "tests" / "test_live_shadow_config_evidence_2026_11_17.py").read_text(encoding="utf-8"))
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "test_builder_with_explicit_b40_reproduces_the_historical_configuration_exactly" in names


def test_the_live_telemetry_scrape_validates_the_channel_and_passes_it_on_unchanged(tmp_path, monkeypatch, capsys):
    module = load_module(REPO / "Integration" / "system_integration" / "collect_live_telemetry.py", "collect_a0b")
    seen = []

    class FakeAdapter:
        def __init__(self, endpoints, timeout_s=2.0):
            pass

        def collect(self, channel):
            seen.append(channel)
            return types.SimpleNamespace(as_dict=lambda: {"channel": channel}, health={})
    monkeypatch.setattr(module, "PrometheusTelemetryAdapter", FakeAdapter)
    for bad in ("", "E1-x", "e1-a b"):
        monkeypatch.setattr(sys, "argv", ["collect", "--endpoint", "http://x/metrics", "--channel", bad])
        with pytest.raises(SystemExit) as caught:
            module.main()
        assert caught.value.code == 2
    assert seen == []                                                      # nothing was scraped for an invalid channel
    for good in ("e1-new", "e1-cfg001"):                                   # read-only: an existing channel may be observed
        monkeypatch.setattr(sys, "argv", ["collect", "--endpoint", "http://x/metrics", "--channel", good])
        assert module.main() == 0
    assert seen == ["e1-new", "e1-cfg001"]


# ================================== C. consistency: the same input is refused the same way by every runner and verifier
def invoke_pilot(tmp_path, channel, capsys):
    import e1_existing_channel_pilot as pilot
    return exit_of(pilot.main, ["--plan", str(tmp_path / "p.json"), "--validation-channel", channel, "--run-dir", str(tmp_path / "r")], capsys)


def invoke_replay(tmp_path, channel, capsys):
    return exit_of(load_module(E1 / "synchronized_shadow_replay.py", "replay_consistency").main, replay_argv(tmp_path, channel), capsys)


def invoke_capture(tmp_path, channel, capsys):
    return exit_of(load_module(E1 / "run_contemporaneous_capture.py", "capture_consistency").main, capture_argv(tmp_path, channel), capsys)


def invoke_verifier(tmp_path, channel, capsys):
    import e1_independent_config_verify as module
    return exit_of(module.main, verifier_argv(tmp_path, "--channel", channel, "--expect-max-message-count", "45"), capsys)


def invoke_shadow(tmp_path, channel, capsys):
    module = load_module(REPO / "Fabric_Live_Testbed" / "controllers" / "run_production_shadow_validation.py", "shadow_consistency")
    return exit_of(module.main, shadow_argv(tmp_path, channel), capsys)


RUNNERS_AND_VERIFIERS = [invoke_pilot, invoke_replay, invoke_capture, invoke_verifier, invoke_shadow]
CANONICAL = {"e1-r002": cs.PROTECTED, "e1-cfg001": cs.PROTECTED, "e1-b001": cs.HISTORICAL, "e1-cp03-b40-l20": cs.HISTORICAL,
             "": cs.EMPTY, "E1-NEW": cs.MALFORMED, "e1-a_b": cs.MALFORMED}


@pytest.mark.parametrize("channel", list(CANONICAL))
@pytest.mark.parametrize("invoke", RUNNERS_AND_VERIFIERS, ids=lambda f: f.__name__.removeprefix("invoke_"))
def test_every_runner_and_verifier_refuses_a_channel_with_the_shared_modules_own_message(invoke, channel, tmp_path, capsys):
    code, err = invoke(tmp_path, channel, capsys)
    expected = " ".join(str(refusal(cs.select_experiment_channel, channel, option=None)).split())
    assert code == 2 and expected in " ".join(err.split()), (invoke.__name__, err)


@pytest.mark.parametrize("channel", ["e1-new", "e1-val.001"])
def test_every_runner_and_verifier_gets_past_validation_for_the_same_dedicated_channel(channel, tmp_path, capsys):
    """Past validation each tool stops at its own first real step (a missing file or a stubbed call), never at the channel."""
    import e1_existing_channel_pilot as pilot
    import e1_independent_config_verify as verifier
    with pytest.raises(FileNotFoundError):
        pilot.main(["--plan", str(tmp_path / "p.json"), "--validation-channel", channel, "--run-dir", str(tmp_path / "r")])
    with pytest.raises(FileNotFoundError):
        load_module(E1 / "run_contemporaneous_capture.py", "capture_pass").main(capture_argv(tmp_path, channel))
    assert load_module(E1 / "synchronized_shadow_replay.py", "replay_pass").main(replay_argv(tmp_path, channel)) == 0
    with pytest.raises(AssertionError, match="external process"):         # the verifier's first step is to run `peer`
        verifier.main(verifier_argv(tmp_path, "--channel", channel, "--expect-max-message-count", "45"))


# ============================ D. structural guards: no accidental default, allowlist or fallback can come back
SCANNED_ROOTS = ("src", "scripts", "Fabric_Live_Testbed", "Integration", "p4_replay")


def protected_sites(path: Path):
    """(line, context) of every non-docstring string constant that contains a protected channel name."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                docstrings.add(first.value)
    parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node not in docstrings and any(p in node.value for p in PROTECTED):
            ancestor = parents.get(node)
            keyword = ancestor.arg if isinstance(ancestor, ast.keyword) else None
            yield node.lineno, keyword


def python_files():
    for root in SCANNED_ROOTS:
        for path in sorted((REPO / root).rglob("*.py")):
            rel = path.relative_to(REPO).as_posix()
            if "__pycache__" in path.parts or rel.startswith(FROZEN_DIR.relative_to(REPO).as_posix()) or "/tests/" in "/" + rel:
                continue
            yield path, rel


# Every non-test, non-frozen Python file that names a protected channel in a string constant, and why that is acceptable.
EXPECTED_LITERAL_FILES = {
    "scripts/e1/e1_quicklook.py": "pinned P1a tool: refuses earlier channels for new runs; fingerprints the two protected channels read-only",
    "scripts/e1/e1_quicklook_p1b.py": "pinned P1b tool: same",
    "scripts/e1/e1_independent_config_verify.py": "the explicit historical read-only mode",
    "scripts/e1/run_contemporaneous_capture.py": "the template placeholder, and the scan that proves it was rewritten",
    "scripts/e1/run_fabric_verifier_evidence_matrix.py": "offline fixture matrix: the names are labels of stored fixtures",
    "scripts/research_audit/phase4_consolidate_evidence.py": "historical record of the 2026-11-11 replay",
    "src/fabric_perf/controllers/channel_selection.py": "the registry itself",
    "Integration/system_integration/a0b_2026-11-20/mutation_check.py": "verification harness: its mutants model silent fallbacks to a protected channel; imported by nothing",
}


def test_every_file_that_names_a_protected_channel_is_accounted_for():
    found = {rel for path, rel in python_files() if list(protected_sites(path))}
    assert found == set(EXPECTED_LITERAL_FILES), (sorted(found - set(EXPECTED_LITERAL_FILES)), sorted(set(EXPECTED_LITERAL_FILES) - found))


def test_no_protected_channel_is_a_default_a_parameter_default_or_a_choice_anywhere_outside_frozen_evidence():
    offenders = []
    for path, rel in python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            values = []
            if isinstance(node, ast.keyword) and node.arg in {"default", "choices"}:
                values.append(node.value)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                values += [*node.args.defaults, *[d for d in node.args.kw_defaults if d is not None]]
            for value in values:
                if any(isinstance(n, ast.Constant) and isinstance(n.value, str) and any(p in n.value for p in PROTECTED) for n in ast.walk(value)):
                    offenders.append((rel, node.lineno))
    assert offenders == [], offenders


def test_no_allowlist_or_table_of_channels_remains_in_the_runners():
    names = {"ALLOWED_CHANNELS", "CHANNELS", "PROTECTED_CHANNELS", "EXISTING_CHANNELS"}
    for script in ("e1_existing_channel_pilot.py", "synchronized_shadow_replay.py", "run_contemporaneous_capture.py",
                   "e1_final_comparison.py", "e1_independent_config_verify.py"):
        tree = ast.parse((E1 / script).read_text(encoding="utf-8"))
        assigned = {t.id for n in ast.walk(tree) if isinstance(n, ast.Assign) for t in n.targets if isinstance(t, ast.Name)}
        assert not assigned & names, (script, assigned & names)


WIRING = {
    "scripts/e1/e1_existing_channel_pilot.py": ("--validation-channel", "experiment_channel_arg"),
    "scripts/e1/synchronized_shadow_replay.py": ("--channel", "experiment_channel_arg"),
    "scripts/e1/run_contemporaneous_capture.py": ("--channel", "experiment_channel_arg"),
    "scripts/e1/e1_independent_config_verify.py": ("--channel", "experiment_channel_arg"),
    "Fabric_Live_Testbed/controllers/run_production_shadow_validation.py": ("--channel", "experiment_channel_arg"),
    "scripts/e1/build_contemporaneous_transaction_evidence.py": ("--channel", "observed_channel_arg"),
    "Integration/system_integration/collect_live_telemetry.py": ("--channel", "observed_channel_arg"),
}


@pytest.mark.parametrize("script", sorted(WIRING))
def test_each_channel_bound_tool_uses_the_shared_validator_and_has_no_default(script):
    flag, validator = WIRING[script]
    tree = ast.parse((REPO / script).read_text(encoding="utf-8"))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "add_argument"
             and any(isinstance(a, ast.Constant) and a.value == flag for a in n.args)]
    assert len(calls) == 1, (script, flag)
    keywords = {k.arg: k.value for k in calls[0].keywords}
    assert "default" not in keywords and "choices" not in keywords
    kind = keywords["type"]
    assert isinstance(kind, ast.Attribute) and kind.attr == validator and getattr(kind.value, "id", "") == "cs"
    imports = [n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module == "fabric_perf.controllers"
               and any(a.name == "channel_selection" for a in n.names)]
    assert imports, f"{script} does not import fabric_perf.controllers.channel_selection"


def test_the_runners_require_the_channel_and_the_explicit_verifier_mode_requires_it_in_code():
    for script in ("scripts/e1/e1_existing_channel_pilot.py", "scripts/e1/synchronized_shadow_replay.py",
                   "scripts/e1/run_contemporaneous_capture.py", "Fabric_Live_Testbed/controllers/run_production_shadow_validation.py",
                   "scripts/e1/build_contemporaneous_transaction_evidence.py", "Integration/system_integration/collect_live_telemetry.py"):
        flag = WIRING[script][0]
        tree = ast.parse((REPO / script).read_text(encoding="utf-8"))
        call = next(n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "add_argument"
                    and any(isinstance(a, ast.Constant) and a.value == flag for a in n.args))
        required = {k.arg: k.value for k in call.keywords}.get("required")
        assert isinstance(required, ast.Constant) and required.value is True, script
    verifier = (E1 / "e1_independent_config_verify.py").read_text(encoding="utf-8")
    assert "no channel was selected" in verifier                              # --channel is optional only because the historical flag is its alternative


def test_the_pinned_tools_that_a0b_leaves_alone_still_carry_their_known_hazards_documented_in_the_report():
    """If a pinned tool is ever changed, this fails and the report's 'left unchanged' list must be revisited."""
    render = (E1 / "e1_la2_render.py").read_text(encoding="utf-8")
    assert 'channel = channel or f"e1-{run_id.lower()}"' in render            # derivation: caught by the pilot's plan check
    assert re.search(r"^CHANNEL_RE = re\.compile\(r\"\^\[a-z\]\[a-z0-9\.-\]\{0,248\}\$\"\)", (E1 / "e1_channel_run.py").read_text(encoding="utf-8"), re.M)


# ================================== E. frozen evidence and historical behaviour stay exactly as they were
def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.skipif(not (FROZEN_DIR / "SHA256SUMS.txt").exists(), reason="recovery evidence directory not present")
def test_the_199_file_recovery_evidence_manifest_still_verifies_byte_for_byte():
    entries = {}
    for line in (FROZEN_DIR / "SHA256SUMS.txt").read_text(encoding="utf-8").splitlines():
        digest, _, name = line.partition(" *")
        entries[name[2:] if name.startswith("./") else name] = digest
    assert len(entries) == 199
    bad = [name for name, digest in entries.items() if sha256(FROZEN_DIR / name) != digest]
    assert bad == []
    on_disk = {p.relative_to(FROZEN_DIR).as_posix() for p in FROZEN_DIR.rglob("*") if p.is_file() and p.name != "SHA256SUMS.txt"}
    assert on_disk == set(entries)                                             # nothing added to, or removed from, the evidence


@pytest.mark.skipif(not (FROZEN_DIR / "SHA256SUMS.txt").exists(), reason="recovery evidence directory not present")
def test_the_frozen_script_that_defaults_to_the_historical_channel_is_unchanged_and_documented():
    script = FROZEN_DIR / "scripts" / "bounded_scrape_continuity.py"
    assert sha256(script) == "e2ac6f48728f2a19b80c51570ea45386be9c2fe27ad1ad59dc5644cab4176e88"
    assert f"{sha256(script)} *./scripts/bounded_scrape_continuity.py" in (FROZEN_DIR / "SHA256SUMS.txt").read_text(encoding="utf-8")
    tree = ast.parse(script.read_text(encoding="utf-8"))
    defaults = [n.value.value for n in ast.walk(tree) if isinstance(n, ast.keyword) and n.arg == "default" and isinstance(n.value, ast.Constant)]
    assert "e1-cfg001" in defaults                                             # its historical behaviour is exactly as recorded
    assert (FROZEN_DIR / "scripts" / "bounded_scrape_continuity.py") not in [p for p, _ in python_files()]   # excluded from the guards


def test_the_eight_frozen_p1b_files_still_equal_the_hashes_the_p4_launch_gate_pins():
    pins = literal_assignment(REPO / "p4_replay" / "preflight.py", "FROZEN_P1B_SHA256")
    assert len(pins) == 8
    for relative, digest in pins.items():
        assert sha256(REPO / relative) == digest, relative


def test_a0b_did_not_touch_the_p1b_pinned_tools_it_deliberately_left_alone():
    """Tools in the P1b INVOKED_TOOLS list are pinned for future launches; A0b edits none of them (hashes at A0b start)."""
    start = {"e1_la2_render.py": "f77a83e7de106b3c", "e1_channel_run.py": "f549e9b05ae5eb22", "e1_block_fetch.py": "08eedd2ea373dfa1",
             "e1_run_orchestrator.py": "8b5608a74ea05f0f", "e1_config_actuator.py": "f4b979535b9abb4c",
             "e1_log_collector.py": "e6b6914303152a69", "e1_telemetry_common.py": "584bef86870de785",
             "e1_quicklook.py": "38b8ec29b5fd", "e1_quicklook_p1b.py": "00575656d47c"}
    # e1_config_actuator.py was revised AFTER A0b (2026-11-22: owner-authorized broadcast path + channel_selection guard;
    # the owner approved the revised actuator for the cs01 experiment on 2026-11-22). The A0b claim is historical, so for
    # that one file the claim is checked against the committed blob, in
    # tests/e1/test_e1_config_actuator_broadcast.py::test_committed_actuator_is_still_the_a0b_start_version (that check
    # needs git, and this module refuses every process start). The working tree is deliberately not re-pinned here.
    revised_after_a0b = {"e1_config_actuator.py"}
    for name, prefix in start.items():
        if name in revised_after_a0b:
            continue
        assert sha256(E1 / name).startswith(prefix), name
