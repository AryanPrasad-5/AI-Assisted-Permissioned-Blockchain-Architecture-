"""Phase 2C, Package C: gate R4 by classification instead of substring search. Synthetic fixtures only. Whether the REAL idle Caliper container shows a keep-alive process was not
checked (no `docker top` was run): these tests show what each possible answer would do, which is the point of the pending read-only check."""

import pytest

from p4_replay import gate_r4 as g4, preflight as pf
from p4_replay.tests import support_2c as s2

IDLE_DOCUMENTED = "1 sleep infinity"
IDLE_WITH_WORKSPACE_PATH = '1 /bin/sh -c "cd /hyperledger/caliper/workspace && sleep infinity"'
CLIENT_NODE = "42 node /hyperledger/caliper/workspace/node_modules/.bin/caliper launch manager --caliper-workspace ."
CLIENT_BIN = "43 /usr/local/bin/caliper launch worker --caliper-workspace ."
CLIENT_NPX = "44 npx caliper launch manager"


@pytest.mark.parametrize("line", ["1 sleep infinity", "1 /bin/sh -c sleep infinity", "1 tail -f /dev/null", "1 0 sleep infinity", '1 sh -c "sleep infinity"', IDLE_WITH_WORKSPACE_PATH])
def test_keep_alive_processes_are_idle(line):
    c = g4.classify_container_processes("PID COMMAND\n" + line + "\n")
    assert c["idle"] and not c["client"] and not c["unknown"] and c["rows"] == 1


@pytest.mark.parametrize("line", [CLIENT_NODE, CLIENT_BIN, CLIENT_NPX, "50 node /app/node_modules/@hyperledger/caliper-cli/caliper.js launch manager",
                                  "45 /usr/local/bin/caliper bind --caliper-bind-sut fabric:2.5", "46 caliper --version", "47 /usr/bin/caliper-cli unbind"])
def test_caliper_client_processes_are_clients(line):
    c = g4.classify_container_processes("PID COMMAND\n" + line + "\n")
    assert c["client"] and not c["idle"]


@pytest.mark.parametrize("line", ["60 sh", "61 node server.js", "62 /bin/bash", "63 python3 -c print(1)"])
def test_anything_unidentified_inside_the_container_is_unknown_and_so_a_conflict(line):
    c = g4.classify_container_processes("PID COMMAND\n" + line + "\n")
    assert c["unknown"] and not c["idle"] and not c["client"]


def test_the_header_row_is_not_a_process_and_blank_input_has_no_rows():
    assert g4.classify_container_processes("PID COMMAND\n")["rows"] == 0 and g4.classify_container_processes("")["rows"] == 0


# ----------------------------------------------------------------------------------------------- the false-positive question, answered on fixtures
def gate(top_rows, own_pid=300, **world_kw):
    be = s2.FakeLaunchBackend(s2.world(caliper_top=top_rows, **world_kw))
    return be, g4.gate_r4(be, own_pid), pf.gate_r4_no_conflicting_process(be, own_pid)


def test_with_the_documented_sleep_infinity_container_neither_gate_false_positives():
    _, v2, v1 = gate([IDLE_DOCUMENTED])
    assert v2.status == "PASS" and v1.status == "PASS"


def test_if_the_idle_entrypoint_spells_the_workspace_path_phase_2_would_block_every_launch_and_2c_does_not():
    _, v2, v1 = gate([IDLE_WITH_WORKSPACE_PATH])
    assert v1.status == "FAIL", "Phase 2's substring test false-positives on an idle container whose command line names the workspace"
    assert v2.status == "PASS" and "keep-alive" in v2.detail


@pytest.mark.parametrize("row", [CLIENT_NODE, CLIENT_BIN, CLIENT_NPX])
def test_a_caliper_client_inside_the_container_fails_both(row):
    _, v2, v1 = gate([IDLE_DOCUMENTED, row])
    assert v2.status == "FAIL" and "Caliper client" in v2.detail and v1.status == "FAIL"


def test_an_unidentified_process_inside_the_container_fails_closed():
    _, v2, _ = gate([IDLE_DOCUMENTED, "77 sh"])
    assert v2.status == "FAIL" and "unidentified" in v2.detail


def test_docker_top_failing_or_empty_is_a_failure_not_a_pass():
    be = s2.FakeLaunchBackend(s2.world(caliper_top=[]))
    assert g4.gate_r4(be, 300).status == "FAIL" and "no process rows" in g4.gate_r4(be, 300).detail
    orig = be.run
    be.run = lambda argv, env=None, timeout=None: (s2.p1b.CmdResult(code=1, stdout=b"", stderr=b"no such container") if argv[:2] == ["docker", "top"] else orig(argv, env, timeout))
    r = g4.gate_r4(be, 300)
    assert r.status == "FAIL" and "docker top" in r.detail and "idleness cannot be shown" in r.detail


def test_ps_failing_is_a_failure():
    be = s2.FakeLaunchBackend(s2.world(caliper_top=[IDLE_DOCUMENTED]))
    orig = be.run
    be.run = lambda argv, env=None, timeout=None: (s2.p1b.CmdResult(code=2, stdout=b"", stderr=b"") if argv[0] == "ps" else orig(argv, env, timeout))
    assert g4.gate_r4(be, 300).status == "FAIL"


def test_a_second_caliper_container_fails():
    _, v2, _ = gate([IDLE_DOCUMENTED], other_containers=["caliper-extra"])
    assert v2.status == "FAIL" and "container caliper-extra" in v2.detail


# ----------------------------------------------------------------------------------------------- the host side
@pytest.mark.parametrize("args", ["python3 -m p4_replay.live_entry launch --x", "python3 -m p4_replay.series run", "python3 -m p4_replay.launch_series", "python3 -m p4_replay.driver run",
                                  "python3 /mnt/c/x/p4_replay/live_entry.py launch", "python3 -B /mnt/c/x/scripts/e1/e1_quicklook_p1b.py run", "python3 e1_channel_run.py --channel x",
                                  "python3 /x/e1_clock_rate_probe.py collect", "python3 /x/e1_host_sampler.py", "docker exec -w /hyperledger/caliper/workspace caliper-benchmark-001 caliper launch manager",
                                  "caliper launch worker --x"])
def test_host_experiment_processes_and_a_second_launch_are_conflicts(args):
    assert g4.conflicting_host_processes(f"  100     1 init\n  500   100 {args}\n", own_pid=300), args


@pytest.mark.parametrize("args", ["vim /home/mailf18/e1/caliper-ws-benchmark/notes.txt", "tail -f /home/mailf18/e1/caliper-ws-benchmark/runs/rp90/log.txt", "ls /hyperledger/caliper",
                                  "python3 -m pytest p4_replay", "bash", "grep caliper README.md"])
def test_a_command_that_merely_names_a_caliper_path_is_not_a_conflict(args):
    assert g4.conflicting_host_processes(f"  100     1 init\n  500   100 {args}\n", own_pid=300) == []


@pytest.mark.parametrize("args", ["vim /home/mailf18/e1/caliper-ws-benchmark/notes.txt", "tail -f /home/mailf18/e1/caliper-ws-benchmark/runs/rp90/log.txt", "grep caliper README.md"])
def test_phase_2s_substring_rule_over_matches_those_same_commands(args):
    assert pf.conflicting_processes(f"  100     1 init\n  500   100 {args}\n", own_pid=300), "the Phase 2 host check would have blocked the launch on this"


def test_this_process_and_its_ancestors_are_not_conflicts_but_a_second_launch_is():
    ps = "  100     1 init\n  200   100 bash\n  300   200 python3 -m p4_replay.live_entry launch\n  301   300 ps -eo pid,ppid,args\n"
    assert g4.conflicting_host_processes(ps, own_pid=300) == []
    second = ps + "  900     1 python3 -m p4_replay.live_entry launch\n"
    hits = g4.conflicting_host_processes(second, own_pid=300)
    assert len(hits) == 1 and hits[0].startswith("900 ")


def test_the_evidence_names_the_two_pending_read_only_live_checks():
    _, v2, _ = gate([IDLE_DOCUMENTED])
    assert v2.evidence["live_verification_pending"] == ["docker top (read-only)", "docker inspect entrypoint (read-only)"]
