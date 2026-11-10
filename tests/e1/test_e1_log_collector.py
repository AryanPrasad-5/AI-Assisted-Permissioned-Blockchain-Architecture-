"""Synthetic tests of the E0/E1 log collector. The log lines are written from the Fabric v2.5 message
formats as read in planning; they are not captured container logs and are not E0/E1 results."""
import calendar
import csv
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_log_collector as lc  # noqa: E402
import e1_reconcile as rc  # noqa: E402
from tests.e1.test_e1_reconcile import expect, synth_run  # noqa: E402

CH = "e1-r001"
OTHER = "e1-r000"
T0 = calendar.timegm((2026, 11, 11, 10, 0, 0)) * 1000  # epoch ms
ORDERERS = ["orderer0", "orderer1", "orderer2"]
PEERS = ["peer0org1", "peer0org2"]


# ------------------------------------------------------------------ synthetic log lines
def stamp(ms: int) -> str:
    d = dt.datetime.fromtimestamp(ms // 1000, tz=dt.timezone.utc)
    return d.strftime("%Y-%m-%d %H:%M:%S") + f".{ms % 1000:03d}"


def docker(ms: int, lag_ns: int = 250_000) -> str:
    ns = ms * 1_000_000 + lag_ns
    d = dt.datetime.fromtimestamp(ns // 1_000_000_000, tz=dt.timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns % 1_000_000_000:09d}Z "


def header(ms, module, func, fmt="new", tz="UTC", seq=0x45, ansi=False):
    if fmt == "new":
        h = f"{stamp(ms)} {tz} {seq:04x} INFO [{module}] {func} -> "
    else:
        h = f"{stamp(ms)} {tz} [{module}] {func} -> INFO {seq:03x} "
    return f"\x1b[34m{h}\x1b[0m" if ansi else h


def oline(block, ms, node=1, channel=CH, fmt="new", tz="UTC", ansi=False, lag_ns=250_000, dock=True):
    fields = f" channel={channel} node={node}" if channel else f" node={node}"
    line = header(ms, "orderer.consensus.etcdraft", "writeBlock", fmt, tz, ansi=ansi) + \
        f"Writing block [{block}] (Raft index: {block + 4}) to ledger{fields}"
    return (docker(ms, lag_ns) if dock else "") + line


def pline(block, ms, ntx, channel=CH, breakdown=True, fmt="new", lag_ns=400_000):
    msg = f"[{channel}] Committed block [{block}] with {ntx} transaction(s) in 25ms"
    if breakdown:
        msg += " (state_validation=2ms block_and_pvtdata_commit=15ms state_commit=7ms)"
    msg += " commitHash=[47dc540c94ceb704a23875c11273e16bb0b8a87aed84de911f2133568115f254]"
    return docker(ms, lag_ns) + header(ms, "kvledger", "commit", fmt) + msg


def noise(ms, i):
    return docker(ms) + header(ms, "comm.grpc.server", "1", seq=i) + "unary call completed grpc.code=OK"


def write_ms(block):      # orderer write time of block b
    return T0 + block * 2000


def commit_ms(block):     # peer commit time: 30 ms after the write
    return write_ms(block) + 30


def make_logs(tmp: Path, blocks=range(1, 13), ntx=lambda b: 10, mutate=None):
    """Write one stderr log per container. mutate(name, lines) may edit the line list in place."""
    tmp.mkdir(parents=True, exist_ok=True)
    inputs = []
    for i, name in enumerate(ORDERERS):
        lines = ["", noise(T0, 1)]
        for b in blocks:
            lines.append(oline(b, write_ms(b) + i, node=i + 1))
            lines.append(oline(b, write_ms(b) + 500 + i, node=i + 1, channel=OTHER))  # other channel
            lines.append(noise(write_ms(b) + 3, b))
        if mutate:
            mutate(name, lines)
        p = tmp / f"{name}.stderr.log"
        p.write_bytes(("\n".join(lines) + "\n").encode("utf-8"))
        inputs.append(lc.LogInput(name, "orderer", p))
    for name in PEERS:
        lines = [noise(T0, 2)]
        for b in blocks:
            lines.append(pline(b, commit_ms(b), ntx(b)))
            lines.append(pline(b, commit_ms(b) + 500, 3, channel=OTHER))
        if mutate:
            mutate(name, lines)
        p = tmp / f"{name}.stderr.log"
        p.write_bytes(("\n".join(lines) + "\n").encode("utf-8"))
        inputs.append(lc.LogInput(name, "peer", p))
    return inputs


def blocks_csv(blocks=range(1, 13), ntx=lambda b: 10):
    return [{"number": str(b), "tx_count": str(ntx(b))} for b in blocks]


def kinds(out: Path, severity=None):
    with (out / "log_anomalies.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return [(r["kind"], r["container"], r["block"]) for r in rows if severity in (None, r["severity"])], rows


def read(out: Path, name: str):
    with (out / name).open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def find_line(lines, needle):
    return next(i for i, line in enumerate(lines) if needle in line)


# ------------------------------------------------------------------ timestamp parsing
@pytest.mark.parametrize("text,ns", [
    ("2026-11-11T10:00:00.123456789Z rest", (T0 + 123) * 1_000_000 + 456_789),
    ("2026-11-11T10:00:00.5Z rest", (T0 + 500) * 1_000_000),
    ("2026-11-11T10:00:00Z rest", T0 * 1_000_000),
    ("2026-11-11T15:30:00.000000000+05:30 rest", T0 * 1_000_000),
])
def test_docker_timestamp(text, ns):
    ts, got, rest = lc.parse_docker_ts(text)
    assert got == ns and rest == "rest" and ts == text.split(" ")[0]


@pytest.mark.parametrize("text", ["2026-13-11T10:00:00Z x", "2026-02-30T10:00:00Z x", "2026-11-11 10:00:00Z x",
                                  "2026-11-11T10:00:00.1234567891Z x", "no timestamp", ""])
def test_docker_timestamp_rejected(text):
    assert lc.parse_docker_ts(text)[1] is None


@pytest.mark.parametrize("fmt", ["new", "old"])
@pytest.mark.parametrize("ansi", [False, True])
def test_fabric_header_formats(fmt, ansi):
    line = lc._ANSI.sub("", header(T0 + 7, "kvledger", "commit", fmt, ansi=ansi) + "hello [x]")
    h = lc.parse_fabric_header(line)
    assert h.ms == T0 + 7 and h.level == "INFO" and h.module == "kvledger" and h.msg == "hello [x]"


def test_fabric_header_zones():
    assert lc.parse_fabric_header(f"{stamp(T0)} +0530 0001 INFO [m] f -> x").ms == T0 - 19_800_000
    ist = lc.parse_fabric_header(f"{stamp(T0)} IST 0001 INFO [m] f -> x")
    assert ist is not None and ist.ms is None
    assert lc.parse_fabric_header("2026-11-11 10:00:00 UTC 0001 INFO [m] f -> x") is None  # no ms


# ------------------------------------------------------------------ clean run
def test_clean_run_complete_and_traceable(tmp_path):
    inputs = make_logs(tmp_path / "logs")
    out = tmp_path / "out"
    m = lc.run_parse(inputs, CH, out, blocks_csv=blocks_csv(), reconcile_peer="peer0org1")
    assert m["status"] == "COMPLETE", kinds(out)
    assert m["events"]["orderer_write"] == 3 * 12 * 2 and m["events"]["peer_commit"] == 2 * 12 * 2
    assert m["events"]["in_scope_canonical"] == 5 * 12 and m["events"]["duplicates"] == 0
    assert m["events"]["per_channel"][OTHER] == {"orderer_write": 36, "peer_commit": 24}
    for acc in m["inputs"]:  # every line accounted for
        assert acc["lines_total"] == acc["lines_blank"] + acc["lines_event"] + acc["lines_malformed_event"] + acc["lines_other"]
        assert acc["sha256"] == hashlib.sha256(Path(acc["path"]).read_bytes()).hexdigest()
    ow = read(out, "orderer_write.csv")
    assert {r["in_scope"] for r in ow} == {"True", "False"}  # other channel kept, not dropped
    first = next(r for r in ow if r["container"] == "orderer1" and r["block"] == "4" and r["in_scope"] == "True")
    src_lines = Path(first["source_file"]).read_text(encoding="utf-8").split("\n")
    assert src_lines[int(first["line_no"]) - 1] == first["raw_line"]  # original reference resolves
    assert int(first["fabric_ms"]) == write_ms(4) + 1 and first["node"] == "2" and first["raft_index"] == "8"
    summary = read(out, "block_summary.csv")
    assert [int(r["block"]) for r in summary] == list(range(1, 13))
    assert all(r["orderers_logged"] == "3" and r["peers_logged"] == "2" and r["flags"] == "" for r in summary)
    assert summary[0]["orderer_first_write_ms"] == str(write_ms(1)) and summary[0]["ledger_tx_count"] == "10"
    rec = read(out, "peer_commit_reconcile.csv")
    assert [(int(r["block"]), int(r["commit_ms"])) for r in rec] == [(b, commit_ms(b)) for b in range(1, 13)]
    for name, digest in m["outputs_sha256"].items():  # outputs hashed, LF only
        data = (out / name).read_bytes()
        assert hashlib.sha256(data).hexdigest() == digest and b"\r" not in data


def test_old_header_and_ansi_parse_complete(tmp_path):
    def to_old(name, lines):
        if name.startswith("orderer"):
            for i, line in enumerate(lines):
                if "Writing block" in line and f"channel={CH}" in line:
                    b = int(line.split("Writing block [")[1].split("]")[0])
                    lines[i] = oline(b, write_ms(b) + ORDERERS.index(name), node=ORDERERS.index(name) + 1,
                                     fmt="old", ansi=True)
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=to_old), CH, tmp_path / "o", blocks_csv=blocks_csv())
    assert m["status"] == "COMPLETE"


def test_peer_line_without_breakdown(tmp_path):
    def strip(name, lines):
        if name == "peer0org2":
            lines[find_line(lines, f"[{CH}] Committed block [3]")] = pline(3, commit_ms(3), 10, breakdown=False)
    out = tmp_path / "o"
    assert lc.run_parse(make_logs(tmp_path / "l", mutate=strip), CH, out)["status"] == "COMPLETE"
    row = next(r for r in read(out, "peer_commit.csv") if r["container"] == "peer0org2" and r["block"] == "3"
               and r["in_scope"] == "True")
    assert row["state_validation_ms"] == "" and row["commit_duration_ms"] == "25" and row["tx_count"] == "10"


# ------------------------------------------------------------------ missing, malformed, incomplete
def test_missing_event_flagged(tmp_path):
    def drop(name, lines):
        if name == "orderer1":
            del lines[find_line(lines, f"Writing block [7] (Raft index: 11) to ledger channel={CH}")]
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=drop), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCOMPLETE"
    assert kinds(out)[0] == [("missing_event", "orderer1", "7")]
    row7 = read(out, "block_summary.csv")[6]
    assert row7["orderers_logged"] == "2" and row7["orderers_missing"] == "orderer1"


def test_truncated_run_missing_tail(tmp_path):
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", blocks=range(1, 10)), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCOMPLETE"
    missing = [k for k in kinds(out)[0] if k[0] == "missing_event"]
    assert len(missing) == 5 * 3 and {k[2] for k in missing} == {"10", "11", "12"}


def test_gap_detected_without_ledger_from_observed_range(tmp_path):
    def drop(name, lines):
        del lines[find_line(lines, f"[{CH}] Committed block [5]" if name.startswith("peer")
                            else f"Writing block [5] (Raft index: 9) to ledger channel={CH}")]
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=drop), CH, out)
    assert m["expected_blocks"] == {"source": "observed_range", "first": 1, "last": 12, "count": 12}
    assert m["status"] == "INCOMPLETE" and len(kinds(out)[0]) == 5


def test_malformed_event_line_kept_as_anomaly(tmp_path):
    bad = docker(write_ms(8)) + header(write_ms(8), "orderer.consensus.etcdraft", "writeBlock") + "Writing block [8"

    def corrupt(name, lines):
        if name == "orderer2":
            lines[find_line(lines, f"Writing block [8] (Raft index: 12) to ledger channel={CH}")] = bad
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=corrupt), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCOMPLETE"
    found, rows = kinds(out)
    assert ("malformed_event_line", "orderer2", "") in found and ("missing_event", "orderer2", "8") in found
    mal = next(r for r in rows if r["kind"] == "malformed_event_line")
    assert mal["raw_line"] == bad and int(mal["line_no"]) > 0
    acc = next(a for a in m["inputs"] if a["container"] == "orderer2")
    assert acc["lines_malformed_event"] == 1


def test_empty_log_is_incomplete(tmp_path):
    inputs = make_logs(tmp_path / "l")
    inputs[1].path.write_bytes(b"")
    out = tmp_path / "o"
    m = lc.run_parse(inputs, CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCOMPLETE" and ("input_empty", "orderer1", "") in kinds(out)[0]


def test_unattributed_channel_and_unusable_zone(tmp_path):
    def edit(name, lines):
        if name == "orderer0":
            lines[find_line(lines, f"Writing block [2] (Raft index: 6) to ledger channel={CH}")] = oline(2, write_ms(2), channel="")
            lines[find_line(lines, f"Writing block [3] (Raft index: 7) to ledger channel={CH}")] = oline(3, write_ms(3), tz="IST")
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=edit), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCOMPLETE"
    found = kinds(out)[0]
    assert ("channel_unattributed", "orderer0", "2") in found and ("fabric_ts_unusable", "orderer0", "3") in found
    assert m["events"]["per_channel"]["<unattributed>"]["orderer_write"] == 1  # kept, not dropped


def test_no_in_scope_events(tmp_path):
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l"), "e1-r999", out)
    assert m["status"] == "INCOMPLETE" and ("no_in_scope_events", "", "") in kinds(out)[0]


# ------------------------------------------------------------------ duplicates
def test_exact_duplicates_from_overlapping_captures(tmp_path):
    inputs = make_logs(tmp_path / "l")
    first = inputs[0].path.read_text(encoding="utf-8").split("\n")
    overlap = tmp_path / "l" / "orderer0.part2.stderr.log"
    overlap.write_text("\n".join(first[-7:]), encoding="utf-8")  # last 2 blocks again, no final newline
    inputs.insert(1, lc.LogInput("orderer0", "orderer", overlap))
    out = tmp_path / "o"
    m = lc.run_parse(inputs, CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "COMPLETE_WITH_WARNINGS"
    dups = [r for r in read(out, "orderer_write.csv") if r["canonical"] == "False"]
    assert len(dups) == 4 and {r["dup_kind"] for r in dups} == {"exact"}
    assert all(r["dup_of"].startswith(str(inputs[0].path) + ":") for r in dups)
    assert m["events"]["in_scope_canonical"] == 60


def test_conflicting_duplicate_is_inconsistent(tmp_path):
    def dup(name, lines):
        if name == "peer0org1":
            lines.append(pline(6, commit_ms(6) + 900, 7))
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=dup), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCONSISTENT"
    assert ("conflicting_duplicate", "peer0org1", "6") in kinds(out)[0]
    row = next(r for r in read(out, "peer_commit.csv") if r["dup_kind"] == "conflicting")
    assert row["tx_count"] == "7" and row["canonical"] == "False"


# ------------------------------------------------------------------ ordering and reconciliation
def test_timestamp_order_violation(tmp_path):
    def back(name, lines):
        if name == "orderer0":
            lines[find_line(lines, f"Writing block [5] (Raft index: 9) to ledger channel={CH}")] = oline(5, write_ms(4) - 10)
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=back), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCONSISTENT" and ("ts_order_violation", "orderer0", "5") in kinds(out)[0]


def test_log_order_violation(tmp_path):
    def swap(name, lines):
        if name == "peer0org2":
            i, j = find_line(lines, f"[{CH}] Committed block [5]"), find_line(lines, f"[{CH}] Committed block [6]")
            lines[i], lines[j] = lines[j], lines[i]
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=swap), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCONSISTENT" and ("log_order_violation", "peer0org2", "5") in kinds(out)[0]


def test_commit_before_write(tmp_path):
    def early(name, lines):
        if name == "peer0org2":
            lines[find_line(lines, f"[{CH}] Committed block [3]")] = pline(3, write_ms(3) - 5, 10)
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=early), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCONSISTENT" and ("commit_before_write", "peer0org2", "3") in kinds(out)[0]
    assert read(out, "block_summary.csv")[2]["flags"] == "commit_before_write"


def test_commit_within_offset_bound_is_not_flagged(tmp_path):
    def same_ms(name, lines):
        if name == "peer0org2":  # 1 ms before the first write: inside the default 1 ms bound
            lines[find_line(lines, f"[{CH}] Committed block [3]")] = pline(3, write_ms(3) - 1, 10)
    assert lc.run_parse(make_logs(tmp_path / "l", mutate=same_ms), CH, tmp_path / "o")["status"] == "COMPLETE"


def test_tx_count_mismatch_with_ledger_and_between_peers(tmp_path):
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l"), CH, out, blocks_csv=blocks_csv(ntx=lambda b: 9 if b == 12 else 10))
    assert m["status"] == "INCONSISTENT"
    assert [k for k in kinds(out)[0] if k[0] == "tx_count_mismatch_ledger"] == [
        ("tx_count_mismatch_ledger", "peer0org1", "12"), ("tx_count_mismatch_ledger", "peer0org2", "12")]

    def differ(name, lines):
        if name == "peer0org2":
            lines[find_line(lines, f"[{CH}] Committed block [4]")] = pline(4, commit_ms(4), 8)
    out2 = tmp_path / "o2"
    lc.run_parse(make_logs(tmp_path / "l2", mutate=differ), CH, out2)
    assert ("peer_tx_count_disagreement", "", "4") in kinds(out2)[0]


def test_event_in_unexpected_role(tmp_path):
    def stray(name, lines):
        if name == "peer0org1":
            lines.append(oline(99, write_ms(12) + 10))
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=stray), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "INCONSISTENT" and ("event_in_unexpected_role", "peer0org1", "99") in kinds(out)[0]


# ------------------------------------------------------------------ warnings
def test_warnings_do_not_fail_the_run(tmp_path):
    def warn(name, lines):
        if name == "orderer0":
            lines[find_line(lines, f"Writing block [2] (Raft index: 6) to ledger channel={CH}")] = oline(2, write_ms(2), dock=False)
            lines[find_line(lines, f"Writing block [3] (Raft index: 7) to ledger channel={CH}")] = oline(3, write_ms(3), lag_ns=1_500_000_000)
            lines[find_line(lines, f"Writing block [4] (Raft index: 8) to ledger channel={CH}")] = oline(4, write_ms(4), lag_ns=-5_000_000)
    out = tmp_path / "o"
    m = lc.run_parse(make_logs(tmp_path / "l", mutate=warn), CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "COMPLETE_WITH_WARNINGS"
    assert m["anomalies"]["warning"] == {"docker_ts_missing": 1, "ts_docker_lag": 1, "ts_docker_before_fabric": 1}
    assert m["anomalies"]["error"] == {}


def test_invalid_utf8_is_flagged_not_dropped(tmp_path):
    inputs = make_logs(tmp_path / "l")
    p = inputs[3].path  # peer0org1
    data = p.read_bytes().replace(b"commitHash=[47dc", b"commitHash=[47dc\xff", 1)
    p.write_bytes(data)
    out = tmp_path / "o"
    m = lc.run_parse(inputs, CH, out, blocks_csv=blocks_csv())
    assert m["status"] == "COMPLETE_WITH_WARNINGS" and m["anomalies"]["warning"] == {"decode_replaced": 1}
    row = next(r for r in read(out, "peer_commit.csv") if "decode_replaced" in r["flags"])
    assert row["block"] == "1" and row["commit_hash"] == ""  # hash unreadable after replacement, event kept


# ------------------------------------------------------------------ CLI and output safety
def cli_args(inputs, out, *extra):
    args = ["parse", "--channel", CH, "--out-dir", str(out)]
    for i in inputs:
        args += [f"--{i.role}-log", f"{i.container}={i.path}"]
    return args + list(extra)


def test_cli_exit_codes_and_no_overwrite(tmp_path, capsys):
    inputs = make_logs(tmp_path / "l")
    out = tmp_path / "o"
    assert lc.main(cli_args(inputs, out, "--expect-blocks", "1:12", "--reconcile-peer", "peer0org1")) == 0
    assert "LOGS COMPLETE" in capsys.readouterr().out
    before = {p.name: p.read_bytes() for p in out.iterdir()}
    assert lc.main(cli_args(inputs, out)) == 2  # refuses to overwrite
    assert {p.name: p.read_bytes() for p in out.iterdir()} == before
    assert lc.main(cli_args(inputs, tmp_path / "o2", "--expect-blocks", "1:13")) == 1  # block 13 missing
    assert lc.main(cli_args(inputs, tmp_path / "o3", "--reconcile-peer", "orderer0")) == 2
    assert lc.main(cli_args(inputs, tmp_path / "o4", "--expect-blocks", "9:3")) == 2
    missing = [lc.LogInput("peer9", "peer", tmp_path / "nope.log")]
    assert lc.main(cli_args(missing, tmp_path / "o5")) == 2
    assert not (tmp_path / "o5").exists()


# ------------------------------------------------------------------ capture
def test_capture_plan_and_validation(capsys, tmp_path):
    cmds = lc.capture_commands(["orderer0.example.com"], "2026-11-11T10:00:00Z", "2026-11-11T10:05:00.5Z")
    assert cmds == [["docker", "logs", "--timestamps", "--since", "2026-11-11T10:00:00Z",
                     "--until", "2026-11-11T10:05:00.5Z", "orderer0.example.com"]]
    for bad in [(["bad name"], "2026-11-11T10:00:00Z", "2026-11-11T10:05:00Z"),
                (["a", "a"], "2026-11-11T10:00:00Z", "2026-11-11T10:05:00Z"),
                (["a"], "2026-11-11T10:05:00Z", "2026-11-11T10:00:00Z"),
                (["a"], "10 minutes ago", "2026-11-11T10:00:00Z"),
                (["a"], "2026-11-11T10:00:00Z junk", "2026-11-11T10:05:00Z")]:
        with pytest.raises(ValueError):
            lc.capture_commands(*bad)
    out = tmp_path / "cap"
    assert lc.main(["capture", "--container", "orderer0", "--since", "2026-11-11T10:00:00Z",
                    "--until", "2026-11-11T10:05:00Z", "--out-dir", str(out)]) == 0
    assert "PLAN docker logs --timestamps" in capsys.readouterr().out and not out.exists()  # dry run writes nothing


def test_capture_with_fake_runner(tmp_path):
    calls = []

    def runner(argv):
        calls.append(argv)
        name = argv[-1]
        return (1 if name == "peer0org2" else 0), b"", f"{name} line1\n{name} line2\n".encode()
    out = tmp_path / "cap"
    m = lc.run_capture(["orderer0", "peer0org2"], "2026-11-11T10:00:00Z", "2026-11-11T10:05:00Z", out, runner)
    assert len(calls) == 2 and m["status"] == "FAILED"
    rec = m["captures"][0]
    assert rec["exit_code"] == 0 and rec["stderr"]["lines"] == 2 and rec["stdout"]["bytes"] == 0
    assert rec["stderr"]["sha256"] == hashlib.sha256((out / "orderer0.stderr.log").read_bytes()).hexdigest()
    assert json.loads((out / "capture_manifest.json").read_text())["captures"][1]["exit_code"] == 1
    with pytest.raises(FileExistsError):
        lc.run_capture(["orderer0"], "2026-11-11T10:00:00Z", "2026-11-11T10:05:00Z", out, runner)


# ------------------------------------------------------------------ hand-off to the reconciler
def _peer_logs_from_commits(tmp: Path, peer_commits, shift=None):
    shift = shift or {}
    lines = [pline(int(p["block"]), int(p["commit_ms"]) + shift.get(int(p["block"]), 0), 10) for p in peer_commits]
    path = tmp / "peer0org1.stderr.log"
    tmp.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return [lc.LogInput("peer0org1", "peer", path)]


@pytest.mark.parametrize("shift,verdict", [({}, "PASS"), ({7: 60}, "FAIL")])
def test_reconcile_peer_file_feeds_e1_reconcile(tmp_path, shift, verdict):
    records, submits, manifest, ledger, peer = synth_run(n=200, b=10)
    out = tmp_path / "o"
    m = lc.run_parse(_peer_logs_from_commits(tmp_path / "l", peer, shift), CH, out, reconcile_peer="peer0org1")
    assert m["status"] == "COMPLETE"
    commits = read(out, "peer_commit_reconcile.csv")
    checks = rc.reconcile(records, submits, manifest, ledger, expect(), commits)
    v4 = next(c for c in checks if c.check_id == "V4-cross-source")
    assert v4.status == verdict, v4.detail  # a commit logged 60 ms after the finals must fail


def test_reconciler_rejects_duplicate_commit_rows():
    records, submits, manifest, ledger, peer = synth_run(n=200, b=10)
    checks = rc.reconcile(records, submits, manifest, ledger, expect(), peer + [peer[3]])
    v4 = next(c for c in checks if c.check_id == "V4-cross-source")
    assert v4.status == "FAIL" and "duplicate_commit_rows=1" in v4.detail
