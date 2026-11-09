"""Synthetic tests of the channel-per-run script with a fake Fabric CLI. The configtx.yaml below is
modelled on the fabric-samples test-network layout from memory; it is not the pinned file. Decoded
blocks are synthetic configtxlator-style JSON. Nothing here is an E0/E1 result."""
import hashlib
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_channel_run as cr  # noqa: E402

BASE = """\
---
Organizations:
  - &OrdererOrg
    Name: OrdererOrg
    ID: OrdererMSP
    MSPDir: ../organizations/ordererOrganizations/example.com/msp
  - &Org1
    Name: Org1MSP
    ID: Org1MSP
    MSPDir: "../organizations/peerOrganizations/org1.example.com/msp"
  - &Org2
    Name: Org2MSP
    ID: Org2MSP
    MSPDir: ../organizations/peerOrganizations/org2.example.com/msp

Orderer: &OrdererDefaults
  # Batch Timeout: The amount of time to wait before creating a batch
  BatchTimeout: 2s
  # Batch Size: Controls the number of messages batched into a block
  BatchSize:
    # Max Message Count: The maximum number of messages to permit in a batch
    MaxMessageCount: 10   # fabric-samples default
    AbsoluteMaxBytes: 99 MB
    PreferredMaxBytes: 512 KB
  Organizations:

Profiles:
  ChannelUsingRaft:
    Orderer:
      <<: *OrdererDefaults
      OrdererType: etcdraft
      EtcdRaft:
        Consenters:
          - Host: orderer0.example.com
            Port: 7050
            ClientTLSCert: ../organizations/ordererOrganizations/example.com/orderers/orderer0/tls/server.crt
            ServerTLSCert: /abs/server.crt
      Organizations:
        - *OrdererOrg
    Application:
      Organizations:
        - *Org1
        - *Org2
"""
MIB99 = 99 * 1024 * 1024


# ------------------------------------------------------------------ network fixture
def make_network(tmp: Path, orderers=3, peers=(("peer0org1", "Org1MSP"), ("peer0org2", "Org2MSP")), base=BASE, **extra):
    tmp.mkdir(parents=True, exist_ok=True)
    (tmp / "configtx").mkdir(exist_ok=True)
    (tmp / "configtx" / "configtx.yaml").write_text(base, encoding="utf-8", newline="")

    def f(name):
        p = tmp / name
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.write_text("x", encoding="utf-8")
        return str(p)
    net = {
        "configtx_base": str(tmp / "configtx" / "configtx.yaml"), "profile": "ChannelUsingRaft",
        "bin_dir": "/opt/fabric/bin", "fabric_cfg_path": "/opt/fabric/config", "orderer_ca": f("tlsca.pem"),
        "orderers": [{"name": f"orderer{i}", "address": f"localhost:{7050 + 100 * i}",
                      "tls_hostname": f"orderer{i}.example.com", "admin_address": f"localhost:{7053 + 100 * i}",
                      "admin_client_cert": f(f"o{i}/client.crt"), "admin_client_key": f(f"o{i}/client.key")}
                     for i in range(orderers)],
        "peers": [{"name": n, "msp_id": m, "address": f"localhost:{7051 + 2000 * k}",
                   "tls_root_cert": f(f"{n}/ca.crt"), "admin_msp_dir": f(f"{n}/msp/x").rsplit("x", 1)[0].rstrip("/\\")}
                  for k, (n, m) in enumerate(peers)],
        "chaincode": {"name": "e1asset", "version": "1.0", "sequence": 1, "package_id": "e1asset_1.0:" + "ab" * 32},
        "expect": {"consensus": "etcdraft", "consenters": orderers},
        "poll_attempts": 3, "poll_sleep_s": 0.5, "peer_join_attempts": 3, "peer_join_sleep_s": 2.0,
    }
    net.update(extra)
    return net


def req(**kw):
    base = dict(channel="e1-r001", run_id="r001", max_message_count=45, batch_timeout="2s",
                preferred_max_bytes=10_485_760)
    base.update(kw)
    return cr.Request(**base)


# ------------------------------------------------------------------ fake Fabric CLI
def go_duration(ms: int) -> str:
    """Go time.Duration.String() for whole ms (enough for the values used here)."""
    if ms < 1000:
        return f"{ms}ms"
    s, rem = divmod(ms, 1000)
    frac = f".{rem:03d}".rstrip("0") if rem else ""
    if s < 60:
        return f"{s}{frac}s"
    return f"{s // 60}m{s % 60}{frac}s"


def _grab(text, key):
    return re.search(rf"^\s*{key}:\s*([^#\n]*?)\s*(#.*)?$", text, re.M).group(1)


class FakeFabric:
    """Imitates the CLI tools' observable behaviour. Hooks let a test break one thing."""

    def __init__(self, orderers=3, msps=("Org1MSP", "Org2MSP"), anchors=True, fail=None, osn_join_status=201,
                 list_states=("active",), approvals=None, genesis_edit=None, fetched_edit=None, raise_on=None):
        self.calls, self.orderers, self.msps, self.anchors = [], orderers, msps, anchors
        self.fail, self.osn_join_status, self.raise_on = fail or {}, osn_join_status, raise_on
        self.list_states = {}
        self.list_template = list(list_states)
        self.approved, self.approvals_override = set(), approvals
        self.genesis_edit, self.fetched_edit = genesis_edit, fetched_edit
        self.genesis_bytes = None

    def _doc(self, cfgtx: str, channel: str) -> dict:
        tau_ms = cr.tau_to_ms(_grab(cfgtx, "BatchTimeout"))
        orderer = {
            "BatchSize": {"value": {"max_message_count": int(_grab(cfgtx, "MaxMessageCount")),
                                    "absolute_max_bytes": cr.parse_byte_size(_grab(cfgtx, "AbsoluteMaxBytes")),
                                    "preferred_max_bytes": cr.parse_byte_size(_grab(cfgtx, "PreferredMaxBytes"))}},
            "BatchTimeout": {"value": {"timeout": go_duration(tau_ms)}},
            "ConsensusType": {"value": {"type": "etcdraft", "metadata": {
                "consenters": [{"host": f"orderer{i}.example.com", "port": 7050} for i in range(self.orderers)]}}},
        }
        app = {m: {"values": {"MSP": {"value": {"config": {"name": m}}},
                              **({"AnchorPeers": {"value": {"anchor_peers": [{"host": "p", "port": 7051}]}}}
                                 if self.anchors else {})}} for m in self.msps}
        config = {"channel_group": {"groups": {"Orderer": {"values": orderer}, "Application": {"groups": app}}}}
        body = json.dumps(config, sort_keys=True)
        return {"header": {"number": "0", "data_hash": hashlib.sha256((channel + body).encode()).hexdigest()},
                "data": {"data": [{"payload": {"data": {"config": config}}}]}}

    def __call__(self, argv, env):
        self.calls.append((argv, env))
        tool = argv[0].rsplit("/", 1)[-1]
        sub = " ".join(a for a in argv[1:4] if not a.startswith("-"))
        key = f"{tool} {sub}"
        if self.raise_on and self.raise_on in key:
            raise FileNotFoundError(argv[0])
        for pat, n in list(self.fail.items()):
            if pat in key and n:
                self.fail[pat] = n - 1
                return 1, b"", f"Error: injected failure in {key}".encode()

        def arg(flag):
            return argv[argv.index(flag) + 1]
        if tool == "configtxgen":
            cfgtx = (Path(arg("-configPath")) / "configtx.yaml").read_text(encoding="utf-8")
            doc = self._doc(cfgtx, arg("-channelID"))
            self.genesis_bytes = json.dumps(doc).encode()
            Path(arg("-outputBlock")).write_bytes(self.genesis_bytes)
            return 0, b"", b"INFO Generating genesis block\n"
        if tool == "configtxlator":
            doc = json.loads(Path(arg("--input")).read_bytes())
            edit = self.genesis_edit if arg("--input").endswith("genesis.block") else self.fetched_edit
            if edit:
                edit(doc)
            Path(arg("--output")).write_text(json.dumps(doc), encoding="utf-8")
            return 0, b"", b""
        if tool == "osnadmin" and argv[2] == "join":
            body = {"name": arg("--channelID"), "consensusRelation": "consenter", "status": "active", "height": 1}
            return 0, f"Status: {self.osn_join_status}\n{json.dumps(body, indent=1)}\n".encode(), b""
        if tool == "osnadmin" and argv[2] == "list":
            states = self.list_states.setdefault(arg("-o"), list(self.list_template))
            state = states.pop(0) if len(states) > 1 else states[0]
            body = {"name": arg("--channelID"), "consensusRelation": "consenter", "status": state, "height": 1}
            return 0, f"Status: 200\n{json.dumps(body)}\n".encode(), b""
        if tool == "peer" and argv[1:3] == ["channel", "join"]:
            return 0, b"", b"INFO Successfully submitted proposal to join channel\n"
        if tool == "peer" and argv[1:3] == ["channel", "getinfo"]:
            return 0, b'Blockchain info: {"height":1,"currentBlockHash":"abc"}\n', b""
        if tool == "peer" and argv[1:3] == ["channel", "fetch"]:
            Path(argv[4]).write_bytes(self.genesis_bytes)
            return 0, b"", b"INFO Received block: 0\n"
        if tool == "peer" and argv[1] == "lifecycle":
            verb = argv[3]
            if verb == "approveformyorg":
                self.approved.add(env["CORE_PEER_LOCALMSPID"])
                return 0, b"", b""
            approvals = self.approvals_override or {m: m in self.approved for m in self.msps}
            if verb == "checkcommitreadiness":
                return 0, json.dumps({"approvals": approvals}).encode(), b""
            if verb == "commit":
                return 0, b"", b""
            if verb == "querycommitted":
                return 0, json.dumps({"sequence": 1, "version": "1.0", "endorsement_plugin": "escc",
                                      "approvals": approvals}).encode(), b""
        raise AssertionError(f"unexpected command {argv}")


def execute(tmp, fake=None, request=None, net=None, **kw):
    net = cr.Network(net or make_network(tmp / "net"))
    fake = fake or FakeFabric()
    sleeps = []
    clock = iter(range(10_000))
    rec = cr.run(request or req(), net, tmp / "run", fake, sleep=sleeps.append, monotonic=lambda: float(next(clock)),
                 now=lambda: "2026-11-09T00:00:00.000+00:00", **kw)
    return rec, fake, sleeps


def labels(rec):
    return [s["label"] for s in rec["steps"]]


# ------------------------------------------------------------------ request validation
@pytest.mark.parametrize("b,tau,p", [(40, "2s", 10_485_760), (45, "2s", 10_485_760), (10, "2s", 524_288),
                                     (1000, "4s", 524_288), (1, "1ms", 1), (50, "1.5s", 524_288)])
def test_valid_requests(b, tau, p):
    r = req(max_message_count=b, batch_timeout=tau, preferred_max_bytes=p)
    assert r.as_record()["B_max_message_count"] == b and r.tau_ms == cr.tau_to_ms(tau)


@pytest.mark.parametrize("kw", [
    dict(max_message_count=0), dict(max_message_count=-1), dict(max_message_count=True),
    dict(max_message_count=1.5), dict(max_message_count="45"), dict(max_message_count=2**32),
    dict(preferred_max_bytes=0), dict(preferred_max_bytes=2**32), dict(preferred_max_bytes=None),
    dict(absolute_max_bytes=1000, preferred_max_bytes=1001), dict(absolute_max_bytes=0),
    dict(batch_timeout="0s"), dict(batch_timeout="-1s"), dict(batch_timeout="2"), dict(batch_timeout=""),
    dict(batch_timeout="1.5ms"), dict(batch_timeout="250us"), dict(batch_timeout="61s"), dict(batch_timeout="2 s"),
    dict(batch_timeout="2S"), dict(batch_timeout=2), dict(batch_timeout="1h"),
    dict(channel="E1-r001"), dict(channel="1ch"), dict(channel="e1_r001"), dict(channel="e" * 250), dict(channel=""),
    dict(run_id="r 1"), dict(run_id=""), dict(run_id="r" * 65),
])
def test_invalid_requests_rejected(kw):
    with pytest.raises(cr.RequestError):
        req(**kw)


@pytest.mark.parametrize("text,ms", [("2s", 2000), ("500ms", 500), ("1.5s", 1500), ("0.7s", 700), ("1m30s", 90_000),
                                     ("1000000ns", 1), ("1000us", 1), ("2m0s", 120_000)])
def test_tau_exact_ms(text, ms):
    assert cr.tau_to_ms(text) == ms


@pytest.mark.parametrize("ms,text", [(2000, "2s"), (500, "500ms"), (1500, "1500ms"), (60_000, "60s")])
def test_canonical_tau_round_trips(ms, text):
    assert cr.canonical_tau(ms) == text and cr.tau_to_ms(text) == ms


@pytest.mark.parametrize("text,n", [("512 KB", 524_288), ("99 MB", MIB99), ("10 MB", 10_485_760), ("524288", 524_288),
                                    ("2 GB", 2 * 1024**3), ("512k", 524_288), (" 1 Mb ", 1_048_576)])
def test_byte_sizes(text, n):
    assert cr.parse_byte_size(text) == n


@pytest.mark.parametrize("text", ["", "512 KiB", "1.5 MB", "-1", "MB", "10 TB"])
def test_bad_byte_sizes(text):
    with pytest.raises(cr.ConfigtxError):
        cr.parse_byte_size(text)


# ------------------------------------------------------------------ configtx rendering
def test_render_sets_only_requested_values_and_paths():
    text, changes, base_vals = cr.render_configtx(BASE, "/w/test-network/configtx", "ChannelUsingRaft",
                                                  req(batch_timeout="2500ms"))
    before, after = BASE.split("\n"), text.split("\n")
    diff = [(i, a, b) for i, (a, b) in enumerate(zip(before, after)) if a != b]
    assert len(before) == len(after) and len(diff) == len(changes) == 3 + 4  # 3 requested + 4 relative paths
    assert "  BatchTimeout: 2500ms" in after and "    MaxMessageCount: 45   # fabric-samples default" in after
    assert "    PreferredMaxBytes: 10485760" in after and "    AbsoluteMaxBytes: 99 MB" in after  # A kept
    assert '    MSPDir: "/w/test-network/organizations/peerOrganizations/org1.example.com/msp"' in after
    assert "            ServerTLSCert: /abs/server.crt" in after  # absolute path untouched
    assert {c["key"] for c in changes if c["reason"] == "requested"} == {"BatchTimeout", "MaxMessageCount",
                                                                         "PreferredMaxBytes"}
    assert base_vals["AbsoluteMaxBytes_bytes"] == MIB99 and base_vals["MaxMessageCount"] == "10"
    # comments untouched
    assert [l for l in before if l.lstrip().startswith("#")] == [l for l in after if l.lstrip().startswith("#")]


def test_unchanged_value_is_not_reported_as_a_change():
    _, changes, _ = cr.render_configtx(BASE, "/w", "ChannelUsingRaft", req())  # tau 2s equals the base
    assert "BatchTimeout" not in {c["key"] for c in changes}


def test_render_absolute_max_bytes_when_requested_and_crlf_preserved():
    text, changes, _ = cr.render_configtx(BASE.replace("\n", "\r\n"), "/w", "ChannelUsingRaft",
                                          req(absolute_max_bytes=20_000_000))
    assert "\r\n    AbsoluteMaxBytes: 20000000\r\n" in text and "\n" not in text.replace("\r\n", "")


@pytest.mark.parametrize("mutate,msg", [
    (lambda t: t.replace("      <<: *OrdererDefaults\n", "      <<: *OrdererDefaults\n      BatchTimeout: 9s\n"), "outside"),
    (lambda t: t.replace("    PreferredMaxBytes: 512 KB\n", "    PreferredMaxBytes: 512 KB\n    PreferredMaxBytes: 1 MB\n"), "times"),
    (lambda t: t.replace("Orderer: &OrdererDefaults", "Orderer: &Defaults"), "OrdererDefaults"),
    (lambda t: t.replace("  ChannelUsingRaft:", "  OtherProfile:"), "profile"),
    (lambda t: t.replace("      <<: *OrdererDefaults\n", ""), "inherit"),
    (lambda t: t.replace("  BatchSize:\n", "  Batch:\n"), "BatchSize"),
    (lambda t: t.replace("    AbsoluteMaxBytes: 99 MB", "    AbsoluteMaxBytes: 1 MB"), "PreferredMaxBytes"),
    (lambda t: t.replace("    AbsoluteMaxBytes: 99 MB", "    AbsoluteMaxBytes: lots"), "byte size"),
])
def test_render_refuses_unsafe_base(mutate, msg):
    with pytest.raises(cr.ConfigtxError, match=msg):
        cr.render_configtx(mutate(BASE), "/w", "ChannelUsingRaft", req())


def test_commented_override_is_ignored():
    text, _, _ = cr.render_configtx(BASE.replace("      <<: *OrdererDefaults\n",
                                                 "      <<: *OrdererDefaults\n      # BatchTimeout: 9s\n"),
                                    "/w", "ChannelUsingRaft", req())
    assert "      # BatchTimeout: 9s" in text


# ------------------------------------------------------------------ plan
def test_plan_order_and_arguments(tmp_path):
    net = cr.Network(make_network(tmp_path))
    steps = cr.plan(req(), net, "/runs/r001")
    names = [s.label for s in steps]
    assert names == (["configtxgen", "decode_genesis"] + [f"osnadmin_join:orderer{i}" for i in range(3)]
                     + [f"osnadmin_list:orderer{i}" for i in range(3)] + ["peer_join:peer0org1", "peer_join:peer0org2",
                     "peer_getinfo:peer0org1", "peer_getinfo:peer0org2", "approve:Org1MSP", "approve:Org2MSP",
                     "checkcommitreadiness", "commit", "querycommitted", "fetch_config", "decode_config"])
    assert steps[0].argv == ["/opt/fabric/bin/configtxgen", "-configPath", "/runs/r001/configtx", "-profile",
                             "ChannelUsingRaft", "-outputBlock", "/runs/r001/genesis.block", "-channelID", "e1-r001"]
    commit = next(s for s in steps if s.label == "commit").argv
    assert commit.count("--peerAddresses") == 2 and "--package-id" not in commit
    approve = next(s for s in steps if s.label == "approve:Org2MSP")
    assert approve.env["CORE_PEER_LOCALMSPID"] == "Org2MSP" and approve.env["CORE_PEER_TLS_ENABLED"] == "true"
    assert all(isinstance(a, str) for s in steps for a in s.argv)  # argv lists, never a shell string


@pytest.mark.parametrize("edit", [
    lambda n: n.pop("orderers"), lambda n: n["peers"][0].pop("msp_id"), lambda n: n.update(orderers=[]),
    lambda n: n["chaincode"].update(package_id="e1asset_1.0"), lambda n: n["chaincode"].update(sequence="1"),
    lambda n: n["peers"].append(dict(n["peers"][0])),
])
def test_bad_network_description(tmp_path, edit):
    net = make_network(tmp_path)
    edit(net)
    with pytest.raises(cr.RequestError):
        cr.Network(net)


# ------------------------------------------------------------------ execution with the fake CLI
def test_happy_path_verified_and_traceable(tmp_path):
    rec, fake, sleeps = execute(tmp_path)
    assert rec["status"] == "VERIFIED" and rec["failed_step"] is None and sleeps == []
    assert all(c["pass"] for c in rec["checks"]) and {c["source"] for c in rec["checks"]} == {"orderer config block"}
    eff = rec["effective"]["orderer_values"]
    assert (eff["max_message_count"], eff["batch_timeout_ms"], eff["preferred_max_bytes"], eff["absolute_max_bytes"]) \
        == (45, 2000.0, 10_485_760, MIB99)
    assert rec["effective"]["data_hash"] == rec["genesis"]["data_hash"]
    names = {c["check"] for c in rec["checks"]}
    assert {"MaxMessageCount", "BatchTimeout_ms", "PreferredMaxBytes", "AbsoluteMaxBytes", "ConsensusType",
            "Consenters", "ApplicationOrgs", "ConfigBlockNumber", "ConfigBlockIsGeneratedGenesis"} <= names
    run = tmp_path / "run"
    on_disk = json.loads((run / "channel_run_record.json").read_text(encoding="utf-8"))
    assert on_disk["status"] == "VERIFIED"
    assert rec["configtx"]["rendered_sha256"] == hashlib.sha256((run / "configtx" / "configtx.yaml").read_bytes()).hexdigest()
    for s in rec["steps"]:
        for stream in ("stdout", "stderr"):
            assert hashlib.sha256((run / s[stream]["file"]).read_bytes()).hexdigest() == s[stream]["sha256"]
    assert len(rec["steps"]) == 19 and rec["timing"]["setup_s"] > 0
    assert rec["chaincode"]["approvals"] == {"Org1MSP": True, "Org2MSP": True}


def test_cli_execute_and_dry_run(tmp_path, capsys):
    netfile = tmp_path / "network.json"
    netfile.write_text(json.dumps(make_network(tmp_path / "net")), encoding="utf-8")
    args = ["--network", str(netfile), "--channel", "e1-r001", "--run-id", "r001", "--max-message-count", "40",
            "--batch-timeout", "2s", "--preferred-max-bytes", "10485760"]
    fake = FakeFabric()
    assert cr.main(args + ["--run-dir", str(tmp_path / "dry")], runner=fake) == 0
    out = capsys.readouterr().out
    assert "PLAN configtxgen" in out and not (tmp_path / "dry").exists() and fake.calls == []
    assert cr.main(args + ["--run-dir", str(tmp_path / "x"), "--execute"], runner=fake,
                   sleep=lambda s: None) == 0
    assert "CHANNEL VERIFIED" in capsys.readouterr().out
    assert cr.main(args + ["--run-dir", str(tmp_path / "x"), "--execute"], runner=FakeFabric()) == 2  # exists
    bad = [a if a != "40" else "0" for a in args]
    assert cr.main(bad + ["--run-dir", str(tmp_path / "y"), "--execute"], runner=FakeFabric()) == 2
    assert not (tmp_path / "y").exists()


def _set(path):
    def edit(doc):
        node = doc
        for k in path[:-2]:
            node = node[k]
        node[path[-2]] = path[-1]
    return edit


ORD = ["data", "data", 0, "payload", "data", "config", "channel_group", "groups", "Orderer", "values"]


def test_effective_mismatch_detected(tmp_path):
    fake = FakeFabric(fetched_edit=_set(ORD + ["BatchSize", "value", "max_message_count", 10]))
    rec, _, _ = execute(tmp_path, fake)
    assert rec["status"] == "CONFIG_MISMATCH" and rec["failed_step"] == "decode_config"
    bad = [c for c in rec["checks"] if not c["pass"]]
    assert [(c["check"], c["requested"], c["decoded"]) for c in bad][0] == ("MaxMessageCount", 45, 10)
    assert cr.EXIT[rec["status"]] == 1


def test_genesis_gate_stops_before_any_join(tmp_path):
    fake = FakeFabric(genesis_edit=_set(ORD + ["BatchTimeout", "value", "timeout", "1s"]))
    rec, fake, _ = execute(tmp_path, fake)
    assert rec["status"] == "CONFIG_MISMATCH" and rec["failed_step"] == "decode_genesis"
    assert labels(rec) == ["configtxgen", "decode_genesis"]
    assert not any(c[0][0].endswith(("osnadmin", "peer")) for c in fake.calls)
    assert {c["source"] for c in rec["checks"]} == {"genesis"}


def test_absolute_max_bytes_checked_against_base_when_not_requested(tmp_path):
    fake = FakeFabric(fetched_edit=_set(ORD + ["BatchSize", "value", "absolute_max_bytes", 50_000_000]))
    rec, _, _ = execute(tmp_path, fake)
    assert rec["status"] == "CONFIG_MISMATCH"
    assert [c["check"] for c in rec["checks"] if not c["pass"]] == ["AbsoluteMaxBytes"]


def test_consenters_and_anchor_peers(tmp_path):
    rec, _, _ = execute(tmp_path / "a", FakeFabric(orderers=1))
    assert rec["status"] == "CONFIG_MISMATCH" and "Consenters" in [c["check"] for c in rec["checks"] if not c["pass"]]
    net = make_network(tmp_path / "b" / "net")
    net["expect"]["anchor_peers_required"] = True
    rec, _, _ = execute(tmp_path / "b", FakeFabric(anchors=False), net=net)
    assert rec["status"] == "CONFIG_MISMATCH" and [c["check"] for c in rec["checks"] if not c["pass"]] == ["AnchorPeers"]
    rec, _, _ = execute(tmp_path / "c", FakeFabric(msps=("Org1MSP",)))
    assert [c["check"] for c in rec["checks"] if not c["pass"]] == ["ApplicationOrgs"]


def test_fetched_block_is_not_the_generated_genesis(tmp_path):
    def other(doc):
        doc["header"]["data_hash"] = "ff" * 32
    rec, _, _ = execute(tmp_path, FakeFabric(fetched_edit=other))
    assert rec["status"] == "CONFIG_MISMATCH"
    assert [c["check"] for c in rec["checks"] if not c["pass"]] == ["ConfigBlockIsGeneratedGenesis"]


def test_config_block_number_changed(tmp_path):
    def bump(doc):
        doc["header"]["number"] = "3"
    rec, _, _ = execute(tmp_path, FakeFabric(fetched_edit=bump))
    assert rec["status"] == "CONFIG_MISMATCH" and "ConfigBlockNumber" in [c["check"] for c in rec["checks"] if not c["pass"]]


@pytest.mark.parametrize("edit", [
    lambda d: d["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"].pop("BatchTimeout"),
    lambda d: d.pop("header"),
    lambda d: d["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"].pop("Application"),
])
def test_unverifiable_when_fields_missing(tmp_path, edit):
    rec, _, _ = execute(tmp_path, FakeFabric(fetched_edit=edit))
    assert rec["status"] == "UNVERIFIABLE" and rec["failed_step"] == "decode_config" and cr.EXIT["UNVERIFIABLE"] == 4


def test_fetch_failure_is_unverifiable(tmp_path):
    rec, _, _ = execute(tmp_path, FakeFabric(fail={"peer channel fetch": 99}))
    assert rec["status"] == "UNVERIFIABLE" and rec["failed_step"] == "fetch_config"


def test_peer_join_retries_then_incomplete(tmp_path):
    rec, fake, sleeps = execute(tmp_path, FakeFabric(fail={"peer channel join": 99}))
    assert rec["status"] == "INCOMPLETE_SETUP" and rec["failed_step"] == "peer_join:peer0org1"
    assert [s["attempt"] for s in rec["steps"] if s["label"] == "peer_join:peer0org1"] == [1, 2, 3]
    assert sleeps == [2.0, 2.0] and labels(rec)[-1] == "peer_join:peer0org1"  # nothing after the failure
    assert cr.EXIT[rec["status"]] == 3


def test_peer_join_succeeds_on_retry(tmp_path):
    rec, _, sleeps = execute(tmp_path, FakeFabric(fail={"peer channel join": 1}))
    assert rec["status"] == "VERIFIED" and sleeps == [2.0]


def test_osnadmin_join_wrong_status(tmp_path):
    rec, _, _ = execute(tmp_path, FakeFabric(osn_join_status=405))
    assert rec["status"] == "INCOMPLETE_SETUP" and rec["failed_step"] == "osnadmin_join:orderer0"
    assert "HTTP 405" in rec["detail"]


def test_orderer_onboarding_then_active(tmp_path):
    rec, _, sleeps = execute(tmp_path, FakeFabric(list_states=("onboarding", "active")))
    assert rec["status"] == "VERIFIED" and sleeps == [0.5, 0.5, 0.5]
    assert rec["participation"]["orderer0"]["list"]["status"] == "active"


def test_orderer_never_active(tmp_path):
    rec, _, _ = execute(tmp_path, FakeFabric(list_states=("onboarding",)))
    assert rec["status"] == "INCOMPLETE_SETUP" and rec["failed_step"] == "osnadmin_list:orderer0"
    assert len([s for s in rec["steps"] if s["label"] == "osnadmin_list:orderer0"]) == 3


def test_chaincode_not_approved(tmp_path):
    rec, _, _ = execute(tmp_path, FakeFabric(approvals={"Org1MSP": True, "Org2MSP": False}))
    assert rec["status"] == "INCOMPLETE_SETUP" and rec["failed_step"] == "checkcommitreadiness"


def test_missing_binary_is_recorded(tmp_path):
    rec, _, _ = execute(tmp_path, FakeFabric(raise_on="configtxgen"))
    assert rec["status"] == "INCOMPLETE_SETUP" and rec["steps"][0]["exit_code"] is None
    assert "FileNotFoundError" in rec["detail"]


def test_existing_run_dir_and_missing_files_refused(tmp_path):
    (tmp_path / "run").mkdir()
    fake = FakeFabric()
    with pytest.raises(cr.RequestError, match="exists"):
        execute(tmp_path, fake)
    assert fake.calls == [] and list((tmp_path / "run").iterdir()) == []
    net = make_network(tmp_path / "n2")
    net["orderers"][1]["admin_client_key"] = str(tmp_path / "absent.key")
    fake = FakeFabric()
    with pytest.raises(cr.RequestError, match="do not exist"):
        cr.run(req(), cr.Network(net), tmp_path / "run2", fake)
    assert fake.calls == [] and not (tmp_path / "run2").exists()


def test_e0_cells_render_and_verify(tmp_path):
    """E0-1 (B=10, tau=2s, P=512 KiB) and E0-1b (B=1000, tau=4s, P=512 KiB) flow through unchanged."""
    for k, (b, tau, p) in enumerate([(10, "2s", 524_288), (1000, "4s", 524_288), (50, "1500ms", 524_288)]):
        rec, _, _ = execute(tmp_path / str(k), request=req(max_message_count=b, batch_timeout=tau, preferred_max_bytes=p))
        assert rec["status"] == "VERIFIED", rec["detail"]
        assert rec["effective"]["orderer_values"]["batch_timeout_raw"] == go_duration(cr.tau_to_ms(tau))
