                                                                                                                
import json
import re
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "e1"))

import e1_channel_run as cr              
import e1_la2_render as lr              
import e1_run_orchestrator as ro              
import e1_telemetry_common as tc              

TN = "/home/aryan/fabric/fabric-samples/test-network"
ENV = {"test_network": TN, "fabric_bin": "/home/aryan/fabric/bin", "fabric_cfg": "/home/aryan/fabric/fabric-samples/config",
       "repo": "/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-",
       "caliper_workspace": "/home/aryan/e1/caliper-ws",
       "chaincode": {"name": "e1asset", "version": "1.0", "sequence": 1},
       "protocol": {"event_strategy": "msp_any", "invoke_timeout_s": 60, "binding": "fabric:2.5"}}
PKG = "e1asset_1.0:" + "ab" * 32
CONFIGTX = REPO / "fabric-testbed" / "network" / "configtx" / "configtx-e1.yaml"
COMPOSE = REPO / "fabric-testbed" / "network" / "compose-e1-raft3.yaml"


def write_env(tmp_path, **over):
    p = tmp_path / "env.json"
    p.write_text(json.dumps({**ENV, **over}), encoding="utf-8")
    return p


def test_env_validation(tmp_path):
    assert lr.load_env(write_env(tmp_path))["repo"].startswith("/mnt/c/")
    for over in ({"repo": "C:\\OJT_SEM3\\repo"}, {"test_network": "relative/tn"}, {"protocol": {"binding": "x"}}):
        with pytest.raises(ValueError):
            lr.load_env(write_env(tmp_path, **over))


def test_network_json_validates_and_is_wsl_only():
    doc = lr.network(ENV, PKG)
    net = cr.Network(doc)
    assert [o["name"] for o in doc["orderers"]] == ["orderer.example.com", "orderer2.example.com", "orderer3.example.com"]
    assert doc["expect"] == {"consensus": "etcdraft", "consenters": 3, "anchor_peers_required": True,
                             "raft_leader_wait": True}
    assert [o["ops_url"] for o in doc["orderers"]] == [f"http://localhost:{p}/metrics" for p in (9443, 9446, 9447)]
    assert net.relative_base(Path("/ignored/configtx-e1.yaml")) == f"{TN}/configtx"
    assert tc.windows_style_paths(doc) == []
    assert net.peer_env(net.orgs()[0])["CORE_PEER_MSPCONFIGPATH"].endswith("users/Admin@org1.example.com/msp")
    with pytest.raises(cr.RequestError):
        cr.Network({**doc, "configtx_relative_to": "relative/dir"})


def test_e1_configtx_renders_with_the_channel_tool():
    req = cr.Request("e1-r001", "r001", 45, "2s", 10_485_760)
    text, changes, base = cr.render_configtx(CONFIGTX.read_text(encoding="utf-8"), f"{TN}/configtx", lr.PROFILE, req)
    assert base["AbsoluteMaxBytes_bytes"] == 99 * 1024 * 1024 and base["MaxMessageCount"] == "10"
    assert "    MaxMessageCount: 45" in text and "    PreferredMaxBytes: 10485760" in text and "  BatchTimeout: 2s" in text
    certs = re.findall(r"(?:Client|Server)TLSCert: (\S+)", text)
    assert len(certs) == 6 and all(c.startswith(f"{TN}/organizations/ordererOrganizations/") for c in certs)
    assert {re.search(r"orderers/([^/]+)/", c).group(1) for c in certs} == \
        {"orderer.example.com", "orderer2.example.com", "orderer3.example.com"}
    msp = re.findall(r"MSPDir: (\S+)", text)
    assert len(msp) == 3 and all(m.startswith(f"{TN}/organizations/") for m in msp)
    assert text.count("AnchorPeers:") == 2 and b"\r" not in CONFIGTX.read_bytes()


def test_compose_overlay_ports_images_and_services():
    t = COMPOSE.read_text(encoding="utf-8")
    assert "latest" not in t.split("services:", 1)[1]
    assert t.count("image: hyperledger/fabric-orderer:2.5.16") == 3 and t.count("image: hyperledger/fabric-peer:2.5.16") == 2
    assert t.count("CORE_CHAINCODE_BUILDER=hyperledger/fabric-ccenv:2.5.16") == 2
    published = re.findall(r"^\s+- (\d+):(\d+)$", t, re.M)
    assert sorted(int(a) for a, b in published) == [8050, 8053, 9050, 9053, 9446, 9447] and all(a == b for a, b in published)
    for name, port, admin, ops in lr.ORDERERS[1:]:
        assert f"ORDERER_GENERAL_LISTENPORT={port}" in t and f"ORDERER_ADMIN_LISTENADDRESS=0.0.0.0:{admin}" in t
        assert f"ORDERER_OPERATIONS_LISTENADDRESS={name}:{ops}" in t
        assert f"orderers/{name}/tls/:/var/hyperledger/orderer/tls" in t
                                                                                 
    ctx = CONFIGTX.read_text(encoding="utf-8")
    for name, port, _, _ in lr.ORDERERS:
        assert re.search(rf"- Host: {re.escape(name)}\n\s+Port: {port}\n", ctx)


def test_run_render_plan_and_caliper_files(tmp_path):
    netf = tmp_path / "network.json"
    netf.write_text(json.dumps(lr.network(ENV, PKG)), encoding="utf-8")
    r = lr.render_run(ENV, str(netf), "r001", 45, "2s", 10_485_760, 1000, 160.0, 30, "create", tmp_path / "plan.json",
                      repo_root=REPO, ws_root=tmp_path / "ws")
    plan = ro.Plan(json.loads((tmp_path / "plan.json").read_text()))
    assert plan.request.max_message_count == 45 and plan.request.preferred_max_bytes == 10_485_760
    cal = plan.raw["caliper"]
    bench = json.loads((r["dir"] / "benchmark.yaml").read_text())
    rounds = bench["test"]["rounds"]
    assert [x["label"] for x in rounds] == ["wu-r001", cal["measured_label"]] and cal["measured_round"] == 1
    assert rounds[1]["txNumber"] == 1000 and rounds[1]["workload"]["arguments"]["settleSeconds"] == 10
    assert bench["test"]["workers"]["number"] == 1
    rec = bench["monitors"]["transaction"][0]["options"]
    assert rec["outputDir"] == cal["recorder_dir_in_container"] and rec["runId"] == "r001"
    assert cal["recorder_dir"] == f"{ENV['caliper_workspace']}/runs/r001/recorder"
                                                                                             
    assert plan.recorder_base("measured_round", "measured_label").name == "r001_w0_r1_m-r001"
    netcfg = json.loads((r["dir"] / "networkconfig.yaml").read_text())
    assert netcfg["channels"][0]["channelName"] == plan.raw["channel"] == "e1-r001"
    for name, rel in lr.MODULES.items():
        assert tc.sha256_file(r["dir"] / "modules" / name) == tc.sha256_file(REPO / rel)
    assert cal["argv"][:5] == ["docker", "exec", "-w", lr.CAL_WS, "caliper"]
    assert tc.windows_style_paths(plan.raw | {"network": "/x"}) == []
    with pytest.raises(FileExistsError):
        lr.render_run(ENV, str(netf), "r001", 45, "2s", 10_485_760, 1000, 160.0, 30, "create", tmp_path / "p2.json",
                      repo_root=REPO, ws_root=tmp_path / "ws")


def test_run_without_warmup_and_rejections(tmp_path):
    netf = tmp_path / "network.json"
    netf.write_text(json.dumps(lr.network(ENV, PKG)), encoding="utf-8")
    r = lr.render_run(ENV, str(netf), "e00", 10, "2s", 524_288, 200, 10.0, 0, "fail-every", tmp_path / "p.json",
                      repo_root=REPO, ws_root=tmp_path / "ws")
    assert r["plan"]["caliper"]["measured_round"] == 0 and r["plan"]["caliper"]["warmup_label"] is None
    bench = json.loads((r["dir"] / "benchmark.yaml").read_text())
    assert len(bench["test"]["rounds"]) == 1 and bench["test"]["rounds"][0]["workload"]["arguments"]["mode"] == "fail-every"
    for bad in ({"B": 0}, {"P": 10**12}, {"tau": "2"}, {"run_id": "bad id"}, {"workload": "random"}):
        kw = {"run_id": "x1", "B": 10, "tau": "2s", "P": 524_288, "workload": "create", **bad}
        with pytest.raises(ValueError):
            lr.render_run(ENV, str(netf), kw["run_id"], kw["B"], kw["tau"], kw["P"], 200, 10.0, 0, kw["workload"],
                          tmp_path / f"q{len(bad)}.json", repo_root=REPO, ws_root=tmp_path / "ws2")


def test_duration_plan_uses_observed_count_and_preserves_warmup(tmp_path):
    netf = tmp_path / "network.json"
    netf.write_text(json.dumps(lr.network(ENV, PKG)), encoding="utf-8")
    r = lr.render_run(ENV, str(netf), "dur01", 45, "2s", 10_485_760, None, 22.0, 5, "create",
                      tmp_path / "duration-plan.json", repo_root=REPO, ws_root=tmp_path / "ws", duration_s=15)
    plan = ro.Plan(json.loads((tmp_path / "duration-plan.json").read_text()))
    assert plan.raw["cell"] == {"B": 45, "P": 10_485_760, "duration_s": 15, "mode": "txDuration",
                                  "n": 0, "rate_tps": 22.0, "tau": "2s"}
    bench = json.loads((r["dir"] / "benchmark.yaml").read_text())
    rounds = bench["test"]["rounds"]
    assert rounds[0]["txDuration"] == 5 and rounds[1]["txDuration"] == 15
    assert "txNumber" not in rounds[1]
    assert plan.raw["caliper"]["measured_round"] == 1


def test_existing_channel_override_keeps_unique_run_id(tmp_path):
    netf = tmp_path / "network.json"
    netf.write_text(json.dumps(lr.network(ENV, PKG)), encoding="utf-8")
    r = lr.render_run(ENV, str(netf), "pilotx", 45, "2s", 10_485_760, 60, 10.0, 0, "create", tmp_path / "p.json",
                      repo_root=REPO, ws_root=tmp_path / "ws", channel="e1-r002")
    assert r["plan"]["channel"] == "e1-r002"
    netcfg = json.loads((r["dir"] / "networkconfig.yaml").read_text())
    assert netcfg["channels"][0]["channelName"] == "e1-r002"
    assert r["plan"]["run_id"] == "pilotx"


def test_commands_print_only(tmp_path, capsys):
    env = write_env(tmp_path)
    assert lr.main(["commands", "--env", str(env)]) == 0
    out = capsys.readouterr().out
    assert "compose-e1-raft3.yaml config --quiet" in out and "--entrypoint sleep hyperledger/caliper:0.6.0" in out
    assert "printed only" in out and list(tmp_path.iterdir()) == [env]
    assert lr.main(["network", "--env", str(env), "--package-id", "bad", "--out", str(tmp_path / "n.json")]) == 2
    assert lr.main(["network", "--env", str(env), "--package-id", PKG, "--out", str(tmp_path / "n.json")]) == 0
    assert lr.main(["network", "--env", str(env), "--package-id", PKG, "--out", str(tmp_path / "n.json")]) == 2


def test_fabric_cli_tools_refuse_real_execution_off_linux(tmp_path, capsys):
                                                                                                             
                                                                                            
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import e1_block_fetch as bf
    import e1_chaincode_deploy as cd
    import synthetic_fabric as sf
    netf = tmp_path / "network.json"
    netf.write_text(json.dumps(sf.network(tmp_path)), encoding="utf-8")
    assert cr.main(["--network", str(netf), "--channel", "e1-r001", "--run-id", "r001", "--max-message-count", "10",
                    "--batch-timeout", "2s", "--preferred-max-bytes", "524288", "--run-dir", str(tmp_path / "ch"),
                    "--execute"], platform_name="win32") == 2
    assert bf.main(["fetch", "--network", str(netf), "--channel", "e1-r001", "--max-message-count", "10",
                    "--preferred-max-bytes", "524288", "--out-dir", str(tmp_path / "bl"), "--execute"],
                   platform_name="win32") == 2
    assert cd.main(["--network", str(netf), "--cc-dir", str(tmp_path), "--label", "e1asset_1.0", "--out-dir",
                    str(tmp_path / "cc"), "--execute"], platform_name="win32") == 2
    out = capsys.readouterr().out
    assert out.count("Ubuntu WSL distro") == 3
    assert not any((tmp_path / d).exists() for d in ("ch", "bl", "cc"))


def test_bootstrap_network_is_for_chaincode_deploy_only(tmp_path):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import synthetic_fabric as sf
    boot = lr.network(ENV, None, bootstrap=True)
    assert boot["bootstrap"] is True and boot["chaincode"]["package_id"] == "e1asset_1.0:" + "0" * 64
    with pytest.raises(ValueError):
        lr.network(ENV, PKG, bootstrap=True)
    with pytest.raises(ValueError):
        lr.network(ENV, None)
                                                          
    local = {**sf.network(tmp_path), "bootstrap": True}
    with pytest.raises(cr.RequestError, match="bootstrap"):
        cr.run(cr.Request("e1-r001", "r001", 10, "2s", 524288), cr.Network(local), tmp_path / "run", lambda a, e: (0, b"", b""))
    assert not (tmp_path / "run").exists()
                                       
    netf = tmp_path / "boot.json"
    netf.write_text(json.dumps(local), encoding="utf-8")
    plan = lr.run_plan(ENV, str(netf), "r001", "e1-r001", {"B": 10, "tau": "2s", "P": 524288, "n": 1}, 0, True)
    with pytest.raises(ro.PlanError, match="bootstrap"):
        ro.Plan(plan)
