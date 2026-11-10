                                                                                                 

                                                                                                        
                                                                                                           
                                                                                                          
                                                                                                        

        
                                                                                                              
                                                                                                      
                                                                       
                                                                                                         
                                                                                                           
                                                                         
                                                                                                                  

           
                                                                                           
                                                                                                 
                                                                                                                       
                                                                                                  
                                                                                                         
                                                                                                       
                                                                                    
                                                                                                     
                                                                                                              
                      
   
from __future__ import annotations

import argparse
import json
import posixpath
import re
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_telemetry_common as tc              

TOOL_VERSION = "e1_la2_render 0.1 (2026-11-09)"
CAL_WS, CAL_ORGS, CAL_CONTAINER = "/hyperledger/caliper/workspace", "/fabric/organizations", "caliper"
CALIPER_IMAGE = "hyperledger/caliper:0.6.0"
PROFILE = "E1ChannelRaft3"
ORDERERS = [("orderer.example.com", 7050, 7053, 9443), ("orderer2.example.com", 8050, 8053, 9446),
            ("orderer3.example.com", 9050, 9053, 9447)]
PEERS = [("peer0.org1.example.com", "Org1MSP", "org1.example.com", 7051, 9444),
         ("peer0.org2.example.com", "Org2MSP", "org2.example.com", 9051, 9445)]
MODULES = {"e1_per_tx_recorder.js": "scripts/e1/caliper/e1_per_tx_recorder.js",
           "e1_create_asset.js": "fabric-testbed/caliper/workloads/e1_create_asset.js",
           "e1_workload_core.js": "fabric-testbed/caliper/workloads/e1_workload_core.js"}
ENV_KEYS = {"test_network", "fabric_bin", "fabric_cfg", "repo", "caliper_workspace", "chaincode", "protocol"}
RUN_ID = re.compile(r"^[A-Za-z0-9_.-]{1,40}$")


def load_env(path: Path) -> dict:
    env = json.loads(path.read_text(encoding="utf-8"))
    missing, extra = ENV_KEYS - set(env), set(env) - ENV_KEYS
    if missing or extra:
        raise ValueError(f"env keys: missing {sorted(missing)}, unknown {sorted(extra)}")
    for k in ("test_network", "fabric_bin", "fabric_cfg", "repo", "caliper_workspace"):
        if not isinstance(env[k], str) or not env[k].startswith("/") or tc.windows_style_paths(env[k]):
            raise ValueError(f"env {k} must be an absolute POSIX path inside WSL, got {env[k]!r}")
    for k in ("event_strategy", "invoke_timeout_s", "binding"):
        if k not in env["protocol"]:
            raise ValueError(f"env protocol.{k} missing")
    return env


def _org(tn: str, *parts: str) -> str:
    return posixpath.join(tn, "organizations", *parts)


                                                                                 
def network(env: dict, package_id: str | None, bootstrap: bool = False) -> dict:
                                                                                                                
                                                                                                                      
    if bootstrap == (package_id is not None):
        raise ValueError("give exactly one of a package id or bootstrap")
    if bootstrap:
        package_id = f"{env['chaincode']['name']}_{env['chaincode']['version']}:" + "0" * 64
    tn = env["test_network"]
    oo = ("ordererOrganizations", "example.com")
    return {**({"bootstrap": True} if bootstrap else {}),
        "configtx_base": posixpath.join(env["repo"], "fabric-testbed/network/configtx/configtx-e1.yaml"),
        "configtx_relative_to": posixpath.join(tn, "configtx"),
        "profile": PROFILE, "fabric_cfg_path": env["fabric_cfg"], "bin_dir": env["fabric_bin"],
        "orderer_ca": _org(tn, *oo, "tlsca", "tlsca.example.com-cert.pem"),
        "orderers": [{"name": n, "address": f"localhost:{port}", "tls_hostname": n, "admin_address": f"localhost:{admin}",
                      "admin_client_cert": _org(tn, *oo, "orderers", n, "tls", "server.crt"),
                      "admin_client_key": _org(tn, *oo, "orderers", n, "tls", "server.key"),
                      "ops_url": f"http://localhost:{ops}/metrics"}
                     for n, port, admin, ops in ORDERERS],
        "peers": [{"name": n, "msp_id": msp, "address": f"localhost:{port}",
                   "tls_root_cert": _org(tn, "peerOrganizations", dom, "tlsca", f"tlsca.{dom}-cert.pem"),
                   "admin_msp_dir": _org(tn, "peerOrganizations", dom, "users", f"Admin@{dom}", "msp")}
                  for n, msp, dom, port, _ in PEERS],
        "chaincode": {**env["chaincode"], "package_id": package_id},
        "expect": {"consensus": "etcdraft", "consenters": len(ORDERERS), "anchor_peers_required": True,
                   "raft_leader_wait": True},
    }


                                                                            
def caliper_files(env: dict, run_id: str, channel: str, n: int | None, rate: float, warmup_s: int,
                  workload: str, duration_s: int | None = None) -> dict:
                                                                                               
    run_c = posixpath.join(CAL_WS, "runs", run_id)
    org1 = ("peerOrganizations", "org1.example.com")
    args = {"runId": run_id, "contractId": env["chaincode"]["name"], "invokerIdentity": "User1", "valueBytes": 128,
            "mode": workload, "failEvery": 25, "rmwKey": "hot"}
    rate_ctl = {"type": "fixed-rate", "opts": {"tps": rate}}
    module = posixpath.join(run_c, "modules", "e1_create_asset.js")
    rounds = []
    if warmup_s > 0:
        rounds.append({"label": f"wu-{run_id}", "txDuration": warmup_s, "rateControl": rate_ctl,
                       "workload": {"module": module, "arguments": {**args, "mode": "create", "settleSeconds": 0}}})
    measured = {"txDuration": duration_s} if duration_s is not None else {"txNumber": n}
    rounds.append({"label": f"m-{run_id}", **measured, "rateControl": rate_ctl,
                   "workload": {"module": module, "arguments": {**args, "settleSeconds": 10 if warmup_s > 0 else 0}}})
    bench = {"test": {"name": f"e1-{run_id}", "workers": {"number": 1}, "rounds": rounds},
             "monitors": {"transaction": [{"module": posixpath.join(run_c, "modules", "e1_per_tx_recorder.js"),
                                           "options": {"runId": run_id, "outputDir": posixpath.join(run_c, "recorder"),
                                                       "binding": env["protocol"]["binding"],
                                                       "eventSemantics": "gateway_peer_commit_status",
                                                       "caliperVersion": "0.6.0"}}]}}
    ccp = {"name": "e1-org1", "version": "1.0.0", "client": {"organization": "Org1"},
           "organizations": {"Org1": {"mspid": "Org1MSP", "peers": ["peer0.org1.example.com"]}},
           "peers": {"peer0.org1.example.com": {
               "url": "grpcs://peer0.org1.example.com:7051",
               "tlsCACerts": {"path": posixpath.join(CAL_ORGS, *org1, "tlsca", "tlsca.org1.example.com-cert.pem")},
               "grpcOptions": {"ssl-target-name-override": "peer0.org1.example.com",
                               "hostnameOverride": "peer0.org1.example.com"}}}}
    user = posixpath.join(CAL_ORGS, *org1, "users", "User1@org1.example.com", "msp")
    netcfg = {"name": "e1-network", "version": "2.0.0", "caliper": {"blockchain": "fabric"},
              "channels": [{"channelName": channel, "contracts": [{"id": env["chaincode"]["name"]}]}],
              "organizations": [{"mspid": "Org1MSP",
                                 "identities": {"certificates": [{
                                     "name": "User1",
                                     "clientPrivateKey": {"path": posixpath.join(user, "keystore", "priv_sk")},
                                     "clientSignedCert": {"path": posixpath.join(
                                         user, "signcerts", "User1@org1.example.com-cert.pem")}}]},
                                 "connectionProfile": {"path": posixpath.join(run_c, "ccp-org1.json"),
                                                       "discover": True}}]}
    return {"benchmark.yaml": bench, "networkconfig.yaml": netcfg, "ccp-org1.json": ccp}


def run_plan(env: dict, network_path: str, run_id: str, channel: str, cell: dict, warmup_s: int,
             block_pattern: bool) -> dict:
    run_h = posixpath.join(env["caliper_workspace"], "runs", run_id)
    run_c = posixpath.join(CAL_WS, "runs", run_id)
    measured_round = 1 if warmup_s > 0 else 0
    return {
        "run_id": run_id, "channel": channel, "cell": cell, "network": network_path,
        "containers": {"orderers": [o[0] for o in ORDERERS], "peers": [p[0] for p in PEERS], "caliper": CAL_CONTAINER},
        "metrics_targets": [f"orderer{i + 1}:orderer=http://localhost:{o[3]}/metrics" for i, o in enumerate(ORDERERS)]
        + [f"{p[0].split('.')[0]}{p[2].split('.')[0]}:peer=http://localhost:{p[4]}/metrics" for p in PEERS],
        "caliper": {"argv": ["docker", "exec", "-w", CAL_WS, CAL_CONTAINER, "caliper", "launch", "manager",
                             "--caliper-workspace", CAL_WS, "--caliper-benchconfig", f"runs/{run_id}/benchmark.yaml",
                             "--caliper-networkconfig", f"runs/{run_id}/networkconfig.yaml",
                             "--caliper-flow-only-test"],
                    "container": CAL_CONTAINER, "recorder_dir": posixpath.join(run_h, "recorder"),
                    "recorder_dir_in_container": posixpath.join(run_c, "recorder"), "worker": 0,
                    "measured_round": measured_round, "measured_label": f"m-{run_id}",
                    "warmup_round": 0, "warmup_label": f"wu-{run_id}" if warmup_s > 0 else None},
        "reconcile_peer": PEERS[0][0],
        "expect": {**{k: env["protocol"][k] for k in ("event_strategy", "invoke_timeout_s", "binding")},
                   "expect_all_success": True, "expect_block_pattern": block_pattern},
        "clock": {"rounds": 5, "protocol_bound_ms": 1.0, "drift_tolerance_ms": 2.0},
        "sampling": {"interval_s": 1.0, "metrics_family_filter": "^(blockcutter|broadcast|consensus|ledger)_"},
        "log_margin_s": 5,
    }


def render_run(env: dict, network_path: str, run_id: str, B: int, tau: str, P: int, n: int | None, rate: float,
               warmup_s: int, workload: str, out_plan: Path, *, repo_root: Path | None = None,
               ws_root: Path | None = None, block_pattern: bool = True, argv: list[str] | None = None,
               channel: str | None = None, duration_s: int | None = None) -> dict:
                                                                                                         
    if not RUN_ID.match(run_id):
        raise ValueError(f"run id {run_id!r} must match {RUN_ID.pattern}")
    if workload not in ("create", "fail-every", "rmw"):
        raise ValueError(f"workload {workload!r}")
    channel = channel or f"e1-{run_id.lower()}"
    if not re.match(r"^e1-[A-Za-z0-9_.-]{1,40}$", channel):
        raise ValueError(f"channel {channel!r} must be an E1 channel name")
    if (n is None) == (duration_s is None):
        raise ValueError("specify exactly one of n or duration_s")
    cell = {"B": B, "tau": tau, "P": P, "n": n if n is not None else 0,
            "mode": "txDuration" if duration_s is not None else "txNumber", "rate_tps": rate}
    if duration_s is not None:
        cell["duration_s"] = duration_s
    plan = run_plan(env, network_path, run_id, channel, cell, warmup_s, block_pattern)
    import e1_run_orchestrator as ro                                                                           
    ro.Plan(plan)
    repo = repo_root or Path(env["repo"])
    out = (ws_root or Path(env["caliper_workspace"])) / "runs" / run_id
    out.mkdir(parents=True, exist_ok=False)
    (out / "modules").mkdir()
    record = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "run_id": run_id,
              "channel": channel, "cell": cell, "files": {}, "modules": {}}
    for name, doc in caliper_files(env, run_id, channel, n, rate, warmup_s, workload, duration_s).items():
        record["files"][name] = tc.write_json_x(out / name, doc)
    for name, rel in MODULES.items():
        src = repo / rel
        shutil.copyfile(src, out / "modules" / name)
        a, b = tc.sha256_file(src), tc.sha256_file(out / "modules" / name)
        if a != b:
            raise RuntimeError(f"copy of {rel} differs from the repo")
        record["modules"][name] = {"source": rel, "sha256": a}
    record["plan_sha256"] = tc.write_json_x(out_plan, plan)
    tc.write_json_x(out / "render_record.json", record)
    return {"plan": plan, "record": record, "dir": out}


                                                                                                     
def commands(env: dict) -> list[str]:
    tn, repo, ws = env["test_network"], env["repo"], env["caliper_workspace"]
    compose = (f"docker compose -p e1 -f compose/compose-test-net.yaml -f compose/docker/docker-compose-test-net.yaml "
               f"-f {repo}/fabric-testbed/network/compose-e1-raft3.yaml")
    return [
        f"export PATH={env['fabric_bin']}:$PATH FABRIC_CFG_PATH={env['fabric_cfg']} DOCKER_SOCK=/var/run/docker.sock",
        f"cd {tn}",
        "test ! -d organizations/peerOrganizations && test ! -d organizations/ordererOrganizations   # fresh crypto only",
        "cryptogen generate --config=./organizations/cryptogen/crypto-config-org1.yaml --output=organizations",
        "cryptogen generate --config=./organizations/cryptogen/crypto-config-org2.yaml --output=organizations",
        "cryptogen generate --config=./organizations/cryptogen/crypto-config-orderer.yaml --output=organizations",
        f"{compose} config --quiet          # validates the merged compose files; starts nothing",
        f"{compose} up -d                   # E0-0 approval: starts 3 orderers and 2 peers",
        f"mkdir -p {ws}/runs",
        f"docker run -d --name {CAL_CONTAINER} --network fabric_test -v {ws}:{CAL_WS} -v {tn}/organizations:{CAL_ORGS}:ro "
        f"--entrypoint sleep {CALIPER_IMAGE} infinity     # long-lived: the clock checks exec into it",
        f"docker exec -w {CAL_WS} {CAL_CONTAINER} caliper bind --caliper-bind-sut {env['protocol']['binding']}"
        "     # npm download: environment Stage 7c",
        f"python3 -B {repo}/scripts/e1/e1_la2_render.py network --env <env.json> --bootstrap --out <bootstrap.json>",
        f"python3 -B {repo}/scripts/e1/e1_chaincode_deploy.py --network <bootstrap.json> --cc-dir "
        f"{repo}/fabric-testbed/chaincode/e1asset --label {env['chaincode']['name']}_{env['chaincode']['version']} "
        "--out-dir <new dir> --execute",
        f"python3 -B {repo}/scripts/e1/e1_clock_handshake.py identity "
        + " ".join(f"--container {c}" for c in [o[0] for o in ORDERERS] + [p[0] for p in PEERS] + [CAL_CONTAINER])
        + " --out-dir <new dir> --execute",
    ]


                                                                        
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    nw = sub.add_parser("network")
    nw.add_argument("--env", required=True, type=Path)
    grp = nw.add_mutually_exclusive_group(required=True)
    grp.add_argument("--package-id")
    grp.add_argument("--bootstrap", action="store_true", help="placeholder id, for e1_chaincode_deploy.py only")
    nw.add_argument("--out", required=True, type=Path)
    rn = sub.add_parser("run")
    rn.add_argument("--env", required=True, type=Path)
    rn.add_argument("--network", required=True)
    rn.add_argument("--run-id", required=True)
    rn.add_argument("--channel")
    rn.add_argument("--B", required=True, type=int, help="MaxMessageCount (count)")
    rn.add_argument("--tau", required=True, help="BatchTimeout (Go duration)")
    rn.add_argument("--P", required=True, type=int, help="PreferredMaxBytes (bytes)")
    count_or_duration = rn.add_mutually_exclusive_group(required=True)
    count_or_duration.add_argument("--n", type=int)
    count_or_duration.add_argument("--duration-s", type=int)
    rn.add_argument("--rate", required=True, type=float)
    rn.add_argument("--warmup-s", type=int, default=30)
    rn.add_argument("--workload", default="create", choices=["create", "fail-every", "rmw"])
    rn.add_argument("--no-block-pattern", action="store_true")
    rn.add_argument("--out-plan", required=True, type=Path)
    cm = sub.add_parser("commands")
    cm.add_argument("--env", required=True, type=Path)
    args = ap.parse_args(argv)
    cmdline = sys.argv if argv is None else argv
    try:
        env = load_env(args.env)
        if args.cmd == "commands":
            for line in commands(env):
                print(line)
            print("(printed only: nothing executed; each line needs its stage or E0-0 approval)")
            return 0
        if args.cmd == "network":
            import e1_channel_run as cr
            doc = network(env, args.package_id, bootstrap=args.bootstrap)
            cr.Network(doc)                                                            
            if args.out.exists():
                print(f"refusing to overwrite {args.out}")
                return 2
            tc.write_json_x(args.out, doc)
            print(f"network.json -> {args.out}")
            return 0
        if args.out_plan.exists():
            print(f"refusing to overwrite {args.out_plan}")
            return 2
        r = render_run(env, args.network, args.run_id, args.B, args.tau, args.P, args.n, args.rate, args.warmup_s,
                       args.workload, args.out_plan, block_pattern=not args.no_block_pattern, argv=cmdline,
                       channel=args.channel, duration_s=args.duration_s)
        print(f"run files -> {r['dir']}; plan -> {args.out_plan}")
        return 0
    except (OSError, ValueError, KeyError, TypeError) as err:
        print(f"INVALID: {err!r}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
