"""X1 (2026-11-25): closed_loop_v2/ops/verify_v2.py with the configuration menu widened to {2,5,10} and the output root from X1_OUT. verify_v2 was derived from closed_loop/series_ops_v2/verify_run.py with exactly these changes: output root from LPC_OUT, stage path
required, genesis B = the staged initial B, and an extra independent re-derivation of the predictive_live decisions.
Independent post-run verification of one run (cxNN / cmNN) from its preserved evidence. Read-only on Fabric: reads the
files the run wrote and decodes its config blocks with configtxlator. Uses NO project analysis code. Writes NEW files only,
under <OUT>/<run>_verification/. Prints a one-line INDEPENDENT_VERDICT at the end."""
import csv
import datetime as dt
import os
import glob
import hashlib
import json
import math
import re
import subprocess
import sys
from pathlib import Path

RUN = sys.argv[1]
OUT = Path(os.environ.get("X1_OUT", "/home/mailf18/e1/x1-block-20261126"))
RD = OUT / RUN
WS = Path("/home/mailf18/e1/caliper-ws-benchmark/runs") / RUN
STAGE = Path(sys.argv[2])
V = Path(sys.argv[3]) if len(sys.argv) > 3 else OUT / f"{RUN}_verification"   # argv[3] only for the offline self-test
SELFTEST = len(sys.argv) > 3
V.mkdir(exist_ok=False)
CTL = "/home/mailf18/fabric/fabric-samples/bin/configtxlator"
ALLOWED_DIFF = {"/groups/Orderer/values/BatchSize/value/max_message_count", "/groups/Orderer/values/BatchSize/version"}
checks, R = [], {"run": RUN}


def check(name, ok, detail=""):
    checks.append({"check": name, "pass": bool(ok), "detail": detail})


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def iso_ms(s):
    return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def pct(xs, q):  # nearest rank
    xs = sorted(xs)
    return xs[max(0, math.ceil(q / 100 * len(xs)) - 1)] if xs else None


arm = json.loads((STAGE / RUN / "arm.json").read_text())
rr = json.loads((RD / "run_result.json").read_text())
an = json.loads((RD / "analysis.json").read_text())
R["arm_label"], R["channel"] = arm["arm_label"], arm["channel"]
R["runner"] = {"verdict": rr.get("verdict"), "analysis_verdict": rr.get("analysis_verdict"), "step_failure": rr.get("step_failure"),
               "started_utc": rr.get("started_utc"), "finished_utc": rr.get("finished_utc"),
               "analysis_problems": an.get("problems"), "analysis_flags": an.get("flags")}
check("runner verdict exactly VALID", rr.get("verdict") == "VALID", rr.get("verdict"))
check("analysis verdict exactly VALID", an.get("verdict") == "VALID", an.get("verdict"))
check("run channel = staged channel", rr.get("channel") == arm["channel"], rr.get("channel"))
check("stage sha in run_result", rr.get("stage_sha256") == sha(STAGE / "SHA256SUMS.txt"))

# 1. reconciliation (stdlib)
ledger = list(csv.DictReader(open(RD / "ledger" / "ledger" / "ledger_tx.csv", encoding="utf-8")))
endorser = {x["tx_id"]: x for x in ledger if x["header_type"] == "3"}
config = [x for x in ledger if x["header_type"] == "1"]
rec_rows, rec_ok = {}, {}
for r in arm["rounds"]:
    files = [f for f in glob.glob(str(WS / "recorder" / f"*_{r['label']}.csv"))]
    rows = []
    for f in files:
        rows += list(csv.DictReader(open(f, encoding="utf-8")))
    rec_rows[r["index"]] = rows
    for x in rows:
        if x["status"] == "success":
            rec_ok[x["tx_id"]] = x
lifecycle = {}
for p in sorted((RD / "channel" / "cmd").glob("*.stderr")):
    if "_approve_" in p.name or "_commit_" in p.name:
        for tx in re.findall(r"txid \[([0-9a-f]{64})\] committed with status \(VALID\)", p.read_text(errors="replace")):
            lifecycle.setdefault(tx, p.name)
extra = set(endorser) - set(rec_ok)
missing = set(rec_ok) - set(endorser)
app = [endorser[t] for t in rec_ok if t in endorser]
non_success = sum(1 for rows in rec_rows.values() for x in rows if x["status"] != "success")
sched = sum(r["n_tx"] for r in arm["rounds"])
R["reconciliation"] = {"scheduled": sched, "recorder_rows": sum(len(v) for v in rec_rows.values()), "recorder_success": len(rec_ok),
                       "recorder_non_success": non_success, "ledger_endorser": len(endorser), "ledger_config": len(config),
                       "ledger_blocks": max(int(x["block"]) for x in ledger) + 1, "app_on_ledger": len(app),
                       "app_valid": sum(1 for x in app if x["validation_code"] == "0"), "missing_from_ledger": len(missing),
                       "extra_on_ledger": sorted(extra), "lifecycle_from_logs": lifecycle}
check("every scheduled tx recorded as success", len(rec_ok) == sched and non_success == 0, f"{len(rec_ok)}/{sched}, non-success {non_success}")
check("every recorder success VALID on ledger", not missing and all(x["validation_code"] == "0" for x in app), f"missing {len(missing)}")
check("extra ledger tx == 3 lifecycle tx from channel logs", extra == set(lifecycle) and len(lifecycle) == 3,
      f"extra {len(extra)}, lifecycle {len(lifecycle)}")
check("lifecycle tx VALID", all(endorser[t]["validation_code"] == "0" for t in lifecycle if t in endorser))

# 2. config blocks: own decode, deep diff, transitions, limits
cfg_nums = sorted(int(x["block"]) for x in config)
dec = {}
for n in cfg_nums:
    f = sorted((RD / "ledger" / "blocks").glob(f"{arm['channel']}_{n:06d}_a*.block"))[-1]
    js = V / f"config_block_{n:06d}.json"
    subprocess.run([CTL, "proto_decode", "--input", str(f), "--type", "common.Block", "--output", str(js)], check=True)
    cfg = json.loads(js.read_text())["data"]["data"][0]["payload"]["data"]["config"]
    ov = cfg["channel_group"]["groups"]["Orderer"]["values"]
    dec[n] = {"sha256": sha(f), "sequence": cfg.get("sequence"), "B": ov["BatchSize"]["value"]["max_message_count"],
              "batch_timeout": ov["BatchTimeout"]["value"]["timeout"], "pmb": ov["BatchSize"]["value"]["preferred_max_bytes"],
              "amb": ov["BatchSize"]["value"]["absolute_max_bytes"], "_g": cfg["channel_group"]}


def diff(a, b, path=""):
    if isinstance(a, dict) and isinstance(b, dict):
        return [d for k in sorted(set(a) | set(b)) for d in diff(a.get(k), b.get(k), f"{path}/{k}")]
    return [] if a == b else [{"path": path, "before": a, "after": b}]


chain = [{"from": a, "to": b, "diffs": diff(dec[a]["_g"], dec[b]["_g"])} for a, b in zip(cfg_nums, cfg_nums[1:])]
R["config_blocks"] = {str(n): {k: v for k, v in d.items() if k != "_g"} for n, d in dec.items()}
R["config_diffs"] = chain
check("genesis B = staged initial B, BatchTimeout 2s", dec[0]["B"] == arm["initial_B"] and dec[0]["batch_timeout"] == "2s", f"B={dec[0]['B']} {dec[0]['batch_timeout']}")
check("every config diff touches only MaxMessageCount + BatchSize.version",
      all({d["path"] for d in c["diffs"]} <= ALLOWED_DIFF and c["diffs"] for c in chain), json.dumps([[d["path"] for d in c["diffs"]] for c in chain]))
check("BatchTimeout 2s and byte limits unchanged in every config", all(d["batch_timeout"] == "2s" and d["pmb"] == dec[0]["pmb"] and d["amb"] == dec[0]["amb"] for d in dec.values()))
check("every config B in menu {2,5,10}", all(d["B"] in (2, 5, 10) for d in dec.values()))
trans = rr["control"]["transitions"]
applied = [t for t in trans if t.get("success")]
check("applied transitions == later config blocks", [t.get("ledger_config_block") for t in applied] == cfg_nums[1:] and
      [t["to_B"] for t in applied] == [dec[n]["B"] for n in cfg_nums[1:]],
      f"transitions {[(t.get('ledger_config_block'), t['to_B']) for t in applied]} blocks {[(n, dec[n]['B']) for n in cfg_nums[1:]]}")
check("no failed transition attempts", len(applied) == len(trans), f"{len(trans)} attempts")
check("broadcasts <= 10", len(trans) <= 10)
final_B = dec[cfg_nums[-1]]["B"]
check("final ledger B == runner final config == controller final", final_B == rr["final_config"]["max_message_count"],
      f"ledger {final_B} runner {rr['final_config'].get('max_message_count')}")
fill = {}
for x in endorser.values():
    fill[int(x["block"])] = fill.get(int(x["block"]), 0) + 1
over = [(b, c) for b, c in fill.items() if c > dec[max(n for n in cfg_nums if n < b)]["B"]]
check("no block exceeds the MaxMessageCount in force", not over, str(over[:5]))

# 3. decisions, controller identity, actuations + native verifier
decs = rr["control"]["decisions"]
R["decisions"] = [{k: d.get(k) for k in ("decision_index", "telemetry_round", "current_B", "target_B", "changed", "applied", "abstained", "reason",
                                         "arm", "telemetry_age_s", "applies_to_round")} for d in decs]
check("one decision per staged decision time, all by the staged arm", len(decs) == len(arm["decision_times_utc"]) and all(d["arm"] == arm["arm"] for d in decs), f"{len(decs)}")
check("every decision target in menu", all(d["target_B"] in (2, 5, 10) for d in decs))
if arm["arm"] == "static":
    check("static never changes", not any(d["changed"] for d in decs))
if arm["arm"] == "predictive":
    rows = arm["predictive"]["rows"]
    ok = all((d["target_B"] == rows[d["decision_index"]]["predictive_B_stored"]) if d["current_B"] == rows[d["decision_index"]]["current_B_before"]
             else (not d["changed"]) for d in decs)
    check("predictive decisions == stored D-E2-10 rows (or precondition hold)", ok)
if arm["arm"] == "reactive":
    p = arm["reactive"]
    obs = {o["round_index"]: o for o in rr["control"]["observations"]}
    # independent replay of the hysteresis rule on the runner's observed round means
    cur, hi, lo, k = 10, p["latency_high_ms"], p["latency_low_ms"], int(p["consecutive_samples"])
    run_hi = run_lo = 0
    expect = []
    for d in decs:
        m = obs[d["telemetry_round"]]["latency_mean_ms"]
        if cur == 10:
            run_hi = run_hi + 1 if m >= hi else 0
            tgt = p["congested_B"] if run_hi >= k else 10
        else:
            run_lo = run_lo + 1 if m <= lo else 0
            tgt = 10 if run_lo >= k else cur
        expect.append(tgt)
        if tgt != cur:
            cur, run_hi, run_lo = tgt, 0, 0
    R["reactive_replay_expected_targets"] = expect
    check("reactive decisions == independent hysteresis replay", [d["target_B"] for d in decs] == expect,
          f"runner {[d['target_B'] for d in decs]} replay {expect}")
if arm["arm"] == "predictive_live":
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from Fabric_Live_Testbed.closed_loop_v2.independent import check_predictive_live
    for c_ in check_predictive_live(arm, decs, WS):
        check("predictive_live: " + c_["check"], c_["pass"], c_["detail"])
acts = []
for u in sorted(glob.glob(str(RD / "actuator" / "update-*" / "update_record.json"))):
    rec, d = json.loads(Path(u).read_text()), Path(u).parent
    nv = {}
    for lab in ("07_inspect_config_update_before_signing", "09_inspect_config_update_after_signing", "10_fabric_native_envelope_verification"):
        pth = d / f"{lab}.stdout"
        if pth.exists():
            j = json.loads(pth.read_text())
            nv[lab] = {k: j.get(k) for k in ("valid", "fabric_source_commit", "config_update_sha256", "target_max_message_count")}
    acts.append({"attempt": d.name, "success": rec.get("success"), "before": rec.get("before_config", {}).get("max_message_count"),
                 "after": (rec.get("after_config") or {}).get("max_message_count"), "after_block": rec.get("after_block_number"),
                 "commit_latency_ms": rec.get("commit_latency_ms"), "total_adaptation_latency_ms": rec.get("total_adaptation_latency_ms"),
                 "native_verifier": nv})
R["actuations"] = acts
check("each applied update: native verifier valid at 3 stages, commit f871cf92, one ConfigUpdate sha",
      len(acts) == len(applied) and all(len(a["native_verifier"]) == 3 and all(v["valid"] is True and str(v["fabric_source_commit"]).startswith("f871cf92")
                                                                             for v in a["native_verifier"].values())
                                       and len({v["config_update_sha256"] for v in a["native_verifier"].values()}) == 1 for a in acts),
      f"{len(acts)} actuator records")

# 4. per-round metrics recomputed from the recorder + manifests, compared with the official analysis
rounds = []
man = {}
for r in arm["rounds"]:
    m = json.loads(Path(glob.glob(str(WS / "recorder" / f"*_{r['label']}.manifest.json"))[0]).read_text())
    man[r["index"]] = m["clock"]
    ok_rows = [x for x in rec_rows[r["index"]] if x["status"] == "success"]
    lat = [int(x["final_ms"]) - int(x["create_ms"]) for x in ok_rows]
    wall = (m["clock"]["wall1_ms"] - m["clock"]["wall0_ms"]) / 1000
    blocks = {endorser[x["tx_id"]]["block"] for x in ok_rows if x["tx_id"] in endorser}
    regime_B = sorted({dec[max(n for n in cfg_nums if n < int(b))]["B"] for b in blocks})
    rounds.append({"round": r["index"], "label": r["label"], "measured": r["measured"], "n": len(lat), "wall_s": wall,
                   "offered_tps": r["n_tx"] / (r["length_ms"] / 1000), "throughput_tps": len(lat) / wall if wall else None,
                   "lat_mean": sum(lat) / len(lat) if lat else None, "lat_p50": pct(lat, 50), "lat_p95": pct(lat, 95), "lat_p99": pct(lat, 99),
                   "lat_max": max(lat) if lat else None, "committed_under_B": regime_B})
R["rounds_independent"] = rounds
off = []
for mine, theirs in zip(rounds, an["rounds"]):
    if abs(mine["lat_mean"] - theirs["latency_ms"]["mean"]) > 0.01 or mine["lat_p95"] != theirs["latency_ms"]["p95"] or mine["n"] != theirs["success"]:
        off.append(mine["round"])
check("per-round n/mean/p95 equal the official analysis", not off, f"differs at rounds {off}")
check("each measured round committed under exactly one B", all(len(r["committed_under_B"]) == 1 for r in rounds if r["measured"]),
      json.dumps({r["round"]: r["committed_under_B"] for r in rounds}))
meas = [r for r in rounds if r["measured"]]
alll = [int(x["final_ms"]) - int(x["create_ms"]) for r in arm["rounds"] if r["measured"] for x in rec_rows[r["index"]] if x["status"] == "success"]
R["run_level_measured"] = {"n": len(alll), "offered_tps": sum(r["n"] for r in meas) / sum(r["length_ms"] / 1000 for r in arm["rounds"] if r["measured"]),
                           "throughput_tps": len(alll) / sum(r["wall_s"] for r in meas), "lat_mean": sum(alll) / len(alll),
                           "lat_p50": pct(alll, 50), "lat_p95": pct(alll, 95), "lat_p99": pct(alll, 99), "failed": non_success,
                           "B_by_round": [r["committed_under_B"] for r in meas]}
gaps = [man[i + 1]["wall0_ms"] - man[i]["wall1_ms"] for i in range(len(arm["rounds"]) - 1)]
R["inter_round_gaps_ms"] = gaps

# 5. overlap of each applied update with the round it precedes
ov = []
tx_block = {t: int(x["block"]) for t, x in endorser.items()}
for t in applied:
    nxt = t["decision_index"] + 1
    sub0 = iso_ms(t["submission_start_timestamp_utc"])
    ver = iso_ms(t["independent_verification_timestamp_utc"])
    ok_rows = [x for x in rec_rows[nxt] if x["status"] == "success"]
    ov.append({"decision": t["decision_index"], "to_B": t["to_B"], "config_block": t["ledger_config_block"],
               "round_start_minus_verified_ms": man[nxt]["wall0_ms"] - ver, "actuator_ms": t["duration_ms"],
               "round_tx_committed_before_cfg": sum(1 for x in ok_rows if tx_block.get(x["tx_id"], 10 ** 9) < t["ledger_config_block"]),
               "round_tx_created_before_verified": sum(1 for x in ok_rows if int(x["create_ms"]) < ver)})
R["overlap"] = ov
check("no tx of a following round committed under the previous config", all(o["round_tx_committed_before_cfg"] == 0 for o in ov))

# 6. Fabric logs
cats = {}
for f in glob.glob(str(OUT / f"{RUN}_fabric_logs" / "*.log")):
    for line in open(f, encoding="utf-8", errors="replace"):
        if re.search(r" (ERRO|PANI)", line):
            cats.setdefault(Path(f).stem, 0)
            cats[Path(f).stem] += 1
R["fabric_log_erro_pani_lines"] = cats
check("container logs preserved", len(glob.glob(str(OUT / f"{RUN}_fabric_logs" / "*.log"))) == 6)
try:
    st = (OUT / f"{RUN}_fabric_logs" / ("containers_after_cs01.txt" if RUN == "cs01" else "containers_after.txt")).read_text()
    check("no container restarted", st.count("restarts=0") == 6 and st.count("running=true") == 6, st.strip().replace("\n", " | "))
except FileNotFoundError:
    check("no container restarted", False, "containers_after.txt missing")

R["checks"] = checks
R["independent_pass"] = all(c["pass"] for c in checks)
(V / "independent_verification.json").write_text(json.dumps(R, indent=2, sort_keys=True, default=str))
# evidence checksums for this run
lines = []
for root in (RD, WS, OUT / f"{RUN}_fabric_logs", V):
    for p in sorted(q for q in root.rglob("*") if q.is_file()):
        lines.append(f"{sha(p)}  {p}")
for p in sorted(OUT.glob(f"{RUN}_*")):
    if p.is_file() and not p.name.endswith("EVIDENCE_SHA256SUMS.txt"):
        lines.append(f"{sha(p)}  {p}")
sums = (V / "SELFTEST_SUMS.txt") if SELFTEST else OUT / f"{RUN}_EVIDENCE_SHA256SUMS.txt"
sums.write_text("\n".join(lines) + "\n")
for c in checks:
    print(("PASS " if c["pass"] else "FAIL ") + c["check"] + (f"  [{c['detail']}]" if c["detail"] and not c["pass"] else ""))
rl = R["run_level_measured"]
print("RUN", RUN, arm["arm_label"], "| B by round", [b[0] if len(b) == 1 else b for b in rl["B_by_round"]], "| transitions",
      [(t["decision_index"], t["from_B"], t["to_B"], t["ledger_config_block"]) for t in applied])
print("MEASURED n=%d offered %.2f thr %.3f tx/s mean %.1f p50 %s p95 %s p99 %s failed %d" % (
    rl["n"], rl["offered_tps"], rl["throughput_tps"], rl["lat_mean"], rl["lat_p50"], rl["lat_p95"], rl["lat_p99"], rl["failed"]))
print("ROUNDS", [(r["round"], r["committed_under_B"], round(r["throughput_tps"], 2), round(r["lat_mean"], 1), r["lat_p95"]) for r in rounds])
print("GAPS ms", gaps, "| overlap", ov)
print("EVIDENCE", sums, len(lines), "files, list sha256", sha(sums))
print("INDEPENDENT_VERDICT", "PASS" if R["independent_pass"] else "FAIL", "| runner", rr.get("verdict"), "| analysis", an.get("verdict"))
