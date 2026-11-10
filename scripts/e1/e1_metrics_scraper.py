                                                                                                        

                                                                                               
                                                                                             
                                                                                       

                                                                                                   
                                                                                              
                                                                     
                                                                                                 
                                                                                                  
                                                        

                                                                                      
                               
                                                                                                
                                                                                                   
                                                                                       
                                                                                              
                                                                                     
                                              
                                                                           

                                                         
                                                                                    
                                                                                
                                                                                           
                                                                                                         
                                                                                                          
                                                                                                           
                                                                                                       
                                                                               
                                                                                                    
                                                                                                 
                                                                                                       
                                                                                                   

      
                                                                                                 
                                                                                   
                                                                                                    
                                                                                                     
                                                        
                                                                                             
                                                                                            
                                                                                           
                                                                                               
                                                                                           
                                                                                            
                                                                                                     
                                                                                                     
                                                                                                    

                                                                                                  
                                                                     
                      
   
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_telemetry_common as tc              

TOOL_VERSION = "e1_metrics_scraper 0.2 (2026-11-09, E0-0 remediation F3a)"
                                                                                                      
                                                                              
DEFAULT_EXPECTED = {
    "orderer": ["consensus_etcdraft_is_leader", "consensus_etcdraft_leader_changes",
                "consensus_etcdraft_committed_block_number"],
    "peer": ["ledger_blockchain_height", "ledger_block_processing_time", "ledger_blockstorage_commit_time",
             "ledger_statedb_commit_time"],
    "other": [],
}
                                                                                                          
                                                                                                
DEFAULT_EXPECTED_ANY = {
    "orderer": ["blockcutter_block_fill_duration", "broadcast_processed_count"],
    "peer": [],
    "other": [],
}
COMPLETENESS = {"scrape_failed", "http_status", "decode_error", "empty_scrape", "malformed_line",
                "expected_metric_absent", "no_successful_scrape", "delta_unavailable",
                "broadcast_receiver_unidentified", "block_cutter_unidentified"}
CONSISTENCY = {"duplicate_series", "counter_reset", "type_conflict", "host_clock_step"}
WARNINGS = {"interval_overrun", "expected_metric_absent_early", "unknown_type", "exposition_timestamp",
            "cutter_not_leader", "broadcast_not_success"}
HOST_STEP_NS = 2_000_000
SUFFIXES = ("_bucket", "_sum", "_count")
MONOTONE_TYPES = {"counter", "histogram", "summary"}
_NAME = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
_LABEL = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
_VALUE = re.compile(r"^[+-]?(?:(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?|[Ii]nf|NaN|nan)$")
_TS = re.compile(r"^-?\d+$")
_TARGET = re.compile(r"^(?P<name>[A-Za-z0-9][A-Za-z0-9_.-]*):(?P<role>orderer|peer|other)=(?P<url>https?://\S+)$")
SCRAPE_COLS = ["round", "phase", "target", "role", "url", "t0_wall_ns", "t1_wall_ns", "t0_utc", "t_mid_wall_ns",
               "t0_mono_ns", "t1_mono_ns", "duration_ms", "http_status", "bytes", "sha256", "raw_file", "n_samples",
               "n_malformed", "n_duplicates", "error"]
SAMPLE_COLS = ["round", "phase", "target", "t_mid_wall_ns", "metric", "family", "type", "labels", "value_raw", "value",
               "line_no", "duplicate"]
DELTA_COLS = ["target", "role", "metric", "family", "type", "labels", "before", "after", "delta", "before_source",
              "reset_seen", "status"]


                                                                                       
def _labels(line: str, i: int) -> tuple[tuple, int]:
                                                                                                
    pairs, seen = [], set()
    i += 1
    while True:
        while i < len(line) and line[i] in " \t":
            i += 1
        if i < len(line) and line[i] == "}":
            return tuple(sorted(pairs)), i + 1
        m = _LABEL.match(line, i)
        if not m:
            raise ValueError("bad label name")
        key, i = m.group(), m.end()
        if key in seen:
            raise ValueError(f"label {key!r} repeated")
        seen.add(key)
        if line[i:i + 2] != '="':
            raise ValueError("expected =\" after a label name")
        i += 2
        val = []
        while True:
            if i >= len(line):
                raise ValueError("unterminated label value")
            c = line[i]
            if c == "\\":
                nxt = line[i + 1:i + 2]
                if nxt not in ("\\", '"', "n"):
                    raise ValueError("bad escape in label value")
                val.append("\n" if nxt == "n" else nxt)
                i += 2
            elif c == '"':
                i += 1
                break
            else:
                val.append(c)
                i += 1
        pairs.append((key, "".join(val)))
        if i < len(line) and line[i] == ",":
            i += 1
        elif not (i < len(line) and line[i] == "}"):
            raise ValueError("expected , or } after a label value")


def parse_sample(line: str) -> tuple[str, tuple, str, float, int | None]:
    m = _NAME.match(line)
    if not m:
        raise ValueError("no metric name")
    name, i = m.group(), m.end()
    labels: tuple = ()
    if i < len(line) and line[i] == "{":
        labels, i = _labels(line, i)
    if i >= len(line) or line[i] not in " \t":
        raise ValueError("no whitespace before the value")
    parts = line[i:].split()
    if len(parts) not in (1, 2) or not _VALUE.match(parts[0]):
        raise ValueError("value missing or not a Prometheus float")
    ts = None
    if len(parts) == 2:
        if not _TS.match(parts[1]):
            raise ValueError("timestamp is not an integer")
        ts = int(parts[1])
    return name, labels, parts[0], float(parts[0]), ts


def family_of(name: str, types: dict) -> str:
    if name in types:
        return name
    for s in SUFFIXES:
        if name.endswith(s) and name[: -len(s)] in types:
            return name[: -len(s)]
    return name


def _esc(v: str) -> str:
    return v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def labels_text(labels: tuple) -> str:
                                                                                   
    return ",".join(f'{k}="{_esc(v)}"' for k, v in labels)


def has_label(text: str, key: str, value: str) -> bool:
    return re.search(rf'(?:^|,){re.escape(key)}="{re.escape(_esc(value))}"(?:,|$)', text) is not None


def parse_exposition(text: str) -> tuple[list[dict], dict, list[tuple[str, int, str, str]]]:
                                                                                                    
                                                              
    samples, types, problems, seen = [], {}, [], set()
    for no, raw in enumerate(text.split("\n"), start=1):
        line = raw.rstrip("\r")
        if not line.strip():
            continue
        if line.startswith("#"):
            parts = line.split(None, 3)
            if len(parts) >= 2 and parts[1] == "TYPE":
                if len(parts) != 4 or not _NAME.fullmatch(parts[2]):
                    problems.append(("malformed_line", no, "bad # TYPE line", raw))
                    continue
                fam, typ = parts[2], parts[3].strip()
                if typ not in ("counter", "gauge", "histogram", "summary", "untyped"):
                    problems.append(("unknown_type", no, f"type {typ!r}", raw))
                if fam in types and types[fam] != typ:
                    problems.append(("type_conflict", no, f"{fam} declared {types[fam]} and {typ}", raw))
                types.setdefault(fam, typ)
            continue
        try:
            name, labels, vraw, value, ts = parse_sample(line)
        except ValueError as err:
            problems.append(("malformed_line", no, str(err), raw))
            continue
        if ts is not None:
            problems.append(("exposition_timestamp", no, f"sample timestamp {ts} ignored", raw))
        key = (name, labels)
        dup = key in seen
        seen.add(key)
        if dup:
            problems.append(("duplicate_series", no, f"{name}{{{labels_text(labels)}}} repeated", raw))
        samples.append({"metric": name, "labels": labels, "value_raw": vraw, "value": value, "line_no": no,
                        "duplicate": dup})
    for s in samples:
        s["family"] = family_of(s["metric"], types)
        s["type"] = types.get(s["family"], "untyped")
    return samples, types, problems


def _monotone(sample: dict) -> bool:
    if sample["type"] == "counter":
        return True
    return sample["type"] in ("histogram", "summary") and sample["metric"].endswith(SUFFIXES)


                                                                             
Fetch = Callable[[str, float], "tuple[int, bytes]"]


def http_fetch(url: str, timeout: float) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"Accept": "text/plain;version=0.0.4"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read() or b""


def parse_targets(values: list[str]) -> list[dict]:
    out = []
    for v in values:
        m = _TARGET.match(v)
        if not m:
            raise ValueError(f"--target wants NAME:ROLE=URL with ROLE orderer|peer|other, got {v!r}")
        out.append(m.groupdict())
    if len({t["name"] for t in out}) != len(out):
        raise ValueError("duplicate target names")
    if not out:
        raise ValueError("no targets")
    return out


def run_scrape(targets: list[dict], phase: str, out_dir: Path, *, fetch: Fetch = http_fetch,
               wall_ns: Callable[[], int] = time.time_ns, mono_ns: Callable[[], int] = time.monotonic_ns,
               sleep: Callable[[float], None] = time.sleep, interval_s: float = 1.0, duration_s: float | None = None,
               max_rounds: int | None = None, stop: Callable[[], bool] | None = None, timeout_s: float = 2.0,
               expected: dict | None = None, expected_any: dict | None = None, family_filter: str | None = None,
               argv: list[str] | None = None) -> dict:
    if phase not in ("before", "during", "after"):
        raise ValueError(f"phase {phase!r}")
    if phase == "during" and duration_s is None and max_rounds is None and stop is None:
        raise ValueError("phase during needs --duration, --max-rounds or --stop-file")
    if interval_s <= 0:
        raise ValueError("interval must be > 0")
    expected = DEFAULT_EXPECTED if expected is None else expected
    expected_any = DEFAULT_EXPECTED_ANY if expected_any is None else expected_any
    role_of = {t["name"]: t["role"] for t in targets}
    any_holders: dict[str, set[str]] = {f"{r}:{f}": set() for r, fams in expected_any.items() for f in fams}
    any_skipped = sorted(r for r, fams in expected_any.items() if fams and r not in role_of.values())
    keep = re.compile(family_filter) if family_filter else None
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "raw").mkdir()
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    scrape_rows, sample_rows = [], []
    last_values: dict[tuple, tuple[float, int]] = {}
    ok_count = {t["name"]: 0 for t in targets}
    filtered = 0
    single = phase != "during"
    start_m, start_w = mono_ns(), wall_ns()
    rounds_done, missed, k = 0, 0, 0
    while True:
        if single and rounds_done == 1:
            break
        if not single:
            if (max_rounds is not None and rounds_done >= max_rounds) or (stop is not None and stop()) \
                    or (duration_s is not None and mono_ns() - start_m >= duration_s * 1e9):
                break
        round_fams: dict[str, set[str]] = {}
        for t in targets:
            name, role = t["name"], t["role"]
            rel = f"raw/{phase}_{k:06d}_{name}.prom"
            t0w, t0m = wall_ns(), mono_ns()
            status, body, error = None, b"", ""
            try:
                status, body = fetch(t["url"], timeout_s)
            except Exception as exc:                                                                     
                error = f"{type(exc).__name__}: {exc}"
            t1w, t1m = wall_ns(), mono_ns()
            mid = (t0w + t1w) // 2
            src = f"{phase} round {k} {name}"
            row = {"round": k, "phase": phase, "target": name, "role": role, "url": t["url"], "t0_wall_ns": t0w,
                   "t1_wall_ns": t1w, "t0_utc": tc.utc_iso(t0w), "t_mid_wall_ns": mid, "t0_mono_ns": t0m,
                   "t1_mono_ns": t1m, "duration_ms": round((t1m - t0m) / 1e6, 3), "http_status": status,
                   "bytes": len(body), "error": error, "n_samples": 0, "n_malformed": 0, "n_duplicates": 0}
            if abs((t1w - t0w) - (t1m - t0m)) > HOST_STEP_NS:
                an.add("host_clock_step", name, f"wall {t1w - t0w} ns vs monotonic {t1m - t0m} ns", src)
            if error:
                an.add("scrape_failed", name, error, src)
            else:
                row["sha256"], row["raw_file"] = tc.write_bytes_x(out_dir / rel, body), rel
                if status != 200:
                    an.add("http_status", name, f"HTTP {status}", src)
            if not error and status == 200:
                try:
                    text = body.decode("utf-8")
                except UnicodeDecodeError as err:
                    an.add("decode_error", name, str(err), src)
                    text = None
                if text is not None and not text.strip():
                    an.add("empty_scrape", name, "no content", src)
                elif text is not None:
                    samples, _, problems = parse_exposition(text)
                    for kind, no, detail, raw in problems:
                        an.add(kind, name, detail, f"{rel}:{no}", raw)
                    row["n_malformed"] = sum(1 for p in problems if p[0] == "malformed_line")
                    row["n_duplicates"] = sum(1 for s in samples if s["duplicate"])
                    row["n_samples"] = len(samples)
                    ok_count[name] += 1
                    fams = {s["family"] for s in samples}
                    round_fams[name] = fams
                    for fam in expected.get(role, []):
                        if fam not in fams:
                            an.add("expected_metric_absent" if phase == "after"
                                   else "expected_metric_absent_early", name, fam, src)
                    for s in samples:
                        key = (name, s["metric"], s["labels"])
                        if not s["duplicate"] and _monotone(s):
                            prev = last_values.get(key)
                            if prev is not None and s["value"] < prev[0]:
                                an.add("counter_reset", name, f"{s['metric']}{{{labels_text(s['labels'])}}} "
                                                              f"{prev[0]} (round {prev[1]}) -> {s['value']}", src)
                            last_values[key] = (s["value"], k)
                        if keep is not None and not keep.search(s["family"]):
                            filtered += 1
                            continue
                        sample_rows.append({"round": k, "phase": phase, "target": name, "t_mid_wall_ns": mid,
                                            "metric": s["metric"], "family": s["family"], "type": s["type"],
                                            "labels": labels_text(s["labels"]), "value_raw": s["value_raw"],
                                            "value": s["value"], "line_no": s["line_no"], "duplicate": s["duplicate"]})
            scrape_rows.append(row)
                                                                                                          
                                                                                                               
        for role, fams in expected_any.items():
            scraped = [n for n in round_fams if role_of[n] == role]
            if not scraped:
                continue
            for fam in fams:
                holders = [n for n in scraped if fam in round_fams[n]]
                any_holders[f"{role}:{fam}"].update(holders)
                if not holders:
                    an.add("expected_metric_absent" if phase == "after" else "expected_metric_absent_early",
                           f"any {role}", f"{fam} on none of {sorted(scraped)}", f"{phase} round {k}")
        rounds_done += 1
        if single:
            continue
        due = start_m + int((k + 1) * interval_s * 1e9)
        now = mono_ns()
        if now > due:
            behind = int((now - due) // (interval_s * 1e9)) + 1
            an.add("interval_overrun", "", f"round {k} ended {(now - due) / 1e6:.3f} ms late; "
                                           f"{behind - 1} round slot(s) skipped, not back-filled", f"round {k}")
            missed += behind - 1
            k += behind
        else:
            sleep((due - now) / 1e9)
            k += 1
    for name, n in ok_count.items():
        if n == 0:
            an.add("no_successful_scrape", name, f"no successful scrape in phase {phase}")
    end_w, end_m = wall_ns(), mono_ns()
    if abs((end_w - start_w) - (end_m - start_m)) > HOST_STEP_NS:
        an.add("host_clock_step", "", f"over the phase: wall {end_w - start_w} ns vs monotonic {end_m - start_m} ns")
    outputs = {"scrapes.csv": tc.write_csv_x(out_dir / "scrapes.csv", SCRAPE_COLS, scrape_rows),
               "samples.csv": tc.write_csv_x(out_dir / "samples.csv", SAMPLE_COLS, sample_rows),
               "metrics_anomalies.csv": tc.write_csv_x(out_dir / "metrics_anomalies.csv", tc.ANOMALY_COLS, an.rows())}
    manifest = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "phase": phase,
                "targets": targets, "parameters": {"interval_s": interval_s, "duration_s": duration_s,
                                                   "max_rounds": max_rounds, "timeout_s": timeout_s,
                                                   "family_filter": family_filter, "expected": expected,
                                                   "expected_any": expected_any},
                "expected_any_holders": {k2: sorted(v) for k2, v in any_holders.items()},
                "expected_any_skipped_roles": any_skipped,
                "start_utc": tc.utc_iso(start_w), "end_utc": tc.utc_iso(end_w),
                "rounds": rounds_done, "missed_round_slots": missed, "scrapes": len(scrape_rows),
                "successful_scrapes": ok_count, "samples_written": len(sample_rows),
                "samples_filtered_out": filtered,
                "filter_note": "raw/*.prom keep every sample; samples.csv keeps the families matching family_filter",
                "timestamp_note": "host wall clock and monotonic clock; align with containers via e1_clock_handshake",
                "status": an.status(), "anomalies": an.counts(), "outputs_sha256": outputs}
    tc.write_json_x(out_dir / "metrics_manifest.json", manifest)
    return manifest


                                                                              
def traffic_roles(out_rows: list[dict], channel: str, leaders_after: list[str]) -> dict:
                                                                                                           
                                                                                                      
                                                                                                    
    receivers: dict[str, float] = {}
    by_status: dict[str, dict[str, float]] = {}
    cutters: dict[str, float] = {}
    for r in out_rows:
        if r.get("status") != "ok" or r.get("role") != "orderer" or not has_label(r["labels"], "channel", channel):
            continue
        if r["metric"] == "broadcast_processed_count" and r["delta"] > 0:
            st = re.search(r'(?:^|,)status="([^"]*)"', r["labels"])
            status = st.group(1) if st else ""
            by_status.setdefault(r["target"], {})
            by_status[r["target"]][status] = by_status[r["target"]].get(status, 0.0) + r["delta"]
            if status == "SUCCESS" and has_label(r["labels"], "type", "ENDORSER_TRANSACTION"):
                receivers[r["target"]] = receivers.get(r["target"], 0.0) + r["delta"]
        if r["metric"] == "blockcutter_block_fill_duration_count" and r["delta"] > 0:
            cutters[r["target"]] = cutters.get(r["target"], 0.0) + r["delta"]
    return {"broadcast_receivers": receivers, "broadcast_by_target_status": by_status, "block_cutters": cutters,
            "leaders_after": sorted(leaders_after)}


def summarize(before: Path, after: Path, out_dir: Path, *, during: Path | None = None, channel: str | None = None,
              fresh_channel: bool = False, expect_traffic: bool = False, argv: list[str] | None = None) -> dict:
                                                                                                        
                                                                                                     
                                                                                                     
                                                                                                          
                                                                                                     
    if expect_traffic and channel is None:
        raise ValueError("expect_traffic needs a channel")
    an = tc.Anomalies(COMPLETENESS, CONSISTENCY, WARNINGS)
    phases = {"before": before, "after": after}
    if during is not None:
        phases["during"] = during
    manifests, rows_by_phase = {}, {}
    for ph, d in phases.items():
        manifests[ph] = json.loads((d / "metrics_manifest.json").read_text(encoding="utf-8"))
        rows_by_phase[ph] = [r for r in tc.read_csv(d / "samples.csv") if r["duplicate"] == "false"]
    roles = {t["name"]: t["role"] for ph in manifests for t in manifests[ph]["targets"]}

    def last_obs(rows: list[dict]) -> dict:
        out: dict[tuple, dict] = {}
        for r in sorted(rows, key=lambda r: int(r["t_mid_wall_ns"])):
            out[(r["target"], r["metric"], r["labels"])] = r
        return out

    def on_ch(labels: str) -> bool:
        return channel is not None and has_label(labels, "channel", channel)

    b_vals, a_vals = last_obs(rows_by_phase["before"]), last_obs(rows_by_phase["after"])
    series_obs: dict[tuple, list[tuple[int, float]]] = {}
    for ph in ("before", "during", "after"):
        for r in rows_by_phase.get(ph, []):
            series_obs.setdefault((r["target"], r["metric"], r["labels"]), []).append(
                (int(r["t_mid_wall_ns"]), float(r["value"])))
    out_rows = []
    for key in sorted(set(b_vals) | set(a_vals)):
        ref = a_vals.get(key) or b_vals[key]
        if not _monotone({"type": ref["type"], "metric": ref["metric"]}):
            continue
        target, metric, labels = key
        obs = sorted(series_obs.get(key, []))
        reset = any(y < x for (_, x), (_, y) in zip(obs, obs[1:]))
        row = {"target": target, "role": roles.get(target, ""), "metric": metric, "family": ref["family"],
               "type": ref["type"], "labels": labels, "reset_seen": reset}
        on_channel = on_ch(labels)
        if key in a_vals and key in b_vals:
            row.update(before=float(b_vals[key]["value"]), before_source="observed")
        elif key in a_vals and fresh_channel and on_channel:
            row.update(before=0.0, before_source="assumed_zero")
        else:
            row.update(status="unavailable", before=b_vals[key]["value"] if key in b_vals else "",
                       after=a_vals[key]["value"] if key in a_vals else "",
                       before_source="observed" if key in b_vals else "absent")
            if channel is None or on_channel:
                an.add("delta_unavailable", target, f"{metric}{{{labels}}} absent "
                                                    f"{'after' if key in b_vals else 'before'} the run")
            out_rows.append(row)
            continue
        row["after"] = float(a_vals[key]["value"])
        if reset:
            an.add("counter_reset", target, f"{metric}{{{labels}}} decreased between scrapes")
            row.update(delta="", status="reset")
        else:
            row.update(delta=row["after"] - row["before"], status="ok")
        out_rows.append(row)

    derived: dict[str, dict] = {}
    for r in out_rows:
        if channel is not None and not on_ch(r["labels"]):
            continue
        d = derived.setdefault(r["target"], {"role": r["role"]})
        if r["status"] != "ok":
            continue
        if r["metric"] == "consensus_etcdraft_leader_changes":
            d["leader_changes_delta"] = r["delta"]
        if r["metric"] == "blockcutter_block_fill_duration_sum":
            d["fill_duration_sum_delta_s"] = r["delta"]
        if r["metric"] == "blockcutter_block_fill_duration_count":
            d["fill_duration_count_delta"] = r["delta"]
    for d in derived.values():
        if d.get("fill_duration_count_delta"):
            d["fill_duration_mean_s"] = d["fill_duration_sum_delta_s"] / d["fill_duration_count_delta"]
    for r in rows_by_phase["after"]:
        if r["metric"] == "consensus_etcdraft_is_leader" and (channel is None or on_ch(r["labels"])):
            derived.setdefault(r["target"], {"role": roles.get(r["target"], "")})["is_leader_after"] = float(r["value"])
    traffic = None
    if channel is not None:
        leaders = [t for t, d in derived.items() if d.get("role") == "orderer" and d.get("is_leader_after") == 1.0]
        traffic = traffic_roles(out_rows, channel, leaders)
        for t, n in traffic["broadcast_receivers"].items():
            derived.setdefault(t, {"role": roles.get(t, "")})["broadcast_success_delta"] = n
        for t, n in traffic["block_cutters"].items():
            derived.setdefault(t, {"role": roles.get(t, "")})["blocks_cut_delta"] = n
        for t, sts in traffic["broadcast_by_target_status"].items():
            for status, n in sts.items():
                if status != "SUCCESS":
                    an.add("broadcast_not_success", t, f"broadcast_processed_count status={status} delta {n} "
                                                       f"on {channel}")
        if expect_traffic:
            if not traffic["broadcast_receivers"]:
                an.add("broadcast_receiver_unidentified", "any orderer",
                       f"no orderer shows an ENDORSER_TRANSACTION SUCCESS broadcast delta on {channel}")
            if not traffic["block_cutters"]:
                an.add("block_cutter_unidentified", "any orderer",
                       f"no orderer shows a blockcutter_block_fill_duration_count delta on {channel}")
        if traffic["block_cutters"] and traffic["leaders_after"] and \
                set(traffic["block_cutters"]) != set(traffic["leaders_after"]):
            an.add("cutter_not_leader", "any orderer", f"cutters {sorted(traffic['block_cutters'])} vs leaders "
                                                       f"after {traffic['leaders_after']}")
    inputs_status = {ph: m["status"] for ph, m in manifests.items()}
    status = tc.worst([an.status(), *inputs_status.values()])
    out_dir.mkdir(parents=True, exist_ok=False)
    outputs = {"metric_deltas.csv": tc.write_csv_x(out_dir / "metric_deltas.csv", DELTA_COLS, out_rows),
               "metrics_summary_anomalies.csv": tc.write_csv_x(out_dir / "metrics_summary_anomalies.csv",
                                                               tc.ANOMALY_COLS, an.rows())}
    summary = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "channel": channel,
               "fresh_channel_assumption": fresh_channel, "expect_traffic": expect_traffic, "traffic": traffic,
               "inputs": {ph: {"dir": str(d), "status": manifests[ph]["status"],
                               "manifest_sha256": tc.sha256_file(d / "metrics_manifest.json")}
                          for ph, d in phases.items()},
               "derived": derived,
               "derived_note": "inputs for gates V5 (fill_duration_mean_s, all cut causes pooled) and V6 "
                               "(leader_changes_delta); no outcome metric",
               "status": status, "anomalies": an.counts(), "outputs_sha256": outputs}
    tc.write_json_x(out_dir / "metrics_summary.json", summary)
    return summary


                                                                        
def main(argv: list[str] | None = None, fetch: Fetch | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scrape")
    s.add_argument("--target", action="append", required=True)
    s.add_argument("--phase", required=True, choices=["before", "during", "after"])
    s.add_argument("--out-dir", required=True, type=Path)
    s.add_argument("--interval", type=float, default=1.0)
    s.add_argument("--duration", type=float)
    s.add_argument("--max-rounds", type=int)
    s.add_argument("--stop-file", type=Path)
    s.add_argument("--timeout", type=float, default=2.0)
    s.add_argument("--family-filter")
    s.add_argument("--execute", action="store_true", help="contact the endpoints (needs Fabric authorization)")
    z = sub.add_parser("summarize")
    z.add_argument("--before", required=True, type=Path)
    z.add_argument("--after", required=True, type=Path)
    z.add_argument("--during", type=Path)
    z.add_argument("--channel")
    z.add_argument("--fresh-channel", action="store_true")
    z.add_argument("--expect-traffic", action="store_true",
                   help="the run submitted transactions on --channel: require a receiving and a cutting orderer")
    z.add_argument("--out-dir", required=True, type=Path)
    args = ap.parse_args(argv)
    if args.out_dir.exists():
        print(f"refusing to overwrite {args.out_dir}")
        return 2
    cmdline = sys.argv if argv is None else argv
    try:
        if args.cmd == "summarize":
            m = summarize(args.before, args.after, args.out_dir, during=args.during, channel=args.channel,
                          fresh_channel=args.fresh_channel, expect_traffic=args.expect_traffic, argv=cmdline)
            print(f"METRICS SUMMARY {m['status']} -> {args.out_dir}")
            return tc.EXIT[m["status"]]
        targets = parse_targets(args.target)
        if not args.execute:
            for t in targets:
                print(f"PLAN GET {t['url']}  ({t['name']}, {t['role']}, phase {args.phase})")
            print("dry run: nothing contacted or written (add --execute once Fabric use is authorized)")
            return 0
        stop = (lambda: args.stop_file.exists()) if args.stop_file else None
        m = run_scrape(targets, args.phase, args.out_dir, fetch=fetch or http_fetch, interval_s=args.interval,
                       duration_s=args.duration, max_rounds=args.max_rounds, stop=stop, timeout_s=args.timeout,
                       family_filter=args.family_filter, argv=cmdline)
    except (OSError, ValueError, KeyError) as err:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {err!r}")
        return 2
    for sev in ("error", "warning"):
        for k, n in sorted(m["anomalies"][sev].items()):
            print(f"  {sev:<7} {k:<30} {n}")
    print(f"METRICS {m['phase']} {m['status']} rounds={m['rounds']} -> {args.out_dir}")
    return tc.EXIT[m["status"]]


if __name__ == "__main__":
    sys.exit(main())
