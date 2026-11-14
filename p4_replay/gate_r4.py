"""Gate R4, version 2C: no conflicting experiment process. The parsing and decision logic is OFFLINE_TESTED on synthetic fixtures; running it against the real Caliper
container is LIVE_UNTESTED and was NOT done (no `docker top` was run in Phase 2C).

What Phase 2 `preflight.gate_r4_no_conflicting_process` does, and why it is replaced:
  * it takes `docker top <container> -eo pid,args` and flags every line whose lowercased text CONTAINS "caliper". That is a substring test on a path-bearing command line,
    so it answers "does any command line mention caliper anywhere" and not "is a Caliper client running". The question is which it answers wrongly:
      - With the container as the P1a plan documents it ("a `sleep infinity` shell", D:\\PLAN_BLOCK18\\p1a_quicklook_plan_2026-11-12, line 72 and 171) the idle process list contains no
        "caliper" and the Phase 2 gate does NOT false-positive. That documented description is an assumption: nothing in the repository records the container's actual entrypoint.
      - If the idle entrypoint were any command that spells the workspace (the benchmark runs with `-w /hyperledger/caliper/workspace`), for example
        `sh -c "cd /hyperledger/caliper/workspace && sleep infinity"`, the Phase 2 gate WOULD false-positive and block the series on every launch (tested below on a fixture).
      - In the other direction its host-side half (`ps` lines containing "caliper") also matches an editor or `tail` that merely names a file under caliper-ws.
  * it excludes lines containing "docker top", which never appear inside the container.
Here each process line is classified instead of searched:
    CLIENT   an executable token whose basename is `caliper` or `caliper-cli`, or `launch manager`/`launch worker` arguments, or the `@hyperledger/caliper` package path in argv0/argv1
             -> conflict (a Caliper run is in progress or was left behind)
    IDLE     a keep-alive command (`sleep infinity`, `tail -f /dev/null`) that is NOT also a CLIENT line -> allowed
    UNKNOWN  anything else inside the Caliper container (a left-over `docker exec` shell, a stray node) -> conflict, because idleness cannot be shown (fail closed)
The gate still fails when `docker top` cannot be run at all. Nothing is assumed healthy by absence of output.

PENDING OWNER AUTHORISATION (read-only live checks that would settle the one assumption left): `docker top caliper-benchmark-001 -eo pid,ppid,args` and
`docker inspect -f "{{json .Config.Entrypoint}} {{json .Config.Cmd}} {{.Path}} {{json .Args}}" caliper-benchmark-001`. Neither was run.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath

CLIENT_BASENAMES = {"caliper", "caliper-cli"}
CLIENT_ARGS_RE = re.compile(r"\blaunch\s+(manager|worker)\b")
CLIENT_PACKAGE_RE = re.compile(r"@hyperledger/caliper")
IDLE_RE = re.compile(r"(?:^|[\s;&|'\"/])(?:sleep\s+infinity|tail\s+-f\s+/dev/null)(?:['\"\s]|$)")
PS_ROW_RE = re.compile(r"^\s*(\d+)\s+(?:(\d+)\s+)?(.+?)\s*$")

HOST_TOOL_BASENAMES = ("e1_quicklook", "e1_channel_run", "e1_clock_rate_probe", "e1_host_sampler", "e1_resource_sampler", "e1_metrics_scraper", "e1_trace_replay")
HOST_MODULES = ("p4_replay.series", "p4_replay.driver", "p4_replay.live_entry", "p4_replay.launch_series")
HOST_FILES = ("p4_replay/series.py", "p4_replay/driver.py", "p4_replay/live_entry.py", "p4_replay/launch_series.py")
HOST_CLIENT_RE = re.compile(r"docker\s+exec\b.*\bcaliper-benchmark\S*\s|\bcaliper\s+launch\s+(manager|worker)\b")


def _tokens(args: str) -> list[str]:
    return args.replace('"', " ").replace("'", " ").split()


def is_client_line(args: str) -> bool:
    toks = _tokens(args)
    if any(PurePosixPath(t).name.lower() in CLIENT_BASENAMES for t in toks[:6]):
        return True
    return bool(CLIENT_ARGS_RE.search(args) or CLIENT_PACKAGE_RE.search(args))


def classify_container_processes(top_text: str) -> dict:
    """Rows of `docker top <c> -eo pid,args` (or pid,ppid,args) -> {"client": [...], "idle": [...], "unknown": [...], "rows": n}. The header row is any row whose first field is not a number."""
    out = {"client": [], "idle": [], "unknown": [], "rows": 0}
    for line in top_text.splitlines():
        m = PS_ROW_RE.match(line)
        if not m:
            continue
        out["rows"] += 1
        pid, args = m.group(1), m.group(3)
        entry = f"{pid} {args[:160]}"
        if is_client_line(args):
            out["client"].append(entry)
        elif IDLE_RE.search(args):
            out["idle"].append(entry)
        else:
            out["unknown"].append(entry)
    return out


def conflicting_host_processes(ps_text: str, own_pid: int) -> list[str]:
    """Rows of `ps -eo pid,ppid,args` that are an experiment tool or a second series/launch, minus this process, its ancestors and `ps` itself. Matches executable and module
    tokens, not any substring of any argument."""
    rows = []
    for line in ps_text.splitlines():
        m = re.match(r"\s*(\d+)\s+(\d+)\s+(.*)$", line)
        if m:
            rows.append((int(m.group(1)), int(m.group(2)), m.group(3)))
    parent = {pid: ppid for pid, ppid, _ in rows}
    ancestors, cur = {own_pid}, own_pid
    while cur in parent and parent[cur] not in ancestors and parent[cur] != 0:
        cur = parent[cur]
        ancestors.add(cur)
    hits = []
    for pid, _, args in rows:
        if pid in ancestors or re.match(r"ps\s", args):
            continue
        toks = _tokens(args)
        tool = any(PurePosixPath(t).name.startswith(HOST_TOOL_BASENAMES) for t in toks[:6])
        module = any(t in HOST_MODULES for t in toks) or any(t.replace("\\", "/").endswith(HOST_FILES) for t in toks[:6])
        if tool or module or HOST_CLIENT_RE.search(args):
            hits.append(f"{pid} {args[:160]}")
    return hits


def gate_r4(be, own_pid: int):
    """GateResult R4. The same four observations as Phase 2 (ps, docker top, docker ps), judged by classification instead of substring search."""
    from . import driver as dr
    p1b = dr.p1b_module()
    rule = ("no host process of an experiment tool or of another launch (other than this one and its ancestors), the Caliper container shows only keep-alive processes "
            "(no Caliper client, nothing unidentified), and no second Caliper container")

    def result(ok: bool, detail: str, evidence: dict):
        return p1b.GateResult("R4", "no conflicting experiment process", "PASS" if ok else "FAIL", rule, detail, evidence)

    r = be.run(["ps", "-eo", "pid,ppid,args"], None, 30)
    if r.code != 0:
        return result(False, f"ps failed (exit {r.code}): the absence of a conflict cannot be shown", {})
    host = conflicting_host_processes(r.stdout.decode("utf-8", "replace"), own_pid)
    top = be.run(["docker", "top", p1b.CALIPER, "-eo", "pid,ppid,args"], None, 30)
    if top.code != 0:
        return result(False, f"docker top {p1b.CALIPER} failed (exit {top.code}): idleness cannot be shown", {"host": host})
    inside = classify_container_processes(top.stdout.decode("utf-8", "replace"))
    ps_c = be.run(["docker", "ps", "--format", "{{.Names}}"], None, 30)
    others = sorted(n for n in ps_c.stdout.decode("utf-8", "replace").split() if n.startswith("caliper") and n != p1b.CALIPER) if ps_c.code == 0 else ["docker ps failed"]
    problems = [f"host process: {h}" for h in host] + [f"Caliper client inside {p1b.CALIPER}: {c}" for c in inside["client"]] + \
               [f"unidentified process inside {p1b.CALIPER} (idleness not shown): {u}" for u in inside["unknown"]] + [f"container {o}" for o in others]
    if inside["rows"] == 0:
        problems.append(f"docker top {p1b.CALIPER} returned no process rows (the container may not be running)")
    return result(not problems, "; ".join(problems) or f"no conflict; {len(inside['idle'])} keep-alive process(es) inside {p1b.CALIPER}",
                  {"host": host, "inside": inside, "other_containers": others, "live_verification_pending": ["docker top (read-only)", "docker inspect entrypoint (read-only)"]})
