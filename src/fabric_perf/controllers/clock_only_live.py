"""Dedicated, single-operation live route for the Windows <-> WSL clock round (task A0w, 2026-11-22).

Why a new module. The A0v design needs three global switches (``wsl_transport.REAL_SPAWN_COMPILED_IN``, ``process_runner.REAL_TRANSPORT_ENABLED``,
``wsl_clock_probe.LIVE_CLOCK_SAMPLING_ENABLED``) plus a changed executor check, and flipping them would open every operation category at once. A0w leaves ALL of them False and
every earlier candidate file unchanged. This module is the only place with a spawn call, and it is hard-wired to ONE vector::

    <explicit absolute path of wsl.exe> -d <distribution> --exec date +%s%N

What it enforces (each failure is a stable code; nothing is repaired, nothing is retried):

* the operation-specific activation record of A0v (``wsl_transport.verify_activation_record``, category ``wsl_clock_sampling``) must verify, including the owner's approval
  document that pins the record's own ``record_sha256``;
* the record's ``binding`` must equal what is observed NOW: host name and OS build, SHA-256 of ``wsl.exe``, the registered distribution, the SHA-256 of every source file in
  ``SNAPSHOT_FILES`` and the candidate manifest's own SHA-256 (the manifest is also verified file by file);
* the schedule is capped by constants in this file (``CAPS``): at most 2 x 20 process starts, 10 s per process, a 5 s gap, 420 s in total, 3 consecutive failures end the round;
* the output directory must be new and under ``EVIDENCE_ROOT``; every file is created exclusively;
* the argv that reaches the process API is compared with the one allowed vector immediately before every start; any other argv is refused;
* a failed or timed-out sample is recorded as invalid and is NOT repeated; the round continues to its planned count unless a stop condition is reached.

What it does not do: start Docker, Caliper, Fabric, Prometheus or a model, open a network connection, accept an environment variable, a flag or a Boolean that widens the scope, or
choose a clock tolerance. The evaluator is the unchanged A0e ``clock_preflight.assess_phases``; its tolerance and bounds are DIAGNOSTIC parameters, labelled as such in every output.
The module-local key ``CLOCK_ONLY_SPAWN_COMPILED_IN`` is not one of the three A0v keys and opens nothing but this vector.
"""

from __future__ import annotations

import hashlib
import json
import math
import platform
import re
import statistics
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import bounded_execution as be
from . import clock_preflight as cp
from . import prelive_integrity as pi
from . import process_runner as pr
from . import wsl_clock_probe as wp
from . import wsl_transport as wt

LIVE_VERSION = "clock_only_live 0.1 (A0w, 2026-11-22)"
CLOCK_ONLY_SPAWN_COMPILED_IN = True        # this module's own key; it opens ONE read-only vector and nothing else. The three A0v global keys are untouched (False).
OPERATION = "wsl_clock_sampling"
PHASES = ("pre_run", "post_run")
EVIDENCE_ROOT = "docs/research/e1/a0w_runtime_evidence_2026-11-22"
CAPS = {"samples_per_phase": 20, "probe_timeout_s": 10.0, "phase_gap_s": 5.0, "max_total_runtime_s": 420.0, "max_process_starts": 40, "max_consecutive_failures": 3,
        "max_capture_bytes": 65536}
STEP_ID = "K01+K03:clock_probe"
SNAPSHOT_FILES = (
    "src/fabric_perf/controllers/clock_only_live.py", "src/fabric_perf/controllers/wsl_clock_probe.py", "src/fabric_perf/controllers/clock_preflight.py",
    "src/fabric_perf/controllers/wsl_transport.py", "src/fabric_perf/controllers/process_runner.py", "src/fabric_perf/controllers/bounded_execution.py",
    "src/fabric_perf/controllers/prelive_integrity.py", "scripts/e1/e1_a0w_clock_live.py")
_DISTRO = re.compile(r"[A-Za-z0-9_.-]{1,64}")
_SHA = re.compile(r"[0-9a-f]{64}")
DIAGNOSTIC_LABEL = "DIAGNOSTIC_NOT_APPROVED: an evaluator input needed to run the unchanged A0e code; it is not an accepted clock tolerance"


class ClockOnlyRefused(RuntimeError):
    """A gate failed; nothing was started. ``findings`` lists every problem found."""

    def __init__(self, gate: str, findings: Sequence[Mapping[str, str]]):
        super().__init__(f"{gate}: {[f.get('code') for f in findings]}")
        self.gate, self.findings = gate, [dict(f) for f in findings]


@dataclass(frozen=True)
class ProcOutcome:
    returncode: int | None
    stdout: bytes
    stderr: bytes
    timed_out: bool


# ------------------------------------------------------------------------------------------------ the one vector
def allowed_vector(distro: str, *, timeout_s: float, uses: int) -> wt.AllowedVector:
    if not (isinstance(distro, str) and _DISTRO.fullmatch(distro)):
        raise ClockOnlyRefused("scope", [{"code": "distro", "message": "the WSL distribution is required (no default)"}])
    return wt.AllowedVector(STEP_ID, ("wsl.exe", "-d", distro, "--exec", "date", "+%s%N"), None, float(timeout_s), "read_only_observation", int(uses))


def spawn_argv(executable: str, distro: str) -> list[str]:
    """The only argv that may reach the process API."""
    return [executable, "-d", distro, "--exec", "date", "+%s%N"]


def check_scope(binding: Mapping[str, Any]) -> list[dict[str, str]]:
    """Problems with the requested schedule and paths against the hard caps in this file. Empty = within scope."""
    found: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        found.append({"code": code, "message": message})

    sched = binding.get("schedule")
    if not isinstance(sched, Mapping):
        return [{"code": "schedule_missing", "message": "the binding carries no schedule"}]
    if tuple(sched.get("phases") or ()) != PHASES:
        bad("phases", f"phases must be exactly {list(PHASES)}")
    for key, cap in (("samples_per_phase", CAPS["samples_per_phase"]), ("probe_timeout_s", CAPS["probe_timeout_s"]), ("phase_gap_s", CAPS["phase_gap_s"]),
                     ("max_total_runtime_s", CAPS["max_total_runtime_s"]), ("max_process_starts", CAPS["max_process_starts"])):
        value = sched.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= cap:
            bad(key, f"{key} must be a number in (0, {cap}]; got {value!r}")
    if isinstance(sched.get("samples_per_phase"), int) and isinstance(sched.get("max_process_starts"), int) and sched["max_process_starts"] != 2 * sched["samples_per_phase"]:
        bad("max_process_starts", "max_process_starts must equal 2 x samples_per_phase (one start per planned sample, no retry slot)")
    if isinstance(sched.get("samples_per_phase"), int) and isinstance(sched.get("min_valid_samples"), int) and not 1 <= sched["min_valid_samples"] <= sched["samples_per_phase"]:
        bad("min_valid_samples", "min_valid_samples must be within 1..samples_per_phase")
    res = binding.get("remote_resolution_ns")
    if isinstance(res, bool) or not isinstance(res, int) or res < 1:
        bad("remote_resolution_ns", "remote_resolution_ns must be a positive integer (declared)")
    if binding.get("reference_domain") in (None, "") or binding.get("remote_domain") in (None, "") or binding.get("reference_domain") == binding.get("remote_domain"):
        bad("clock_domains", "two different clock-domain labels are required")
    diag = binding.get("diagnostic_evaluator_params")
    if not (isinstance(diag, Mapping) and diag.get("label") == DIAGNOSTIC_LABEL and all(isinstance(diag.get(k), (int, float)) and not isinstance(diag.get(k), bool) and diag[k] > 0
                                                                                      for k in ("tolerance_ms", "min_valid_samples", "max_wall_monotonic_disagreement_ms"))):
        bad("diagnostic_params", "the evaluator parameters must be present and labelled as diagnostic, not as an accepted tolerance")
    if binding.get("operation") != OPERATION:
        bad("operation", f"operation must be {OPERATION!r}")
    out = str(binding.get("output_dir") or "")
    norm = out.replace("\\", "/")
    if not re.fullmatch(r"[A-Za-z]:/.*", norm) or ".." in norm.split("/") or f"/{EVIDENCE_ROOT}/" not in norm + "/":
        bad("output_dir", f"output_dir must be an absolute Windows path under .../{EVIDENCE_ROOT}/")
    return found


# ------------------------------------------------------------------------------------------------ live observations (injectable)
def registered_distributions() -> list[str]:
    """Registered WSL distribution names from the per-user registry (no process is started)."""
    import winreg                                                                       # noqa: PLC0415 - Windows only
    names: list[str] = []
    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Microsoft\Windows\CurrentVersion\Lxss") as root:
        index = 0
        while True:
            try:
                sub = winreg.EnumKey(root, index)
            except OSError:
                break
            index += 1
            with winreg.OpenKey(root, sub) as key:
                try:
                    names.append(str(winreg.QueryValueEx(key, "DistributionName")[0]))
                except OSError:
                    continue
    return names


def live_binding(record_binding: Mapping[str, Any], *, repo_root: Path, node_name: str, os_version: str, distros: Sequence[str],
                 sha_of: Callable[[Path], str] = pi.sha256_file) -> dict[str, Any]:
    """What this run would be, with every observable value read NOW. Compared with the record's own binding by the A0v verifier."""
    exe = str(record_binding.get("wsl_exe_path") or "")
    live = dict(record_binding)
    live["host_node"], live["host_os_version"] = node_name, os_version
    live["wsl_exe_sha256"] = sha_of(Path(exe)) if exe and Path(exe).is_file() else None
    live["distro_registered"] = record_binding.get("distro") in set(distros)
    live["source_snapshot"] = {rel: (sha_of(Path(repo_root) / rel) if (Path(repo_root) / rel).is_file() else None) for rel in SNAPSHOT_FILES}
    return live


def preflight(record: Any, *, repo_root: Path, manifest_path: Path, node_name: str, os_version: str, distros: Sequence[str],
              sha_of: Callable[[Path], str] = pi.sha256_file) -> dict[str, Any]:
    """Gates 1, 2, 3, 5, 6. Raises :class:`ClockOnlyRefused` at the first failed gate (tests are Gate 4, run by the operator before this)."""
    if not isinstance(record, Mapping) or not isinstance(record.get("binding"), Mapping):
        raise ClockOnlyRefused("gate5_authorization", [{"code": "record_missing", "message": "no activation record or no binding"}])
    rb = record["binding"]
    scope = check_scope(rb)
    if scope or record.get("operation_category") != wt.OPC_CLOCK:
        raise ClockOnlyRefused("gate3_scope", scope or [{"code": "operation_category", "message": f"{record.get('operation_category')!r} is not {wt.OPC_CLOCK!r}"}])
    manifest_file = Path(manifest_path)
    manifest_sha = sha_of(manifest_file) if manifest_file.is_file() else None
    integrity: list[dict[str, str]] = []
    if manifest_sha is None:
        integrity.append({"code": "manifest_missing", "message": str(manifest_file)})
    else:
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        integrity += [f.as_dict() for f in pi.verify_candidate_manifest(manifest, Path(repo_root))]
        listed = {e["path"]: e["sha256"] for e in manifest.get("entries", [])}
        integrity += [{"code": "snapshot_file_not_in_manifest", "message": rel} for rel in SNAPSHOT_FILES if rel not in listed]
    if integrity:
        raise ClockOnlyRefused("gate1_integrity", integrity)
    binding = live_binding(rb, repo_root=repo_root, node_name=node_name, os_version=os_version, distros=distros, sha_of=sha_of)
    registered = binding.pop("distro_registered")                                    # an observation, not part of the bound record
    identity = []
    if not registered:
        identity.append({"code": "distro_not_registered", "message": f"{rb.get('distro')!r} is not among the registered distributions {sorted(distros)}"})
    if binding["wsl_exe_sha256"] is None:
        identity.append({"code": "wsl_exe_missing", "message": str(rb.get("wsl_exe_path"))})
    if binding["host_node"] != rb.get("host_node") or binding["host_os_version"] != rb.get("host_os_version"):
        identity.append({"code": "host_mismatch", "message": f"live host {node_name!r}/{os_version!r}, authorized {rb.get('host_node')!r}/{rb.get('host_os_version')!r}"})
    if identity:
        raise ClockOnlyRefused("gate2_runtime_identity", identity)
    sched = rb["schedule"]
    allowed = [allowed_vector(rb["distro"], timeout_s=sched["probe_timeout_s"], uses=sched["max_process_starts"])]
    problems = wt.verify_activation_record(record, category=wt.OPC_CLOCK, allowed=allowed, binding=binding, expected_candidate_manifest_sha256=manifest_sha)
    if record.get("executable") != rb.get("wsl_exe_path"):
        problems.append({"code": "executable_binding", "message": "the record's executable differs from the bound wsl.exe path"})
    if record.get("max_active_processes") != 1:
        problems.append({"code": "max_active_processes", "message": "a clock round runs exactly one process at a time"})
    if isinstance(record.get("max_capture_bytes"), (int, float)) and record["max_capture_bytes"] > CAPS["max_capture_bytes"]:
        problems.append({"code": "max_capture_bytes", "message": f"exceeds the cap {CAPS['max_capture_bytes']}"})
    if isinstance(record.get("max_process_lifetime_s"), (int, float)) and not sched["probe_timeout_s"] <= record["max_process_lifetime_s"] <= CAPS["probe_timeout_s"]:
        problems.append({"code": "max_process_lifetime_s", "message": f"must be within [{sched['probe_timeout_s']}, {CAPS['probe_timeout_s']}]"})
    if problems:
        raise ClockOnlyRefused("gate5_authorization", problems)
    out = Path(rb["output_dir"])
    if out.exists():
        raise ClockOnlyRefused("gate6_output_isolation", [{"code": "output_directory_exists", "message": str(out)}])
    root = (Path(repo_root) / EVIDENCE_ROOT).resolve()
    if root not in out.resolve().parents:
        raise ClockOnlyRefused("gate6_output_isolation", [{"code": "output_outside_evidence_root", "message": f"{out} is not under {root}"}])
    return {"gates": {"gate1_integrity": "PASS", "gate2_runtime_identity": "PASS", "gate3_scope": "PASS", "gate5_authorization": "PASS", "gate6_output_isolation": "PASS"},
            "manifest_sha256": manifest_sha, "record_sha256": record["record_sha256"], "live_binding": binding, "allowed_vector": allowed[0].as_dict()}


# ------------------------------------------------------------------------------------------------ record preparation and sealing
SCHEDULE = {"phases": list(PHASES), "samples_per_phase": 20, "min_valid_samples": 10, "phase_gap_s": 5.0, "probe_timeout_s": 10.0, "max_total_runtime_s": 420.0, "max_process_starts": 40}
DIAGNOSTIC = {"label": DIAGNOSTIC_LABEL, "tolerance_ms": 250.0, "min_valid_samples": 10, "max_wall_monotonic_disagreement_ms": 50.0}


def build_record(*, repo_root: Path, distro: str, wsl_exe: str, manifest: Path, out_dir: Path, node_name: str, os_version: str, distros: Sequence[str],
                 schedule: Mapping[str, Any] | None = None, diagnostic: Mapping[str, Any] | None = None, sha_of: Callable[[Path], str] = pi.sha256_file) -> dict[str, Any]:
    """The UNSIGNED activation record (every observable value read now). Not valid until :func:`seal_record` adds an approval block that pins its ``record_sha256``."""
    base = {"operation": OPERATION, "host_node": None, "host_os_version": None, "wsl_exe_path": wsl_exe, "wsl_exe_sha256": None, "distro": distro, "schedule": dict(schedule or SCHEDULE),
            "reference_domain": "windows_host_time_time_ns", "remote_domain": f"wsl_{distro}_CLOCK_REALTIME_date_%s%N", "remote_resolution_ns": 1000,
            "diagnostic_evaluator_params": dict(diagnostic or DIAGNOSTIC), "output_dir": str(out_dir), "source_snapshot": None,
            "candidate_manifest_path": str(Path(manifest).resolve().relative_to(Path(repo_root).resolve())).replace("\\", "/")}
    live = live_binding(base, repo_root=repo_root, node_name=node_name, os_version=os_version, distros=distros, sha_of=sha_of)
    if not live.pop("distro_registered"):
        raise ClockOnlyRefused("gate2_runtime_identity", [{"code": "distro_not_registered", "message": f"{distro!r} is not a registered WSL distribution"}])
    allowed = [allowed_vector(distro, timeout_s=base["schedule"]["probe_timeout_s"], uses=base["schedule"]["max_process_starts"])]
    record = wt.activation_template(wt.OPC_CLOCK, allowed, live)
    record.update(distro=distro, executable=wsl_exe, max_process_lifetime_s=base["schedule"]["probe_timeout_s"], max_active_processes=1, max_capture_bytes=CAPS["max_capture_bytes"],
                  candidate_manifest_sha256=sha_of(Path(manifest)), activation_reference="A0w task instruction 2026-11-22 (clock-only, single bounded session)", real_spawn_authorized=True)
    record["note"] = "A0w clock-only activation record. NOT valid until an approval block pins record_sha256 (see seal)."
    record["record_sha256"] = be._digest(record, drop=("record_sha256", "approval"))
    return record


def seal_record(record: Mapping[str, Any], approval_doc: Path, approved_by: str, *, now_utc: datetime | None = None, sha_of: Callable[[Path], str] = pi.sha256_file) -> dict[str, Any]:
    """A copy of ``record`` with the approval block set, after checking that the document contains the record's own ``record_sha256``."""
    doc = Path(approval_doc)
    if str(record.get("record_sha256")) not in doc.read_text(encoding="utf-8"):
        raise ClockOnlyRefused("seal", [{"code": "approval_document_does_not_pin_record", "message": "the approval document does not contain the record's record_sha256"}])
    sealed = json.loads(json.dumps(dict(record)))
    sealed["approval"] = {"status": "APPROVED_BY_OWNER", "approved_by": approved_by, "approved_utc": (now_utc or datetime.now(timezone.utc)).isoformat(timespec="seconds"),
                          "record_path": str(doc), "record_sha256": sha_of(doc)}
    return sealed


# ------------------------------------------------------------------------------------------------ the real process call
def real_process_run(argv: Sequence[str], timeout_s: float) -> ProcOutcome:
    """The ONLY call that starts a process in this module: structured argv, no shell, no stdin, bounded by ``timeout_s`` (the child is killed on timeout)."""
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        done = subprocess.run(list(argv), stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout_s, shell=False, check=False, creationflags=flags)
        return ProcOutcome(done.returncode, done.stdout or b"", done.stderr or b"", False)
    except subprocess.TimeoutExpired as exc:
        return ProcOutcome(None, exc.stdout or b"", exc.stderr or b"", True)


# ------------------------------------------------------------------------------------------------ evidence helpers
def _iso(now: datetime) -> str:
    return now.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _sample_row(seq: int, sampling_id: str, phase: str, rb: Mapping[str, Any], w0: int, m0: int, remote: int, m1: int, w1: int) -> dict[str, Any]:
    return {"seq": seq, "sampling_id": sampling_id, "phase": phase, "reference_domain": rb["reference_domain"], "remote_domain": rb["remote_domain"], "ref_wall_before_ns": w0,
            "remote_wall_ns": remote, "ref_wall_after_ns": w1, "ref_mono_before_ns": m0, "ref_mono_after_ns": m1, "remote_resolution_ns": rb["remote_resolution_ns"],
            "round_trip_ns": w1 - w0, "read_order": "wall_before,mono_before,request,mono_after,wall_after", "unit": "ns",
            "ref_wall_label": "Unix epoch UTC (Windows time.time_ns)", "ref_mono_label": "Windows monotonic: elapsed time only"}


def analyze(records: Sequence[Mapping[str, Any]], verdict_doc: Mapping[str, Any]) -> dict[str, Any]:
    """Tolerance-independent characterization of the stored samples: per-sample offset interval, round trip, per-phase intersection and the phase-to-phase difference."""
    ms = lambda x2: x2 / 2 / 1_000_000                                                   # noqa: E731 - exact half-ns units to ms
    out: dict[str, Any] = {"schema": "a0w_clock_analysis/1", "version": LIVE_VERSION, "offset_sign": "remote (WSL) minus reference (Windows), ms", "phases": {}}
    mids: dict[str, float] = {}
    for rec in records:
        rows = []
        for s in rec["samples"]:
            cs = cp.parse_sample(s)
            rows.append({"seq": s["seq"], "offset_ms": ms(cs.offset_x2), "uncertainty_ms": ms(cs.uncertainty_x2), "low_ms": ms(cs.offset_x2 - cs.uncertainty_x2),
                         "high_ms": ms(cs.offset_x2 + cs.uncertainty_x2), "round_trip_ms": (s["ref_wall_after_ns"] - s["ref_wall_before_ns"]) / 1e6,
                         "mono_round_trip_ms": (s["ref_mono_after_ns"] - s["ref_mono_before_ns"]) / 1e6})
        phase = {"count_requested": rec["count_requested"], "count_valid_rows": len(rows), "invalid": list(rec["invalid"]), "samples": rows}
        if rows:
            offs, rtts, uncs = [r["offset_ms"] for r in rows], [r["round_trip_ms"] for r in rows], [r["uncertainty_ms"] for r in rows]
            low, high = max(r["low_ms"] for r in rows), min(r["high_ms"] for r in rows)
            best = min(rows, key=lambda r: r["uncertainty_ms"])
            phase.update(offset_ms={"min": min(offs), "median": statistics.median(offs), "max": max(offs), "spread": max(offs) - min(offs)},
                         round_trip_ms={"min": min(rtts), "median": statistics.median(rtts), "max": max(rtts)}, uncertainty_ms={"min": min(uncs), "median": statistics.median(uncs), "max": max(uncs)},
                         intersection_ms=({"low": low, "high": high, "width": high - low} if low <= high else None), intersection_empty=low > high,
                         best_sample={"seq": best["seq"], "offset_ms": best["offset_ms"], "uncertainty_ms": best["uncertainty_ms"], "low_ms": best["low_ms"], "high_ms": best["high_ms"]})
            mids[rec["phase"]] = best["offset_ms"]
            gran = [s["remote_wall_ns"] for s in rec["samples"]]
            phase["remote_reading_granularity_observed_ns"] = math.gcd(*gran) if len(gran) > 1 else gran[0]
        out["phases"][rec["phase"]] = phase
    if set(mids) == set(PHASES):
        pre, post = out["phases"]["pre_run"], out["phases"]["post_run"]
        out["phase_to_phase"] = {"best_sample_offset_difference_ms": mids["post_run"] - mids["pre_run"],
                                 "median_offset_difference_ms": post["offset_ms"]["median"] - pre["offset_ms"]["median"],
                                 "best_sample_intervals_overlap": not (post["best_sample"]["low_ms"] > pre["best_sample"]["high_ms"] or pre["best_sample"]["low_ms"] > post["best_sample"]["high_ms"]),
                                 "note": "a nonzero difference over the phase gap is observed clock movement between phases, not a drift rate"}
    out["evaluator_verdict_diagnostic"] = {"verdict": verdict_doc.get("verdict"), "label": DIAGNOSTIC_LABEL}
    return out


def classify_outcome(*, real: bool, starts: int, planned: int, valid_by_phase: Mapping[str, int], min_valid: int, stop_reason: str | None, write_errors: int) -> dict[str, Any]:
    if not real:
        cls = "SIMULATED_NOT_EVIDENCE"
    elif starts == 0:
        cls = "NO_REAL_EVIDENCE"
    elif write_errors == 0 and stop_reason is None and starts == planned and all(valid_by_phase.get(p, 0) >= min_valid for p in PHASES):
        cls = "REAL_CLOCK_EVIDENCE_COMPLETE"
    else:
        cls = "REAL_CLOCK_EVIDENCE_PARTIAL"
    return {"schema": "a0w_run_outcome/1", "class": cls, "real_process_runner": real, "process_starts": starts, "planned_starts": planned, "valid_samples_by_phase": dict(valid_by_phase),
            "stop_reason": stop_reason, "write_errors": write_errors,
            "note": "A0w's own classification (the A0v classifier needs the global switches, which stay False). The clock verdict is a separate, diagnostic result."}


# ------------------------------------------------------------------------------------------------ the round
def execute(record: Mapping[str, Any], pre: Mapping[str, Any], *, run_process: Callable[[Sequence[str], float], ProcOutcome] | None = None,
            wall_ns: Callable[[], int] = time.time_ns, mono_ns: Callable[[], int] = time.monotonic_ns, sleep: Callable[[float], None] = time.sleep,
            now_utc: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    """Run the authorized round. ``pre`` is the result of :func:`preflight`. Refuses unless the module key is set and every gate passed."""
    if not CLOCK_ONLY_SPAWN_COMPILED_IN:
        raise ClockOnlyRefused("compiled_in", [{"code": "clock_only_spawn_not_compiled_in", "message": "nothing was started"}])
    if not (isinstance(pre, Mapping) and set(pre.get("gates", {}).values()) == {"PASS"} and len(pre["gates"]) == 5 and pre.get("record_sha256") == record.get("record_sha256")):
        raise ClockOnlyRefused("preflight", [{"code": "preflight_not_passed", "message": "execute() needs the passing result of preflight() for this record"}])
    rb, sched = record["binding"], record["binding"]["schedule"]
    real = run_process is None
    runner = real_process_run if real else run_process
    argv = spawn_argv(record["executable"], rb["distro"])
    out = Path(rb["output_dir"])
    out.mkdir(parents=True, exist_ok=False)
    cap = int(record["max_capture_bytes"])
    diag = rb["diagnostic_evaluator_params"]

    def write(rel: str, data: Any) -> None:
        blob = data if isinstance(data, bytes) else (json.dumps(data, indent=2, sort_keys=True) + "\n").encode("utf-8")
        be._create_new(out / rel, blob)

    status: dict[str, Any] = {"schema": "a0w_clock_round_status/1", "version": LIVE_VERSION, "status": "COMPLETED", "errors": [], "stop_reason": None, "started_utc": _iso(now_utc()),
                              "record_sha256": record["record_sha256"], "manifest_sha256": pre["manifest_sha256"], "transactions_attempted": 0}
    plog: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    starts = write_errors = 0
    consecutive = 0
    t_start = time.monotonic()
    interruption: BaseException | None = None
    try:
        write("preflight.json", {"schema": "a0w_clock_preflight/1", **pre, "argv_template": spawn_argv(record["executable"], rb["distro"]), "caps": CAPS})
        write("authorization/activation_record.json", dict(record))
        sampling_id = hashlib.sha256(json.dumps([argv, rb["reference_domain"], rb["remote_domain"], record["record_sha256"]], sort_keys=True).encode()).hexdigest()[:16]
        for phase in PHASES:
            samples, invalid = [], []
            try:
                for seq in range(1, sched["samples_per_phase"] + 1):
                    if status["stop_reason"]:
                        break
                    if time.monotonic() - t_start >= sched["max_total_runtime_s"]:
                        status["stop_reason"] = "max_total_runtime_reached"
                        break
                    if starts >= sched["max_process_starts"]:
                        status["stop_reason"] = "max_process_starts_reached"
                        break
                    if argv[0] != record["executable"] or argv[1:] != ["-d", rb["distro"], "--exec", "date", "+%s%N"]:
                        status["stop_reason"] = "unexpected_command_vector"
                        break
                    pr.validate_argv(list(argv))
                    starts += 1                                                                 # the ATTEMPT counts, whatever happens to it
                    started_utc = _iso(now_utc())
                    w0, m0 = wall_ns(), mono_ns()
                    proc = runner(argv, float(sched["probe_timeout_s"]))
                    m1, w1 = mono_ns(), wall_ns()
                    ended_utc = _iso(now_utc())
                    stdout, stderr = bytes(proc.stdout)[:cap], bytes(proc.stderr)[:cap]
                    entry = {"index": starts, "phase": phase, "seq": seq, "argv": list(argv), "started_utc": started_utc, "ended_utc": ended_utc, "returncode": proc.returncode,
                             "timed_out": proc.timed_out, "ref_wall_before_ns": w0, "ref_wall_after_ns": w1, "ref_mono_before_ns": m0, "ref_mono_after_ns": m1,
                             "stdout_bytes": len(proc.stdout), "stderr_bytes": len(proc.stderr), "output_truncated": len(proc.stdout) > cap or len(proc.stderr) > cap,
                             "stdout_sha256": hashlib.sha256(proc.stdout).hexdigest(), "stderr_sha256": hashlib.sha256(proc.stderr).hexdigest(), "retried": False}
                    try:
                        write(f"process_logs/{starts:03d}.stdout", stdout)
                        write(f"process_logs/{starts:03d}.stderr", stderr)
                    except OSError as exc:
                        write_errors += 1
                        status["stop_reason"] = "evidence_write_failure"
                        status["errors"].append({"error": type(exc).__name__, "message": str(exc)})
                    try:
                        if proc.timed_out:
                            raise wp.SampleInvalid("remote_command_timeout", f"the command exceeded {sched['probe_timeout_s']} s and was ended; not repeated")
                        if proc.returncode != 0:
                            raise wp.SampleInvalid("remote_command_failed", f"the remote command exited {proc.returncode}")
                        remote, remote_mono = wp.parse_remote_reading(stdout)
                        if remote_mono is not None:
                            raise wp.SampleInvalid("remote_reading_malformed", "an unexpected second output line was returned")
                        if w1 < w0:
                            raise wp.SampleInvalid("wall_clock_went_backwards", f"Windows wall clock moved back {w0 - w1} ns during the sample")
                        if m1 <= m0:
                            raise wp.SampleInvalid("monotonic_not_advancing", f"Windows monotonic clock did not advance ({m0} -> {m1})")
                        samples.append(_sample_row(seq, sampling_id, phase, rb, w0, m0, remote, m1, w1))
                        entry["valid"], consecutive = True, 0
                    except wp.SampleInvalid as exc:
                        invalid.append({"seq": seq, "code": exc.code, "message": str(exc)})
                        entry.update(valid=False, invalid_code=exc.code)
                        consecutive += 1
                        if consecutive >= CAPS["max_consecutive_failures"] and not status["stop_reason"]:
                            status["stop_reason"] = f"{consecutive}_consecutive_failures"
                    plog.append(entry)
            finally:
                record_doc = {"schema": wp.PHASE_SCHEMA, "version": wp.PROBE_VERSION, "phase": phase, "sampling_id": sampling_id, "count_requested": sched["samples_per_phase"],
                              "count_collected": len(samples), "unit": "ns", "reference_domain": rb["reference_domain"], "remote_domain": rb["remote_domain"],
                              "remote_resolution_ns": rb["remote_resolution_ns"], "samples": samples, "invalid": invalid, "utc_label_is_not_evidence": True,
                              "evidence_origin": "REAL wsl.exe process" if real else "INJECTED TEST RUNNER (not evidence)",
                              "note": "samples only; agreement is decided by clock_preflight from the offset interval, not from a label"}
                records.append(record_doc)
                write(f"clock/{phase}.json", record_doc)
            if status["stop_reason"]:
                break
            if phase == PHASES[0]:
                sleep(float(sched["phase_gap_s"]))
        if len(records) == len(PHASES) and all(r["count_collected"] >= 1 for r in records):
            verdict_doc = wp.evaluate(records, tolerance_ms=diag["tolerance_ms"], min_valid_samples=diag["min_valid_samples"], max_wall_monotonic_disagreement_ms=diag["max_wall_monotonic_disagreement_ms"])
            verdict_doc["parameters_label"] = DIAGNOSTIC_LABEL
            verdict_doc["parameters"] = dict(diag)
            write("clock/clock_verdict.json", verdict_doc)
            write("clock/analysis.json", analyze(records, verdict_doc))
            status["verdict_diagnostic"] = verdict_doc["verdict"]
        else:
            status["verdict_diagnostic"] = "NOT_EVALUATED_INSUFFICIENT_PHASES"
    except BaseException as exc:                                                            # noqa: BLE001 - evidence first, then re-raise
        interruption = exc
        status["status"] = "INTERRUPTED" if isinstance(exc, (KeyboardInterrupt, SystemExit)) else "ERROR"
        status["stop_reason"] = status["stop_reason"] or type(exc).__name__
        status["errors"].append({"error": type(exc).__name__, "message": str(exc)})
    finally:
        status["ended_utc"] = _iso(now_utc())
        status["process_starts"] = starts
        status["elapsed_s"] = round(time.monotonic() - t_start, 3)
        for rel, doc in (("process_logs/process_log.json", {"schema": "a0w_process_log/1", "processes": plog, "retried_any": False}), ("round_status.json", status)):
            try:
                write(rel, doc)
            except OSError as exc:
                write_errors += 1
                status["errors"].append({"error": type(exc).__name__, "message": str(exc)})
        valid = {r["phase"]: r["count_collected"] for r in records}
        try:
            write("run_outcome.json", classify_outcome(real=real, starts=starts, planned=2 * sched["samples_per_phase"], valid_by_phase=valid, min_valid=sched["min_valid_samples"],
                                                       stop_reason=status["stop_reason"], write_errors=write_errors))
            files = {str(p.relative_to(out)).replace("\\", "/"): pi.sha256_file(p) for p in sorted(out.rglob("*")) if p.is_file()}
            write("integrity_manifest.json", {"schema": "a0w_clock_integrity/1", "algorithm": "sha256", "files": files})
        except OSError as exc:
            status["errors"].append({"error": type(exc).__name__, "message": str(exc)})
    if isinstance(interruption, (KeyboardInterrupt, SystemExit)):
        raise interruption
    return status
