"""Read-only gate: the owner's S5-A2 approval file (cl04 infrastructure-abort amendment), through the project's strict parser.
The assistant never writes AUTH_S5_A2.txt; without a valid owner-written file every cl05..cl12 launch via launcher v3 is refused.
Usage: check_s5_a2.py <approval file> <run_id> <launcher path> [--now YYYY-MM-DDTHH:MM:SSZ (self-test only)]
Exit 0 = ACCEPTED, 1 = REFUSED."""
import datetime as dt
import hashlib
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts" / "e1"))
import e1_config_actuator as ca  # noqa: E402  (parse_owner_kv: printable ASCII, LF, key=value, no duplicates, no blanks)

EXP = Path("/mnt/d/PLAN_BLOCK18/closed_loop_experiment_2026-11-22")
OUT = Path("/home/mailf18/e1/closed-loop-20261123")
OPS = Path(__file__).resolve().parent
RECORD = "S5_AMENDMENT_A2_2026-11-25.md"
REQUIRED = {"approved_by", "approved_utc", "amendment_id", "amendment_record_sha256", "a1_approval_sha256",
            "auth_series_sha256", "stage_sha256", "launcher_sha256", "continuation_check_sha256", "runs"}
CL04_ABORT = dt.datetime(2026, 11, 23, 9, 41, 22, tzinfo=dt.timezone.utc)   # orphan Caliper stopped; the abort is after this
SERIES_REST = [f"cl{i:02d}" for i in range(5, 13)]


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def utc(text):
    if len(text) != 20 or not text.endswith("Z"):
        raise ValueError("approved_utc must be YYYY-MM-DDTHH:MM:SSZ")
    return dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)


def check(path, run_id, launcher, now, exp=EXP, out=OUT):
    problems = []
    kv = ca.parse_owner_kv(Path(path).read_bytes())
    missing = sorted(REQUIRED - set(kv))
    extra = sorted(set(kv) - REQUIRED)
    if missing or extra:
        return [f"keys missing {missing} / unexpected {extra}"]
    if kv["approved_by"] != "owner":
        problems.append("approved_by must be owner")
    try:
        t = utc(kv["approved_utc"])
        if not CL04_ABORT < t <= now:
            problems.append(f"approved_utc {kv['approved_utc']} must be after the cl04 abort ({CL04_ABORT:%FT%TZ}) and not in the future")
    except ValueError as exc:
        problems.append(str(exc))
    if kv["amendment_id"] != "S5-A2":
        problems.append("amendment_id must be S5-A2")
    want = {"amendment_record_sha256": sha(exp / RECORD),
            "a1_approval_sha256": sha(exp / "AUTH_S5_AMENDMENT.txt"),
            "auth_series_sha256": sha(exp / "AUTH_SERIES.txt"),
            "stage_sha256": sha(exp / "stage_series" / "SHA256SUMS.txt"),
            "launcher_sha256": sha(launcher),
            "continuation_check_sha256": sha(OPS / "s5a1_continuation_check.py")}
    for k, v in want.items():
        if kv[k].lower() != v:
            problems.append(f"{k} does not match the current file ({kv[k][:12]} vs {v[:12]})")
    if kv["runs"].split(",") != SERIES_REST:
        problems.append("runs must be exactly cl05..cl12")
    if run_id not in SERIES_REST:
        problems.append(f"run {run_id} is not governed by S5-A2")
    # cl04 must really be an infrastructure abort that is on disk, never a measured run
    if (out / "cl04" / "run_result.json").exists():
        problems.append("cl04 has a run_result.json: it is not an infrastructure abort")
    if not (exp / "series_evidence_2026-11-22" / "cl04_ABORTED").is_dir():
        problems.append("cl04_ABORTED evidence directory absent on D:")
    return problems


def main():
    path, run_id, launcher = sys.argv[1], sys.argv[2], sys.argv[3]
    now = utc(sys.argv[5]) if len(sys.argv) > 5 and sys.argv[4] == "--now" else dt.datetime.now(dt.timezone.utc)
    if not Path(path).exists():
        print("S5A2_APPROVAL REFUSED -> approval file absent:", path)
        return 1
    print("now", now.isoformat(timespec="seconds"), "| approval file sha256", sha(path))
    try:
        problems = check(path, run_id, launcher, now)
    except (ca.SafetyError, OSError) as exc:
        problems = [str(exc)]
    if problems:
        print("S5A2_APPROVAL REFUSED ->", "; ".join(problems))
        return 1
    print("S5A2_APPROVAL ACCEPTED for", run_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
