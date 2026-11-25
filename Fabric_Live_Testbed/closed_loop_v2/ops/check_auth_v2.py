"""Read-only: an owner authorization file for a v2 phase, through the runner's own parser, plus phase-specific limits.
Usage: check_auth_v2.py <phase x0|m> <AUTH file> <stage dir>      Exit 0 = ACCEPTED, 1 = REFUSED.
The assistant never writes the file. It must authorize exactly the staged runs, only their channels, and:
  x0: max_broadcasts_per_run = 0 (X0 never changes a configuration)
  m : max_broadcasts_per_run <= 10, native_verification = required"""
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from Fabric_Live_Testbed.closed_loop import authorization as A  # noqa: E402

phase, auth_path, stage = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
now = dt.datetime.now(dt.timezone.utc)
if not auth_path.is_file():
    print("V2_AUTH REFUSED -> owner authorization file absent:", auth_path)
    sys.exit(1)
print("now UTC", now.isoformat(timespec="seconds"), "| file sha256", hashlib.sha256(auth_path.read_bytes()).hexdigest())
problems = []
try:
    auth = A.ExperimentAuthorization.load(auth_path, now=now)
except A.AuthorizationError as exc:
    print("PARSER VERDICT: REFUSED ->", exc)
    sys.exit(1)
man = json.loads((stage / "STAGE_MANIFEST.json").read_text())
want = [r["run_id"] for r in man["runs"]]
sums_sha = hashlib.sha256((stage / "SHA256SUMS.txt").read_bytes()).hexdigest()
if man.get("kind", "").lower() != phase:
    problems.append(f"stage kind {man.get('kind')} is not phase {phase}")
if list(auth.runs) != want:
    problems.append(f"authorized runs {list(auth.runs)} != staged runs {want}")
for rid in want:
    try:
        auth.require_run(rid, sums_sha, now=now)
    except A.AuthorizationError as exc:
        problems.append(f"{rid}: {exc}")
if list(auth.broadcast.channels) != [r["channel"] for r in man["runs"]]:
    problems.append("authorized channels differ from the staged channels")
if phase == "x0" and auth.max_broadcasts_per_run != 0:
    problems.append("X0 authorization must set max_broadcasts_per_run=0")
if phase == "m":
    if auth.max_broadcasts_per_run > 10:
        problems.append("M authorization: max_broadcasts_per_run must be <= 10")
    if auth.broadcast.native_verification != "required":
        problems.append("M authorization: native_verification must be required")
for rid in ("cl01", "cs01", "cl13"):
    try:
        auth.require_run(rid, sums_sha, now=now)
        problems.append(f"{rid} must not be authorized")
    except A.AuthorizationError:
        pass
left_h = (dt.datetime.strptime(auth.broadcast.expires_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc) - now).total_seconds() / 3600
print("approved", auth.broadcast.approved_utc, "expires", auth.broadcast.expires_utc, "hours left", round(left_h, 2), "| runs", list(auth.runs))
print("channels", list(auth.broadcast.channels), "| targets", list(auth.broadcast.targets), "| max_broadcasts", auth.max_broadcasts_per_run,
      "| native", auth.broadcast.native_verification, "| stage SHA256SUMS", sums_sha)
if problems:
    print("V2_AUTH REFUSED ->", "; ".join(problems))
    sys.exit(1)
print("V2_AUTH ACCEPTED for phase", phase)
sys.exit(0)
