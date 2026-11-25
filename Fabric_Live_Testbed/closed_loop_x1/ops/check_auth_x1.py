"""Read-only: an owner authorization file for the X1 phase, through the runner's own parser, plus X1-specific limits.
Usage: check_auth_x1.py <AUTH file> <stage dir>      Exit 0 = ACCEPTED, 1 = REFUSED.
The assistant never writes the file. It must authorize exactly the staged runs, only their channels, and:
  max_broadcasts_per_run = 0 (X1 never changes a configuration), native_verification = required, broadcast_targets = 10.
  (The frozen actuator parser accepts targets only in 10..800, so 2 and 5 cannot be written there. X1 authorizes NO broadcast: static runs,
  max_broadcasts_per_run = 0, so the single value 10 is inert. Channels are CREATED at B=2/5/10 by the unchanged e1_channel_run.py, whose range is 1..uint32.)
It must not authorize any run of an earlier experiment (cs01, cl01..cl13, cx01..cx04, cm01..cm12)."""
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from Fabric_Live_Testbed.closed_loop import authorization as A  # noqa: E402

auth_path, stage = Path(sys.argv[1]), Path(sys.argv[2])
now = dt.datetime.now(dt.timezone.utc)
if not auth_path.is_file():
    print("X1_AUTH REFUSED -> owner authorization file absent:", auth_path)
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
if man.get("kind") != "X1":
    problems.append(f"stage kind {man.get('kind')} is not X1")
if list(auth.runs) != want:
    problems.append(f"authorized runs {list(auth.runs)} != staged runs {want}")
for rid in want:
    try:
        auth.require_run(rid, sums_sha, now=now)
    except A.AuthorizationError as exc:
        problems.append(f"{rid}: {exc}")
if list(auth.broadcast.channels) != [r["channel"] for r in man["runs"]]:
    problems.append("authorized channels differ from the staged channels")
if [int(t) for t in auth.broadcast.targets] != [10]:
    problems.append("broadcast_targets must be 10 (inert: X1 authorizes no broadcast)")
if auth.max_broadcasts_per_run != 0:
    problems.append("X1 authorization must set max_broadcasts_per_run=0")
if auth.broadcast.native_verification != "required":
    problems.append("native_verification must be required")
earlier = [f"cs01", *[f"cl{i:02d}" for i in range(1, 14)], *[f"cx{i:02d}" for i in range(1, 5)], *[f"cm{i:02d}" for i in range(1, 13)]]
for rid in earlier:
    if rid in auth.runs:
        problems.append(f"{rid} (an earlier experiment) must not be authorized")
left_h = (dt.datetime.strptime(auth.broadcast.expires_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc) - now).total_seconds() / 3600
print("approved", auth.broadcast.approved_utc, "expires", auth.broadcast.expires_utc, "hours left", round(left_h, 2), "| runs", list(auth.runs))
print("channels", list(auth.broadcast.channels), "| targets", list(auth.broadcast.targets), "| max_broadcasts", auth.max_broadcasts_per_run,
      "| native", auth.broadcast.native_verification, "| stage SHA256SUMS", sums_sha)
if problems:
    print("X1_AUTH REFUSED ->", "; ".join(problems))
    sys.exit(1)
print("X1_AUTH ACCEPTED")
sys.exit(0)
