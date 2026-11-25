"""Read-only: an owner authorization file for an M2 phase, through the runner's own parser (under range policy ACT-RANGE-M2-v1), plus M2 limits.
Usage: check_auth_m2.py <m2|act> <AUTH file> <stage dir>      Exit 0 = ACCEPTED, 1 = REFUSED.
The assistant never writes the file. It must authorize exactly the staged runs, only their channels, and:
  m2  : kind M2 stage; broadcast_targets = 2,5,10; 1 <= max_broadcasts_per_run <= 10; native_verification = required
  act : kind ACT stage (the single scripted ACTUATION_CHECK run); broadcast_targets = 2,5,10; max_broadcasts_per_run = 4; native_verification = required
It must not authorize any run of an earlier experiment (cs01, cl01..cl13, cx01..cx04, cm01..cm12, cb01..cb09).
"""
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
for p in (ROOT, ROOT / "src", ROOT / "scripts" / "e1"):
    sys.path.insert(0, str(p))
import e1_actuator_range_m2 as policy  # noqa: E402
import e1_config_actuator as frozen_actuator  # noqa: E402

from Fabric_Live_Testbed.closed_loop import authorization as A  # noqa: E402

PHASE, auth_path, stage = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
KIND = {"m2": "M2", "act": "ACT"}[PHASE]
now = dt.datetime.now(dt.timezone.utc)
if not auth_path.is_file():
    print("M2_AUTH REFUSED -> owner authorization file absent:", auth_path)
    sys.exit(1)
print("now UTC", now.isoformat(timespec="seconds"), "| file sha256", hashlib.sha256(auth_path.read_bytes()).hexdigest())
problems = []
try:
    with policy.range_applied(frozen_actuator):
        auth = A.ExperimentAuthorization.load(auth_path, now=now)
except (A.AuthorizationError, policy.PolicyError) as exc:
    print("PARSER VERDICT: REFUSED ->", exc)
    sys.exit(1)
man = json.loads((stage / "STAGE_MANIFEST.json").read_text())
want = [r["run_id"] for r in man["runs"]]
sums_sha = hashlib.sha256((stage / "SHA256SUMS.txt").read_bytes()).hexdigest()
if man.get("kind") != KIND:
    problems.append(f"stage kind {man.get('kind')} is not {KIND}")
if list(auth.runs) != want:
    problems.append(f"authorized runs {list(auth.runs)} != staged runs {want}")
for rid in want:
    try:
        auth.require_run(rid, sums_sha, now=now)
    except A.AuthorizationError as exc:
        problems.append(f"{rid}: {exc}")
if list(auth.broadcast.channels) != [r["channel"] for r in man["runs"]]:
    problems.append("authorized channels differ from the staged channels")
if sorted(int(t) for t in auth.broadcast.targets) != sorted(policy.MENU_B):
    problems.append(f"broadcast_targets must be exactly {','.join(str(b) for b in policy.MENU_B)}")
if PHASE == "m2" and not 1 <= auth.max_broadcasts_per_run <= 10:
    problems.append("M2 authorization: max_broadcasts_per_run must be 1..10")
if PHASE == "act" and auth.max_broadcasts_per_run != len(man["schedule_B"]) - 2:
    problems.append(f"ACT authorization: max_broadcasts_per_run must be {len(man['schedule_B']) - 2} (the scheduled changes)")
if auth.broadcast.native_verification != "required":
    problems.append("native_verification must be required")
earlier = ["cs01", *[f"cl{i:02d}" for i in range(1, 14)], *[f"cx{i:02d}" for i in range(1, 5)], *[f"cm{i:02d}" for i in range(1, 13)],
           *[f"cb{i:02d}" for i in range(1, 10)]]
if PHASE == "act":
    earlier = [r for r in earlier]   # the ACT run is ca01; it must not be authorized together with comparison runs (checked by exact runs list)
for rid in earlier:
    if rid in auth.runs:
        problems.append(f"{rid} (an earlier experiment) must not be authorized")
left_h = (dt.datetime.strptime(auth.broadcast.expires_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc) - now).total_seconds() / 3600
print("approved", auth.broadcast.approved_utc, "expires", auth.broadcast.expires_utc, "hours left", round(left_h, 2), "| runs", list(auth.runs))
print("channels", list(auth.broadcast.channels), "| targets", list(auth.broadcast.targets), "| max_broadcasts", auth.max_broadcasts_per_run,
      "| native", auth.broadcast.native_verification, "| stage SHA256SUMS", sums_sha)
if problems:
    print("M2_AUTH REFUSED ->", "; ".join(problems))
    sys.exit(1)
print("M2_AUTH ACCEPTED")
sys.exit(0)
