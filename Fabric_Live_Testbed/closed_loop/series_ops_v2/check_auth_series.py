"""Read-only: AUTH_SERIES.txt through the runner's own parser + require_run for every series run."""
import datetime as dt
import hashlib
import sys
from pathlib import Path

sys.path.insert(0, "/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-")
from Fabric_Live_Testbed.closed_loop import authorization as A  # noqa: E402

auth_path, stage_sums = Path(sys.argv[1]), Path(sys.argv[2])
now = dt.datetime.now(dt.timezone.utc)
print("now UTC", now.isoformat(timespec="seconds"), "| file sha256", hashlib.sha256(auth_path.read_bytes()).hexdigest())
try:
    a = A.ExperimentAuthorization.load(auth_path, now=now)
except A.AuthorizationError as exc:
    print("PARSER VERDICT: REFUSED ->", exc)
    sys.exit(1)
b = a.broadcast
print("PARSER VERDICT: ACCEPTED")
print("approved", b.approved_utc, "expires", b.expires_utc, "remaining h",
      round((dt.datetime.fromisoformat(str(b.expires_utc).replace("Z", "+00:00")) - now).total_seconds() / 3600, 2),
      "| native_verification", b.native_verification)
print("runs", a.runs)
print("channels", b.channels, "| targets", b.targets, "| max_broadcasts_per_run", a.max_broadcasts_per_run)
sums_sha = hashlib.sha256(stage_sums.read_bytes()).hexdigest()
bad = 0
for i in range(1, 13):
    rid = f"cl{i:02d}"
    try:
        a.require_run(rid, sums_sha, now=now)
    except A.AuthorizationError as exc:
        bad += 1
        print("require_run", rid, "REFUSED ->", exc)
print("require_run cl01..cl12:", "ALL OK" if not bad else f"{bad} refused", "| stage SHA256SUMS", sums_sha)
for rid in ("cs01", "cl13"):
    try:
        a.require_run(rid, sums_sha, now=now)
        print("require_run", rid, "ACCEPTED (!)")
    except A.AuthorizationError as exc:
        print("require_run", rid, "refused (expected):", str(exc)[:90])
sys.exit(1 if bad else 0)
