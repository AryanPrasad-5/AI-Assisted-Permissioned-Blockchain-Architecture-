"""Owner experiment authorization for the closed-loop runner. One owner-written file serves the runner and the actuator.

Format: printable ASCII, LF, key=value (the actuator's strict parser). Keys read here:
  approved_by=owner
  approved_utc=YYYY-MM-DDTHH:MM:SSZ
  expires_utc=YYYY-MM-DDTHH:MM:SSZ
  stage_sha256=<SHA-256 of the stage's SHA256SUMS.txt>
  runs=<run id>,<run id>,...            runs this file allows (each run is still started one at a time)
  broadcast_channels=<channel>,...       channels on which a live MaxMessageCount update may be broadcast (actuator)
  broadcast_targets=10,40                MaxMessageCount values a broadcast may set (actuator)
  max_broadcasts_per_run=<int 0..20>     most configuration updates the runner may attempt in one run (2026-11-22 preflight)
The assistant never writes this file; the runner refuses without it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from dataclasses import dataclass
from pathlib import Path

from . import ensure_paths

ensure_paths()

import e1_config_actuator as ca  # noqa: E402

REQUIRED = ("approved_by", "approved_utc", "expires_utc", "stage_sha256", "runs", "broadcast_channels", "broadcast_targets",
            "max_broadcasts_per_run")
MAX_BROADCASTS_CEILING = 20


class AuthorizationError(ValueError):
    pass


@dataclass(frozen=True)
class ExperimentAuthorization:
    path: str
    sha256: str
    stage_sha256: str
    runs: tuple[str, ...]
    broadcast: "ca.BroadcastAuthorization"
    max_broadcasts_per_run: int

    @classmethod
    def load(cls, path: Path, *, now: dt.datetime | None = None) -> "ExperimentAuthorization":
        raw = Path(path).read_bytes()
        try:
            kv = ca.parse_owner_kv(raw)
            missing = [k for k in REQUIRED if k not in kv]
            if missing:
                raise AuthorizationError(f"authorization lacks {missing}")
            broadcast = ca.BroadcastAuthorization.load(Path(path), now=now)
        except ca.SafetyError as exc:
            raise AuthorizationError(str(exc)) from exc
        stage = kv["stage_sha256"].lower()
        if len(stage) != 64 or any(c not in "0123456789abcdef" for c in stage):
            raise AuthorizationError("stage_sha256 must be 64 hex characters")
        runs = tuple(kv["runs"].split(","))
        if not runs or len(set(runs)) != len(runs) or any(not r or len(r) > 40 for r in runs):
            raise AuthorizationError("runs must be a list of distinct run ids")
        text = kv["max_broadcasts_per_run"]
        if not text.isdigit() or not 0 <= int(text) <= MAX_BROADCASTS_CEILING:
            raise AuthorizationError(f"max_broadcasts_per_run must be an integer 0..{MAX_BROADCASTS_CEILING}")
        return cls(str(path), hashlib.sha256(raw).hexdigest(), stage, runs, broadcast, int(text))

    def require_run(self, run_id: str, stage_sums_sha256: str, *, now: dt.datetime | None = None) -> None:
        if run_id not in self.runs:
            raise AuthorizationError(f"run {run_id} is not authorized")
        if stage_sums_sha256.lower() != self.stage_sha256:
            raise AuthorizationError("stage SHA256SUMS.txt differs from the authorized stage")
        current = now or dt.datetime.now(dt.timezone.utc)
        if current >= dt.datetime.strptime(self.broadcast.expires_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc):
            raise AuthorizationError("authorization has expired")
