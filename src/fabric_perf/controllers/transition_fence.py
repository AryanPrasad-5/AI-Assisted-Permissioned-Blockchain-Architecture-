"""Durable channel-scoped fence for configuration transition attempts.

The caller must hold ``channel_lock`` from before preflight through final
reconciliation. State is append-by-replacement and survives process restart.
This module does not submit Fabric transactions or decide that an update
committed; callers must resolve UNKNOWN only from a fresh independent read.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Mapping


class FenceError(RuntimeError):
    pass


class ChannelTransitionFence:
    def __init__(self, root: Path, channel: str, *, cooldown_s: float = 0.0):
        if not channel or any(ch.isspace() for ch in channel):
            raise ValueError("channel must be a non-empty identifier")
        self.root = Path(root)
        self.channel = channel
        if cooldown_s < 0:
            raise ValueError("cooldown_s must be non-negative")
        self.cooldown_s = float(cooldown_s)
        key = hashlib.sha256(channel.encode("utf-8")).hexdigest()
        self.directory = self.root / key
        self.lock_path = self.directory / "channel.lock"
        self.state_path = self.directory / "transition.json"

    @contextmanager
    def channel_lock(self) -> Iterator[None]:
        self.directory.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as handle:
            if os.name == "nt":
                import msvcrt
                try:
                    handle.seek(0)
                    if self.lock_path.stat().st_size == 0:
                        handle.write(b"0")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    raise FenceError("channel transition lock is held") from exc
            else:
                import fcntl
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise FenceError("channel transition lock is held") from exc
            try:
                yield
            finally:
                if os.name == "nt":
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def read(self) -> dict | None:
        if not self.state_path.exists():
            return None
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FenceError("transition fence is unreadable; refusing submissions") from exc
        if not isinstance(state, dict) or state.get("channel") != self.channel or not state.get("transition_id"):
            raise FenceError("transition fence is malformed or belongs to another channel")
        if state.get("status") not in {"PREPARED", "SUBMITTING", "UNKNOWN", "VERIFIED", "COMMITTED", "NOT_SUBMITTED"}:
            raise FenceError("transition fence has an unknown status; refusing submissions")
        return state

    def _write(self, state: Mapping) -> dict:
        self.directory.mkdir(parents=True, exist_ok=True)
        data = dict(state)
        encoded = (json.dumps(data, sort_keys=True, indent=2) + "\n").encode("utf-8")
        fd, name = tempfile.mkstemp(prefix=".transition-", suffix=".tmp", dir=self.directory)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.state_path)
            if os.name != "nt":
                dir_fd = os.open(self.directory, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
        except Exception:
            try:
                os.unlink(name)
            except OSError:
                pass
            raise
        return data

    def begin(self, transition_id: str, intended_config: Mapping, *, context: Mapping | None = None) -> dict:
        prior = self.read()
        if prior and prior["transition_id"] == transition_id:
            raise FenceError(f"transition ID {transition_id} was already recorded; refusing duplicate invocation")
        if prior and prior["status"] in {"PREPARED", "SUBMITTING", "UNKNOWN"}:
            raise FenceError(f"unresolved transition {prior['transition_id']} blocks a new submission")
        if prior and prior["status"] in {"VERIFIED", "COMMITTED"} and self.cooldown_s:
            verified_at = prior.get("verified_epoch")
            if verified_at is None:
                raise FenceError("committed transition lacks verified timestamp; refusing submission")
            remaining = self.cooldown_s - (time.time() - float(verified_at))
            if remaining > 0:
                raise FenceError(f"channel cooldown active for {remaining:.3f}s")
        return self._write({"channel": self.channel, "transition_id": transition_id,
                            "intended_config": dict(intended_config), "status": "PREPARED",
                            "context": dict(context or {}),
                            "created_epoch": time.time(), "submission_attempts": 0})

    def mark_submitting(self, transition_id: str) -> dict:
        state = self._require(transition_id, {"PREPARED"})
        state.update(status="SUBMITTING", submission_attempts=state["submission_attempts"] + 1,
                     submission_started_epoch=time.time())
        return self._write(state)

    def mark_submission_acknowledged(self, transition_id: str, evidence: Mapping) -> dict:
        state = self._require(transition_id, {"SUBMITTING"})
        state.update(submission_outcome="CLI_ACKNOWLEDGED", submission_acknowledgment=dict(evidence),
                     submission_acknowledged_epoch=time.time())
        # A CLI acknowledgment is deliberately not a commit or verification state.
        return self._write(state)

    def mark_not_submitted(self, transition_id: str, reason: str) -> dict:
        state = self._require(transition_id, {"PREPARED"})
        state.update(status="NOT_SUBMITTED", resolution=reason, resolved_epoch=time.time())
        return self._write(state)

    def mark_unknown(self, transition_id: str, reason: str) -> dict:
        state = self._require(transition_id, {"SUBMITTING", "UNKNOWN"})
        state.update(status="UNKNOWN", resolution=reason, updated_epoch=time.time())
        return self._write(state)

    def reconcile(self, transition_id: str, *, observed_config: Mapping,
                  expected_config: Mapping, evidence: Mapping) -> dict:
        state = self._require(transition_id, {"SUBMITTING", "UNKNOWN"})
        if not evidence.get("independent") or not evidence.get("complete"):
            raise FenceError("reconciliation requires complete independent inspection evidence")
        outcome = evidence.get("outcome")
        matched = dict(observed_config) == dict(expected_config)
        if outcome not in {"VERIFIED", "COMMITTED"}:
            raise FenceError("post-state reconciliation must establish VERIFIED")
        if not matched:
            raise FenceError("reconciliation outcome conflicts with independently observed configuration")
        state.update(status="VERIFIED",
                     observed_config=dict(observed_config), reconciliation=dict(evidence),
                     resolved_epoch=time.time())
        state["verified_epoch"] = time.time()
        return self._write(state)

    def resolve_not_submitted(self, transition_id: str, *, observed_config: Mapping,
                              evidence: Mapping, explicit_authorization: str) -> dict:
        state = self._require(transition_id, {"SUBMITTING", "UNKNOWN"})
        if evidence.get("independent") is not True or evidence.get("complete") is not True:
            raise FenceError("pre-state resolution requires complete independent evidence")
        if evidence.get("outcome") != "NOT_SUBMITTED" or evidence.get("safe_to_retry") is not True:
            raise FenceError("pre-state evidence does not establish a safe non-submission resolution")
        if explicit_authorization != f"RESOLVE-NOT-SUBMITTED:{transition_id}":
            raise FenceError("explicit transition-specific resolution authorization is required")
        if dict(observed_config) != dict(state.get("context", {}).get("pre_config", {})):
            raise FenceError("observed state does not exactly match the recorded pre-state")
        state.update(status="NOT_SUBMITTED", observed_config=dict(observed_config),
                     reconciliation={**dict(evidence), "explicit_authorization": explicit_authorization},
                     resolved_epoch=time.time())
        return self._write(state)

    def record_unresolved_observation(self, transition_id: str, evidence: Mapping) -> dict:
        """Durably attach fresh independent observations without clearing the fence."""
        state = self._require(transition_id, {"PREPARED", "SUBMITTING", "UNKNOWN"})
        if not isinstance(evidence, Mapping) or not evidence:
            raise FenceError("unresolved observation evidence must be a non-empty mapping")
        observations = list(state.get("observations", []))
        observations.append(dict(evidence))
        state.update(observations=observations, last_observation_epoch=time.time())
        return self._write(state)

    def _require(self, transition_id: str, statuses: set[str]) -> dict:
        state = self.read()
        if state is None or state["transition_id"] != transition_id or state["status"] not in statuses:
            raise FenceError("transition ID or state does not permit this operation")
        return state
