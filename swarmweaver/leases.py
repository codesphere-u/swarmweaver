"""
FileScopeLeaseManager — Runtime pessimistic/optimistic lock manager for parallel subagents.
"""

from __future__ import annotations

import hashlib
import secrets
import threading
import time
from collections.abc import Iterable

from swarmweaver.dag import normalize_scope_pattern, scopes_overlap
from swarmweaver.models import LeaseConflict, LeaseGrant, LeaseMode


class LeaseAcquisitionError(RuntimeError):
    """Raised when a task cannot acquire a requested file-scope lease due to an active conflict."""

    def __init__(self, conflict: LeaseConflict) -> None:
        self.conflict = conflict
        super().__init__(
            f"Lease conflict prevented: task {conflict.requesting_task_id!r} requested "
            f"{conflict.requested_mode} on {conflict.requested_scope!r}, which conflicts with "
            f"task {conflict.holding_task_id!r} holding {conflict.held_mode} on {conflict.held_scope!r}."
        )


class FileScopeLeaseManager:
    """
    Thread-safe runtime lock manager enforcing SHARED_READ and EXCLUSIVE_WRITE leases
    on normalized file paths, directory prefixes, glob patterns, and AST symbol scopes.

    Features:
    - Deadlock-free canonical sorted scope acquisition order
    - TTL-based automatic expiration and garbage collection
    - Cryptographic SHA-256 lease tokens
    - Full audit log of prevented race conditions (`LeaseConflict`)
    """

    def __init__(self, default_ttl_ms: int = 30_000) -> None:
        if default_ttl_ms <= 0:
            raise ValueError("default_ttl_ms must be > 0")
        self._default_ttl_ns = int(default_ttl_ms * 1_000_000)
        self._lock = threading.RLock()
        self._active_grants: dict[str, LeaseGrant] = {}  # lease_token -> LeaseGrant
        self._task_tokens: dict[str, str] = {}  # task_id -> lease_token
        self._conflicts_log: list[LeaseConflict] = []
        self._secret_salt = secrets.token_hex(16)

    @staticmethod
    def _canonicalize_scopes(scopes: Iterable[str]) -> tuple[str, ...]:
        """Normalizes and sorts scopes deterministically to guarantee deadlock-free ordering."""
        normalized: set[str] = set()
        for raw in scopes:
            if not raw or not raw.strip():
                continue
            file_part, sym_part = normalize_scope_pattern(raw)
            if sym_part == "*":
                normalized.add(file_part)
            else:
                normalized.add(f"{file_part}::{sym_part}")
        return tuple(sorted(normalized))

    def _evict_expired_unlocked(self, now_ns: int) -> int:
        expired_tokens = [
            tok for tok, grant in self._active_grants.items() if grant.is_expired(now_ns)
        ]
        for tok in expired_tokens:
            grant = self._active_grants.pop(tok)
            if self._task_tokens.get(grant.task_id) == tok:
                self._task_tokens.pop(grant.task_id, None)
        return len(expired_tokens)

    def evict_expired(self, now_ns: int | None = None) -> int:
        """Purges expired leases and returns the number of evicted grants."""
        ts = time.monotonic_ns() if now_ns is None else now_ns
        with self._lock:
            return self._evict_expired_unlocked(ts)

    def check_conflict(
        self,
        task_id: str,
        read_scopes: Iterable[str] = (),
        write_scopes: Iterable[str] = (),
        *,
        now_ns: int | None = None,
    ) -> LeaseConflict | None:
        """
        Checks whether acquiring `read_scopes` and `write_scopes` for `task_id` would conflict
        with any currently held lease. Does not mutate state except evicting expired leases.
        """
        ts = time.monotonic_ns() if now_ns is None else now_ns
        r_scopes = self._canonicalize_scopes(read_scopes)
        w_scopes = self._canonicalize_scopes(write_scopes)

        with self._lock:
            self._evict_expired_unlocked(ts)
            return self._detect_conflict_unlocked(task_id, r_scopes, w_scopes, ts)

    def _detect_conflict_unlocked(
        self,
        task_id: str,
        r_scopes: tuple[str, ...],
        w_scopes: tuple[str, ...],
        now_ns: int,
    ) -> LeaseConflict | None:
        for held in self._active_grants.values():
            if held.task_id == task_id:
                continue

            # 1. Requested EXCLUSIVE_WRITE conflicts with held EXCLUSIVE_WRITE or SHARED_READ
            for req_w in w_scopes:
                for held_w in held.write_scopes:
                    if scopes_overlap(req_w, held_w):
                        return LeaseConflict(
                            requesting_task_id=task_id,
                            holding_task_id=held.task_id,
                            requested_scope=req_w,
                            requested_mode=LeaseMode.EXCLUSIVE_WRITE.value,
                            held_scope=held_w,
                            held_mode=LeaseMode.EXCLUSIVE_WRITE.value,
                            detected_at_ns=now_ns,
                        )
                for held_r in held.read_scopes:
                    if scopes_overlap(req_w, held_r):
                        return LeaseConflict(
                            requesting_task_id=task_id,
                            holding_task_id=held.task_id,
                            requested_scope=req_w,
                            requested_mode=LeaseMode.EXCLUSIVE_WRITE.value,
                            held_scope=held_r,
                            held_mode=LeaseMode.SHARED_READ.value,
                            detected_at_ns=now_ns,
                        )

            # 2. Requested SHARED_READ conflicts with held EXCLUSIVE_WRITE
            for req_r in r_scopes:
                for held_w in held.write_scopes:
                    if scopes_overlap(req_r, held_w):
                        return LeaseConflict(
                            requesting_task_id=task_id,
                            holding_task_id=held.task_id,
                            requested_scope=req_r,
                            requested_mode=LeaseMode.SHARED_READ.value,
                            held_scope=held_w,
                            held_mode=LeaseMode.EXCLUSIVE_WRITE.value,
                            detected_at_ns=now_ns,
                        )

        return None

    def acquire(
        self,
        task_id: str,
        read_scopes: Iterable[str] = (),
        write_scopes: Iterable[str] = (),
        *,
        ttl_ms: int | None = None,
        now_ns: int | None = None,
    ) -> LeaseGrant:
        """
        Atomically acquires SHARED_READ and EXCLUSIVE_WRITE leases for `task_id` in sorted
        deadlock-free canonical order. Raises `LeaseAcquisitionError` and records a
        `LeaseConflict` audit entry if any scope collides with an active grant.
        """
        if not task_id or not task_id.strip():
            raise ValueError("task_id must be a non-empty string.")

        ts = time.monotonic_ns() if now_ns is None else now_ns
        ttl_ns = self._default_ttl_ns if ttl_ms is None else int(ttl_ms * 1_000_000)
        if ttl_ns <= 0:
            raise ValueError("ttl_ms must be > 0")

        w_scopes = self._canonicalize_scopes(write_scopes)
        # If a task both reads and writes the same scope, EXCLUSIVE_WRITE subsumes SHARED_READ
        r_scopes = tuple(
            s for s in self._canonicalize_scopes(read_scopes) if s not in w_scopes
        )

        with self._lock:
            self._evict_expired_unlocked(ts)

            conflict = self._detect_conflict_unlocked(task_id, r_scopes, w_scopes, ts)
            if conflict is not None:
                self._conflicts_log.append(conflict)
                raise LeaseAcquisitionError(conflict)

            # Replace any prior lease held by the same task_id
            old_token = self._task_tokens.pop(task_id, None)
            if old_token is not None:
                self._active_grants.pop(old_token, None)

            expires_ns = ts + ttl_ns
            token_payload = (
                f"{task_id}|{','.join(r_scopes)}|{','.join(w_scopes)}|"
                f"{ts}|{expires_ns}|{self._secret_salt}"
            )
            lease_token = hashlib.sha256(token_payload.encode("utf-8")).hexdigest()

            grant = LeaseGrant(
                lease_token=lease_token,
                task_id=task_id,
                read_scopes=r_scopes,
                write_scopes=w_scopes,
                granted_at_ns=ts,
                expires_at_ns=expires_ns,
            )
            self._active_grants[lease_token] = grant
            self._task_tokens[task_id] = lease_token
            return grant

    def release(self, lease_token: str) -> bool:
        """Releases an active lease by its cryptographic `lease_token`."""
        with self._lock:
            grant = self._active_grants.pop(lease_token, None)
            if grant is None:
                return False
            if self._task_tokens.get(grant.task_id) == lease_token:
                self._task_tokens.pop(grant.task_id, None)
            return True

    def release_task(self, task_id: str) -> bool:
        """Releases any active lease held by `task_id`."""
        with self._lock:
            tok = self._task_tokens.pop(task_id, None)
            if tok is None:
                return False
            self._active_grants.pop(tok, None)
            return True

    def renew(
        self,
        lease_token: str,
        *,
        additional_ttl_ms: int | None = None,
        now_ns: int | None = None,
    ) -> LeaseGrant:
        """Extends the TTL of an active, non-expired lease."""
        ts = time.monotonic_ns() if now_ns is None else now_ns
        ttl_ns = (
            self._default_ttl_ns
            if additional_ttl_ms is None
            else int(additional_ttl_ms * 1_000_000)
        )
        with self._lock:
            self._evict_expired_unlocked(ts)
            grant = self._active_grants.get(lease_token)
            if grant is None:
                raise KeyError(f"Lease token {lease_token!r} is not active or has expired.")
            updated = LeaseGrant(
                lease_token=grant.lease_token,
                task_id=grant.task_id,
                read_scopes=grant.read_scopes,
                write_scopes=grant.write_scopes,
                granted_at_ns=grant.granted_at_ns,
                expires_at_ns=ts + ttl_ns,
            )
            self._active_grants[lease_token] = updated
            return updated

    def validate_token(
        self,
        lease_token: str,
        *,
        required_scope: str | None = None,
        require_write: bool = False,
        now_ns: int | None = None,
    ) -> bool:
        """Verifies that `lease_token` is active and optionally covers `required_scope`."""
        ts = time.monotonic_ns() if now_ns is None else now_ns
        with self._lock:
            self._evict_expired_unlocked(ts)
            grant = self._active_grants.get(lease_token)
            if grant is None:
                return False
            if required_scope is None:
                return True
            for w in grant.write_scopes:
                if scopes_overlap(w, required_scope):
                    return True
            if not require_write:
                for r in grant.read_scopes:
                    if scopes_overlap(r, required_scope):
                        return True
            return False

    @property
    def active_grants(self) -> list[LeaseGrant]:
        with self._lock:
            self._evict_expired_unlocked(time.monotonic_ns())
            return list(self._active_grants.values())

    @property
    def conflicts_log(self) -> list[LeaseConflict]:
        with self._lock:
            return list(self._conflicts_log)
