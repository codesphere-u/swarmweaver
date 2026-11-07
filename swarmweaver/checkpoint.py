"""
CheckpointJournal & Crash-Safe Resume Engine with SHA-256 Hash Chaining.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import threading
import time
from typing import Any

from swarmweaver.models import CheckpointRecord

GENESIS_HASH = "0" * 64


class CheckpointIntegrityError(RuntimeError):
    """Raised when tampering or hash-chain corruption is detected in a CheckpointJournal."""


@dataclass(frozen=True)
class IntegrityVerificationResult:
    """
    Result of verifying a CheckpointJournal's SHA-256 hash chain.
    Behaves as a `bool` in truthiness checks and supports 2-tuple unpacking `(valid, error)`.
    """

    valid: bool
    records_checked: int
    head_hash: str
    tampered_sequence: int | None = None
    error: str | None = None

    def __bool__(self) -> bool:
        return self.valid

    def __iter__(self):
        yield self.valid
        yield self.error

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "records_checked": self.records_checked,
            "head_hash": self.head_hash,
            "tampered_sequence": self.tampered_sequence,
            "error": self.error,
        }


def compute_canonical_digest(payload: Any) -> str:
    """Computes a deterministic SHA-256 hex digest over any JSON-serializable structure."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_record_entry_hash(
    *,
    sequence: int,
    workflow_id: str,
    task_id: str,
    input_digest: str,
    output_payload: dict[str, Any],
    tokens_used: int,
    elapsed_ms: float,
    timestamp_ns: int,
    prev_hash: str,
) -> str:
    """Computes the chained SHA-256 entry_hash for a checkpoint record."""
    envelope = {
        "sequence": sequence,
        "workflow_id": workflow_id,
        "task_id": task_id,
        "input_digest": input_digest,
        "output_payload": output_payload,
        "tokens_used": tokens_used,
        "elapsed_ms": round(float(elapsed_ms), 6),
        "timestamp_ns": timestamp_ns,
        "prev_hash": prev_hash,
    }
    return compute_canonical_digest(envelope)


class CheckpointJournal:
    """
    Append-only, content-addressable checkpoint journal with SHA-256 hash chaining
    (`prev_hash -> entry_hash`). Supports both in-memory operation and atomic JSONL
    persistence on disk.
    """

    def __init__(
        self,
        journal_path: str | Path | None = None,
        *,
        workflow_id: str = "default-workflow",
    ) -> None:
        self._workflow_id = workflow_id
        self._path = Path(journal_path) if journal_path is not None else None
        self._lock = threading.RLock()
        self._records: list[CheckpointRecord] = []
        # Fast O(1) lookup index: (task_id, input_digest) -> latest CheckpointRecord
        self._lookup_index: dict[tuple[str, str], CheckpointRecord] = {}
        self._by_task: dict[str, CheckpointRecord] = {}

        if self._path is not None and self._path.exists():
            self._load_from_disk()

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    @property
    def path(self) -> Path | None:
        return self._path

    def _load_from_disk(self) -> None:
        assert self._path is not None
        self._records.clear()
        self._lookup_index.clear()
        self._by_task.clear()

        raw_text = self._path.read_text(encoding="utf-8")
        for line_no, raw_line in enumerate(raw_text.splitlines(), start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                data = json.loads(stripped)
                rec = CheckpointRecord.from_dict(data)
            except Exception as exc:
                raise CheckpointIntegrityError(
                    f"Malformed checkpoint record at line {line_no} in {self._path}: {exc}"
                ) from exc
            self._records.append(rec)
            self._lookup_index[(rec.task_id, rec.input_digest)] = rec
            self._by_task[rec.task_id] = rec
            if rec.workflow_id:
                self._workflow_id = rec.workflow_id

    def reload(self) -> None:
        """Reloads journal records from disk if backed by a file."""
        with self._lock:
            if self._path is not None and self._path.exists():
                self._load_from_disk()

    def compute_input_digest(
        self,
        task_id: str,
        input_state: dict[str, Any],
        task_config: dict[str, Any] | None = None,
    ) -> str:
        """
        Computes a content-addressable digest for a task's inputs and specification.
        """
        return compute_canonical_digest(
            {
                "task_id": task_id,
                "input_state": input_state,
                "task_config": task_config or {},
            }
        )

    def append(
        self,
        task_id: str,
        input_digest: str,
        output_payload: dict[str, Any],
        *,
        tokens_used: int = 0,
        elapsed_ms: float = 0.0,
        timestamp_ns: int | None = None,
    ) -> CheckpointRecord:
        """
        Appends a completed task's output to the hash-chained journal and persists it atomically.
        """
        with self._lock:
            seq = len(self._records)
            prev_hash = self._records[-1].entry_hash if self._records else GENESIS_HASH
            ts = time.time_ns() if timestamp_ns is None else int(timestamp_ns)
            norm_elapsed = round(float(elapsed_ms), 6)
            payload_copy = json.loads(json.dumps(output_payload, default=str))

            entry_hash = compute_record_entry_hash(
                sequence=seq,
                workflow_id=self._workflow_id,
                task_id=task_id,
                input_digest=input_digest,
                output_payload=payload_copy,
                tokens_used=int(tokens_used),
                elapsed_ms=norm_elapsed,
                timestamp_ns=ts,
                prev_hash=prev_hash,
            )

            record = CheckpointRecord(
                sequence=seq,
                workflow_id=self._workflow_id,
                task_id=task_id,
                input_digest=input_digest,
                output_payload=payload_copy,
                tokens_used=int(tokens_used),
                elapsed_ms=norm_elapsed,
                timestamp_ns=ts,
                prev_hash=prev_hash,
                entry_hash=entry_hash,
            )

            if self._path is not None:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                serialized_line = (
                    json.dumps(record.to_dict(), sort_keys=True, separators=(",", ":")) + "\n"
                )
                with self._path.open("a", encoding="utf-8") as fh:
                    fh.write(serialized_line)
                    fh.flush()

            self._records.append(record)
            self._lookup_index[(task_id, input_digest)] = record
            self._by_task[task_id] = record
            return record

    def lookup(self, task_id: str, input_digest: str | None = None) -> CheckpointRecord | None:
        """
        O(1) lookup of a checkpointed task execution matching `(task_id, input_digest)`.
        If `input_digest` is omitted, returns the latest checkpoint for `task_id`.
        """
        with self._lock:
            if input_digest is None:
                return self._by_task.get(task_id)
            return self._lookup_index.get((task_id, input_digest))

    def verify_integrity(self, *, raise_on_error: bool = False) -> IntegrityVerificationResult:
        """
        Verifies the entire SHA-256 hash chain (`GENESIS_HASH -> ... -> head_hash`)
        and recomputes every record's `entry_hash` to detect any tampering or truncation.
        """
        with self._lock:
            if self._path is not None and self._path.exists():
                self._load_from_disk()

            expected_prev = GENESIS_HASH
            for idx, rec in enumerate(self._records):
                if rec.sequence != idx:
                    err = (
                        f"Sequence discontinuity at index {idx}: "
                        f"expected sequence={idx}, found {rec.sequence}."
                    )
                    if raise_on_error:
                        raise CheckpointIntegrityError(err)
                    return IntegrityVerificationResult(
                        valid=False,
                        records_checked=idx,
                        head_hash=expected_prev,
                        tampered_sequence=idx,
                        error=err,
                    )

                if rec.prev_hash != expected_prev:
                    err = (
                        f"Broken prev_hash link at sequence {idx} (task {rec.task_id!r}): "
                        f"expected {expected_prev[:12]}..., found {rec.prev_hash[:12]}..."
                    )
                    if raise_on_error:
                        raise CheckpointIntegrityError(err)
                    return IntegrityVerificationResult(
                        valid=False,
                        records_checked=idx,
                        head_hash=expected_prev,
                        tampered_sequence=idx,
                        error=err,
                    )

                recomputed = compute_record_entry_hash(
                    sequence=rec.sequence,
                    workflow_id=rec.workflow_id,
                    task_id=rec.task_id,
                    input_digest=rec.input_digest,
                    output_payload=rec.output_payload,
                    tokens_used=rec.tokens_used,
                    elapsed_ms=rec.elapsed_ms,
                    timestamp_ns=rec.timestamp_ns,
                    prev_hash=rec.prev_hash,
                )
                if recomputed != rec.entry_hash:
                    err = (
                        f"Payload tampering detected at sequence {idx} (task {rec.task_id!r}): "
                        f"recomputed hash {recomputed[:12]}... != stored entry_hash {rec.entry_hash[:12]}..."
                    )
                    if raise_on_error:
                        raise CheckpointIntegrityError(err)
                    return IntegrityVerificationResult(
                        valid=False,
                        records_checked=idx,
                        head_hash=expected_prev,
                        tampered_sequence=idx,
                        error=err,
                    )

                expected_prev = rec.entry_hash

            return IntegrityVerificationResult(
                valid=True,
                records_checked=len(self._records),
                head_hash=expected_prev,
                tampered_sequence=None,
                error=None,
            )

    @property
    def records(self) -> list[CheckpointRecord]:
        with self._lock:
            return list(self._records)

    @property
    def head_hash(self) -> str:
        with self._lock:
            return self._records[-1].entry_hash if self._records else GENESIS_HASH

    def completed_task_ids(self) -> list[str]:
        with self._lock:
            return list(self._by_task.keys())
