"""
TypedBlackboard — Shared multi-agent state store with schema contract enforcement and lineage tracking.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from swarmweaver.checkpoint import compute_canonical_digest
from swarmweaver.models import BlackboardEntry


class BlackboardContractError(ValueError):
    """Raised when a task's input_contract or output_contract is violated on the TypedBlackboard."""


_INLINE_TYPE_MAP: dict[str, type | tuple[type, ...]] = {
    "str": str,
    "string": str,
    "int": int,
    "integer": int,
    "float": (int, float),
    "number": (int, float),
    "bool": bool,
    "boolean": bool,
    "dict": dict,
    "object": dict,
    "map": dict,
    "list": list,
    "array": list,
}


def parse_contract_spec(spec: str) -> tuple[str, str | None]:
    """
    Parses a contract specification string such as `"api_schema:dict"` or `"auth_plan"`.
    Returns `(key_name, expected_type_name | None)`.
    """
    raw = spec.strip()
    if ":" in raw:
        key_part, type_part = raw.split(":", 1)
        return key_part.strip(), type_part.strip().lower() or None
    return raw, None


class TypedBlackboard:
    """
    Thread-safe shared state store for multi-agent workflows with:
    - Schema & validator registration per key
    - Strict `input_contract` enforcement before a downstream task starts
    - Strict `output_contract` enforcement before a task completes
    - Cryptographic SHA-256 digest and version lineage (`produced_by_task`, `version`)
    """

    def __init__(self, initial_state: dict[str, Any] | None = None) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, BlackboardEntry] = {}
        self._history: list[BlackboardEntry] = []
        self._schemas: dict[str, type | tuple[type, ...] | Callable[[Any], bool]] = {}

        if initial_state:
            for k, v in initial_state.items():
                self.write(k, v, produced_by_task="__init__")

    def register_schema(
        self,
        key: str,
        validator: type | tuple[type, ...] | Callable[[Any], bool],
    ) -> None:
        """
        Registers an expected Python type (e.g. `dict`, `list`, `str`) or predicate callable
        for a blackboard key.
        """
        clean_key, _ = parse_contract_spec(key)
        with self._lock:
            self._schemas[clean_key] = validator

    def _validate_value_unlocked(
        self,
        key: str,
        value: Any,
        inline_type_name: str | None = None,
        *,
        task_id: str = "unknown",
        stage: str = "write",
    ) -> None:
        if value is None:
            raise BlackboardContractError(
                f"[{stage}] Task {task_id!r} provided None for required blackboard key {key!r}."
            )

        if inline_type_name is not None:
            expected_type = _INLINE_TYPE_MAP.get(inline_type_name)
            if expected_type is None:
                raise BlackboardContractError(
                    f"[{stage}] Unknown inline contract type {inline_type_name!r} for key {key!r}."
                )
            # Note: in Python bool is a subclass of int; guard against bool passing as int/float
            if expected_type in (int, (int, float)) and isinstance(value, bool):
                raise BlackboardContractError(
                    f"[{stage}] Task {task_id!r}: blackboard key {key!r} expected {inline_type_name}, got bool."
                )
            if not isinstance(value, expected_type):
                raise BlackboardContractError(
                    f"[{stage}] Task {task_id!r}: blackboard key {key!r} expected {inline_type_name}, "
                    f"got {type(value).__name__}."
                )

        validator = self._schemas.get(key)
        if validator is not None:
            if isinstance(validator, (type, tuple)):
                if validator in (int, (int, float)) and isinstance(value, bool):
                    raise BlackboardContractError(
                        f"[{stage}] Task {task_id!r}: blackboard key {key!r} failed type check (got bool)."
                    )
                if not isinstance(value, validator):
                    raise BlackboardContractError(
                        f"[{stage}] Task {task_id!r}: blackboard key {key!r} failed type check "
                        f"(expected {validator}, got {type(value).__name__})."
                    )
            elif callable(validator):
                try:
                    ok = bool(validator(value))
                except Exception as exc:
                    raise BlackboardContractError(
                        f"[{stage}] Task {task_id!r}: validator for key {key!r} raised {exc}."
                    ) from exc
                if not ok:
                    raise BlackboardContractError(
                        f"[{stage}] Task {task_id!r}: blackboard key {key!r} failed custom schema validator."
                    )

    def write(
        self,
        key: str,
        value: Any,
        *,
        produced_by_task: str,
    ) -> BlackboardEntry:
        """
        Writes a key-value pair to the blackboard after verifying any registered schema,
        incrementing its lineage version and recording a SHA-256 content digest.
        """
        clean_key, inline_type = parse_contract_spec(key)
        if not clean_key:
            raise ValueError("Blackboard key must be a non-empty string.")

        with self._lock:
            self._validate_value_unlocked(
                clean_key,
                value,
                inline_type,
                task_id=produced_by_task,
                stage="write",
            )
            prev = self._entries.get(clean_key)
            version = 1 if prev is None else prev.version + 1
            digest = compute_canonical_digest(value)
            entry = BlackboardEntry(
                key=clean_key,
                value=value,
                produced_by_task=produced_by_task,
                version=version,
                sha256_digest=digest,
                updated_at_ns=time.time_ns(),
            )
            self._entries[clean_key] = entry
            self._history.append(entry)
            return entry

    def write_many(
        self,
        payload: dict[str, Any],
        *,
        produced_by_task: str,
    ) -> list[BlackboardEntry]:
        """Atomically validates and writes multiple keys produced by a task."""
        with self._lock:
            # First validate all keys before mutating state
            for raw_key, val in payload.items():
                clean_key, inline_type = parse_contract_spec(raw_key)
                self._validate_value_unlocked(
                    clean_key,
                    val,
                    inline_type,
                    task_id=produced_by_task,
                    stage="write",
                )
            return [
                self.write(raw_key, val, produced_by_task=produced_by_task)
                for raw_key, val in payload.items()
            ]

    def read(self, key: str) -> Any:
        """Returns the current value for `key`, or raises `KeyError` if absent."""
        clean_key, _ = parse_contract_spec(key)
        with self._lock:
            if clean_key not in self._entries:
                raise KeyError(f"Blackboard key {clean_key!r} does not exist.")
            return self._entries[clean_key].value

    def get_entry(self, key: str) -> BlackboardEntry | None:
        clean_key, _ = parse_contract_spec(key)
        with self._lock:
            return self._entries.get(clean_key)

    def verify_input_contract(
        self,
        task_id: str,
        input_contract: list[str],
    ) -> dict[str, Any]:
        """
        Verifies that all keys required by `task_id`'s `input_contract` exist on the
        blackboard and satisfy their type/schema constraints. Returns the scoped input dict.
        """
        with self._lock:
            scoped_inputs: dict[str, Any] = {}
            for contract_spec in input_contract:
                clean_key, inline_type = parse_contract_spec(contract_spec)
                entry = self._entries.get(clean_key)
                if entry is None:
                    raise BlackboardContractError(
                        f"[input_contract] Task {task_id!r} cannot start: required key "
                        f"{clean_key!r} is missing from the blackboard."
                    )
                self._validate_value_unlocked(
                    clean_key,
                    entry.value,
                    inline_type,
                    task_id=task_id,
                    stage="input_contract",
                )
                scoped_inputs[clean_key] = entry.value
            return scoped_inputs

    def verify_output_contract(
        self,
        task_id: str,
        output_contract: list[str],
        *,
        require_produced_by_task: bool = True,
    ) -> dict[str, Any]:
        """
        Verifies that all keys declared in `task_id`'s `output_contract` exist on the
        blackboard, satisfy their type/schema constraints, and were produced by `task_id`.
        """
        with self._lock:
            verified_outputs: dict[str, Any] = {}
            for contract_spec in output_contract:
                clean_key, inline_type = parse_contract_spec(contract_spec)
                entry = self._entries.get(clean_key)
                if entry is None:
                    raise BlackboardContractError(
                        f"[output_contract] Task {task_id!r} cannot complete: required output key "
                        f"{clean_key!r} was not written to the blackboard."
                    )
                if require_produced_by_task and entry.produced_by_task != task_id:
                    raise BlackboardContractError(
                        f"[output_contract] Task {task_id!r} did not produce required output key "
                        f"{clean_key!r} (last produced by {entry.produced_by_task!r})."
                    )
                self._validate_value_unlocked(
                    clean_key,
                    entry.value,
                    inline_type,
                    task_id=task_id,
                    stage="output_contract",
                )
                verified_outputs[clean_key] = entry.value
            return verified_outputs

    def snapshot(self) -> dict[str, Any]:
        """Returns a shallow dictionary of `{key: value}` for all current blackboard entries."""
        with self._lock:
            return {k: e.value for k, e in self._entries.items()}

    def entries_snapshot(self) -> dict[str, BlackboardEntry]:
        """Returns a dictionary of `{key: BlackboardEntry}` with full lineage metadata."""
        with self._lock:
            return dict(self._entries)

    @property
    def history(self) -> list[BlackboardEntry]:
        with self._lock:
            return list(self._history)
