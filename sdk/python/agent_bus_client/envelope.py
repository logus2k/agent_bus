"""The event envelope — the canonical bus contract, exported by this SDK.

This is the **shared model** every bus participant should import (gateway,
actors, scheduler, agent_runtime) instead of vendoring its own copy. It is pure
Pydantic (no glide), so importing it never requires the Valkey client.

Wire format: an envelope travels as a single stream field ``data`` holding the
JSON document. ``to_fields()`` / ``from_fields()`` bridge to glide's
``List[Tuple[field, value]]`` xadd shape and its bytes-keyed read results.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from pydantic import BaseModel, Field

SCHEMA_VERSION = "1.0"

# The single stream field under which the JSON envelope is stored.
WIRE_FIELD = "data"


class EventType:
    """Event taxonomy (string constants, not an enum, to stay open/extensible)."""

    REQUEST = "request"
    AGENT_THOUGHT = "agent.thought"
    TOOL_EXEC = "tool.exec"
    TOOL_RESULT = "tool.result"
    WORKFLOW_TERMINATED = "workflow.terminated"


class Header(BaseModel):
    stream_id: str
    cid: str
    sid: int
    timestamp: str
    sender: str
    event_type: str


class Payload(BaseModel):
    data: dict[str, Any] = Field(default_factory=dict)
    context: Optional[dict[str, Any]] = None


class Metadata(BaseModel):
    version: str = SCHEMA_VERSION
    trace_parent: Optional[str] = None


class EventEnvelope(BaseModel):
    header: Header
    payload: Payload = Field(default_factory=Payload)
    metadata: Metadata = Field(default_factory=Metadata)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str | bytes) -> "EventEnvelope":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.model_validate_json(raw)

    def to_fields(self) -> list[tuple[str, str]]:
        """Glide xadd ``values`` shape: a single (field, json) pair."""
        return [(WIRE_FIELD, self.to_json())]

    @classmethod
    def from_fields(cls, fields: list[list[bytes]]) -> "EventEnvelope":
        """Parse glide's read result for one entry: ``[[field, value], ...]``."""
        for pair in fields:
            key = pair[0].decode("utf-8") if isinstance(pair[0], bytes) else pair[0]
            if key == WIRE_FIELD:
                return cls.from_json(pair[1])
        raise ValueError(f"stream entry missing '{WIRE_FIELD}' field: {fields!r}")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_event(
    *,
    stream_id: str,
    cid: str,
    sid: int,
    sender: str,
    event_type: str,
    data: Optional[Mapping[str, Any]] = None,
    context: Optional[Mapping[str, Any]] = None,
    trace_parent: Optional[str] = None,
    timestamp: Optional[str] = None,
) -> EventEnvelope:
    """Construct a well-formed envelope with sensible defaults."""
    return EventEnvelope(
        header=Header(
            stream_id=stream_id,
            cid=cid,
            sid=sid,
            timestamp=timestamp or now_iso(),
            sender=sender,
            event_type=event_type,
        ),
        payload=Payload(
            data=dict(data or {}),
            context=dict(context) if context is not None else None,
        ),
        metadata=Metadata(trace_parent=trace_parent),
    )
