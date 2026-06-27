"""Envelope unit tests — no Valkey needed."""

from agent_bus.envelope import EventEnvelope, EventType, new_event


def test_json_roundtrip():
    e = new_event(
        stream_id="client-1", cid="wf-1", sid=3, sender="echo_agent",
        event_type=EventType.AGENT_THOUGHT, data={"text": "hi"}, trace_parent="tp-1",
    )
    assert EventEnvelope.from_json(e.to_json()) == e


def test_fields_roundtrip_from_bytes():
    """from_fields must handle glide's bytes-keyed entries."""
    e = new_event(
        stream_id="c", cid="wf", sid=1, sender="s", event_type=EventType.REQUEST,
        data={"text": "x"},
    )
    wire = [[f.encode(), v.encode()] for (f, v) in e.to_fields()]
    assert EventEnvelope.from_fields(wire) == e


def test_missing_wire_field_raises():
    import pytest

    with pytest.raises(ValueError):
        EventEnvelope.from_fields([[b"other", b"{}"]])


def test_defaults():
    e = new_event(stream_id="c", cid="wf", sid=0, sender="s",
                  event_type=EventType.REQUEST)
    assert e.payload.data == {}
    assert e.payload.context is None
    assert e.metadata.version == "1.0"
    assert e.header.timestamp  # auto-stamped
