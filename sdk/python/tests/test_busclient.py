"""Integration test for the glide BusClient — the consumer/producer surface
agent_runtime imports. Needs a live Valkey on VALKEY_HOST:VALKEY_PORT
(default 127.0.0.1:6379); skips otherwise. Uses unique stream/group names so it
never collides with a running app's actors.

    pip install -e 'sdk/python[bus]'
    pytest sdk/python/tests/test_busclient.py
"""
import asyncio
import os
import uuid

import pytest

pytest.importorskip("glide", reason="valkey-glide not installed ([bus] extra)")

from agent_bus_client import EventEnvelope, new_event  # noqa: E402
from agent_bus_client.bus import BusClient, Delivery, make_consumer  # noqa: E402

HOST = os.getenv("VALKEY_HOST", "127.0.0.1")
PORT = int(os.getenv("VALKEY_PORT", "6379"))


async def _bus_or_skip() -> BusClient:
    try:
        return await BusClient.create(HOST, PORT)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Valkey at {HOST}:{PORT}: {exc}")


async def test_busclient_full_surface():
    bus = await _bus_or_skip()
    tag = uuid.uuid4().hex[:8]
    stream = bus.stream_key(f"it-{tag}")
    group = f"cg:it-{tag}"

    # ensure_group idempotent (BUSYGROUP-safe)
    await bus.ensure_group(stream, group, start="0")
    await bus.ensure_group(stream, group, start="0")

    # publish + envelope fidelity through the wire
    env = new_event(stream_id=f"it-{tag}", cid="wf-1", sid=1, sender="tester",
                    event_type="schedule.fired", data={"agent": "news"})
    entry_id = await bus.publish(stream, env)
    assert isinstance(entry_id, str)

    deliveries = await bus.read_group([stream], group, make_consumer("farm"), count=10)
    assert len(deliveries) == 1
    d = deliveries[0]
    assert isinstance(d, Delivery) and isinstance(d.envelope, EventEnvelope)
    assert d.envelope.header.cid == "wf-1" and d.envelope.payload.data == {"agent": "news"}
    assert await bus.ack(stream, group, [d.entry_id]) == 1

    # competing consumers load-balance (no overlap, full coverage)
    for i in range(10):
        await bus.publish(stream, new_event(stream_id=f"it-{tag}", cid=f"wf-{i}", sid=1,
                          sender="t", event_type="x", data={"n": i}))
    cA, cB = make_consumer("A"), make_consumer("B")
    idsA = {x.entry_id for x in await bus.read_group([stream], group, cA, count=5)}
    idsB = {x.entry_id for x in await bus.read_group([stream], group, cB, count=5)}
    assert idsA.isdisjoint(idsB) and len(idsA | idsB) == 10
    await bus.ack(stream, group, list(idsA))

    # reclaim (XAUTOCLAIM) redelivers B's unacked entries (at-least-once)
    await asyncio.sleep(0.25)
    _cursor, claimed = await bus.reclaim(stream, group, make_consumer("C"), min_idle_ms=200)
    claimed_ids = {x.entry_id for x in claimed}
    assert idsB.issubset(claimed_ids)
    await bus.ack(stream, group, list(claimed_ids))

    # poison entry -> DLQ + acked (never returned)
    dlq_before = await bus.stream_len("stream:dlq")
    await bus.client.custom_command(["XADD", stream, "*", "garbage", "x"])
    assert await bus.read_group([stream], group, cA, count=10) == []
    assert await bus.stream_len("stream:dlq") == dlq_before + 1

    # observe replays everything; block_ms returns promptly when empty
    _c, envs = await bus.observe(stream, "0", count=100)
    assert len(envs) == 11 and all(isinstance(e, EventEnvelope) for e in envs)
    assert await bus.read_group([stream], group, cA, count=5, block_ms=300) == []

    await bus.client.delete([stream])
    await bus.close()
