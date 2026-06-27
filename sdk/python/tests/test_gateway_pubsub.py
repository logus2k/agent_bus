"""Integration test for the gateway publish/subscribe surface across two clients.
Needs the running gateway at AGENT_BUS_URL (default http://127.0.0.1:6815); skips
otherwise.

    pytest sdk/python/tests/test_gateway_pubsub.py
"""
import asyncio
import os
import uuid

import pytest

from agent_bus_client import AgentBusClient

URL = os.getenv("AGENT_BUS_URL", "http://127.0.0.1:6815")


async def _client_or_skip() -> AgentBusClient:
    try:
        return await AgentBusClient(URL).connect(timeout=5)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no gateway at {URL}: {exc}")


async def test_publish_subscribe_two_clients():
    feed = f"feed-{uuid.uuid4().hex[:8]}"
    a = await _client_or_skip()
    b = await AgentBusClient(URL).connect(timeout=5)

    received = []
    sub = await a.subscribe(feed)
    task = asyncio.create_task(_drain(sub, received))
    await asyncio.sleep(0.2)

    for i in range(3):
        ack = await b.publish(feed, "feed.item", {"n": i})
        assert ack.get("ok") and ack.get("entry_id")

    for _ in range(50):
        if len(received) >= 3:
            break
        await asyncio.sleep(0.05)

    assert len(received) == 3
    assert [e.data.get("n") for e in received] == [0, 1, 2]
    assert all(e.stream_id == feed for e in received)
    assert all(e.sender != a.stream_id for e in received)  # events it did not produce

    # unsubscribe stops delivery
    await sub.unsubscribe()
    await asyncio.sleep(0.1)
    before = len(received)
    await b.publish(feed, "feed.item", {"n": 99})
    await asyncio.sleep(0.4)
    assert len(received) == before

    task.cancel()
    await a.disconnect()
    await b.disconnect()


async def _drain(sub, out):
    async for ev in sub:
        out.append(ev)
