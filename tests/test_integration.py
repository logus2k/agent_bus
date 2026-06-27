"""End-to-end choreography against a live Valkey.

Covers the no-auto-cap model: finite natural termination, a forever loop that
runs until explicitly terminated, the optional cap backstop, and reaper
reclaim. Skips itself if it cannot reach Valkey.

These tests need **exclusive** access to the bus — stop the app container first
so its actors (same consumer groups) don't compete:

    docker stop agent-bus-app
    VALKEY_HOST=127.0.0.1 pytest -m integration
"""

import asyncio
from dataclasses import replace

import pytest

from agent_bus.actors import EchoAgent, EchoTool
from agent_bus.bus import EventBus
from agent_bus.config import settings as base_settings
from agent_bus.discovery import Discovery
from agent_bus.envelope import EventType, new_event
from agent_bus.reaper import Reaper
from agent_bus.registry import WorkflowRegistry

pytestmark = pytest.mark.integration

BASE = replace(base_settings, actor_poll_ms=30, reaper_min_idle_ms=200,
               reaper_interval_s=1)


async def _bus_or_skip(settings):
    try:
        bus = await EventBus.create(settings)
        await bus.client.flushall()
        return bus
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no live Valkey: {exc}")


async def _pending(bus, stream, group):
    res = await bus.client.custom_command(["XPENDING", stream, group])
    return int(res[0]) if res else 0


async def _start(bus, S, initiator, cid, text="go"):
    reg = WorkflowRegistry(bus, S)
    disc = Discovery(bus, S)
    agent = EchoAgent("echo_agent", "cg:agent", bus, reg, disc, S)
    tool = EchoTool("echo_tool", "cg:tool", bus, reg, disc, S)
    tasks = [asyncio.create_task(agent.run()), asyncio.create_task(tool.run())]
    await disc.register(initiator)
    seq = await reg.next_sid(cid)
    await bus.publish(S.stream_key(initiator),
                      new_event(stream_id=initiator, cid=cid, sid=seq, sender="test",
                                event_type=EventType.REQUEST, data={"text": text}))
    return reg, tasks


async def _stop(tasks):
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def test_finite_run_terminates_naturally_without_cap():
    """Default model: no cap, echo not looping -> ends on the agent's terminated."""
    S = replace(BASE, max_threshold=0, echo_loop=False)
    bus = await _bus_or_skip(S)
    _reg, tasks = await _start(bus, S, "client-fin", "wf-fin")

    stream, last, types = S.stream_key("client-fin"), "0", []
    for _ in range(200):
        last, envs = await bus.observe(stream, last)
        types += [e.header.event_type for e in envs]
        if EventType.WORKFLOW_TERMINATED in types:
            break
        await asyncio.sleep(0.02)
    await asyncio.sleep(0.3)
    await _stop(tasks)

    assert types == [EventType.REQUEST, EventType.AGENT_THOUGHT,
                     EventType.TOOL_RESULT, EventType.WORKFLOW_TERMINATED]
    assert await _pending(bus, stream, "cg:agent") == 0
    assert await _pending(bus, stream, "cg:tool") == 0
    await bus.close()


async def test_forever_loop_runs_until_terminated():
    """ECHO_LOOP with no cap runs indefinitely; explicit terminate stops it."""
    S = replace(BASE, max_threshold=0, echo_loop=True)
    bus = await _bus_or_skip(S)
    reg, tasks = await _start(bus, S, "client-loop", "wf-loop")

    # Let it climb well past where a finite run would have stopped (sid 4).
    for _ in range(200):
        if await reg.current_sid("wf-loop") >= 12:
            break
        await asyncio.sleep(0.02)
    climbed = await reg.current_sid("wf-loop")
    assert climbed >= 12, f"loop did not run (sid={climbed})"

    # Eliminate the outlier (what the gateway 'terminate' command does).
    await reg.set_terminated("wf-loop")
    await asyncio.sleep(0.4)
    stopped_at = await reg.current_sid("wf-loop")
    await asyncio.sleep(0.4)
    assert await reg.current_sid("wf-loop") == stopped_at  # loop halted
    await _stop(tasks)
    await bus.close()


async def test_cap_backstop_terminates_when_enabled():
    """A positive MAX_THRESHOLD still force-terminates a runaway loop."""
    S = replace(BASE, max_threshold=6, echo_loop=True)
    bus = await _bus_or_skip(S)
    reg, tasks = await _start(bus, S, "client-cap", "wf-cap")

    stream, last, types = S.stream_key("client-cap"), "0", []
    for _ in range(200):
        last, envs = await bus.observe(stream, last)
        types += [e.header.event_type for e in envs]
        if EventType.WORKFLOW_TERMINATED in types:
            break
        await asyncio.sleep(0.02)
    await asyncio.sleep(0.3)
    await _stop(tasks)

    assert types[-1] == EventType.WORKFLOW_TERMINATED
    assert types.count(EventType.WORKFLOW_TERMINATED) == 1
    await bus.close()


async def test_reaper_reclaims_abandoned_entry():
    S = replace(BASE, max_threshold=0)
    bus = await _bus_or_skip(S)
    reg = WorkflowRegistry(bus, S)
    disc = Discovery(bus, S)
    from glide import StreamReadGroupOptions

    stream, group = S.stream_key("client-r2"), "cg:tool"
    await bus.ensure_group(stream, group, start="0")
    await disc.register("client-r2")
    seq = await reg.next_sid("wf-r2")
    await bus.publish(stream,
                      new_event(stream_id="client-r2", cid="wf-r2", sid=seq, sender="test",
                                event_type=EventType.AGENT_THOUGHT, data={"text": "stuck"}))
    await bus.client.xreadgroup({stream: ">"}, group, "dead",
                                StreamReadGroupOptions(count=10))
    assert await _pending(bus, stream, group) == 1

    tool = EchoTool("echo_tool", group, bus, reg, disc, S)
    reaper = Reaper([tool], S)
    tasks = [asyncio.create_task(tool.run()), asyncio.create_task(reaper.run())]
    await asyncio.sleep(2.0)
    await _stop(tasks)

    assert await _pending(bus, stream, group) == 0
    await bus.close()
