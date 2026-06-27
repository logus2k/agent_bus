"""Agent Bus — Python client SDK.

High-level client for the Agent Bus Socket.IO gateway. Start workflows and
iterate their events without hand-rolling Socket.IO or cid-correlation::

    import asyncio
    from agent_bus_client import AgentBusClient

    async def main():
        async with AgentBusClient("http://127.0.0.1:6815") as client:
            wf = await client.start("hello")
            async for ev in wf:
                print(ev.sid, ev.type, ev.data)
            await wf.completed

    asyncio.run(main())

This wraps the *bus* protocol (envelopes + commands). It is unrelated to the
agent_server SDK, which wraps the LLM brain.

Two client surfaces:
  * :class:`AgentBusClient` — Socket.IO **gateway** client (browser + server):
    ``publish`` / ``subscribe`` / ``start`` (initiator + observer).
  * ``agent_bus_client.bus.BusClient`` — glide **direct-Valkey** client (server
    only): consumer groups (``read_group``/``ack``/``reclaim``) + ``publish``.
    Import explicitly: ``from agent_bus_client.bus import BusClient`` (needs the
    ``valkey-glide`` extra). Not imported here so the package loads without glide.

The canonical :class:`EventEnvelope` is exported here for every participant to
import instead of vendoring a copy.
"""

from .client import AgentBusClient
from .envelope import EventEnvelope, EventType, new_event
from .workflow import Event, Subscription, Workflow

__version__ = "0.2.0"
__all__ = [
    "AgentBusClient",
    "Workflow",
    "Subscription",
    "Event",
    "EventEnvelope",
    "EventType",
    "new_event",
]
