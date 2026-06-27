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
"""

from .client import AgentBusClient
from .workflow import Event, Workflow

__version__ = "0.1.0"
__all__ = ["AgentBusClient", "Workflow", "Event"]
