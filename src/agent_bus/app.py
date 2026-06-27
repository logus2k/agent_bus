"""Mono-process entrypoint — wires the backbone and runs it.

One shared EventBus (glide multiplexes a single connection), the echo actors
and the reaper as asyncio tasks, and the Socket.IO gateway under uvicorn.
A clean shutdown cancels the tasks and closes the client.
"""

from __future__ import annotations

import asyncio
import logging

import uvicorn

from .actors import EchoAgent, EchoTool
from .bus import EventBus
from .cleanup import StreamCleaner
from .config import settings
from .discovery import Discovery
from .gateway import Gateway
from .reaper import Reaper
from .registry import WorkflowRegistry

log = logging.getLogger("agent_bus.app")


async def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    bus = await EventBus.create(settings)
    registry = WorkflowRegistry(bus, settings)
    discovery = Discovery(bus, settings)
    cleaner = StreamCleaner(bus, discovery, settings)

    agent = EchoAgent("echo_agent", "cg:agent", bus, registry, discovery, settings)
    tool = EchoTool("echo_tool", "cg:tool", bus, registry, discovery, settings)
    reaper = Reaper([agent, tool], settings)
    gateway = Gateway(bus, registry, discovery, cleaner, settings)

    tasks = [
        asyncio.create_task(agent.run(), name="echo_agent"),
        asyncio.create_task(tool.run(), name="echo_tool"),
        asyncio.create_task(reaper.run(), name="reaper"),
    ]

    config = uvicorn.Config(
        gateway.asgi,
        host=settings.gateway_host,
        port=settings.gateway_port,
        log_level=settings.log_level.lower(),
    )
    server = uvicorn.Server(config)
    log.info(
        "agent_bus up — gateway on %s:%s", settings.gateway_host, settings.gateway_port
    )
    try:
        await server.serve()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await bus.close()
        log.info("agent_bus shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())
