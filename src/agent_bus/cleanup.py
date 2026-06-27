"""Stream cleanup — initiator-scoped lifecycle (architecture §5).

A workflow ending (`workflow.terminated`) is only a state flip; the initiator's
stream lives on for its other/future workflows. The stream itself is removed
when the initiator disconnects/shuts down, with an idle-TTL safety net for
initiators that vanish without a clean disconnect.
"""

from __future__ import annotations

import logging

from .bus import EventBus
from .config import Settings
from .discovery import Discovery

log = logging.getLogger("agent_bus.cleanup")


class StreamCleaner:
    def __init__(self, bus: EventBus, discovery: Discovery, settings: Settings):
        self._bus = bus
        self._discovery = discovery
        self._settings = settings

    async def touch(self, initiator_id: str) -> None:
        """Refresh the idle-TTL safety net on an initiator's stream."""
        key = self._settings.stream_key(initiator_id)
        await self._bus.expire(key, self._settings.stream_ttl_s)

    async def close(self, initiator_id: str) -> None:
        """Clean disconnect: drop from the active set and delete the stream."""
        await self._discovery.unregister(initiator_id)
        await self._bus.delete_stream(self._settings.stream_key(initiator_id))
        log.info("cleaned up initiator stream: %s", initiator_id)
