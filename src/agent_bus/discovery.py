"""Discovery — how actors find the dynamically-created initiator streams.

First-slice mechanism: a single ``streams:active`` Set is the registry of
live initiator ids. The initiator registers itself there and publishes its
opening event onto its own dedicated stream; actors poll the Set each cycle,
``ensure_group`` on any stream they haven't attached to yet (creating the
group at id ``0`` so backlog added before attach is still delivered), and
read across all of them.

Why a polled Set and not the event-driven ``stream:control`` from §4 of the
architecture: with glide's multiplexed connection we read non-blocking and
rebuild the stream set per cycle, so a Set membership check is all that's
needed. An event-driven control stream (tail-to-attach, no polling) is the
documented evolution once volume warrants it.
"""

from __future__ import annotations

import logging

from .bus import EventBus
from .config import Settings

log = logging.getLogger("agent_bus.discovery")


class Discovery:
    def __init__(self, bus: EventBus, settings: Settings):
        self._bus = bus
        self._settings = settings

    async def register(self, initiator_id: str) -> None:
        """Mark an initiator's stream as live so actors begin consuming it."""
        await self._bus.sadd(self._settings.active_streams_key, initiator_id)
        log.info("registered initiator stream: %s", initiator_id)

    async def unregister(self, initiator_id: str) -> None:
        """Remove an initiator from the live set (on disconnect/cleanup)."""
        await self._bus.srem(self._settings.active_streams_key, initiator_id)
        log.info("unregistered initiator stream: %s", initiator_id)

    async def active_ids(self) -> set[str]:
        return await self._bus.smembers(self._settings.active_streams_key)

    async def active_stream_keys(self) -> set[str]:
        return {self._settings.stream_key(i) for i in await self.active_ids()}
