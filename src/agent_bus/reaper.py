"""Reaper — background crash-recovery via XAUTOCLAIM.

AOF recovers the *stream*; entries stuck in a dead consumer's Pending Entries
List are recovered here. On each tick the reaper asks every actor to reclaim
its group's idle entries (older than ``REAPER_MIN_IDLE_MS``) to a live
consumer and reprocess them. Valkey tracks idle time per message, so the
threshold is enforced per entry for free.

Reclaim routing lives on the actor (``reclaim_idle``) so the reaper stays
handler-agnostic and reuses the exact guard/handle/ack path.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Sequence

from .actor import BaseActor
from .config import Settings

log = logging.getLogger("agent_bus.reaper")


class Reaper:
    def __init__(self, actors: Sequence[BaseActor], settings: Settings):
        self._actors = actors
        self._settings = settings
        self._running = False

    async def run(self) -> None:
        self._running = True
        interval = self._settings.reaper_interval_s
        log.info(
            "reaper started (interval=%ss, min_idle=%sms)",
            interval,
            self._settings.reaper_min_idle_ms,
        )
        try:
            while self._running:
                await asyncio.sleep(interval)
                for actor in self._actors:
                    try:
                        await actor.reclaim_idle()
                    except Exception as exc:  # noqa: BLE001 - never let the loop die
                        log.warning("reaper error on %s: %s", actor.name, exc)
        except asyncio.CancelledError:
            raise
        finally:
            log.info("reaper stopped")

    def stop(self) -> None:
        self._running = False
