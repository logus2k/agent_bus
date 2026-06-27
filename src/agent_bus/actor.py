"""BaseActor — the consume → guard → handle → emit → ack loop.

Design notes (architecture §4, §5, §8):

* Each actor type owns one **consumer group**; it polls the active-stream Set,
  attaches (group at id ``0``) to any stream it hasn't seen, then issues one
  non-blocking ``XREADGROUP`` across all attached streams per cycle.
* Every read entry is **acked** by this group, whether or not the actor reacts
  to it — a delivery sits in *this* group's PEL regardless of event type.
* ``handle()`` is an **async generator yielding 0..N OutEvents**, so a real
  streaming brain call (one bus event per LLM delta) maps directly; the echo
  actors just yield one.
* The **Termination Guard** runs before handling. Idempotency: this actor
  dedupes on ``(cid, sid)`` so a reclaimed redelivery isn't handled twice.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from dataclasses import dataclass
from typing import AsyncIterator, Optional, Any, Mapping

from .bus import Delivery, EventBus
from .config import Settings
from .discovery import Discovery
from .envelope import EventEnvelope, EventType, new_event
from .registry import Guard, WorkflowRegistry


@dataclass
class OutEvent:
    """A draft output from an actor; BaseActor stamps sid + plumbing and publishes."""

    event_type: str
    data: Mapping[str, Any]
    context: Optional[Mapping[str, Any]] = None


class BaseActor:
    #: event types this actor reacts to (others are read+acked but ignored).
    subscribes_to: frozenset[str] = frozenset()

    def __init__(
        self,
        name: str,
        group: str,
        bus: EventBus,
        registry: WorkflowRegistry,
        discovery: Discovery,
        settings: Settings,
    ):
        self.name = name
        self.group = group
        self._bus = bus
        self._registry = registry
        self._discovery = discovery
        self._settings = settings
        self._consumer = f"{name}-{socket.gethostname()}-{os.getpid()}"
        self._attached: set[str] = set()
        self._seen: set[tuple[str, int]] = set()
        self._running = False
        # Serializes _process between the run loop and the reaper's reclaim,
        # so they never interleave on _seen / acks for the same actor.
        self._lock = asyncio.Lock()
        self._log = logging.getLogger(f"agent_bus.actor.{name}")

    # --- subclasses override this ---

    async def handle(self, env: EventEnvelope) -> AsyncIterator[OutEvent]:
        """React to a subscribed event, yielding 0..N OutEvents. Default: nothing."""
        return
        yield  # pragma: no cover - makes this an async generator

    # --- lifecycle ---

    async def run(self) -> None:
        self._running = True
        self._log.info("actor '%s' started (group=%s)", self.name, self.group)
        poll_s = self._settings.actor_poll_ms / 1000.0
        try:
            while self._running:
                await self._sync_attachments()
                if self._attached:
                    await self._drain_once()
                await asyncio.sleep(poll_s)
        except asyncio.CancelledError:  # graceful shutdown
            raise
        finally:
            self._log.info("actor '%s' stopped", self.name)

    def stop(self) -> None:
        self._running = False

    # --- internals ---

    async def _sync_attachments(self) -> None:
        active = await self._discovery.active_stream_keys()
        for stream in active - self._attached:
            await self._bus.ensure_group(stream, self.group, start="0")
            self._attached.add(stream)
            self._log.debug("attached to %s", stream)
        # Drop streams that were cleaned up so we don't read a dead group.
        self._attached &= active

    async def _drain_once(self) -> None:
        try:
            deliveries = await self._bus.read_group(
                self._attached, self.group, self._consumer
            )
        except Exception as exc:  # noqa: BLE001 - a stream may vanish mid-read
            self._log.debug("read_group reconcile: %s", exc)
            return
        for delivery in deliveries:
            await self._process(delivery)

    async def reclaim_idle(self) -> None:
        """Reclaim this group's entries abandoned by dead consumers (the reaper
        path). XAUTOCLAIM transfers idle PEL entries to *this* live consumer,
        then they run through the normal handler. Reprocessing is idempotent
        (acks are no-ops; (cid, sid) dedupe skips re-handling)."""
        min_idle = self._settings.reaper_min_idle_ms
        for stream in list(self._attached):
            try:
                _cursor, deliveries = await self._bus.reclaim(
                    stream, self.group, self._consumer, min_idle
                )
            except Exception as exc:  # noqa: BLE001 - stream may have vanished
                self._log.debug("reclaim skip %s: %s", stream, exc)
                continue
            for delivery in deliveries:
                self._log.info(
                    "reclaimed idle entry %s on %s", delivery.entry_id, stream
                )
                await self._process(delivery)

    async def _process(self, delivery: Delivery) -> None:
        async with self._lock:
            await self._process_locked(delivery)

    async def _process_locked(self, delivery: Delivery) -> None:
        env = delivery.envelope
        decision = await self._registry.guard(env)

        if decision is Guard.DROP:
            await self._bus.ack(delivery.stream, self.group, [delivery.entry_id])
            return

        if decision is Guard.TERMINATE_NOW:
            self._log.info(
                "workflow %s hit threshold at sid=%s; terminating",
                env.header.cid,
                env.header.sid,
            )
            await self._emit(
                env,
                EventType.WORKFLOW_TERMINATED,
                {"reason": f"sid>={self._settings.max_threshold}"},
            )
            await self._bus.ack(delivery.stream, self.group, [delivery.entry_id])
            return

        # PROCEED — react only to subscribed types, once per (cid, sid).
        key = (env.header.cid, env.header.sid)
        if env.header.event_type in self.subscribes_to and key not in self._seen:
            self._seen.add(key)
            async for out in self.handle(env):
                await self._emit(env, out.event_type, out.data, out.context)

        await self._bus.ack(delivery.stream, self.group, [delivery.entry_id])

    async def _emit(
        self,
        src: EventEnvelope,
        event_type: str,
        data: Mapping[str, Any],
        context: Optional[Mapping[str, Any]] = None,
    ) -> None:
        cid = src.header.cid
        # A voluntary terminal event flips workflow state so any stragglers drop.
        if event_type == EventType.WORKFLOW_TERMINATED:
            await self._registry.set_terminated(cid)
        sid = await self._registry.next_sid(cid)
        out = new_event(
            stream_id=src.header.stream_id,
            cid=cid,
            sid=sid,
            sender=self.name,
            event_type=event_type,
            data=data,
            context=context,
            trace_parent=src.metadata.trace_parent,
        )
        await self._bus.publish(self._settings.stream_key(src.header.stream_id), out)
