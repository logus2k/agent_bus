"""EventBus — the *only* module that talks to valkey-glide directly.

Everything else (actors, registry, discovery, reaper, gateway) goes through
this wrapper, so swapping the client or tuning stream semantics has a blast
radius of one file. Wraps the XADD / XREADGROUP / XACK / XAUTOCLAIM / XGROUP
boilerplate plus the small KV/Set ops the registry and cleanup need.

Reads are *non-blocking* by default: actors poll across the (dynamic) set of
active streams and rebuild that set each cycle. This avoids stalling glide's
multiplexed connection on a BLOCK and sidesteps the "new stream appeared
mid-block" problem. Per-consumer blocking connections are a later optimization.
"""

from __future__ import annotations

import logging
from typing import Iterable, NamedTuple, Optional

from glide import (
    ConditionalChange,
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
    RequestError,
    StreamAddOptions,
    StreamGroupOptions,
    StreamReadGroupOptions,
    StreamReadOptions,
)

from .config import Settings, settings as default_settings
from .envelope import EventEnvelope, WIRE_FIELD

log = logging.getLogger("agent_bus.bus")


def _s(value) -> str:
    """Decode glide's bytes results to str (pass through str/None)."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return value


class Delivery(NamedTuple):
    """One successfully-parsed stream entry, ready for an actor to handle."""

    stream: str       # stream key the entry came from
    entry_id: str     # stream entry id (for XACK)
    envelope: EventEnvelope


class EventBus:
    """Async facade over a single glide connection."""

    def __init__(self, client: GlideClient, settings: Settings):
        self._client = client
        self._settings = settings

    @property
    def client(self) -> GlideClient:
        return self._client

    @classmethod
    async def create(cls, settings: Settings = default_settings) -> "EventBus":
        config = GlideClientConfiguration(
            [NodeAddress(settings.valkey_host, settings.valkey_port)]
        )
        client = await GlideClient.create(config)
        log.info(
            "EventBus connected to %s:%s", settings.valkey_host, settings.valkey_port
        )
        return cls(client, settings)

    async def close(self) -> None:
        await self._client.close()

    # --- streams: produce ---

    async def publish(self, stream: str, env: EventEnvelope) -> str:
        """XADD an envelope; returns the generated entry id.

        Refreshes the stream's idle TTL on every publish, so an active (even
        forever-running) stream is never reaped while a quiet one still expires.
        """
        entry_id = await self._client.xadd(
            stream, env.to_fields(), StreamAddOptions(make_stream=True)
        )
        await self._client.expire(stream, self._settings.stream_ttl_s)
        return _s(entry_id)

    # --- streams: consumer groups ---

    async def ensure_group(self, stream: str, group: str, start: str = "0") -> None:
        """Create the consumer group (and the stream, via MKSTREAM) idempotently."""
        try:
            await self._client.xgroup_create(
                stream, group, start, StreamGroupOptions(make_stream=True)
            )
        except RequestError as exc:
            # Group already exists — the normal steady state, not an error.
            if "BUSYGROUP" in str(exc):
                return
            raise

    async def read_group(
        self,
        streams: Iterable[str],
        group: str,
        consumer: str,
        count: int = 32,
        block_ms: Optional[int] = None,
    ) -> list[Delivery]:
        """XREADGROUP new (``>``) messages across one or more streams.

        Poison entries (unparseable envelopes) are routed to the DLQ and
        acked here so they never block the choreography; only good entries
        are returned for the caller to handle.
        """
        keys_and_ids = {s: ">" for s in streams}
        if not keys_and_ids:
            return []
        options = StreamReadGroupOptions(count=count, block_ms=block_ms)
        result = await self._client.xreadgroup(keys_and_ids, group, consumer, options)
        if not result:
            return []

        deliveries: list[Delivery] = []
        for stream_b, entries in result.items():
            stream = _s(stream_b)
            if not entries:
                continue
            for entry_id_b, fields in entries.items():
                entry_id = _s(entry_id_b)
                if fields is None:  # entry trimmed/deleted from the stream
                    await self.ack(stream, group, [entry_id])
                    continue
                try:
                    env = EventEnvelope.from_fields(fields)
                except Exception as exc:  # noqa: BLE001 - poison message
                    log.warning("DLQ poison entry %s on %s: %s", entry_id, stream, exc)
                    await self.dead_letter(stream, entry_id, fields, str(exc))
                    await self.ack(stream, group, [entry_id])
                    continue
                deliveries.append(Delivery(stream, entry_id, env))
        return deliveries

    async def ack(self, stream: str, group: str, entry_ids: list[str]) -> int:
        if not entry_ids:
            return 0
        return await self._client.xack(stream, group, entry_ids)

    async def reclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_ms: int,
        start: str = "0-0",
        count: int = 32,
    ) -> tuple[str, list[Delivery]]:
        """XAUTOCLAIM idle pending entries for *consumer*.

        Returns the next cursor (pass back as ``start``) and the reclaimed,
        parsed deliveries. Poison reclaimed entries are DLQ'd + acked.
        """
        res = await self._client.xautoclaim(
            stream, group, consumer, min_idle_ms, start, count
        )
        # res = [next_cursor, {id: [[f,v],...]}, (optional) [deleted_ids]]
        next_cursor = _s(res[0])
        claimed = res[1] if len(res) > 1 and res[1] else {}
        deliveries: list[Delivery] = []
        for entry_id_b, fields in claimed.items():
            entry_id = _s(entry_id_b)
            try:
                env = EventEnvelope.from_fields(fields)
            except Exception as exc:  # noqa: BLE001
                log.warning("DLQ reclaimed poison %s on %s: %s", entry_id, stream, exc)
                await self.dead_letter(stream, entry_id, fields, str(exc))
                await self.ack(stream, group, [entry_id])
                continue
            deliveries.append(Delivery(stream, entry_id, env))
        return next_cursor, deliveries

    async def dead_letter(
        self, stream: str, entry_id: str, fields, error: str
    ) -> None:
        """Route an unprocessable entry (raw payload + error) to the DLQ."""
        raw = ""
        try:
            for pair in fields:
                if _s(pair[0]) == WIRE_FIELD:
                    raw = _s(pair[1])
                    break
        except Exception:  # noqa: BLE001 - never let DLQ routing raise
            raw = repr(fields)
        await self._client.xadd(
            self._settings.dlq_stream,
            [
                ("source_stream", stream),
                ("source_id", entry_id),
                ("error", error),
                ("raw", raw),
            ],
            StreamAddOptions(make_stream=True),
        )

    async def observe(
        self, stream: str, last_id: str = "0", count: int = 100
    ) -> tuple[str, list[EventEnvelope]]:
        """XREAD entries with id greater than ``last_id`` *without* a consumer
        group — a read-only observer (the gateway) that never touches PELs.

        Returns the new cursor and the parsed envelopes. Unparseable entries
        are skipped (the actor groups own DLQ routing).
        """
        result = await self._client.xread(
            {stream: last_id}, StreamReadOptions(count=count)
        )
        if not result:
            return last_id, []
        envelopes: list[EventEnvelope] = []
        cursor = last_id
        entries = result.get(stream.encode()) or result.get(stream) or {}
        for entry_id_b, fields in entries.items():
            cursor = _s(entry_id_b)
            try:
                envelopes.append(EventEnvelope.from_fields(fields))
            except Exception:  # noqa: BLE001 - observer skips poison, doesn't DLQ
                continue
        return cursor, envelopes

    async def stream_len(self, stream: str) -> int:
        return await self._client.xlen(stream)

    async def delete_stream(self, stream: str) -> None:
        await self._client.delete([stream])

    # --- KV / counters (registry) ---

    async def incr(self, key: str) -> int:
        return await self._client.incr(key)

    async def get_str(self, key: str) -> Optional[str]:
        return _s(await self._client.get(key))

    async def set_str(self, key: str, value: str) -> None:
        await self._client.set(key, value)

    async def set_if_absent(self, key: str, value: str) -> bool:
        """Atomic SET NX. Returns True iff *this* call created the key — used
        to elect the single actor that terminates a workflow."""
        result = await self._client.set(
            key, value, conditional_set=ConditionalChange.ONLY_IF_DOES_NOT_EXIST
        )
        return result is not None

    async def expire(self, key: str, seconds: int) -> None:
        await self._client.expire(key, seconds)

    async def delete(self, *keys: str) -> None:
        if keys:
            await self._client.delete(list(keys))

    # --- sets (active-streams registry) ---

    async def sadd(self, key: str, member: str) -> None:
        await self._client.sadd(key, [member])

    async def srem(self, key: str, member: str) -> None:
        await self._client.srem(key, [member])

    async def smembers(self, key: str) -> set[str]:
        members = await self._client.smembers(key)
        return {_s(m) for m in members}
