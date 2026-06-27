"""BusClient — the glide (direct-Valkey) bus client: produce + consume.

This is the **server-side** client for components that need real consumer-group
semantics (competing consumers, ack, at-least-once, crash recovery) — e.g.
``agent_runtime``. It talks to Valkey directly via ``valkey-glide`` (glibc only;
NOT alpine/musl), so it is not browser-usable. For the browser/initiator
(Socket.IO) surface, use :class:`agent_bus_client.AgentBusClient`.

One client, both directions:
  * **produce** — ``publish(stream, envelope)``
  * **consume** — ``ensure_group`` / ``read_group`` / ``ack`` / ``reclaim`` / ``observe``

Semantics (preserved from the agent_bus EventBus):
  * **At-least-once** — a redelivered entry (after reclaim) is expected; dedupe in
    your handler on ``cid``+``sid``, not in the client.
  * **Poison routing** — an unparseable entry goes to the DLQ *and* is acked, so one
    bad message can't wedge the group.

Install the optional dependency: ``pip install valkey-glide`` (or the ``[bus]`` extra).
"""

from __future__ import annotations

import os
import socket
from typing import Iterable, NamedTuple, Optional

from glide import (
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
    RequestError,
    StreamAddOptions,
    StreamGroupOptions,
    StreamReadGroupOptions,
    StreamReadOptions,
)

from .envelope import WIRE_FIELD, EventEnvelope


def _s(value) -> str:
    """Decode glide's bytes results to str (pass through str/None)."""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return value


def make_consumer(name: str) -> str:
    """A consumer id that encodes host+pid, so multiple instances of the same
    service share one group and load-balance (matches the bus's BaseActor)."""
    return f"{name}-{socket.gethostname()}-{os.getpid()}"


class Delivery(NamedTuple):
    """One successfully-parsed stream entry. Access by name: ``.entry_id`` (for
    ``ack``), ``.stream``, ``.envelope`` (``.header.cid`` + ``.sid`` = idempotency key)."""

    stream: str
    entry_id: str
    envelope: EventEnvelope


class BusClient:
    """Async produce+consume facade over a single direct-Valkey (glide) connection."""

    def __init__(
        self,
        client: GlideClient,
        *,
        stream_prefix: str = "stream:",
        dlq_stream: str = "stream:dlq",
        stream_ttl_s: int = 3600,
    ):
        self._client = client
        self._stream_prefix = stream_prefix
        self._dlq_stream = dlq_stream
        self._stream_ttl_s = stream_ttl_s

    @property
    def client(self) -> GlideClient:
        return self._client

    @classmethod
    async def create(
        cls,
        host: str = "127.0.0.1",
        port: int = 6379,
        *,
        stream_prefix: str = "stream:",
        dlq_stream: str = "stream:dlq",
        stream_ttl_s: int = 3600,
    ) -> "BusClient":
        client = await GlideClient.create(
            GlideClientConfiguration([NodeAddress(host, port)])
        )
        return cls(
            client,
            stream_prefix=stream_prefix,
            dlq_stream=dlq_stream,
            stream_ttl_s=stream_ttl_s,
        )

    async def close(self) -> None:
        await self._client.close()

    def stream_key(self, stream_id: str) -> str:
        """`stream:<stream_id>` — build a key from a bare id."""
        return f"{self._stream_prefix}{stream_id}"

    # --- produce ---

    async def publish(self, stream: str, env: EventEnvelope) -> str:
        """XADD an envelope to ``stream`` (a full key); returns the entry id.
        Refreshes the stream's idle TTL so an active stream is never reaped."""
        entry_id = await self._client.xadd(
            stream, env.to_fields(), StreamAddOptions(make_stream=True)
        )
        await self._client.expire(stream, self._stream_ttl_s)
        return _s(entry_id)

    # --- consume (consumer groups) ---

    async def ensure_group(self, stream: str, group: str, start: str = "0") -> None:
        """XGROUP CREATE (with MKSTREAM), idempotent (BUSYGROUP-safe).
        ``start="0"`` delivers backlog; ``"$"`` only new messages."""
        try:
            await self._client.xgroup_create(
                stream, group, start, StreamGroupOptions(make_stream=True)
            )
        except RequestError as exc:
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
        Poison entries are DLQ'd + acked here (never returned)."""
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
                if fields is None:  # trimmed/deleted from the stream
                    await self.ack(stream, group, [entry_id])
                    continue
                try:
                    env = EventEnvelope.from_fields(fields)
                except Exception as exc:  # noqa: BLE001 - poison
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
        """XAUTOCLAIM idle pending entries (crash/reaper recovery). Returns the
        next cursor and the reclaimed deliveries (poison → DLQ + ack)."""
        res = await self._client.xautoclaim(
            stream, group, consumer, min_idle_ms, start, count
        )
        next_cursor = _s(res[0])
        claimed = res[1] if len(res) > 1 and res[1] else {}
        deliveries: list[Delivery] = []
        for entry_id_b, fields in claimed.items():
            entry_id = _s(entry_id_b)
            try:
                env = EventEnvelope.from_fields(fields)
            except Exception as exc:  # noqa: BLE001
                await self.dead_letter(stream, entry_id, fields, str(exc))
                await self.ack(stream, group, [entry_id])
                continue
            deliveries.append(Delivery(stream, entry_id, env))
        return next_cursor, deliveries

    async def observe(
        self, stream: str, last_id: str = "0", count: int = 100
    ) -> tuple[str, list[EventEnvelope]]:
        """XREAD (no group) — read-only replay/tail. Skips poison (no DLQ)."""
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
            except Exception:  # noqa: BLE001
                continue
        return cursor, envelopes

    async def dead_letter(self, stream: str, entry_id: str, fields, error: str) -> None:
        """Route an unprocessable entry (raw payload + error) to the DLQ."""
        raw = ""
        try:
            for pair in fields:
                if _s(pair[0]) == WIRE_FIELD:
                    raw = _s(pair[1])
                    break
        except Exception:  # noqa: BLE001
            raw = repr(fields)
        await self._client.xadd(
            self._dlq_stream,
            [("source_stream", stream), ("source_id", entry_id),
             ("error", error), ("raw", raw)],
            StreamAddOptions(make_stream=True),
        )

    # --- small KV / counter / set helpers (sequencing, registry) ---

    async def incr(self, key: str) -> int:
        return await self._client.incr(key)

    async def expire(self, key: str, seconds: int) -> None:
        await self._client.expire(key, seconds)

    async def sadd(self, key: str, member: str) -> None:
        await self._client.sadd(key, [member])

    async def srem(self, key: str, member: str) -> None:
        await self._client.srem(key, [member])

    async def smembers(self, key: str) -> set[str]:
        return {_s(m) for m in await self._client.smembers(key)}

    async def stream_len(self, stream: str) -> int:
        return await self._client.xlen(stream)
