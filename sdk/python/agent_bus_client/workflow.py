"""Workflow + Event — the high-level abstraction over a single ``cid``.

A ``Workflow`` represents one workflow you started on the bus. It hides
cid-correlation and stream multiplexing: you async-iterate it to get *only*
this workflow's events, ``await`` its completion, and ``terminate()`` it.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

WORKFLOW_TERMINATED = "workflow.terminated"
_SENTINEL = object()


class Event:
    """A friendly view over one event envelope (see client_sdk.md §3)."""

    __slots__ = ("raw", "stream_id", "cid", "sid", "type", "sender", "timestamp", "data")

    def __init__(self, env: dict[str, Any]):
        self.raw = env
        header = env.get("header", {})
        self.stream_id: str = header.get("stream_id")
        self.cid: str = header.get("cid")
        self.sid: int = header.get("sid", 0)
        self.type: str = header.get("event_type")
        self.sender: str = header.get("sender")
        self.timestamp: str = header.get("timestamp")
        self.data: dict[str, Any] = env.get("payload", {}).get("data", {}) or {}

    @property
    def is_terminal(self) -> bool:
        return self.type == WORKFLOW_TERMINATED

    def __repr__(self) -> str:
        return f"Event(sid={self.sid}, type={self.type!r}, sender={self.sender!r})"


class Workflow:
    """One workflow (`cid`). Async-iterable; yields its events until it ends.

    Usage::

        wf = await client.start("summarize X")
        async for ev in wf:          # only THIS workflow's events
            print(ev.sid, ev.type)
            if ev.sid > 500:
                await wf.terminate() # kill an outlier
        await wf.completed           # True if terminated, False if disconnected

    Iteration ends after the terminal ``workflow.terminated`` event, on
    disconnect, or — if ``idle_timeout`` is set — after that many seconds of
    silence (the way to detect a workflow that ended by "going quiet", which
    emits no terminal event).
    """

    def __init__(self, client, cid: str, *, idle_timeout: Optional[float] = None):
        self._client = client
        self.cid = cid
        self.idle_timeout = idle_timeout
        self.sid = 0                       # latest step seen = live iteration count
        self._queue: asyncio.Queue = asyncio.Queue()
        self._ended = False
        self.completed: asyncio.Future = asyncio.get_event_loop().create_future()

    # --- fed by the client's event dispatcher ---

    def _feed(self, event: Event) -> None:
        self.sid = max(self.sid, event.sid)
        self._queue.put_nowait(event)
        if event.is_terminal:
            self._end(True)

    def _disconnected(self) -> None:
        self._end(False)

    def _end(self, terminated: bool) -> None:
        if self._ended:
            return
        self._ended = True
        if not self.completed.done():
            self.completed.set_result(terminated)
        self._queue.put_nowait(_SENTINEL)

    # --- async iteration ---

    def __aiter__(self) -> "Workflow":
        return self

    async def __anext__(self) -> Event:
        if self._ended and self._queue.empty():
            raise StopAsyncIteration
        try:
            if self.idle_timeout is not None:
                item = await asyncio.wait_for(self._queue.get(), self.idle_timeout)
            else:
                item = await self._queue.get()
        except asyncio.TimeoutError:  # went quiet -> treat as done
            if not self.completed.done():
                self.completed.set_result(False)
            raise StopAsyncIteration
        if item is _SENTINEL:
            raise StopAsyncIteration
        return item

    # --- commands scoped to this workflow ---

    async def terminate(self) -> dict[str, Any]:
        """Eliminate this workflow (the outlier kill switch)."""
        return await self._client.terminate(self.cid)

    async def status(self) -> dict[str, Any]:
        """Snapshot this workflow's live step count + state."""
        return await self._client.status(self.cid)

    async def collect(self, timeout: Optional[float] = None) -> list[Event]:
        """Convenience: drain to completion and return all events."""
        async def _drain() -> list[Event]:
            return [ev async for ev in self]
        if timeout is not None:
            return await asyncio.wait_for(_drain(), timeout)
        return await _drain()


class Subscription:
    """A live subscription to a stream — async-iterable over EVERY event published
    to it (read-only observer; events you did not produce). Unlike a Workflow (one
    cid, terminates), a Subscription spans all cids on the stream and runs until you
    ``unsubscribe()`` or the client disconnects.

        sub = await client.subscribe("some-stream-id")
        async for ev in sub:
            print(ev.cid, ev.type, ev.data)
        await sub.unsubscribe()

    Note: observer semantics — every subscriber sees every event. This is NOT
    consumer-group work distribution (use the glide ``BusClient`` for that).
    """

    def __init__(self, client, stream_id: str):
        self._client = client
        self.stream_id = stream_id
        self._queue: asyncio.Queue = asyncio.Queue()
        self._closed = False

    def _feed(self, event: Event) -> None:
        self._queue.put_nowait(event)

    def _close(self) -> None:
        if not self._closed:
            self._closed = True
            self._queue.put_nowait(_SENTINEL)

    def __aiter__(self) -> "Subscription":
        return self

    async def __anext__(self) -> Event:
        if self._closed and self._queue.empty():
            raise StopAsyncIteration
        item = await self._queue.get()
        if item is _SENTINEL:
            raise StopAsyncIteration
        return item

    async def unsubscribe(self) -> None:
        """Stop the subscription (server-side observer too) and end iteration."""
        await self._client._unsubscribe(self.stream_id)
