"""AgentBusClient — the connection + the thin protocol core.

Wraps the Socket.IO gateway (see documents/client_sdk.md): one connection owns
one dedicated stream; ``start()`` returns a :class:`Workflow` you drive at a
high level. A single ``event`` dispatcher routes each envelope to the right
Workflow by ``cid``.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Optional

import socketio

from .workflow import Event, Workflow

log = logging.getLogger("agent_bus_client")


class AgentBusClient:
    def __init__(self, url: str, *, reconnection: bool = True, **sio_kwargs: Any):
        """``url`` is the gateway origin, e.g. ``http://127.0.0.1:6815`` (dev) or
        your nginx-fronted URL. Extra kwargs pass through to ``socketio.AsyncClient``."""
        self.url = url
        self._sio = socketio.AsyncClient(reconnection=reconnection, **sio_kwargs)
        self._workflows: dict[str, Workflow] = {}
        # Events that arrive in the tiny window before start() registers their
        # Workflow are buffered here and drained on registration (no lost events).
        self._orphans: dict[str, list[Event]] = defaultdict(list)
        self._stream_id: Optional[str] = None
        self._connected = asyncio.Event()
        self._register_handlers()

    @property
    def stream_id(self) -> Optional[str]:
        """This connection's dedicated stream id (set after connect)."""
        return self._stream_id

    def _register_handlers(self) -> None:
        sio = self._sio

        @sio.on("connected")
        async def _connected(data):  # noqa: ANN001
            self._stream_id = (data or {}).get("stream_id")
            self._connected.set()

        @sio.on("event")
        async def _event(env):  # noqa: ANN001
            ev = Event(env)
            wf = self._workflows.get(ev.cid)
            if wf is not None:
                wf._feed(ev)
            else:
                self._orphans[ev.cid].append(ev)

        @sio.event
        async def disconnect():
            for wf in self._workflows.values():
                wf._disconnected()

    # --- lifecycle ---

    async def connect(self, timeout: float = 10.0) -> "AgentBusClient":
        await self._sio.connect(self.url)
        await asyncio.wait_for(self._connected.wait(), timeout)
        return self

    async def disconnect(self) -> None:
        await self._sio.disconnect()

    async def __aenter__(self) -> "AgentBusClient":
        return await self.connect()

    async def __aexit__(self, *exc) -> None:
        await self.disconnect()

    # --- high-level ---

    async def start(self, text: str, *, idle_timeout: Optional[float] = None) -> Workflow:
        """Start a workflow and return a :class:`Workflow` bound to its ``cid``."""
        ack = await self._sio.call("request", {"text": text})
        cid = ack["cid"]
        wf = Workflow(self, cid, idle_timeout=idle_timeout)
        self._workflows[cid] = wf
        for ev in self._orphans.pop(cid, []):  # drain anything that raced in
            wf._feed(ev)
        return wf

    # --- thin protocol passthroughs (advanced / by cid) ---

    async def request(self, text: str) -> str:
        """Low-level: emit a request, return the cid (no Workflow object)."""
        ack = await self._sio.call("request", {"text": text})
        return ack["cid"]

    async def status(self, cid: str) -> dict[str, Any]:
        return await self._sio.call("status", {"cid": cid})

    async def terminate(self, cid: str) -> dict[str, Any]:
        return await self._sio.call("terminate", {"cid": cid})
