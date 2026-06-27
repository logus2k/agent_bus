"""Gateway — the Socket.IO ⇄ Valkey bridge (architecture §7).

Bidirectional and brain-free: it never runs agent logic, only translates
between Socket.IO frames and Valkey stream entries, sharing the same envelope
models as the actors.

* **connect**     → the socket id becomes the initiator id (`stream_id`); the
                    gateway registers it and starts an observer task.
* **request**     → publishes a `request` envelope (new `cid`) onto the
                    client's dedicated stream; actors take it from there.
* **observer**    → `XREAD`s the dedicated stream (no consumer group) and
                    `emit`s every envelope back to that one socket, live.
* **disconnect**  → stops the observer and cleans up the stream.
"""

from __future__ import annotations

import asyncio
import logging
import uuid

import socketio

from .bus import EventBus
from .cleanup import StreamCleaner
from .config import Settings
from .discovery import Discovery
from .envelope import EventType, new_event
from .registry import WorkflowRegistry

log = logging.getLogger("agent_bus.gateway")


class Gateway:
    def __init__(
        self,
        bus: EventBus,
        registry: WorkflowRegistry,
        discovery: Discovery,
        cleaner: StreamCleaner,
        settings: Settings,
    ):
        self._bus = bus
        self._registry = registry
        self._discovery = discovery
        self._cleaner = cleaner
        self._settings = settings
        self._observers: dict[str, asyncio.Task] = {}

        self._sio = socketio.AsyncServer(
            async_mode="asgi", cors_allowed_origins="*"
        )
        self.asgi = socketio.ASGIApp(self._sio, static_files=self._static_files())
        self._register_handlers()

    def _static_files(self):
        """Serve the web-client dashboard (and the JS SDK it imports) when
        WEBCLIENT_DIR is configured; returns None to disable static serving."""
        static: dict[str, str] = {}
        if self._settings.webclient_dir:
            d = self._settings.webclient_dir
            static["/"] = f"{d}/index.html"
            static["/static"] = d
        if self._settings.sdk_dir:
            static["/sdk"] = self._settings.sdk_dir
        return static or None

    def _register_handlers(self) -> None:
        sio = self._sio

        @sio.event
        async def connect(sid, environ, auth=None):  # noqa: ANN001
            await self._discovery.register(sid)
            await self._cleaner.touch(sid)
            self._observers[sid] = asyncio.create_task(self._observe(sid))
            await sio.emit("connected", {"stream_id": sid}, to=sid)
            log.info("client connected: %s", sid)

        @sio.event
        async def request(sid, data):  # noqa: ANN001
            """Client starts a workflow. data: {"text": "..."}."""
            text = (data or {}).get("text", "") if isinstance(data, dict) else str(data)
            cid = str(uuid.uuid4())
            seq = await self._registry.next_sid(cid)
            env = new_event(
                stream_id=sid,
                cid=cid,
                sid=seq,
                sender="gateway",
                event_type=EventType.REQUEST,
                data={"text": text},
            )
            await self._bus.publish(self._settings.stream_key(sid), env)
            await self._cleaner.touch(sid)
            log.info("request from %s -> cid=%s", sid, cid)
            return {"cid": cid}

        @sio.event
        async def terminate(sid, data):  # noqa: ANN001
            """Eliminate an outlier: flip a workflow to TERMINATED and emit the
            terminal event so observers see it. data: {"cid": "..."}."""
            cid = (data or {}).get("cid") if isinstance(data, dict) else None
            if not cid:
                return {"ok": False, "error": "cid required"}
            await self._registry.set_terminated(cid)
            seq = await self._registry.next_sid(cid)
            await self._bus.publish(
                self._settings.stream_key(sid),
                new_event(stream_id=sid, cid=cid, sid=seq, sender="gateway",
                          event_type=EventType.WORKFLOW_TERMINATED,
                          data={"reason": "client_terminated"}),
            )
            log.info("client %s terminated cid=%s", sid, cid)
            return {"ok": True, "cid": cid}

        @sio.event
        async def status(sid, data):  # noqa: ANN001
            """Snapshot a workflow's live iteration count + state for outlier
            detection. data: {"cid": "..."} -> {cid, sid, status}."""
            cid = (data or {}).get("cid") if isinstance(data, dict) else None
            if not cid:
                return {"ok": False, "error": "cid required"}
            return {
                "ok": True,
                "cid": cid,
                "sid": await self._registry.current_sid(cid),
                "status": await self._registry.status(cid),
            }

        @sio.event
        async def disconnect(sid):  # noqa: ANN001
            task = self._observers.pop(sid, None)
            if task:
                task.cancel()
            await self._cleaner.close(sid)
            log.info("client disconnected: %s", sid)

    async def _observe(self, sid: str) -> None:
        """Tail the client's dedicated stream and mirror every event to the socket."""
        stream = self._settings.stream_key(sid)
        last_id = "0"
        poll_s = self._settings.actor_poll_ms / 1000.0
        try:
            while True:
                last_id, envelopes = await self._bus.observe(stream, last_id)
                for env in envelopes:
                    await self._sio.emit("event", env.model_dump(), to=sid)
                await asyncio.sleep(poll_s)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - keep the gateway alive
            log.warning("observer error for %s: %s", sid, exc)
