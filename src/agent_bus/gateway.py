"""Gateway — the Socket.IO ⇄ Valkey bridge (architecture §7).

Bidirectional and brain-free: it never runs agent logic, only translates
between Socket.IO frames and Valkey stream entries, sharing the same envelope
models as the actors.

* **connect**       → the socket id becomes the initiator id (`stream_id`); the
                      gateway registers it and observes the client's own stream.
* **request**       → publishes a `request` envelope (new `cid`) onto the client's
                      own stream (initiator convenience); actors take it from there.
* **publish**       → publishes an arbitrary `event_type` to ANY stream (general producer).
* **subscribe**     → `XREAD`s ANY stream (no consumer group) and `emit`s every
                      envelope back to the socket — receive events you didn't produce.
* **unsubscribe**   → stops a subscription's observer.
* **terminate/status** → outlier kill switch / live iteration snapshot.
* **disconnect**    → stops all observers and cleans up the client's stream.

Delivery is read-only observer semantics (every subscriber sees every event); this
is NOT consumer-group work distribution (that is a server-side actor concern).
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
        # (sid, stream_id) -> observer task for an explicit subscription.
        self._subscriptions: dict[tuple[str, str], asyncio.Task] = {}

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
            # Observe the client's own stream (so it sees its own workflows).
            self._observers[sid] = asyncio.create_task(self._observe_stream(sid, sid))
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
        async def publish(sid, data):  # noqa: ANN001
            """Publish an event to ANY stream (general producer).
            data: {stream_id, event_type, data?, cid?}. Returns {cid, sid, entry_id}."""
            if not isinstance(data, dict):
                return {"ok": False, "error": "object payload required"}
            stream_id = data.get("stream_id")
            event_type = data.get("event_type")
            if not stream_id or not event_type:
                return {"ok": False, "error": "stream_id and event_type required"}
            cid = data.get("cid") or str(uuid.uuid4())
            seq = await self._registry.next_sid(cid)
            env = new_event(stream_id=stream_id, cid=cid, sid=seq, sender=sid,
                            event_type=event_type, data=data.get("data") or {})
            # Register the stream so actors/observers/reaper see it, then publish.
            await self._discovery.register(stream_id)
            entry_id = await self._bus.publish(self._settings.stream_key(stream_id), env)
            await self._cleaner.touch(stream_id)
            return {"ok": True, "cid": cid, "sid": seq, "entry_id": entry_id}

        @sio.event
        async def subscribe(sid, data):  # noqa: ANN001
            """Observe ANY stream — receive every event published to it (read-only,
            you did not have to produce them). data: {stream_id}."""
            stream_id = (data or {}).get("stream_id") if isinstance(data, dict) else None
            if not stream_id:
                return {"ok": False, "error": "stream_id required"}
            key = (sid, stream_id)
            if key not in self._subscriptions:
                self._subscriptions[key] = asyncio.create_task(
                    self._observe_stream(sid, stream_id)
                )
                log.info("client %s subscribed to %s", sid, stream_id)
            return {"ok": True, "stream_id": stream_id}

        @sio.event
        async def unsubscribe(sid, data):  # noqa: ANN001
            stream_id = (data or {}).get("stream_id") if isinstance(data, dict) else None
            task = self._subscriptions.pop((sid, stream_id), None)
            if task:
                task.cancel()
            return {"ok": True, "stream_id": stream_id}

        @sio.event
        async def disconnect(sid):  # noqa: ANN001
            task = self._observers.pop(sid, None)
            if task:
                task.cancel()
            for key in [k for k in self._subscriptions if k[0] == sid]:
                self._subscriptions.pop(key).cancel()
            await self._cleaner.close(sid)
            log.info("client disconnected: %s", sid)

    async def _observe_stream(self, sid: str, stream_id: str) -> None:
        """Tail `stream:<stream_id>` and mirror every event to socket `sid`.
        Used for both the client's own stream (stream_id == sid) and explicit
        subscriptions. Each event carries its source in `header.stream_id`, so the
        client routes it to the right Workflow (by cid) or Subscription (by stream)."""
        stream = self._settings.stream_key(stream_id)
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
            log.warning("observer error for %s on %s: %s", sid, stream_id, exc)
