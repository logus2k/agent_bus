# Implementation Plan: Agent Bus (First Slice)

Goal of the first slice: prove the **choreography backbone end-to-end** with infra-only echo actors — no LLM logic. A command enters via the control stream, gets routed to a dedicated initiator stream, flows through two actors, sequences with `INCR`, terminates by shared agreement, and is mirrored to a browser via Socket.IO. Everything is built behind clean seams so real agents drop in later.

See [technical_architecture.md](technical_architecture.md) for the design rationale; this document is the build order.

---

## Proposed project layout

```
agent_bus/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── documents/
│   ├── technical_architecture.md
│   └── implementation_plan.md
└── src/
    └── agent_bus/
        ├── __init__.py
        ├── config.py            # env-driven settings (thresholds, TTLs, addresses)
        ├── envelope.py          # Pydantic EventEnvelope + event_type taxonomy
        ├── bus.py               # EventBus: valkey-glide wrapper (XADD/XREADGROUP/XACK, DLQ)
        ├── registry.py          # WorkflowRegistry: per-cid state + INCR sid + termination guard
        ├── discovery.py         # control-stream announce/attach helpers
        ├── reaper.py            # XAUTOCLAIM loop
        ├── actor.py             # BaseActor: consume → guard → handle → ack loop
        ├── actors/
        │   ├── echo_agent.py    # reacts to request → emits agent.thought
        │   └── echo_tool.py     # reacts to agent.thought → emits tool.result
        ├── gateway.py           # python-socketio ASGI bridge
        └── app.py               # mono-process entrypoint: wires actors + reaper, runs loop
```

---

## Build order

### Step 0 — Infrastructure (`docker-compose.yml`, `Dockerfile`, `requirements.txt`)
- Update `docker-compose.yml`: attach `valkey-bus` to the external `logus2k_network`; bind the port to `127.0.0.1:6379:6379` (behind corporate nginx for external access).
  - **Fix the network-name typo:** the current file declares `logus2k-network` (hyphen); the real external network is `logus2k_network` (underscore) — the same one `agent_server` is on. Without this fix the app can't reach Valkey *or* the agent brain.
- Add an app service (mono-process) on the same network, built from `Dockerfile` (`python:3.12-slim-bookworm`, pinned to 3.12.3, venv from `requirements.txt`), depends_on valkey healthcheck.
  - **Image base must stay glibc (`slim-bookworm`), NOT `alpine`/MUSL:** valkey-glide's Rust core has no MUSL support, so the *client/app* image cannot be Alpine. The Valkey *server* container staying on the Alpine image is fine — glide never runs there.
- `requirements.txt`: `valkey-glide`, `pydantic`, `python-socketio`, `uvicorn`, `python-dotenv` (+ pytest for tests).
- **Done when:** `docker compose up` brings up a healthy Valkey reachable from the app container.

### Step 1 — Config (`config.py`)
- Env-driven settings: Valkey host/port, `MAX_THRESHOLD` (50), reaper `MIN_IDLE_MS` (30000), stream idle `TTL_SECONDS` (3600), control-stream name, group-name prefixes.
- `.env.example` documents every knob.

### Step 2 — Envelope (`envelope.py`)
- Pydantic `EventEnvelope` (`header`/`payload`/`metadata`) matching §3 of the architecture doc.
- An `EventType` enum / constants for the taxonomy (`request`, `agent.thought`, `tool.exec`, `tool.result`, `workflow.terminated`).
- Helpers: `new_event(...)`, JSON (de)serialization for stream fields.
- **Done when:** unit test round-trips an envelope through serialize/deserialize.

### Step 3 — EventBus (`bus.py`)
- Encapsulate **all** valkey-glide access (`await GlideClient.create(...)`).
- Methods: `publish(stream, envelope)` (`XADD`), `read_group(stream, group, consumer)` (`XREADGROUP`), `ack(stream, group, id)` (`XACK`), `ensure_group(stream, group)` (`XGROUP CREATE … MKSTREAM`), `dead_letter(raw, error)` (`XADD stream:dlq`).
- **Done when:** an integration test publishes and consumes one envelope through a consumer group against a live Valkey.

### Step 4 — WorkflowRegistry (`registry.py`)
- `next_sid(cid)` → `INCR sid:<cid>`.
- `status(cid)` / `set_terminated(cid)` over `state:<cid>` keys.
- `guard(envelope)` → the shared Termination Guard (§5): check status, evaluate `sid >= MAX_THRESHOLD`, flip + emit `workflow.terminated`, or signal "drop".
- **Done when:** unit tests cover the three guard outcomes (pass / terminate-now / drop).

### Step 5 — Discovery (`discovery.py`)
- Initiator side: `announce(stream_id)` → `XADD stream:control` + `SADD streams:active`.
- Actor side: `watch_control()` → yields new `stream_id`s; on each, `ensure_group` + start consuming that stream.
- **Done when:** an actor attaches to a stream announced after it started.

### Step 6 — BaseActor + echo actors (`actor.py`, `actors/`)
- `BaseActor`: the consume → **guard** → filter by `event_type` → `handle` → **emit each produced event** → **ack** loop, with idempotency (dedupe on `cid`+`sid`). `handle()` returns/yields **0..N events** (not exactly one) so streaming brain calls map cleanly later — see architecture §8.
- `echo_agent`: on `request` → emit `agent.thought`. `echo_tool`: on `agent.thought` → emit `tool.result`.
- **Done when:** an end-to-end test drives `request → agent.thought → tool.result → workflow.terminated` on a dedicated stream.

### Step 7 — Reaper (`reaper.py`)
- Periodic `XAUTOCLAIM` over active streams/groups with `MIN_IDLE_MS`; re-delivered messages flow back through the normal actor loop.
- **Done when:** a test kills a consumer mid-flight and the reaper re-delivers the pending message.

### Step 8 — Cleanup
- On `workflow.terminated`: nothing to the stream (state flip only, per §5).
- On initiator disconnect/shutdown: delete `stream:<id>` + `SREM streams:active`; set/refresh `EXPIRE` as the idle TTL safety net.

### Step 9 — Gateway (`gateway.py`)
- `python-socketio` ASGI app: on connect, derive `stream_id` from the socket id; on client message, `announce` + publish a `request`; background task tails the initiator stream and `emit`s each envelope back to that socket.
- **Done when:** a browser/socket.io client sends a request and sees `agent.thought` → `tool.result` → `workflow.terminated` stream back live.

### Step 10 — Entrypoint (`app.py`)
- Wire EventBus, registry, both actors, and the reaper as asyncio tasks; run the gateway under uvicorn. Graceful shutdown cancels tasks and closes the client.
- **Done when:** `docker compose up` runs the whole slice; a manual end-to-end request round-trips through the gateway.

---

## Testing strategy
- **Unit:** envelope round-trip, registry guard outcomes (no Valkey needed).
- **Integration:** bus publish/consume, discovery attach, reaper reclaim, full echo flow (against a live Valkey via compose).

## Out of scope for this slice (later)
- Real LLM agents / tool execution; Judge & Monitor actors.
- Strict Pydantic schema validation → DLQ enforcement.
- Per-actor-type containers; horizontal scaling.
- Admin/global-feed overview dashboard.
- OpenTelemetry exporter wiring (the `trace_parent` field is carried now; exporting is later).
```
