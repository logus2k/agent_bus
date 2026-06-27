## Technical Architecture: Reactive Event-Driven Agent Bus

This document outlines the architecture for a decentralized, event-driven agentic communication backbone. By moving from a centralized orchestrator to a **choreographed protocol**, we achieve horizontal scalability, language agnosticism, and fault tolerance.

---

## 1. System Overview

The architecture replaces imperative execution loops with a reactive message-driven bus. Each component (Agent, Tool, Judge, Monitor) functions as an autonomous actor that subscribes to specific event types and emits downstream consequences.

* **Bus Backbone:** [Valkey](https://valkey.io/) (using Streams for persistence).
* **Client Library:** [valkey-glide](https://github.com/valkey-io/valkey-glide/tree/main/python) (async, Rust core, official Valkey client). All bus access is encapsulated behind an `EventBus` wrapper so actors never call the client directly.
* **Real-time Interface:** Socket.IO via `python-socketio` on ASGI/uvicorn (for UI/Dashboard synchronization).
* **Contract:** Pydantic models for the event envelope (schema *validation* against the bus is deferred — see §6).
* **Runtime:** Python 3.12.3. Initial deployment is **mono-process** — all actor loops run as `asyncio` tasks in a single container; this can be split into per-actor containers later without changing the bus contract.

---

## 2. Identity Model: Streams, Initiators, and Workflows

Routing is keyed on **who initiated the work**, not on a global event-type bucket.

* **Initiator:** a connected frontend client (its socket/connection id) or a server component that starts work. Each initiator owns **one long-lived stream**, `stream:<initiator_id>`. The initiator **generates the stream id** and it is reused by every participant in that workflow.
* **Workflow (`cid`):** a single logical trace (request). **Multiple concurrent workflows are multiplexed onto one initiator stream**, each distinguished by its Correlation ID (`cid`). `cid` is what ties `agent.thought → tool.exec → tool.result → judge.verdict` together as one conversation.
* **Step (`sid`):** a monotonic per-workflow step counter, allocated with `INCR sid:<cid>` (a single atomic command — replaces any distributed/`WATCH/MULTI` scheme).

| Concept | Key | Lifetime | Purpose |
|---|---|---|---|
| Initiator stream | `stream:<initiator_id>` | Long-lived (per connection/component) | Transport for all of an initiator's workflows |
| Correlation id | `cid` | Per workflow | Trace + termination + sequencing key |
| Step id | `sid` (`INCR sid:<cid>`) | Per workflow | Monotonic step counter / termination threshold |

---

## 3. Event Envelope (The Contract)

All events transmitted via the bus adhere to this JSON schema (modeled with Pydantic).

```json
{
  "header": {
    "stream_id": "string",     // Initiator id == the stream key (stream:<stream_id>)
    "cid": "uuid-v4",          // Correlation ID: one workflow trace (multiplexed on the stream)
    "sid": "int",              // Sequence ID: monotonic step counter (INCR sid:<cid>)
    "timestamp": "iso8601",    // Event generation time
    "sender": "string",        // Originating Actor ID
    "event_type": "string"     // Taxonomy: e.g. 'agent.thought', 'tool.exec'
  },
  "payload": {
    "data": "object",          // Actor-specific content
    "context": "object"        // Optional: Local state snapshot
  },
  "metadata": {
    "version": "string",       // Schema versioning
    "trace_parent": "string"   // Distributed trace header (OpenTelemetry)
  }
}
```

---

## 4. Discovery & Choreography

Because initiator streams are created dynamically, actors cannot statically subscribe to a fixed list. Discovery uses a **well-known control stream** for the one-time rendezvous:

1. **Announce:** the initiator picks `stream_id`, `XADD`s the opening event to the well-known `stream:control` carrying that `stream_id`, and registers it (`SADD streams:active <stream_id>`).
2. **Attach:** every actor type tails `stream:control`; on seeing a new `stream_id` it creates its consumer group on `stream:<stream_id>` (`XGROUP CREATE … MKSTREAM`) and begins `XREADGROUP`.
3. **Run:** from then on, all workflow traffic for that initiator flows on its dedicated stream.

### Consumer Groups

* One **consumer group per actor type** on each stream (e.g. `cg:agent`, `cg:tool`, `cg:judge`); consumer name is per worker instance.
* **`XADD`** appends events; **`XREADGROUP`** claims pending work; **`XACK`** removes a message from the Pending Entries List (PEL) once handled.
* Delivery is **at-least-once**: after a reclaim a handler may see the same event twice, so **handlers must be idempotent** (dedupe on `cid` + `sid`).

---

## 5. Lifecycle Management

### Workflow termination (per `cid`) — a shared agreement

Termination is not an orchestrator command. Before processing any event, every actor runs a **Termination Guard**:

* **Check:** read `state[cid].status` in Valkey.
* **Evaluate:** if `sid >= MAX_THRESHOLD`, set `status = TERMINATED` and emit `workflow.terminated`.
* **React:** if `status == TERMINATED`, drop the event immediately.

A terminated workflow is a **state flip only** — the initiator's stream stays alive for its other/future workflows. `MAX_THRESHOLD` is env-configurable (default **50**).

### Stream cleanup (per initiator)

* **Delete on terminate:** when the initiator disconnects / the component shuts down, delete `stream:<initiator_id>` and `SREM streams:active <initiator_id>`.
* **TTL safety net:** an idle expiry (default **1h** after last activity) reclaims streams whose initiator vanished without a clean disconnect.

---

## 6. Durability, Fault Tolerance & Observability

### Persistence: AOF (Append Only File)

We use **AOF** rather than RDB snapshots for an exact, replayable log of all interactions.

* `--appendonly yes` enables the AOF log; every `XADD` is written to a disk-backed append-only log.
* `--appendfsync everysec` balances I/O performance and safety (max 1-second loss window).
* **Recovery:** on restart Valkey replays the AOF to reconstruct stream state, consumer-group offsets, and unacknowledged (PEL) messages.

### Crash recovery: reclaiming abandoned work

AOF recovers the *stream*; messages stuck in a **dead consumer's PEL** are reclaimed with **`XAUTOCLAIM`**. Valkey tracks idle time **per message** automatically; a background **reaper** loop runs `XAUTOCLAIM <stream> <group> <consumer> <min-idle-time>` to re-deliver work abandoned by crashed consumers. `min-idle-time` is env-configurable per stream (default **30s**).

### Dead Letter Queue (DLQ)

Messages that cannot be processed are routed to the single shared `stream:dlq` (storing raw payload + error), preventing poison messages from blocking the choreography. *Note: strict schema validation against the contract is deferred for now; the DLQ path and envelope models are in place so it can be enabled later.*

### Distributed Tracing

Each actor carries `trace_parent` forward (OpenTelemetry, natively supported by valkey-glide). Replaying a stream reconstructs the exact execution state of any workflow, enabling:

* **Retries:** re-injecting failed events back into the stream.
* **Replayability:** re-running the exact sequence of inputs to debug a failure.

---

## 7. Real-Time Gateway

The Socket.IO gateway is a **bidirectional bridge** between browsers and the bus; it runs no agent logic and shares the same Pydantic envelope models. Using `redis`-style async via valkey-glide on the asyncio loop:

```
Browser  <--Socket.IO-->  Gateway  <--Valkey streams-->  Bus / Actors
```

* **Connect:** Socket.IO assigns a connection id; the gateway acts as the *initiator* on the client's behalf and uses that id as `stream_id`.
* **Commands in (browser → Valkey):** user actions are `XADD`ed to `stream:control` with the chosen `stream_id` (new `cid` per request).
* **Events out (Valkey → browser):** a background task tails the initiator's stream as an **observer** and `sio.emit(...)`s each event to that specific browser in real time.

---

## 8. LLM Integration Layer (the Agent Brain)

Agent_bus does **not** host the LLM. The "brain" is the existing **`agent_server`** service, reachable at `http://agent_server:7701` on `logus2k_network`. Real agent actors *call* it; the bus orchestrates, agent_server reasons.

* **Invocation:** REST `POST /v1/chat/completions` with `model` = an **agent name** (one call — agent_server applies that agent's system prompt + sampling), or Socket.IO `Chat` for **streaming** (`RunStarted` → `ChatChunk` → `ChatDone`, `Interrupt` to cancel).
* **An agent there = prompt + sampling preset** on one shared active model. Bus actors reference an agent **name** and send user input — they never pick models or prompts.
* **Client:** the **Python SDK** at `agent_server/sdk/python/agent_server_sdk` (handles streaming, `<think>`/`<voice>`/answer parsing, thinking toggle, discovery). Preferred over hand-rolled HTTP. Packaged as a local dependency when real agents land (out of scope for the echo slice).

**Implications for the actor seam (designed in from the start):**

1. **`handle()` emits 0..N events, not exactly one** — agent_server streams deltas, so each `ChatChunk` maps to an incremental `agent.thought`/`agent.delta` bus event, mirrored live to the browser by the gateway. The actor loop is therefore *consume → guard → handle → emit each produced event → ack*.
2. **`cid` → `thread_id`** — stateful agents (`memory_policy: "thread_window"`) take a `thread_id`; the per-workflow `cid` maps onto it directly.
3. **Termination ↔ Interrupt** — a workflow flipping to `TERMINATED` triggers an `Interrupt` to agent_server for any in-flight run.
4. **Two distinct Socket.IO layers** — gateway Socket.IO is *browser ↔ bus*; agent_server Socket.IO is *actor ↔ brain*. The actor↔brain path lives behind the `EventBus`/actor seam, never exposed to the gateway.

---

## 9. Implementation Checklist

* [ ] **Infrastructure:** Deploy Valkey (AOF enabled, persistent volume, bound to `127.0.0.1`, attached to `logus2k_network`).
* [ ] **Contract:** Implement `EventEnvelope` Pydantic models.
* [ ] **Bus:** `EventBus` wrapper over valkey-glide (`XADD/XREADGROUP/XACK`, control-stream announce/attach, `INCR` sequencing, DLQ routing).
* [ ] **Guards:** `WorkflowRegistry` for per-`cid` termination state and `INCR sid` allocation.
* [ ] **Reaper:** `XAUTOCLAIM` loop with configurable `min-idle-time`.
* [ ] **Actors:** echo-agent and echo-tool (infra-only, no LLM) to prove the end-to-end flow.
* [ ] **Cleanup:** stream delete-on-terminate + TTL safety net.
* [ ] **Gateway:** `python-socketio` ASGI bridge that broadcasts envelopes to the owning client.
