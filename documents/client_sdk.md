# Agent Bus — Client Integration Guide

This is the **first reference for any client** integrating with the Agent Bus —
a browser front-end, a service backend, or any component that wants to start
workflows and observe their events in real time.

Clients talk to the bus through the **Socket.IO Gateway**. They never speak to
Valkey or the actors directly: the gateway is a bidirectional bridge that turns
your messages into bus events and streams the resulting events back to you
(architecture §7). One Socket.IO connection owns one **dedicated stream**; the
workflows you start on it are multiplexed and told apart by a correlation id
(`cid`).

> **Prefer the SDKs over raw wire.** Ready-made high-level clients wrap
> everything below — connection, cid-correlation, async-iterable workflows, the
> `terminate`/`status` helpers, and the live iteration count:
> - **Python:** [`sdk/python/`](../sdk/python/) — `pip install -e sdk/python`
> - **JavaScript (ES6, browser + Node):** [`sdk/javascript/`](../sdk/javascript/)
>
> Read the wire reference below to understand the protocol; reach for the SDK
> when building a real client.

> Scope: this describes the bus's **first slice** (echo actors, no LLM yet).
> The wire contract below is stable; new event *types* will appear as real
> agents/tools land, but the envelope shape, the connection lifecycle, and the
> `request` command do not change.

---

## 1. Connection

| | |
|---|---|
| **Transport** | Socket.IO (Engine.IO 4 / Socket.IO protocol 5) |
| **Default endpoint** | `http://127.0.0.1:6815` (localhost-bound; reach it externally through the corporate nginx) |
| **Path** | default `/socket.io/` |
| **CORS** | open (`*`) in dev; restrict at the nginx layer in production |
| **Auth** | none — trusted-network surface, kept behind nginx (same posture as the rest of the stack) |
| **Browser client lib** | `socket.io-client` **v4.x** |
| **Python client lib** | `python-socketio` **v5.x** (+ an async HTTP backend such as `aiohttp`) |

Match the client major version to the protocol: a `socket.io-client` v2/v3
browser build will **not** connect to this v5 server.

---

## 2. Protocol reference

### Client → Server

| Event | Payload | Ack (callback) | Effect |
|---|---|---|---|
| `request` | `{ "text": "<your input>" }` | `{ "cid": "<uuid>" }` | Starts a workflow on **your own** stream (initiator convenience). The ack's `cid` correlates every event that workflow produces. |
| `publish` | `{ "stream_id", "event_type", "data"?, "cid"? }` | `{ "ok", "cid", "sid", "entry_id" }` | **Publish** an arbitrary event to **any** stream (general producer). |
| `subscribe` | `{ "stream_id" }` | `{ "ok", "stream_id" }` | **Subscribe** to any stream — the gateway then `event`-mirrors every envelope on it to you (including events you did not produce). |
| `unsubscribe` | `{ "stream_id" }` | `{ "ok", "stream_id" }` | Stop a subscription. |
| `terminate` | `{ "cid": "<uuid>" }` | `{ "ok": true, "cid": "..." }` | **Eliminates** a workflow: flips it to TERMINATED and emits a final `workflow.terminated`. |
| `status` | `{ "cid": "<uuid>" }` | `{ "ok", "cid", "sid", "status" }` | Snapshot of a workflow's **live iteration count** (`sid`) and state. |

> To **abandon** all in-flight work, simply disconnect — the gateway cancels your
> observer + subscriptions and deletes your stream.

**Two transports — pick by role.** This gateway surface (Socket.IO) gives **publish +
subscribe** with *observer* semantics: every subscriber to a stream sees every event.
That is **not** consumer-group work distribution (competing consumers, ack, at-least-once).
For a server-side **worker** that needs those, use the glide **`BusClient`** (direct Valkey)
— see §12. The browser SDK is observer-only by design.

### Server → Client

| Event | Payload | When |
|---|---|---|
| `connected` | `{ "stream_id": "<your socket id>" }` | Once, right after connect. `stream_id` is your dedicated stream's id (equals your socket id). |
| `event` | a full **event envelope** (see §3) | For every event appended to your stream, mirrored live — including your own `request` echoed back, the agents' `agent.thought`, tools' `tool.result`, and the final `workflow.terminated`. |

You receive `event` frames for (a) your **own** stream and (b) **any stream you
`subscribe`d to**. Each frame carries its source in `header.stream_id`, so the SDKs
route it to the right `Workflow` (by `cid`) or `Subscription` (by `stream_id`).

---

## 3. The event envelope

Every `event` frame is one envelope. This is the same contract the actors use
(architecture §3):

```json
{
  "header": {
    "stream_id": "wyTf84Mr7-kk0iEAAAAB",   // your stream (== socket id)
    "cid": "b9d15308-88d6-47e5-a06e-...",   // the workflow this event belongs to
    "sid": 2,                                // monotonic step number within the workflow
    "timestamp": "2026-06-27T15:14:03.6Z",   // ISO-8601
    "sender": "echo_agent",                  // which actor emitted it ('gateway' for your request)
    "event_type": "agent.thought"            // see the taxonomy below
  },
  "payload": {
    "data": { "text": "thinking about: hello" },  // actor-specific content
    "context": null                                // optional local state snapshot
  },
  "metadata": {
    "version": "1.0",                        // schema version
    "trace_parent": null                     // distributed-trace header (OpenTelemetry), when present
  }
}
```

Field guide for clients:

- **`cid`** — correlation id. Your routing key. The `request` ack hands you the
  `cid`; match it against `header.cid` on every `event`.
- **`sid`** — step counter. Strictly increasing per `cid`. Useful for ordering
  and for de-duplicating (delivery is at-least-once; the same `(cid, sid)` may
  arrive twice after a recovery — render idempotently).
- **`event_type`** — what happened (next section).
- **`sender`** — the originating actor; `gateway` for the `request` you sent.
- **`payload.data`** — the meaningful content; its shape depends on
  `event_type` (e.g. `agent.thought` carries `{text}`, `tool.result` carries
  `{result}`).

---

## 4. Event taxonomy

Types a client observes today, and the contract around them:

| `event_type` | Meaning | `payload.data` (today) |
|---|---|---|
| `request` | Your input, echoed onto the stream by the gateway | `{ "text": "..." }` |
| `agent.thought` | An agent step (today: echo; later: streamed LLM reasoning) | `{ "text": "..." }` |
| `tool.result` | A tool produced a result | `{ "result": "..." }` |
| `workflow.terminated` | **End marker** for a workflow (`cid`) | `{ "reason": "..." }` |

`workflow.terminated` is the signal to finalize that `cid` — no further events
for that workflow will follow. It is emitted when a workflow ends **explicitly**
(an actor concludes, or you send `terminate`) or hits the optional runaway
backstop. A workflow may also end **naturally by going quiet** (no actor reacts
to the last event), which produces *no* terminal event — so don't block on
`workflow.terminated` forever; treat a prolonged silence as "done/idle" too.

> Coming as real agents land: `tool.exec`, judge/monitor events, and
> incrementally-streamed `agent.thought` deltas (one envelope per LLM token
> chunk). Treat unknown `event_type`s gracefully — render or ignore, don't
> crash.

---

## 5. Connection lifecycle

```
 client                         gateway                       bus / actors
   │   connect ───────────────────▶│  register stream, start observer
   │◀──────────── connected{stream_id}
   │
   │   emit request{text} ─────────▶│  publish request → your stream
   │◀──────────── ack{cid}          │
   │                                │  actors react on the stream …
   │◀──── event{request}            │  (mirrored from "0", so you see your own)
   │◀──── event{agent.thought}      │
   │◀──── event{tool.result}        │
   │            …                   │
   │◀──── event{workflow.terminated}│  ← finalize this cid
   │
   │   disconnect ─────────────────▶│  cancel observer, delete stream
```

**Durability semantics (important):**

- A stream lives only for the duration of a **connection**. On disconnect the
  gateway deletes it (with an idle-TTL safety net). There is **no cross-reconnect
  resume** in this slice: reconnecting gives you a *new* `stream_id` and a fresh,
  empty stream. Persist anything you need to keep on the client side.
- While connected, the observer replays your stream from the beginning, so
  events that occurred between `connect` and your first handler binding are not
  missed — bind handlers before/at connect.
- **Workflows are not auto-capped.** A workflow runs as long as its actors keep
  reacting — potentially indefinitely. Govern long runs with iteration
  visibility + `terminate` (next section), not by waiting for an automatic stop.

---

## 5b. Long-running workflows & outlier detection

There is **no global step ceiling** — a workflow runs until it ends explicitly
or goes quiet. To keep that safe, the bus gives you **live iteration visibility**
and a **kill switch**:

- **`header.sid`** on every `event` is the workflow's monotonic step number.
  The latest `sid` you've seen for a `cid` *is* its current iteration count.
  Watch it climb to spot a runaway (e.g. `sid` past your expected ceiling, or
  rising too fast).
- **`status`** gives a snapshot (`{ sid, status }`) without consuming the stream
  — handy for a monitoring dashboard polling many `cid`s.
- **`terminate`** ends a workflow you've judged an outlier.

```js
// Trip a kill switch if a workflow exceeds an expected step budget.
const BUDGET = 500;
const steps = new Map();                 // cid -> latest sid

socket.on("event", (env) => {
  const { cid, sid } = env.header;
  steps.set(cid, sid);
  if (sid > BUDGET) {
    socket.emit("terminate", { cid }, (ack) => console.warn("killed outlier", ack));
  }
});

// Or poll a snapshot for a dashboard:
socket.emit("status", { cid }, ({ sid, status }) => {
  console.log(`${cid}: step ${sid} (${status})`);
});
```

> Automating this server-side — a **Monitor** actor that watches `sid` rates and
> terminates outliers by policy — is on the roadmap; the client-side hooks above
> are the manual equivalent today.

---

## 6. Use case: browser front-end (JavaScript)

```html
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<script>
  // Point at the gateway (directly in dev, or your nginx-fronted URL in prod).
  const socket = io("http://127.0.0.1:6815");

  const workflows = new Map();   // cid -> array of events

  socket.on("connected", ({ stream_id }) => {
    console.log("bus stream:", stream_id);
    startWorkflow("Summarize the onboarding doc");
  });

  socket.on("event", (env) => {
    const { cid, event_type, sid, sender } = env.header;
    if (!workflows.has(cid)) workflows.set(cid, []);
    workflows.get(cid).push(env);

    switch (event_type) {
      case "agent.thought":
        render(cid, `🤖 ${env.payload.data.text}`);
        break;
      case "tool.result":
        render(cid, `🔧 ${env.payload.data.result}`);
        break;
      case "workflow.terminated":
        render(cid, `✅ done (${env.payload.data.reason})`);
        break;
    }
  });

  socket.on("connect_error", (err) => console.error("connect failed:", err));
  socket.on("disconnect", (reason) => console.warn("disconnected:", reason));

  function startWorkflow(text) {
    // The ack hands you the cid so you can correlate the stream that follows.
    socket.emit("request", { text }, (ack) => {
      console.log("workflow started, cid =", ack.cid);
    });
  }

  function render(cid, line) {
    /* append `line` to the UI bucket for `cid` */
  }
</script>
```

---

## 7. Use case: service backend (Python)

For a backend that drives the bus as a client (`pip install python-socketio aiohttp`):

```python
import asyncio
import socketio

GATEWAY = "http://agent-bus-app:6815"   # service name on the shared network, or http://127.0.0.1:6815

async def run_once(text: str) -> list[dict]:
    sio = socketio.AsyncClient()
    events: list[dict] = []
    done = asyncio.Event()

    @sio.on("event")
    async def on_event(env):
        events.append(env)
        if env["header"]["event_type"] == "workflow.terminated":
            done.set()

    await sio.connect(GATEWAY)
    ack = await sio.call("request", {"text": text})   # returns {"cid": ...}
    cid = ack["cid"]

    try:
        await asyncio.wait_for(done.wait(), timeout=30)
    finally:
        await sio.disconnect()

    # Keep only this workflow's events (a connection can host several).
    return [e for e in events if e["header"]["cid"] == cid]

if __name__ == "__main__":
    out = asyncio.run(run_once("hello from a backend"))
    for e in out:
        print(e["header"]["sid"], e["header"]["event_type"], e["payload"]["data"])
```

`sio.call(...)` is the request/ack round-trip; use `sio.emit("request", {...})`
fire-and-forget if you don't need the `cid` immediately (you can still read it
from `header.cid` on the mirrored events).

---

## 8. Running concurrent workflows on one connection

A single connection can host many workflows; they share the stream and are
separated by `cid`:

```js
const ids = new Set();
socket.emit("request", { text: "task A" }, (a) => ids.add(a.cid));
socket.emit("request", { text: "task B" }, (b) => ids.add(b.cid));

socket.on("event", (env) => {
  const { cid } = env.header;
  // route env to the right workflow bucket by cid; sid orders within it
});
```

Each workflow ends with its own `workflow.terminated`. There is a global step
ceiling (`MAX_THRESHOLD`) per workflow; long-running echoes terminate
automatically when they hit it.

---

## 9. Error handling & edge cases

- **Connect failure** (`connect_error` / `ConnectionError`): the gateway or
  nginx is unreachable, or a Socket.IO version mismatch. Verify the URL, the
  client major version (v4 browser / v5 python), and the nginx upgrade headers.
- **Mid-workflow disconnect**: your stream is deleted; you will not receive the
  remaining events. Reconnect and re-issue `request` (you get a new `cid`).
- **Duplicate `(cid, sid)`**: possible after a server-side recovery
  (at-least-once delivery). Render/process idempotently keyed on `(cid, sid)`.
- **Unknown `event_type`**: forward-compatible clients ignore or generically
  display it rather than failing.
- **No events arriving**: confirm you received `connected`, that your `request`
  ack returned a `cid`, and that the app container is healthy
  (`docker logs agent-bus-app`).

---

## 10. Production: nginx in front of the gateway

The gateway binds to `127.0.0.1:6815`; expose it through nginx with WebSocket
upgrade enabled:

```nginx
location /agentbus/ {
    proxy_pass http://127.0.0.1:6815/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 3600s;        # long-lived Socket.IO connections
}
```

Then clients connect to `https://<host>/agentbus/` (set the Socket.IO `path`
accordingly if you mount under a sub-path).

---

## 11. Publish & subscribe (the SDKs)

Both SDKs are publish **and** subscribe — not just initiate-and-watch-your-own.

```python
# Python — observe a stream you didn't start, and publish to any stream
client = await AgentBusClient("http://127.0.0.1:6815").connect()
sub = await client.subscribe("some-stream-id")     # -> Subscription (async-iterable)
async for ev in sub:
    print(ev.cid, ev.type, ev.data)                 # every event on that stream
# from another client / elsewhere:
await client.publish("some-stream-id", "feed.item", {"n": 1})
await sub.unsubscribe()
```

```js
// JavaScript (ES6) — identical shape
const sub = await client.subscribe("some-stream-id");
for await (const ev of sub) console.log(ev.cid, ev.type, ev.data);
await client.publish("some-stream-id", "feed.item", { n: 1 });
await sub.unsubscribe();
```

Semantics: **observer** — every subscriber to a stream sees every event. There is no
work distribution or ack here; for that, see §12.

---

## 12. Server-side consumer — the glide `BusClient` (Python only)

When a **service** needs real **consumer-group** semantics — competing consumers that
load-balance a stream, `ack`, at-least-once redelivery, crash recovery — the Socket.IO
gateway is the wrong tool (it has no group API). Use the glide **`BusClient`**, which
talks to Valkey directly:

```python
from agent_bus_client.bus import BusClient, make_consumer   # pip install agent-bus-client[bus]

bus = await BusClient.create("valkey-bus", 6379)
group, consumer = "cg:my-service", make_consumer("my-service")   # consumer encodes host+pid
await bus.ensure_group(stream, group, start="$")                  # idempotent (BUSYGROUP-safe)

while True:
    for d in await bus.read_group([stream], group, consumer, count=32, block_ms=2000):
        handle(d.envelope)                  # d.envelope.header.cid + .sid = idempotency key
        await bus.ack(d.stream, group, [d.entry_id])
    # crash recovery: reclaim entries abandoned by dead consumers
    _cursor, claimed = await bus.reclaim(stream, group, consumer, min_idle_ms=30000)
```

- **At-least-once** — a reclaimed entry is redelivered; dedupe in your handler on
  `cid`+`sid`, not in the client.
- **Poison routing** — unparseable entries go to `stream:dlq` and are acked, so one bad
  message can't wedge the group.
- **Same client, both directions** — `BusClient.publish(stream, envelope)` produces, too.
- **glibc only** (`valkey-glide` has no musl/alpine wheels); server-side, not browser.
- The canonical `EventEnvelope` is exported (`from agent_bus_client import EventEnvelope`,
  `new_event`) — import it instead of vendoring a copy.

| Need | Use |
|---|---|
| Browser / initiator; publish + observe a stream | `AgentBusClient` (Socket.IO gateway) |
| Server worker; consume a shared stream with ack/at-least-once | `BusClient` (glide, direct Valkey) |

---

## 13. Quick reference

```
connect            →  receive  connected { stream_id }
emit  request {text}  →  ack    { cid }              # start a workflow on your stream
emit  publish {stream_id, event_type, data}          # publish to any stream
emit  subscribe {stream_id}                          # observe any stream
receive  event { header:{stream_id,cid,sid,...}, payload:{data,context}, metadata:{...} }
route              →  by header.stream_id (Subscription) or header.cid (Workflow)
worker (server)    →  BusClient: ensure_group / read_group / ack / reclaim  (§12)
```

For the system design behind this contract, see
[technical_architecture.md](technical_architecture.md).
