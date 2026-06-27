# agent-bus-client (Python)

Python client for the **Agent Bus**, in **two surfaces**:

1. **`AgentBusClient`** — the Socket.IO **gateway** client: **publish**, **subscribe**
   (observe any stream), and `start`/observe your own workflows. Browser-capable.
2. **`agent_bus_client.bus.BusClient`** — the glide **direct-Valkey** client:
   **consumer groups** (`read_group`/`ack`/`reclaim`) + `publish`. Server-side workers.

> Wraps the **bus** protocol (envelopes + commands). Unrelated to the
> `agent_server` SDK, which wraps the LLM brain. Full wire reference:
> [../../documents/client_sdk.md](../../documents/client_sdk.md).

## Install

```bash
pip install -e sdk/python            # gateway client (python-socketio, aiohttp)
pip install -e 'sdk/python[bus]'     # + the glide BusClient (valkey-glide; glibc only)
```

## Quick start

```python
import asyncio
from agent_bus_client import AgentBusClient

async def main():
    async with AgentBusClient("http://127.0.0.1:6815") as client:
        wf = await client.start("summarize the onboarding doc")
        async for ev in wf:                 # only THIS workflow's events
            print(ev.sid, ev.type, ev.data)
        await wf.completed                  # True=terminated, False=disconnected

asyncio.run(main())
```

## API

### `AgentBusClient(url, *, reconnection=True, **sio_kwargs)`
- `await connect(timeout=10)` / `await disconnect()` — also usable as
  `async with AgentBusClient(url) as client: ...`.
- `await start(text, *, idle_timeout=None) -> Workflow` — start a workflow.
- `await publish(stream_id, event_type, data=None, *, cid=None) -> {cid,sid,entry_id}`
  — publish to **any** stream.
- `await subscribe(stream_id) -> Subscription` / `await unsubscribe(stream_id)` —
  observe **any** stream (events you didn't produce).
- `stream_id` — this connection's dedicated stream id.
- Low-level passthroughs (by `cid`): `await request(text) -> cid`,
  `await status(cid)`, `await terminate(cid)`.

### `Subscription`
- **async-iterable** — `async for ev in sub` yields **every** event on the stream
  until `await sub.unsubscribe()` or disconnect. (Observer semantics — every
  subscriber sees every event; not consumer-group work distribution.)

### `Workflow`
- **async-iterable** — `async for ev in wf` yields this workflow's `Event`s
  until it terminates, disconnects, or (if `idle_timeout` set) goes quiet.
- `await wf.completed` — resolves `True` on `workflow.terminated`, `False` on
  disconnect.
- `wf.sid` — latest step seen = **live iteration count** (watch for outliers).
- `await wf.terminate()` — the kill switch. `await wf.status()` — snapshot.
- `await wf.collect(timeout=None)` — drain to completion, return all events.

### `Event`
`raw`, `cid`, `sid`, `type`, `sender`, `timestamp`, `data`, `is_terminal`.

## Outlier detection

There is no automatic step cap — workflows run until they end. Use the live
`sid` to detect runaways and `terminate()` to eliminate them:

```python
async for ev in wf:
    if ev.sid > 500 and not ev.is_terminal:
        await wf.terminate()
```

See [examples/](examples/) — `basic.py` and `outlier_kill.py`.

## Server-side consumer — `BusClient` (glide, direct Valkey)

For a service that needs **consumer groups** (competing consumers, ack,
at-least-once, crash recovery), the gateway is the wrong tool. Use `BusClient`:

```python
from agent_bus_client.bus import BusClient, make_consumer
from agent_bus_client import EventEnvelope, new_event       # canonical model — don't vendor

bus = await BusClient.create("valkey-bus", 6379)
group, consumer = "cg:my-service", make_consumer("my-service")   # encodes host+pid
await bus.ensure_group(stream, group, start="$")                  # idempotent

for d in await bus.read_group([stream], group, consumer, count=32, block_ms=2000):
    handle(d.envelope)                          # d.envelope.header.cid + .sid = idempotency key
    await bus.ack(d.stream, group, [d.entry_id])

_cursor, claimed = await bus.reclaim(stream, group, consumer, min_idle_ms=30000)  # recovery
await bus.publish(stream, new_event(...))        # same client also produces
```

- **At-least-once**: a reclaimed entry is redelivered — dedupe on `cid`+`sid` in your
  handler, not the client. **Poison** entries are DLQ'd + acked automatically.
- **glibc only** (`valkey-glide`), server-side. Install with the `[bus]` extra.
