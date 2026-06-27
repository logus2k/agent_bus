# agent-bus-client (Python)

High-level Python client for the **Agent Bus** Socket.IO gateway. Start
workflows and async-iterate their events without hand-rolling Socket.IO,
cid-correlation, or lifecycle.

> Wraps the **bus** protocol (envelopes + commands). Unrelated to the
> `agent_server` SDK, which wraps the LLM brain. Full wire reference:
> [../../documents/client_sdk.md](../../documents/client_sdk.md).

## Install

```bash
pip install -e sdk/python      # from the repo, editable
# deps: python-socketio[asyncio_client], aiohttp
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
- `stream_id` — this connection's dedicated stream id.
- Low-level passthroughs (by `cid`): `await request(text) -> cid`,
  `await status(cid)`, `await terminate(cid)`.

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
