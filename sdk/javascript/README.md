# agent-bus-client (JavaScript / ES6)

High-level ES6 client for the **Agent Bus** Socket.IO gateway. Start workflows
and `for await` their events without hand-rolling Socket.IO or cid-correlation.
Works in the browser and in Node.

> Wraps the **bus** protocol (envelopes + commands). Unrelated to the
> `agent_server` SDK (the LLM brain). Wire reference:
> [../../documents/client_sdk.md](../../documents/client_sdk.md).

## Install

**Node / bundlers:**

```bash
npm install socket.io-client     # peer; the SDK imports it
```

**Browser (no bundler):** map the bare import to socket.io's ESM CDN build:

```html
<script type="importmap">
  { "imports": { "socket.io-client": "https://cdn.socket.io/4.7.5/socket.io.esm.min.js" } }
</script>
<script type="module">
  import { AgentBusClient } from "./src/agent-bus-client.js";
  // ...
</script>
```

## Quick start

```js
import { AgentBusClient } from "agent-bus-client";

const client = new AgentBusClient("http://127.0.0.1:6815");
await client.connect();

const wf = await client.start("summarize the onboarding doc");
for await (const ev of wf) {            // only THIS workflow's events
  console.log(ev.sid, ev.type, ev.data);
}
await wf.completed;                     // true=terminated, false=disconnected
client.disconnect();
```

## API

### `new AgentBusClient(url, options?)`
- `await connect(timeout=10000)` / `disconnect()`.
- `await start(text, { idleTimeout=null }) -> Workflow`.
- `streamId` — this connection's dedicated stream id.
- Low-level (by `cid`): `await request(text) -> cid`, `await status(cid)`,
  `await terminate(cid)`. `options` pass through to `io(url, options)`.

### `Workflow`
- **async-iterable** — `for await (const ev of wf)` yields this workflow's
  `BusEvent`s until it terminates, disconnects, or (if `idleTimeout` seconds set)
  goes quiet.
- `await wf.completed` — `true` on `workflow.terminated`, `false` on disconnect.
- `wf.sid` — latest step seen = **live iteration count**.
- `await wf.terminate()` — kill switch. `await wf.status()` — snapshot.
- `await wf.collect()` — drain to completion, return all events.

### `BusEvent`
`raw`, `cid`, `sid`, `type`, `sender`, `timestamp`, `data`, `isTerminal`.

## Outlier detection

No automatic step cap — watch the live `sid` and `terminate()` runaways:

```js
for await (const ev of wf) {
  if (ev.sid > 500 && !ev.isTerminal) await wf.terminate();
}
```

See [examples/](examples/): `node-basic.mjs`, `outlier-kill.mjs`, `browser.html`.
