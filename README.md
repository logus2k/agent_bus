# Agent Bus

A reactive, event-driven agentic communication backbone. Components (agents,
tools, judges) are autonomous actors that subscribe to event types on a
[Valkey](https://valkey.io/) Streams bus and emit downstream consequences —
choreography instead of a central orchestrator.

See [documents/technical_architecture.md](documents/technical_architecture.md)
for the design and [documents/implementation_plan.md](documents/implementation_plan.md)
for the build order.

## Status: first slice (infra-only)

The choreography backbone is implemented and proven end to end with **echo
actors** (no LLM yet). The brain (the `agent_server` LLM service) drops in
behind the existing actor seam — see architecture §8.

What works today:

- **Event envelope** contract (Pydantic) with per-workflow `cid` and atomic
  `INCR`-based `sid` sequencing.
- **EventBus** over [valkey-glide](https://github.com/valkey-io/valkey-glide)
  (the only module touching the client): `XADD` / `XREADGROUP` / `XACK` /
  `XAUTOCLAIM` / `XGROUP`, DLQ routing, observer `XREAD`.
- **Per-initiator streams** discovered via a polled `streams:active` Set;
  groups attach at id `0` so there's no attach/publish race.
- **No auto-cap termination** — workflows run unbounded; they end explicitly
  (`terminate` command / an actor) or naturally (flow goes quiet). Live
  iteration count (`sid`) is streamed for outlier detection; `MAX_THRESHOLD>0`
  is an optional runaway backstop (default 0 = unlimited).
- **Reaper** crash-recovery via `XAUTOCLAIM` (reclaims dead consumers' PEL).
- **Stream cleanup** on disconnect + idle-TTL safety net.
- **Socket.IO gateway** — `request` / `publish` / `subscribe` / `terminate` / `status`;
  mirrors stream events to clients.
- **Client SDKs** ([sdk/](sdk/)) — two surfaces:
  - **Gateway clients** (Python + JS/ES6): publish, **subscribe** to any stream, and
    drive/observe workflows. Browser-capable.
  - **glide `BusClient`** (Python, `sdk/python` `[bus]` extra): direct-Valkey
    **consumer groups** (`read_group`/`ack`/`reclaim`) + `publish` for server-side
    workers — the surface `agent_runtime` consumes. Exports the canonical `EventEnvelope`.

## Architecture at a glance

```
Browser ⇄ Socket.IO ⇄ Gateway ⇄ Valkey streams ⇄ Echo actors (agent ⇄ tool)
                                      │
                                  Reaper (XAUTOCLAIM)
```

## Run it

```bash
docker compose up -d --build          # Valkey + the mono-process app
docker logs -f agent-bus-app          # actors, reaper, gateway

# Echo runs a finite request->thought->result->terminated by default.
# Make it ping-pong forever (to exercise iteration-visibility + the terminate
# kill switch), optionally with a runaway backstop:
ECHO_LOOP=true docker compose up -d agent-bus-app
ECHO_LOOP=true MAX_THRESHOLD=20 docker compose up -d agent-bus-app
```

The gateway listens on `127.0.0.1:6815` (Socket.IO). Connect, emit a
`request` event `{"text": "..."}`, and receive the workflow's events streamed
back as `event` frames until `workflow.terminated`.

### Web console

The gateway also serves a **session console** dashboard at
`http://127.0.0.1:6815/` — start workflows, watch each one's live event trace
and **iteration counter**, and terminate stalled/runaway workflows (set a step
budget to flag ones exceeding it). It's a vanilla ES6 app on the JS SDK
([webclient/](webclient/)), scoped to the workflows this browser starts.
Try it with the echo loop on: `ECHO_LOOP=true docker compose up -d agent-bus-app`.

## Test

```bash
.venv_agent_bus/bin/python -m pytest -q                       # unit tests
VALKEY_HOST=127.0.0.1 .venv_agent_bus/bin/python -m pytest -m integration  # needs live Valkey
```

## Layout

```
src/agent_bus/
  config.py     envelope.py   bus.py        registry.py
  discovery.py  actor.py      reaper.py     cleanup.py    gateway.py   app.py
  actors/       echo_agent.py echo_tool.py
tests/          documents/    docker-compose.yml  Dockerfile  requirements.txt
```
