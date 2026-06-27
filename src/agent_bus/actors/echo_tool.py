"""EchoTool — stand-in for a tool executor (no real tool yet).

Reacts to ``agent.thought`` and emits a ``tool.result``, which EchoAgent then
reacts to — the ping-pong half that keeps the workflow stepping until the
guard terminates it.
"""

from __future__ import annotations

from typing import AsyncIterator

from ..actor import BaseActor, OutEvent
from ..envelope import EventEnvelope, EventType


class EchoTool(BaseActor):
    subscribes_to = frozenset({EventType.AGENT_THOUGHT})

    async def handle(self, env: EventEnvelope) -> AsyncIterator[OutEvent]:
        thought = env.payload.data.get("text", "")
        yield OutEvent(
            event_type=EventType.TOOL_RESULT,
            data={"result": f"echo({thought})"},
        )
