"""EchoAgent — stand-in for an LLM agent (no brain call yet).

* On the opening ``request`` it emits an ``agent.thought``.
* On a ``tool.result`` it either concludes (emitting ``workflow.terminated``)
  or, when ``ECHO_LOOP`` is set, emits another ``agent.thought`` — turning the
  agent/tool pair into a forever ping-pong (a synthetic outlier for testing
  iteration-visibility and the terminate kill switch).

When real agents land, ``handle`` becomes a streaming agent_server call that
yields one OutEvent per delta (architecture §8); the seam is unchanged.
"""

from __future__ import annotations

from typing import AsyncIterator

from ..actor import BaseActor, OutEvent
from ..envelope import EventEnvelope, EventType


class EchoAgent(BaseActor):
    subscribes_to = frozenset({EventType.REQUEST, EventType.TOOL_RESULT})

    async def handle(self, env: EventEnvelope) -> AsyncIterator[OutEvent]:
        if env.header.event_type == EventType.REQUEST:
            prompt = env.payload.data.get("text", "")
            yield OutEvent(EventType.AGENT_THOUGHT, {"text": f"thinking about: {prompt}"})
            return

        # event_type == TOOL_RESULT
        if self._settings.echo_loop:
            result = env.payload.data.get("result", "")
            yield OutEvent(EventType.AGENT_THOUGHT, {"text": f"reconsidering: {result}"})
        else:
            yield OutEvent(EventType.WORKFLOW_TERMINATED, {"reason": "echo complete"})
