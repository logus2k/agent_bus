"""WorkflowRegistry — per-correlation-id state and the Termination Guard.

Two responsibilities (architecture §2 and §5):

* **Sequencing:** ``next_sid(cid)`` allocates a monotonic step id with a
  single atomic ``INCR sid:<cid>`` — no distributed lock, no WATCH/MULTI.
* **Termination as shared agreement:** every actor runs ``guard(env)`` before
  handling an event. The first actor to see ``sid >= MAX_THRESHOLD`` flips the
  workflow to TERMINATED; everyone else thereafter drops its events.
"""

from __future__ import annotations

import enum

from .bus import EventBus
from .config import Settings
from .envelope import EventEnvelope

STATUS_RUNNING = "RUNNING"
STATUS_TERMINATED = "TERMINATED"


class Guard(enum.Enum):
    """Outcome of the termination guard for one event."""

    PROCEED = "proceed"            # handle normally
    TERMINATE_NOW = "terminate"    # this event crossed the threshold; emit workflow.terminated then stop
    DROP = "drop"                  # workflow already terminated; ack and ignore


class WorkflowRegistry:
    def __init__(self, bus: EventBus, settings: Settings):
        self._bus = bus
        self._settings = settings

    def _sid_key(self, cid: str) -> str:
        return f"sid:{cid}"

    def _state_key(self, cid: str) -> str:
        return f"state:{cid}"

    async def next_sid(self, cid: str) -> int:
        """Atomically allocate the next step id for a workflow.

        The TTL is refreshed on *every* step so an active (even forever-running)
        workflow's counter is never reaped mid-flight, while an abandoned one
        still self-cleans after STREAM_TTL_S of inactivity.
        """
        key = self._sid_key(cid)
        value = await self._bus.incr(key)
        await self._bus.expire(key, self._settings.stream_ttl_s)
        return value

    async def current_sid(self, cid: str) -> int:
        """The latest allocated step id (live iteration count); 0 if unknown."""
        raw = await self._bus.get_str(self._sid_key(cid))
        return int(raw) if raw is not None else 0

    async def status(self, cid: str) -> str:
        value = await self._bus.get_str(self._state_key(cid))
        return value or STATUS_RUNNING

    async def is_terminated(self, cid: str) -> bool:
        return (await self.status(cid)) == STATUS_TERMINATED

    async def set_terminated(self, cid: str) -> None:
        """Unconditionally mark a workflow TERMINATED (voluntary/explicit end)."""
        key = self._state_key(cid)
        await self._bus.set_str(key, STATUS_TERMINATED)
        await self._bus.expire(key, self._settings.stream_ttl_s)

    async def try_terminate(self, cid: str) -> bool:
        """Atomically elect the single terminator. Returns True iff *this* caller
        flipped the workflow to TERMINATED (SET NX); concurrent crossers get False."""
        key = self._state_key(cid)
        won = await self._bus.set_if_absent(key, STATUS_TERMINATED)
        if won:
            await self._bus.expire(key, self._settings.stream_ttl_s)
        return won

    async def guard(self, env: EventEnvelope) -> Guard:
        """The shared Termination Guard, run before processing any event.

        With ``max_threshold == 0`` (the default) there is no automatic step
        cap: workflows run until explicitly terminated. A positive threshold
        re-enables the runaway backstop.
        """
        cid = env.header.cid
        if await self.is_terminated(cid):
            return Guard.DROP
        cap = self._settings.max_threshold
        if cap > 0 and env.header.sid >= cap:
            # First crosser terminates and emits; any racing crosser just drops.
            return Guard.TERMINATE_NOW if await self.try_terminate(cid) else Guard.DROP
        return Guard.PROCEED
