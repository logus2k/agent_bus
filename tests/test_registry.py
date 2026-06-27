"""WorkflowRegistry guard tests — fake in-memory bus, no Valkey."""

from dataclasses import replace

from agent_bus.config import settings
from agent_bus.envelope import EventType, new_event
from agent_bus.registry import Guard, WorkflowRegistry


class FakeBus:
    """Implements just the EventBus surface the registry touches."""

    def __init__(self):
        self.counters: dict[str, int] = {}
        self.kv: dict[str, str] = {}

    async def incr(self, key):
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def get_str(self, key):
        if key in self.kv:
            return self.kv[key]
        if key in self.counters:  # INCR-backed keys (e.g. sid:<cid>)
            return str(self.counters[key])
        return None

    async def set_if_absent(self, key, value):
        if key in self.kv:
            return False
        self.kv[key] = value
        return True

    async def expire(self, key, seconds):
        pass


def _ev(sid, cid="wf"):
    return new_event(stream_id="c", cid=cid, sid=sid, sender="s",
                     event_type=EventType.AGENT_THOUGHT)


async def test_next_sid_monotonic():
    reg = WorkflowRegistry(FakeBus(), settings)
    assert [await reg.next_sid("wf") for _ in range(3)] == [1, 2, 3]


async def test_guard_proceeds_below_threshold():
    s = replace(settings, max_threshold=10)
    reg = WorkflowRegistry(FakeBus(), s)
    assert await reg.guard(_ev(1)) is Guard.PROCEED


async def test_guard_no_cap_never_terminates():
    s = replace(settings, max_threshold=0)  # unlimited (the default)
    reg = WorkflowRegistry(FakeBus(), s)
    assert await reg.guard(_ev(1_000_000)) is Guard.PROCEED


async def test_current_sid_tracks_latest():
    reg = WorkflowRegistry(FakeBus(), settings)
    assert await reg.current_sid("wf") == 0
    await reg.next_sid("wf")
    await reg.next_sid("wf")
    assert await reg.current_sid("wf") == 2


async def test_guard_terminates_at_threshold_once():
    s = replace(settings, max_threshold=5)
    reg = WorkflowRegistry(FakeBus(), s)
    # First crosser wins -> TERMINATE_NOW; a racing crosser -> DROP.
    assert await reg.guard(_ev(5)) is Guard.TERMINATE_NOW
    assert await reg.guard(_ev(5)) is Guard.DROP


async def test_guard_drops_after_terminated():
    s = replace(settings, max_threshold=5)
    reg = WorkflowRegistry(FakeBus(), s)
    await reg.try_terminate("wf")
    assert await reg.guard(_ev(2)) is Guard.DROP  # below threshold but already terminated
