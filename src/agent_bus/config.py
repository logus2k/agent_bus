"""Environment-driven configuration.

A single immutable ``Settings`` instance, populated from the environment
(with a ``.env`` loaded in dev). Every knob has a safe default so the app
runs with an empty environment. Keep ALL tunables here — no magic numbers
scattered across the codebase.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Load .env if present (no-op in containers that inject real env vars).
load_dotenv()


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return default if raw is None or raw == "" else raw


@dataclass(frozen=True)
class Settings:
    # --- Valkey connection ---
    valkey_host: str = _str("VALKEY_HOST", "127.0.0.1")
    valkey_port: int = _int("VALKEY_PORT", 6379)

    # --- Discovery / well-known keys ---
    active_streams_key: str = _str("ACTIVE_STREAMS_KEY", "streams:active")
    stream_prefix: str = _str("STREAM_PREFIX", "stream:")
    dlq_stream: str = _str("DLQ_STREAM", "stream:dlq")

    # --- Actor consume loop ---
    actor_poll_ms: int = _int("ACTOR_POLL_MS", 100)

    # --- Workflow lifecycle ---
    # Optional runaway backstop. 0 = unlimited (no automatic step cap) — the
    # default. Termination is otherwise explicit (an actor/client emits
    # workflow.terminated) or natural (the reaction-chain goes quiet). The live
    # step count (sid) is surfaced to clients for outlier detection.
    max_threshold: int = _int("MAX_THRESHOLD", 0)

    # Echo demo: when True the echo actors ping-pong forever (a synthetic
    # outlier for testing iteration-visibility and the terminate kill switch);
    # when False they run a finite request -> thought -> result -> terminated.
    echo_loop: bool = _str("ECHO_LOOP", "false").lower() in ("1", "true", "yes")

    # --- Reaper (XAUTOCLAIM) ---
    reaper_min_idle_ms: int = _int("REAPER_MIN_IDLE_MS", 30_000)
    reaper_interval_s: int = _int("REAPER_INTERVAL_S", 15)

    # --- Stream cleanup ---
    stream_ttl_s: int = _int("STREAM_TTL_S", 3_600)

    # --- Gateway (Socket.IO) ---
    gateway_host: str = _str("GATEWAY_HOST", "0.0.0.0")
    gateway_port: int = _int("GATEWAY_PORT", 6815)

    # --- Logging ---
    log_level: str = _str("LOG_LEVEL", "INFO")

    def stream_key(self, initiator_id: str) -> str:
        """The dedicated stream key for an initiator: ``stream:<initiator_id>``."""
        return f"{self.stream_prefix}{initiator_id}"


settings = Settings()
