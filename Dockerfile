# Agent Bus — mono-process app container (actors + reaper + gateway).
#
# IMPORTANT: base image MUST be glibc (Debian slim), NOT alpine/MUSL.
# valkey-glide ships a Rust core with no MUSL wheels, so an Alpine base
# would fail to install/run the client. The Valkey *server* container may
# stay on Alpine — glide never runs there.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first for layer caching.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Application source (package lives under src/agent_bus).
COPY src/ ./src/
ENV PYTHONPATH=/app/src

# Runs the mono-process entrypoint (actors + reaper + gateway under uvicorn).
CMD ["python", "-m", "agent_bus.app"]
