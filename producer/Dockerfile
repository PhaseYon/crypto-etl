FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HEALTH_FILE=/tmp/producer-alive

WORKDIR /app
RUN useradd --system --uid 10001 --no-create-home producer

# Dependencies first so code changes don't invalidate this layer
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY pyproject.toml ./
COPY crypto_producer ./crypto_producer
RUN pip install --no-deps . && rm -rf build *.egg-info

USER producer

# Alive = the feed (trades or the per-second heartbeat) touched HEALTH_FILE in the last 2 minutes
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["python", "-c", "import os, sys, time; sys.exit(time.time() - os.path.getmtime(os.environ['HEALTH_FILE']) > 120)"]

ENTRYPOINT ["python", "-m", "crypto_producer"]
