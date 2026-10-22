# Read-only dashboard API. No secrets are baked in: credentials come from the environment.
FROM python:3.14.4-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --system --uid 10001 --home-dir /app app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY crypto_api ./crypto_api

USER app
EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/livez', timeout=2)"]

# One worker: the source cache and ingest-rate tracker are per process
CMD ["uvicorn", "crypto_api.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips", "*"]
