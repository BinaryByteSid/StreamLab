# One multi-stage Dockerfile, one image per component.
# Every component shares the "base" layers, so building all four is fast.

FROM python:3.12.16-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN useradd --create-home --uid 10001 swma \
 && mkdir -p /state /tmp/prom-mp \
 && chown -R swma:swma /state /tmp/prom-mp
COPY pytest.ini ./
COPY swma ./swma
COPY tests ./tests
LABEL org.opencontainers.image.source="StreamLab virtual lab" \
      org.opencontainers.image.description="Streaming Window Metrics Aggregator"

# 1. Event Sources: simulated e-commerce microservices
FROM base AS producer
USER swma
CMD ["python", "-m", "swma.producer"]

# 3. Stream Processing Engine: supervisor + worker processes
FROM base AS processor
USER swma
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prom-mp
CMD ["python", "-m", "swma.processor"]

# 4. FastAPI backend (also runs the one-shot bootstrap job)
FROM base AS api
USER swma
CMD ["uvicorn", "swma.api.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

# Lab helper: chaos actions, container stats and the CI/CD pipeline runner.
# Runs as root because it talks to the Docker socket.
FROM base AS ops-agent
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/* \
 && git config --system --add safe.directory '*'
CMD ["uvicorn", "swma.opsagent.main:app", "--host", "0.0.0.0", "--port", "8095", "--no-access-log"]
