"""FastAPI backend (diagram section 4).

Runs as two replicas (api-1, api-2) behind the nginx load balancer.
Every response carries ``X-Served-By`` so you can watch the load balancing.
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from swma.common import log as logsetup, settings

from . import explore, lab, public, ws
from .deps import C

REQUESTS = Counter("swma_api_requests_total", "HTTP requests", ["method", "route", "status"])
LATENCY = Histogram("swma_api_request_seconds", "HTTP request latency", ["route"],
                    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logsetup.setup("api")
    C.start()
    yield
    await C.stop()


app = FastAPI(
    title="StreamLab API - Streaming Window Metrics Aggregator",
    description=(
        "Real-time window metrics computed from the telemetry stream.\n\n"
        "**Public API (`/api/v1`)** needs an API key: click *Authorize* and enter `lab-public-key`. "
        "It is rate limited (see the Control Plane).\n\n"
        "**Lab API (`/api/lab`)** powers the virtual lab. Actions that change the system need an admin "
        "JWT from `POST /api/auth/token`."
    ),
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url=None,
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)


@app.middleware("http")
async def observe(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    path = getattr(route, "path", "unmatched")
    if path != "/metrics":
        REQUESTS.labels(request.method, path, str(response.status_code)).inc()
        LATENCY.labels(path).observe(time.perf_counter() - t0)
    response.headers["X-Served-By"] = settings.INSTANCE
    return response


@app.get("/api/health", tags=["health"])
async def health():
    return {"status": "ok", "instance": settings.INSTANCE}


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


app.include_router(public.router)
app.include_router(lab.router)
app.include_router(lab.alerts_router)
app.include_router(explore.router)
app.include_router(ws.router)
