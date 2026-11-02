"""Public REST API (diagram section 4): versioned, authenticated, rate limited."""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from swma.common import settings
from swma.common.log import audit
from swma.common.schema import SERVICE_CATALOG

from .deps import AUTH_FAILURES, C, create_token, rate_limit, reader

router = APIRouter()
WTYPES = ("tumbling", "sliding")


class Login(BaseModel):
    username: str
    password: str


@router.post("/api/auth/token", tags=["auth"], summary="Get a JWT (lab admin sign-in)")
async def login(body: Login, request: Request):
    import logging
    if body.username != settings.LAB_ADMIN_USER or body.password != settings.LAB_ADMIN_PASSWORD:
        AUTH_FAILURES.labels("password").inc()
        audit(logging.getLogger("api"), "login_failed", user=body.username)
        raise HTTPException(401, "Wrong username or password")
    audit(logging.getLogger("api"), "login", user=body.username)
    return create_token(body.username)


def _check_type(window_type: str) -> str:
    if window_type not in WTYPES:
        raise HTTPException(400, "window_type must be tumbling or sliding")
    return window_type


async def _pointer(key: str) -> dict | None:
    raw = await C.r.hget(key, "json")
    return json.loads(raw) if raw else None


def _apply_headers(request: Request, response: Response) -> None:
    for k, v in getattr(request.state, "rate_headers", {}).items():
        response.headers[k] = v


guard = [Depends(reader), Depends(rate_limit)]


@router.get("/api/v1/services", tags=["metrics"], dependencies=guard, summary="Monitored microservices")
async def services(request: Request, response: Response):
    _apply_headers(request, response)
    return {"services": [{"name": k, "team": v["team"], "tier": v["tier"]} for k, v in SERVICE_CATALOG.items()]}


@router.get("/api/v1/metrics", tags=["metrics"], dependencies=guard, summary="Latest metrics for every service")
async def all_metrics(request: Request, response: Response, window_type: str = "tumbling"):
    _apply_headers(request, response)
    _check_type(window_type)
    out = {}
    for svc in SERVICE_CATALOG:
        out[svc] = {"last_complete": await _pointer(f"metrics:last_closed:{window_type}:{svc}"),
                    "live": await _pointer(f"metrics:latest:{window_type}:{svc}")}
    return {"window_type": window_type, "services": out, "served_by": settings.INSTANCE}


@router.get("/api/v1/metrics/{service}", tags=["metrics"], dependencies=guard,
            summary="Latest tumbling + sliding metrics for one service")
async def service_metrics(service: str, request: Request, response: Response):
    _apply_headers(request, response)
    return {wt: {"last_complete": await _pointer(f"metrics:last_closed:{wt}:{service}"),
                 "live": await _pointer(f"metrics:latest:{wt}:{service}")} for wt in WTYPES}


@router.get("/api/v1/metrics/{service}/latest", tags=["metrics"], dependencies=guard)
async def service_latest(service: str, request: Request, response: Response, window_type: str = "tumbling"):
    _apply_headers(request, response)
    res = await _pointer(f"metrics:last_closed:{_check_type(window_type)}:{service}")
    if not res:
        raise HTTPException(404, "No complete window yet for this service")
    return res


@router.get("/api/v1/metrics/{service}/history", tags=["metrics"], dependencies=guard,
            summary="Recent windows (newest first)")
async def service_history(service: str, request: Request, response: Response,
                          window_type: str = "tumbling", limit: int = 30):
    _apply_headers(request, response)
    return {"service": service, "window_type": window_type,
            "windows": await history(service, _check_type(window_type), min(max(limit, 1), 300))}


@router.get("/api/v1/windows/{window_type}/{window_id}", tags=["metrics"], dependencies=guard,
            summary="One window, all services (Redis key metrics:<type>:<window_id>)")
async def one_window(window_type: str, window_id: str, request: Request, response: Response):
    _apply_headers(request, response)
    data = await C.r.hgetall(f"metrics:{_check_type(window_type)}:{window_id}")
    if not data:
        raise HTTPException(404, "Window not found (expired or not created yet)")
    return {"window_id": window_id, "window_type": window_type,
            "services": {k: json.loads(v) for k, v in data.items()}}


async def history(service: str, window_type: str, limit: int) -> list[dict]:
    ids = await C.r.zrevrange(f"metrics:history:{window_type}:{service}", 0, limit - 1)
    if not ids:
        return []
    pipe = C.r.pipeline(transaction=False)
    for wid in ids:
        pipe.hget(f"metrics:{window_type}:{wid}", service)
    return [json.loads(v) for v in await pipe.execute() if v]
