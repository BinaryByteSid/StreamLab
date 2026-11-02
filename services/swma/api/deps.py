"""Shared clients, authentication (JWT + API keys) and rate limiting."""
from __future__ import annotations

import threading
import time

import httpx
import jwt
import redis
import redis.asyncio as aioredis
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from fastapi import HTTPException, Request
from minio import Minio
from prometheus_client import Counter

from swma.common import settings
from swma.common.config import LiveConfig

RATE_LIMITED = Counter("swma_api_rate_limited_total", "Requests rejected by the rate limiter", ["client"])
AUTH_FAILURES = Counter("swma_api_auth_failures_total", "Rejected credentials", ["kind"])

TOKEN_BUCKET = """
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local rate, burst, now = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local tokens = tonumber(b[1]) or burst
local ts = tonumber(b[2]) or now
tokens = math.min(burst, tokens + (now - ts) / 1000 * rate)
local allowed = 0
if tokens >= 1 then tokens = tokens - 1 allowed = 1 end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 60000)
return {allowed, tostring(tokens)}
"""


class Clients:
    def __init__(self) -> None:
        self.r: aioredis.Redis | None = None
        self.rs: redis.Redis | None = None
        self.http: httpx.AsyncClient | None = None
        self.producer: Producer | None = None
        self.admin: AdminClient | None = None
        self.consumer: Consumer | None = None
        self.consumer_lock = threading.Lock()
        self.s3: Minio | None = None
        self.cfg: LiveConfig | None = None
        self.bucket = None

    def start(self) -> None:
        self.r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=3)
        self.rs = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=3)
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(4.0, connect=2.0))
        self.producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP, "client.id": f"{settings.INSTANCE}-api",
                                  "acks": "all", "enable.idempotence": True, "linger.ms": 5})
        self.reset_kafka()
        self.s3 = Minio(settings.S3_ENDPOINT, settings.S3_ACCESS_KEY, settings.S3_SECRET_KEY, secure=False)
        self.cfg = LiveConfig().start()
        self.bucket = self.r.register_script(TOKEN_BUCKET)

    def reset_kafka(self) -> None:
        """(Re)create the inspection clients, e.g. after a broker restart left them
        pointing at a stale group coordinator."""
        old = self.consumer
        self.admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP})
        # Never subscribes: only used to read committed offsets / watermarks.
        self.consumer = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP, "group.id": settings.CONSUMER_GROUP,
                                  "client.id": f"{settings.INSTANCE}-inspector", "enable.auto.commit": False,
                                  "topic.metadata.refresh.interval.ms": 5000})
        if old is not None:
            try:
                old.close()
            except Exception:
                pass

    async def stop(self) -> None:
        if self.http:
            await self.http.aclose()
        if self.producer:
            self.producer.flush(3)


C = Clients()


# ------------------------------------------------------------------ auth
def create_token(user: str) -> dict:
    now = int(time.time())
    token = jwt.encode({"sub": user, "role": "admin", "iat": now, "exp": now + 8 * 3600},
                       settings.JWT_SECRET, algorithm="HS256")
    return {"access_token": token, "token_type": "bearer", "expires_in": 8 * 3600}


def _bearer(request: Request) -> dict | None:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return None
    try:
        return jwt.decode(auth[7:], settings.JWT_SECRET, algorithms=["HS256"])
    except jwt.PyJWTError:
        AUTH_FAILURES.labels("jwt").inc()
        raise HTTPException(401, "Invalid or expired token. Sign in again.")


async def reader(request: Request) -> dict:
    """Public API: an API key (X-API-Key header) or a JWT."""
    key = request.headers.get("x-api-key") or request.query_params.get("api_key")
    if key:
        if key in settings.LAB_API_KEYS:
            return {"kind": "api_key", "id": key}
        AUTH_FAILURES.labels("api_key").inc()
        raise HTTPException(401, "Unknown API key")
    claims = _bearer(request)
    if claims:
        return {"kind": "jwt", "id": claims["sub"]}
    AUTH_FAILURES.labels("missing").inc()
    raise HTTPException(401, "Send an X-API-Key header (demo key: lab-public-key) or a Bearer token",
                        headers={"WWW-Authenticate": "Bearer"})


async def admin(request: Request) -> dict:
    """Lab actions that change the system need a signed-in admin (JWT)."""
    claims = _bearer(request)
    if not claims:
        raise HTTPException(401, "Admin sign-in required for this action", headers={"WWW-Authenticate": "Bearer"})
    if claims.get("role") != "admin":
        raise HTTPException(403, "Admin role required")
    return claims


async def rate_limit(request: Request) -> None:
    ident = request.headers.get("x-api-key") or (request.client.host if request.client else "anon")
    rate = C.cfg.get("api/rate_limit_rps")
    burst = C.cfg.get("api/rate_limit_burst")
    try:
        allowed, left = await C.bucket(keys=[f"ratelimit:{ident}"], args=[rate, burst, int(time.time() * 1000)])
    except Exception:
        return  # fail open if Redis is down: availability over strictness
    request.state.rate_headers = {"X-RateLimit-Limit": str(rate), "X-RateLimit-Burst": str(burst),
                                  "X-RateLimit-Remaining": str(int(float(left)))}
    if not int(allowed):
        RATE_LIMITED.labels(ident[:20]).inc()
        raise HTTPException(429, f"Rate limit exceeded: {rate} requests/second (burst {burst}). Slow down.",
                            headers={"Retry-After": "1", **request.state.rate_headers})
