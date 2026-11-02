"""Read-only explorers so the lab can show what is *inside* each service."""
from __future__ import annotations

import base64
import json
import time

from fastapi import APIRouter, HTTPException
from starlette.concurrency import run_in_threadpool

from swma.common import settings

from .deps import C

router = APIRouter(prefix="/api/explore", tags=["explore"])


# ------------------------------------------------------------------- Redis
@router.get("/redis/keys")
async def redis_keys(pattern: str = "*", limit: int = 200):
    keys = []
    async for k in C.r.scan_iter(match=pattern, count=500):
        keys.append(k)
        if len(keys) >= limit:
            break
    keys.sort()
    pipe = C.r.pipeline(transaction=False)
    for k in keys:
        pipe.type(k)
        pipe.ttl(k)
    meta = await pipe.execute()
    info = await C.r.info("memory")
    return {"keys": [{"key": k, "type": meta[i * 2], "ttl": meta[i * 2 + 1]} for i, k in enumerate(keys)],
            "dbsize": await C.r.dbsize(), "used_memory_human": info.get("used_memory_human")}


@router.get("/redis/key")
async def redis_key(key: str):
    t = await C.r.type(key)
    if t == "string":
        v = await C.r.get(key)
    elif t == "hash":
        v = await C.r.hgetall(key)
    elif t == "zset":
        v = await C.r.zrevrange(key, 0, 49, withscores=True)
    elif t == "list":
        v = await C.r.lrange(key, 0, 49)
    elif t == "set":
        v = sorted(await C.r.smembers(key))[:100]
    elif t == "stream":
        v = [{"id": i, **f} for i, f in await C.r.xrevrange(key, count=20)]
    else:
        raise HTTPException(404, "key not found")
    return {"key": key, "type": t, "ttl": await C.r.ttl(key), "value": v}


# ------------------------------------------------------------------ Consul
@router.get("/consul/kv")
async def consul_kv(prefix: str = "swma/"):
    r = await C.http.get(f"{settings.CONSUL_URL}/v1/kv/{prefix}", params={"recurse": "true"})
    if r.status_code == 404:
        return []
    return [{"key": i["Key"], "value": base64.b64decode(i["Value"]).decode() if i.get("Value") else "",
             "modify_index": i["ModifyIndex"]} for i in r.json()]


@router.get("/consul/services")
async def consul_services():
    r = await C.http.get(f"{settings.CONSUL_URL}/v1/agent/checks")
    checks = r.json()
    out = [{"service": c.get("ServiceName"), "status": c.get("Status"), "output": (c.get("Output") or "")[:160],
            "check": c.get("Name")} for c in checks.values()]
    return sorted(out, key=lambda x: x["service"] or "")


# ---------------------------------------------------------------------- S3
@router.get("/s3/buckets")
async def s3_buckets():
    def go():
        out = []
        for b in C.s3.list_buckets():
            n, size = 0, 0
            for o in C.s3.list_objects(b.name, recursive=True):
                n += 1
                size += o.size or 0
                if n >= 5000:
                    break
            out.append({"bucket": b.name, "objects": n, "bytes": size})
        return out
    return await run_in_threadpool(go)


@router.get("/s3/objects")
async def s3_objects(bucket: str, prefix: str = "", limit: int = 100):
    def go():
        objs = []
        for o in C.s3.list_objects(bucket, prefix=prefix or None, recursive=False):
            objs.append({"key": o.object_name, "size": o.size, "dir": o.is_dir,
                         "modified": o.last_modified.isoformat() if o.last_modified else None})
        objs.sort(key=lambda o: (not o["dir"], o["key"]), reverse=False)
        return objs[-limit:] if len(objs) > limit else objs
    return await run_in_threadpool(go)


@router.get("/s3/object")
async def s3_object(bucket: str, key: str):
    def go():
        resp = C.s3.get_object(bucket, key)
        try:
            data = resp.read(200_000)
        finally:
            resp.close()
            resp.release_conn()
        text = data.decode("utf-8", "replace")
        try:
            return {"key": key, "json": json.loads(text)}
        except Exception:
            return {"key": key, "text": text[:20000]}
    return await run_in_threadpool(go)


# ---------------------------------------------------------------- Registry
@router.get("/registry")
async def registry():
    r = await C.http.get(f"{settings.REGISTRY_URL}/v2/_catalog")
    repos = r.json().get("repositories", [])
    out = []
    for repo in repos:
        t = await C.http.get(f"{settings.REGISTRY_URL}/v2/{repo}/tags/list")
        out.append({"repository": repo, "tags": sorted(t.json().get("tags") or [])})
    return out


# --------------------------------------------------- Prometheus and Loki
@router.get("/prometheus/query")
async def prom_query(query: str):
    r = await C.http.get(f"{settings.PROMETHEUS_URL}/api/v1/query", params={"query": query})
    return r.json()


@router.get("/prometheus/range")
async def prom_range(query: str, minutes: int = 10, step: int = 5):
    end = time.time()
    r = await C.http.get(f"{settings.PROMETHEUS_URL}/api/v1/query_range",
                         params={"query": query, "start": end - minutes * 60, "end": end, "step": step})
    return r.json()


@router.get("/prometheus/targets")
async def prom_targets():
    r = await C.http.get(f"{settings.PROMETHEUS_URL}/api/v1/targets")
    return [{"job": t["labels"].get("job"), "instance": t["labels"].get("instance"), "health": t["health"],
             "last_error": t.get("lastError"), "last_scrape": t.get("lastScrape")}
            for t in r.json()["data"]["activeTargets"]]


@router.get("/loki")
async def loki(query: str = '{kind=~".+"}', limit: int = 60, minutes: int = 15):
    end = time.time_ns()
    r = await C.http.get(f"{settings.LOKI_URL}/loki/api/v1/query_range",
                         params={"query": query, "limit": limit, "start": end - minutes * 60 * 10**9,
                                 "end": end, "direction": "backward"})
    if r.status_code != 200:
        raise HTTPException(400, r.text[:300])
    lines = []
    for stream in r.json()["data"]["result"]:
        for ts, line in stream["values"]:
            try:
                parsed = json.loads(line)
            except Exception:
                parsed = {"msg": line}
            lines.append({"ts": int(ts), "labels": stream["stream"], "line": parsed})
    lines.sort(key=lambda x: x["ts"], reverse=True)
    return lines[:limit]


# --------------------------------------------------------- Schema Registry
@router.get("/schema")
async def schema():
    r = await C.http.get(f"{settings.SCHEMA_REGISTRY_URL}/subjects/{settings.SCHEMA_SUBJECT}/versions/latest")
    if r.status_code != 200:
        raise HTTPException(404, "schema not registered yet")
    body = r.json()
    return {"subject": body.get("subject"), "version": body.get("version"), "id": body.get("id"),
            "type": body.get("schemaType", "AVRO"), "schema": json.loads(body["schema"])}
