"""Ops agent: the lab's hands on Docker.

* /containers           every lab container with state, CPU and memory
* /containers/x/action  chaos actions (only on containers labelled swma.chaos=true)
* /ci/run               a real CI/CD pipeline: checkout -> unit tests -> security scan ->
                        docker build -> push to the registry -> rolling deploy behind the ALB
* /metrics              container CPU / memory for Prometheus (a tiny cAdvisor)

Only reachable on the internal Docker network and only with the shared token.
"""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import docker
import httpx
import redis
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from swma.common import labbus, log as logsetup, settings

PROJECT = os.getenv("COMPOSE_PROJECT", "swma-lab")
WORKSPACE = os.getenv("WORKSPACE", "/workspace")
PUSH_HOST = os.getenv("REGISTRY_PUSH_HOST", "localhost:5001")
ACTIONS = {"stop", "start", "restart", "pause", "unpause", "kill"}

CPU = Gauge("swma_container_cpu_percent", "Container CPU %", ["container"])
MEM = Gauge("swma_container_memory_bytes", "Container memory", ["container"])
UP = Gauge("swma_container_up", "1 if the container is running", ["container"])

app = FastAPI(title="StreamLab ops agent")
log = logsetup.setup("ops-agent")
client = docker.from_env()
r = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=3)
stats: dict[str, dict] = {}
ci_state: dict = {"run": None}
ci_lock = threading.Lock()


def _auth(token: str | None) -> None:
    if token != settings.OPS_AGENT_TOKEN:
        raise HTTPException(403, "bad ops token")


def lab_containers(all_: bool = True):
    return client.containers.list(all=all_, filters={"label": f"com.docker.compose.project={PROJECT}"})


def _service(c) -> str:
    return c.labels.get("com.docker.compose.service", c.name)


def _collect_one(c) -> None:
    name = _service(c)
    try:
        s = c.stats(stream=False)
        cpu_d = s["cpu_stats"]["cpu_usage"]["total_usage"] - s["precpu_stats"]["cpu_usage"]["total_usage"]
        sys_d = s["cpu_stats"].get("system_cpu_usage", 0) - s["precpu_stats"].get("system_cpu_usage", 0)
        ncpu = s["cpu_stats"].get("online_cpus") or 1
        cpu = (cpu_d / sys_d) * ncpu * 100 if sys_d > 0 else 0.0
        mem = s["memory_stats"].get("usage", 0) - s["memory_stats"].get("stats", {}).get("inactive_file", 0)
        stats[name] = {"cpu": round(cpu, 1), "mem": max(0, mem)}
        CPU.labels(name).set(round(cpu, 2))
        MEM.labels(name).set(max(0, mem))
    except Exception:
        stats.pop(name, None)


def _stats_loop() -> None:
    pool = ThreadPoolExecutor(8)
    while True:
        try:
            cs = lab_containers(all_=True)
            for c in cs:
                UP.labels(_service(c)).set(1 if c.status == "running" else 0)
            list(pool.map(_collect_one, [c for c in cs if c.status == "running"]))
        except Exception as exc:
            log.warning("stats loop: %s", exc)
        time.sleep(10)


threading.Thread(target=_stats_loop, daemon=True).start()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/metrics", include_in_schema=False)
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/containers")
def containers(x_ops_token: str | None = Header(None)):
    _auth(x_ops_token)
    out = []
    for c in lab_containers():
        name = _service(c)
        health = (c.attrs.get("State", {}).get("Health") or {}).get("Status")
        out.append({"service": name, "container": c.name, "status": c.status, "health": health,
                    "image": (c.image.tags or [c.image.short_id])[0] if c.image else None,
                    "chaos_allowed": c.labels.get("swma.chaos") == "true",
                    "section": c.labels.get("swma.section"), "role": c.labels.get("swma.role"),
                    "started_at": c.attrs.get("State", {}).get("StartedAt"),
                    "restart_count": c.attrs.get("RestartCount", 0),
                    "cpu": stats.get(name, {}).get("cpu"), "mem_bytes": stats.get(name, {}).get("mem")})
    return sorted(out, key=lambda x: (int(x["section"] or 99), x["service"]))


@app.post("/containers/{service}/{action}")
def container_action(service: str, action: str, x_ops_token: str | None = Header(None)):
    _auth(x_ops_token)
    if action not in ACTIONS:
        raise HTTPException(400, f"action must be one of {sorted(ACTIONS)}")
    for c in lab_containers():
        if _service(c) == service:
            if c.labels.get("swma.chaos") != "true":
                raise HTTPException(403, f"{service} is protected (not labelled swma.chaos=true)")
            getattr(c, action)()
            log.info("chaos: %s %s", action, service)
            return {"ok": True, "service": service, "action": action}
    raise HTTPException(404, f"no container for service {service}")


# ------------------------------------------------------------------ CI / CD
STAGES = [
    ("checkout", "Code commit (Git)"),
    ("test", "Build & unit tests (pytest)"),
    ("scan", "Security scan (bandit)"),
    ("build", "Build Docker images"),
    ("push", "Push to container registry"),
    ("deploy", "Rolling deploy behind the load balancer"),
]
IMAGES = [("producer", "services", "producer"), ("processor", "services", "processor"),
          ("api", "services", "api"), ("ops-agent", "services", "ops-agent"), ("dashboard", "portal", None)]


def _publish(run: dict) -> None:
    try:
        r.set("lab:ci:latest", json.dumps(run), ex=7 * 24 * 3600)
        r.publish(labbus.CHANNEL, json.dumps({"kind": "ci", "message": f"CI {run['status']}", "ts": labbus.iso_now(),
                                              "run": {"id": run["id"], "status": run["status"]}}))
    except Exception:
        pass


def _sh(cmd: list[str], cwd: str, timeout: int = 300) -> tuple[int, str]:
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    return p.returncode, (p.stdout + p.stderr)[-6000:]


def _stage_checkout(run):
    code, out = _sh(["git", "log", "-1", "--pretty=format:%h|%an|%ar|%s"], WORKSPACE)
    if code != 0:
        run["commit"] = {"sha": time.strftime("local-%Y%m%d%H%M%S"), "message": "working copy (no git commit yet)"}
        return True, "No git history found - building the current working copy.\n" + out
    sha, author, when, msg = out.split("|", 3)
    run["commit"] = {"sha": sha, "author": author, "when": when, "message": msg}
    _, status = _sh(["git", "status", "--short"], WORKSPACE)
    return True, f"commit {sha} by {author} ({when})\n  {msg}\n\nuncommitted changes:\n{status or '  none'}"


def _stage_test(run):
    code, out = _sh(["python", "-m", "pytest", "tests/unit", "-q", "-p", "no:cacheprovider"],
                    os.path.join(WORKSPACE, "services"))
    run["tests"] = out.strip().splitlines()[-1] if out.strip() else ""
    return code == 0, out


def _stage_scan(run):
    report = "/tmp/bandit.json"
    code, out = _sh(["bandit", "-r", "swma", "-q", "-f", "json", "-o", report, "--severity-level", "medium"],
                    os.path.join(WORKSPACE, "services"))
    try:
        with open(report, encoding="utf-8") as fh:
            data = json.load(fh)
        issues = data.get("results", [])
        high = [i for i in issues if i["issue_severity"] == "HIGH"]
        lines = [f"{i['issue_severity']:6} {i['filename']}:{i['line_number']}  {i['issue_text']}" for i in issues]
        run["scan"] = {"medium_or_higher": len(issues), "high": len(high)}
        summary = f"{len(issues)} medium/high findings, {len(high)} high\n" + "\n".join(lines[:30])
        return not high, summary
    except Exception:
        return code == 0, out


def _stage_build(run):
    tag = run["commit"]["sha"]
    logs = []
    for name, ctx, target in IMAGES:
        t0 = time.time()
        repo = f"{PUSH_HOST}/swma/{name}"
        image, _ = client.images.build(path=os.path.join(WORKSPACE, ctx), target=target, tag=f"{repo}:{tag}",
                                       rm=True, labels={"swma.commit": tag})
        image.tag(repo, "latest")
        logs.append(f"built {repo}:{tag}  ({image.short_id}, {time.time() - t0:.1f}s)")
    return True, "\n".join(logs)


def _stage_push(run):
    tag, logs = run["commit"]["sha"], []
    for name, _, _ in IMAGES:
        repo = f"{PUSH_HOST}/swma/{name}"
        for t in (tag, "latest"):
            err = None
            for line in client.images.push(repo, tag=t, stream=True, decode=True):
                if "error" in line:
                    err = line["error"]
            if err:
                return False, "\n".join(logs) + f"\npush {repo}:{t} failed: {err}"
            logs.append(f"pushed {repo}:{t}")
    return True, "\n".join(logs)


def _stage_deploy(run):
    """Rolling restart of the API replicas while a probe hits the load balancer."""
    probe = {"ok": 0, "fail": 0, "stop": False}

    def hammer():
        with httpx.Client(timeout=2) as h:
            while not probe["stop"]:
                try:
                    probe["ok" if h.get("http://alb/api/health").status_code == 200 else "fail"] += 1
                except Exception:
                    probe["fail"] += 1
                time.sleep(0.1)

    t = threading.Thread(target=hammer, daemon=True)
    t.start()
    logs = []
    for svc in ("api-1", "api-2"):
        c = next(c for c in lab_containers() if _service(c) == svc)
        logs.append(f"restarting {svc} ... (the load balancer routes traffic to the other replica)")
        c.restart(timeout=10)
        for _ in range(60):
            try:
                if httpx.get(f"http://{svc}:8000/api/health", timeout=1).status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        logs.append(f"{svc} healthy again")
    time.sleep(1)
    probe["stop"] = True
    t.join(3)
    run["deploy"] = {"requests_ok": probe["ok"], "requests_failed": probe["fail"]}
    logs.append(f"\nprobe through the ALB during the deploy: {probe['ok']} OK, {probe['fail']} failed")
    logs.append("On AWS this step runs `docker compose pull && docker compose up -d` on the EC2 hosts "
                "(see deploy/aws and .github/workflows/cd.yml).")
    return True, "\n".join(logs)


RUNNERS = {"checkout": _stage_checkout, "test": _stage_test, "scan": _stage_scan,
           "build": _stage_build, "push": _stage_push, "deploy": _stage_deploy}


def _run_pipeline(run: dict) -> None:
    for st in run["stages"]:
        st["status"], st["started_at"] = "running", labbus.iso_now()
        _publish(run)
        t0 = time.time()
        try:
            ok, out = RUNNERS[st["id"]](run)
        except Exception as exc:
            ok, out = False, f"{type(exc).__name__}: {exc}"
        st["status"], st["seconds"], st["log"] = ("passed" if ok else "failed"), round(time.time() - t0, 1), out
        _publish(run)
        if not ok:
            run["status"] = "failed"
            for rest in run["stages"]:
                if rest["status"] == "pending":
                    rest["status"] = "skipped"
            break
    else:
        run["status"] = "passed"
    run["finished_at"] = labbus.iso_now()
    _publish(run)
    labbus.emit(r, "ci", f"CI/CD pipeline {run['status'].upper()} for commit {run['commit']['sha'] if run.get('commit') else '?'}")


@app.post("/ci/run")
def ci_run(body: dict | None = None, x_ops_token: str | None = Header(None)):
    _auth(x_ops_token)
    with ci_lock:
        cur = ci_state["run"]
        if cur and cur["status"] == "running":
            raise HTTPException(409, "a pipeline is already running")
        run = {"id": int(time.time()), "status": "running", "started_at": labbus.iso_now(),
               "triggered_by": (body or {}).get("user", "?"), "registry": PUSH_HOST,
               "stages": [{"id": i, "name": n, "status": "pending", "log": ""} for i, n in STAGES]}
        ci_state["run"] = run
    labbus.emit(r, "ci", f"CI/CD pipeline started by {run['triggered_by']}")
    threading.Thread(target=_run_pipeline, args=(run,), daemon=True).start()
    return run


@app.get("/ci/latest")
def ci_latest(x_ops_token: str | None = Header(None)):
    _auth(x_ops_token)
    if ci_state["run"]:
        return ci_state["run"]
    raw = r.get("lab:ci:latest")
    return json.loads(raw) if raw else {"status": "never-run", "stages": [
        {"id": i, "name": n, "status": "pending", "log": ""} for i, n in STAGES]}
