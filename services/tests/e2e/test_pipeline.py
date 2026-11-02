"""End-to-end tests against the running stack.

    docker compose exec -T -e STREAMLAB_E2E=1 api-1 python -m pytest tests/e2e -q
"""
import os
import time
import uuid

import httpx
import pytest

pytestmark = pytest.mark.skipif(os.getenv("STREAMLAB_E2E") != "1", reason="needs the running stack")
BASE = os.getenv("STREAMLAB_URL", "http://alb")
KEY = {"X-API-Key": "lab-public-key"}


def wait_trace(eid, until, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(f"{BASE}/api/lab/trace/{eid}")
        if r.status_code == 200 and until(r.json()["trace"]):
            return r.json()
        time.sleep(1)
    raise AssertionError(f"trace for {eid} never satisfied the condition")


def send(**body):
    r = httpx.post(f"{BASE}/api/lab/events", json=body, timeout=20)
    r.raise_for_status()
    return r.json()["events"][0]


def test_every_component_healthy():
    # One snapshot can catch a slow health check (2 s timeout); retry for a minute.
    deadline = time.time() + 60
    while True:
        d = httpx.get(f"{BASE}/api/lab/status", timeout=15).json()
        down = [c["id"] for c in d["components"] if not c["ok"]]
        if not down or time.time() > deadline:
            break
        time.sleep(3)
    assert not down, f"down: {down}"


def test_event_is_processed_and_windowed():
    ev = send(service="payment-service", response_time_ms=123)
    t = wait_trace(ev["event_id"], lambda t: t.get("outcome"))
    assert t["trace"]["outcome"] in ("on_time", "out_of_order")
    assert len(t["window_results"]) == 7  # 1 tumbling + 6 sliding


def test_duplicate_is_ignored():
    eid = f"e2e-dup-{uuid.uuid4().hex[:8]}"
    send(service="cart-service", event_id=eid)
    wait_trace(eid, lambda t: t.get("outcome"))
    send(service="cart-service", event_id=eid)
    wait_trace(eid, lambda t: t.get("outcome") == "duplicate")


def test_too_late_event_goes_to_dlq():
    ev = send(service="cart-service", delay_s=600)
    wait_trace(ev["event_id"], lambda t: t.get("outcome") == "too_late")


def test_malformed_event_goes_to_dlq():
    ev = send(service="cart-service", malformed="wrong_type")
    t = wait_trace(ev["event_id"], lambda t: t.get("outcome") == "dead_letter")
    assert t["trace"]["dlq_reason"] == "schema_invalid"


def test_public_api_needs_key_and_is_rate_limited():
    assert httpx.get(f"{BASE}/api/v1/metrics/payment-service").status_code == 401
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(20) as pool:  # a burst, faster than the bucket refills
        codes = list(pool.map(lambda _: httpx.get(f"{BASE}/api/v1/services", headers=KEY).status_code, range(40)))
    assert 200 in codes and 429 in codes


def test_final_windows_match_ground_truth():
    d = httpx.get(f"{BASE}/api/lab/correctness?limit=2", timeout=15).json()
    assert d["checked"] > 0
    assert d["matched"] == d["checked"], [r for r in d["rows"] if not r["match"]]
