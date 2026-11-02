"""Telemetry event model: JSON Schema, service catalog and time helpers."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import httpx
from jsonschema import Draft202012Validator

log = logging.getLogger("swma.schema")

# Registered in the Redpanda Schema Registry under "telemetry.events-value".
EVENT_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://streamlab.local/schemas/telemetry-event-v1.json",
    "title": "TelemetryEvent",
    "description": "One request handled by an e-commerce microservice.",
    "type": "object",
    "required": ["event_id", "service", "event_time", "response_time_ms", "status_code"],
    "properties": {
        "event_id": {"type": "string", "minLength": 3, "maxLength": 100,
                     "description": "Unique id, used for duplicate detection"},
        "service": {"type": "string", "pattern": "^[a-z][a-z0-9-]{1,40}$",
                    "description": "Microservice that produced the event (also the Kafka key)"},
        "event_time": {"type": "string", "minLength": 10,
                       "description": "When the request happened, ISO-8601 UTC"},
        "endpoint": {"type": "string"},
        "method": {"type": "string"},
        "response_time_ms": {"type": "number", "minimum": 0, "maximum": 600000},
        "status_code": {"type": "integer", "minimum": 100, "maximum": 599},
        "cpu_percent": {"type": "number", "minimum": 0, "maximum": 100},
        "memory_percent": {"type": "number", "minimum": 0, "maximum": 100},
        "region": {"type": "string"},
        "source": {"type": "string"},
        "trace": {"type": "boolean", "description": "Lab only: record this event's journey"},
    },
    "additionalProperties": True,
}

# Used by the producer (base behaviour) and by the processor (enrichment).
SERVICE_CATALOG: dict[str, dict] = {
    "api-gateway":       {"team": "platform", "tier": "critical", "latency_ms": 45,  "error_pct": 0.4, "weight": 3,
                          "endpoints": ["/", "/api/v1/route", "/health"]},
    "order-service":     {"team": "checkout", "tier": "critical", "latency_ms": 120, "error_pct": 0.8, "weight": 2,
                          "endpoints": ["/orders", "/orders/{id}"]},
    "payment-service":   {"team": "checkout", "tier": "critical", "latency_ms": 180, "error_pct": 1.0, "weight": 2,
                          "endpoints": ["/pay", "/refund"]},
    "product-service":   {"team": "catalog",  "tier": "standard", "latency_ms": 60,  "error_pct": 0.5, "weight": 3,
                          "endpoints": ["/products", "/products/{id}", "/search"]},
    "cart-service":      {"team": "checkout", "tier": "standard", "latency_ms": 50,  "error_pct": 0.5, "weight": 2,
                          "endpoints": ["/cart", "/cart/items"]},
    "inventory-service": {"team": "catalog",  "tier": "standard", "latency_ms": 90,  "error_pct": 0.7, "weight": 1,
                          "endpoints": ["/stock/{sku}", "/reserve"]},
    "user-service":      {"team": "identity", "tier": "standard", "latency_ms": 70,  "error_pct": 0.3, "weight": 1,
                          "endpoints": ["/login", "/profile"]},
}


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_event_time(value) -> int:
    """ISO-8601 string (or epoch millis) -> epoch millis. Raises ValueError."""
    if isinstance(value, bool):
        raise ValueError("event_time must be a timestamp")
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str) or len(value) < 10:
        raise ValueError("event_time must be an ISO-8601 string")
    text = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class Validator:
    """Schema validation step. Uses the registry's schema when available."""

    def __init__(self, schema: dict | None = None):
        self.schema = schema or EVENT_SCHEMA
        self.source = "schema-registry" if schema else "built-in"
        self._v = Draft202012Validator(self.schema)

    def errors(self, event: dict) -> list[str]:
        out = []
        for err in self._v.iter_errors(event):
            where = ".".join(str(p) for p in err.path) or "event"
            out.append(f"{where}: {err.message}")
            if len(out) >= 5:
                break
        return out


def fetch_registry_schema(registry_url: str, subject: str) -> tuple[dict | None, int | None]:
    try:
        r = httpx.get(f"{registry_url}/subjects/{subject}/versions/latest", timeout=5)
        if r.status_code == 200:
            body = r.json()
            return json.loads(body["schema"]), body.get("version")
    except Exception as exc:
        log.warning("schema registry unavailable (%s); using built-in schema", exc)
    return None, None
