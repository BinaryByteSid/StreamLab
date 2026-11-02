"""Structured logging: JSON lines on stdout + push to Loki.

Loki receives three kinds of logs (label ``kind``): ``app`` (normal),
``error`` (level >= ERROR) and ``audit`` (who changed what in the lab).
"""
from __future__ import annotations

import json
import logging
import queue
import threading
import time

import httpx

from . import settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "service": settings.SERVICE_NAME,
            "instance": settings.INSTANCE,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class LokiHandler(logging.Handler):
    """Batches log lines and pushes them to Loki's HTTP API every second."""

    def __init__(self, url: str):
        super().__init__()
        self.url = url.rstrip("/") + "/loki/api/v1/push"
        self.q: queue.Queue = queue.Queue(maxsize=10000)
        self.setFormatter(JsonFormatter())
        threading.Thread(target=self._loop, name="loki-push", daemon=True).start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            kind = getattr(record, "kind", None) or ("error" if record.levelno >= logging.ERROR else "app")
            self.q.put_nowait((kind, record.levelname.lower(), str(time.time_ns()), self.format(record)))
        except queue.Full:
            pass

    def _loop(self) -> None:
        client = httpx.Client(timeout=5)
        while True:
            time.sleep(1.0)
            batch = []
            while not self.q.empty() and len(batch) < 2000:
                batch.append(self.q.get_nowait())
            if not batch:
                continue
            streams: dict[tuple, list] = {}
            for kind, level, ts, line in batch:
                streams.setdefault((kind, level), []).append([ts, line])
            body = {"streams": [
                {"stream": {"service": settings.SERVICE_NAME, "instance": settings.INSTANCE,
                            "kind": kind, "level": level}, "values": values}
                for (kind, level), values in streams.items()
            ]}
            try:
                client.post(self.url, json=body)
            except Exception:
                pass  # Loki down: drop logs rather than block the pipeline


_configured = False


def setup(service: str | None = None, loki: bool = True) -> logging.Logger:
    global _configured
    if service:
        settings.SERVICE_NAME = service
    root = logging.getLogger()
    if not _configured:
        root.setLevel(logging.INFO)
        for h in list(root.handlers):
            root.removeHandler(h)
        out = logging.StreamHandler()
        out.setFormatter(JsonFormatter())
        root.addHandler(out)
        if loki:
            root.addHandler(LokiHandler(settings.LOKI_URL))
        for noisy in ("httpx", "httpcore", "kafka", "urllib3", "uvicorn.access", "docker"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
        _configured = True
    return logging.getLogger(service or settings.SERVICE_NAME)


def audit(logger: logging.Logger, action: str, **fields) -> None:
    logger.info(action, extra={"kind": "audit", "fields": {"audit": True, "action": action, **fields}})
