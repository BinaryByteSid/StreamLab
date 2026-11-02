"""WebSockets: live window results / lab events, and a live Kafka tail."""
from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid

from confluent_kafka import Consumer, TopicPartition
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from prometheus_client import Gauge

from swma.common import labbus, settings

from .deps import C

router = APIRouter()
WS_CLIENTS = Gauge("swma_api_websocket_clients", "Open WebSocket connections", ["channel"])
TOPICS = (settings.TOPIC_EVENTS, settings.TOPIC_DLQ, settings.TOPIC_METRICS)


@router.websocket("/ws/live")
async def live(ws: WebSocket):
    """Pushes every window emission and every lab timeline event."""
    await ws.accept()
    WS_CLIENTS.labels("live").inc()
    pubsub = C.r.pubsub()
    await pubsub.subscribe("swma:updates", labbus.CHANNEL)
    try:
        await ws.send_json({"type": "hello", "served_by": settings.INSTANCE})
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is None:
                continue
            data = json.loads(msg["data"])
            if msg["channel"] == labbus.CHANNEL:
                await ws.send_json({"type": "lab", "event": data})
            else:
                await ws.send_json(data)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        WS_CLIENTS.labels("live").dec()
        await pubsub.unsubscribe()
        await pubsub.aclose()


def _tail_thread(topic: str, loop, queue: asyncio.Queue, stop: threading.Event, max_rate: int) -> None:
    c = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP, "group.id": f"lab-viewer-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False, "client.id": "lab-stream-viewer"})
    try:
        md = c.list_topics(topic, timeout=5)
        tps = []
        for p in md.topics[topic].partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=3)
            tps.append(TopicPartition(topic, p, max(lo, hi - 3)))
        c.assign(tps)
        sec, sent, skipped = int(time.time()), 0, 0
        while not stop.is_set():
            batch = []
            for m in c.consume(200, 0.3):
                if m.error():
                    continue
                now = int(time.time())
                if now != sec:
                    sec, sent = now, 0
                if sent >= max_rate:
                    skipped += 1
                    continue
                sent += 1
                raw = (m.value() or b"").decode("utf-8", "replace")
                try:
                    value = json.loads(raw)
                except Exception:
                    value = raw[:1500]
                batch.append({"partition": m.partition(), "offset": m.offset(),
                              "key": (m.key() or b"").decode("utf-8", "replace"),
                              "timestamp": m.timestamp()[1], "value": value})
            if batch or skipped:
                loop.call_soon_threadsafe(queue.put_nowait, {"type": "messages", "topic": topic,
                                                             "items": batch, "skipped": skipped})
                skipped = 0
    except Exception as exc:
        loop.call_soon_threadsafe(queue.put_nowait, {"type": "error", "error": str(exc)})
    finally:
        c.close()


@router.websocket("/ws/stream")
async def stream(ws: WebSocket, topic: str = settings.TOPIC_EVENTS, rate: int = 25):
    """Live Kafka viewer: every message of a topic with partition and offset."""
    await ws.accept()
    if topic not in TOPICS:
        await ws.send_json({"type": "error", "error": "unknown topic"})
        await ws.close()
        return
    WS_CLIENTS.labels("stream").inc()
    queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    stop = threading.Event()
    t = threading.Thread(target=_tail_thread, args=(topic, asyncio.get_running_loop(), queue, stop,
                                                    max(1, min(rate, 100))), daemon=True)
    t.start()
    try:
        await ws.send_json({"type": "hello", "topic": topic, "served_by": settings.INSTANCE})
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=15)
            except asyncio.TimeoutError:
                item = {"type": "ping"}
            await ws.send_json(item)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        stop.set()
        WS_CLIENTS.labels("stream").dec()
