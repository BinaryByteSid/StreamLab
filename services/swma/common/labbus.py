"""Lab timeline: a human-readable feed of what just happened in the system.

Every component writes short events ("worker processor-2 crashed",
"partition 3 restored from RocksDB at offset 1520") to a Redis Stream so the
virtual lab can explain each moment to the learner.
"""
from __future__ import annotations

import json
import time

TIMELINE = "lab:timeline"
CHANNEL = "swma:lab"


def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time() * 1000) % 1000:03d}Z"


def emit(r, kind: str, message: str, **data) -> None:
    item = {"ts": iso_now(), "kind": kind, "message": message, **data}
    try:
        payload = json.dumps(item, default=str)
        r.xadd(TIMELINE, {"data": payload}, maxlen=500, approximate=True)
        r.publish(CHANNEL, payload)
    except Exception:
        pass  # the timeline is best-effort; never break the pipeline for it
