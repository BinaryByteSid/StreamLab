"""State Store: one RocksDB database per Kafka partition (on local disk).

Each checkpoint writes the *changed* window states, the dedup ids and the next
Kafka offset in a single atomic WriteBatch. Because state and offset are saved
together, a restarted worker restores exactly the state that matches the
offset it resumes from -- no double counting after a crash.
"""
from __future__ import annotations

import os
import shutil
import time

from rocksdict import Options, Rdict, WriteBatch


class StateStore:
    def __init__(self, base_dir: str, partition: int, open_timeout_s: float = 20) -> None:
        self.partition = partition
        self.path = os.path.join(base_dir, f"partition-{partition}")
        self.existed = os.path.isdir(self.path) and bool(os.listdir(self.path))
        opts = Options(raw_mode=False)
        opts.create_if_missing(True)
        deadline = time.time() + open_timeout_s
        while True:  # another worker may still be closing it during a rebalance
            try:
                self.db = Rdict(self.path, opts)
                break
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(0.25)
        self.last_checkpoint: float | None = None
        self.checkpoints = 0

    def items(self):
        for k, v in self.db.items():
            yield k, v

    def is_empty(self) -> bool:
        for _ in self.db.keys():
            return False
        return True

    def write(self, puts: dict[str, str], deletes: list[str]) -> None:
        wb = WriteBatch(raw_mode=False)
        for k, v in puts.items():
            wb.put(k, v)
        for k in deletes:
            wb.delete(k)
        self.db.write(wb)
        self.last_checkpoint = time.time()
        self.checkpoints += 1

    def replace_all(self, items: dict[str, str]) -> None:
        for k in list(self.db.keys()):
            del self.db[k]
        self.write(items, [])

    def size_bytes(self) -> int:
        total = 0
        for root, _, files in os.walk(self.path):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
        return total

    def close(self) -> None:
        try:
            self.db.close()
        except Exception:
            pass


def wipe_all(base_dir: str) -> int:
    """Lab 'disk failure': delete every partition's local state."""
    n = 0
    if os.path.isdir(base_dir):
        for name in os.listdir(base_dir):
            if name.startswith("partition-"):
                shutil.rmtree(os.path.join(base_dir, name), ignore_errors=True)
                n += 1
    return n
