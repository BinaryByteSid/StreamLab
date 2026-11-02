"""Admin CLI for the Control Plane (diagram section 6: "Admin UI / CLI").

    docker compose exec api python -m swma.cli list
    docker compose exec api python -m swma.cli get window/tumbling_size_s
    docker compose exec api python -m swma.cli set window/tumbling_size_s 30
    docker compose exec api python -m swma.cli reset
"""
from __future__ import annotations

import sys

from swma.common.config import SPECS, SPEC_BY_KEY, LiveConfig, coerce, put


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return 0
    cmd = argv[0]
    if cmd == "list":
        cfg = LiveConfig()
        cfg._apply(*cfg._fetch(0, wait=None))
        group = None
        for s in SPECS:
            if s.group != group:
                group = s.group
                print(f"\n[{group}]")
            mark = "" if cfg.get(s.key) == s.default else "   (changed)"
            print(f"  {s.key:38} = {cfg.get(s.key)!s:20}{mark}")
        return 0
    if cmd == "get" and len(argv) == 2:
        cfg = LiveConfig()
        cfg._apply(*cfg._fetch(0, wait=None))
        print(cfg.get(argv[1]))
        return 0
    if cmd == "set" and len(argv) == 3:
        key, value = argv[1], argv[2]
        if key not in SPEC_BY_KEY:
            print(f"unknown key {key}; run `list` to see them", file=sys.stderr)
            return 2
        put(key, coerce(key, value))
        print(f"{key} = {coerce(key, value)}  (pushed to Consul; services pick it up in ~1s)")
        return 0
    if cmd == "reset":
        for s in SPECS:
            put(s.key, s.default)
        print("all settings reset to defaults")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
