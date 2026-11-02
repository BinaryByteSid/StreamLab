"""Window assignment.

All windows are epoch-aligned and half-open: ``[start, end)``. An event exactly
on a boundary belongs to the window that *starts* there.
"""
from __future__ import annotations


def tumbling(ts_ms: int, size_ms: int) -> list[tuple[int, int]]:
    """Each event belongs to exactly one tumbling window."""
    start = ts_ms - (ts_ms % size_ms)
    return [(start, start + size_ms)]


def sliding(ts_ms: int, size_ms: int, slide_ms: int) -> list[tuple[int, int]]:
    """Each event belongs to every window with ``start <= ts < start + size``.

    With size=60s and slide=10s that is 6 overlapping windows.
    """
    out = []
    start = ts_ms - (ts_ms % slide_ms)
    while start > ts_ms - size_ms:
        out.append((start, start + size_ms))
        start -= slide_ms
    out.reverse()
    return out


def window_id(start_ms: int, end_ms: int) -> str:
    """Readable, sortable id: '<start epoch s>-<end epoch s>'."""
    return f"{start_ms // 1000}-{end_ms // 1000}"
