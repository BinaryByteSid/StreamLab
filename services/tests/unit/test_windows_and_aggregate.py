"""Window Correctness Tests + aggregation tests (diagram section 7, Automated Tests)."""
from swma.engine.aggregate import Aggregate
from swma.engine.watermark import Watermark
from swma.engine.windows import sliding, tumbling, window_id

T0 = 1_700_000_040_000  # an exact minute boundary (epoch ms)


def test_tumbling_boundaries_are_half_open():
    assert tumbling(T0, 60_000) == [(T0, T0 + 60_000)]
    assert tumbling(T0 + 59_999, 60_000) == [(T0, T0 + 60_000)]
    # exactly at the end -> next window
    assert tumbling(T0 + 60_000, 60_000) == [(T0 + 60_000, T0 + 120_000)]


def test_sliding_event_lands_in_size_over_slide_windows():
    wins = sliding(T0 + 35_000, 60_000, 10_000)
    assert len(wins) == 6
    for start, end in wins:
        assert start <= T0 + 35_000 < end
        assert end - start == 60_000
    assert wins == sorted(wins)


def test_sliding_on_boundary():
    wins = sliding(T0, 60_000, 10_000)
    assert wins[-1] == (T0, T0 + 60_000)
    assert all(s <= T0 < e for s, e in wins)


def test_window_id_is_readable():
    assert window_id(T0, T0 + 60_000) == f"{T0 // 1000}-{T0 // 1000 + 60}"


def test_incremental_aggregation_matches_project_example():
    # Project doc section 42: 100, 200, 300, 400 ms with two failures.
    agg = Aggregate()
    for latency, status in [(100, 200), (200, 500), (300, 200), (400, 503)]:
        agg.add(latency, status, T0)
    r = agg.result(60)
    assert r["count"] == 4
    assert r["sum"] == 1000
    assert r["avg"] == 250
    assert r["min"] == 100 and r["max"] == 400
    assert r["error_count"] == 2
    assert r["error_rate"] == 50.0


def test_aggregate_round_trips_through_checkpoint():
    agg = Aggregate()
    agg.add(42, 200, T0, cpu=50, mem=60)
    copy = Aggregate.from_dict(agg.to_dict())
    assert copy.result(60) == agg.result(60)


def test_watermark_is_monotonic_and_bounded():
    wm = Watermark(bound_ms=5_000, idle_timeout_ms=10_000)
    now = T0 + 100_000
    wm.observe(T0 + 50_000, now)
    assert wm.value(now) == T0 + 45_000
    wm.observe(T0 + 20_000, now)  # out-of-order event must not move it back
    assert wm.value(now) == T0 + 45_000


def test_watermark_follows_wall_clock_when_idle():
    wm = Watermark(bound_ms=5_000, idle_timeout_ms=10_000)
    wm.observe(T0, T0)
    later = T0 + 60_000
    assert wm.value(later) == later - 10_000 - 5_000
    assert wm.is_idle(later)
