"""Out-of-order, late, duplicate and failure-recovery tests for the engine."""
import random

from swma.engine.engine import EngineConfig, PartitionEngine

T0 = 1_700_000_040_000  # minute boundary
CFG = EngineConfig(tumbling_ms=60_000, sliding_ms=60_000, slide_ms=10_000, lateness_ms=10_000,
                   bound_ms=5_000, idle_ms=600_000, emit_interval_ms=0)


def ev(i, ts_ms, latency=100, status=200, service="payment-service"):
    return {"event_id": f"e{i}", "service": service, "response_time_ms": latency,
            "status_code": status}, ts_ms


def run(engine, events, wall=None):
    out = []
    for e, ts in events:
        out.append(engine.process(e, ts, wall or ts))
    return out


def finals(emissions, wtype="tumbling"):
    return {e["window_id"]: e for e in emissions if e["status"] == "final" and e["window_type"] == wtype}


def test_out_of_order_inside_bound_is_counted():
    eng = PartitionEngine(0, CFG)
    outcomes = run(eng, [ev(1, T0 + 10_000), ev(2, T0 + 30_000), ev(3, T0 + 20_000)])
    assert [o.kind for o in outcomes] == ["on_time", "on_time", "out_of_order"]
    w = eng.windows[("tumbling", "payment-service", T0, T0 + 60_000)]
    assert w.agg.count == 3


def test_late_event_within_allowed_lateness_updates_window():
    eng = PartitionEngine(0, CFG)
    run(eng, [ev(1, T0 + 10_000), ev(2, T0 + 66_000)])  # watermark = T0+61s > window end
    em = eng.tick(T0 + 66_000)
    assert any(e["status"] == "closed" and e["start_ms"] == T0 for e in em)
    late = eng.process(*ev(3, T0 + 40_000), T0 + 66_000)
    assert late.kind == "late_accepted"
    em = eng.tick(T0 + 66_000)
    upd = [e for e in em if e["status"] == "updated" and e["window_type"] == "tumbling"]
    assert upd and upd[0]["count"] == 2 and upd[0]["late_events"] == 1


def test_event_after_allowed_lateness_is_too_late():
    eng = PartitionEngine(0, CFG)
    run(eng, [ev(1, T0 + 10_000), ev(2, T0 + 140_000)])  # wm = T0+135s
    em = eng.tick(T0 + 140_000)
    assert T0 // 1000 in {e["start_ms"] // 1000 for e in em if e["status"] == "final"}
    o = eng.process(*ev(3, T0 + 5_000), T0 + 140_000)
    assert o.kind == "too_late"
    assert o.windows == [] and len(o.dropped) == 7  # 1 tumbling + 6 sliding


def test_duplicates_are_counted_once():
    eng = PartitionEngine(0, CFG)
    e, ts = ev(1, T0 + 1_000)
    assert eng.process(e, ts, ts).kind == "on_time"
    assert eng.process(e, ts, ts).kind == "duplicate"
    assert eng.windows[("tumbling", "payment-service", T0, T0 + 60_000)].agg.count == 1


def test_matches_reference_implementation_with_shuffled_events():
    rnd = random.Random(7)
    events = [ev(i, T0 + rnd.randint(0, 179_999), latency=rnd.randint(5, 900),
                 status=rnd.choice([200, 200, 200, 500])) for i in range(600)]
    events.sort(key=lambda x: x[1])
    # shuffle locally (by up to 4s) to create out-of-order arrival inside the bound
    jittered = sorted(events, key=lambda x: x[1] + rnd.randint(0, 4_000))
    eng = PartitionEngine(0, CFG)
    run(eng, jittered)
    emissions = eng.tick(T0 + 400_000)  # far in the future: everything final... via idle?
    eng.wm.observe(T0 + 400_000, T0 + 400_000)
    emissions += eng.tick(T0 + 400_000)
    got = finals(emissions)
    # reference: plain batch computation
    ref = {}
    for e, ts in events:
        start = ts - ts % 60_000
        r = ref.setdefault(start, [])
        r.append(e["response_time_ms"])
    assert len(got) == len(ref)
    for start, lats in ref.items():
        g = next(x for x in got.values() if x["start_ms"] == start)
        assert g["count"] == len(lats)
        assert g["min"] == min(lats) and g["max"] == max(lats)
        assert abs(g["avg"] - sum(lats) / len(lats)) < 0.01


def test_crash_recovery_from_checkpoint_gives_exact_counts():
    events = [ev(i, T0 + i * 100) for i in range(500)]
    # Uninterrupted run
    a = PartitionEngine(0, CFG)
    run(a, events)
    # Crashing run: checkpoint after 200 events, keep processing, then "crash"
    b = PartitionEngine(0, CFG)
    run(b, events[:200])
    b.next_offset = 200
    store = {}
    puts, deletes = b.drain_changes()
    store.update(puts)
    run(b, events[200:350])  # processed but never checkpointed -> lost in the crash
    c = PartitionEngine(0, CFG)
    c.restore(store.items(), T0 + 50_000)
    assert c.next_offset == 200
    run(c, events[c.next_offset:])  # replay from the checkpointed offset
    key = ("tumbling", "payment-service", T0, T0 + 60_000)
    assert c.windows[key].agg.count == a.windows[key].agg.count == 500


def test_incremental_checkpoint_deletes_finalized_windows():
    eng = PartitionEngine(0, CFG)
    run(eng, [ev(1, T0 + 1_000)])
    store = dict(eng.drain_changes()[0])
    assert any(k.startswith("w|tumbling") for k in store)
    eng.process(*ev(2, T0 + 200_000), T0 + 200_000)
    eng.tick(T0 + 200_000)
    puts, deletes = eng.drain_changes()
    for k in deletes:
        store.pop(k, None)
    store.update(puts)
    assert not any(k.startswith(f"w|tumbling|payment-service|{T0}|") for k in store)
