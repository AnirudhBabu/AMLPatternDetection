"""Runs the real Cypher against a live Memgraph + MAGE (CI starts one as a service).

    MEMGRAPH_URI=bolt://localhost:7687 uv run pytest -m memgraph
"""

import os

import pytest

from aml.cycles import CycleRules, detect_cycles

pytestmark = pytest.mark.memgraph
URI = os.environ.get("MEMGRAPH_URI")
DAY = 86_400

TRANSFERS = [  # (tx_id, src, dst, ts, amount)
    # 1 -> 2 -> 3 -> 1, only valid if you look past the earliest 1 -> 2 transfer
    (1, 1, 2, 1 * DAY, 1000.0), (2, 1, 2, 8 * DAY, 990.0), (3, 2, 3, 9 * DAY, 950.0),
    (4, 3, 1, 3 * DAY, 1000.0),
    # 10 -> 11 -> 12 -> 13 -> 10 in order
    (5, 10, 11, 1 * DAY, 500.0), (6, 11, 12, 2 * DAY, 480.0), (7, 12, 13, 3 * DAY, 460.0),
    (8, 13, 10, 4 * DAY, 440.0),
    # 20 -> 21 -> 22 -> 20 exists, but the money goes backwards in time
    (9, 20, 21, 9 * DAY, 100.0), (10, 21, 22, 5 * DAY, 100.0), (11, 22, 20, 1 * DAY, 100.0),
    # a triangle that is not a directed cycle
    (12, 30, 31, 1 * DAY, 100.0), (13, 32, 31, 2 * DAY, 100.0), (14, 32, 30, 3 * DAY, 100.0),
]


@pytest.mark.skipif(not URI, reason="set MEMGRAPH_URI to run against a live Memgraph")
def test_cycle_candidates_against_live_memgraph():
    from aml.graph import connect, fetch_cycle_candidates, load_graph

    driver = connect(URI)
    try:
        with driver.session() as session:
            existing = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        if existing > 100:
            pytest.skip(
                f"won't wipe a Memgraph holding {existing:,} nodes - use a scratch instance"
            )
        accounts = sorted({t[1] for t in TRANSFERS} | {t[2] for t in TRANSFERS})
        rows = [[src, dst, tx, amount, ts, "2022-10-07T00:00:00", "Cycle"]
                for tx, src, dst, ts, amount in TRANSFERS]
        loaded = load_graph(driver, [accounts], [rows[:5], rows[5:]], reload=True)
        assert (loaded.nodes, loaded.edges) == (len(accounts), len(TRANSFERS))
        candidates = list(fetch_cycle_candidates(driver, min_len=3, max_len=6))
    finally:
        driver.close()

    results, stats = detect_cycles(candidates, CycleRules())
    assert {r.ring for r in results} == {(1, 2, 3), (10, 11, 12, 13)}
    assert stats.directed_rings == 3  # 20-21-22 is directed but never in time order
    first = {r.ring: r.tightest for r in results}[(1, 2, 3)]
    assert first.originator == 3 and first.tx_ids == (4, 2, 3)
