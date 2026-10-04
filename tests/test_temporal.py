import itertools
import random

from aml.temporal import Transfer, directed_rings, temporal_instances, tightest

DAY = 86_400
_ids = itertools.count(1)


def tr(src, dst, ts, amount=100.0):
    return Transfer(src, dst, ts, amount, next(_ids))


def legacy_cypher_check(cycle_nodes, transfers):
    """What the original Cypher did: walk the nodes in the order cycles.get() returned them,
    keep only the earliest transfer per hop, then look for a rotation in time order."""
    n = len(cycle_nodes)
    edges = []
    for i in range(n):
        src, dst = cycle_nodes[i], cycle_nodes[(i + 1) % n]
        hop = [t for t in transfers if t.src == src and t.dst == dst]
        if not hop:
            return False
        edges.append(min(hop, key=lambda t: t.ts))
    return any(
        all(edges[(k + j) % n].ts <= edges[(k + j + 1) % n].ts for j in range(n - 1))
        for k in range(n)
    )


def _hamiltonian_rings(nodes, pairs):
    """Oracle: try every ordering of the accounts."""
    first, rest = nodes[0], nodes[1:]
    rings = set()
    for perm in itertools.permutations(rest):
        ring = (first, *perm)
        if all((ring[i], ring[(i + 1) % len(ring)]) in pairs for i in range(len(ring))):
            rings.add(ring)
    return rings


def test_finds_cycle_that_earliest_edge_shortcut_misses():
    # C pays A on day 3, A pays B on day 8, B pays C on day 9: a valid round trip.
    # A also paid B on day 1, and keeping only that earliest A->B transfer hides it.
    a, b, c = 1, 2, 3
    transfers = [tr(a, b, 1 * DAY), tr(a, b, 8 * DAY), tr(b, c, 9 * DAY), tr(c, a, 3 * DAY)]
    assert not legacy_cypher_check([a, b, c], transfers)

    [instance] = temporal_instances((a, b, c), transfers)
    assert instance.originator == c
    assert [(h.src, h.dst, h.ts) for h in instance.hops] == [
        (c, a, 3 * DAY), (a, b, 8 * DAY), (b, c, 9 * DAY),
    ]


def test_node_order_from_cycles_get_does_not_matter():
    # cycles.get() is undirected and gives no ordering guarantee.
    transfers = [tr(1, 2, 1), tr(2, 3, 2), tr(3, 4, 3), tr(4, 1, 4)]
    for listed in ([1, 2, 3, 4], [1, 4, 3, 2], [3, 1, 4, 2]):
        assert directed_rings(listed, transfers) == [(1, 2, 3, 4)]
    assert legacy_cypher_check([1, 2, 3, 4], transfers)
    assert not legacy_cypher_check([1, 4, 3, 2], transfers)  # same loop, listed backwards
    assert not legacy_cypher_check([3, 1, 4, 2], transfers)  # scrambled


def test_every_directed_ring_through_the_accounts_is_found():
    nodes = [1, 2, 3, 4]
    pairs = [(1, 2), (2, 3), (3, 4), (4, 1), (1, 3), (3, 2), (2, 4), (4, 3)]
    transfers = [tr(s, d, i) for i, (s, d) in enumerate(pairs)]
    expected = _hamiltonian_rings(nodes, set(pairs))
    assert set(directed_rings(nodes, transfers)) == expected
    assert expected == {(1, 2, 3, 4), (1, 3, 2, 4)}


def test_loop_that_is_not_directed_has_no_ring():
    transfers = [tr(1, 2, 1), tr(3, 2, 2), tr(3, 1, 3)]  # a triangle, but not a cycle
    assert directed_rings([1, 2, 3], transfers) == []


def test_window_skips_slow_trips_and_keeps_fast_ones():
    slow = [tr(1, 2, 0), tr(2, 3, 20 * DAY), tr(3, 1, 40 * DAY)]
    fast = [tr(1, 2, 100 * DAY), tr(2, 3, 101 * DAY), tr(3, 1, 104 * DAY)]
    everything = temporal_instances((1, 2, 3), slow + fast)
    windowed = temporal_instances((1, 2, 3), slow + fast, max_window=30 * DAY)
    assert {i.tx_ids for i in windowed} == {tuple(t.tx_id for t in fast)}
    assert tightest(everything).duration == 4 * DAY
    assert len(everything) > len(windowed)


def test_strict_mode_rejects_simultaneous_hops():
    transfers = [tr(1, 2, 50), tr(2, 3, 50), tr(3, 1, 60)]
    assert temporal_instances((1, 2, 3), transfers)
    assert not temporal_instances((1, 2, 3), transfers, strict=True)


def test_two_account_round_trip():
    transfers = [tr(7, 8, 10, 1000.0), tr(8, 7, 20, 950.0)]
    assert directed_rings([8, 7], transfers) == [(7, 8)]
    [instance] = temporal_instances((7, 8), transfers)
    assert instance.duration == 10
    assert round(instance.amount_retention, 3) == 0.95


def _brute_force(ring, transfers, max_window, strict):
    """For every first transfer: the earliest possible end of a valid trip (or absent)."""
    n = len(ring)
    hops = [
        [t for t in transfers if (t.src, t.dst) == (ring[i], ring[(i + 1) % n])] for i in range(n)
    ]
    best = {}
    for k in range(n):
        for chain in itertools.product(*(hops[(k + j) % n] for j in range(n))):
            ordered = all(
                (b.ts > a.ts) if strict else (b.ts >= a.ts) for a, b in zip(chain, chain[1:])
            )
            if not ordered:
                continue
            if max_window is not None and chain[-1].ts - chain[0].ts > max_window:
                continue
            first = chain[0].tx_id
            best[first] = min(best.get(first, chain[-1].ts), chain[-1].ts)
    return best


def test_matches_brute_force_on_random_graphs():
    rng = random.Random(7)
    checked = 0
    for _ in range(400):
        nodes = list(range(1, rng.randint(2, 5) + 1))
        transfers = [
            tr(s, d, rng.randint(0, 12), float(rng.randint(1, 9)))
            for s in nodes
            for d in nodes
            if s != d and rng.random() < 0.6
            for _ in range(rng.randint(1, 3))
        ]
        pairs = {(t.src, t.dst) for t in transfers}
        rings = directed_rings(nodes, transfers)
        assert set(rings) == _hamiltonian_rings(nodes, pairs)
        for ring in rings:
            for max_window in (None, 4):
                for strict in (False, True):
                    found = temporal_instances(
                        ring, transfers, max_window=max_window, strict=strict
                    )
                    got = {inst.hops[0].tx_id: inst.end for inst in found}
                    assert got == _brute_force(ring, transfers, max_window, strict)
                    checked += 1
    assert checked > 100
