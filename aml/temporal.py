"""Direction- and time-aware cycle logic, kept free of any database so it can be unit-tested.

Memgraph's ``cycles.get()`` works on the *undirected* graph and makes no promise about the
order of the nodes it returns, so it can only tell us which accounts form a loop. Whether
money actually travelled around that loop, hop after hop in time order, is decided here.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class Transfer:
    """One transaction, i.e. one TRANSFERRED relationship."""

    src: int
    dst: int
    ts: int  # seconds since the Unix epoch
    amount: float
    tx_id: int


@dataclass(frozen=True, slots=True)
class TemporalInstance:
    """One trip around a ring, each hop happening at or after the previous one."""

    hops: tuple[Transfer, ...]

    @property
    def originator(self) -> int:
        return self.hops[0].src

    @property
    def accounts(self) -> tuple[int, ...]:
        return tuple(hop.src for hop in self.hops)

    @property
    def start(self) -> int:
        return self.hops[0].ts

    @property
    def end(self) -> int:
        return self.hops[-1].ts

    @property
    def duration(self) -> int:
        return self.end - self.start

    @property
    def tx_ids(self) -> tuple[int, ...]:
        return tuple(hop.tx_id for hop in self.hops)

    @property
    def amount_retention(self) -> float:
        """Share of the first hop's amount that arrives back at the originator."""
        first = self.hops[0].amount
        return self.hops[-1].amount / first if first else float("nan")


class SearchBudgetExceeded(RuntimeError):
    """A candidate is too densely connected to enumerate its rings cheaply."""


def directed_rings(
    accounts: Iterable[int], transfers: Iterable[Transfer], *, max_steps: int = 100_000
) -> list[tuple[int, ...]]:
    """Every directed cycle that passes through each of ``accounts`` exactly once.

    Node order in ``accounts`` is irrelevant. Rings start at the smallest account id so
    rotations of the same ring are not reported twice; the two directions of travel are
    different rings.
    """
    members = set(accounts)
    if len(members) < 2:
        return []
    successors: dict[int, set[int]] = defaultdict(set)
    has_inbound: set[int] = set()
    for t in transfers:
        if t.src != t.dst and t.src in members and t.dst in members:
            successors[t.src].add(t.dst)
            has_inbound.add(t.dst)
    if len(successors) < len(members) or len(has_inbound) < len(members):
        return []  # some account has no way in or no way out

    start = min(members)
    size = len(members)
    rings: list[tuple[int, ...]] = []
    path = [start]
    on_path = {start}
    steps = 0

    def extend() -> None:
        nonlocal steps
        steps += 1
        if steps > max_steps:
            raise SearchBudgetExceeded(f"over {max_steps} search steps for {size} accounts")
        here = path[-1]
        if len(path) == size:
            if start in successors[here]:
                rings.append(tuple(path))
            return
        for nxt in sorted(successors[here]):
            if nxt not in on_path:
                path.append(nxt)
                on_path.add(nxt)
                extend()
                path.pop()
                on_path.discard(nxt)

    extend()
    return rings


def temporal_instances(
    ring: Sequence[int],
    transfers: Iterable[Transfer],
    *,
    max_window: int | None = None,
    strict: bool = False,
) -> list[TemporalInstance]:
    """All time-respecting trips around ``ring``: one per transfer that can start a trip.

    Once the first transfer is fixed, taking the earliest eligible transfer at every later
    hop gets back to the originator as early as possible. So if that greedy walk fails, or
    overruns ``max_window`` seconds, no other choice of later transfers could succeed.
    Trying every first transfer on every rotation therefore finds every instance that
    exists, each completed in its tightest possible way.

    ``strict=True`` requires every hop to happen strictly after the previous one.
    """
    n = len(ring)
    if n < 2:
        return []
    hop_index = {(ring[i], ring[(i + 1) % n]): i for i in range(n)}
    hops: list[list[Transfer]] = [[] for _ in range(n)]
    for t in transfers:
        i = hop_index.get((t.src, t.dst))
        if i is not None:
            hops[i].append(t)
    if not all(hops):
        return []
    for hop in hops:
        hop.sort(key=lambda t: (t.ts, t.tx_id))
    times = [[t.ts for t in hop] for hop in hops]
    seek = bisect_right if strict else bisect_left

    found: dict[tuple[int, ...], TemporalInstance] = {}
    for k in range(n):
        for first in hops[k]:
            deadline = None if max_window is None else first.ts + max_window
            chain = [first]
            for j in range(1, n):
                h = (k + j) % n
                pos = seek(times[h], chain[-1].ts)
                if pos == len(times[h]):
                    break
                nxt = hops[h][pos]
                if deadline is not None and nxt.ts > deadline:
                    break
                chain.append(nxt)
            else:
                instance = TemporalInstance(tuple(chain))
                found.setdefault(instance.tx_ids, instance)
    return sorted(found.values(), key=lambda inst: (inst.start, inst.end, inst.tx_ids))


def tightest(instances: Iterable[TemporalInstance]) -> TemporalInstance:
    """The shortest trip; ties go to the earliest one."""
    return min(instances, key=lambda inst: (inst.duration, inst.start, inst.tx_ids))


def format_ts(epoch_seconds: int) -> str:
    """Render epoch seconds the way they appear in SAML-D (the data has no time zone)."""
    return datetime.fromtimestamp(int(epoch_seconds), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
