"""Cycling (round-tripping): Memgraph finds loops, aml.temporal checks direction and time."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd

from aml.graph import Candidate
from aml.temporal import (
    SearchBudgetExceeded,
    TemporalInstance,
    directed_rings,
    format_ts,
    temporal_instances,
    tightest,
)

# Same columns, in the same order, as the original detected_cycles.csv (so the Metabase
# questions keep working); everything new is appended after them.
LEGACY_COLUMNS = [
    "Cycle_ID", "Sender_account", "Receiver_account", "Amount", "Timestamp", "Hop_Number",
    "Cycle_Length",
]
EXTRA_COLUMNS = [
    "Tx_ID", "Ring_ID", "Originator", "Cycle_Start", "Cycle_Duration_Hours", "Amount_Retention",
    "Valid_Instances",
]


@dataclass(frozen=True)
class CycleRules:
    min_len: int = 3
    max_len: int = 20
    max_window_seconds: int | None = None  # None: a trip may take any amount of time
    strict: bool = False  # True: each hop strictly after the previous one
    all_instances: bool = False  # False: report only the tightest trip per ring


@dataclass(frozen=True)
class RingResult:
    ring: tuple[int, ...]
    instances: tuple[TemporalInstance, ...]  # never empty

    @property
    def tightest(self) -> TemporalInstance:
        return tightest(self.instances)


@dataclass
class CycleStats:
    candidates: int = 0  # loops returned by cycles.get()
    duplicate_sets: int = 0  # loops over an account set we had already checked
    skipped_dense: int = 0  # too densely connected to enumerate (raise the budget if > 0)
    directed_rings: int = 0  # loops that money can travel around in one direction
    temporal_rings: int = 0  # ... and did, in time order (within the window, if one is set)


def detect_cycles(
    candidates: Iterable[Candidate], rules: CycleRules = CycleRules()
) -> tuple[list[RingResult], CycleStats]:
    stats = CycleStats()
    seen_sets: set[frozenset[int]] = set()
    seen_rings: set[tuple[int, ...]] = set()
    results: list[RingResult] = []
    for candidate in candidates:
        stats.candidates += 1
        members = frozenset(candidate.accounts)
        if not rules.min_len <= len(members) <= rules.max_len:
            continue
        if members in seen_sets:
            stats.duplicate_sets += 1
            continue
        seen_sets.add(members)
        try:
            rings = directed_rings(members, candidate.transfers)
        except SearchBudgetExceeded:
            stats.skipped_dense += 1
            continue
        for ring in rings:
            if ring in seen_rings:
                continue
            seen_rings.add(ring)
            stats.directed_rings += 1
            instances = temporal_instances(
                ring,
                candidate.transfers,
                max_window=rules.max_window_seconds,
                strict=rules.strict,
            )
            if instances:
                stats.temporal_rings += 1
                results.append(RingResult(ring, tuple(instances)))
    results.sort(key=lambda r: (r.tightest.start, r.ring))
    return results, stats


def cycles_frame(results: list[RingResult], rules: CycleRules = CycleRules()) -> pd.DataFrame:
    """One row per hop, ready for detected_cycles.csv and the Metabase funnel chart."""
    rows = []
    cycle_id = 0
    for ring_id, result in enumerate(results, start=1):
        chosen = result.instances if rules.all_instances else (result.tightest,)
        for instance in chosen:
            cycle_id += 1
            for hop_number, hop in enumerate(instance.hops, start=1):
                rows.append(
                    {
                        "Cycle_ID": cycle_id,
                        "Sender_account": hop.src,
                        "Receiver_account": hop.dst,
                        "Amount": hop.amount,
                        "Timestamp": format_ts(hop.ts),
                        "Hop_Number": hop_number,
                        "Cycle_Length": len(instance.hops),
                        "Tx_ID": hop.tx_id,
                        "Ring_ID": ring_id,
                        "Originator": instance.originator,
                        "Cycle_Start": format_ts(instance.start),
                        "Cycle_Duration_Hours": round(instance.duration / 3600, 2),
                        "Amount_Retention": round(instance.amount_retention, 4),
                        "Valid_Instances": len(result.instances),
                    }
                )
    return pd.DataFrame(rows, columns=LEGACY_COLUMNS + EXTRA_COLUMNS)
