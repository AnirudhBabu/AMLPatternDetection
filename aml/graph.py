"""Memgraph I/O: connecting, bulk-loading the transaction graph and fetching loop candidates."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass

from aml.temporal import Transfer

DEFAULT_URI = "bolt://localhost:7687"

# The loader streams rows over Bolt: Memgraph never opens a file, so there's no path inside
# its container to get wrong, nothing to mount, and no half-loaded graph from a missing chunk.
ACCOUNT_BATCH = "UNWIND $ids AS id CREATE (:Account {accountID: id})"

TRANSFER_BATCH = """
UNWIND $rows AS row
MATCH (s:Account {accountID: row[0]}), (r:Account {accountID: row[1]})
CREATE (s)-[:TRANSFERRED {
    tx_id: row[2], amount: row[3], ts: row[4], datetime: datetime(row[5]), type: row[6]
}]->(r)
"""

# cycles.get() treats the graph as undirected and doesn't promise any node order, so it
# is only used to find *which* accounts form a loop. Every transfer among those accounts
# comes back with it; direction and chronology are checked in Python (aml.temporal).
CYCLE_CANDIDATES = """
CALL cycles.get() YIELD cycle_id, node
WITH cycle_id, collect(node) AS nodes
WHERE size(nodes) >= $min_len AND size(nodes) <= $max_len
UNWIND nodes AS a
MATCH (a)-[e:TRANSFERRED]->(b:Account)
WHERE b IN nodes AND b <> a
WITH cycle_id, nodes,
     collect({src: a.accountID, dst: b.accountID, tx_id: e.tx_id, ts: e.ts,
              amount: e.amount}) AS edges
RETURN cycle_id, [n IN nodes | n.accountID] AS accounts, edges
"""


@dataclass(frozen=True)
class Candidate:
    """Accounts that cycles.get() says form a loop, plus every transfer between them."""

    structure_id: int
    accounts: tuple[int, ...]
    transfers: tuple[Transfer, ...]


@dataclass(frozen=True)
class LoadResult:
    nodes: int
    edges: int
    skipped: bool


def connect(uri: str = DEFAULT_URI, *, auth: tuple[str, str] = ("", ""), wait_seconds: float = 60):
    """Open a driver, retrying while Memgraph is still starting up."""
    from neo4j import GraphDatabase
    from neo4j.exceptions import ServiceUnavailable

    # The driver logs server notifications (e.g. "index already exists") as warnings, which
    # would print over the spinner; real errors still raise.
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)
    driver = GraphDatabase.driver(uri, auth=auth)
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            with driver.session() as session:
                session.run("RETURN 1").consume()
            return driver
        except ServiceUnavailable:
            if time.monotonic() >= deadline:
                driver.close()
                raise
            time.sleep(2)


def graph_counts(driver) -> tuple[int, int]:
    with driver.session() as session:
        nodes = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        edges = session.run("MATCH ()-[e]->() RETURN count(e) AS c").single()["c"]
    return nodes, edges


def load_graph(
    driver,
    accounts: Iterable[list[int]],
    transfers: Iterable[list[list]],
    *,
    workers: int = 1,
    reload: bool = False,
    progress: Callable[[int], None] | None = None,
) -> LoadResult:
    """Bulk-load the graph from row batches (see aml.dataset's batch readers).

    ``accounts`` yields lists of account ids. ``transfers`` yields lists of
    [sender, receiver, tx_id, amount, ts, ISO datetime, laundering type] rows. An existing
    graph is left alone unless ``reload``. ``workers > 1`` sends that many batches at once
    (IN_MEMORY_ANALYTICAL mode allows parallel writes). ``progress`` gets the running total
    of transfers loaded.
    """
    with driver.session() as session:
        session.run("STORAGE MODE IN_MEMORY_ANALYTICAL").consume()
        existing = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        if existing and not reload:
            nodes, edges = graph_counts(driver)
            return LoadResult(nodes, edges, skipped=True)
        if existing:
            try:
                session.run("DROP GRAPH").consume()
            except Exception:  # older Memgraph without DROP GRAPH
                session.run("MATCH (n) DETACH DELETE n").consume()
        try:
            session.run("CREATE INDEX ON :Account(accountID)").consume()
        except Exception as exc:  # left over from an earlier run
            if "exist" not in str(exc).lower():
                raise
        for ids in accounts:
            session.run(ACCOUNT_BATCH, ids=ids).consume()

    def send(rows: list[list]) -> int:
        with driver.session() as batch_session:
            batch_session.run(TRANSFER_BATCH, rows=rows).consume()
        return len(rows)

    loaded = 0

    def done(count: int) -> None:
        nonlocal loaded
        loaded += count
        if progress is not None:
            progress(loaded)

    if workers <= 1:
        for rows in transfers:
            done(send(rows))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            pending: set = set()
            for rows in transfers:  # at most 2 x workers batches in memory at once
                pending.add(pool.submit(send, rows))
                if len(pending) >= 2 * workers:
                    finished, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in finished:
                        done(future.result())
            for future in as_completed(pending):
                done(future.result())
    nodes, edges = graph_counts(driver)
    return LoadResult(nodes, edges, skipped=False)


def fetch_cycle_candidates(driver, *, min_len: int = 3, max_len: int = 20) -> Iterator[Candidate]:
    """Stream loop candidates from cycles.get() with all transfers among their accounts."""
    with driver.session() as session:
        result = session.run(CYCLE_CANDIDATES, min_len=min_len, max_len=max_len)
        for record in result:
            edges = record["edges"]
            if any(e["ts"] is None or e["tx_id"] is None for e in edges):
                raise RuntimeError(
                    "TRANSFERRED relationships have no ts/tx_id - the graph was loaded by the "
                    "old loader. Run `aml load-graph --reload` first."
                )
            yield Candidate(
                structure_id=record["cycle_id"],
                accounts=tuple(record["accounts"]),
                transfers=tuple(
                    Transfer(e["src"], e["dst"], e["ts"], e["amount"], e["tx_id"]) for e in edges
                ),
            )
