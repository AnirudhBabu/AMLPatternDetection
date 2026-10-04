import threading

from aml.graph import ACCOUNT_BATCH, TRANSFER_BATCH, load_graph


class _Result:
    def __init__(self, record=None):
        self._record = record

    def consume(self):
        return None

    def single(self):
        return self._record


class FakeMemgraph:
    """Stands in for a neo4j driver: records what load_graph sends and keeps counts."""

    def __init__(self, nodes=0, edges=0):
        self.nodes, self.edges, self.sent = nodes, edges, []
        self._lock = threading.Lock()

    def session(self):
        fake = self

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def run(self, query, **params):
                return fake.run(query, params)

        return Session()

    def run(self, query, params):
        with self._lock:
            if query.startswith("MATCH (n) RETURN count"):
                return _Result({"c": self.nodes})
            if query.startswith("MATCH ()-[e]->()"):
                return _Result({"c": self.edges})
            self.sent.append(query if not params else (query, params))
            if query == "DROP GRAPH":
                self.nodes = self.edges = 0
            elif query == ACCOUNT_BATCH:
                self.nodes += len(params["ids"])
            elif query == TRANSFER_BATCH:
                self.edges += len(params["rows"])
            return _Result()


ROW = [1, 2, 10, 500.0, 1665100800, "2022-10-07T00:00:00", "Normal_Small_Fan_Out"]


def _batches(n, size):
    rows = [[*ROW[:2], i, *ROW[3:]] for i in range(n)]
    return [rows[i:i + size] for i in range(0, n, size)]


def test_loads_accounts_then_transfers_in_batches_with_progress():
    memgraph, seen = FakeMemgraph(), []
    result = load_graph(memgraph, [[1, 2], [3]], _batches(5, 2), progress=seen.append)
    assert (result.nodes, result.edges, result.skipped) == (3, 5, False)
    assert seen == [2, 4, 5]
    queries = [q if isinstance(q, str) else q[0] for q in memgraph.sent]
    setup = ["STORAGE MODE IN_MEMORY_ANALYTICAL", "CREATE INDEX ON :Account(accountID)"]
    assert queries[:2] == setup
    assert queries.index(TRANSFER_BATCH) > queries.index(ACCOUNT_BATCH)


def test_leaves_an_existing_graph_alone_unless_asked_to_reload():
    memgraph = FakeMemgraph(nodes=7, edges=9)
    result = load_graph(memgraph, [[1]], _batches(3, 2))
    assert (result.nodes, result.edges, result.skipped) == (7, 9, True)
    assert all(q == "STORAGE MODE IN_MEMORY_ANALYTICAL" for q in memgraph.sent)

    result = load_graph(memgraph, [[1, 2]], _batches(3, 2), reload=True)
    assert (result.nodes, result.edges, result.skipped) == (2, 3, False)
    assert "DROP GRAPH" in memgraph.sent


def test_parallel_workers_send_every_batch_once():
    memgraph, seen = FakeMemgraph(), []
    result = load_graph(memgraph, [[1, 2]], _batches(1_001, 10), workers=4, progress=seen.append)
    assert result.edges == 1_001 and seen[-1] == 1_001 and len(seen) == 101
    sent_ids = sorted(row[2] for q in memgraph.sent if isinstance(q, tuple)
                      and q[0] == TRANSFER_BATCH for row in q[1]["rows"])
    assert sent_ids == list(range(1_001))
