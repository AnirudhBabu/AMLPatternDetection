// How `uv run aml load-graph` builds the graph, for reference.
// It reads data/prepared/transactions.parquet in batches and sends them over Bolt, so
// Memgraph never opens a file: there is no path inside its container to get wrong.

STORAGE MODE IN_MEMORY_ANALYTICAL;

CREATE INDEX ON :Account(accountID);

// $ids: a batch of account ids
UNWIND $ids AS id
CREATE (:Account {accountID: id});

// $rows: a batch of [sender, receiver, tx_id, amount, ts (epoch seconds),
//                    "YYYY-MM-DDTHH:MM:SS", laundering type]
UNWIND $rows AS row
MATCH (s:Account {accountID: row[0]}), (r:Account {accountID: row[1]})
CREATE (s)-[:TRANSFERRED {
  tx_id: row[2], amount: row[3], ts: row[4], datetime: datetime(row[5]), type: row[6]
}]->(r);
