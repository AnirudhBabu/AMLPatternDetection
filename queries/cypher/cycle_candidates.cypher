// The loop search `uv run aml cycles` runs.
// cycles.get() treats the graph as undirected and returns each loop's nodes in no
// particular order, so this only collects the loop's accounts and every transfer among
// them. Direction and chronology are checked in Python (aml/temporal.py).
CALL cycles.get() YIELD cycle_id, node
WITH cycle_id, collect(node) AS nodes
WHERE size(nodes) >= 3 AND size(nodes) <= 20
UNWIND nodes AS a
MATCH (a)-[e:TRANSFERRED]->(b:Account)
WHERE b IN nodes AND b <> a
WITH cycle_id, nodes,
     collect({src: a.accountID, dst: b.accountID, tx_id: e.tx_id, ts: e.ts,
              amount: e.amount}) AS edges
RETURN cycle_id, [n IN nodes | n.accountID] AS accounts, edges;
