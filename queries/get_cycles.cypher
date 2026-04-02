/// 1. Get structural cycles
CALL cycles.get() YIELD cycle_id, node
WITH cycle_id, collect(node) AS cycle_nodes
WHERE size(cycle_nodes) >= 3

// 2. Re-link nodes to their specific TRANSFERRED relationships
UNWIND range(0, size(cycle_nodes) - 1) AS i
WITH cycle_id, cycle_nodes, i,
     cycle_nodes[i] AS source,
     cycle_nodes[(i + 1) % size(cycle_nodes)] AS target
MATCH (source)-[e:TRANSFERRED]->(target)
// Pick the earliest transaction if multiple exist between the same pair
WITH cycle_id, cycle_nodes, i, e ORDER BY e.datetime ASC
WITH cycle_id, cycle_nodes, i, head(collect(e)) AS first_e
ORDER BY cycle_id, i
WITH cycle_id, cycle_nodes, collect(first_e) AS edges

// 3. Find the chronological starting point (The Arrow of Time)
WHERE size(edges) = size(cycle_nodes)
WITH cycle_id, cycle_nodes, edges,
     [k IN range(0, size(edges) - 1) 
      WHERE ALL(j IN range(0, size(edges) - 2) 
                WHERE edges[(k + j) % size(edges)].datetime 
                   <= edges[(k + j + 1) % size(edges)].datetime)
     ] AS valid_starts
WHERE size(valid_starts) > 0

// 4. Align both lists so Node[0] is the sender of Edge[0]
WITH cycle_id, cycle_nodes, edges, valid_starts[0] AS start
WITH cycle_id,
     [k IN range(0, size(cycle_nodes) - 1) | cycle_nodes[(start + k) % size(cycle_nodes)]] AS ord_nodes,
     [k IN range(0, size(edges) - 1) | edges[(start + k) % size(edges)]] AS ord_edges

// 5. Return raw entities for graph inspection
RETURN cycle_id, ord_nodes, ord_edges;