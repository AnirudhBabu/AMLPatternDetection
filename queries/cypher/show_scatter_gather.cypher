// Visualize one scatter-gather episode from data/scatter_gather_suspects.csv:
// paste its Source_account, Destination_account and the Mule_account values.
WITH 2521152088 AS source, 8657935466 AS destination, [718745407, 2361741456, 2299468667] AS mules
MATCH (s:Account {accountID: source})-[a:TRANSFERRED]->(m:Account)
      -[b:TRANSFERRED]->(d:Account {accountID: destination})
WHERE m.accountID IN mules AND a.ts <= b.ts
RETURN s, a, m, b, d;
