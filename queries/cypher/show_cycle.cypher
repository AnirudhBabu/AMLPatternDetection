// Visualize one detected ring: paste the accounts of one Ring_ID from
// data/detected_cycles.csv (Sender_account column, in Hop_Number order).
WITH [2521152088, 718745407, 8657935466, 2361741456, 2299468667] AS ring
MATCH (a:Account)-[e:TRANSFERRED]->(b:Account)
WHERE a.accountID IN ring AND b.accountID IN ring
RETURN a, e, b;
