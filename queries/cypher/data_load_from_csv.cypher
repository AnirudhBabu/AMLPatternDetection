
// Data loaders

LOAD CSV FROM "/data/SAML-D.csv" WITH HEADER AS row
MERGE (s:Account {accountID: toInteger(row.Sender_account)})
SET s:Sender
MERGE (r:Account {accountID: toInteger(row.Receiver_account)})
SET r:Receiver

// Create a direct relationship instead of a node
CREATE (s)-[:TRANSFERRED {
    amount: toFloat(row.Amount),
    datetime: datetime(row.Date+"T"+row.Time),
    type: row.Laundering_type
}]->(r);