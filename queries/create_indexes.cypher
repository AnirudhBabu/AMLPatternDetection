// Index creation - Memgraph

// Primary lookups for accounts
CREATE INDEX ON :Account(accountID);

// Filtering for the start of your 5000+ GBP cycles
CREATE INDEX ON :Transaction(amount);
CREATE INDEX ON :Transaction(sent_currency);
CREATE INDEX ON :Transaction(datetime);