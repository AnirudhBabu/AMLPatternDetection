SELECT
	Sender_account, 
	Receiver_account,
	Timestamp,
	Hop_Number,
	Cycle_Length,
	CAST(Amount AS DOUBLE) AS Amount
FROM cycles
WHERE {{cycle}}
ORDER BY Timestamp;