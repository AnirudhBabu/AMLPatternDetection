SELECT 
    Receiver_account, 
    AVG(CAST(Amount AS DOUBLE)) AS Avg_Amount,
    CASE 
        -- If the Duration string contains "days"
        WHEN Duration LIKE '%days' THEN 
            CAST(SUBSTR(Duration, 1, INSTR(Duration, ' ') - 1) AS DOUBLE)
        -- If it's hours (or anything else), divide by 24
        ELSE 
            CAST(SUBSTR(Duration, 1, INSTR(Duration, ' ') - 1) AS DOUBLE) / 24.0
    END AS Duration_Days,
	CAST(Total_amount AS DOUBLE) AS Total_amount,
	Sender_count
FROM smurfs
WHERE Duration_Days >= 1
GROUP BY Receiver_account;