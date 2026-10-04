-- Deposit-send: hours between cash deposit and payment
SELECT CAST(Hours_Held / 6 AS INTEGER) * 6 AS hours_held, COUNT(*) AS pairs
FROM deposit_send_suspects
GROUP BY 1
ORDER BY 1
