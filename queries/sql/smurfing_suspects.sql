-- Smurfing: episode duration vs average payment
SELECT Episode_ID, Receiver_account, MAX(Duration_Days) AS duration_days,
       ROUND(AVG(Amount), 2) AS avg_amount, MAX(Sender_count) AS senders
FROM smurfing_suspects
GROUP BY Episode_ID, Receiver_account
