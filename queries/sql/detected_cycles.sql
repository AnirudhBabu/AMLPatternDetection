-- Cycling: average amount at each hop
SELECT Hop_Number AS hop, ROUND(AVG(Amount), 2) AS avg_amount
FROM detected_cycles
GROUP BY Hop_Number
ORDER BY Hop_Number
