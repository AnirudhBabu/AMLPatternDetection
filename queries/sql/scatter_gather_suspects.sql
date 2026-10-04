-- Scatter-gather: episodes by number of mules
SELECT Episode_ID, Source_account, Destination_account, MAX(Mule_count) AS mules,
       MAX(Total_scattered) AS scattered, MAX(Total_gathered) AS gathered,
       MAX(Gather_Ratio) AS gather_ratio, MIN(Episode_Start) AS started,
       MAX(Episode_Duration_Days) AS days
FROM scatter_gather_suspects
GROUP BY Episode_ID, Source_account, Destination_account
ORDER BY mules DESC, scattered DESC
