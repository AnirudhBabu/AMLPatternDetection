-- Detector scorecard
SELECT detector, flagged_tx AS "flagged transactions",
       ROUND(100 * precision_tx, 2) AS "precision % (transactions)",
       ROUND(lift_tx, 1) AS "lift (transactions)",
       flagged_accounts AS "flagged accounts",
       ROUND(100 * precision_accounts, 2) AS "precision % (accounts)"
FROM evaluation_summary
