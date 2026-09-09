-- Recursive aggregate rewriting for multiple groups that span chunks.
-- pragma sortresult

SELECT uFlags, ROUND(100.0 * COUNT(flags) / COUNT(*), 1) AS flagsPerc
FROM   Object
GROUP BY uFlags;
