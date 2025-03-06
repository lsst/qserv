-- This query is frequently executed by buildbot

-- See ticket #2048

SELECT `offset`, mjdRef, drift, whenUtc
FROM LeapSeconds
WHERE whenUtc < 39900600000000000000000000
ORDER BY whenUtc DESC
LIMIT 1;
