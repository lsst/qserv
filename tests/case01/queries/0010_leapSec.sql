-- This query is frequently executed by buildbot

-- See ticket #2048

SELECT offset, mjdRef, drift
FROM LeapSeconds 
WHERE whenUtc = (
        SELECT MAX(whenUtc) 
        FROM LeapSeconds 
        WHERE whenUtc <=  NAME_CONST('nsecs_',39900600000000000000000000)
                )
