-- trivial query, should return one row (some cleverness
-- is needed to execute this query on one node only!)

-- pragma sortresult
SELECT `offset`, mjdRef, drift FROM LeapSeconds where `offset` = 10
