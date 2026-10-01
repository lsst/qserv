-- pragma sortresult
-- A replicated lookup does not select one director's chunk copy of a match.
-- See 0011_replicated_lookup_or.sql for the synthetic fixture details.
-- Each synthetic cross-chunk match must appear once, as in reference MySQL.
-- Preserve matches with no first director; their second-director copy is required.
-- The inherited case03 data supplies 222 such rows with refObjectId NULL.
-- With the two synthetic matches and the single lookup row, expect 224 rows.
-- Keep the restriction in ON to also exercise a query without a WHERE clause.
SELECT m.refObjectId, m.deepSourceId, l.lookupId
FROM qcase05_matches.RefDeepSrcMatch AS m
JOIN MatchLookup AS l ON l.lookupId = 1
  AND (m.refObjectId IN (8000000000000000001, 8000000000000000002)
       OR m.refObjectId IS NULL);
