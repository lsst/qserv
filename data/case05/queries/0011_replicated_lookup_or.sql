-- pragma sortresult
-- Synthetic fixtures: refObjectId 8000000000000000001 matches deepSourceId
-- 8000000000000000101, and 8000000000000000002 matches 8000000000000000102.
-- Both pairs have RA 55 degrees. Their declinations are -1.059023529412 and
-- -1.058623529412 degrees, with the director positions reversed in the second
-- pair. They straddle a chunk boundary for the case05 partitioning (85 stripes,
-- 14 substripes), separated by 0.0004 degrees within the 0.001-degree overlap.
-- Each match therefore has a copy in each director's chunk (flags 1 and 2).
-- MatchLookup is replicated and contains just lookupId=1, so it cannot select
-- one chunk copy. Duplicate filtering must return each synthetic match once.
-- MatchLookup also has refObjectId (NULL) and flags columns: an unqualified
-- duplicate filter would be ambiguous in MySQL, and one bound to l would be a no-op.
-- Keep this OR at the top level: the injected duplicate filter must constrain
-- BOTH alternatives. Without parentheses around the original WHERE, the second
-- match's flags=2 copy would survive. Compare row multiplicities with MySQL.
SELECT m.refObjectId, m.deepSourceId, l.lookupId
FROM MatchLookup AS l
JOIN qcase05_matches.RefDeepSrcMatch AS m ON l.lookupId = 1
WHERE m.refObjectId = 8000000000000000001
   OR m.refObjectId = 8000000000000000002;
