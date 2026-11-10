-- pragma sortresult
-- Edge case where match 3 and 4 are near a chunk/subchunk boundary around ra 55.08
-- to force this to take the duplicate handling path. Match 5 has directors that
-- are co-located in the same chunk.
SELECT r.refObjectId, r.rMag, s.id AS deepSourceId
FROM qcase05_1.RefObject AS r
JOIN qcase05_matches.RefDeepSrcMatch AS m ON r.refObjectId = m.refObjectId
JOIN qcase05_2.RunDeepSource AS s ON m.deepSourceId = s.id
WHERE m.angSep BETWEEN 1.4 AND 1.5;
