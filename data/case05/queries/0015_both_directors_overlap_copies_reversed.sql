-- pragma sortresult
-- Same as 0014_both_directors_overlap_copies.sql, with RunDeepSource first
-- so overlap comes from RefObject
SELECT r.refObjectId, r.rMag, s.id AS deepSourceId
FROM qcase05_2.RunDeepSource AS s
JOIN qcase05_matches.RefDeepSrcMatch AS m ON m.deepSourceId = s.id
JOIN qcase05_1.RefObject AS r ON r.refObjectId = m.refObjectId
WHERE m.angSep BETWEEN 1.4 AND 1.5;
