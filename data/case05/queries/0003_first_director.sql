-- pragma sortresult
SELECT r.refObjectId, m.deepSourceId
FROM RefObject AS r
JOIN qcase05_matches.RefDeepSrcMatch AS m ON r.refObjectId = m.refObjectId;
