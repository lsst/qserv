-- pragma sortresult
SELECT COUNT(*)
FROM RefObject AS r
JOIN qcase05_matches.RefDeepSrcMatch AS m ON r.refObjectId = m.refObjectId
JOIN qcase05_2.RunDeepSource AS s ON m.deepSourceId = s.id;
