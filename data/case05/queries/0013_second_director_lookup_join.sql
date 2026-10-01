-- pragma sortresult
-- A lookup must not enable standalone filtering when a director selects the copy.
-- Both synthetic matches must survive in their second director's chunk.
SELECT m.refObjectId, s.id AS deepSourceId, s.flux_psf, l.lookupId
FROM qcase05_matches.RefDeepSrcMatch AS m
JOIN qcase05_2.RunDeepSource AS s ON m.deepSourceId = s.id
JOIN MatchLookup AS l ON l.lookupId = 1
WHERE s.coadd_filter_id = 99;
