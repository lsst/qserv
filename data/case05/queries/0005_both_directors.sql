-- pragma sortresult
SELECT r.refObjectId, r.rMag, s.id AS deepSourceId, s.flux_psf, s.coadd_filter_id
FROM RefObject AS r
JOIN qcase05_matches.RefDeepSrcMatch AS m ON r.refObjectId = m.refObjectId
JOIN qcase05_2.RunDeepSource AS s ON m.deepSourceId = s.id;
