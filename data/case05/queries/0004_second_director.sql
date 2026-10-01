-- pragma sortresult
SELECT m.refObjectId, s.id AS deepSourceId, s.flux_psf, s.coadd_filter_id
FROM qcase05_matches.RefDeepSrcMatch AS m
JOIN qcase05_2.RunDeepSource AS s ON m.deepSourceId = s.id;
