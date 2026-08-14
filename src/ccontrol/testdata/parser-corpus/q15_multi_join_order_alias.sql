SELECT mt.id_truth_type AS mt_id_truth_type,
       mt.match_objectId AS mt_match_objectId,
       obj.objectId AS obj_objectId,
       ts.redshift AS ts_redshift
FROM dp02_dc2_catalogs.MatchesTruth AS mt
JOIN dp02_dc2_catalogs.TruthSummary AS ts ON mt.id_truth_type = ts.id_truth_type
JOIN dp02_dc2_catalogs.Object AS obj ON mt.match_objectId = obj.objectId
WHERE obj.objectId = 1486698050427598336
  AND ts.truth_type = 1
  AND obj.detect_isPrimary = 1
ORDER BY obj_objectId DESC
LIMIT 100000001
