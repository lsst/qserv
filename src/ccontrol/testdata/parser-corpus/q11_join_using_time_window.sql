SELECT
    s.objectId,
    s.ra,
    s.decl,
    s.taiMidPoint,
    s.raFlux,
    s.declFlux,
    o.ra_PS,
    o.decl_PS,
    o.latestObsTime,
    o.earliestObsTime,
    o.latestObsTime - o.earliestObsTime AS object_time_span,
    s.taiMidPoint - o.earliestObsTime AS source_offset_start,
    o.latestObsTime - s.taiMidPoint AS source_offset_end
FROM Object o
JOIN Source s USING (objectId)
WHERE o.objectId BETWEEN 100000 AND 999999
  AND o.latestObsTime BETWEEN s.taiMidPoint - 300 AND s.taiMidPoint + 300
  AND s.ra BETWEEN o.ra_PS - 0.01 AND o.ra_PS + 0.01
  AND s.decl BETWEEN o.decl_PS - 0.01 AND o.decl_PS + 0.01
ORDER BY s.objectId, s.taiMidPoint
LIMIT 250
