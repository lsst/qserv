SELECT v.visit, v.expMidptMJD, v.band, COUNT(s.sourceId)
FROM dp1.Visit AS v
JOIN dp1.Source AS s ON s.visit = v.visit
WHERE scisql_s2PtInCircle(v.ra, v.dec, 53.13, -28.1, 3) = 1
  AND v.band = 'r'
  AND v.expMidptMJD > 60623.25
  AND v.expMidptMJD < 60623.27
GROUP BY v.visit
LIMIT 100000001
