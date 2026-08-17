SELECT
    Source.objectId,
    COUNT(*) AS n_sources,
    MAX(Source.raFlux) - MIN(Source.raFlux) AS ra_flux_span,
    MAX(Source.declFlux) - MIN(Source.declFlux) AS decl_flux_span,
    MAX(Source.taiMidPoint) - MIN(Source.taiMidPoint) AS time_span,
    MIN(Source.ra) AS min_ra,
    MAX(Source.ra) AS max_ra,
    MIN(Source.decl) AS min_decl,
    MAX(Source.decl) AS max_decl
FROM Source
WHERE Source.objectId BETWEEN 100000 AND 999999
  AND Source.raFlux > 0
  AND Source.declFlux > 0
GROUP BY Source.objectId
ORDER BY Source.objectId
LIMIT 100
