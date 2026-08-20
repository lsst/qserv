-- Variant of 1030_timeSeries.sql using implicit join conditions.

-- pragma noheader
SELECT s.objectId, s.taiMidPoint, scisql_fluxToAbMag(s.psfFlux)
FROM   Source AS s
JOIN   Object AS o
INNER JOIN Filter AS f
WHERE  s.objectId = o.objectId
  AND  s.filterId = f.filterId
  AND  o.ra_PS BETWEEN 355 AND 360 -- noQserv
  AND  o.decl_PS BETWEEN 0 AND 20  -- noQserv
-- withQserv AND qserv_areaspec_box(355, 0, 360, 20)
  AND  f.filterName = 'g'
ORDER BY s.objectId, s.taiMidPoint ASC
