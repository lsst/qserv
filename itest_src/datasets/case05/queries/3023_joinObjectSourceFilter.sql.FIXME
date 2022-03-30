-- Join on Source and Filter and select specific filter in region
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- https://dev.lsstcorp.org/trac/wiki/db/queries/007

SELECT objectId, taiMidPoint, fluxToAbMag(psfMag)
FROM   Source
JOIN   Object USING(objectId)
JOIN   Filter USING(filterId)
WHERE   ra_PS BETWEEN 1 AND 2 -- noQserv
   AND decl_PS BETWEEN 3 AND 4 -- noQserv
-- withQserv  qserv_areaspec_box(1,3,2,4)
   AND  filterName = 'u'
   AND  variability BETWEEN 0 AND 2
ORDER BY objectId, taiMidPoint 
