-- Select time series data for all objects 
-- in a given area of the sky, 
-- in a given photometric band 
-- Similar query: http://dev.lsstcorp.org/trac/wiki/dbQuery007

-- See ticket #2052

SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux)
FROM   Source
JOIN   Object USING(objectId)
JOIN   Filter USING(filterId)
 WHERE ra_PS BETWEEN 355 AND 360 -- noQserv
   and decl_PS BETWEEN 0 AND 20  -- noQserv
-- withQserv WHERE qserv_areaspec_box(355, 0, 360, 20)
   AND filterName = 'g'
ORDER BY objectId, taiMidPoint ASC
