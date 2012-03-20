-- Select time series data for all objects 
-- in a given area of the sky, 
-- in a given photometric band 
-- Similar query: http://dev.lsstcorp.org/trac/wiki/dbQuery007

SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux)
FROM   Source
JOIN   Object USING(objectId)
JOIN   Filter USING(filterId)
-- WHERE  areaSpec_box(:raMin, :declMin, :raMax, :declMax)
 WHERE ra_PS between 355 and 360 and decl_PS between 0 and 20
   AND filterName = 'u'
ORDER BY objectId, taiMidPoint ASC
