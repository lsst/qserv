-- Select time series data for all objects 
-- in a given area of the sky, 
-- in a given photometric band 
-- with a given variability index 
-- See http://dev.lsstcorp.org/trac/wiki/dbQuery007

SELECT objectId, taiObs, psfMag
FROM   Source
JOIN   Object USING(objectId)
JOIN   Filter USING(filterId)
WHERE  areaSpec_box(:raMin, :declMin, :raMax, :declMax)
  AND  filterName = 'u'
  AND  variability BETWEEN :varMin AND :varMax
ORDER BY objectId, taiObs ASC;
