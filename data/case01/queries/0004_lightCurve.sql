-- Extract light curve for a given object (time, magnitude and position)
-- See http://dev.lsstcorp.org/trac/wiki/dbQuery001

-- pragma sortresult

SELECT taiMidPoint, psfFlux, psfFluxSigma, ra, decl
FROM   Source
JOIN   Filter USING (filterId)
WHERE  objectId = 402412665835716
   AND filterName = 'r'
