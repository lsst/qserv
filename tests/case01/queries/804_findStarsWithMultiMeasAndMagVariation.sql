-- Find stars with multiple measurements and with certain magnitude variations
-- http://dev.lsstcorp.org/trac/wiki/dbQuery010


-- Missing in current schema: extendedParameter

SELECT  objectId
FROM    Object
WHERE   extendedParameter > 0.8 -- a star
  AND   uMag BETWEEN 1 AND 27  -- magnitudes are reasonable
  AND   gMag BETWEEN 1 AND 27
  AND   rMag BETWEEN 1 AND 27
  AND   iMag BETWEEN 1 AND 27
  AND   zMag BETWEEN 1 AND 27
  AND   yMag BETWEEN 1 AND 27
  AND (                           -- and one of the colors is  different.
         uAmplitude > .1 + ABS(uMagSigma)
      OR gAmplitude > .1 + ABS(gMagSigma)
      OR rAmplitude > .1 + ABS(rMagSigma)
      OR iAmplitude > .1 + ABS(iMagSigma)
      OR zAmplitude > .1 + ABS(zMagSigma)
      OR yAmplitude > .1 + ABS(yMagSigma));