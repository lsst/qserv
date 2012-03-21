-- Select all objects with certain variability or period or amplitude
-- http://dev.lsstcorp.org/trac/wiki/dbQuery011

-- Missing in current schema: variability

SELECT *
FROM   Object
WHERE  variability > 0.8 -- variable object
   AND uTimescale < :timescaleMax
   AND gTimescale < :timescaleMax
   AND rTimescale < :timescaleMax
   AND iTimescale < :timescaleMax
   AND zTimescale < :timescaleMax
   AND yTimescale < :timescaleMax
    OR primaryPeriod BETWEEN :periodMin AND :periodMax 
    OR uAmplitude > :amplitudeMin
    OR gAmplitude > :amplitudeMin
    OR rAmplitude > :amplitudeMin
    OR iAmplitude > :amplitudeMin
    OR zAmplitude > :amplitudeMin
    OR yAmplitude > :amplitudeMin