-- Qserv doesn't support this query because of:
-- ERROR 4110 (Proxy): Query processing error: QI=?: Failed to instantiate query: AnalysisError:Query
--   involves partitioned table joins that Qserv does not know how to evaluate using only partition-local data

-- Find all objects within ? arcseconds of one another 
-- that have very similar colors
-- http://dev.lsstcorp.org/trac/wiki/dbQuery013
--
-- Similar queries:
--
-- * Find all galaxies without saturated pixels within 
--   certain distance of a given point
--   http://dev.lsstcorp.org/trac/wiki/dbQuery023

-- see ticket #1840 (the code that needs subchunks is
-- currently using hardcoded database name "LSST")

SELECT DISTINCT o1.objectId, o2.objectId
FROM   Object o1, 
       Object o2
WHERE  scisql_angSep(o1.ra_PS, o1.decl_PS, o2.ra_PS, o2.decl_PS) < 1
  AND  o1.objectId <> o2.objectId
  AND  ABS( (scisql_fluxToAbMag(o1.gFlux_PS)-scisql_fluxToAbMag(o1.rFlux_PS)) - 
            (scisql_fluxToAbMag(o2.gFlux_PS)-scisql_fluxToAbMag(o2.rFlux_PS)) ) < 1
  AND  ABS( (scisql_fluxToAbMag(o1.rFlux_PS)-scisql_fluxToAbMag(o1.iFlux_PS)) - 
            (scisql_fluxToAbMag(o2.rFlux_PS)-scisql_fluxToAbMag(o2.iFlux_PS)) ) < 1
  AND  ABS( (scisql_fluxToAbMag(o1.iFlux_PS)-scisql_fluxToAbMag(o1.zFlux_PS)) - 
            (scisql_fluxToAbMag(o2.iFlux_PS)-scisql_fluxToAbMag(o2.zFlux_PS)) ) < 1
