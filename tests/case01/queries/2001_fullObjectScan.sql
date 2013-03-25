-- Full table scan for Object table with some cuts.
--
-- Similar queries:
--
-- * Find quasars 
--   http://dev.lsstcorp.org/trac/wiki/dbQuery018
--
-- * Low-z QSO candidates using the color cuts
--   http://dev.lsstcorp.org/trac/wiki/dbQuery020
--
-- * Find high proper motion white dwarf candidates
--   http://dev.lsstcorp.org/trac/wiki/dbQuery026
--
-- * Find extremely red galaxies
--   http://dev.lsstcorp.org/trac/wiki/dbQuery037


SELECT objectId,
       scisql_fluxToAbMag(uFlux_PS),
       scisql_fluxToAbMag(gFlux_PS),
       scisql_fluxToAbMag(rFlux_PS),
       scisql_fluxToAbMag(iFlux_PS),
       scisql_fluxToAbMag(zFlux_PS),
       scisql_fluxToAbMag(yFlux_PS),
       ra_PS, decl_PS
FROM   Object
WHERE  ( scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.7 OR 
         scisql_fluxToAbMag(gFlux_PS) > 22.3 )
AND    scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 0.1
AND    ( scisql_fluxToAbMag(rFlux_PS)-scisql_fluxToAbMag(iFlux_PS) < 
         (0.08 + 0.42 * (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) - 0.96)) 
        OR scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS) > 1.26 )
AND    scisql_fluxToAbMag(iFlux_PS)-scisql_fluxToAbMag(zFlux_PS) < 0.8
ORDER BY 1
