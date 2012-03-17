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
       fluxToAbMag(uFlux_PS),
       fluxToAbMag(gFlux_PS),
       fluxToAbMag(rFlux_PS),
       fluxToAbMag(iFlux_PS),
       fluxToAbMag(zFlux_PS),
       fluxToAbMag(yFlux_PS),
       ra_PS, decl_PS
FROM   Object
WHERE  ( fluxToAbMag(uFlux_PS)-fluxToAbMag(gFlux_PS) > 2.0 OR 
         fluxToAbMag(uFlux_PS) > 22.3 )
AND    fluxToAbMag(uFlux_PS) BETWEEN 0 AND 19 
AND    fluxToAbMag(gFlux_PS)-fluxToAbMag(rFlux_PS) > 1.0 
AND    ( fluxToAbMag(rFlux_PS)-fluxToAbMag(iFlux_PS) < 
         (0.08 + 0.42 * (fluxToAbMag(gFlux_PS)-fluxToAbMag(rFlux_PS) - 0.96)) 
        OR fluxToAbMag(gFlux_PS)-fluxToAbMag(rFlux_PS) > 2.26 )
AND    fluxToAbMag(iFlux_PS)-fluxToAbMag(zFlux_PS) < 0.25 ;