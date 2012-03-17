-- Find all objects within ? arcseconds of one another 
-- that have very similar colors
-- http://dev.lsstcorp.org/trac/wiki/dbQuery013

SELECT DISTINCT o1.objectId, o2.objectId
FROM   Object o1, 
       Object o2
WHERE  spDist(o1.ra, o1.decl, o2.ra, o2.decl) < 0.1
  AND  o1.objectId <> o2.objectId
  AND  ABS( (o1.fluxToAbMag(uFlux_PS)-o1.fluxToAbMag(gFlux_PS)) - (o2.fluxToAbMag(uFlux_PS)-o2.fluxToAbMag(gFlux_PS)) ) < 0.1
  AND  ABS( (o1.fluxToAbMag(gFlux_PS)-o1.fluxToAbMag(rFlux_PS)) - (o2.fluxToAbMag(gFlux_PS)-o2.fluxToAbMag(rFlux_PS)) ) < 0.1
  AND  ABS( (o1.fluxToAbMag(rFlux_PS)-o1.fluxToAbMag(iFlux_PS)) - (o2.fluxToAbMag(rFlux_PS)-o2.fluxToAbMag(iFlux_PS)) ) < 0.1
  AND  ABS( (o1.fluxToAbMag(iFlux_PS)-o1.fluxToAbMag(zFlux_PS)) - (o2.fluxToAbMag(iFlux_PS)-o2.fluxToAbMag(zFlux_PS)) ) < 0.1
