SELECT o1.objectId, o2.objectId
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
