SELECT objectId, ra_PS, decl_PS, 
       scisql_fluxToAbMag(zFlux_PS) 
 FROM Object 
 WHERE scisql_fluxToAbMag(zFlux_PS)
       BETWEEN 20 AND 24
