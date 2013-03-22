SELECT count(*) 
 FROM   Object 
 WHERE ra_PS BETWEEN 0. AND 0.1
  AND  decl_PS BETWEEN 0.2 AND 0.3
  AND scisql_fluxToAbMag(zFlux_PS)
      BETWEEN 21 AND 21.5  
  AND scisql_fluxToAbMag(gFlux_PS)
      - scisql_fluxToAbMag(rFlux_PS) 
      BETWEEN 0.3 AND 0.4  
  AND scisql_fluxToAbMag(iFlux_PS)
      - scisql_fluxToAbMag(zFlux_PS) 
      BETWEEN 0.1 AND 0.12
