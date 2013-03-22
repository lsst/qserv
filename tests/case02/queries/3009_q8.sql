SELECT count(*) 
FROM Object
WHERE ra_PS BETWEEN 0. AND 1
  AND decl_PS BETWEEN 2 AND 3
  AND scisql_fluxToAbMag(zFlux_PS) BETWEEN 21 and 21.5
