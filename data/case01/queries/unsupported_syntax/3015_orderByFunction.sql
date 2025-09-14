-- Functions in the ORDER BY clause aren't supported by Qserv.

SELECT objectID
FROM Object
WHERE scisql_fluxToAbMag(zFlux_PS) BETWEEN 20 AND 24
ORDER BY (scisql_fluxToAbMag(gFlux_PS)-scisql_fluxToAbMag(rFlux_PS))
