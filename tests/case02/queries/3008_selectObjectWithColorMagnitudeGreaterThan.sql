-- Select object with IZ magnitude greater than a constant
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/queries/051

SELECT objectId, ra_PS, decl_PS,
        uFlux_PS, gFlux_PS, rFlux_PS, 
        iFlux_PS, zFlux_PS, yFlux_PS 
 FROM Object
 WHERE scisql_fluxToAbMag(iFlux_PS)
       - scisql_fluxToAbMag(zFlux_PS) > 4
