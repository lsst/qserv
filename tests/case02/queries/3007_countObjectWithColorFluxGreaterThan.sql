-- Count the number of object with a color flux greater than a constant
-- See https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/queries/026

SELECT COUNT(*)
 FROM Object
 WHERE gFlux_PS>1e-25
