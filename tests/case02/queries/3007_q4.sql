-- See https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013

SELECT COUNT(*)
 FROM Object
 WHERE gFlux_PS>1e-25
