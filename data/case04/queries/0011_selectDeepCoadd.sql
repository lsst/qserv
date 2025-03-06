-- Note: ordering is required to compare MySQL vs Qserv results because
-- partial results reported for Qserv chunked queries have non-deterministic
-- order.

-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

SELECT DISTINCT tract,patch,filterName FROM DeepCoadd ORDER BY tract,patch,filterName;
