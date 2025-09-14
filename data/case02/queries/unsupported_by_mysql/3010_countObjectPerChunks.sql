-- Count object per Chunks
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/Qserv/250Development

-- Note: chunkId field is not in MySQL tables. The query is still
-- a valid Qserv query.

SELECT count(*) AS n,
        AVG(ra_PS),
        AVG(decl_PS),
        objectId,
        chunkId
 FROM Object 
 GROUP BY chunkId
