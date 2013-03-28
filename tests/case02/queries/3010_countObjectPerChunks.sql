-- Count object per Chunks
-- https://dev.lsstcorp.org/trac/wiki/db/Qserv/IN2P3/BenchmarkMarch2013
-- Based on https://dev.lsstcorp.org/trac/wiki/db/Qserv/250Development

SELECT count(*) AS n,
        AVG(ra_PS),
        AVG(decl_PS),
        objectId,
        _chunkId
 FROM Object 
 GROUP BY _chunkId
