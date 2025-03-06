-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- Exclude special columns added by the partitioner and the Qserv ingest system.
-- If the query wouldn't be restricted the integration test would fail.
SHOW COLUMNS FROM RefObject WHERE Field NOT IN ('qserv_trans_id','chunkId','subChunkId');
