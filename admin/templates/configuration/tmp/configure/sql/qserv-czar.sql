CREATE OR REPLACE USER 'qsmaster'@'localhost';

CREATE DATABASE IF NOT EXISTS qservResult;
GRANT ALL ON qservResult.* TO 'qsmaster'@'localhost';

-- Secondary index database (i.e. objectId/chunkId relation)
-- created by integration test script/loader for now
CREATE DATABASE IF NOT EXISTS qservMeta;
GRANT ALL ON qservMeta.* TO 'qsmaster'@'localhost';

-- CSS database
CREATE DATABASE IF NOT EXISTS qservCssData;
GRANT ALL ON qservCssData.* TO 'qsmaster'@'localhost';

-- Database for business (i.e. LSST) data
-- In the long term:
--    * has to created by the dataloader
--    * should be only on workers
-- For now, mysql-proxy fails if this table
-- doesn't exist in the database.
-- CREATE DATABASE IF NOT EXISTS LSST;
-- GRANT SELECT ON LSST.* TO 'qsmaster'@'localhost';
