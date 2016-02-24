CREATE USER IF NOT EXISTS 'qsmaster'@'localhost';

CREATE DATABASE IF NOT EXISTS qservResult;
GRANT ALL ON qservResult.* TO 'qsmaster'@'localhost';

-- Secondary index database (i.e. objectId/chunkId relation)
-- created by integration test script/loader for now
CREATE DATABASE IF NOT EXISTS qservMeta;
GRANT ALL ON qservMeta.* TO 'qsmaster'@'localhost';

-- CSS database
CREATE DATABASE IF NOT EXISTS qservCssData;
GRANT ALL ON qservCssData.* TO 'qsmaster'@'localhost';
