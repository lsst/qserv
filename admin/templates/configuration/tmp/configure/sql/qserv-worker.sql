-- Used by xrootd Qserv plugin:

--   to publish LSST databases
DROP DATABASE IF EXISTS qservw_worker;
CREATE DATABASE qservw_worker;
GRANT SELECT ON qservw_worker.* TO 'qsmaster'@'localhost';
CREATE TABLE qservw_worker.Dbs (
  `db` char(200) NOT NULL
);

CREATE DATABASE IF NOT EXISTS qservScratch;
GRANT ALL ON qservScratch.* TO 'qsmaster'@'localhost';

GRANT SELECT ON mysql.* TO 'qsmaster'@'localhost';
GRANT ALL ON `q\_%`.* TO 'qsmaster'@'localhost';

-- Subchunks databases
GRANT ALL ON `Subchunks\_%`.* TO 'qsmaster'@'localhost';

-- Database for business (i.e. LSST) data
-- Has to be created by the dataloader in the long term
-- CREATE DATABASE IF NOT EXISTS LSST;
-- GRANT ALL ON LSST.* TO 'qsmaster'@'localhost';
-- INSERT INTO qservw_worker.Dbs VALUES('LSST');
