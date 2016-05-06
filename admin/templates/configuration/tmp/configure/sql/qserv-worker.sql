CREATE USER IF NOT EXISTS '{{MYSQLD_USER_QSERV}}'@'localhost';

-- Used by xrootd Qserv plugin:
-- to publish LSST databases
DROP DATABASE IF EXISTS qservw_worker;
CREATE DATABASE qservw_worker;
GRANT SELECT ON qservw_worker.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';
CREATE TABLE qservw_worker.Dbs (
  `db` char(200) NOT NULL
);

GRANT ALL ON `q\_memoryLockDb`.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';

-- Subchunks databases
GRANT ALL ON `Subchunks\_%`.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';


-- Create user for external monitoring applications
CREATE USER IF NOT EXISTS '{{MYSQLD_USER_MONITOR}}'@'localhost' IDENTIFIED BY '{{MYSQLD_PASSWORD_MONITOR}}';
GRANT PROCESS ON *.* TO '{{MYSQLD_USER_MONITOR}}'@'localhost';
