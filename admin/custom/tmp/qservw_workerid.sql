DROP DATABASE IF EXISTS qservw_worker;
CREATE DATABASE qservw_worker;
GRANT ALL ON qservw_worker.* TO 'qsmaster'@'localhost';
FLUSH PRIVILEGES;
CREATE TABLE qservw_worker.Dbs (
  `db` char(200) NOT NULL
);
INSERT INTO qservw_worker.Dbs VALUES('LSST'); 
