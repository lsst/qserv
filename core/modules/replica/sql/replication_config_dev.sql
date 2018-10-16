SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 ;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 ;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL' ;

-----------------------------------------------------------
-- Preload configuration parameters for testing purposes --
-----------------------------------------------------------
--
-- The script is meant to configure a single-worker side replication
-- server which embedds all worker serviceses and the relevant file
-- servers.
--
-- The data files (affected by the system) are located on the shared
-- GPFS file system:
--     /datasets/gapon/test/replication/{worker}
--
-- All servers are supposed to run on this host:
--     lsst-dev.ncsa.illinois.edu
--


-- Common parameters of all types of servers

INSERT INTO `config` VALUES ('common', 'request_buf_size_bytes',     '131072');
INSERT INTO `config` VALUES ('common', 'request_retry_interval_sec', '1');

-- Controller-specific parameters

INSERT INTO `config` VALUES ('controller', 'num_threads',           '1');
INSERT INTO `config` VALUES ('controller', 'http_server_port',      '80');
INSERT INTO `config` VALUES ('controller', 'http_server_threads',    '1');
INSERT INTO `config` VALUES ('controller', 'request_timeout_sec',  '600');
INSERT INTO `config` VALUES ('controller', 'job_timeout_sec',     '6000');
INSERT INTO `config` VALUES ('controller', 'job_heartbeat_sec',     '60');

-- Database service-specific parameters

INSERT INTO `config` VALUES ('database', 'services_pool_size', '1');

-- Connection parameters for the Qserv Management Services

INSERT INTO `config` VALUES ('xrootd', 'auto_notify',         '0');
INSERT INTO `config` VALUES ('xrootd', 'host',                'localhost');
INSERT INTO `config` VALUES ('xrootd', 'port',                '1094');
INSERT INTO `config` VALUES ('xrootd', 'request_timeout_sec', '600');

-- Default parameters for all workers unless overwritten in worker-specific
-- tables

INSERT INTO `config` VALUES ('worker', 'technology',                 'FS');
INSERT INTO `config` VALUES ('worker', 'svc_port',                   '25000');
INSERT INTO `config` VALUES ('worker', 'fs_port',                    '25001');
INSERT INTO `config` VALUES ('worker', 'num_svc_processing_threads', '10');
INSERT INTO `config` VALUES ('worker', 'num_fs_processing_threads',  '16');
INSERT INTO `config` VALUES ('worker', 'fs_buf_size_bytes',          '1048576');
INSERT INTO `config` VALUES ('worker', 'data_dir',                   '/datasets/gapon/test/replication/{worker}');

-- Preload parameters for runnig all services on the same host

INSERT INTO `config_worker` VALUES ('one',   1, 0, 'lsst-dev01', '25001', 'lsst-dev01', '25101', NULL);
INSERT INTO `config_worker` VALUES ('two',   1, 0, 'lsst-dev01', '25002', 'lsst-dev01', '25102', NULL);
INSERT INTO `config_worker` VALUES ('three', 1, 0, 'lsst-dev01', '25003', 'lsst-dev01', '25103', NULL);
INSERT INTO `config_worker` VALUES ('four',  1, 0, 'lsst-dev01', '25004', 'lsst-dev01', '25104', NULL);
INSERT INTO `config_worker` VALUES ('five',  1, 0, 'lsst-dev01', '25005', 'lsst-dev01', '25105', NULL);
INSERT INTO `config_worker` VALUES ('six',   1, 0, 'lsst-dev01', '25006', 'lsst-dev01', '25106', NULL);
INSERT INTO `config_worker` VALUES ('seven', 1, 0, 'lsst-dev01', '25007', 'lsst-dev01', '25107', NULL);

-- This database lives witin its own family

INSERT INTO `config_database_family` VALUES ('db1', 1, 340, 12);
INSERT INTO `config_database`        VALUES ('db1', 'db1');
INSERT INTO `config_database_table`  VALUES ('db1', 'Object',       1);
INSERT INTO `config_database_table`  VALUES ('db1', 'Source',       1);
INSERT INTO `config_database_table`  VALUES ('db1', 'ForcedSource', 1);
INSERT INTO `config_database_table`  VALUES ('db1', 'Filter',       0);

-- This family has two members for which the replication activities need
-- to be coordinated.

INSERT INTO `config_database_family` VALUES ('production', 3, 340, 12);
INSERT INTO `config_database`        VALUES ('db2', 'production');
INSERT INTO `config_database`        VALUES ('db3', 'production');

INSERT INTO `config_database_table`  VALUES ('db2', 'Main', 1);

INSERT INTO `config_database_table`  VALUES ('db3', 'Object',       1);
INSERT INTO `config_database_table`  VALUES ('db3', 'Source',       1);
INSERT INTO `config_database_table`  VALUES ('db3', 'ForcedSource', 1);


SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;
