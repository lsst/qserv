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
-- The data files (affected by the system) are located on each worker's
-- local file system:
--     /qserv/replication
-- 
-- All worker servers are supposed to run on thess hosts:
--     lsst-gapon-qserv-worker-[1-10]
--
-- The controller (master side) operations can be run from any
-- host, invluding the master host of the cluster:
--     lsst-gapon-qserv-master-01

-- Common parameters of all types of servers

INSERT INTO `config` VALUES ('common', 'request_buf_size_bytes',     '1024');
INSERT INTO `config` VALUES ('common', 'request_retry_interval_sec', '1');

-- Controller-specific parameters

INSERT INTO `config` VALUES ('controller', 'http_server_port',    '80');
INSERT INTO `config` VALUES ('controller', 'http_server_threads', '1');
INSERT INTO `config` VALUES ('controller', 'request_timeout_sec', '600');

-- Connection parameters for the Qserv Management Services

INSERT INTO `config` VALUES ('xrootd', 'host',                'localhost');
INSERT INTO `config` VALUES ('xrootd', 'port',                '1094');
INSERT INTO `config` VALUES ('xrootd', 'request_timeout_sec', '600');

-- Default parameters for all workers unless overwritten in worker-specific
-- tables

INSERT INTO `config` VALUES ('worker', 'technology',                 'FS');
INSERT INTO `config` VALUES ('worker', 'svc_port',                   '50000');
INSERT INTO `config` VALUES ('worker', 'fs_port',                    '50001');
INSERT INTO `config` VALUES ('worker', 'num_svc_processing_threads', '10');
INSERT INTO `config` VALUES ('worker', 'num_fs_processing_threads',  '16');
INSERT INTO `config` VALUES ('worker', 'fs_buf_size_bytes',          '1048576');
INSERT INTO `config` VALUES ('worker', 'data_dir',                   '/qserv/replication');

-- Preload parameters for runnig all services on the same host

INSERT INTO `config_worker` VALUES ('worker-1',  1, 0, 'lsst-gapon-qserv-worker-1',  NULL, 'lsst-gapon-qserv-worker-1',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-2',  1, 0, 'lsst-gapon-qserv-worker-2',  NULL, 'lsst-gapon-qserv-worker-2',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-3',  1, 0, 'lsst-gapon-qserv-worker-3',  NULL, 'lsst-gapon-qserv-worker-3',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-4',  1, 0, 'lsst-gapon-qserv-worker-4',  NULL, 'lsst-gapon-qserv-worker-4',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-5',  1, 0, 'lsst-gapon-qserv-worker-5',  NULL, 'lsst-gapon-qserv-worker-5',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-6',  1, 0, 'lsst-gapon-qserv-worker-6',  NULL, 'lsst-gapon-qserv-worker-6',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-7',  1, 0, 'lsst-gapon-qserv-worker-7',  NULL, 'lsst-gapon-qserv-worker-7',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-8',  1, 0, 'lsst-gapon-qserv-worker-8',  NULL, 'lsst-gapon-qserv-worker-8',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-9',  1, 0, 'lsst-gapon-qserv-worker-9',  NULL, 'lsst-gapon-qserv-worker-9',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('worker-10', 1, 0, 'lsst-gapon-qserv-worker-10', NULL, 'lsst-gapon-qserv-worker-10', NULL, NULL);

-- This database lives witin its own family

INSERT INTO `config_database_family` VALUES ('db1', 1);
INSERT INTO `config_database`        VALUES ('db1', 'db1');
INSERT INTO `config_database_table`  VALUES ('db1', 'Object',       1);
INSERT INTO `config_database_table`  VALUES ('db1', 'Source',       1);
INSERT INTO `config_database_table`  VALUES ('db1', 'ForcedSource', 1);
INSERT INTO `config_database_table`  VALUES ('db1', 'Filter',       0);

-- This family has two members for which the replication activities need
-- to be coordinated.

INSERT INTO `config_database_family` VALUES ('production', 3);
INSERT INTO `config_database`        VALUES ('db2', 'production');
INSERT INTO `config_database`        VALUES ('db3', 'production');

INSERT INTO `config_database_table`  VALUES ('db2', 'Main', 1);

INSERT INTO `config_database_table`  VALUES ('db3', 'Object',       1);
INSERT INTO `config_database_table`  VALUES ('db3', 'Source',       1);
INSERT INTO `config_database_table`  VALUES ('db3', 'ForcedSource', 1);


SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;