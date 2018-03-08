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
--     /qserv/data/mysql
-- 
-- All worker servers are supposed to run on thess hosts:
--     lsst-gapon-qserv-worker-[1-10]
--
-- The controller (master side) operations can be run from any
-- host, invluding the master host of the cluster:
--     lsst-gapon-qserv-master-03

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
INSERT INTO `config` VALUES ('worker', 'data_dir',                   '/qserv/data/mysql');

-- Preload parameters for runnig all services on the same host

INSERT INTO `config_worker` VALUES ('1fa1b434-0793-11e8-92c7-fa163e090caf', 1, 0, 'lsst-gapon-qserv-worker-1',  NULL, 'lsst-gapon-qserv-worker-1',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('2d526ae4-0793-11e8-a8e1-fa163e53bca0', 1, 0, 'lsst-gapon-qserv-worker-2',  NULL, 'lsst-gapon-qserv-worker-2',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('4175eabd-0793-11e8-a29e-fa163e50be0b', 1, 0, 'lsst-gapon-qserv-worker-3',  NULL, 'lsst-gapon-qserv-worker-3',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('68b673cd-0793-11e8-b97e-fa163e273ca5', 1, 0, 'lsst-gapon-qserv-worker-4',  NULL, 'lsst-gapon-qserv-worker-4',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('7338f93f-0793-11e8-8d98-fa163ec74a6d', 1, 0, 'lsst-gapon-qserv-worker-5',  NULL, 'lsst-gapon-qserv-worker-5',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('7d413e45-0793-11e8-8599-fa163ead27cd', 1, 0, 'lsst-gapon-qserv-worker-6',  NULL, 'lsst-gapon-qserv-worker-6',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('8d9e0a08-0793-11e8-8c2c-fa163e3dd200', 1, 0, 'lsst-gapon-qserv-worker-7',  NULL, 'lsst-gapon-qserv-worker-7',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('9f3a8712-0793-11e8-9651-fa163ee9c17d', 1, 0, 'lsst-gapon-qserv-worker-8',  NULL, 'lsst-gapon-qserv-worker-8',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ac07f0ec-0793-11e8-8856-fa163ef4b714', 1, 0, 'lsst-gapon-qserv-worker-9',  NULL, 'lsst-gapon-qserv-worker-9',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('fb0d729f-0792-11e8-9ff8-fa163ec7fdcd', 1, 0, 'lsst-gapon-qserv-worker-10', NULL, 'lsst-gapon-qserv-worker-10', NULL, NULL);

-- This database lives witin its own family

INSERT INTO `config_database_family` VALUES ('production', 2);
INSERT INTO `config_database`        VALUES ('wise_00', 'production');
INSERT INTO `config_database_table`  VALUES ('wise_00', 'Object',       1);
INSERT INTO `config_database_table`  VALUES ('wise_00', 'ForcedSource', 1);


SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;