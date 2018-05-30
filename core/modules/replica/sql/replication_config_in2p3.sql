SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 ;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 ;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL' ;

-----------------------------------------------------------
-- Preload configuration parameters for testing purposes --
-----------------------------------------------------------

-- Common parameters of all types of servers

INSERT INTO `config` VALUES ('common', 'request_buf_size_bytes',     '131072');
INSERT INTO `config` VALUES ('common', 'request_retry_interval_sec', '1');

-- Controller-specific parameters

INSERT INTO `config` VALUES ('controller', 'num_threads',           '1');
INSERT INTO `config` VALUES ('controller', 'http_server_port',      '80');
INSERT INTO `config` VALUES ('controller', 'http_server_threads',    '1');
INSERT INTO `config` VALUES ('controller', 'request_timeout_sec', '14400');   -- 4 hours
INSERT INTO `config` VALUES ('controller', 'job_timeout_sec',     '28800');   -- 8 hours
INSERT INTO `config` VALUES ('controller', 'job_heartbeat_sec',     '60');

-- Connection parameters for the Qserv Management Services

INSERT INTO `config` VALUES ('xrootd', 'auto_notify',         '1');
INSERT INTO `config` VALUES ('xrootd', 'host',                'localhost');
INSERT INTO `config` VALUES ('xrootd', 'port',                '21094');
INSERT INTO `config` VALUES ('xrootd', 'request_timeout_sec', '600');

-- Default parameters for all workers unless overwritten in worker-specific
-- tables

INSERT INTO `config` VALUES ('worker', 'technology',                 'FS');
INSERT INTO `config` VALUES ('worker', 'svc_port',                   '50000');
INSERT INTO `config` VALUES ('worker', 'fs_port',                    '50001');
INSERT INTO `config` VALUES ('worker', 'num_svc_processing_threads', '10');
INSERT INTO `config` VALUES ('worker', 'num_fs_processing_threads',  '16');
INSERT INTO `config` VALUES ('worker', 'fs_buf_size_bytes',          '1048576');
INSERT INTO `config` VALUES ('worker', 'data_dir',                   '/qserv/replication/data/mysql');

-- Preload parameters for runnig all services on the same host

INSERT INTO `config_worker` VALUES ('ccqserv126', 1, 0, 'ccqserv126',  NULL, 'ccqserv126',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv127', 1, 0, 'ccqserv127',  NULL, 'ccqserv127',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv128', 1, 0, 'ccqserv128',  NULL, 'ccqserv128',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv129', 1, 0, 'ccqserv129',  NULL, 'ccqserv129',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv130', 1, 0, 'ccqserv130',  NULL, 'ccqserv130',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv131', 1, 0, 'ccqserv131',  NULL, 'ccqserv131',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv132', 1, 0, 'ccqserv132',  NULL, 'ccqserv132',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv133', 1, 0, 'ccqserv133',  NULL, 'ccqserv133',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv134', 1, 0, 'ccqserv134',  NULL, 'ccqserv134',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv135', 1, 0, 'ccqserv135',  NULL, 'ccqserv135',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv136', 1, 0, 'ccqserv136',  NULL, 'ccqserv136',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv137', 1, 0, 'ccqserv137',  NULL, 'ccqserv137',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv138', 1, 0, 'ccqserv138',  NULL, 'ccqserv138',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv139', 1, 0, 'ccqserv139',  NULL, 'ccqserv139',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv140', 1, 0, 'ccqserv140',  NULL, 'ccqserv140',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv141', 1, 0, 'ccqserv141',  NULL, 'ccqserv141',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv142', 1, 0, 'ccqserv142',  NULL, 'ccqserv142',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv143', 1, 0, 'ccqserv143',  NULL, 'ccqserv143',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv144', 1, 0, 'ccqserv144',  NULL, 'ccqserv144',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv145', 1, 0, 'ccqserv145',  NULL, 'ccqserv145',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv146', 1, 0, 'ccqserv146',  NULL, 'ccqserv146',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv147', 1, 0, 'ccqserv147',  NULL, 'ccqserv147',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv148', 1, 0, 'ccqserv148',  NULL, 'ccqserv148',  NULL, NULL);
INSERT INTO `config_worker` VALUES ('ccqserv149', 1, 0, 'ccqserv149',  NULL, 'ccqserv149',  NULL, NULL);

-- This database lives witin its own family

INSERT INTO `config_database_family` VALUES ('production',        2, 340, 12);
INSERT INTO `config_database`        VALUES ('sdss_stripe82_01', 'production');
INSERT INTO `config_database_table`  VALUES ('sdss_stripe82_01', 'RunDeepSource',       1);
INSERT INTO `config_database_table`  VALUES ('sdss_stripe82_01', 'RunDeepForcedSource', 1);
INSERT INTO `config_database`        VALUES ('wise_00',          'production');
INSERT INTO `config_database_table`  VALUES ('wise_00',          'allwise_p3as_psd',    1);
INSERT INTO `config_database_table`  VALUES ('wise_00',          'allwise_p3as_mep',    1);


SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;
