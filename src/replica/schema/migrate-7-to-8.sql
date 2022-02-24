-- Eliminate obsolete parameters from the table. Worker connection parameters
-- are now available from the transient Redirector server.

ALTER TABLE `config_worker` DROP INDEX IF EXISTS `svc_host` ;
ALTER TABLE `config_worker` DROP INDEX IF EXISTS `fs_host` ;
ALTER TABLE `config_worker` DROP INDEX IF EXISTS `loader_host` ;
ALTER TABLE `config_worker` DROP INDEX IF EXISTS `exporter_host` ;
ALTER TABLE `config_worker` DROP INDEX IF EXISTS `http_loader_host` ;


ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `svc_host` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `svc_port` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `fs_host` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `fs_port` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `data_dir` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `loader_host` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `loader_port` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `loader_tmp_dir` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `exporter_host` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `exporter_port` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `exporter_tmp_dir` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `http_loader_host` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `http_loader_port` ;
ALTER TABLE `config_worker` DROP COLUMN IF EXISTS `http_loader_tmp_dir` ;
