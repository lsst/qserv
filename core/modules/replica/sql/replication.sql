SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 ;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 ;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL' ;

-- --------------------------------------------------------------
-- Table `config`
-- --------------------------------------------------------------
--
-- The common parameters and defaults shared by all components
-- of the replication system. It also provides default values
-- for some critical parameters of the worker-side services.

DROP TABLE IF EXISTS `config` ;

CREATE TABLE IF NOT EXISTS `config` (

  `category` VARCHAR(255) NOT NULL ,
  `param`    VARCHAR(255) NOT NULL ,
  `value`    VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`category`,`param`)
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `config_worker`
-- -----------------------------------------------------
--
-- Worker-specific configuration parameters and overrides
-- of the corresponidng default values if needed

DROP TABLE IF EXISTS `config_worker` ;

CREATE TABLE IF NOT EXISTS `config_worker` (

  `name`         VARCHAR(255)       NOT NULL ,      -- the name of the worker

  `is_enabled`   BOOLEAN            NOT NULL ,      -- is enabled for replication
  `is_read_only` BOOLEAN            NOT NULL ,      -- a subclass of 'is_enabled' which restricts use of
                                                    -- the worker for reading replicas. No new replicas can't be
                                                    -- placed onto this class of workers.

  `svc_host`     VARCHAR(255)       NOT NULL ,      -- the host name on which the worker server runs
  `svc_port`     SMALLINT UNSIGNED  DEFAULT NULL ,  -- override for the global default

  `fs_host`      VARCHAR(255)       NOT NULL ,      -- the host name on which the built-in FileServer runs
  `fs_port`      SMALLINT UNSIGNED  DEFAULT NULL ,  -- override for the global default

  `data_dir`     VARCHAR(255)       DEFAULT NULL ,  -- a file system path to the databases

  PRIMARY KEY (`name`) ,

  UNIQUE  KEY (`svc_host`, `svc_port`) ,
  UNIQUE  KEY (`fs_host`,  `fs_port`)
)
ENGINE = InnoDB;


-- --------------------------------------------------------------
-- Table `config_worker_ext`
-- --------------------------------------------------------------
--
-- The additional parameters overriding the defaults for individual
-- worker services.

DROP TABLE IF EXISTS `config_worker_ext` ;

CREATE TABLE IF NOT EXISTS `config_worker_ext` (

  `worker_name`  VARCHAR(255) NOT NULL ,
  `param`        VARCHAR(255) NOT NULL ,
  `value`        VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`worker_name`, `param`) ,

  CONSTRAINT `config_worker_ext_fk_1`
    FOREIGN KEY (`worker_name` )
    REFERENCES `config_worker` (`name` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `config_database_family`
-- -----------------------------------------------------
--
-- Groups of databases which require coordinated replication
-- efforts (the number of replicas, chunk collocation)
--
-- NOTE: chunk collocation is implicitly determined
--       by database membership within a family

DROP TABLE IF EXISTS `config_database_family` ;

CREATE TABLE IF NOT EXISTS `config_database_family` (

  `name`                   VARCHAR(255)  NOT NULL ,
  `min_replication_level`  INT UNSIGNED  NOT NULL ,    -- minimum number of replicas per chunk
  `num_stripes`            INT UNSIGNED  NOT NULL ,
  `num_sub_stripes`        INT UNSIGNED  NOT NULL ,

  PRIMARY KEY (`name`)
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `config_database`
-- -----------------------------------------------------
--
-- Databases which are managd by the replication system
--
-- NOTE: Each database belongs to exctly one family, even
--       if that family has the only members (that database).

DROP TABLE IF EXISTS `config_database` ;

CREATE TABLE IF NOT EXISTS `config_database` (

  `database`     VARCHAR(255)  NOT NULL ,
  `family_name`  VARCHAR(255)  NOT NULL ,

  -- Each database is allowed to belong to one family only
  --
  PRIMARY KEY (`database`) ,
  UNIQUE  KEY (`database`,`family_name`) ,

  CONSTRAINT `config_database_fk_1`
    FOREIGN KEY (`family_name` )
    REFERENCES `config_database_family` (`name` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `config_database_table`
-- -----------------------------------------------------
--
-- Database tables

DROP TABLE IF EXISTS `config_database_table` ;

CREATE TABLE IF NOT EXISTS `config_database_table` (

  `database`  VARCHAR(255)  NOT NULL ,
  `table`     VARCHAR(255)  NOT NULL ,

  `is_partitioned` BOOLEAN NOT NULL ,

  PRIMARY KEY (`database`, `table`) ,

  CONSTRAINT `config_database_table_fk_1`
    FOREIGN KEY (`database` )
    REFERENCES `config_database` (`database` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `controller`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `controller` ;

CREATE TABLE IF NOT EXISTS `controller` (

  `id`  VARCHAR(255) NOT NULL ,

  `hostname`  VARCHAR(255) NOT NULL ,
  `pid`       INT          NOT NULL ,

  `start_time`  BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY (`id`)
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `job` ;

CREATE TABLE IF NOT EXISTS `job` (

  `id`             VARCHAR(255) NOT NULL ,
  `controller_id`  VARCHAR(255) NOT NULL ,  -- all jobs must be associated with a controller
  `parent_job_id`  VARCHAR(255)     NULL ,  -- for jobs formming a tree

  `type`       VARCHAR(255) NOT NULL ,
  `state`      VARCHAR(255) NOT NULL ,
  `ext_state`  VARCHAR(255) DEFAULT '' ,

  `begin_time`     BIGINT UNSIGNED NOT NULL ,
  `end_time`       BIGINT UNSIGNED NOT NULL ,
  `heartbeat_time` BIGINT UNSIGNED NOT NULL ,

  -- Job options

  `priority`    INT     NOT NULL ,
  `exclusive`   BOOLEAN NOT NULL ,
  `preemptable` BOOLEAN NOT NULL ,

  PRIMARY KEY (`id`) ,

  CONSTRAINT `job_fk_1`
    FOREIGN KEY (`controller_id`)
    REFERENCES `controller` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE ,

  CONSTRAINT `job_fk_2`
    FOREIGN KEY (`parent_job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_ext`
-- -----------------------------------------------------
--
-- Extended parameters of the jobs
--
DROP TABLE IF EXISTS `job_ext` ;

CREATE TABLE IF NOT EXISTS `job_ext` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `param` VARCHAR(255) NOT NULL ,
  `value` LONGBLOB     NOT NULL ,

  KEY (`job_id`) ,
  KEY (`job_id`,`param`) ,

  CONSTRAINT `job_ext_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `request` ;

CREATE TABLE IF NOT EXISTS `request` (

  `id`      VARCHAR(255) NOT NULL ,
  `job_id`  VARCHAR(255) NOT NULL ,

  `name`     VARCHAR(255) NOT NULL ,
  `worker`   VARCHAR(255) NOT NULL ,
  `priority` INT          DEFAULT 0 ,

  `state`          VARCHAR(255) NOT NULL ,
  `ext_state`      VARCHAR(255) DEFAULT '' ,
  `server_status`  VARCHAR(255) DEFAULT '' ,

  `c_create_time`   BIGINT UNSIGNED NOT NULL ,
  `c_start_time`    BIGINT UNSIGNED NOT NULL ,
  `w_receive_time`  BIGINT UNSIGNED NOT NULL ,
  `w_start_time`    BIGINT UNSIGNED NOT NULL ,
  `w_finish_time`   BIGINT UNSIGNED NOT NULL ,
  `c_finish_time`   BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY (`id`) ,

  CONSTRAINT `request_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request_ext`
-- -----------------------------------------------------
--
-- Extended parameters of the requests
--
DROP TABLE IF EXISTS `request_ext` ;

CREATE TABLE IF NOT EXISTS `request_ext` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `param` VARCHAR(255) NOT NULL ,
  `value` LONGBLOB     NOT NULL ,

  KEY (`request_id`) ,
  KEY (`request_id`,`param`) ,

  CONSTRAINT `request_ext_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replica`
-- -----------------------------------------------------
--
-- ATTENTION: replicas have dependencies onto worker and database
--            entries of the Configuration. Removing workers or
--            databases from the correspondig configuration tables
--            will also eliminate replicas.
--
DROP TABLE IF EXISTS `replica` ;

CREATE TABLE IF NOT EXISTS `replica` (

  `id`  INT NOT NULL AUTO_INCREMENT ,

  `worker`   VARCHAR(255) NOT NULL ,
  `database` VARCHAR(255) NOT NULL ,
  `chunk`    INT UNSIGNED NOT NULL ,

  `verify_time` BIGINT UNSIGNED NOT NULL ,

  PRIMARY KEY (`id`) ,
  KEY         (`worker`,`database`) ,
  UNIQUE  KEY (`worker`,`database`,`chunk`) ,

  CONSTRAINT `replica_fk_1`
    FOREIGN KEY (`worker` )
    REFERENCES `config_worker` (`name` )
    ON DELETE CASCADE
    ON UPDATE CASCADE ,

  CONSTRAINT `replica_fk_2`
    FOREIGN KEY (`database` )
    REFERENCES `config_database` (`database` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `replica_file`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `replica_file` ;

CREATE TABLE IF NOT EXISTS `replica_file` (

  `replica_id`  INT NOT NULL ,

  `name`  VARCHAR(255)    NOT NULL ,
  `size`  BIGINT UNSIGNED NOT NULL ,
  `mtime` INT    UNSIGNED NOT NULL,
  `cs`    VARCHAR(255)    NOT NULL ,

  `begin_create_time`  BIGINT UNSIGNED NOT NULL ,
  `end_create_time`    BIGINT UNSIGNED NOT NULL ,

  PRIMARY  KEY (`replica_id`,`name`) ,

  CONSTRAINT `replica_file_fk_1`
    FOREIGN KEY (`replica_id` )
    REFERENCES `replica` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `controller_log`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `controller_log` ;

CREATE TABLE IF NOT EXISTS `controller_log` (

  `id`            INT             NOT NULL AUTO_INCREMENT ,
  `controller_id` VARCHAR(255)    NOT NULL ,
  `time`          BIGINT UNSIGNED NOT NULL ,  -- 64-bit timestamp: seconds and nanoseconds when
                                              -- an event was posted

  `task`          VARCHAR(255)    NOT NULL ,  -- the name of a task which runs within Controllers
  `operation`     VARCHAR(255)    NOT NULL ,  -- the name of a request, a jobs or some other action launched
                                              -- in a scope of the corresponding task
  `status`        VARCHAR(255)    NOT NULL ,  -- status of the operation (STARTED, COMPLETED, CANCELLED,
                                              -- FAILED, etc.). Can be an empty string.

  `request_id`    VARCHAR(255) DEFAULT NULL ,  -- (optional) depends on an operation
  `job_id`        VARCHAR(255) DEFAULT NULL ,  -- (optional) depends on an operation

  PRIMARy KEY (`id`) ,

  CONSTRAINT `controller_log_fk_1`
    FOREIGN KEY (`controller_id` )
    REFERENCES `controller` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE ,

  CONSTRAINT `controller_log_fk_2`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE ,

  CONSTRAINT `controller_log_fk_3`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `controller_log_ext`
-- -----------------------------------------------------
--
-- This table is for adding extra notes on the logged events.
--
DROP TABLE IF EXISTS `controller_log_ext` ;

CREATE TABLE IF NOT EXISTS `controller_log_ext` (

  `controller_log_id`  INT NOT NULL ,

  `key` VARCHAR(255) NOT NULL ,
  `val` TEXT         NOT NULL ,

  CONSTRAINT `controller_log_ext_fk_1`
    FOREIGN KEY (`controller_log_id` )
    REFERENCES `controller_log` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;
