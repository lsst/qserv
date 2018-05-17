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

  `type` ENUM ('FIXUP',
               'FIND_ALL',
               'REPLICATE',
               'PURGE',
               'REBALANCE',
               'VERIFY',
               'DELETE_WORKER',
               'ADD_WORKER',
               'MOVE_REPLICA',
               'CREATE_REPLICA',
               'DELETE_REPLICA',
               'QSERV_SYNC',
               'QSERV_GET_REPLICAS') NOT NULL ,

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
-- Table `job_fixup`
-- -----------------------------------------------------
--
-- Extended parameters of the 'FIXUP' jobs
--
DROP TABLE IF EXISTS `job_fixup` ;

CREATE TABLE IF NOT EXISTS `job_fixup` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_fixup_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `job_find_all`
-- -----------------------------------------------------
--
-- Extended parameters of the 'FIND_ALL' jobs
--
DROP TABLE IF EXISTS `job_find_all` ;

CREATE TABLE IF NOT EXISTS `job_find_all` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_find_all_fk_1`
    FOREIGN KEY (`job_id`)
    REFERENCES `job` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_replicate`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICATE' jobs
--
DROP TABLE IF EXISTS `job_replicate` ;

CREATE TABLE IF NOT EXISTS `job_replicate` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,
  `num_replicas`     INT          NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_replicate_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_purge`
-- -----------------------------------------------------
--
-- Extended parameters of the 'PURGE' jobs
--
DROP TABLE IF EXISTS `job_purge` ;

CREATE TABLE IF NOT EXISTS `job_purge` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,
  `num_replicas`     INT          NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_purge_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_rebalance`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REBALANCE' jobs
--
DROP TABLE IF EXISTS `job_rebalance` ;

CREATE TABLE IF NOT EXISTS `job_rebalance` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_rebalance_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_verify`
-- -----------------------------------------------------
--
-- Extended parameters of the 'VERIFY' jobs
--
DROP TABLE IF EXISTS `job_verify` ;

CREATE TABLE IF NOT EXISTS `job_verify` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `max_replicas`  INT     NOT NULL ,
  `compute_cs`    BOOLEAN NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_verify_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_delete_worker`
-- -----------------------------------------------------
--
-- Extended parameters of the 'DELETE_WORKER' jobs
--
DROP TABLE IF EXISTS `job_delete_worker` ;

CREATE TABLE IF NOT EXISTS `job_delete_worker` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `worker`    VARCHAR(255) NOT NULL ,
  `permanent` BOOLEAN      NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_delete_worker_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_add_worker`
-- -----------------------------------------------------
--
-- Extended parameters of the 'ADD_WORKER' jobs
--
DROP TABLE IF EXISTS `job_add_worker` ;

CREATE TABLE IF NOT EXISTS `job_add_worker` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `worker` VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_add_worker_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_move_replica`
-- -----------------------------------------------------
--
-- Extended parameters of the 'MOVE_REPLICA' jobs
--
DROP TABLE IF EXISTS `job_move_replica` ;

CREATE TABLE IF NOT EXISTS `job_move_replica` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`    VARCHAR(255) NOT NULL ,
  `chunk`              INT UNSIGNED NOT NULL ,
  `source_worker`      VARCHAR(255) NOT NULL ,
  `destination_worker` VARCHAR(255) NOT NULL ,
  `purge`              BOOLEAN      NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_move_replica_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `job_create_replica`
-- -----------------------------------------------------
--
-- Extended parameters of the 'CREATE_REPLICA' jobs
--
DROP TABLE IF EXISTS `job_create_replica` ;

CREATE TABLE IF NOT EXISTS `job_create_replica` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family`    VARCHAR(255) NOT NULL ,
  `chunk`              INT UNSIGNED NOT NULL ,
  `source_worker`      VARCHAR(255) NOT NULL ,
  `destination_worker` VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_create_replica_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `job_delete_replica`
-- -----------------------------------------------------
--
-- Extended parameters of the 'DELETE_REPLICA' jobs
--
DROP TABLE IF EXISTS `job_delete_replica` ;

CREATE TABLE IF NOT EXISTS `job_delete_replica` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family` VARCHAR(255) NOT NULL ,
  `chunk`           INT UNSIGNED NOT NULL ,
  `worker`          VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_delete_replica_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `job_qserv_sync`
-- -----------------------------------------------------
--
-- Extended parameters of the 'QSERV_SYNC' jobs
--
DROP TABLE IF EXISTS `job_qserv_sync` ;

CREATE TABLE IF NOT EXISTS `job_qserv_sync` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family` VARCHAR(255) NOT NULL ,
  `force`           BOOLEAN      NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_qserv_sync_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `job_qserv_get_replicas`
-- -----------------------------------------------------
--
-- Extended parameters of the 'QSERV_GET_REPLICAS' jobs
--
DROP TABLE IF EXISTS `job_qserv_get_replicas` ;

CREATE TABLE IF NOT EXISTS `job_qserv_get_replicas` (

  `job_id`  VARCHAR(255) NOT NULL ,

  `database_family` VARCHAR(255) NOT NULL ,
  `in_use_only`     BOOLEAN      NOT NULL ,

  PRIMARY KEY (`job_id`) ,

  CONSTRAINT `job_qserv_get_replicas_fk_1`
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

  `name` ENUM ('REPLICA_CREATE',
               'REPLICA_DELETE',
               'REPLICA_FIND',
               'REPLICA_FIND_ALL',
               'QSERV_ADD_REPLICA',
               'QSERV_REMOVE_REPLICA',
               'QSERV_GET_REPLICAS',
               'QSERV_SET_REPLICAS') NOT NULL ,

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
-- Table `request_replica_create`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICA_CREATE' requests
--
DROP TABLE IF EXISTS `request_replica_create` ;

CREATE TABLE IF NOT EXISTS `request_replica_create` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `database` VARCHAR(255) NOT NULL ,
  `chunk`    INT UNSIGNED NOT NULL ,

  `source_worker`  VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`request_id`) ,

  CONSTRAINT `request_replica_create_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request_replica_delete`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICA_DELETE' requests
--
DROP TABLE IF EXISTS `request_replica_delete` ;

CREATE TABLE IF NOT EXISTS `request_replica_delete` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `database` VARCHAR(255) NOT NULL ,
  `chunk`    INT UNSIGNED NOT NULL ,

  PRIMARY KEY (`request_id`) ,

  CONSTRAINT `request_replica_delete_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request_replica_find`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICA_FIND' requests
--
DROP TABLE IF EXISTS `request_replica_find` ;

CREATE TABLE IF NOT EXISTS `request_replica_find` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `database` VARCHAR(255) NOT NULL ,
  `chunk`    INT UNSIGNED NOT NULL ,

  PRIMARY KEY (`request_id`) ,

  CONSTRAINT `request_replica_find_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request_replica_find_all`
-- -----------------------------------------------------
--
-- Extended parameters of the 'REPLICA_FIND_ALL' requests
--
DROP TABLE IF EXISTS `request_replica_find_all` ;

CREATE TABLE IF NOT EXISTS `request_replica_find_all` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `database` VARCHAR(255) NOT NULL ,

  PRIMARY KEY (`request_id`) ,

  CONSTRAINT `request_replica_find_all_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request_qserv_add_replica`
-- -----------------------------------------------------
--
-- Extended parameters of the 'QSERV_ADD_REPLICA' requests
--
DROP TABLE IF EXISTS `request_qserv_add_replica` ;

CREATE TABLE IF NOT EXISTS `request_qserv_add_replica` (

  `request_id`  VARCHAR(255) NOT NULL ,

  `databases`   LONGTEXT     NOT NULL ,
  `chunk`       INT UNSIGNED NOT NULL ,

  PRIMARY KEY (`request_id`) ,

  CONSTRAINT `request_qserv_add_replica_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `request_qserv_remove_replica`
-- -----------------------------------------------------
--
-- Extended parameters of the 'QSERV_REMOVE_REPLICA' requests
--
DROP TABLE IF EXISTS `request_qserv_remove_replica` ;

CREATE TABLE IF NOT EXISTS `request_qserv_remove_replica` (

  `request_id`  VARCHAR(255) NOT NULL ,

  --
  -- comma separated sequence of database names
  --
  --   '<database1>,<database2>, ...'
  --
  `databases`   LONGTEXT     NOT NULL ,
  `chunk`       INT UNSIGNED NOT NULL ,
  `force`       BOOLEAN      NOT NULL ,

  PRIMARY KEY (`request_id`) ,

  CONSTRAINT `request_qserv_remove_replica_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `request_qserv_get_replicas`
-- -----------------------------------------------------
--
-- Extended parameters of the 'QSERV_GET_REPLICAS' requests
--
DROP TABLE IF EXISTS `request_qserv_get_replicas` ;

CREATE TABLE IF NOT EXISTS `request_qserv_get_replicas` (

  `request_id`   VARCHAR(255) NOT NULL ,

  `database_family`  VARCHAR(255) NOT NULL ,
  `in_use_only`      BOOLEAN      NOT NULL ,

  PRIMARY KEY (`request_id`) ,

  CONSTRAINT `request_qserv_get_replicas_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `request_qserv_set_replicas`
-- -----------------------------------------------------
--
-- Extended parameters of the 'QSERV_SET_REPLICAS' requests
--
DROP TABLE IF EXISTS `request_qserv_set_replicas` ;

CREATE TABLE IF NOT EXISTS `request_qserv_set_replicas` (

  `request_id`  VARCHAR(255) NOT NULL ,

  --
  -- comma separated sequence of pairs representing databases
  -- and chunks:
  --
  --   '<database1>:<chunk1>,<database2>:<chunk2>, ...'
  --
  `replicas` LONGTEXT NOT NULL ,
  `force`    BOOLEAN  NOT NULL ,

  PRIMARY KEY (`request_id`) ,

  CONSTRAINT `request_qserv_set_replicas_fk_1`
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


SET SQL_MODE=@OLD_SQL_MODE ;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS ;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS ;
