CREATE TABLE IF NOT EXISTS `config` (
  `category` VARCHAR(255) NOT NULL ,
  `param`    VARCHAR(255) NOT NULL ,
  `value`    VARCHAR(255) NOT NULL ,
  PRIMARY KEY (`category`,`param`)
)
ENGINE = InnoDB
COMMENT = 'The common parameters and defaults shared by all components
 of the replication system. It also provides default values
 for some critical parameters of the worker-side services';


CREATE TABLE IF NOT EXISTS `config_worker` (
  `name`            VARCHAR(255)       NOT NULL ,     -- the name of the worker
  `is_enabled`      BOOLEAN            NOT NULL ,     -- is enabled for replication
  `is_read_only`    BOOLEAN            NOT NULL ,     -- a subclass of 'is_enabled' which restricts use of
                                                      -- the worker for reading replicas. No new replicas can't be
                                                      -- placed onto this class of workers.
  `svc_host`        VARCHAR(255)       NOT NULL ,     -- the host name on which the worker server runs
  `svc_port`        SMALLINT UNSIGNED  DEFAULT NULL , -- override for the global default
  `fs_host`         VARCHAR(255)       DEFAULT NULL,  -- the host name on which the built-in FileServer runs
  `fs_port`         SMALLINT UNSIGNED  DEFAULT NULL , -- override for the global default
  `data_dir`        VARCHAR(255)       DEFAULT NULL , -- a file system path to the databases

  -- Ingest service
  `loader_host`     VARCHAR(255)       DEFAULT NULL,  -- the host name on which the worker's ingest server runs
  `loader_port`     SMALLINT UNSIGNED  DEFAULT NULL , -- override for the global default
  `loader_tmp_dir`  VARCHAR(255)       DEFAULT NULL , -- a file system path to the temporary folder

  -- Data exporting service
  `exporter_host`     VARCHAR(255)        DEFAULT NULL,   -- the host name on which the worker's data exporting server runs
  `exporter_port`     SMALLINT UNSIGNED   DEFAULT NULL ,  -- override for the global default
  `exporter_tmp_dir`  VARCHAR(255)        DEFAULT NULL ,  -- a file system path to the temporary folder

  -- HTTP-based ingest service
  `http_loader_host`    VARCHAR(255)       DEFAULT NULL,     -- the host name on which the worker's HTTP-based ingest server runs
  `http_loader_port`    SMALLINT UNSIGNED  DEFAULT NULL ,    -- override for the global default
  `http_loader_tmp_dir` VARCHAR(255)       DEFAULT NULL ,    -- a file system path to the temporary folder

  PRIMARY KEY (`name`) ,
  UNIQUE  KEY (`svc_host`, `svc_port`) ,
  UNIQUE  KEY (`fs_host`,  `fs_port`) ,
  UNIQUE  KEY (`loader_host`, `loader_port`),
  UNIQUE  KEY (`exporter_host`, `exporter_port`),
  UNIQUE  KEY (`http_loader_host`, `http_loader_port`)
)
ENGINE = InnoDB
COMMENT = 'Worker-specific configuration parameters and overrides
 of the corresponidng default values if needed';


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
ENGINE = InnoDB
COMMENT = 'The additional parameters overriding the defaults for individual
 worker services';


CREATE TABLE IF NOT EXISTS `config_database_family` (
  `name`                   VARCHAR(255)  NOT NULL ,
  `min_replication_level`  INT UNSIGNED  NOT NULL ,    -- minimum number of replicas per chunk
  `num_stripes`            INT UNSIGNED  NOT NULL ,
  `num_sub_stripes`        INT UNSIGNED  NOT NULL ,
  `overlap`                DOUBLE        NOT NULL ,
  PRIMARY KEY (`name`)
)
ENGINE = InnoDB
COMMENT = 'Groups of databases which require coordinated replication efforts';


CREATE TABLE IF NOT EXISTS `config_database` (
  `database`          VARCHAR(255)  NOT NULL ,
  `family_name`       VARCHAR(255)  NOT NULL ,
  `is_published`      BOOLEAN DEFAULT TRUE ,
  `chunk_id_key`      VARCHAR(255)  DEFAULT "" ,
  `sub_chunk_id_key`  VARCHAR(255)  DEFAULT "" ,
  PRIMARY KEY (`database`) ,
  UNIQUE  KEY (`database`,`family_name`) ,  -- a database is allowed to belong to one family only
  CONSTRAINT `config_database_fk_1`
    FOREIGN KEY (`family_name` )
    REFERENCES `config_database_family` (`name` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Databases managed by the replication system';


CREATE TABLE IF NOT EXISTS `config_database_table` (
  `database`        VARCHAR(255)  NOT NULL ,
  `table`           VARCHAR(255)  NOT NULL ,
  `is_partitioned`  BOOLEAN NOT NULL ,
  `is_director`     BOOLEAN NOT NULL ,
  `director_key`    VARCHAR(255) DEFAULT "" ,
  `latitude_key`    VARCHAR(255) DEFAULT "" , -- The name for latitude (declination) column in this table
  `longitude_key`   VARCHAR(255) DEFAULT "" , -- The name for longitude (right ascension) column in this table
  PRIMARY KEY (`database`, `table`) ,
  CONSTRAINT `config_database_table_fk_1`
    FOREIGN KEY (`database` )
    REFERENCES `config_database` (`database` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Database tables managed by the replication system';


CREATE TABLE IF NOT EXISTS `config_database_table_schema` (
  `database`      VARCHAR(255)  NOT NULL ,
  `table`         VARCHAR(255)  NOT NULL ,
  `col_position`  INT NOT NULL ,              -- for preserving an order of columns in the table
  `col_name`      VARCHAR(255)  NOT NULL ,
  `col_type`      VARCHAR(255)  NOT NULL ,
  UNIQUE KEY (`database`, `table`, `col_position`) ,
  UNIQUE KEY (`database`, `table`, `col_name`) ,
  CONSTRAINT `config_database_table_schema_fk_1`
    FOREIGN KEY (`database`, `table`)
    REFERENCES `config_database_table` (`database`, `table`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Table schemas for the managed tables. The schemas are registered during
  catalog ingests, and they are used by the Ingest system.';


CREATE TABLE IF NOT EXISTS `controller` (
  `id`          VARCHAR(255)    NOT NULL ,
  `hostname`    VARCHAR(255)    NOT NULL ,
  `pid`         INT             NOT NULL ,
  `start_time`  BIGINT UNSIGNED NOT NULL ,
  PRIMARY KEY (`id`)
)
ENGINE = InnoDB
COMMENT = 'A Registry of the Controllers. Normally there is just one active controller.
 The active controller has the newest start time.';


CREATE TABLE IF NOT EXISTS `job` (
  `id`              VARCHAR(255)    NOT NULL ,
  `controller_id`   VARCHAR(255)    NOT NULL ,  -- all jobs must be associated with a controller
  `parent_job_id`   VARCHAR(255)        NULL,   -- for jobs formming a tree
  `type`            VARCHAR(255)    NOT NULL ,
  `state`           VARCHAR(255)    NOT NULL ,
  `ext_state`       VARCHAR(255)    DEFAULT '' ,
  `begin_time`      BIGINT UNSIGNED NOT NULL ,
  `end_time`        BIGINT UNSIGNED NOT NULL ,
  `heartbeat_time`  BIGINT UNSIGNED NOT NULL ,

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
ENGINE = InnoDB
COMMENT = 'The persisteht state of the "jobs" - the high-level algorithms implementiong
 various operations witin the Replication/Ingest system.';


CREATE TABLE IF NOT EXISTS `job_ext` (
  `job_id`  VARCHAR(255) NOT NULL ,
  `param`   VARCHAR(255) NOT NULL ,
  `value`   LONGBLOB     NOT NULL ,
  KEY (`job_id`) ,
  KEY (`job_id`,`param`) ,
  CONSTRAINT `job_ext_fk_1`
    FOREIGN KEY (`job_id` )
    REFERENCES `job` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Extended parameters of the jobs';


CREATE TABLE IF NOT EXISTS `request` (
  `id`              VARCHAR(255)    NOT NULL ,
  `job_id`          VARCHAR(255)    NOT NULL ,
  `name`            VARCHAR(255)    NOT NULL ,
  `worker`          VARCHAR(255)    NOT NULL ,
  `priority`        INT             DEFAULT 0 ,
  `state`           VARCHAR(255)    NOT NULL ,
  `ext_state`       VARCHAR(255)    DEFAULT '' ,
  `server_status`   VARCHAR(255)    DEFAULT '' ,
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
ENGINE = InnoDB
COMMENT = 'Persistent state of the low-level requests sent to workers';


CREATE TABLE IF NOT EXISTS `request_ext` (
  `request_id`  VARCHAR(255) NOT NULL ,
  `param`       VARCHAR(255) NOT NULL ,
  `value`       LONGBLOB     NOT NULL ,
  KEY (`request_id`) ,
  KEY (`request_id`,`param`) ,
  CONSTRAINT `request_ext_fk_1`
    FOREIGN KEY (`request_id` )
    REFERENCES `request` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Extended parameters of the requests';


CREATE TABLE IF NOT EXISTS `replica` (
  `id`          INT             NOT NULL AUTO_INCREMENT ,
  `worker`      VARCHAR(255)    NOT NULL ,
  `database`    VARCHAR(255)    NOT NULL ,
  `chunk`       INT UNSIGNED    NOT NULL ,
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
ENGINE = InnoDB
COMMENT = 'Chunk replicas';


CREATE TABLE IF NOT EXISTS `replica_file` (
  `replica_id`        INT             NOT NULL ,
  `name`              VARCHAR(255)    NOT NULL ,
  `size`              BIGINT UNSIGNED NOT NULL ,
  `mtime`             INT    UNSIGNED NOT NULL,
  `cs`                VARCHAR(255)    NOT NULL ,
  `begin_create_time` BIGINT UNSIGNED NOT NULL ,
  `end_create_time`   BIGINT UNSIGNED NOT NULL ,
  PRIMARY  KEY (`replica_id`,`name`) ,
  CONSTRAINT `replica_file_fk_1`
    FOREIGN KEY (`replica_id` )
    REFERENCES `replica` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Collections of files for the corresponidng replicas';


CREATE TABLE IF NOT EXISTS `controller_log` (
  `id`            INT             NOT NULL AUTO_INCREMENT ,
  `controller_id` VARCHAR(255)    NOT NULL ,
  `time`          BIGINT UNSIGNED NOT NULL ,      -- 64-bit timestamp: seconds and nanoseconds when
                                                  -- an event was posted
  `task`          VARCHAR(255)    NOT NULL ,      -- the name of a task which runs within Controllers
  `operation`     VARCHAR(255)    NOT NULL ,      -- the name of a request, a jobs or some other action launched
                                                  -- in a scope of the corresponding task
  `status`        VARCHAR(255)    NOT NULL ,      -- status of the operation (STARTED, COMPLETED, CANCELLED,
                                                  -- FAILED, etc.). Can be an empty string.
  `request_id`    VARCHAR(255)    DEFAULT NULL ,  -- (optional) depends on an operation
  `job_id`        VARCHAR(255)    DEFAULT NULL ,  -- (optional) depends on an operation
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
ENGINE = InnoDB
COMMENT = 'The persistent log of events recorded by controllers';


CREATE TABLE IF NOT EXISTS `controller_log_ext` (
  `controller_log_id` INT           NOT NULL ,
  `key`               VARCHAR(255)  NOT NULL ,
  `val`               TEXT          NOT NULL ,
  CONSTRAINT `controller_log_ext_fk_1`
    FOREIGN KEY (`controller_log_id` )
    REFERENCES `controller_log` (`id` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Extended attributes for the corresponding events logged by controllers';


CREATE TABLE IF NOT EXISTS `transaction` (
  `id`          INT UNSIGNED    NOT NULL AUTO_INCREMENT ,
  `database`    VARCHAR(255)    NOT NULL ,
  `state`       VARCHAR(255)    NOT NULL ,
  `begin_time`  BIGINT UNSIGNED NOT NULL ,
  `end_time`    BIGINT UNSIGNED DEFAULT 0 ,
  `context`     MEDIUMBLOB      DEFAULT '' ,
  UNIQUE KEY (`id`) ,
  PRIMARY KEY (`id`,`database`) ,
  CONSTRAINT `transaction_fk_1`
    FOREIGN KEY (`database` )
    REFERENCES `config_database` (`database`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'The super-transactions created by the ingest system';


CREATE TABLE IF NOT EXISTS `transaction_contrib` (
  `id`              INT UNSIGNED    NOT NULL AUTO_INCREMENT,  -- a unique identifier of the contribution
  `transaction_id`  INT UNSIGNED    NOT NULL ,                -- FK to the parent transaction
  `worker`          VARCHAR(255)    NOT NULL ,
  `database`        VARCHAR(255)    NOT NULL ,
  `table`           VARCHAR(255)    NOT NULL ,
  `chunk`           INT UNSIGNED    NOT NULL ,
  `is_overlap`      BOOLEAN         NOT NULL ,
  `url`             TEXT            NOT NULL ,
  `begin_time`      BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `end_time`        BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `num_bytes`       BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `num_rows`        BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `success`         BOOLEAN         NOT NULL DEFAULT 0 ,
  PRIMARY KEY (`id`) ,
  CONSTRAINT `transaction_contrib_fk_1`
    FOREIGN KEY (`transaction_id`)
    REFERENCES `transaction` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `transaction_contrib_fk_2`
    FOREIGN KEY (`worker` )
    REFERENCES `config_worker` (`name`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `transaction_contrib_fk_3`
    FOREIGN KEY (`database`, `table`)
    REFERENCES `config_database_table` (`database`, `table`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Table/chunk contributions made in a context of the super-transactions during ingests';


CREATE TABLE IF NOT EXISTS `database_ingest` (
  `database` VARCHAR(255) NOT NULL ,
  `category` VARCHAR(255) NOT NULL ,
  `param`    VARCHAR(255) NOT NULL ,
  `value`    TEXT         NOT NULL ,
  PRIMARY KEY (`database`, `category`, `param`) ,
  CONSTRAINT `database_ingest_fk_1`
    FOREIGN KEY (`database` )
    REFERENCES `config_database` (`database` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Parameters and a state of the catalog ingests';


CREATE TABLE IF NOT EXISTS `QMetadata` (
  `metakey` CHAR(64) NOT NULL COMMENT 'Key string' ,
  `value`   TEXT         NULL COMMENT 'Value string' ,
  PRIMARY KEY (`metakey`)
)
ENGINE = InnoDB
COMMENT = 'Metadata about database as a whole, key-value pairs' ;

-- Add record for schema version, migration script expects this record to exist

INSERT INTO `QMetadata` (`metakey`, `value`) VALUES ('version', '1');
