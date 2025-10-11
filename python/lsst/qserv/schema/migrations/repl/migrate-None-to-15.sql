-- This schema file is meant to be used for initializing the replication system's
-- database with the latest version of the schema. An alternative approach would
-- be to use the schema migration tool 'smig'. The 'smig' support has been added
-- to this package as well. See the version specific files for further details.

CREATE TABLE IF NOT EXISTS `config_worker` (
  `name`            VARCHAR(255)       NOT NULL ,     -- the name of the worker
  `is_enabled`      BOOLEAN            NOT NULL ,     -- is enabled for replication
  `is_read_only`    BOOLEAN            NOT NULL ,     -- a subclass of 'is_enabled' which restricts use of
                                                      -- the worker for reading replicas. No new replicas can't be
                                                      -- placed onto this class of workers.
  PRIMARY KEY (`name`)
)
ENGINE = InnoDB
COMMENT = 'Worker-specific configuration parameters';


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
  `create_time`       BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `publish_time`      BIGINT UNSIGNED NOT NULL DEFAULT (UNIX_TIMESTAMP() * 1000) ,  -- the current time in milliseconds since EPOCH
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
  `director_table`  VARCHAR(255) DEFAULT "" , -- The name of the corresponding 'director' table (if any)
                                              -- or the first referenced 'director' table (if the current table is RefMatch)
  `director_key`    VARCHAR(255) DEFAULT "" , -- The name of the table's FK key linking to the PK of the `director` table (if any)
                                              -- or the first referenced 'director' table's FK key (if the current table is RefMatch)
  `director_table2` VARCHAR(255) DEFAULT "" , -- The name of the second referenced 'director' table (if the current table is RefMatch)
  `director_key2`   VARCHAR(255) DEFAULT "" , -- The name of the second referenced 'director' table's FK key (if the current table is RefMatch)
  `flag`            VARCHAR(255) DEFAULT "" , -- The name of a column for flags (if the current table is RefMatch)
  `ang_sep`         DOUBLE       DEFAULT 0 ,  -- The angular separation parameter (if the current table is RefMatch)
  `unique_primary_key` BOOLEAN NOT NULL DEFAULT TRUE ,
  `charset_name`    VARCHAR(255) DEFAULT "" , -- The name of the character set for the table (optional)
  `collation_name`  VARCHAR(255) DEFAULT "" , -- The name of the collation for the table (optional)
  `latitude_key`    VARCHAR(255) DEFAULT "" , -- The name for latitude (declination) column in this table
  `longitude_key`   VARCHAR(255) DEFAULT "" , -- The name for longitude (right ascension) column in this table
  `is_published`    BOOLEAN DEFAULT TRUE ,
  `create_time`     BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `publish_time`    BIGINT UNSIGNED NOT NULL DEFAULT (UNIX_TIMESTAMP() * 1000) ,  -- the current time in milliseconds since EPOCH
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
  `priority`        INT             NOT NULL ,  -- the priority level of the job

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
  `start_time`  BIGINT UNSIGNED DEFAULT 0 ,
  `transition_time`  BIGINT UNSIGNED DEFAULT 0 ,
  `end_time`    BIGINT UNSIGNED DEFAULT 0 ,
  `context`     MEDIUMBLOB      DEFAULT '' ,
  UNIQUE KEY (`id`) ,
  PRIMARY KEY (`id`,`database`) ,
  KEY (`state`) ,
  CONSTRAINT `transaction_fk_1`
    FOREIGN KEY (`database` )
    REFERENCES `config_database` (`database`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'The super-transactions created by the ingest system';

CREATE TABLE IF NOT EXISTS `transaction_log` (
  `id`                 INT UNSIGNED    NOT NULL AUTO_INCREMENT,   -- a unique identifier of the event
  `transaction_id`     INT UNSIGNED    NOT NULL ,                 -- FK to the parent transaction
  `transaction_state`  VARCHAR(255)    NOT NULL ,                 -- a state of the transaction tion when the event was recorded
  `time`               BIGINT UNSIGNED NOT NULL ,                 -- an timestamp (milliseconds) when the event was recorded
  `name`               VARCHAR(255)    NOT NULL ,                 -- an identifier of the event
  `data`               MEDIUMBLOB      DEFAULT '' ,               -- optional parameters (JSON object) of the event
  PRIMARY KEY (`id`) ,
  CONSTRAINT `transaction_log_fk_1`
    FOREIGN KEY (`transaction_id`)
    REFERENCES `transaction` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Events logged in a context of the super-transactions. The information is meant
  for progress tracking, monitoring and performance analysis of the transactions.';

CREATE TABLE IF NOT EXISTS `transaction_contrib` (
  `id`              INT UNSIGNED  NOT NULL AUTO_INCREMENT,  -- a unique identifier of the contribution
  `transaction_id`  INT UNSIGNED  NOT NULL ,                -- FK to the parent transaction
  `worker`          VARCHAR(255)  NOT NULL ,
  `database`        VARCHAR(255)  NOT NULL ,
  `table`           VARCHAR(255)  NOT NULL ,
  `chunk`           INT UNSIGNED  NOT NULL ,
  `is_overlap`      BOOLEAN       NOT NULL ,
  `url`             TEXT          NOT NULL ,

  `type`  ENUM ('SYNC', 'ASYNC') NOT NULL DEFAULT 'SYNC' ,

  `max_retries`        INT UNSIGNED NOT NULL DEFAULT 0 ,  -- the number of retries allowed on failed input reads
  `num_failed_retries` INT UNSIGNED NOT NULL DEFAULT 0 ,  -- the actuall number of failed retries. The number is stored here
                                                          -- to optimize summary reports on the contributions without pulling
                                                          -- the detailed info on the retries from the related table
                                                          -- `transaction_contrib_retry`.
  `num_bytes`   BIGINT UNSIGNED   NOT NULL DEFAULT 0 ,
  `num_rows`    BIGINT UNSIGNED   NOT NULL DEFAULT 0 ,
  `create_time` BIGINT UNSIGNED   NOT NULL DEFAULT 0 ,    -- the time a request was received and (possibly) queued
  `start_time`  BIGINT UNSIGNED   NOT NULL DEFAULT 0 ,    -- the time the request processing started (by reading/loading input data)
  `read_time`   BIGINT UNSIGNED   NOT NULL DEFAULT 0 ,    -- the time the input data file was read and preprocessed
  `load_time`   BIGINT UNSIGNED   NOT NULL DEFAULT 0 ,    -- the time the contiribution was fully loaded

  `status` ENUM ('IN_PROGRESS',   -- the transient state of a request before it's FINISHED or failed
                 'CREATE_FAILED', -- the request was received but rejected right away (incorrect parameters, etc.)
                 'START_FAILED',  -- the request couldn't start after being pulled from a queue due to changed conditions
                 'READ_FAILED',   -- reading/preprocessing of the input file failed
                 'LOAD_FAILED',   -- loading into MySQL failed
                 'CANCELLED',     -- the request was explicitly cancelled by the ingest workflow (ASYNC)
                 'FINISHED'       -- the request succeeded
                ) NOT NULL DEFAULT 'IN_PROGRESS' ,

  `tmp_file` TEXT NOT NULL ,  -- the temporary file open to store preprocessed data

  `num_warnings` INT UNSIGNED NOT NULL DEFAULT 0 ,  -- the total number of MySQL warnings detected after loading the contribution

  `num_rows_loaded` BIGINT UNSIGNED NOT NULL DEFAULT 0 ,  -- the total number of rows affected by the loading operation

  -- Columns for storing the extended info on a problem that prevented a request
  -- from succeeding.
  `http_error`    INT     NOT NULL DEFAULT 0 ,  -- HTTP response code, if applies to a request
  `system_error`  INT     NOT NULL DEFAULT 0 ,  -- the UNIX errno captured at a point where a problem occurred
  `error`         TEXT    NOT NULL DEFAULT '' , -- the human-readable message
  `retry_allowed` BOOLEAN NOT NULL DEFAULT 0 ,  -- a client was informed that the request could be re-tried or not

  PRIMARY KEY (`id`) ,
  KEY (`type`) ,
  KEY (`status`) ,
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

CREATE TABLE IF NOT EXISTS `transaction_contrib_ext` (
  `contrib_id` INT UNSIGNED NOT NULL ,
  `key`        VARCHAR(255) NOT NULL ,
  `val`        TEXT         NOT NULL ,
  CONSTRAINT `transaction_contrib_ext_fk_1`
    FOREIGN KEY (`contrib_id`)
    REFERENCES `transaction_contrib` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Extended attributes for the corresponding transaction contributions';

CREATE TABLE IF NOT EXISTS `transaction_contrib_warn` (
  `contrib_id` INT UNSIGNED NOT NULL ,
  `pos`        INT UNSIGNED NOT NULL ,  -- the column allows preserving the original order of warnings
                                        -- as they were reported by MySQL
  `level`      VARCHAR(32)  NOT NULL ,
  `code`       INT          NOT NULL ,
  `message`    TEXT         NOT NULL ,
  PRIMARY KEY (`contrib_id`, `pos`) ,
  CONSTRAINT `transaction_contrib_warn_fk_1`
    FOREIGN KEY (`contrib_id`)
    REFERENCES `transaction_contrib` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Warnings captured after executing "LOAD DATA [LOCAL] INFILE ..." for
 the corresponding transaction contributions';

CREATE TABLE IF NOT EXISTS `transaction_contrib_retry` (
  `contrib_id` INT UNSIGNED NOT NULL ,

  `num_bytes`  BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `num_rows`   BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `start_time` BIGINT UNSIGNED NOT NULL DEFAULT 0 , -- the time the reading/loading input data started
  `read_time`  BIGINT UNSIGNED NOT NULL DEFAULT 0 , -- the time the operation failed

  `tmp_file` TEXT NOT NULL ,  -- the temporary file open to store preprocessed data

  -- Columns for storing the extended info on a problem that prevented a request
  -- from succeeding.
  `http_error`   INT  NOT NULL DEFAULT 0 ,  -- HTTP response code, if applies to a request
  `system_error` INT  NOT NULL DEFAULT 0 ,  -- the UNIX errno captured at a point where a problem occurred
  `error`        TEXT NOT NULL DEFAULT '' , -- the human-readable message

  CONSTRAINT `transaction_contrib_retry_fk_1`
    FOREIGN KEY (`contrib_id`)
    REFERENCES `transaction_contrib` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Info on the failed retries to pull the input data of the corresponding transaction contributions';

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


CREATE TABLE IF NOT EXISTS `stats_table_rows` (
  `database` VARCHAR(255) NOT NULL ,
  `table`    VARCHAR(255) NOT NULL ,

  -- NULL is used to support tables that don't have transaction identifiers,
  -- which would be a case of catalogs where the transactions were eliminated,
  -- or where it never existed (legacy data).
  `transaction_id` INT UNSIGNED DEFAULT NULL ,

  `chunk`       INT    UNSIGNED NOT NULL ,
  `is_overlap`  BOOLEAN         NOT NULL ,
  `num_rows`    BIGINT UNSIGNED DEFAULT 0 ,
  `update_time` BIGINT UNSIGNED NOT NULL ,

  UNIQUE KEY (`database`, `table`, `transaction_id`, `chunk`, `is_overlap`) ,
  KEY (`database`, `table`, `transaction_id`) ,
  KEY (`database`, `table`) ,
  CONSTRAINT `stats_table_rows_fk_1`
    FOREIGN KEY (`database`, `table`)
    REFERENCES `config_database_table` (`database`, `table`)
    ON DELETE CASCADE
    ON UPDATE CASCADE ,
  CONSTRAINT `stats_table_rows_fk_2`
    FOREIGN KEY (`transaction_id`)
    REFERENCES `transaction` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Row counters for the internal tables. The table is supposed to be populated
  by the ingest system when publishing the catalog, or afterwards by the special
  table scanner.';


CREATE TABLE IF NOT EXISTS `QMetadata` (
  `metakey` CHAR(64) NOT NULL COMMENT 'Key string' ,
  `value`   TEXT         NULL COMMENT 'Value string' ,
  PRIMARY KEY (`metakey`)
)
ENGINE = InnoDB
COMMENT = 'Metadata about database as a whole, key-value pairs' ;

-- Add record for schema version, migration script expects this record to exist
INSERT INTO `QMetadata` (`metakey`, `value`) VALUES ('version', '15');
