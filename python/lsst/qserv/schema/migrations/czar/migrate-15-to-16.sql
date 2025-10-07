-- -----------------------------------------------------
-- Table `UserTables`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `UserTables` (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'A unique identifier of the ingest request',
  `status` ENUM('IN_PROGRESS','COMPLETED','FAILED','FAILED_LR') DEFAULT 'IN_PROGRESS' COMMENT 'The status of the request',
  `begin_time` BIGINT UNSIGNED NOT NULL COMMENT 'The timestamp (ms) since UNIX Epoch when the request processing started',
  `end_time` BIGINT UNSIGNED DEFAULT 0 COMMENT 'The timestamp (ms) since UNIX Epoch when the request processing finished or failed',
  `delete_time` BIGINT UNSIGNED DEFAULT 0 COMMENT 'The timestamp (ms) since UNIX Epoch when the temporary table/database was deleted',
  `error` TEXT DEFAULT '' COMMENT 'The error message explaining reasons why the request failed',
  `database` VARCHAR(64) NOT NULL COMMENT 'The name of a database',
  `table` VARCHAR(64) NOT NULL COMMENT 'The name of a table',
  `table_type` ENUM('FULLY_REPLICATED','DIRECTOR','CHILD','REF_MATCH') NOT NULL COMMENT 'The type of the table',
  `is_temporary` TINYINT(1) NOT NULL COMMENT 'The flag indicating if the table is temporary and is expected to be deleted after use (FULLY_REPLICATED tables only)',
  `data_format` ENUM('CSV','JSON','PARQUET') NOT NULL COMMENT 'The format of the input data',
  `num_chunks` INT UNSIGNED DEFAULT 0 COMMENT 'The number of chunks ingested (0 for FULLY_REPLICATED tables)',
  `num_rows` BIGINT UNSIGNED DEFAULT 0 COMMENT 'The number of rows in the input file',
  `num_bytes` BIGINT UNSIGNED DEFAULT 0 COMMENT 'The number of bytes in the input file',
  `transaction_id` INT UNSIGNED DEFAULT 0 COMMENT 'The transaction ID (Replication/Ingest system)',
  PRIMARY KEY (`id`),
  INDEX `idx_database` (`database` ASC),
  INDEX `idx_table` (`table` ASC),
  INDEX `idx_begin_time` (`begin_time` ASC),
  INDEX `idx_end_time` (`end_time` ASC),
  INDEX `idx_status` (`status` ASC)
)
ENGINE = InnoDB
COMMENT = 'A registry for the user table ingest requests';

-- -----------------------------------------------------
-- Table `UserTablesParams`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `UserTablesParams` (
  `id` INT UNSIGNED NOT NULL COMMENT 'A unique identifier of the ingest request',
  `key` VARCHAR(256) NOT NULL COMMENT 'The name of a parameter',
  `val` MEDIUMTEXT NOT NULL COMMENT 'The value of a parameter',
  INDEX `idx_id` (`id` ASC),
  INDEX `idx_key` (`key` ASC),
  CONSTRAINT `fk_id`
    FOREIGN KEY (`id`)
    REFERENCES `UserTables` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB
COMMENT = 'Extended parameters of the table ingest requests';
