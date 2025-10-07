-- Update the transaction contribution table. Note that the procedure will
-- make the best attempt to preserve existing data.

ALTER TABLE `transaction_contrib` ADD COLUMN `type` ENUM ('SYNC', 'ASYNC') NOT NULL DEFAULT 'SYNC' AFTER `url` ;
ALTER TABLE `transaction_contrib` ADD COLUMN `expiration_timeout_sec` INT UNSIGNED NOT NULL DEFAULT 0 AFTER `type` ;

ALTER TABLE `transaction_contrib` ADD COLUMN `fields_terminated_by` VARCHAR(255) NOT NULL DEFAULT '\\t' AFTER `expiration_timeout_sec` ;
ALTER TABLE `transaction_contrib` ADD COLUMN `fields_enclosed_by`   VARCHAR(255) NOT NULL DEFAULT ''    AFTER `fields_terminated_by` ;
ALTER TABLE `transaction_contrib` ADD COLUMN `fields_escaped_by`    VARCHAR(255) NOT NULL DEFAULT '\\'  AFTER `fields_enclosed_by` ;
ALTER TABLE `transaction_contrib` ADD COLUMN `lines_terminated_by`  VARCHAR(255) NOT NULL DEFAULT '\\n' AFTER `fields_escaped_by` ;

ALTER TABLE `transaction_contrib` ADD COLUMN `create_time` BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `success` ;
ALTER TABLE `transaction_contrib` ADD COLUMN `start_time`  BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `create_time` ;
ALTER TABLE `transaction_contrib` ADD COLUMN `read_time`   BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `start_time` ;
ALTER TABLE `transaction_contrib` ADD COLUMN `load_time`   BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `read_time` ;

-- The old schema didn't differentiate between request's `create_time` and `start_time`.
-- It also didn't diffirentiate between `read_time` and `load_time`.

UPDATE `transaction_contrib` SET `create_time` = `begin_time`;
UPDATE `transaction_contrib` SET `start_time`  = `begin_time` ;
UPDATE `transaction_contrib` SET `read_time`   = `end_time` ;
UPDATE `transaction_contrib` SET `load_time`   = `end_time` ;

ALTER TABLE `transaction_contrib` ADD  COLUMN `status` ENUM (
    'IN_PROGRESS',
    'CREATE_FAILED',
    'START_FAILED',
    'READ_FAILED',
    'LOAD_FAILED',
    'CANCELLED',
    'EXPIRED',
    'FINISHED') NOT NULL DEFAULT 'IN_PROGRESS' AFTER `load_time` ;

-- In the old implementation it wasn't possible to tell if a request failed
-- when reading/preprocessing an input file or when the prep[rocessed file
-- was being loaded into MySQL. Hence an assumpton is that it failed early.

UPDATE `transaction_contrib` SET `status` = IF(`success`, 'FINISHED', 'READ_FAILED');

-- Add 4 new columns to capture a context of errors.

ALTER TABLE `transaction_contrib` ADD  COLUMN `http_error`    INT     NOT NULL DEFAULT 0 ;
ALTER TABLE `transaction_contrib` ADD  COLUMN `system_error`  INT     NOT NULL DEFAULT 0 ;
ALTER TABLE `transaction_contrib` ADD  COLUMN `error`         TEXT    NOT NULL DEFAULT '' ;
ALTER TABLE `transaction_contrib` ADD  COLUMN `retry_allowed` BOOLEAN NOT NULL DEFAULT 0 ;

-- Get rid of the older columns

ALTER TABLE `transaction_contrib` DROP COLUMN `begin_time` ;
ALTER TABLE `transaction_contrib` DROP COLUMN `end_time` ;
ALTER TABLE `transaction_contrib` DROP COLUMN `success` ;
