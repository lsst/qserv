-- Add an index to the transactions table
ALTER TABLE `transaction` ADD KEY (`state`);

-- Add a new table to store extended parameters of the transaction contributions
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

-- Move the content of these columns into table `transaction_contrib_ext`. Then
-- drop the columns from the original table.
INSERT INTO `transaction_contrib_ext` SELECT `id`, 'fields_terminated_by', `fields_terminated_by` FROM `transaction_contrib` ;
INSERT INTO `transaction_contrib_ext` SELECT `id`, 'fields_enclosed_by',   `fields_enclosed_by`   FROM `transaction_contrib` ;
INSERT INTO `transaction_contrib_ext` SELECT `id`, 'fields_escaped_by',    `fields_escaped_by`    FROM `transaction_contrib` ;
INSERT INTO `transaction_contrib_ext` SELECT `id`, 'lines_terminated_by',  `lines_terminated_by`  FROM `transaction_contrib` ;

ALTER TABLE `transaction_contrib` DROP COLUMN `fields_terminated_by` ;
ALTER TABLE `transaction_contrib` DROP COLUMN `fields_enclosed_by` ;
ALTER TABLE `transaction_contrib` DROP COLUMN `fields_escaped_by` ;
ALTER TABLE `transaction_contrib` DROP COLUMN `lines_terminated_by` ;

-- Drop the obsolete column
ALTER TABLE `transaction_contrib` DROP COLUMN `expiration_timeout_sec` ;

-- Drop 'EXPIRED' from the contribution status
ALTER TABLE `transaction_contrib` CHANGE COLUMN `status` `status` ENUM (
    'IN_PROGRESS',
    'CREATE_FAILED',
    'START_FAILED',
    'READ_FAILED',
    'LOAD_FAILED',
    'CANCELLED',
    'FINISHED') NOT NULL DEFAULT 'IN_PROGRESS' ;

-- Add a new column
ALTER TABLE `transaction_contrib` ADD COLUMN `tmp_file` TEXT NOT NULL DEFAULT '' AFTER `status` ;

-- Add indexes to the transaction contribution table
ALTER TABLE `transaction_contrib` ADD KEY (`type`);
ALTER TABLE `transaction_contrib` ADD KEY (`status`);
