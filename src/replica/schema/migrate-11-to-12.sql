ALTER TABLE `transaction_contrib`
  ADD COLUMN `num_warnings` INT UNSIGNED NOT NULL DEFAULT 0
  AFTER `tmp_file`;

ALTER TABLE `transaction_contrib`
  ADD COLUMN `num_rows_loaded` BIGINT UNSIGNED NOT NULL DEFAULT 0
  AFTER `num_warnings`;

-- This approximation should work for existing catalogs ingested
-- into Qserv before a support for capturing this number was added.
UPDATE `transaction_contrib` SET `num_rows_loaded` = `num_rows`;

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
