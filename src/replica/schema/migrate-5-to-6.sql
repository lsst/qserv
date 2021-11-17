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

-- Update the schema version
UPDATE `QMetadata` SET `value` = '6' WHERE `metakey` = 'version' ;
