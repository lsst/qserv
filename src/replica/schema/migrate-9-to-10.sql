-- A few notes on the default values of the new columns:
--
-- 1) Values of the `create_time` and `publish_time` columns of both tables will
--    be incorrect for any catalogs and tables. Correct values should be set
--    (if that will be needed) from at the earliest `create time` and `end_time`
--    values of the database transactions or the relevant persisent data structures.
--    Note that these values are set mainly for the information and bookkeeping
--    purposes, and they won't have any affect on the functioning of
--    the Replication/Ingest system.
--
-- 2) Values of the `is_published` column added to the second table is initialized
--    based on the status of the correspomnding catalog. This may potentially affect
--    catalogs that were "unpublished" for ingesting additional tables. This scenario
--    would need to be manually evaluated to prevent these new (yet to be published tables)
--    to be marked as "published".

ALTER TABLE `config_database` ADD COLUMN `create_time`  BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `is_published` ;
ALTER TABLE `config_database` ADD COLUMN `publish_time` BIGINT UNSIGNED NOT NULL DEFAULT (UNIX_TIMESTAMP() * 1000) AFTER `create_time` ;

ALTER TABLE `config_database_table` ADD COLUMN `is_published` BOOLEAN DEFAULT TRUE AFTER `longitude_key` ;
ALTER TABLE `config_database_table` ADD COLUMN `create_time`  BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER `is_published` ;
ALTER TABLE `config_database_table` ADD COLUMN `publish_time` BIGINT UNSIGNED NOT NULL DEFAULT (UNIX_TIMESTAMP() * 1000) AFTER `create_time` ;
