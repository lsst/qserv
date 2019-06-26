--
-- Migration script from version 2 to version 3 of QMeta database:
--

-- QInfo table adds resultQuery column, migration script expecets this table to exist.
ALTER TABLE `QInfo` ADD COLUMN (`resultQuery` TEXT);

-- QInfo table drops proxyOrderBy column
ALTER TABLE `QInfo` DROP COLUMN `proxyOrderBy`;

-- Update record for schema version, migration script expects this record to exist.
UPDATE `QMetadata` SET `value` = '3' WHERE `metakey` = 'version';
