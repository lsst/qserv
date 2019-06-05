--
-- Migration script from version 1 to version 2 of QMeta database:
--   - QInfo table adds resultQuery column

ALTER TABLE `QInfo` ADD COLUMN (`resultQuery` TEXT);
