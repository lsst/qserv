ALTER TABLE `config_database_table`
  ADD COLUMN `unique_primary_key` BOOLEAN NOT NULL DEFAULT TRUE
  AFTER `ang_sep`;
