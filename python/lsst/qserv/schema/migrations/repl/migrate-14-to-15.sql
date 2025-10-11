ALTER TABLE `config_database_table`
  ADD COLUMN `charset_name` VARCHAR(255) DEFAULT ""
  AFTER `unique_primary_key`;

ALTER TABLE `config_database_table`
  ADD COLUMN `collation_name` VARCHAR(255) DEFAULT ""
  AFTER `charset_name`;
