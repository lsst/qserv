--
-- Changes needed for the RefMatch tables
--
ALTER TABLE `config_database_table` ADD COLUMN `director_table2` VARCHAR(255) DEFAULT "" AFTER `director_key` ;
ALTER TABLE `config_database_table` ADD COLUMN `director_key2`   VARCHAR(255) DEFAULT "" AFTER `director_table2` ;
ALTER TABLE `config_database_table` ADD COLUMN `flag`            VARCHAR(255) DEFAULT "" AFTER `director_key2` ;
ALTER TABLE `config_database_table` ADD COLUMN `ang_sep`         DOUBLE       DEFAULT 0  AFTER `flag` ;
