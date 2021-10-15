-- Drop these columns because Qserv has predefined names for those
ALTER TABLE `config_database` DROP COLUMN `chunk_id_key` ;
ALTER TABLE `config_database` DROP COLUMN `sub_chunk_id_key` ;

-- Add a new column that's needed to support more than one "director" table per catalog.
-- This eliminates any need in having the special flag `is_director` that has to be
-- dropped now. Director tables are disambiguated from the dependent ones by not
-- having the directors.
ALTER TABLE `config_database_table` ADD COLUMN `director_table` TEXT NOT NULL DEFAULT '' AFTER `is_director` ;
-- Fix-up the name of the director table in the dependent tables.
DROP TABLE IF EXISTS `database_director_table` ;
CREATE TABLE IF NOT EXISTS `database_director_table` SELECT `database`,`table` FROM `config_database_table` WHERE `is_director` != 0 ;
DROP FUNCTION IF EXISTS SetDirectorTableF ;
DELIMITER //
CREATE FUNCTION SetDirectorTableF(`database_name` VARCHAR(255), `table_name` VARCHAR(255)) RETURNS INT
BEGIN
  UPDATE `config_database_table`
    SET `director_table` = table_name
    WHERE `database` = database_name
      AND `table` != table_name
      AND `is_partitioned` != 0
      AND `is_director` = 0 ;
  RETURN 1 ;
END //
DELIMITER ;
SELECT SetDirectorTableF(`database`, `table`) FROM `database_director_table` ;
DROP FUNCTION IF EXISTS SetDirectorTableF ;
DROP TABLE IF EXISTS `database_director_table` ;
-- Get rid of this column as it's no longer needed.
ALTER TABLE `config_database_table` DROP COLUMN `is_director` ;
