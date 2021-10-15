-- Drop these columns because Qserv has predefined names for those
ALTER TABLE `config_database` DROP COLUMN `chunk_id_key` ;
ALTER TABLE `config_database` DROP COLUMN `sub_chunk_id_key` ;
