ALTER TABLE `config_database_table`
  ADD COLUMN `persist_sub_chunks`  BOOLEAN NOT NULL DEFAULT FALSE
  AFTER `is_partitioned`;
