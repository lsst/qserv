CREATE TABLE IF NOT EXISTS `chunkMap` (
  `chunks` BLOB NOT NULL COMMENT 'A collection of chunk replicas at workers in the JSON format',
  `update_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'The most recent update time of the map')
ENGINE = InnoDB
COMMENT = 'Chunk disposition across workers';
