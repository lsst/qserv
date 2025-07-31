-- -----------------------------------------------------
-- Table `QProgressHistory`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `QProgressHistory` (
  `queryId` BIGINT NOT NULL COMMENT 'Query ID',
  `history` MEDIUMTEXT COMMENT 'Serialized JSON object representing the latest state of the history',
  `begin` BIGINT UNSIGNED NOT NULL COMMENT 'The timestamp (milliseconds since UNIX Epoch) of the first point in the history',
  `end` BIGINT UNSIGNED NOT NULL COMMENT 'The timestamp (milliseconds since UNIX Epoch) of the last point in the history',
  `totalPoints` INT UNSIGNED NOT NULL COMMENT 'The total number of points in the history',
  PRIMARY KEY (`queryId`),
  INDEX `idx_begin` (`begin` ASC),
  INDEX `idx_end` (`end` ASC),
  INDEX `idx_totalPoints` (`totalPoints` ASC),
  CONSTRAINT `fk_queryId` FOREIGN KEY (`queryId`) REFERENCES `QInfo` (`queryId`) ON DELETE CASCADE ON UPDATE CASCADE)
ENGINE = InnoDB
COMMENT = 'Table to store the query progression history.';
