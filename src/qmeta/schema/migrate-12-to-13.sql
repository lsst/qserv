-- -----------------------------------------------------
-- Table `QProgressPlot`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `QProgressPlot` (
  `queryId` BIGINT NOT NULL COMMENT 'Query ID',
  `plot` MEDIUMTEXT COMMENT 'Serialized JSON object representing the latest state of the plot',
  PRIMARY KEY (`queryId`),
  CONSTRAINT `fk_queryId` FOREIGN KEY (`queryId`) REFERENCES `QInfo` (`queryId`) ON DELETE CASCADE ON UPDATE CASCADE)
ENGINE = InnoDB
COMMENT = 'Table to store the query progression plots.';