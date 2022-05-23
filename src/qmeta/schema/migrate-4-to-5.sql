--
-- Migration script from version 4 to version 5 of QMeta database:
--
-- -----------------------------------------------------
-- Table `QMessages`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `QMessages` (
  `queryId` BIGINT NOT NULL COMMENT 'Query identifier, foreign key into QueryInfo table',
  `msgSource` VARCHAR(63) NOT NULL COMMENT 'Brief string describing the source PARSE, COMM, WORKER, etc',
  `chunkId` INT COMMENT 'chunkId',
  `code` SMALLINT COMMENT 'Error code',
  `message` MEDIUMTEXT NOT NULL COMMENT 'Message generated while executing queryId',
  `severity` VARCHAR(256) NOT NULL COMMENT 'severity of the message INFO or ERROR',
  `timestamp` BIGINT UNSIGNED NOT NULL COMMENT 'time of error message in milliseconds',
  INDEX `QMessages_qId_idx` (`queryId`),
  CONSTRAINT `QMessages_qid_fkey`
    FOREIGN KEY (`queryId`)
    REFERENCES `QInfo` (`queryId`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
COMMENT = 'Table of messages generated during queries.';


--
-- Add the chunkCount column to QInfo.
--  
ALTER TABLE `QInfo` 
  ADD `chunkCount` INT COMMENT 'number of chunks needed by the query',
  ADD `resultBytes` INT DEFAULT 0 COMMENT 'number of bytes in the result',
  ADD `resultRows` INT DEFAULT 0 COMMENT  'number of rows in the result';
  