--
-- Migration script from version 4 to version 5 of QMeta database:
--
-- -----------------------------------------------------
-- Table `QMetaMessages`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `QMessages` (
  `queryId` BIGINT NOT NULL COMMENT 'Query identifier, foreign key into QueryInfo table',
  `chunkId` INT COMMENT 'chunkId',
  `code` SMALLINT COMMENT 'Error code',
  `message` MEDIUMTEXT NOT NULL COMMENT 'Message generated while executing queryId',
  `severity` ENUM('INFO', 'ERROR') COMMENT 'severity of the message INFO or ERROR',
  `timestamp` FLOAT COMMENT 'time of error message',
  INDEX `QMessages_qId_idx` (`queryId`),
  CONSTRAINT `QMessages_qid_fkey`
    FOREIGN KEY (`queryId`)
    REFERENCES `QInfo` (`queryId`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
COMMENT = 'Table of messages generated during queries.';

