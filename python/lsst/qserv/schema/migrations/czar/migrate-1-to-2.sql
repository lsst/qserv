--
-- Migration script from version 1 to version 2 of QMeta database:
--   - QStatsTmp table is added to database
--   - ProcessList views add query progress information
--


-- -----------------------------------------------------
-- Table `QStatsTmp`
-- MEMORY table - will be recreated(but empty) by mariadb every time server starts.
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `QStatsTmp` (
  `queryId` BIGINT NOT NULL COMMENT 'Query ID',
  `totalChunks` INT COMMENT 'Total number of cunks in the query',
  `completedChunks` INT COMMENT 'Number of completed chunks in the query',
  `queryBegin` TIMESTAMP DEFAULT 0 COMMENT 'When the query was started',
  `lastUpdate` TIMESTAMP DEFAULT 0 COMMENT 'Last time completedChunks was updated',
  PRIMARY KEY (`queryId`))
ENGINE = MEMORY
COMMENT = 'Table to track statistics of running queries.';

 -- -----------------------------------------------------
-- View `ShowProcessList`
-- This shows abbreviated Qmeta info suitable for "SHOW PROCESSLIST"
-- -----------------------------------------------------
CREATE OR REPLACE
  SQL SECURITY INVOKER
  VIEW `ShowProcessList` AS
  SELECT DISTINCT
    `QInfo`.`queryId` `Id`,
    `QInfo`.`user` `User`,
    NULL `Host`,
    GROUP_CONCAT(DISTINCT `QTable`.`dbName`) `db`,
    `QInfo`.`qType` `Command`,
    NULL `Time`,
    `QInfo`.`status` `State`,
    `QInfo`.`query` `Info`,
    NULL `Progress`,
    `QInfo`.`submitted` `Submitted`,
    `QInfo`.`completed` `Completed`,
    `QInfo`.`returned` `Returned`,
    `QInfo`.`czarId` `CzarId`,
    REPLACE(`QInfo`.`resultLocation`, '#QID#',  `QInfo`.`queryId`) `ResultLocation`,
    `QStatsTmp`.`totalChunks` `TotalChunks`,
    `QStatsTmp`.`completedChunks` `CompletedChunks`,
    `QStatsTmp`.`lastUpdate` `LastUpdate`
  FROM `QInfo` LEFT OUTER JOIN `QTable` USING (`queryId`)
       LEFT OUTER JOIN `QStatsTmp` USING (`queryId`)
  GROUP BY `QInfo`.`queryId`;

-- -----------------------------------------------------
-- View `InfoSchemaProcessList`
-- This shows full Qmeta info suitable for "SELECT ... FROM INFORMATION_SCHEMA.PROCESSLIST"
-- -----------------------------------------------------
CREATE OR REPLACE
  SQL SECURITY INVOKER
  VIEW `InfoSchemaProcessList` AS
  SELECT DISTINCT
    `QInfo`.`queryId` `ID`,
    `QInfo`.`user` `USER`,
    NULL `HOST`,
    GROUP_CONCAT(DISTINCT `QTable`.`dbName`) `DB`,
    `QInfo`.`qType` `COMMAND`,
    NULL `TIME`,
    `QInfo`.`status` `STATE`,
    `QInfo`.`query` `INFO`,
    `QInfo`.`submitted` `SUBMITTED`,
    `QInfo`.`completed` `COMPLETED`,
    `QInfo`.`returned` `RETURNED`,
    `QInfo`.`czarId` `CZARID`,
    REPLACE(`QInfo`.`resultLocation`, '#QID#',  `QInfo`.`queryId`) `RESULTLOCATION`,
    NULLIF(COUNT(`QWorker`.`chunk`), 0) `NCHUNKS`,
    `QStatsTmp`.`totalChunks` `TotalChunks`,
    `QStatsTmp`.`completedChunks` `CompletedChunks`,
    `QStatsTmp`.`lastUpdate` `LastUpdate`
  FROM `QInfo` LEFT OUTER JOIN `QTable` USING (`queryId`)
        LEFT OUTER JOIN `QWorker` USING (`queryId`)
        LEFT OUTER JOIN `QStatsTmp` USING (`queryId`)
  GROUP BY `QInfo`.`queryId`;
