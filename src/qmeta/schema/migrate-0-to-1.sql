--
-- Migration script from version 0 to version 1 of QMeta database:
--   - QInfo table adds two new columns
--   - processlist views add result location column
--   - QMetadata table is added to keep schema version
--


-- -----------------------------------------------------
-- Add new columns to table `QInfo`
-- -----------------------------------------------------
ALTER TABLE `QInfo` ADD COLUMN (
  `messageTable` CHAR(63) NULL COMMENT 'Name of the message table for the ASYNC query',
  `resultLocation` TEXT NULL COMMENT 'Result destination - table name, file name, etc.'
);

-- -----------------------------------------------------
-- Update view `ShowProcessList`
-- -----------------------------------------------------
ALTER  VIEW `ShowProcessList` AS
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
    REPLACE(`QInfo`.`resultLocation`, '#QID#',  `QInfo`.`queryId`) `ResultLocation`
  FROM `QInfo` LEFT OUTER JOIN `QTable` USING (`queryId`)
  GROUP BY `QInfo`.`queryId`;

-- -----------------------------------------------------
-- Update view `InfoSchemaProcessList`
-- -----------------------------------------------------
ALTER VIEW `InfoSchemaProcessList` AS
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
    NULLIF(COUNT(`QWorker`.`chunk`), 0) `NCHUNKS`
  FROM `QInfo` LEFT OUTER JOIN `QTable` USING (`queryId`)
        LEFT OUTER JOIN `QWorker` USING (`queryId`)
  GROUP BY `QInfo`.`queryId`;

-- -----------------------------------------------------
-- Create table `QMetadata`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `QMetadata` (
  `metakey` CHAR(64) NOT NULL COMMENT 'Key string',
  `value` TEXT NULL COMMENT 'Key string',
  PRIMARY KEY (`metakey`))
ENGINE = InnoDB
COMMENT = 'Metadata about database as a whole, bunch of key-value pairs';

-- Add record for schema version, migration script expects this record to exist
INSERT INTO `QMetadata` (`metakey`, `value`) VALUES ('version', '1');
