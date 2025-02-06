DROP VIEW IF EXISTS `ShowProcessList`;
DROP VIEW IF EXISTS `InfoSchemaProcessList`;

-- ----------------------------------------------------------------------------------------
-- View `InfoSchemaProcessList`
-- This shows full Qmeta info suitable for "SELECT ... FROM INFORMATION_SCHEMA.PROCESSLIST"
-- ----------------------------------------------------------------------------------------
CREATE OR REPLACE
  SQL SECURITY INVOKER
  VIEW `InfoSchemaProcessList` AS
  SELECT `qi`.`queryId` `ID`,
         `qi`.`qType` `TYPE`,
         `qc`.`czar` `CZAR`,
         `qc`.`czarId` `CZAR_ID`,
         `qi`.`submitted` `SUBMITTED`,
         `qs`.`lastUpdate` `UPDATED`,
         `qi`.`chunkCount` `CHUNKS`,
         `qs`.`completedChunks` `CHUNKS_COMP`,
         `qi`.`query` `QUERY`
    FROM `QInfo` AS `qi`
    LEFT OUTER JOIN `QStatsTmp` AS `qs` ON `qi`.`queryId`=`qs`.`queryId`
    JOIN `QCzar` AS `qc` ON `qi`.`czarId`=`qc`.`czarId`
    WHERE `qi`.`status` = 'EXECUTING';

-- ------------------------------------------------------------------------------------
-- View `InfoSchemaQueries`
-- This shows full Qmeta info suitable for "SELECT ... FROM INFORMATION_SCHEMA.QUERIES"
-- ------------------------------------------------------------------------------------
CREATE OR REPLACE
  SQL SECURITY INVOKER
  VIEW `InfoSchemaQueries` AS
  SELECT `qi`.`queryId` `ID`,
         `qi`.`qType` `TYPE`,
         `qc`.`czar` `CZAR`,
         `qc`.`czarId` `CZAR_ID`,
         `qi`.`status` `STATUS`,
         `qi`.`submitted` `SUBMITTED`,
         `qi`.`completed` `COMPLETED`,
         `qi`.`returned` `RETURNED`,
         `qi`.`chunkCount` `CHUNKS`,
         `qi`.`collectedBytes` `BYTES`,
         `qi`.`collectedRows` `ROWS_COLLECTED`,
         `qi`.`finalRows` `ROWS`,
         GROUP_CONCAT(DISTINCT `qt`.`dbName`) `DBS`,
         `qi`.`query` `QUERY`
    FROM `QInfo` AS `qi`
    JOIN `QCzar` AS `qc` ON `qi`.`czarId`=`qc`.`czarId`
    LEFT OUTER JOIN `QTable` AS `qt` ON `qi`.`queryId`=`qt`.`queryId`
    GROUP BY `qi`.`queryId`;
