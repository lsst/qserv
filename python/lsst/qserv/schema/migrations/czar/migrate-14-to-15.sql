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
    LEFT OUTER JOIN `QProgress` AS `qs` ON `qi`.`queryId`=`qs`.`queryId`
    JOIN `QCzar` AS `qc` ON `qi`.`czarId`=`qc`.`czarId`
    WHERE `qi`.`status` = 'EXECUTING';

