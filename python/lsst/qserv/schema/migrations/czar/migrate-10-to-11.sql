-- ------------------------------------------------------------------------------------
-- Added status 'FAILED_LR' to QInfo table
-- This status indicates that the query execution failed due to a large result set.
-- ------------------------------------------------------------------------------------
ALTER TABLE `QInfo`
MODIFY COLUMN `status` ENUM('EXECUTING', 'COMPLETED', 'FAILED', 'FAILED_LR', 'ABORTED') NOT NULL DEFAULT 'EXECUTING'
COMMENT 'Status of query processing.';

-- ------------------------------------------------------------------------------------
-- View `InfoSchemaQueries`
-- This shows full Qmeta info suitable for "SELECT ... FROM INFORMATION_SCHEMA.QUERIES"
-- ------------------------------------------------------------------------------------
-- Updated to include error messages in the view
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
         `qi`.`query` `QUERY`,
         TRIM(BOTH ',' FROM GROUP_CONCAT(IF(`qm`.`severity`='ERROR',`qm`.`message`,''))) `ERROR`
    FROM `QInfo` AS `qi`
    JOIN `QCzar` AS `qc` ON `qi`.`czarId`=`qc`.`czarId`
    LEFT OUTER JOIN `QTable` AS `qt` ON `qi`.`queryId`=`qt`.`queryId`
    LEFT OUTER JOIN `QMessages` AS `qm` ON `qi`.`queryId`=`qm`.`queryId`
    GROUP BY `qi`.`queryId`;
