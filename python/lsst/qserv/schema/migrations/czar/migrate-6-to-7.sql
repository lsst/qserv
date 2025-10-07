--
-- Migration script from version 6 to version 7 of QMeta database:
--
--
-- Rename resultBytes and resultRows to collectedBytes and collectedRows in QInfo.
-- Also, add finalRows to QInfo
--  
ALTER TABLE `QInfo` 
  RENAME COLUMN `resultBytes` TO `collectedBytes`,
  RENAME COLUMN `resultRows` TO `collectedRows`,  
  ADD `finalRows` INT DEFAULT 0 COMMENT  'number of rows in the final result';
 