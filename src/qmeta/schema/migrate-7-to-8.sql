-- Fix a bug that was introduced earlier. In the previous version of
-- the schema these columns were defined as 'INT' (32-bits) which was
-- causing Qserv to crash when result set sizes exceeded the limit.  
ALTER TABLE `QInfo` MODIFY COLUMN `collectedBytes` BIGINT DEFAULT 0 COMMENT 'number of bytes collected from workers';
ALTER TABLE `QInfo` MODIFY COLUMN `collectedRows` BIGINT DEFAULT 0 COMMENT  'number of rows collected from workers';
ALTER TABLE `QInfo` MODIFY COLUMN `finalRows` BIGINT DEFAULT 0 COMMENT  'number of rows in the final result';
