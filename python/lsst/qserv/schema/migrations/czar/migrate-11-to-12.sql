-- -------------------------------------------------------------------
-- Rename table QStatsTmp into QProgress to reflect its purpose
-- and add a foreign key constraint to QInfo table.
-- This table tracks chunk processing progress of the running queries.
-- -------------------------------------------------------------------
ALTER TABLE QStatsTmp RENAME AS QProgress;
ALTER TABLE QProgress ADD CONSTRAINT `fk_queryId` FOREIGN KEY (`queryId`) REFERENCES `QInfo` (`queryId`) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE QProgress COMMENT = 'Table to track chunk processing progress of the running queries.';

-- -------------------------------------------------------------------
-- Drop the QWorker table as it is no longer needed.
-- This table was used to track worker nodes and their statuses.
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS QWorker;
