--
-- Migration script from version 5 to version 6 of QMeta database:
--
-- Add indexes to optimize queries made by the Qserv Web Dashboard
-- while monitoring Qserv progress on the queries submitted by users.
-- 
CREATE INDEX IF NOT EXISTS `QInfo_status_index` ON `QInfo` (`status`);
CREATE INDEX IF NOT EXISTS `QInfo_qtype_index` ON `QInfo` (`qType`);
CREATE INDEX IF NOT EXISTS `QInfo_submitted_index` ON `QInfo` (`submitted`);
CREATE INDEX IF NOT EXISTS `QInfo_completed_index` ON `QInfo` (`completed`);
CREATE INDEX IF NOT EXISTS `QInfo_returned_index` ON `QInfo` (`returned`);

-- This index is meant to be used for searching within the text of
-- the user queries. 
CREATE FULLTEXT INDEX IF NOT EXISTS `QInfo_query_index` ON `QInfo` (`query`);
