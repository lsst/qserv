--
-- Migration script from version 3 to version 4 of QMeta database:
--

-- Increased storage capacity of the columns in the table QInfo
ALTER TABLE `QInfo`
    MODIFY COLUMN `query`       MEDIUMTEXT NOT NULL     COMMENT 'Original query text as was submitted by client.',
    MODIFY COLUMN `qTemplate`   MEDIUMTEXT NOT NULL     COMMENT 'Query template, string used to build final per-chunk query.',
    MODIFY COLUMN `qMerge`      MEDIUMTEXT DEFAULT NULL COMMENT 'Merge (or aggregate) query to be executed on results table, result of this query is stored in merge table. If NULL then it is equivalent to SELECT *.',
    MODIFY COLUMN `resultQuery` MEDIUMTEXT DEFAULT NULL COMMENT 'Query to be used by mysqlproxy to get final results.';