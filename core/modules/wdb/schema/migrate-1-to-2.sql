--
-- Migration script from version 1 to version 2 of the qservw_worker database:
--
--  -  grant extended privileges to user 'qsmaster' for the database tables
--

GRANT ALL ON qservw_worker.* TO 'qsmaster'@'localhost';
