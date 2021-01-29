--
-- Script to initialize the QMeta database
--

CREATE USER IF NOT EXISTS '{{MYSQLD_USER_QSERV}}'@'localhost';
CREATE USER IF NOT EXISTS '{{MYSQLD_USER_QSERV}}'@'%';

-- Secondary index database (i.e. objectId/chunkId relation)
-- created by integration test script/loader for now
CREATE DATABASE IF NOT EXISTS qservMeta;
GRANT ALL ON qservMeta.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';
GRANT ALL ON qservMeta.* TO '{{MYSQLD_USER_QSERV}}'@'%';
