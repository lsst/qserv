CREATE USER IF NOT EXISTS '{{MYSQLD_USER_QSERV}}'@'localhost';

CREATE DATABASE IF NOT EXISTS qservResult;
GRANT ALL ON qservResult.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';

-- Secondary index database (i.e. objectId/chunkId relation)
-- created by integration test script/loader for now
CREATE DATABASE IF NOT EXISTS qservMeta;
GRANT ALL ON qservMeta.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';

-- CSS database
CREATE DATABASE IF NOT EXISTS qservCssData;
GRANT ALL ON qservCssData.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';

-- Create user for external monitoring applications
CREATE USER IF NOT EXISTS '{{MYSQLD_USER_MONITOR}}'@'localhost' IDENTIFIED BY '{{MYSQLD_PASSWORD_MONITOR}}';
GRANT PROCESS ON *.* TO '{{MYSQLD_USER_MONITOR}}'@'localhost';
