CREATE USER 'qsmaster'@'localhost';
CREATE USER 'qsmaster'@'%';

GRANT ALL ON qservResult.* TO 'qsmaster'@'localhost';
GRANT ALL ON qservResult.* TO 'qsmaster'@'%';

-- Secondary index database (i.e. objectId/chunkId relation)
-- created by integration test script/loader for now
GRANT ALL ON qservMeta.* TO 'qsmaster'@'localhost';
GRANT ALL ON qservMeta.* TO 'qsmaster'@'%';

-- CSS database
GRANT ALL ON qservCssData.* TO 'qsmaster'@'localhost';
GRANT ALL ON qservCssData.* TO 'qsmaster'@'%';

-- Grant root access to replication controller pod
GRANT ALL ON *.* TO 'root'@'{{.ReplicationControllerFQDN}}' IDENTIFIED BY 'CHANGEME' WITH GRANT OPTION;

-- Create user for external monitoring applications
CREATE USER 'monitor'@'localhost' IDENTIFIED BY '<MYSQL_MONITOR_PASSWORD>';
CREATE USER 'monitor'@'%' IDENTIFIED BY '<MYSQL_MONITOR_PASSWORD>';
GRANT PROCESS ON *.* TO 'monitor'@'localhost';
GRANT PROCESS ON *.* TO 'monitor'@'%';

FLUSH PRIVILEGES;
