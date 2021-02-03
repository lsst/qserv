-- TODO solve mysql security in DM-21824: 
--   1. qsmaster and qsreplica should have password authentication when remote.
--   2. problem with DNS registration (is the pod registered inside DNS when it tries to connect to repl-db)
--   3. problem with @worker_dn_filter too long:
--      ERROR 1470 (HY000) at line 5: String 'qserv-dev-worker-%.qserv-dev-worker.qserv-dev.svc.cluster.local' is too long for host name (should be no longer than 60)

-- SET @worker_dn_filter := '<WORKER_DN_FILTER>';
-- SET @replctl_dn_filter := '<REPLCTL_DN_FILTER>';

-- SET @query = CONCAT("CREATE USER 'qsreplica'@'", @worker_dn_filter,"'");
-- PREPARE stmt FROM @query;
-- EXECUTE stmt;
-- DEALLOCATE PREPARE stmt;

-- SET @query = CONCAT("GRANT ALL PRIVILEGES ON qservReplica.* TO 'qsreplica'@'", @worker_dn_filter,"'");
-- PREPARE stmt FROM @query;
-- EXECUTE stmt;
-- DEALLOCATE PREPARE stmt;

-- SET @query = CONCAT("CREATE USER 'qsreplica'@'", @replctl_dn_filter,"'");
-- PREPARE stmt FROM @query;
-- EXECUTE stmt;
-- DEALLOCATE PREPARE stmt;

-- SET @query = CONCAT("GRANT ALL PRIVILEGES ON qservReplica.* TO 'qsreplica'@'", @replctl_dn_filter,"'");
-- PREPARE stmt FROM @query;
-- EXECUTE stmt;
-- DEALLOCATE PREPARE stmt;

CREATE USER 'qsreplica'@'%' IDENTIFIED BY '<MYSQL_REPLICA_PASSWORD>';
GRANT ALL ON qservReplica.* TO 'qsreplica'@'%';

CREATE USER 'probe'@'localhost';

FLUSH PRIVILEGES;
