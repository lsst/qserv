-- WARN all '<user>'@'%' must be removed in production

CREATE USER IF NOT EXISTS 'qsmaster'@'localhost';
CREATE USER IF NOT EXISTS 'qsmaster'@'%';
GRANT ALL ON `q\_memoryLockDb`.* TO 'qsmaster'@'localhost';
GRANT ALL ON `q\_memoryLockDb`.* TO 'qsmaster'@'%';
-- Subchunks databases
GRANT ALL ON `Subchunks\_%`.* TO 'qsmaster'@'localhost';
GRANT ALL ON `Subchunks\_%`.* TO 'qsmaster'@'%';

-- Create user for external monitoring applications
CREATE USER IF NOT EXISTS 'monitor'@'localhost' IDENTIFIED BY '<MYSQL_MONITOR_PASSWORD>';
CREATE USER IF NOT EXISTS 'monitor'@'%' IDENTIFIED BY '<MYSQL_MONITOR_PASSWORD>';
GRANT PROCESS ON *.* TO 'monitor'@'localhost';
GRANT PROCESS ON *.* TO 'monitor'@'%';

FLUSH PRIVILEGES;