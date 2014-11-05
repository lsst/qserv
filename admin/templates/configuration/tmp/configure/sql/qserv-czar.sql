CREATE DATABASE IF NOT EXISTS qservResult;
CREATE DATABASE IF NOT EXISTS qservScratch;
GRANT ALL ON qservResult.* TO 'qsmaster'@'localhost';
GRANT ALL ON qservScratch.* TO 'qsmaster'@'localhost';

GRANT SELECT ON *.* TO 'qsmaster'@'localhost';

-- Data table
-- In the long term:
--    * has to created by the dataloader
--    * should be only on workers
CREATE DATABASE IF NOT EXISTS LSST;
GRANT ALL ON LSST.* TO 'qsmaster'@'localhost';
GRANT EXECUTE ON `LSST`.* TO 'qsmaster'@'localhost';



