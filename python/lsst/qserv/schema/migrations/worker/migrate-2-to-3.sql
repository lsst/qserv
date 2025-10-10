-- -----------------------------------------------------
-- Remove unused monitor user
-- -----------------------------------------------------

DROP USER IF EXISTS 'monitor'@'localhost';
DROP USER IF EXISTS 'monitor'@'%';

-- -----------------------------------------------------
-- Remove superfluous grants from former admin module
-- -----------------------------------------------------

REVOKE ALL PRIVILEGES ON `qservMeta`.* FROM 'qsmaster'@'localhost';
REVOKE ALL PRIVILEGES ON `qservMeta`.* FROM 'qsmaster'@'%';

REVOKE ALL PRIVILEGES ON `qservCssData`.* FROM 'qsmaster'@'localhost';
REVOKE ALL PRIVILEGES ON `qservCssData`.* FROM 'qsmaster'@'%';

REVOKE ALL PRIVILEGES ON `qservResult`.* FROM 'qsmaster'@'localhost';
REVOKE ALL PRIVILEGES ON `qservResult`.* FROM 'qsmaster'@'%';
