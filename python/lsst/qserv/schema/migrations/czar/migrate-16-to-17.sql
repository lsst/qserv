-- -----------------------------------------------------
-- Remove unused monitor user
-- -----------------------------------------------------

DROP USER IF EXISTS 'monitor'@'localhost';
DROP USER IF EXISTS 'monitor'@'%';
