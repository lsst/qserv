
-- -----------------------------------------------------
-- Schema qservCssData
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `qservCssData` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
USE `qservCssData`;

-- -----------------------------------------------------
-- Table `qservCssData`.`kvData`
-- -----------------------------------------------------
CREATE TABLE `qservCssData`.`kvData` (
  `kvId` INT NOT NULL AUTO_INCREMENT,
  `kvKey` CHAR(255) NOT NULL COMMENT 'slash delimited path',
  `kvVal` TEXT(1024) NOT NULL COMMENT 'value for key',
  `parentKvId` INT NULL COMMENT 'id of parent key',
  PRIMARY KEY (`kvId`),
  CONSTRAINT `kvData_parent_key`
    FOREIGN KEY (parentKvId)
    REFERENCES kvData(kvId),
  UNIQUE INDEX `kvData_kvKey_UNIQUE` (`kvKey` ASC)
) ENGINE = InnoDB;
