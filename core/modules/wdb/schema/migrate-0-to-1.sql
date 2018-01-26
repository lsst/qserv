--
-- Migration script from version 0 to version 1 of the qservw_worker database:
--
--  -  add the PRIMARY key constraint for column 'db' of table 'Dbs'
--  -  add table 'Chunks' table and populate it with chunks known to the worker for
--     all databases registered in table 'Dbs'
--  -  add table 'Id' and populate it with the default worker identity (based on UUID)
--  -  add table 'QMetadata' and populate it with the initial schema version
--

-- Add the PRIMARY key constrain to the existing table

ALTER TABLE `Dbs` ADD CONSTRAINT PRIMARY KEY (`db`);

-- -----------------------------------------------------
-- Create table `Chunks`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `Chunks` (

  `db`    CHAR(200)    NOT NULL,
  `chunk` INT UNSIGNED NOT NULL,

  UNIQUE KEY(`db`,`chunk`)

) ENGINE=InnoDB;

-- Find all chunks for registered databases and put them into table 'Chunks'

DELIMITER $$

CREATE PROCEDURE PopulateChunks()
BEGIN
  DECLARE dbDone INT          DEFAULT 0;
  DECLARE db     CHAR(200)    DEFAULT "";
  DECLARE chunk  INT UNSIGNED DEFAULT 1;

  DECLARE curs CURSOR      FOR SELECT * FROM Dbs;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET dbDone = 1;

  OPEN curs;

  DELETE FROM Chunks;

  a: LOOP

    FETCH curs INTO db;
    IF dbDone = 1 THEN
      LEAVE a;
    END IF;
    
    INSERT INTO Chunks
      SELECT DISTINCT TABLE_SCHEMA,SUBSTR(TABLE_NAME,POSITION('_' IN TABLE_NAME)+1)
        FROM information_schema.tables
        WHERE TABLE_SCHEMA=db AND TABLE_NAME REGEXP '_[0-9]*$';
  
  END LOOP a;

  CLOSE curs;
END$$

DELIMITER ;

CALL PopulateChunks();

DROP PROCEDURE PopulateChunks;

-- -----------------------------------------------------
-- Create table `Id`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `Id` (

  `id`      VARCHAR(64)  NOT NULL,
  `type`    ENUM('UUID') DEFAULT 'UUID',
  `created` TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

  UNIQUE KEY (`type`)

) ENGINE=InnoDB;

-- Add a record with a unique identifier of a worker

INSERT INTO Id (`id`) VALUES (UUID());

-- -----------------------------------------------------
-- Create table `QMetadata`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `QMetadata` (

  `metakey` CHAR(64) NOT NULL COMMENT 'Key string',
  `value`   TEXT         NULL COMMENT 'Value string',

  PRIMARY KEY (`metakey`)

) ENGINE = InnoDB COMMENT = 'Metadata about database as a whole, key-value pairs';

-- Add record for schema version, migration script expects this record to exist

INSERT INTO `QMetadata` (`metakey`, `value`) VALUES ('version', '1');

