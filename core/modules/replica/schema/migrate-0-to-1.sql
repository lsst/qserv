--
-- Migration script from version 0 to version 1 of qservReplica database:
--   - QMetadata table is added to keep schema version
--   - database_ingest table added to store a parameters of the catalog ingests
--

-- -----------------------------------------------------
-- Table `database_ingest`
-- -----------------------------------------------------
--
-- Store a parameters and a state of the catalog ingest.

CREATE TABLE IF NOT EXISTS `database_ingest` (

  `database` VARCHAR(255) NOT NULL ,
  `category` VARCHAR(255) NOT NULL ,
  `param`    VARCHAR(255) NOT NULL ,
  `value`    TEXT NOT NULL ,

  PRIMARY KEY (`database`, `category`, `param`) ,

  CONSTRAINT `database_ingest_fk_1`
    FOREIGN KEY (`database` )
    REFERENCES `config_database` (`database` )
    ON DELETE CASCADE
    ON UPDATE CASCADE
)
ENGINE = InnoDB ;
