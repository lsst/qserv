SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 ;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 ;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL' ;


CREATE DATABASE qservIngest;
USE qservIngest;

-- --------------------------------------------------------------
-- Table `task`
-- --------------------------------------------------------------
--
-- The list of chunks to load inside a Qserv database
-- Used as a queue by loader jobs
CREATE TABLE `chunkfile_queue` (

  `id`                    INTEGER UNSIGNED    NOT NULL AUTO_INCREMENT,
  `chunk_id`              INTEGER UNSIGNED    NOT NULL ,                  -- the id of the chunk to load
  `chunk_file_path`       VARCHAR(255)        NOT NULL ,                  -- the path of the chunk file to load
  `database`              VARCHAR(255)        NOT NULL ,                  -- the name of the target database
  `is_overlap`            BOOLEAN             NOT NULL ,                  -- is this file an overlap
  `locking_pod`           VARCHAR(255)        DEFAULT NULL,               -- the id of the latest pod which has locked the chunk
  `succeed`               BOOLEAN             NULL ,                      -- the status of the file:
                                                                          --   - NULL (pending),
                                                                          --   - 0 (error during latest ingest attempt),
                                                                          --   - 1 (success during latest ingest attempt)
  `table`                 VARCHAR(255)        NOT NULL ,                  -- the name of the target table

  PRIMARY KEY (`id`),
  UNIQUE KEY (`chunk_id`, `chunk_file_path`, `database`, `is_overlap`, `table`)
)
ENGINE = InnoDB;

