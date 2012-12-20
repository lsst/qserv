CREATE DATABASE qservMeta;
GRANT ALL ON qservMeta.* TO 'qsmaster'@'localhost';
use qservMeta;
CREATE TABLE LSST__Object (
  objectId bigint(20) NOT NULL,
  x_chunkId int(11),
  x_subChunkId int(11),
  PRIMARY KEY ( objectId )
) ENGINE = InnoDB;
