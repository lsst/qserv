CREATE USER IF NOT EXISTS '{{MYSQLD_USER_QSERV}}'@'localhost';

-- Used by xrootd Qserv plugin:
-- to publish LSST databases and chunks

DROP DATABASE IF EXISTS qservw_worker;
CREATE DATABASE qservw_worker;

GRANT SELECT ON qservw_worker.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';

CREATE TABLE qservw_worker.Dbs (

  `db` CHAR(200) NOT NULL,

  PRIMARY KEY (`db`)

) ENGINE=InnoDB;

CREATE TABLE qservw_worker.Chunks (

  `db`    CHAR(200)    NOT NULL,
  `chunk` INT UNSIGNED NOT NULL,

  UNIQUE KEY(`db`,`chunk`)

) ENGINE=InnoDB;

CREATE TABLE qservw_worker.Id (

  `id`      VARCHAR(64)  NOT NULL,
  `type`    ENUM('UUID') DEFAULT 'UUID',
  `created` TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

  UNIQUE KEY (`type`)

) ENGINE=InnoDB;

INSERT INTO qservw_worker.Id (`id`) VALUES (UUID());


GRANT ALL ON `q\_memoryLockDb`.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';

-- Subchunks databases
GRANT ALL ON `Subchunks\_%`.* TO '{{MYSQLD_USER_QSERV}}'@'localhost';


-- Create user for external monitoring applications
CREATE USER IF NOT EXISTS '{{MYSQLD_USER_MONITOR}}'@'localhost' IDENTIFIED BY '{{MYSQLD_PASSWORD_MONITOR}}';
GRANT PROCESS ON *.* TO '{{MYSQLD_USER_MONITOR}}'@'localhost';
