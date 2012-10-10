#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import logging

from metaMySQLDb import MetaMySQLDb

from lsst.qserv.master import config


class Meta():
    def __init__(self):
        self._loggerName = "qsMetaLogger"

        self._initLogging()

        self.internalTables = [
            # The DbMeta table keeps the list of databases managed through 
            # qserv. Databases not entered into that table will be ignored 
            # by qserv.
            ['DbMeta', '''(
   dbId INT NOT NULL PRIMARY KEY,
   dbName VARCHAR(255) NOT NULL,
   dbUuid VARCHAR(255) NOT NULL, -- note: this has to be the same across 
                                 -- all worker nodes

   psName VARCHAR(255), -- partition strategy used for tables
                        -- in this database. Must be the same for all tables
                        -- supported so far: "sphBox". This name is used
                        -- to determine which PartitioningStrategy_DbLevel_*
                        -- and PartitioningStrategy_TbLevel_* tables to use
   psId INT             -- foreign key to the PartitioningStrategy_DbLevel_*
                        -- and PartitioningStrategy_TbLevel_* tables 
)'''],

            # TableMeta table defines table-specific metadata.
            # This metadata is data-independent
            ["TableMeta", '''(
   tableId INT NOT NULL PRIMARY KEY,
   tableName VARCHAR(255) NOT NULL,
   tbUuid VARCHAR(255),       -- uuid of this table
   dbId INT NOT NULL,         -- id of database this table belongs to

   clusteredIdx VARCHAR(255)  -- name of the clustered index, 
                              -- Null if no clustered index.
)'''],

            # Partitioning strategy, database-specific parameters 
            # for sphBox partitioning
            ["PS_Db_sphBox", '''(
   psId INT NOT NULL PRIMARY KEY,
   stripes INT,    -- base number of stripes. 
                   -- Large tables might overwrite that and have 
                   -- finer-grain division.
   subStripes INT, -- base number of subStripes per stripe.
                   -- Large tables might overwrite that and have 
                   -- finer-grain division.
   defaultOverlap_fuzzyness FLOAT, -- in degrees, for fuzziness
   defaultOverlap_nearNeigh FLOAT  -- in degrees, for real neighbor query
)'''],

            # Partitioning strategy, table-specific parameters 
            # for sphBox partitioning
            ["PS_Tb_sphBox", '''(
   psId INT NOT NULL PRIMARY KEY,
   overlap FLOAT          -- in degrees, 0 if not set
   phiCol VARCHAR(255),   -- Null if table not partitioned
   thetaCol VARCHAR(255), -- Null if table not partitioned
   phiColNo INT,          -- Position of the phiColumn in the table, 
                          -- counting from zero
   thetaColNo INT,        -- Position of the thetaColumn in the table, 
                          -- counting from zero
   logicalPart SMALLINT,  -- logical partitioning flag:
                          -- 0x1000 - no chunks
                          -- 0x2000 - one-level chunking
                          -- 0x3000 - two-level chunking (chunks and subchunks)

   physChunking INT       -- physical storage flag:
                          -- 0x1001 - for "no chunking": db rows/columns

                          -- 0x2000 - for 1st level chunking: not persisted
                          -- 0x2001 - for 1st level chunking: db rows/columns

                          -- 0x3000 - for 2nd level chunking: not persisted
                          -- 0x3001 - for 2nd level chunking: db rows/columns
)'''],
            ["EmptyChunks", '''(
   dbId INT,
   chunkId INT
)'''], 
            ["TableStats", '''(
   tableId INT NOT NULL PRIMARY KEY,
   rowCount BIGINT,        -- row count. Doesn't have to be precise.
                           -- used for query cost estimates
   chunkCount INT,         -- count of all chunks
   subChunkCount INT,      -- count of all subchunks
   avgSubChunkCount FLOAT  -- average sub chunk count (per chunk)
)'''],
            ["LockDb", '''(
   dbId INT NOT NULL,
   locked INT,                 -- 1: locked, 0: unlocked for now
   lockKey BIGINT,             -- key required to bypass the lock
   lockedBy VARCHAR(255),      -- name of user who locked the database
   lockDate DATETIME,          -- date/time when lock was created
   estDur INT,                 -- estimated duration in hours 
                               -- (for message facing users, -1: unknown)
   comments TEXT DEFAULT NULL  -- any comments the lock creator wants 
                               -- to attach to this lock 
)'''],
            ["LockTable", '''(
   tableId INT NOT NULL,
   locked INT,                 -- 1: locked, 0: unlocked for now
   lockKey BIGINT,             -- key required to bypass the lock
   lockedBy VARCHAR(255),      -- name of user who locked the database
   lockDate DATETIME,          -- date/time when lock was created
   estDur INT,                 -- estimated duration in hours 
                               -- (for message facing users, -1: unknown)
   comments TEXT DEFAULT NULL  -- any comments the lock creator wants 
                               -- to attach to this lock 
)''']]

    def persistentInit(self):
        mdb = MetaMySQLDb(self._loggerName)
        mdb.connectAndCreateDb()
        for t in self.internalTables:
            mdb.createTable(t[0], t[1])
        mdb.disconnect()

    def _initLogging(self):
        config = lsst.qserv.master.config.config
        outF = config.get("logging", "outFile")
        levelName = config.get("logging", "level")
        if levelName is None:
            loggingLevel = logging.ERROR # default
        else:
            ll = {"debug":logging.DEBUG,
                  "info":logging.INFO,
                  "warning":logging.WARNING,
                  "error":logging.ERROR,
                  "critical":logging.CRITICAL}
            loggingLevel = ll(levelName)
        self.logger = logging.getLogger(self._loggerName)
        hdlr = logging.FileHandler(outFile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr) 
        self.logger.setLevel(loggingLevels[loggingLevelName])
