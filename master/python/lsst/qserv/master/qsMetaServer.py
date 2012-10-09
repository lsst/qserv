#!/usr/bin/env python

import MySQLdb

# Class used for initializing Qserv Metadata structure
# in the internal qserv metadata database
class QsMetaInit:
    def __init__(self):
        self.internalTables = [
            # This table keeps the list of databases managed through qserv. 
            # Shown when user calls "show databases". Databases not listed
            # here will be ignored by qserv.
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

            # metadata that defines table-specific metadata.
            # this metadata is data-independent
            ["TableMeta", '''(
   tableId INT NOT NULL PRIMARY KEY,
   tableName VARCHAR(255) NOT NULL,
   tbUuid VARCHAR(255),       -- uuid of this table
   dbId INT NOT NULL,         -- id of database this table belongs to

   clusteredIdx VARCHAR(255)  -- name of the clustered index, 
                              -- Null if no clustered index.
)'''],

            # partitioning strategy, database-specific parameters 
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

            # partitioning strategy, table-specific parameters 
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

    def tableExists(self, tName):
        print "checking if table %s exists" % tName

    def createTable(self, tName, tSchema):
        cmd = "CREATE TABLE %s %s" % (tName, tSchema)
        print cmd

    def initMeta(self):
        # make sure none of the internal tables exists
        for tt in self.internalTables:
            if self.tableExists(tt[0]):
                raise RuntimeError, ("Table %s exists" % tt[0])
        # create all internal tables
        for tt in self.internalTables:
            self.createTable(tt[0], tt[1])

m = QsMetaInit()
m.initMeta()
