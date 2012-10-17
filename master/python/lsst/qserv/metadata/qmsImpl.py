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

import logging
import os
import StringIO
import uuid

from qmsMySQLDb import QmsMySQLDb
from qmsStatus import QmsStatus

################################################################################
#### installMeta
################################################################################
def installMeta(loggerName):
    """Initializes persistent qserv metadata structures.
    This method should be called only once ever for a given
    qms installation."""
    internalTables = [
        # The DbMeta table keeps the list of databases managed through 
        # qserv. Databases not entered into that table will be ignored 
        # by qserv.
        ['DbMeta', '''(
   dbId INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
   dbName VARCHAR(255) NOT NULL,
   dbUuid VARCHAR(255) NOT NULL, -- note: this has to be the same across 
                                 -- all worker nodes

   psName VARCHAR(255), -- partition strategy used for tables
                        -- in this database. Must be the same for all tables
                        -- supported so far: "sphBox". This name is used to
                        -- determine which PS_Db_* and PS_Tb_* tables to use
   psId INT             -- foreign key to the PS_Db_* table
)'''],
        # ---------------------------------------------------------------------
        # TableMeta table defines table-specific metadata.
        # This metadata is data-independent
        ["TableMeta", '''(
   tableId INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
   tableName VARCHAR(255) NOT NULL,
   tbUuid VARCHAR(255),       -- uuid of this table
   dbId INT NOT NULL,         -- id of database this table belongs to
   psId INT,                  -- foreign key to the PS_Tb_* table
   clusteredIdx VARCHAR(255)  -- name of the clustered index, 
                              -- Null if no clustered index.
)'''],
        # ---------------------------------------------------------------------
        # Partitioning strategy, database-specific parameters 
        # for sphBox partitioning
            ["PS_Db_sphBox", '''(
   psId INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
   stripes INT,    -- base number of stripes. 
                   -- Large tables might overwrite that and have 
                   -- finer-grain division.
   subStripes INT, -- base number of subStripes per stripe.
                   -- Large tables might overwrite that and have 
                   -- finer-grain division.
   defaultOverlap_fuzzyness FLOAT, -- in degrees, for fuzziness
   defaultOverlap_nearNeigh FLOAT  -- in degrees, for real neighbor query
)'''],
        # ---------------------------------------------------------------------
        # Partitioning strategy, table-specific parameters 
        # for sphBox partitioning
        ["PS_Tb_sphBox", '''(
   psId INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
   overlap FLOAT,         -- in degrees, 0 if not set
   phiCol VARCHAR(255),   -- Null if table not partitioned
   thetaCol VARCHAR(255), -- Null if table not partitioned
   phiColNo INT,          -- Position of the phiColumn in the table, 
                          -- counting from zero
   thetaColNo INT,        -- Position of the thetaColumn in the table, 
                          -- counting from zero
   logicalPart SMALLINT,  -- logical partitioning flag:
                          -- 0: no chunks
                          -- 1: one-level chunking
                          -- 2: two-level chunking (chunks and subchunks)
   physChunking INT       -- physical storage flag:                        
            -- least significant bit: 0-not persisted, 1-persisted in RDBMS
            -- second-least significant bit indicates partitioning level,eg
            -- 0x0010: 1st-level partitioning, not persistent
            -- 0x0011: 1st-level partitioning, persisted in RDBMS
            -- 0x0020: 2st-level partitioning, not persistent
            -- 0x0021: 2st-level partitioning, persisted in RDBMS
)'''],
        # ---------------------------------------------------------------------
        ["EmptyChunks", '''(
   dbId INT,
   chunkId INT
)'''], 
        # ---------------------------------------------------------------------
        ["TableStats", '''(
   tableId INT NOT NULL PRIMARY KEY,
   rowCount BIGINT,        -- row count. Doesn't have to be precise.
                           -- used for query cost estimates
   chunkCount INT,         -- count of all chunks
   subChunkCount INT,      -- count of all subchunks
   avgSubChunkCount FLOAT  -- average sub chunk count (per chunk)
)'''],
        # ---------------------------------------------------------------------
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
        # ---------------------------------------------------------------------
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
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connectAndCreateDb()
    if ret != QmsStatus.SUCCESS: return ret
    for t in internalTables:
        mdb.createTable(t[0], t[1])
    return mdb.disconnect()

################################################################################
#### destroyMeta
################################################################################
def destroyMeta(loggerName):
    """This method permanently destroys qserv metadata"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != QmsStatus.SUCCESS: return ret
    mdb.dropDb()
    return mdb.disconnect()

################################################################################
#### printMeta
################################################################################
def _printTable(s, mdb, tableName):
    ret = mdb.execCommandN("SELECT * FROM %s" % tableName)
    s.write(tableName)
    if len(ret) == 0:
        s.write(" is empty.\n")
    else: 
        s.write(':\n')
        for r in ret: print >> s, "   ", r

def printMeta(loggerName):
    """This method prints all metadata into a string"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != QmsStatus.SUCCESS: 
        return None
    s = StringIO.StringIO()
    for t in ["DbMeta", "PS_Db_sphBox", "TableMeta", "PS_Tb_sphBox", 
              "EmptyChunks", "TableStats", "LockDb"]:
        _printTable(s, mdb, t)
    mdb.disconnect()
    return s.getvalue()

################################################################################
#### createDb
################################################################################
def createDb(loggerName, dbName, crDbOptions):
    """Creates metadata about new database to be managed by qserv."""
    logger = logging.getLogger(loggerName)
    # connect to QMS
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != QmsStatus.SUCCESS: 
        logger.error("Failed to connect to qms")
        return None
    # check if db does not exit
    cmd = "SELECT COUNT(*) FROM DbMeta WHERE dbName = '%s'" % dbName
    ret = mdb.execCommand1(cmd)
    if ret[0] > 0:
        logger.error("Database '%s' already registered" % dbName)
        return QmsStatus.ERR_DB_EXISTS
    # create entry in PS_Db_<partitioningStrategy> table
    psName = crDbOptions["partitioningstrategy"]
    psId = '0'
    if psName == "sphBox":
        logger.debug("persisting for sphBox")
        nS = crDbOptions["nstripes"]
        nSS = crDbOptions["nsubstripes"]
        dOvF = crDbOptions["defaultoverlap_fuzziness"]
        dOvN = crDbOptions["defaultoverlap_nearneighbor"]
        cmd = "INSERT INTO PS_Db_sphBox(stripes, subStripes, defaultOverlap_fuzzyness, defaultOverlap_nearNeigh) VALUES(%s, %s, %s, %s)" % (nS, nSS, dOvF, dOvN)
        mdb.execCommand0(cmd)
        psId = (mdb.execCommand1("SELECT LAST_INSERT_ID()"))[0]
    elif psName == "None":
        pass
    # create entry in DbMeta table
    dbUuid = uuid.uuid4() # random UUID
    cmd = "INSERT INTO DbMeta(dbName, dbUuid, psName, psId) VALUES('%s', '%s', '%s', %s)" % (dbName, dbUuid, psName, psId)
    mdb.execCommand0(cmd)
    # finally, create this table as template
    mdb.execCommand0("CREATE DATABASE qms_%s" %dbName)

    return mdb.disconnect()

################################################################################
#### dropDb
################################################################################
def dropDb(loggerName, dbName):
    """Drops metadata about a database managed by qserv."""
    logger = logging.getLogger(loggerName)
    # connect to mysql
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != QmsStatus.SUCCESS: 
        logger.error("Failed to connect to qms")
        return None
    # check if db exists
    cmd = "SELECT COUNT(*) FROM DbMeta WHERE dbName = '%s'" % dbName
    ret = mdb.execCommand1(cmd)
    if ret[0] != 1:
        logger.error("Database '%s' not registered" % dbName)
        return QmsStatus.ERR_DB_NOT_EXISTS
    # get partitioningStrategy, psId and drop the entry
    cmd = "SELECT dbId, psName, psId FROM DbMeta WHERE dbName = '%s'" % dbName
    (dbId, psName, dbPsId) = mdb.execCommand1(cmd)
    if psName == 'sphBox':
        cmd = "DELETE FROM PS_Db_sphBox WHERE psId = %s " % dbPsId
        mdb.execCommand0(cmd)
    # remove the entry about the db
    cmd = "DELETE FROM DbMeta WHERE dbId = %s" % dbId
    mdb.execCommand0(cmd)
    # remove related tables
    if psName == 'sphBox':
        cmd = "DELETE FROM PS_Tb_sphBox WHERE psId IN (SELECT psId FROM TableMeta WHERE dbId=%s)" % dbId
        mdb.execCommand0(cmd)
    cmd = "DELETE FROM TableMeta WHERE dbId = %s" % dbId
    mdb.execCommand0(cmd)
    # drop the template database
    mdb.execCommand0("DROP DATABASE qms_%s" % dbName)
    return mdb.disconnect()

################################################################################
#### retrieveDbInfo
################################################################################
def retrieveDbInfo(loggerName, dbName):
    """Retrieves info about a database"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != QmsStatus.SUCCESS: 
        return [ret, {}]
    if mdb.execCommand1("SELECT COUNT(*) FROM DbMeta WHERE dbName='%s'" % \
                            dbName)[0] == 0:
        return [QmsStatus.ERR_DB_NOT_EXISTS, {}]
    ps = mdb.execCommand1("SELECT psName FROM DbMeta WHERE dbName='%s'" % \
                              dbName)[0]
    values = dict()
    values["partitioningStrategy"] = ps
    if ps == "sphBox":
        ret = mdb.execCommand1("""
          SELECT stripes, subStripes, defaultOverlap_fuzzyness, 
                 defaultOverlap_nearNeigh
          FROM DbMeta 
          JOIN PS_Db_sphBox USING(psId) 
          WHERE dbName='%s'""" % dbName)
        values["stripes"] = ret[0]
        values["subStripes"] = ret[1]
        values["defaultOverlap_fuzziness"] = ret[2]
        values["defaultOverlap_nearNeigh"] = ret[3]
    elif ps == "None":
        pass
    mdb.disconnect()
    return [QmsStatus.SUCCESS, values]

################################################################################
#### listDbs
################################################################################
def listDbs(loggerName):
    """Prints names of all databases managed by qserv into a string"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != QmsStatus.SUCCESS: 
        return None
    ret = mdb.execCommandN("SELECT dbName FROM DbMeta")
    if not ret:
        return "No databases found"
    s = StringIO.StringIO()
    for r in ret:
        s.write(r[0])
        s.write(' ')
    mdb.disconnect()
    return s.getvalue()

################################################################################
#### checkDbExists
################################################################################
def checkDbExists(loggerName, dbName):
    """Checks if db <dbName> exists, returns 0 or 1"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != QmsStatus.SUCCESS: 
        return 0
    ret = mdb.execCommand1("SELECT COUNT(*) FROM DbMeta WHERE dbName='%s'" \
                               % dbName)
    mdb.disconnect()
    return ret[0]

################################################################################
#### createTable
################################################################################
def createTable(loggerName, dbName, crTbOptions):
    """Creates metadata about new table in qserv-managed database."""
    logger = logging.getLogger(loggerName)

    # check if schema file can be read
    schemaFile = crTbOptions["schemaFile"]
    if not os.access(schemaFile, os.R_OK):
        logger.error("Failed to open schema file '%s'" % schemaFile)
        return QmsStatus.ERR_SCHEMA_FILE
    # connect to QMS
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != QmsStatus.SUCCESS: 
        logger.error("Failed to connect to qms")
        return ret
    # get dbid
    dbId = (mdb.execCommand1("SELECT dbId FROM DbMeta WHERE dbName = '%s'" % \
                                 dbName))[0]
    # check if the table already exists
    tableName = crTbOptions["tableName"]
    cmd = "SELECT COUNT(*) FROM TableMeta WHERE dbId=%s AND tableName='%s'" % \
        (dbId, tableName)
    ret = mdb.execCommand1(cmd)
    if ret[0] > 0:
        logger.error("Table '%s' already registred" % tableName)
        return QmsStatus.ERR_TABLE_EXISTS

    # load the template schema
    print "FIXME in createTable: need to load schema (file: %s)" % schemaFile

    # create entry in PS_Tb_<partitioningStrategy>
    psName = crTbOptions["partitioningStrategy"]
    psId = '0'
    if psName == "sphBox":
        logger.debug("persisting for sphBox")
        ov = crTbOptions["overlap"]
        pCN = crTbOptions["phiColName"]
        tCN = crTbOptions["thetaColName"]
        pN = 123 # fixme
        tN = 456 # fixme
        print "FIXME in createTable: need to calculate phiColNumber and thetaColNumber"
        lP = int(crTbOptions["logicalPart"])
        pC = int(crTbOptions["physChunking"], 16)
        cmd = "INSERT INTO PS_Tb_sphBox(overlap, phiCol, thetaCol, phiColNo, thetaColNo, logicalPart, physChunking) VALUES(%s, '%s', '%s', %d, %d, %d, %d)" % (ov, pCN, tCN, pN, tN, lP, pC)
        mdb.execCommand0(cmd)
        psId = (mdb.execCommand1("SELECT LAST_INSERT_ID()"))[0]
    elif ps == "None":
        pass
    # create entry in TableMeta
    tbUuid = uuid.uuid4() # random UUID
    clusteredIdx = crTbOptions["clusteredIndex"]
    cmd = "INSERT INTO TableMeta(tableName, tbUuid, dbId, psId, clusteredIdx) VALUES ('%s', '%s', %s, %s, '%s')" % (tableName, tbUuid, dbId, psId, clusteredIdx)
    mdb.execCommand0(cmd)
    return mdb.disconnect()
