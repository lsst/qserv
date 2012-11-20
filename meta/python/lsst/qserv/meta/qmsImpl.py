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
import random
import StringIO
import tempfile
import uuid

from qmsMySQLDb import QmsMySQLDb
from status import Status


###############################################################################
#### installMeta
###############################################################################
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
    if ret != Status.SUCCESS: return ret
    for t in internalTables:
        mdb.createTable(t[0], t[1])
    return mdb.disconnect()

###############################################################################
#### destroyMeta
###############################################################################
def destroyMeta(loggerName):
    """This method permanently destroys qserv metadata"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS:
        return ret
    qmsDbs = mdb.execCommandN(
        "SHOW DATABASES LIKE '%s%%'" % mdb.getServerPrefix())
    for qmsDb in qmsDbs:
        mdb.execCommand0("DROP DATABASE %s" % qmsDb)
    mdb.dropDb()
    return mdb.disconnect()

###############################################################################
#### printMeta
###############################################################################
def printMeta(loggerName):
    """This method prints all metadata into a string"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        return None
    s = StringIO.StringIO()
    for t in ["DbMeta", "PS_Db_sphBox", "TableMeta", "PS_Tb_sphBox", 
              "EmptyChunks", "TableStats", "LockDb"]:
        _printTable(s, mdb, t)
    mdb.disconnect()
    return s.getvalue()

###############################################################################
#### createDb
###############################################################################
def createDb(loggerName, dbName, crDbOptions):
    """Creates metadata about new database to be managed by qserv."""
    logger = logging.getLogger(loggerName)
    # connect to QMS
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        logger.error("Failed to connect to qms")
        return None
    # check if db does not exit
    cmd = "SELECT COUNT(*) FROM DbMeta WHERE dbName = '%s'" % dbName
    ret = mdb.execCommand1(cmd)
    if ret[0] > 0:
        logger.error("Database '%s' already registered" % dbName)
        return Status.ERR_DB_EXISTS
    # create entry in PS_Db_<partitioningStrategy> table
    if crDbOptions["partitioning"] == "off":
        psId = '\N'
        psName = None
    else:
        psName = crDbOptions["partitioningStrategy"]
        if psName == "sphBox":
            logger.debug("persisting for sphBox")
            nS = crDbOptions["nStripes"]
            nSS = crDbOptions["nSubStripes"]
            dOvF = crDbOptions["defaultOverlap_fuzziness"]
            dOvN = crDbOptions["defaultOverlap_nearNeighbor"]
            cmd = "INSERT INTO PS_Db_sphBox(stripes, subStripes, defaultOverlap_fuzzyness, defaultOverlap_nearNeigh) VALUES(%s, %s, %s, %s)" % (nS, nSS, dOvF, dOvN)
            mdb.execCommand0(cmd)
            psId = (mdb.execCommand1("SELECT LAST_INSERT_ID()"))[0]
            if not psId:
                logger.error("Failed to run '%s'" % cmd)
                return Status.ERR_INTERNAL
        else:
            logger.error("Invalid psName: %s" % psName)
            return Status.ERR_INTERNAL
    # create entry in DbMeta table
    dbUuid = uuid.uuid4() # random UUID
    cmd = "INSERT INTO DbMeta(dbName, dbUuid, psName, psId) VALUES('%s', '%s', '%s', %s)" % (dbName, dbUuid, psName, psId)
    mdb.execCommand0(cmd)
    # finally, create this table as template
    mdb.execCommand0("CREATE DATABASE %s%s" % (mdb.getServerPrefix(), dbName))

    return mdb.disconnect()

###############################################################################
#### dropDb
###############################################################################
def dropDb(loggerName, dbName):
    """Drops metadata about a database managed by qserv."""
    logger = logging.getLogger(loggerName)
    # connect to mysql
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        logger.error("Failed to connect to qms")
        return None
    # check if db exists
    cmd = "SELECT COUNT(*) FROM DbMeta WHERE dbName = '%s'" % dbName
    ret = mdb.execCommand1(cmd)
    if ret[0] != 1:
        logger.error("Database '%s' not registered" % dbName)
        return Status.ERR_DB_NOT_EXISTS
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
    mdb.execCommand0("DROP DATABASE %s%s" % (mdb.getServerPrefix(), dbName))
    return mdb.disconnect()

###############################################################################
#### retrieveDbInfo
###############################################################################
def retrieveDbInfo(loggerName, dbName):
    """Retrieves info about a database"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        return [ret, {}]
    if mdb.execCommand1("SELECT COUNT(*) FROM DbMeta WHERE dbName='%s'" % \
                            dbName)[0] == 0:
        return [Status.ERR_DB_NOT_EXISTS, {}]
    ret = mdb.execCommand1("SELECT dbId, dbUuid, psName FROM DbMeta WHERE dbName='%s'" % dbName)
    values = dict()
    values["dbId"] = ret[0]
    values["dbUuid"] = ret[1]
    ps = ret[2]
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
    ret = mdb.disconnect()
    return [ret, values]

###############################################################################
#### checkDbExists
###############################################################################
def checkDbExists(loggerName, dbName):
    """Checks if db <dbName> exists, returns 0 or 1"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        return 0
    ret = mdb.execCommand1("SELECT COUNT(*) FROM DbMeta WHERE dbName='%s'" \
                               % dbName)
    mdb.disconnect()
    return ret[0]

###############################################################################
#### listDbs
###############################################################################
def listDbs(loggerName):
    """Prints names of all databases managed by qserv into a string"""
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
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

###############################################################################
#### createTable
###############################################################################
def createTable(loggerName, dbName, crTbOptions, schemaStr):
    """Creates metadata about new table in qserv-managed database."""
    logger = logging.getLogger(loggerName)

    # write schema to a temp file
    schemaF = tempfile.NamedTemporaryFile(delete=False)
    schemaF.write(schemaStr)
    logger.debug("wrote schema to tempfile %s" % schemaF.name)
    schemaF.close()

    # connect to QMS
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        os.unlink(schemaF.name)
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
        os.unlink(schemaF.name)
        logger.error("Table '%s' already registred" % tableName)
        return Status.ERR_TABLE_EXISTS

    # load the template schema
    mdb.loadSqlScript(schemaF.name, "%s%s" % (mdb.getServerPrefix(), dbName))
    os.unlink(schemaF.name)

    # create entry in PS_Tb_<partitioningStrategy>
    if crTbOptions["partitioning"] == "off":
        psId = '\N'
        psName = None
    else:
        psName = crTbOptions["partitioningStrategy"]
    if psName == "sphBox":
        # add two special columns
        cmd = "ALTER TABLE %s%s.%s ADD COLUMN chunk BIGINT, ADD COLUMN subChunk BIGINT" % (mdb.getServerPrefix(), dbName, tableName)
        mdb.execCommand0(cmd)

        logger.debug("persisting for sphBox")
        ov = crTbOptions["overlap"]
        pCN = crTbOptions["phiColName"]
        tCN = crTbOptions["thetaColName"]
        if not _checkColumnExists(mdb, dbName, tableName, pCN, loggerName) or \
           not _checkColumnExists(mdb, dbName, tableName, tCN, loggerName):
            return Status.ERR_COL_NOT_FOUND
        pN = _getColumnPos(mdb, dbName, tableName, pCN)
        tN = _getColumnPos(mdb, dbName, tableName, tCN)
        lP = int(crTbOptions["logicalPart"])
        pC = int(crTbOptions["physChunking"], 16)
        cmd = "INSERT INTO PS_Tb_sphBox(overlap, phiCol, thetaCol, phiColNo, thetaColNo, logicalPart, physChunking) VALUES(%s, '%s', '%s', %d, %d, %d, %d)" % (ov, pCN, tCN, pN, tN, lP, pC)
        mdb.execCommand0(cmd)
        psId = (mdb.execCommand1("SELECT LAST_INSERT_ID()"))[0]
    # create entry in TableMeta
    tbUuid = uuid.uuid4() # random UUID
    clusteredIdx = crTbOptions["clusteredIndex"]
    if clusteredIdx == "None":
        cmd = "INSERT INTO TableMeta(tableName, tbUuid, dbId, psId) VALUES ('%s', '%s', %s, %s)" % (tableName, tbUuid, dbId, psId)
    else:
        cmd = "INSERT INTO TableMeta(tableName, tbUuid, dbId, psId, clusteredIdx) VALUES ('%s', '%s', %s, %s, '%s')" % (tableName, tbUuid, dbId, psId, clusteredIdx)
    mdb.execCommand0(cmd)
    return mdb.disconnect()

###############################################################################
#### dropTable
###############################################################################
def dropTable(loggerName, dbName, tableName):
    """Drops metadata about a table.."""
    logger = logging.getLogger(loggerName)
    logger.debug("dropTable: started")
    # connect to mysql
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        logger.error("dropTable: failed to connect to qms")
        return None
    # check if db exists
    cmd = "SELECT COUNT(*) FROM DbMeta WHERE dbName = '%s'" % dbName
    ret = mdb.execCommand1(cmd)
    if ret[0] != 1:
        logger.error("dropTable: database '%s' not registered" % dbName)
        return Status.ERR_DB_NOT_EXISTS
    # get dbId, psName, psId
    cmd = "SELECT dbId, psName, psId FROM DbMeta WHERE dbName = '%s'" % dbName
    (dbId, psName, dbPsId) = mdb.execCommand1(cmd)
    # check if table exists
    cmd = "SELECT tableId FROM TableMeta WHERE dbId=%s AND tableName='%s'" % \
        (dbId, tableName)
    tableId = mdb.execCommand1(cmd)
    if not tableId:
        logger.error("dropTable: table '%s' does not exist." % tableName)
        return Status.ERR_TABLE_NOT_EXISTS
    # remove the entry about the table
    cmd = "DELETE FROM TableMeta WHERE tableId = %s" % tableId
    mdb.execCommand0(cmd)
    # remove related info
    if psName == 'sphBox':
        cmd = "DELETE FROM PS_Tb_sphBox WHERE psId IN (SELECT psId FROM TableMeta WHERE TableId=%s)" % tableId
        mdb.execCommand0(cmd)
    cmd = "DELETE FROM TableMeta WHERE tableId = %s" % tableId
    mdb.execCommand0(cmd)
    ret = mdb.disconnect()
    logger.debug("dropTable: done")
    return ret

###############################################################################
#### retrievePartTables
###############################################################################
def retrievePartTables(loggerName, dbName):
    """Retrieves list of partitioned tables for a given database."""
    logger = logging.getLogger(loggerName)
    logger.debug("retrievePartTables: started")
    # connect to mysql
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        logger.error("retrievePartTables: failed to connect to qms")
        return [ret, []]
    # check if db exists
    cmd = "SELECT dbId FROM DbMeta WHERE dbName = '%s'" % dbName
    ret = mdb.execCommand1(cmd)
    if not ret:
        logger.error("retrievePartTables: database '%s' not registered"%dbName)
        return [Status.ERR_DB_NOT_EXISTS, []]
    dbId = ret[0]
    cmd = "SELECT tableName FROM TableMeta WHERE dbId=%s " % dbId + \
           "AND psId IS NOT NULL"
    tNames = mdb.execCommandN(cmd)
    mdb.disconnect()
    logger.debug("retrieveTableInfo: done")
    tNamesCleaned = []
    for tn in tNames:
        tNamesCleaned.append(tn[0])
    return [Status.SUCCESS, tNamesCleaned]

###############################################################################
#### retrieveTableInfo
###############################################################################
def retrieveTableInfo(loggerName, dbName, tableName):
    """Retrieves metadata about a table.."""
    logger = logging.getLogger(loggerName)
    logger.debug("retrieveTableInfo: started")
    # connect to mysql
    mdb = QmsMySQLDb(loggerName)
    ret = mdb.connect()
    if ret != Status.SUCCESS: 
        logger.error("retrieveTableInfo: failed to connect to qms")
        return [ret, {}]
    # check if db exists
    cmd = "SELECT dbId FROM DbMeta WHERE dbName = '%s'" % dbName
    ret = mdb.execCommand1(cmd)
    if not ret:
        logger.error("retrieveTableInfo: database '%s' not registered"% dbName)
        return [Status.ERR_DB_NOT_EXISTS, {}]
    dbId = ret[0]
    # check if table exists
    cmd = "SELECT tableId FROM TableMeta WHERE dbId=%s AND tableName='%s'" % \
        (dbId, tableName)
    tableId = mdb.execCommand1(cmd)
    if not tableId:
        logger.error("retrieveTableInfo: table '%s' doesn't exist."% tableName)
        return [Status.ERR_TABLE_NOT_EXISTS, {}]
    # get ps name
    cmd = "SELECT psName FROM DbMeta WHERE dbId=%s" % dbId
    psName = mdb.execCommand1(cmd)[0]
    # the partitioning might be turned off for this table, so check it
    cmd = "SELECT psId FROM TableMeta WHERE tableId=%s" % tableId
    ret = mdb.execCommand1(cmd)
    if ret: psId = ret[0]
    # retrieve table info
    values = dict()
    if psId and psName == "sphBox":
        ret = mdb.execCommand1("SELECT clusteredIdx, overlap, phiCol, thetaCol, phiColNo, thetaColNo, logicalPart, physChunking FROM TableMeta JOIN PS_Tb_sphBox USING(psId) WHERE tableId=%s" % tableId)
        values["clusteredIdx"] = ret[0]
        values["overlap"]      = ret[1]
        values["phiCol"]       = ret[2]
        values["thetaCol"]     = ret[3]
        values["phiColNo"]     = ret[4]
        values["thetaColNo"]   = ret[5]
        values["logicalPart"]  = ret[6]
        values["physChunking"] = hex(ret[7])
    else:
        ret = mdb.execCommand1(
            "SELECT clusteredIdx FROM TableMeta WHERE tableId=%s" % tableId)
        values["clusteredIdx"] = ret
    ret = mdb.disconnect()
    logger.debug("retrieveTableInfo: done")
    return [ret, values]


###############################################################################
#### getInternalQmsDbName
###############################################################################
def getInternalQmsDbName(loggerName):
    """Retrieves name of the internal qms database. """
    mdb = QmsMySQLDb(loggerName)
    dbName = mdb.getDbName()
    return (dbName is not None, dbName)

###############################################################################
#### _checkColumnExists
###############################################################################
def _checkColumnExists(mdb, dbName, tableName, columnName, loggerName):
    # note: this function is mysql-specific!
    ret = mdb.execCommand1("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE table_schema='%s%s' and table_name='%s' and column_name='%s'" % (mdb.getServerPrefix(), dbName, tableName, columnName))
    if ret[0] != 1:
        logger = logging.getLogger(loggerName)
        logger.error("column: '%s' not found in table '%s.%s'" % \
                         (columnName, dbName, tableName))
        return False
    return True

###############################################################################
#### _printTable
###############################################################################
def _printTable(s, mdb, tableName):
    ret = mdb.execCommandN("SELECT * FROM %s" % tableName)
    s.write(tableName)
    if len(ret) == 0:
        s.write(" is empty.\n")
    else: 
        s.write(':\n')
        for r in ret: print >> s, "   ", r

def _getColumnPos(mdb, dbName, tableName, columnName):
    # note: this function is mysql-specific!
    return mdb.execCommand1("SELECT ordinal_position FROM information_schema.COLUMNS WHERE table_schema='%s%s' and table_name='%s' and column_name='%s'" % (mdb.getServerPrefix(), dbName, tableName, columnName))[0] -1 









