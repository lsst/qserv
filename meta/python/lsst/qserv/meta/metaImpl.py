#!/usr/bin/env python

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
# Implementation of the qserv metadata server.


import logging
import os
import random
import StringIO
import tempfile
import uuid

import config
from db import Db
from status import Status, getErrMsg, QmsException

class MetaImpl:
    def __init__(self, loggerName):
        # init logger
        self._logger = logging.getLogger(loggerName)
        # get connection info
        c = config.config
        socket = c.get("qmsdb", "unix_socket")
        user = c.get("qmsdb", "user")
        passwd = c.get("qmsdb", "passwd")
        host = c.get("qmsdb", "host")
        port = c.getint("qmsdb", "port")
        dbName = "qms_%s" % c.get("qmsdb", "db")
        # prep db object
        self._mdb = Db(loggerName, host, port, user, passwd, socket, dbName)

    def __destroy__(self):
        self._mdb.disconnect()

    ###########################################################################
    #### installMeta
    ###########################################################################
    def installMeta(self):
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
            # -----------------------------------------------------------------
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
            # -----------------------------------------------------------------
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
            # -----------------------------------------------------------------
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
            # -----------------------------------------------------------------
            ["EmptyChunks", '''(
   dbId INT,
   chunkId INT
)'''], 
            # -----------------------------------------------------------------
            ["TableStats", '''(
   tableId INT NOT NULL PRIMARY KEY,
   rowCount BIGINT,        -- row count. Doesn't have to be precise.
                           -- used for query cost estimates
   chunkCount INT,         -- count of all chunks
   subChunkCount INT,      -- count of all subchunks
   avgSubChunkCount FLOAT  -- average sub chunk count (per chunk)
)'''],
            # -----------------------------------------------------------------
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
            # -----------------------------------------------------------------
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
        self._mdb.createMetaDb()
        self._mdb.selectMetaDb()
        for t in internalTables:
            self._mdb.createTable(t[0], t[1])
        self._mdb.commit()

    ###########################################################################
    #### destroyMeta
    ###########################################################################
    def destroyMeta(self):
        """This method permanently destroys qserv metadata"""
        self._mdb.selectMetaDb()
        cmd = "SHOW DATABASES LIKE '%s%%'" % self._mdb.getServerPrefix() 
        qmsDbs = self._mdb.execCommandN(cmd)
        for qmsDb in qmsDbs:
            self._mdb.execCommand0("DROP DATABASE %s" % qmsDb)
        self._mdb.dropMetaDb()
        self._mdb.commit()

    ###########################################################################
    #### printMeta
    ###########################################################################
    def printMeta(self):
        """This method prints all metadata into a string"""
        self._mdb.selectMetaDb()
        s = StringIO.StringIO()
        for t in ["DbMeta", "PS_Db_sphBox", "TableMeta", "PS_Tb_sphBox", 
                  "EmptyChunks", "TableStats", "LockDb"]:
            self._printTable(s, t)
        return s.getvalue()

    ###########################################################################
    #### createDb
    ###########################################################################
    def _validateKVOptions(self, x, xxOpts, psOpts, whichInfo):
        if not x.has_key("partitioning"):
            self._logger.error("Can't find required param 'partitioning'")
            raise QmsException(Status.ERR_INVALID_OPTION)

        partOff = x["partitioning"] == "off" 
        for (theName, theOpts) in xxOpts.items():
            for o in theOpts:
                # skip optional parameters
                if o == "partitioning":
                    continue
                # if partitioning is "off", partitioningStrategy does not 
                # need to be specified 
                if not (o == "partitiongStrategy" and partOff):
                    continue
                if not x.has_key(o):
                    self._logger.error("Can't find required param '%s'" % o)
                    raise QmsException(Status.ERR_INVALID_OPTION)
        if partOff:
            return
        if x["partitioning"] != "on":
            self._logger.error("Unrecognized value for param 'partitioning' "
                               "(%s), supported on/off" % x["partitioning"])
            raise QmsException(Status.ERR_INVALID_OPTION)
        if not x.has_key("partitioningStrategy"):
            self._logger.error("partitioningStrategy option is required if "
                               "partitioning is on")
            raise QmsException(Status.ERR_INVALID_OPTION)

        psFound = False
        for (psName, theOpts) in psOpts.items():
            if x["partitioningStrategy"] == psName:
                psFound = True
                # check if all required options are specified
                for o in theOpts:
                    if not x.has_key(o):
                        self._logger.error("Can't find param '%s' required for partitioning strategy '%s'" % (o, psName))
                        raise QmsException(Status.ERR_INVALID_OPTION)
                # check if there are any unrecognized options
                for o in x:
                    if not ((o in xxOpts[whichInfo]) or (o in theOpts)):
                        # skip non required, these are not in xxOpts/theOpts
                        if whichInfo=="db_info" and o=="clusteredIndex":
                            continue
                        if whichInfo=="table_info" and o=="partitioningStrategy":
                            continue
                        self._logger.error("Unrecognized param '%s' found" % o)
                        raise QmsException(Status.ERR_INVALID_OPTION)
        if not psFound:
            self._logger.error("Unrecongnized partitioning strategy '%s', supported strategies: 'sphBox'"% x["partitioningStrategy"])
            raise QmsException(Status.ERR_INVALID_OPTION)

    def _processDbOptions(self, opts):
        # add default values for missing parameters
        if not opts.has_key("clusteredIndex"):
            print("param 'clusteredIndex' not found, will use default: NULL")
            opts["clusteredIndex"] = "NULL"
        if not opts.has_key("partitioning"):
            print("param 'partitioning' not found, will use default: off")
            opts["partitioning"] = "off"
        # these are required options for createDb
        _crDbOpts = { "db_info":("partitioning", "partitioningStrategy")}
        _crDbPSOpts = {
            "sphBox":("nStripes", 
                      "nSubStripes", 
                      "defaultOverlap_fuzziness",
                      "defaultOverlap_nearNeighbor")}
        # validate the options
        self._validateKVOptions(opts, _crDbOpts, _crDbPSOpts, "db_info")
        return opts

    def createDb(self, dbName, crDbOptions):
        """Creates metadata about new database to be managed by qserv."""
        # connect to QMS
        self._mdb.selectMetaDb()
        # check if db exits
        cmd = "SELECT COUNT(*) FROM DbMeta WHERE dbName = '%s'" % dbName
        ret = self._mdb.execCommand1(cmd)
        if ret[0] > 0:
            self._logger.error("Database '%s' already registered" % dbName)
            raise QmsException(Status.ERR_DB_EXISTS)
        # add default values for missing parameters and do final validation
        # print "Dopts1:"
        # for k in crDbOptions: print "  ", k, "  --> ",crDbOptions[k]
        crDbOptions = self._processDbOptions(crDbOptions)
        # print "Dopts2:"
        # for k in crDbOptions: print "  ", k, "  --> ", crDbOptions[k]
        # create entry in PS_Db_<partitioningStrategy> table
        if crDbOptions["partitioning"] == "off":
            psId = '\N'
            psName = None
        else:
            psName = crDbOptions["partitioningStrategy"]
            if psName == "sphBox":
                self._logger.debug("persisting for sphBox")
                nS = crDbOptions["nStripes"]
                nSS = crDbOptions["nSubStripes"]
                dOvF = crDbOptions["defaultOverlap_fuzziness"]
                dOvN = crDbOptions["defaultOverlap_nearNeighbor"]
                cmd = "INSERT INTO PS_Db_sphBox(stripes, subStripes, defaultOverlap_fuzzyness, defaultOverlap_nearNeigh) VALUES(%s, %s, %s, %s)" % (nS, nSS, dOvF, dOvN)
                self._mdb.execCommand0(cmd)
                psId = (self._mdb.execCommand1("SELECT LAST_INSERT_ID()"))[0]
                if not psId:
                    self._logger.error("Failed to run '%s'" % cmd)
                    raise QmsException(Status.ERR_INTERNAL)
            else:
                self._logger.error("Invalid psName: %s" % psName)
                raise QmsException(Status.ERR_INTERNAL)
        # create entry in DbMeta table
        dbUuid = uuid.uuid4() # random UUID
        cmd = "INSERT INTO DbMeta(dbName, dbUuid, psName, psId) VALUES('%s', '%s', '%s', %s)" % (dbName, dbUuid, psName, psId)
        self._mdb.execCommand0(cmd)
        # finally, create this table as template
        self._mdb.execCommand0("CREATE DATABASE %s%s" % \
                                   (self._mdb.getServerPrefix(), dbName))
        self._mdb.commit()

    ###########################################################################
    #### dropDb
    ###########################################################################
    def dropDb(self, dbName):
        """Drops metadata about a database managed by qserv."""
        # connect to mysql
        self._mdb.selectMetaDb()
        # check if db exists
        cmd = "SELECT COUNT(*) FROM DbMeta WHERE dbName = '%s'" % dbName
        ret = self._mdb.execCommand1(cmd)
        if ret[0] != 1:
            self._logger.error("Database '%s' not registered" % dbName)
            raise QmsException(Status.ERR_DB_NOT_EXISTS)
        # get partitioningStrategy, psId and drop the entry
        cmd="SELECT dbId, psName, psId FROM DbMeta WHERE dbName = '%s'"%dbName
        (dbId, psName, dbPsId) = self._mdb.execCommand1(cmd)
        if psName == 'sphBox':
            cmd = "DELETE FROM PS_Db_sphBox WHERE psId = %s " % dbPsId
            self._mdb.execCommand0(cmd)
        # remove the entry about the db
        cmd = "DELETE FROM DbMeta WHERE dbId = %s" % dbId
        self._mdb.execCommand0(cmd)
        # remove related tables
        if psName == 'sphBox':
            cmd = "DELETE FROM PS_Tb_sphBox WHERE psId IN (SELECT psId FROM TableMeta WHERE dbId=%s)" % dbId
            self._mdb.execCommand0(cmd)
        cmd = "DELETE FROM TableMeta WHERE dbId = %s" % dbId
        self._mdb.execCommand0(cmd)
        # drop the template database
        self._mdb.execCommand0("DROP DATABASE %s%s" % \
                                   (self._mdb.getServerPrefix(), dbName))
        self._mdb.commit()

    ###########################################################################
    #### retrieveDbInfo
    ###########################################################################
    def retrieveDbInfo(self, dbName):
        """Retrieves info about a database"""
        self._mdb.selectMetaDb()
        if self._mdb.execCommand1("SELECT COUNT(*) FROM DbMeta WHERE dbName='%s'" % dbName)[0] == 0:
            raise QmsException(Status.ERR_DB_NOT_EXISTS)
        ret = self._mdb.execCommand1("SELECT dbId, dbUuid, psName FROM DbMeta WHERE dbName='%s'" % dbName)
        values = dict()
        values["dbId"] = ret[0]
        values["dbUuid"] = ret[1]
        ps = ret[2]
        values["partitioningStrategy"] = ps
        if ps == "sphBox":
            ret = self._mdb.execCommand1("""
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
        return values

    ###########################################################################
    #### checkDbExists
    ###########################################################################
    def checkDbExists(self, dbName):
        """Checks if db <dbName> exists, returns 0 or 1"""
        ret = self._mdb.execCommand1("SELECT COUNT(*) FROM DbMeta WHERE dbName='%s'" % dbName)
        return ret[0]

    ###########################################################################
    #### listDbs
    ###########################################################################
    def listDbs(self):
        """Prints names of all databases managed by qserv into a string"""
        self._mdb.selectMetaDb()
        ret = self._mdb.execCommandN("SELECT dbName FROM DbMeta")
        if not ret:
            return "No databases found"
        s = StringIO.StringIO()
        for r in ret:
            s.write(r[0])
            s.write(' ')
        return s.getvalue()

    ###########################################################################
    #### createTable
    ###########################################################################
    def _processTbOptions(self, opts):
        if not opts.has_key("clusteredIndex"):
            print("param 'clusteredIndex' not found, will use default: NULL")
            opts["clusteredIndex"] = "NULL"

        _crTbOpts = {
            "table_info":("tableName",
                          "partitioning",
                          "schemaFile",
                          "clusteredIndex")}
        _crTbPSOpts = {
            "sphBox":("overlap",
                      "phiColName", 
                      "thetaColName", 
                      "logicalPart",
                      "physChunking")}
        # validate the options
        self._validateKVOptions(opts,_crTbOpts,_crTbPSOpts,"table_info")
        return opts

    def createTable(self, dbName, crTbOptions, schemaStr):
        """Creates metadata about new table in qserv-managed database."""
        # check if db exists
        if self.checkDbExists(dbName) == 0:
            self._logger.error("Database '%s' does not exist." % dbName)
            raise QmsException(Status.ERR_DB_NOT_EXISTS)
        # find out what the partitioning strategy is
        values = self.retrieveDbInfo(dbName)
        # print "Topts1:"
        # for k in crTbOptions: print "  ", k, "  --> ", crTbOptions[k]

        # add default values for missing parameters and do final validation
        crTbOptions["partitioningStrategy"] = values["partitioningStrategy"]
        crTbOptions = self._processTbOptions(crTbOptions)
        s = "createTable in db '%s', options are: " % dbName
        for k in crTbOptions: s+= " (%s-->%s)" % (k, crTbOptions[k])
        self._logger.debug(s)

        # write schema to a temp file
        schemaF = tempfile.NamedTemporaryFile(delete=False)
        schemaF.write(schemaStr)
        self._logger.debug("wrote schema to tempfile %s" % schemaF.name)
        schemaF.close()

        # connect to mysql
        self._mdb.selectMetaDb()
        # get dbid
        dbId = (self._mdb.execCommand1("SELECT dbId FROM DbMeta WHERE dbName = '%s'" % dbName))[0]
        # check if the table already exists
        tableName = crTbOptions["tableName"]
        cmd = "SELECT COUNT(*) FROM TableMeta WHERE dbId=%s AND tableName='%s'" % (dbId, tableName)
        ret = self._mdb.execCommand1(cmd)
        if ret[0] > 0:
            os.unlink(schemaF.name)
            self._logger.error("Table '%s' already registred" % tableName)
            raise QmsException(Status.ERR_TABLE_EXISTS)

        # load the template schema
        self._mdb.loadSqlScript(schemaF.name, "%s%s" % \
                                    (self._mdb.getServerPrefix(), dbName))
        os.unlink(schemaF.name)

        # create entry in PS_Tb_<partitioningStrategy>
        if crTbOptions["partitioning"] == "off":
            psId = '\N'
            psName = None
        else:
            psName = crTbOptions["partitioningStrategy"]
        if psName == "sphBox":
            # add two special columns
            cmd = "ALTER TABLE %s%s.%s ADD COLUMN chunk BIGINT, ADD COLUMN subChunk BIGINT" % (self._mdb.getServerPrefix(), dbName, tableName)
            self._mdb.execCommand0(cmd)

            self._logger.debug("persisting for sphBox")
            ov = crTbOptions["overlap"]
            pCN = crTbOptions["phiColName"]
            tCN = crTbOptions["thetaColName"]
            if not self._checkColumnExists(dbName, tableName, pCN) or \
               not self._checkColumnExists(dbName, tableName, tCN):
                raise QmsException(Status.ERR_COL_NOT_FOUND)
            pN = self._getColumnPos(dbName, tableName, pCN)
            tN = self._getColumnPos(dbName, tableName, tCN)
            lP = int(crTbOptions["logicalPart"])
            pC = int(crTbOptions["physChunking"], 16)
            cmd = "INSERT INTO PS_Tb_sphBox(overlap, phiCol, thetaCol, phiColNo, thetaColNo, logicalPart, physChunking) VALUES(%s, '%s', '%s', %d, %d, %d, %d)" % (ov, pCN, tCN, pN, tN, lP, pC)
            self._mdb.execCommand0(cmd)
            psId = (self._mdb.execCommand1("SELECT LAST_INSERT_ID()"))[0]
        # create entry in TableMeta
        tbUuid = uuid.uuid4() # random UUID
        clusteredIdx = crTbOptions["clusteredIndex"]
        if clusteredIdx == "None":
            cmd = "INSERT INTO TableMeta(tableName, tbUuid, dbId, psId) VALUES ('%s', '%s', %s, %s)" % (tableName, tbUuid, dbId, psId)
        else:
            cmd = "INSERT INTO TableMeta(tableName, tbUuid, dbId, psId, clusteredIdx) VALUES ('%s', '%s', %s, %s, '%s')" % (tableName, tbUuid, dbId, psId, clusteredIdx)
        self._mdb.execCommand0(cmd)
        self._mdb.commit()

    ###########################################################################
    #### dropTable
    ###########################################################################
    def dropTable(self, dbName, tableName):
        """Drops metadata about a table.."""
        self._logger.debug("dropTable: started")
        # connect to mysql
        self._mdb.selectMetaDb()
        # check if db exists
        cmd = "SELECT COUNT(*) FROM DbMeta WHERE dbName = '%s'" % dbName
        ret = self._mdb.execCommand1(cmd)
        if ret[0] != 1:
            self._logger.error("dropTable: database '%s' not registered" % \
                                   dbName)
            raise QmsException(Status.ERR_DB_NOT_EXISTS)
        # get dbId, psName, psId
        cmd = "SELECT dbId, psName, psId FROM DbMeta WHERE dbName='%s'"%dbName
        (dbId, psName, dbPsId) = self._mdb.execCommand1(cmd)
        # check if table exists
        cmd = "SELECT tableId FROM TableMeta WHERE dbId=%s AND tableName='%s'" % (dbId, tableName)
        tableId = self._mdb.execCommand1(cmd)
        if not tableId:
            self._logger.error("dropTable: table '%s' does not exist." % \
                                   tableName)
            raise QmsException(Status.ERR_TABLE_NOT_EXISTS)
        # remove the entry about the table
        cmd = "DELETE FROM TableMeta WHERE tableId = %s" % tableId
        self._mdb.execCommand0(cmd)
        # remove related info
        if psName == 'sphBox':
            cmd = "DELETE FROM PS_Tb_sphBox WHERE psId IN (SELECT psId FROM TableMeta WHERE TableId=%s)" % tableId
            self._mdb.execCommand0(cmd)
        cmd = "DELETE FROM TableMeta WHERE tableId = %s" % tableId
        self._mdb.execCommand0(cmd)
        self._mdb.commit()
        self._logger.debug("dropTable: done")

    ###########################################################################
    #### retrievePartTables
    ###########################################################################
    def retrievePartTables(self, dbName):
        """Retrieves list of partitioned tables for a given database."""
        self._logger.debug("retrievePartTables: started")
        # connect to mysql
        self._mdb.selectMetaDb()
        # check if db exists
        cmd = "SELECT dbId FROM DbMeta WHERE dbName = '%s'" % dbName
        ret = self._mdb.execCommand1(cmd)
        dbId = ret[0]
        cmd = "SELECT tableName FROM TableMeta WHERE dbId=%s " % dbId + \
            "AND psId IS NOT NULL"
        tNames = self._mdb.execCommandN(cmd)
        self._logger.debug("retrieveTableInfo: done")
        return [x[0] for x in tn]

    ###########################################################################
    #### retrieveTableInfo
    ###########################################################################
    def retrieveTableInfo(self, dbName, tableName):
        """Retrieves metadata about a table.."""
        self._logger.debug("retrieveTableInfo: started")
        # connect to mysql
        self._mdb.selectMetaDb()
        # check if db exists
        cmd = "SELECT dbId FROM DbMeta WHERE dbName = '%s'" % dbName
        ret = self._mdb.execCommand1(cmd)
        if not ret:
            self._logger.error("retrieveTableInfo: database '%s' not registered" % dbName)
            raise QmsException(Status.ERR_DB_NOT_EXISTS)
        dbId = ret[0]
        # check if table exists
        cmd = "SELECT tableId FROM TableMeta WHERE dbId=%s AND tableName='%s'" % (dbId, tableName)
        tableId = self._mdb.execCommand1(cmd)
        if not tableId:
            self._logger.error("retrieveTableInfo: table '%s' doesn't exist."%\
                                   tableName)
            raise QmsException(Status.ERR_TABLE_NOT_EXISTS)
        # get ps name
        cmd = "SELECT psName FROM DbMeta WHERE dbId=%s" % dbId
        psName = self._mdb.execCommand1(cmd)[0]
        # the partitioning might be turned off for this table, so check it
        cmd = "SELECT psId FROM TableMeta WHERE tableId=%s" % tableId
        ret = self._mdb.execCommand1(cmd)
        psId = ret[0]
        # retrieve table info
        values = dict()
        if psId and psName == "sphBox":
            ret = self._mdb.execCommand1("SELECT clusteredIdx, overlap, phiCol, thetaCol, phiColNo, thetaColNo, logicalPart, physChunking FROM TableMeta JOIN PS_Tb_sphBox USING(psId) WHERE tableId=%s" % tableId)
            values["clusteredIdx"] = ret[0]
            values["overlap"]      = ret[1]
            values["phiCol"]       = ret[2]
            values["thetaCol"]     = ret[3]
            values["phiColNo"]     = ret[4]
            values["thetaColNo"]   = ret[5]
            values["logicalPart"]  = ret[6]
            values["physChunking"] = hex(ret[7])
        else:
            cmd = "SELECT clusteredIdx FROM TableMeta WHERE tableId=%s" % tableId
            values["clusteredIdx"] = self._mdb.execCommand1(cmd)
        return values

    ###########################################################################
    #### getInternalQmsDbName
    ###########################################################################
    def getInternalQmsDbName(self):
        """Retrieves name of the internal qms database. """
        try:
            dbName = self._mdb.getDbName()
        except QmsException as qe:
            return (None, None)
        return (dbName is not None, dbName)

    ###########################################################################
    #### _checkColumnExists
    ###########################################################################
    def _checkColumnExists(self, dbName, tableName, columnName):
        # note: this function is mysql-specific!
        ret = self._mdb.execCommand1("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE table_schema='%s%s' and table_name='%s' and column_name='%s'" % (self._mdb.getServerPrefix(), dbName, tableName, columnName))
        return ret[0] == 1

    ###########################################################################
    #### _printTable
    ###########################################################################
    def _printTable(self, s, tableName):
        ret = self._mdb.execCommandN("SELECT * FROM %s" % tableName)
        s.write(tableName)
        if len(ret) == 0:
            s.write(" is empty.\n")
        else: 
            s.write(':\n')
            for r in ret: print >> s, "   ", r

    ###########################################################################
    #### _getColumpPos
    ###########################################################################
    def _getColumnPos(self, dbName, tableName, columnName):
        # note: this function is mysql-specific!
        return self._mdb.execCommand1("SELECT ordinal_position FROM information_schema.COLUMNS WHERE table_schema='%s%s' and table_name='%s' and column_name='%s'" % (self._mdb.getServerPrefix(), dbName, tableName, columnName))[0] -1 
