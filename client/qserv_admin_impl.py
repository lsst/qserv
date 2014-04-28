#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013 LSST Corporation.
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

"""
Internals that do the actual work for the qserv client program.

@author  Jacek Becla, SLAC


Known issues and todos:
 - Note that this depends on kazoo, so it'd be best to avoid distributing this to
   every user. For that reason in the future we might run this separately from the
   client, so this may not have access to local config files provided by user
   - and that will complicate error handling, e.g., if we raise exception here, the
     qserv_admin which will run on a separate server will not be able to catch it.
"""

import logging
import uuid

from kvInterface import KvInterface, CssException

class QservAdminImpl(object):
    """
    QservAdminImpl implements functions needed by qserv_admin client program.
    """

    def __init__(self, connInfo):
        """
        Initialize: create KvInterface object.

        @param connInfo     Connection information.
        """
        self._kvI = KvInterface(connInfo)
        self._logger = logging.getLogger("QADMI")

    #### DATABASES #################################################################
    def createDb(self, dbName, options):
        """
        Create database (options specified explicitly).

        @param dbName    Database name
        @param options   Array with options (key/value)
        """
        self._logger.debug("Create database '%s', options: %s" % \
                               (dbName, str(options)))
        if self._dbExists(dbName):
            self._logger.error("Database '%s' already exists." % dbName)
            raise CssException(CssException.DB_EXISTS, dbName)
        # double check if all required options are specified
        for x in ["nStripes", "nSubStripes", "overlap", "dbGroup", "objIdIndex"]:
            if x not in options:
                self._logger.error("Required option '%s' missing" % x)
                raise CssException(CssException.MISSING_PARAM, x)
        dbP = "/DATABASES/%s" % dbName
        ptP = None
        try:
            self._kvI.create(dbP, "PENDING")
            ptP = self._kvI.create("/DATABASE_PARTITIONING/_", sequence=True)
            options["uuid"] = str(uuid.uuid4())
            for x in ["nStripes", "nSubStripes", "overlap", "uuid"]:
                self._kvI.create("%s/%s" % (ptP, x), options[x])
            self._kvI.create("%s/uuid" % dbP, str(uuid.uuid4()))
            pId = ptP[-10:] # the partitioning id is always 10 digit, 0 padded
            self._kvI.create("%s/partitioningId" % dbP, str(pId))
            self._kvI.create("%s/releaseStatus" % dbP,"UNRELEASED")
            for x in ["dbGroup", "objIdIndex"]:
                self._kvI.create("%s/%s" % (dbP, x), options[x])
            self._createDbLockSection(dbP)
            self._kvI.set(dbP, "READY")
        except CssException as e:
            self._logger.error("Failed to create database '%s', " % dbName +
                               "error was: " + e.__str__())
            self._kvI.delete(dbP, recursive=True)
            if ptP is not None: self._kvI.delete(ptP, recursive=True)
            raise
        self._logger.debug("Create database '%s' succeeded." % dbName)

    def createDbLike(self, dbName, dbName2):
        """
        Create database using an existing database as a template.

        @param dbName    Database name (of the database to create)
        @param dbName2   Database name (of the template database)
        """
        self._logger.info("Creating db '%s' like '%s'" % (dbName, dbName2))
        if self._dbExists(dbName):
            self._logger.error("Database '%s' already exists." % dbName)
            raise CssException(CssException.DB_EXISTS, dbName)
        if not self._dbExists(dbName2):
            self._logger.error("Database '%s' does not exist." % dbName2)
            raise CssException(CssException.DB_DOES_NOT_EXIST, dbName2)
        dbP = "/DATABASES/%s" % dbName
        try:
            self._kvI.create(dbP, "PENDING")
            self._kvI.create("%s/uuid" % dbP, str(uuid.uuid4()))
            self._copyKeyValue(dbName, dbName2, 
                               ("dbGroup", "partitioningId", 
                                "releaseStatus", "objIdIndex"))
            self._createDbLockSection(dbP)
            self._kvI.set(dbP, "READY")
        except CssException as e:
            self._logger.error("Failed to create database '%s', " % dbName +
                               "error was: " + e.__str__())
            self._kvI.delete(dbP, recursive=True)
            raise

    def dropDb(self, dbName):
        """
        Drop database.

        @param dbName    Database name.
        """
        self._logger.info("Drop database '%s'" % dbName)
        if not self._dbExists(dbName):
            self._logger.error("Database '%s' does not exist." % dbName)
            raise CssException(CssException.DB_DOES_NOT_EXIST, dbName)
        self._kvI.delete("/DATABASES/%s" % dbName, recursive=True)

    def showDatabases(self):
        """
        Print to stdout the list of databases registered for Qserv use.
        """
        if not self._kvI.exists("/DATABASES"):
            print "No databases found."
        else:
            print self._kvI.getChildren("/DATABASES")

    #### TABLES ####################################################################
    def createTable(self, dbName, tableName, options):
        """
        Create table (options specified explicitly).

        @param dbName    Database name
        @param tableName Table name
        @param options   Array with options (key/value)
        """
        possibleOptions = [
            [""             , "schema"         ],
            [""             , "compression"    ],
            [""             , "isRefMatch"     ],
            [""             , "uuid"           ],
            ["/isRefMatch"  , "keyColInTable1" ],
            ["/isRefMatch"  , "keyColInTable2" ],
            ["/partitioning", "subChunks"      ],
            ["/partitioning", "secIndexColName"],
            ["/partitioning", "drivingTable"   ],
            ["/partitioning", "lonColName"     ],
            ["/partitioning", "latColName"     ],
            ["/partitioning", "keyColName"     ] ]

        self._logger.debug("Create table '%s.%s', options: %s" % \
                               (dbName, tableName, str(options)))
        if not self._dbExists(dbName):
            self._logger.error("Database '%s' does not exist." % dbName)
            raise CssException(CssException.DB_DOES_NOT_EXIST, dbName)
        if self._tableExists(dbName, tableName):
            self._logger.error("Table '%s.%s' exists." % (dbName, tableName))
            raise CssException(CssException.TB_EXISTS, "%s.%s" % (dbName,tableName))
        tbP = "/DATABASES/%s/TABLES/%s" % (dbName, tableName)
        options["uuid"] = str(uuid.uuid4())
        try:
            self._kvI.create(tbP, "PENDING")
            for o in possibleOptions:
                if o[1] in options:
                    k = "%s%s/%s" % (tbP, o[0], o[1])
                    v = options[o[1]]
                    self._kvI.create(k, v)
                else:
                    self._logger.info("'%s' not provided" % o[0])
            self._kvI.set(tbP, "READY")
        except CssException as e:
            self._logger.error("Failed to create table '%s.%s', " % \
                                (dbName, tableName) + "error was: " + e.__str__())
            self._kvI.delete(tbP, recursive=True)
            raise
        self._logger.debug("Create table '%s.%s' succeeded." % (dbName, tableName))

    ################################################################################
    def dumpEverything(self, dest=None):
        """
        Dumps entire metadata in CSS. Output goes to file (if provided through
        "dest"), otherwise to stdout.
        """
        if dest is None:
            self._kvI.dumpAll()
        else:
            with open(dest, "w") as f:
                self._kvI.dumpAll(f)

    def dropEverything(self):
        """
        Delete everything from the CSS (very dangerous, very useful for debugging.)
        """
        self._kvI.delete("/", recursive=True)

    def _dbExists(self, dbName):
        """
        Check if the database exists.

        @param dbName    Database name.
        """
        p = "/DATABASES/%s" % dbName
        return self._kvI.exists(p)

    def _tableExists(self, dbName, tableName):
        """
        Check if the table exists.

        @param dbName    Database name.
        @param tableName Table name.
        """
        p = "/DATABASES/%s/TABLES/%s" % (dbName, tableName)
        return self._kvI.exists(p)

    def _copyKeyValue(self, dbDest, dbSrc, theList):
        """
        Copy items specified in theList from dbSrc to dbDest.

        @param dbDest    Destination database name.
        @param dbSrc     Source database name
        @param theList   The list of elements to copy.
        """
        dbS  = "/DATABASES/%s" % dbSrc
        dbD = "/DATABASES/%s" % dbDest
        for x in theList:
            v = self._kvI.get("%s/%s" % (dbS, x))
            self._kvI.create("%s/%s" % (dbD, x), v)

    def _createDbLockSection(self, dbP):
        """
        Create key/values related to "LOCK" for a given db path.

        @param dbP    Path to the database.
        """
        self._kvI.create("%s/LOCK/comments" % dbP)
        self._kvI.create("%s/LOCK/estimatedDuration" % dbP)
        self._kvI.create("%s/LOCK/lockedBy" % dbP)
        self._kvI.create("%s/LOCK/lockedTime" % dbP)
        self._kvI.create("%s/LOCK/mode" % dbP)
        self._kvI.create("%s/LOCK/reason" % dbP)
