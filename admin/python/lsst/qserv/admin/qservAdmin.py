#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2014 AURA/LSST.
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
import json
import os
import socket
import uuid

from lsst.qserv.css.kvInterface import KvInterface, KvException
from lsst.qserv.admin.qservAdminException import QservAdminException

# Possible options
possibleOpts = {"table" : "schema compression match".split(),
                "match" : ["dirTable1", "dirColName1",
                           "dirTable2", "dirColName2",
                           "flagColName"],
                "partition": ["subChunks", "dirTable",
                              "lonColName", "latColName",
                              "dirColName"]
                }

class QservAdmin(object):
    """
    QservAdmin implements functions needed by qserv_admin client program.
    """

    # this produces a string in a form: 198.129.220.176_2899, used for unique id
    _addrPort = str(socket.getaddrinfo(socket.gethostname(), None)[0][4][0]) + '_' + str(os.getpid())

    def __init__(self, connInfo):
        """
        Initialize: create KvInterface object.

        @param connInfo     Connection information.
        """
        self._kvI = KvInterface(connInfo)
        self._logger = logging.getLogger("QADMI")
        self._uniqueLockId = 0

    def _addPacked(self, origPath, valueDict):
        jsonPath = origPath + ".json"
        return self._kvI.create(k=jsonPath, v=json.dumps(valueDict))

    def _deletePacked(self, origPath):
        jsonPath = origPath + ".json"
        try:
            self._kvI.delete(jsonPath)
        except KvException:
            pass #ignore missing
        try:
            self._kvI.delete(origPath, recursive=True)
        except KvException:
            pass # Ignore missing (dummy) node

    #### DATABASES #################################################################
    def createDb(self, dbName, options):
        """
        Create database (options specified explicitly).

        @param dbName    Database name
        @param options   Array with options (key/value)
        """
        self._logger.debug("Create database '%s', options: %s" % \
                               (dbName, str(options)))
        # double check if all required options are specified
        for x in ["nStripes", "nSubStripes", "overlap", "storageClass"]:
            if x not in options:
                self._logger.error("Required option '%s' missing" % x)
                raise KvException(KvException.MISSING_PARAM, x)
        dbP = "/DBS/%s" % dbName
        ptP = None
        with self._getDbLock(dbName):
            try:
                if self._kvI.exists(dbP):
                    self._logger.info("createDb database '%s' exists, aborting." % \
                                      dbName)
                    return
                self._kvI.create(dbP, "PENDING")
                ptP = self._kvI.create("/PARTITIONING/_", sequence=True)

                options["uuid"] = str(uuid.uuid4())
                partOptions = dict([ (k,v) for k,v in options.items()
                                     if k in set(["nStripes","nSubStripes",
                                                  "overlap","uuid"])
                                     ])
                self._addPacked(ptP, partOptions)
                # old
                # for x in ["nStripes", "nSubStripes", "overlap", "uuid"]:
                #     self._kvI.create("%s/%s" % (ptP, x), options[x])

                pId = ptP[-10:] # the partitioning id is always 10 digit, 0 padded
                dbOpts = {"uuid" : str(uuid.uuid4()),
                          "partitioningId" : pId,
                          "releaseStatus" : "UNRELEASED",
                          "storageClass" : options["storageClass"]}
                self._addPacked(dbP, dbOpts)
                # self._kvI.create("%s/uuid" % dbP, str(uuid.uuid4()))
                # self._kvI.create("%s/partitioningId" % dbP, str(pId))
                # self._kvI.create("%s/releaseStatus" % dbP,"UNRELEASED")
                # for x in ["storageClass"]:
                #     self._kvI.create("%s/%s" % (dbP, x), options[x])
                self._createDbLockSection(dbP)
                self._kvI.set(dbP, "READY")
            except KvException as e:
                self._logger.error("Failed to create database '%s', " % dbName +
                                   "error was: " + e.__str__())
                self._deletePacked(dbP)
                #self._kvI.delete(dbP, recursive=True)
                if ptP is not None: self._deletePacked(ptP)
                #if ptP is not None: self._kvI.delete(ptP, recursive=True)
                raise
        self._logger.debug("Create database '%s' succeeded." % dbName)

    def createDbLike(self, dbName, templateDbName):
        """
        Create database using an existing database as a template.

        @param dbName    Database name (of the database to create)
        @param templateDbName   Database name (of the template database)
        """
        self._logger.info("Creating db '%s' like '%s'" % (dbName, dbName2))
        dbP = "/DBS/%s" % dbName
        dbP2 = "/DBS/%s" % templateDbName
        if dbName == templateDbName:
            raise QservAdminException(QservAdminException.DB_NAME_IS_SELF);
        # Acquire lock in sorted order. Otherwise two admins that run
        # "CREATE DATABASE A LIKE B" and "CREATE DATABASE B LIKE A" can deadlock.
        (name1, name2) = sorted((dbP, dbP2))
        with self._getDbLock(name1):
            with self._getDbLock(name2):
                self._createDbLikeLocked(dbP, dbName, templateDbName)

    def _createDbLikeLocked(self, dbName, templateDbName):
        dbP = "/DBS/%s" % dbName
        srcP = "/DBS/%s" % templateDbName
        try:
            self._kvI.create(dbP, "PENDING")
            packedParms = self._kvI.get(srcP + ".json")
            parms = json.loads(packedParms)
            parms["uuid"] = str(uuid.uuid4())
            self._addPacked(dbP, parms)
            # self._kvI.create("%s/uuid" % dbP, str(uuid.uuid4()))
            # self._copyKeyValue(dbName, dbName2,
            #                    ("storageClass", "partitioningId", "releaseStatus"))
            self._createDbLockSection(dbP)
            self._kvI.set(dbP, "READY")
        except KvException as e:
            self._logger.error("Failed to create database '%s' like '%s', " % \
                                   (dbName, dbName2) + "error was: " + e.__str__())
            self._deletePacked(dbP)
            #self._kvI.delete(dbP, recursive=True)
            raise

    def dropDb(self, dbName):
        """
        Drop database.

        @param dbName    Database name.
        """
        self._logger.info("Drop database '%s'" % dbName)
        with self._getDbLock(dbName):
            dbP = "/DBS/%s" % dbName
            if not self._kvI.exists(dbP):
                self._logger.info("dropDb database '%s' gone, aborting.." % \
                                  dbName)
                return
            self._deletePacked(dbP)
            #self._kvI.delete(dbP, recursive=True)

    def showDatabases(self):
        """
        Print to stdout the list of databases registered for Qserv use.
        """
        if not self._kvI.exists("/DBS"):
            print "No databases found."
        else:
            print self._kvI.getChildren("/DBS")

    #### TABLES ####################################################################
    def createTable(self, dbName, tableName, options):
        """
        Create table (options specified explicitly).

        @param dbName    Database name
        @param tableName Table name
        @param options   Array with options (key/value)
        """
        self._logger.debug("Create table '%s.%s', options: %s" % \
                               (dbName, tableName, str(options)))

        with self._getDbLock(dbName):
            if not self._kvI.exists("/DBS/%s" % dbName):
                self._logger.info("createTable: database '%s' missing, aborting." %\
                                      dbName)
                return
            self._createTable(options, dbName, tableName)

    def _normalizeTableOpts(self, tbOpts, matchOpts, partitionOpts):
        """Apply defaults and fixups to table options."""
        def check(d, possible):
            for k in possible:
                if k not in d:
                    self._logger.info("'%s' not provided" % k)
        check(tbOpts, possibleOpts["table"])
        check(matchOpts, possibleOpts["match"])
        check(partitionOpts, possibleOpts["partition"])
        # Not sure where defaults should be stored now.
        pass

    def _createTable(self, options, dbName, tableName):
        possibleOptions = [
            [""             , "schema"         ],
            [""             , "compression"    ],
            [""             , "match"          ],
            [""             , "uuid"           ],
            ["/match"       , "dirTable1"      ],
            ["/match"       , "dirColName1"    ],
            ["/match"       , "dirTable2"      ],
            ["/match"       , "dirColName2"    ],
            ["/match"       , "flagColName"    ],
            ["/partitioning", "subChunks"      ],
            ["/partitioning", "dirTable"       ],
            ["/partitioning", "lonColName"     ],
            ["/partitioning", "latColName"     ],
            ["/partitioning", "dirColName"     ] ]


        tbP = "/DBS/%s/TABLES/%s" % (dbName, tableName)
        tbOpts = dict([(k, options[k])
                       for k in possibleOpts["table"]
                       if k in options])
        options["uuid"] = str(uuid.uuid4())
        matchOpts = dict([(k, options[k])
                          for k in possibleOpts["match"]
                          if k in options])
        partitionOpts = dict([(k, options[k])
                              for k in possibleOpts["partition"]
                              if k in options])
        self._normalizeTableOpts(tbOpts, matchOpts, partitionOpts)
        try:
            self._kvI.create(tbP, "PENDING")
            self._addPacked(tbP, tbOpts)
            self._addPacked(tbP+"/match", matchOpts)
            self._addPacked(tbP+"/partitioning", partitionOpts)
            # for o in possibleOptions:
            #     if o[1] in options:
            #         k = "%s%s/%s" % (tbP, o[0], o[1])
            #         v = options[o[1]]
            #         self._kvI.create(k, v)
            #     else:
            #         self._logger.info("'%s' not provided" % o[0])
            self._kvI.set(tbP, "READY")
        except KvException as e:
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

    def restore(self, fileName):
        """
        Restore all data from the file fileName.

        @param           fileName Input file name containing data to be restored.
        """
        if len(self._kvI.getChildren("/")) > 1:
            print "Unable to restore, data exists."
            return
        try:
            f = open(fileName, 'r')
            for line in f.readlines():
                (k, v) = line.rstrip().split()
                if v == '\N':
                    v = ''
                if k != '/':
                    self._kvI.create(k, v)
        except IOError:
            print "Can't find file: '" + fileName + "'."
        finally:
            f.close()

    def _dbExists(self, dbName):
        """
        Check if the database exists.

        @param dbName    Database name.
        """
        p = "/DBS/%s" % dbName
        return self._kvI.exists(p)

    def _tableExists(self, dbName, tableName):
        """
        Check if the table exists.

        @param dbName    Database name.
        @param tableName Table name.
        """
        p = "/DBS/%s/TABLES/%s" % (dbName, tableName)
        return self._kvI.exists(p)

    def _copyKeyValue(self, dbDest, dbSrc, theList):
        """
        Copy items specified in theList from dbSrc to dbDest.

        @param dbDest    Destination database name.
        @param dbSrc     Source database name
        @param theList   The list of elements to copy.
        """
        dbS  = "/DBS/%s" % dbSrc
        dbD = "/DBS/%s" % dbDest
        for x in theList:
            v = self._kvI.get("%s/%s" % (dbS, x))
            self._kvI.create("%s/%s" % (dbD, x), v)

    def _createDbLockSection(self, dbP):
        """
        Create key/values related to "LOCK" for a given db path. This is used to
        prevent users from running queries, e.g. during maintenance.

        @param dbP    Path to the database.
        """
        lockOptList = "comments estimatedDuration lockedBy lockedTime mode reason"
        lockOpts = dict([(k,"") for k in lockOptList.split()])
        self._addPacked(dbP + "/LOCK.json", lockOpts)
        # self._kvI.create("%s/LOCK/comments" % dbP)
        # self._kvI.create("%s/LOCK/estimatedDuration" % dbP)
        # self._kvI.create("%s/LOCK/lockedBy" % dbP)
        # self._kvI.create("%s/LOCK/lockedTime" % dbP)
        # self._kvI.create("%s/LOCK/mode" % dbP)
        # self._kvI.create("%s/LOCK/reason" % dbP)

    ##### Locking related ##########################################################
    def _getDbLock(self, dbName):
            return self._kvI.getLockObject("/DBS/%s" % dbName, self._uniqueId())

    def _uniqueId(self):
        self._uniqueLockId += 1
        return QservAdmin._addrPort + '_' + str(self._uniqueLockId)
