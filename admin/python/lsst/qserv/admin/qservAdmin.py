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
# Current metadata version info
from lsst.qserv.css import VERSION_KEY
from lsst.qserv.css import VERSION
from lsst.qserv.admin.qservAdminException import QservAdminException

# Possible options
possibleOpts = {"table" : set(["schema", "compression", "match"]),
                "match" : set(["dirTable1", "dirColName1",
                               "dirTable2", "dirColName2",
                               "flagColName"]),
                "partition": set(["subChunks", "dirTable",
                                  "lonColName", "latColName",
                                  "dirColName", "overlap"])
                }
# match and partition options are only required if table is match or
# partitioned, respectively. In each cases, all possible opts in
# its category are required.
requiredOpts = { "table" : ["schema"]}

class QservAdmin(object):
    """
    QservAdmin implements functions needed by qserv_admin client program.
    """

    # this produces a string in a form: 198.129.220.176_2899, used for unique id
    _addrPort = str(socket.getaddrinfo(socket.gethostname(), None)[0][4][0]) + '_' + str(os.getpid())

    def __init__(self, connInfo=None, config=None):
        """
        Initialize: create KvInterface object. One of the
        connInfo or config parametrs must be supplied,

        @param connInfo     Connection information string, e.g. 'localhost:12181'.
        @param config       Dictionary with configuration options, same as
                            accepted by KvInterface.newImpl.
        """
        if connInfo is not None:
            self._kvI = KvInterface.newImpl(connInfo=connInfo)
        else:
            self._kvI = KvInterface.newImpl(config=config)
        self._logger = logging.getLogger("QADM")
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

    def _getMaybePacked(self, node, keys):
        """Get data from a node which could be packed or not"""
        # try packed stuff first
        if self._kvI.exists(node + '.json'):
            # if json packed then convert back to Python object
            data = self._kvI.get(node + '.json')
            return json.loads(data)
        else:
            # try unpacked, check for every key and get whatever is defined
            if self._kvI.exists(node):
                info = {}
                for key in keys:
                    try:
                        info[key] = self._kvI.get(node + '/' + key)
                    except KvException:
                        pass
                return info

    #### DATABASES #################################################################
    def dbExists(self, dbName):
        """
        Check if the database exists.

        @param dbName    Database name.
        """
        p = "/DBS/%s" % dbName
        return self._kvI.exists(p)

    def getDbInfo(self, dbName):
        """
        Returns dictionary with database configuration or None if database
        does not exist.

        @param dbName:    Database name
        """
        node = "/DBS/" + dbName
        return self._getMaybePacked(node, ['releaseStatus', 'uuid', 'storageClass', 'partitioningId'])

    def getPartInfo(self, partId):
        """
        Returns dictionary with partitioning configuration or None if partitioning ID
        does not exist.

        @param partId:    partitioning ID
        """
        node = "/PARTITIONING/_" + partId
        return self._getMaybePacked(node, ['nStripes', 'nSubStripes', 'overlap', 'uuid'])

    def createDb(self, dbName, options):
        """
        Create database (options specified explicitly). Options is a dictionary
        with these keys and values:
          - nStripes:     number of partitioning stripes
          - nSubStripes:  number of partitioning sub-stripes
          - overlap:      size of chunk overlap, default per-database value
          - storageClass: one of 'L1', 'L2', 'L3'

        @param dbName    Database name
        @param options   Dictionary with options (key/value)
        """
        self._logger.debug("Create database '%s', options: %s",
                           dbName, options)
        # client should guarantee existence of "partitioning" option
        if "partitioning" not in options:
            raise QservAdminException(QservAdminException.INTERNAL)
        usePartitioning = options["partitioning"]
        if usePartitioning:
            # double check if all required options are specified
            for x in ["nStripes", "nSubStripes", "storageClass"]:
                if x not in options:
                    self._logger.error("Required option '%s' missing", x)
                    raise KvException(KvException.MISSING_PARAM, x)

        # first check version or store it
        with self._kvI.getLockObject(VERSION_KEY, self._uniqueId()):
            if self._kvI.exists(VERSION_KEY):
                self._versionCheck()
            else:
                self._versionSave()

        dbP = "/DBS/%s" % dbName
        ptP = None
        with self._getDbLock(dbName):
            try:
                if self._kvI.exists(dbP):
                    self._logger.info("createDb database '%s' exists, aborting.",
                                      dbName)
                    return
                self._kvI.create(dbP, "PENDING")
                options["uuid"] = str(uuid.uuid4())
                dbOpts = {"uuid" : str(uuid.uuid4()),
                          "releaseStatus" : "UNRELEASED",
                          "storageClass" : options["storageClass"]}
                if usePartitioning:
                    ptP = self._kvI.create("/PARTITIONING/_", sequence=True)
                    partOptions = dict((k, v) for k, v in options.items()
                                       if k in ["nStripes",
                                                "nSubStripes",
                                                "overlap",
                                                "uuid"])
                    self._addPacked(ptP, partOptions)
                    # Partitioning id is always 10 digit, 0 padded
                    dbOpts["partitioningId"] = ptP[-10:]
                self._addPacked(dbP, dbOpts)
                self._createDbLockSection(dbP)
                self._kvI.set(dbP, "READY")
            except KvException as e:
                self._logger.error("Failed to create database '%s', error was: %s",
                                   dbName, e)
                self._deletePacked(dbP)
                if ptP is not None: self._deletePacked(ptP)
                raise
        self._logger.debug("Create database '%s' succeeded.", dbName)

    def createDbLike(self, dbName, templateDbName):
        """
        Create database using an existing database as a template.

        @param dbName    Database name (of the database to create)
        @param templateDbName   Database name (of the template database)
        """
        self._logger.info("Creating db '%s' like '%s'", dbName, templateDbName)

        # first check version
        self._versionCheck()

        dbP = "/DBS/%s" % dbName
        dbP2 = "/DBS/%s" % templateDbName
        if dbName == templateDbName:
            raise QservAdminException(QservAdminException.DB_NAME_IS_SELF)
        # Acquire lock in sorted order. Otherwise two admins that run
        # "CREATE DATABASE A LIKE B" and "CREATE DATABASE B LIKE A" can deadlock.
        (name1, name2) = sorted((dbP, dbP2))
        with self._getDbLock(name1), self._getDbLock(name2):
            self._createDbLikeLocked(dbName, templateDbName)

    def _createDbLikeLocked(self, dbName, templateDbName):
        dbP = "/DBS/%s" % dbName
        srcP = "/DBS/%s" % templateDbName
        try:
            self._kvI.create(dbP, "PENDING")
            packedParms = self._kvI.get(srcP + ".json")
            parms = json.loads(packedParms)
            parms["uuid"] = str(uuid.uuid4())
            self._addPacked(dbP, parms)
            self._createDbLockSection(dbP)
            self._kvI.set(dbP, "READY")
        except KvException as e:
            self._logger.error("Failed to create database '%s' like '%s', error was: %s",
                               dbName, templateDbName, e)
            self._deletePacked(dbP)
            raise

    def dropDb(self, dbName):
        """
        Drop database.

        @param dbName    Database name.
        """
        self._logger.info("Drop database '%s'", dbName)

        # first check version
        self._versionCheck()

        with self._getDbLock(dbName):
            dbP = "/DBS/%s" % dbName
            if not self._kvI.exists(dbP):
                self._logger.info("dropDb database '%s' gone, aborting..",
                                  dbName)
                return
            self._deletePacked(dbP)

    def showDatabases(self):
        """
        Print to stdout the list of databases registered for Qserv use.
        """
        if not self._kvI.exists("/DBS"):
            print "No databases found."
        else:
            print self._kvI.getChildren("/DBS")

    #### TABLES ####################################################################
    def tableExists(self, dbName, tableName):
        """
        Check if the table exists.

        @param dbName    Database name.
        @param tableName Table name.
        """
        p = "/DBS/%s/TABLES/%s" % (dbName, tableName)
        return self._kvI.exists(p)

    def tables(self, dbName):
        """ Returns the list of table names defined in CSS """
        key = "/DBS/%s/TABLES" % (dbName,)
        return self._cssChildren(key, [])

    def createTable(self, dbName, tableName, options):
        """
        Create table (options specified explicitly). Options is a dictionary
        with these keys and values:
          - schema:
          - compression:
          - match:
          For match tables:
          - dirTable1:
          - dirColName1:
          - dirTable2:
          - dirColName2:
          - flagColName:
          For partitioned tables:
          - subChunks:
          - dirTable:
          - lonColName:
          - latColName:
          - dirColName:
          - overlap:

        @param dbName    Database name
        @param tableName Table name
        @param options   Dictionary with options (key/value)
        """
        self._logger.debug("Create table '%s.%s', options: %s",
                           dbName, tableName, options)

        # first check version
        self._versionCheck()

        with self._getDbLock(dbName):
            if not self._kvI.exists("/DBS/%s" % dbName):
                self._logger.info("createTable: database '%s' missing, aborting.",
                                  dbName)
                return
            self._createTable(options, dbName, tableName)

    def createTableLike(self, dbName, tableName, options):
        """FIXME, createTableLike is not implemented!"""
        raise QservAdminException(QservAdminException.NOT_IMPLEMENTED)

    def dropTable(self, dbName, tableName):
        """
        Delete table information

        @param dbName    Database name
        @param tableName Table name
        """
        self._logger.debug("Drop table '%s.%s'", dbName, tableName)

        with self._getDbLock(dbName):
            tbP = "/DBS/%s/TABLES/%s" % (dbName, tableName)
            self._deletePacked(tbP)

    def _normalizeTableOpts(self, tbOpts, matchOpts, partitionOpts):
        """Apply defaults and fixups to table options."""
        def check(d, possible):
            for k in possible:
                if k not in d:
                    self._logger.info("'%s' not provided" % k)
        def checkFail(d, required):
            for k in required:
                if k not in d:
                    self._logger.error("'%s' not provided" % k)
                    raise QservAdminException(
                        QservAdminException.MISSING_PARAM)
        check(tbOpts, possibleOpts["table"])
        checkFail(tbOpts, requiredOpts["table"])
        if matchOpts:
            checkFail(matchOpts, possibleOpts["match"])
        if partitionOpts:
            checkFail(partitionOpts, possibleOpts["partition"])
        # Not sure where defaults should be stored now.
        pass

    def _createTable(self, options, dbName, tableName):
        tbP = "/DBS/%s/TABLES/%s" % (dbName, tableName)
        tbOpts = dict(item for item in options.items()
                      if item[0] in possibleOpts["table"])
        options["uuid"] = str(uuid.uuid4())
        matchOpts = dict(item for item in options.items()
                         if item[0] in possibleOpts["match"])
        partitionOpts = dict(item for item in options.items()
                             if item[0] in possibleOpts["partition"])
        self._normalizeTableOpts(tbOpts, matchOpts, partitionOpts)
        try:
            self._kvI.create(tbP, "PENDING")
            self._addPacked(tbP, tbOpts)
            if matchOpts:
                self._addPacked(tbP+"/match", matchOpts)
            if partitionOpts:
                self._addPacked(tbP+"/partitioning", partitionOpts)
            self._kvI.set(tbP, "READY")
        except KvException as e:
            self._logger.error("Failed to create table '%s.%s', error was: %s",
                                dbName, tableName, e)
            self._kvI.delete(tbP, recursive=True)
            raise
        self._logger.debug("Create table '%s.%s' succeeded.", dbName, tableName)

    #### CHUNKS ####################################################################
    def chunks(self, dbName, tableName):
        """
        Returns all chunks defined in CSS. Returned object is a dictionary with
        chunk number as a key and list of worker names as value. Empty dict is
        returned if no chunk info is defined.
        """
        res = {}
        with self._getDbLock(dbName):
            key = "/DBS/%s/TABLES/%s/CHUNKS" % (dbName, tableName)
            for chunk in self._cssChildren(key, []):
                hosts = []
                replKey = "%s/%s/REPLICAS" % (key, chunk)
                for repl in self._cssChildren(replKey, []):
                    host = None
                    if repl.endswith('.json'):
                        data = self._cssGet(replKey + '/' + repl)
                        if data:
                            data = json.loads(data)
                            host = data.get('nodeName')
                    else:
                        host = self._cssGet(replKey + '/' + repl + '/nodeName')
                    if host:
                        hosts.append(host)
                res[int(chunk)] = hosts
        return res

    def addChunk(self, dbName, tableName, chunk, hosts):
        """
        Retuns all chunks defined in CSS. Returned object is a dictionary with
        chunk number as a key and list of worker names as value. Empty dict is
        returned if no chunk info is defined.
        """

        self._logger.debug("Add chunk replicas '%s.%s', chunk: %s hosts: %s",
                           dbName, tableName, chunk, hosts)

        with self._getDbLock(dbName):

            key = "/DBS/%s/TABLES/%s/CHUNKS/%s/REPLICAS" % (dbName, tableName, chunk)

            for host in hosts:
                path = self._kvI.create(key + '/', sequence=True)
                self._logger.debug("New chunk replica key: %s", path)
                self._addPacked(path, dict(nodeName=host))

    ################################################################################
    def dumpEverything(self, dest=None):
        """
        Dumps entire metadata in CSS. Output goes to file (if provided through
        "dest"), otherwise to stdout. Argument can be a file object instance
        (anything that has write() method) or a string in which case it is
        assumed to be a file name to write to.
        """
        if dest is None:
            self._kvI.dumpAll()
        elif getattr(dest, 'write'):
            self._kvI.dumpAll(dest)
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
                if v == r'\N':
                    v = ''
                if k != '/':
                    self._kvI.create(k, v)
        except IOError:
            print "Can't find file: '" + fileName + "'."
        finally:
            f.close()

    def _copyKeyValue(self, dbDest, dbSrc, theList):
        """
        Copy items specified in theList from dbSrc to dbDest.

        @param dbDest    Destination database name.
        @param dbSrc     Source database name
        @param theList   The list of elements to copy.
        """
        dbS = "/DBS/%s" % dbSrc
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
        lockOpts = dict([(k, "") for k in lockOptList.split()])
        self._addPacked(dbP + "/LOCK", lockOpts)

    def _versionCheck(self):
        """
        Checks the store version number against our own version
        Returns normally if they match, throws exception if versions do not match
        or version is missing.
        """
        if self._kvI.exists(VERSION_KEY):
            versionString = self._kvI.get(VERSION_KEY)
            if versionString == str(VERSION):
                return
            else:
                raise QservAdminException(QservAdminException.VERSION_MISMATCH)
        else:
            raise QservAdminException(QservAdminException.VERSION_MISSING)

    def _versionSave(self):
        """
        Stores version number. Version key must not exist yet.
        It is recommended to lock version key before calling this method.
        """
        self._kvI.create(VERSION_KEY, str(VERSION))

    ##### convenience ##########################################################
    def _cssGet(self, key, default=None):
        "Returns key value or default if key does not exist"
        if self._kvI.exists(key):
            return self._kvI.get(key)
        return default

    def _cssChildren(self, key, default=None):
        "Returns list of children or default if key does not exist"
        if self._kvI.exists(key):
            return self._kvI.getChildren(key)
        return default

    ##### Locking related ##########################################################
    def _getDbLock(self, dbName):
        return self._kvI.getLockObject("/DBS/%s" % dbName, self._uniqueId())

    def _uniqueId(self):
        self._uniqueLockId += 1
        return QservAdmin._addrPort + '_' + str(self._uniqueLockId)
