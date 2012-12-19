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

"""
This module implements internals of manipulating qserv worker metadata.
"""

import ConfigParser
import os
import StringIO

from lsst.qserv.meta.db import Db
from lsst.qserv.meta.status import Status, getErrMsg, QmsException
from lsst.qserv.meta.client import Client

class Meta:
    def __init__(self, loggerName,
                 qmsHost, qmsPort, qmsUser, qmsPass,
                 qmwDb, qmwUser, qmwPass, qmwMySqlSocket):
        self._qmwDb = "qmw_%s" % qmwDb
        self._mdb = Db(loggerName, None, None, qmwUser, qmwPass,
                       qmwMySqlSocket, self._qmwDb)
        self._mdb.connectToMySQLServer()
        self._qmsClient = Client(qmsHost, qmsPort, qmsUser, qmsPass)

    def __del__(self):
        self._mdb.disconnect()

    def installMeta(self):
        """Initializes persistent qserv metadata structures on the worker.
        This method should be called only once ever for a given qms
        installation on a given worker."""
        internalTables = [
            # The 'Dbs' table keeps the list of databases managed through 
            # qserv. Databases not entered into that table will be ignored 
            # by qserv.
            ['Dbs', '''(
   dbId INT NOT NULL PRIMARY KEY, 
   dbName VARCHAR(255) NOT NULL, 
   dbUuid VARCHAR(255) NOT NULL
   )''']]
        try:
            self._mdb.createMetaDb()
            self._mdb.selectMetaDb()
            for t in internalTables:
                self._mdb.createTable(t[0], t[1])
            self._mdb.commit()
        except QmsException as qe:
            if qe.getErrNo() == Status.ERR_DB_EXISTS:
                raise Exception(getErrMsg(Status.ERR_META_EXISTS))
            else:
                raise Exception(qe.getErrMsg())

    def destroyMeta(self):
        try:
            self._mdb.selectMetaDb()
            self._mdb.dropMetaDb()
            self._mdb.commit()
        except QmsException as qe:
            raise Exception(qe.getErrMsg())

    def printMeta(self):
        try:
            self._mdb.selectMetaDb()
            s = self._mdb.printTable("Dbs")
        except QmsException as qe:
            raise Exception(qe.getErrMsg())
        return s

    def registerDb(self, dbName):
        try:
            self._mdb.selectMetaDb()
            # check if already registered, fail if it is
            if self._checkDbIsRegistered(dbName):
                raise Exception("Db '%s' is already registered." % dbName)
            # get dbId and dbUuid from qms
            try:
                values = self._qmsClient.retrieveDbInfo(dbName)
            except Exception, e:
                if str(e) == getErrMsg(Status.ERR_DB_NOT_EXISTS):
                    raise Exception("Db '%s' is not registered in the metadata server." % dbName)
                else:
                    raise
            if not 'dbId' in values:
                raise Exception("Invalid dbInfo from qms (dbId not found)")
            if not 'dbUuid' in values:
                raise Exception("Invalid dbInfo from qms (dbUuid not found)")
            # register it
            cmd = "INSERT INTO Dbs(dbId, dbName, dbUuid) VALUES (%s, '%s','%s')" % (values['dbId'], dbName, values['dbUuid'])
            self._mdb.execCommand0(cmd)
            self._mdb.commit()
        except QmsException as qe: raise Exception(qe.getErrMsg())

    def unregisterDb(self, dbName):
        try:
            self._mdb.selectMetaDb()
            # check if already registered, fail if it is not
            if not self._checkDbIsRegistered(dbName):
                raise Exception("Db '%s' is not registered." % dbName)
            # unregister it
            cmd = "DELETE FROM Dbs WHERE dbName='%s'" % dbName;
            self._mdb.execCommand0(cmd)
            self._mdb.commit()
        except QmsException as qe: raise Exception(qe.getErrMsg())

    def listDbs(self):
        xx = []
        try:
            self._mdb.selectMetaDb()
            xx = self._mdb.execCommandN("SELECT dbName FROM Dbs")
        except QmsException as qe: 
            raise Exception(qe.getErrMsg())
        return [x[0] for x in xx]

    ###########################################################################
    ##### miscellaneous
    ###########################################################################
    def _checkDbIsRegistered(self, dbName):
        cmd = "SELECT COUNT(*) FROM Dbs WHERE dbName = '%s'" % dbName
        ret = self._mdb.execCommand1(cmd)
        return ret[0] == 1


###############################################################################
##### read connection info
###############################################################################
def readConnInfoFromFile(fileName):
    if fileName[0] == '~':
        fileName = os.path.expanduser(fileName)
    if not os.path.exists(fileName):
        raise Exception("%s does not exist." % fileName)
    config = ConfigParser.ConfigParser()
    config.read(fileName)
    s = "qmsConn"
    if not config.has_section(s):
        raise Exception("Bad %s, can't find section '%s'" % (fileName, s))
    if not config.has_option(s, "host") or \
       not config.has_option(s, "port") or \
       not config.has_option(s, "user") or \
       not config.has_option(s, "pass"):
        raise Exception("Bad %s, can't find host, port, user or pass"%fileName)
    (host,port,usr,pwd) = (config.get(s, "host"), config.getint(s, "port"),
                           config.get(s, "user"), config.get(s, "pass"))

    s = "qmwConn"
    if not config.has_section(s):
        raise Exception("Bad %s, can't find section '%s'" % (fileName,s))
    if not config.has_option(s, "db") or \
       not config.has_option(s, "user") or \
       not config.has_option(s, "pass") or \
       not config.has_option(s, "mySqlSocket"):
        raise Exception("Bad %s, can't find db, user, pass or mysqlSocket"\
                            % fileName)
    return (host,port,usr,pwd,
            config.get(s, "db"), config.get(s, "user"),
            config.get(s, "pass"), config.get(s, "mySqlSocket"))
