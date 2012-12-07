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
# The guts of manipulating qserv metadata worker. 

import StringIO

from lsst.qserv.meta.db import Db
from lsst.qserv.meta.status import Status, getErrMsg, QmsException
from lsst.qserv.meta.client import Client

class Meta:
    def __init__(self, loggerName,
                 qmsHost, qmsPort, qmsUser, qmsPass,
                 qmwDb, qmwUser, qmwPass, qmwMySqlSocket):
        self._qmwDb   = "qmw_%s" % qmwDb
        self._mdb = Db(loggerName, None, None, qmwUser, qmwPass,
                       qmwMySqlSocket, self._qmwDb)
        self._qmsClient = Client(qmsHost, qmsPort, qmsUser, qmsPass)

    def installMeta(self):
        """Initializes persistent qserv metadata structures on the worker.
        This method should be called only once ever for a given qms
        installation on a given worker."""
        internalTables = [
            # The DbMeta table keeps the list of databases managed through 
            # qserv. Databases not entered into that table will be ignored 
            # by qserv.
            ['Dbs', '''(
   dbId INT NOT NULL PRIMARY KEY, 
   dbName VARCHAR(255) NOT NULL, 
   dbUuid VARCHAR(255) NOT NULL
   )''']]
        try:
            self._mdb.connectAndCreateDb()
            for t in internalTables:
                self._mdb.createTable(t[0], t[1])
            self._mdb.disconnect()
        except QmsException as qe: raise Exception(qe.getErrMsg())

    def destroyMeta(self):
        try:
            self._mdb.connect()
            self._mdb.dropDb()
            self._mdb.disconnect()
        except QmsException as qe: raise Exception(qe.getErrMsg())

    def printMeta(self):
        try:
            self._mdb.connect()
            s = self._mdb.printTable("Dbs")
            self._mdb.disconnect()
        except QmsException as qe:
            if qe.getErrNo() == Status.ERR_NO_META:
                return "No metadata found."           
            else:
                raise Exception(qe.getErrMsg())
        return s

    def registerDb(self, dbName):
        try:
            self._mdb.connect()
            # check if already registered, fail if it is
            if self._checkDbIsRegistered(dbName):
                raise Exception("Db '%s' is already registered." % dbName)
            # get dbId and dbUuid from qms
            try:
                values = self._qmsClient.retrieveDbInfo(dbName)
            except QmsException as qe:
                if qe.getErrNo() == ERR_DB_NOT_EXISTS:
                    raise Exception("Db '%s' is not registered in the metadata server." % dbName)
                else:
                    raise Exception(qe.getErrMsg())
            if not 'dbId' in values:
                raise Exception("Invalid dbInfo from qms (dbId not found)")
            if not 'dbUuid' in values:
                raise Exception("Invalid dbInfo from qms (dbUuid not found)")
            # register it
            cmd = "INSERT INTO Dbs(dbId, dbName, dbUuid) VALUES (%s, '%s','%s')" % (values['dbId'], dbName, values['dbUuid'])
            self._mdb.execCommand0(cmd)
            self._mdb.disconnect()
        except QmsException as qe: raise Exception(qe.getErrMsg())

    def unregisterDb(self, dbName):
        try:
            self._mdb.connect()
            # check if already registered, fail if it is not
            if not self._checkDbIsRegistered(dbName):
                raise Exception("Db '%s' is not registered." % dbName)
            # unregister it
            cmd = "DELETE FROM Dbs WHERE dbName='%s'" % dbName;
            self._mdb.execCommand0(cmd)
            self._mdb.disconnect()
        except QmsException as qe: raise Exception(qe.getErrMsg())

    def listDbs(self):
        xx = []
        try:
            self._mdb.connect()
            xx = self._mdb.execCommandN("SELECT dbName FROM Dbs")
            self._mdb.disconnect()
        except QmsException as qe: raise Exception(qe.getErrMsg())
        return [x[0] for x in xx]

    ###########################################################################
    ##### miscellaneous
    ###########################################################################
    def _checkDbIsRegistered(self, dbName):
        cmd = "SELECT COUNT(*) FROM Dbs WHERE dbName = '%s'" % dbName
        ret = self._mdb.execCommand1(cmd)
        return ret[0] == 1

    ###########################################################################
    ##### connection to QMS
    ###########################################################################
    def _connectToQMS(self):
        (host, port, user, pwd) = self._getConnInfo()
        if host is None or port is None or user is None or pwd is None:
            return False
        self._logger.debug("Using connection: %s:%s, %s,pwd=%s" % \
                               (host, port, user, pwd))
        url = "http://%s:%d/%s" % (host, port, self._defaultXmlPath)
        self._logger.debug("Url is %s" % url)
        qms = xmlrpclib.Server(url)
        if self._echoTest(qms):
            return qms
        else:
            return None

    def _getConnInfo(self):
        # get if from .qmsadm, or fail
        (host, port, user, pwd) = self._getCachedConnInfo()
        if host is None or port is None or user is None or pwd is None:
            print "Missing connection information ", \
                "(hint: use -c or use .qmsadm file)"
            return (None, None, None, None)
        return (host, port, user, pwd)

    def _getCachedConnInfo(self):
        self._logger.debug("Getting cached connection info")
        config = ConfigParser.ConfigParser()
        config.read(self._dotFileName)
        s = "qmsConn"
        if not config.has_section(s):
            print "Can't find section '%s' in .qmsadm" % s
            return (None, None, None, None)
        if not config.has_option(s, "host") or \
           not config.has_option(s, "port") or \
           not config.has_option(s, "user") or \
           not config.has_option(s, "password"):
            print "Bad %s, can't find host, port, user or password" % \
                self._dotFileName
            return (None, None, None, None)
        return (config.get(s, "host"),
                config.getint(s, "port"),
                config.get(s, "user"),
                config.get(s, "password"))

    def _echoTest(self, qms):
        echostring = "QMS test string echo back. 1234567890.()''?"
        try:
            ret = qms.echo(echostring)
        except socket.error, err:
            print "Unable to connect to qms (%s)" % err
            return False
        if ret != echostring:
            print "Expected %s, got %s" % (echostring, ret)
            return False
        return True
