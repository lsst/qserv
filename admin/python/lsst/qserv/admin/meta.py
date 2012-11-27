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

from db import Db  # consider moving to common
from status import Status, getErrMsg

class Meta(object):
    def __init__(self, loggerName,
                 qmsHost, qmsPort, qmsUser, qmsPass,
                 qmwDb, qmwUser, qmwPass, qmwMySqlSocket):
        self._qmsHost = qmsHost
        self._qmsPort = qmsPort
        self._qmsUser = qmsUser
        self._qmsPass = qmsPass
        self._qmwDb   = "qmw_%s" % qmwDb
        self._mdb = Db(loggerName, None, None, qmwUser, qmwPass,
                       qmwMySqlSocket, self._qmwDb)

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
        ret = self._mdb.connectAndCreateDb()
        if ret != Status.SUCCESS:
            raise Exception(getErrMsg(ret))
        for t in internalTables:
            self._mdb.createTable(t[0], t[1])
        return self._mdb.disconnect()

    def destroyMeta(self):
        ret = self._mdb.connect()
        if ret != Status.SUCCESS:
            raise Exception(getErrMsg(ret))
        self._mdb.dropDb()
        return self._mdb.disconnect()

    def printMeta(self):
        ret = self._mdb.connect()
        if ret != Status.SUCCESS:
            if ret == Status.ERR_NO_META:
                return "No metadata found"
            raise Exception(getErrMsg(ret))
        s = self._mdb.printTable("Dbs")
        self._mdb.disconnect()
        return s

    def registerDb(self, dbName):
        raise Exception("registerDb not implemented")

    def unregisterDb(self, dbName):
        raise Exception("unregisterDb not implemented")

    def listDbs(self):
        raise Exception("listDbs not implemented")

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
