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
The "client" module implements client of the qserv metadata server.
It throws QmsException on error.
"""

import os
import re
import socket
import xmlrpclib

# Local package imports
from status import Status, getErrMsg, QmsException

class Client:
    def __init__(self, host, port, user, pwd):
        defaultXmlPath = "qms"
        self._qms = self._connectToQMS(host, port, user, pwd, defaultXmlPath)

    ###########################################################################
    ##### user-facing commands
    ###########################################################################
    def installMeta(self):
        status = self._qms.installMeta()
        if status != Status.SUCCESS: raise QmsException(status)

    def destroyMeta(self):
        status = self._qms.destroyMeta()
        if status != Status.SUCCESS: raise QmsException(status)

    def printMeta(self):
        (status, v) = self._qms.printMeta()
        if status != Status.SUCCESS: raise QmsException(status)
        return v

    def createDb(self, dbName, theOptions):
        status = self._qms.createDb(dbName, theOptions)
        if status != Status.SUCCESS: raise QmsException(status)

    def dropDb(self, dbName):
        status = self._qms.dropDb(dbName)
        if status != Status.SUCCESS: raise QmsException(status)

    def retrieveDbInfo(self, dbName):
        (status, values) = self._qms.retrieveDbInfo(dbName)
        if status != Status.SUCCESS: raise QmsException(status)
        return values

    def checkDbExists(self, dbName):
        (status, existInfo) = self._qms.checkDbExists(dbName)
        if status != Status.SUCCESS: raise QmsException(status)
        return existInfo

    def listDbs(self):
        (status, values) = self._qms.listDbs()
        if status != Status.SUCCESS: raise QmsException(status)
        return values

    def createTable(self, dbName, theOptions):
        print theOptions
        # read schema file and pass it as a string
        if not theOptions.has_key("schemaFile"):
            raise QmsException(Status.ERR_INVALID_OPTION, 
                               "Missing param 'schemaFile'")
        schemaFileName = theOptions["schemaFile"]
        del theOptions["schemaFile"]
        if not os.access(schemaFileName, os.R_OK):
            msg = "Schema file '%s' can't be opened" % schemaFileName
            raise QmsException(Status.ERR_INVALID_OPTION, msg)
        tableNameFromSchema = self._extractTableName(schemaFileName)
        if "tableName" in theOptions:
            if theOptions["tableName"] != tableNameFromSchema:
                msg = "Table name specified through param is '"
                msg += theOptions["tableName"]
                msg +="', but table name extracted from schema file is "
                msg += "'%s' - they have to match." % tableNameFromSchema
                raise QmsException(Status.ERR_INVALID_OPTION, msg)
        else:
            # it is ok to not specify table name - it can be retrieved 
            # from schema file
            theOptions["tableName"] = tableNameFromSchema

        schemaStr = open(schemaFileName, 'r').read()
        # do it
        status = self._qms.createTable(dbName, theOptions, schemaStr)
        if status != Status.SUCCESS: raise QmsException(status)

    def dropTable(self, dbName, tableName):
        status = self._qms.dropTable(dbName, tableName)
        if status != Status.SUCCESS: raise QmsException(status)

    def listTables(self, dbName):
        (status, tNames) = self._qms.listTables(dbName)
        if status != Status.SUCCESS: raise QmsException(status)
        return tNames

    def listPartitionedTables(self, dbName):
        (status, tNames) = self._qms.listPartTables(dbName)
        if status != Status.SUCCESS: raise QmsException(status)
        return tNames

    def retrieveTableInfo(self, dbName, tableName):
        (status, values) = self._qms.retrieveTableInfo(dbName, tableName)
        if status != Status.SUCCESS: raise QmsException(status)
        return values

    def getInternalQmsDbName(self):
        (status, dbName) = self._qms.getInternalQmsDbName()
        if status != Status.SUCCESS: raise QmsException(status)
        return dbName

    ###########################################################################
    ##### connect to QMS
    ###########################################################################
    def _connectToQMS(self, host, port, user, pwd, xmlPath):
        url = "http://%s:%d/%s" % (host, port, xmlPath)
        qms = xmlrpclib.Server(url)
        # run echo test
        echostring = "QMS test string echo back. 1234567890.()''?"
        try:
            status = qms.echo(echostring)
        except socket.error, err:
            raise QmsException(Status.ERR_QMS_CONNECT, err)
        if status != echostring:
            msg = "(expected %s, got %s)" % (echostring, status)
            raise QmsException(Status.ERR_QMS_CONNECT, msg)
        return qms

    ###########################################################################
    ##### extractTableName from schema file
    ###########################################################################
    def _extractTableName(self, fName):
        f = open(fName, 'r')
        findIt = re.compile(r'CREATE TABLE *`?\'?\"?(\w+)', re.IGNORECASE)
        theName = None
        for line in f:
            m = findIt.match(line)
            if m:
                theName = m.group(1)
                break
        f.close()
        return theName
