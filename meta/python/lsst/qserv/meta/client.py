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
# The "client" module was designed to implement a client for the
# qserv metadata server.

from __future__ import with_statement
import ConfigParser
import getpass
import logging
from optparse import OptionParser
import os
import re
import socket
import stat
import xmlrpclib

# Local package imports
from status import Status, getErrMsg

class Client(object):

    def __init__(self, host, port, user, pwd):
        defaultXmlPath = "qms"
        self._qms = self._connectToQMS(host, port, user, pwd, defaultXmlPath)

    ###########################################################################
    ##### user-facing commands
    ###########################################################################
    def installMeta(self):
        ret = self._qms.installMeta()
        if ret != Status.SUCCESS: raise Exception(getErrMsg(ret))

    def destroyMeta(self):
        ret = self._qms.destroyMeta()
        if ret != Status.SUCCESS: raise Exception(getErrMsg(ret))

    def printMeta(self):
        return self._qms.printMeta()

    def createDb(self, dbName, theOptions):
        ret = self._qms.createDb(dbName, theOptions)
        if ret != Status.SUCCESS: raise Exception(getErrMsg(ret))

    def dropDb(self, dbName):
        ret = self._qms.dropDb(dbName)
        if ret != Status.SUCCESS: raise Exception(getErrMsg(ret))

    def retrieveDbInfo(self, dbName):
        (ret, values) = self._qms.retrieveDbInfo(dbName)
        if ret != Status.SUCCESS: raise Exception(getErrMsg(ret))
        return values

    def listDbs(self):
        return self._qms.listDbs()

    def checkDbExists(self, dbName):
        return self._qms.checkDbExists(dbName)

    def createTable(self, dbName, theOptions):
        # read schema file and pass it as a string
        if not theOptions.has_key("schemaFile"):
            raise Exception("Missing param 'schemaFile'")
        schemaFileName = theOptions["schemaFile"]
        del theOptions["schemaFile"]
        if not os.access(schemaFileName, os.R_OK):
            raise Exception("Schema file '%s' can't be opened"%schemaFileName)
        tableNameFromSchema = self._extractTableName(schemaFileName)
        if "tableName" in theOptions:
            if theOptions["tableName"] != tableNameFromSchema:
                raise Exception("Table name specified through param is '%s', but table name extracted from schema file is '%s' - they have to match" % (theOptions["tableName"], tableNameFromSchema))
        else:
            # it is ok to not specify table name - it can be retrieved 
            # from schema file
            theOptions["tableName"] = tableNameFromSchema

        schemaStr = open(schemaFileName, 'r').read()
        # do it
        ret = self._qms.createTable(dbName, theOptions, schemaStr)
        if ret != Status.SUCCESS:
            raise Exception(getErrMsg(ret))

    def dropTable(self, dbName, tableName):
        ret = self._qms.dropTable(dbName, tableName)
        if ret != Status.SUCCESS: raise Exception(getErrMsg(ret))

    def retrievePartitionedTables(self, dbName):
        (retStat, tNames) = self._qms.retrievePartTables(dbName)
        if ret != Status.SUCCESS:
            raise Exception(getErrMsg(ret))
        return tNames

    def retrieveTableInfo(self, dbName, tableName):
        (retStat, values) = self._qms.retrieveTableInfo(dbName, tableName)
        if ret != Status.SUCCESS:
            raise Exception(getErrMsg(ret))
        return values

    def getInternalQmsDbName(self):
        (retStat, dbName) = self._qms.getInternalQmsDbName()
        if ret != Status.SUCCESS:
            raise Exception(getErrMsg(ret))
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
            ret = qms.echo(echostring)
        except socket.error, err:
            raise Exception("Unable to connect to qms (%s)" % err)
        if ret != echostring:
            raise Exception("Qms echo test failed (expected %s, got %s)" % \
                                (echostring, ret))
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
