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
This module is a wrapper around MySQLdb. It contains a set of low level basic
database utilities such as connecting to database. It caches connections, and
handles database errors. It is currently used only by the qserv metadata server,
but is could be easily turned into a generic module and used outside of metadata
code.
"""

from __future__ import with_statement
import MySQLdb as sql
import _mysql_exceptions
import logging
import MySQLdb
import os
import StringIO
import subprocess
import sys

from lsst.qserv.meta.status import Status, QmsException

class Db:
    """This class implements the wrapper around MySQLdb."""

    def __init__(self, loggerName, host, port, user, passwd, socket, dbName):
        self._logger = logging.getLogger(loggerName)
        self._conn = None
        self._isConnectedToDb = False
        self._connType = None
        self._host = host
        self._port = port
        self._user = user
        self._passwd = passwd
        self._socket = socket
        self._dbName = dbName

    def __del__(self):
        self.disconnect()

    def _checkIsConnected(self):
        return self._conn != None

    def _checkIsConnectedToDb(self):
        return self._isConnectedToDb

    def connectToMySQLServer(self):
        """Connects to MySQL Server. If socket is available, it will try to use
           it first. If fails, it will then try to use host:port."""
        if self._checkIsConnected():
            return

        try: # try connect via socket first
            self._connType = "socket"
            self._conn = sql.connect(user=self._user,
                                     passwd=self._passwd,
                                     unix_socket=self._socket)
        except MySQLdb.Error, e:
            if self._host is None or self._port is None:
                msg = "Couldn't connect to MySQL via socket "
                msg += "'%s', " % self._socket
                msg += "and host:port not set, giving up."
                self._logger.error(msg)
                raise QmsException(Status.ERR_MYSQL_CONNECT, msg)
            self._connType = "port"
            try:
                self._conn = sql.connect(user=self._user,
                                         passwd=self._passwd,
                                         host=self._host,
                                         port=self._port)
            except MySQLdb.Error, e2:
                self._connType = None
                self._conn = None
                msg = "Couldn't connect to MySQL using socket '%s' or host:port: '%s:%s'." % (self._socket, self._host,self._port)
                self._logger.error(msg)
                raise QmsException(Status.ERR_MYSQL_CONNECT, msg)

        self._logger.debug("connected to mysql (%s)" % self._connType)

    def createMetaDb(self):
        if not self._checkIsConnected():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        if self.checkMetaDbExists():
            msg = "Can't create db '%s', it exists." % self._dbName
            self._logger.error(msg)
            raise QmsException(Status.ERR_DB_EXISTS, msg)
        else:
            self.execCommand0("CREATE DATABASE %s" % self._dbName)

    def selectMetaDb(self):
        if not self._checkIsConnected():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        if self._checkIsConnectedToDb():
            return
        try:
            self._conn.select_db(self._dbName)
        except MySQLdb.Error, e:
            self._logger.debug("Failed to select db '%s'." % self._dbName)
            raise QmsException(Status.ERR_NO_META)
        self._isConnectedToDb = True
        self._logger.debug("Connected to db '%s'." % self._dbName)

    def commit(self):
        if not self._checkIsConnected():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        self._conn.commit()

    def disconnect(self):
        if self._conn == None: return
        try:
            self.commit()
            self._conn.close()
        except MySQLdb.Error, e:
            msg = "DB Error %d: %s." % \
                                   (e.args[0], e.args[1])
            self._logger.error(msg)
            raise QmsException(Status.ERR_MYSQL_DISCONN, msg)
        self._logger.debug("MySQL connection closed.")
        self._conn = None
        self._isConnectedToDb = False

    def dropMetaDb(self):
        if not self._checkIsConnected():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        if self.checkMetaDbExists():
            self.execCommand0("DROP DATABASE %s" % self._dbName)
            self._isConnectedToDb = False

    def getDbName(self):
        return self._dbName[4:]

    def checkMetaDbExists(self):
        if not self._checkIsConnected():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        if self._dbName is None:
            raise QmsException(Status.ERR_INVALID_DB_NAME)
        cmd = "SELECT COUNT(*) FROM information_schema.schemata "
        cmd += "WHERE schema_name = '%s'" % self._dbName
        count = self.execCommand1(cmd)
        return count[0] == 1

    def getServerPrefix(self):
        if not self._checkIsConnected():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        return "%s_" % self._dbName

    def createTable(self, tableName, tableSchema):
        if not self._checkIsConnectedToDb():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        self.execCommand0("CREATE TABLE %s %s" % (tableName, tableSchema))

    def checkTableExists(self, tableName):
        if not self._checkIsConnectedToDb():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        cmd = "SELECT COUNT(*) FROM information_schema.tables "
        cmd += "WHERE table_schema = '%s' AND table_name = '%s'" % \
               (self._dbName, tableName)
        count = self.execCommand1(cmd)
        return  count[0] == 1

    def printTable(self, tableName):
        if not self._checkIsConnectedToDb():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        ret = self.execCommandN("SELECT * FROM %s" % tableName)
        s = StringIO.StringIO()
        s.write(tableName)
        if len(ret) == 0:
            s.write(" is empty.\n")
        else: 
            s.write(':\n')
        for r in ret:
            print >> s, "   ", r
        return s.getvalue()

    def loadSqlScript(self, scriptPath, dbName):
        """Loads sql script into the database <dbName>."""
        self._logger.debug("loading script %s into db %s" %(scriptPath,dbName))
        if self._passwd:
            if self._connType == "port":
                cmd = 'mysql -h%s -P%s -u%s -p%s %s' % \
                (self._host, self._port, self._user, self._passwd, dbName)
            else:
                cmd = 'mysql -S%s -u%s -p%s %s' % \
                (self._socket, self._user,self._passwd, dbName)
        else:
            if self._connType == "port":
                cmd = 'mysql -h%s -P%s -u%s %s' % \
                (self._host, self._port, self._user, dbName)
            else:
                cmd = 'mysql -S%s -u%s %s' % \
                (self._socket, self._user, dbName)
        self._logger.debug("cmd is %s" % cmd)
        with file(scriptPath) as scriptFile:
            if subprocess.call(cmd.split(), stdin=scriptFile) != 0:
                msg = "Failed to execute %s < %s" % (cmd,scriptPath)
                raise QmsException(Status.ERR_CANT_EXEC_SCRIPT, msg)

    def execCommand0(self, command):
        """Executes mysql commands which return no rows"""
        self._execCommand(command, 0)

    def execCommand1(self, command):
        """Executes mysql commands which return one rows"""
        return self._execCommand(command, 1)

    def execCommandN(self, command):
        """Executes mysql commands which return multiple rows"""
        return self._execCommand(command, 'n')

    def _execCommand(self, command, nRowsRet):
        """Executes mysql commands which return any number of rows.
        Expected number of returned rows should be given in nRowSet"""
        if not self._checkIsConnected():
            raise QmsException(Status.ERR_NOT_CONNECTED)
        cursor = self._conn.cursor()
        self._logger.debug("Executing %s" % command)
        try:
            cursor.execute(command)
        except MySQLdb.Error, e:
            try:
                msg = "MySQL Error [%d]: %s." % (e.args[0], e.args[1])
                self._logger.error(msg)
                raise QmsException(Status.ERR_MYSQL_ERROR, msg)
            except IndexError:
                self._logger.error("MySQL Error: %s." % str(e))
                raise QmsException(Status.ERR_MYSQL_ERROR, str(e))
        if nRowsRet == 0:
            ret = ""
        elif nRowsRet == 1:
            ret = cursor.fetchone()
            self._logger.debug("Got: %s" % str(ret))
        else:
            ret = cursor.fetchall()
            self._logger.debug("Got: %s" % str(ret))
        cursor.close()
        return ret
