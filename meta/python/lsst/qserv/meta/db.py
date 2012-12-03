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
# 

from __future__ import with_statement
import MySQLdb as sql
import _mysql_exceptions
import logging
import MySQLdb
import os
import subprocess
import sys

import config
from status import Status

class Db():
    """
    Db class is a wrapper around MySQLdb for qserv metadata server. 
    It contains a set of low level basic database utilities such 
    as connecting to database. It caches connections, and handles 
    database errors.
    """
    def __init__(self, loggerName):
        self._conn = None
        self._logger = logging.getLogger(loggerName)
        self._connType = None
        c = config.config
        self._socket = c.get("qmsdb", "unix_socket")
        self._user = c.get("qmsdb", "user")
        self._passwd = c.get("qmsdb", "passwd")
        self._host = c.get("qmsdb", "host")
        self._port = c.getint("qmsdb", "port")
        self._dbName = "qms_%s" % c.get("qmsdb", "db")

    def __del__(self):
        self.disconnect()

    def _checkIsConnected(self):
        return self._conn != None

    def connect(self, createDb=False):
        """
        It connects to a server. If createDb flag is set, it will connect to
        the server, create the database, then connect to that database.
        """
        if self._checkIsConnected():
            return

        try: # Socket file first
            self._connType = "socket"
            if createDb:
                self._conn = sql.connect(user=self._user,
                                         passwd=self._passwd,
                                         unix_socket=self._socket)
            else:
                self._conn = sql.connect(user=self._user,
                                         passwd=self._passwd,
                                         unix_socket=self._socket,
                                         db=self._dbName)
        except Exception, e:
            self._connType = "port"
            try:
                if createDb:
                    self._conn = sql.connect(user=self._user,
                                             passwd=self._passwd,
                                             host=self._host,
                                             port=self._port)
                else:
                    self._conn = sql.connect(user=self._user,
                                             passwd=self._passwd,
                                             host=self._host,
                                             port=self._port,
                                             db=self._dbName)
            except Exception, e2:
                self._connType = None
                if e[1].startswith("Unknown database"):
                    return Status.ERR_NO_META
                msg1 = "Couldn't connect using file %s" % self._socket
                self._logger.error(msg1)
                print >> sys.stderr, msg1, e
                msg2 = "Couldn't connect using %s:%s" % (self._host,self._port)
                self._logger.error(msg2)
                print >> sys.stderr, msg2, e2
                self._conn = None
                return Status.ERR_MYSQL_CONNECT

        c = self._conn.cursor()
        if createDb:
            if self.checkDbExists():
                self._logger.error("Can't created db '%s', it exists." % \
                                       self._dbName)
                return Status.ERR_IS_INIT
            else:
                self.execCommand0("CREATE DATABASE %s" % self._dbName)
            self._conn.select_db(self._dbName)
        self._logger.debug("Connected to db %s" % self._dbName)
        return Status.SUCCESS

    def disconnect(self):
        if self._conn == None:
            return Status.SUCCESS
        try:
            self._conn.commit()
            self._conn.close()
        except MySQLdb.Error, e:
            self._logger.error("QmsMySQLDb::disconnect: DB Error %d: %s" % \
                                   (e.args[0], e.args[1]))
            return Status.ERR_MYSQL_DISCONN
        self._logger.debug("MySQL connection closed")
        self._conn = None
        return Status.SUCCESS

    def connectAndCreateDb(self):
        return self.connect(True)

    def dropDb(self):
        if self.checkDbExists():
            self.execCommand0("DROP DATABASE %s" % self._dbName)

    def getDbName(self):
        return self._dbName[4:]

    def checkDbExists(self):
        if self._dbName is None:
            raise RuntimeError("Invalid dbName")
        cmd = "SELECT COUNT(*) FROM information_schema.schemata "
        cmd += "WHERE schema_name = '%s'" % self._dbName
        count = self.execCommand1(cmd)
        if count[0] == 1:
            return True
        return False

    def getServerPrefix(self):
        if not self._checkIsConnected():
            raise RuntimeError("Not connected")
        return "%s_" % self._dbName

    def createTable(self, tableName, tableSchema):
        self.execCommand0("CREATE TABLE %s %s" % (tableName, tableSchema))

    def checkTableExists(self, tableName):
        if not self._checkIsConnected():
            raise RuntimeError("Not connected")
        cmd = "SELECT COUNT(*) FROM information_schema.tables "
        cmd += "WHERE table_schema = '%s' AND table_name = '%s'" % \
               (self._dbName, tableName)
        count = self.execCommand1(cmd)
        return  count[0] == 1

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
                (self._host, self._port, self._user, _dbName)
            else:
                cmd = 'mysql -S%s -u%s %s' % \
                (self._socket, self._user, dbName)
        self._logger.debug("cmd is %s" % cmd)
        with file(scriptPath) as scriptFile:
            if subprocess.call(cmd.split(), stdin=scriptFile) != 0:
                raise RuntimeError("Failed to execute %s < %s" % \
                                       (cmd,scriptPath))

    def execCommand0(self, command):
        """Executes mysql commands which return no rows"""
        return self._execCommand(command, 0)

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
            self.connect()
        cursor = self._conn.cursor()
        self._logger.debug("Executing %s" % command)
        try:
            cursor.execute(command)
        except MySQLdb.Error, e:
            try:
                self._logger.error("MySQL Error [%d]: %s" % \
                                       (e.args[0], e.args[1]))
                return None # fixme: throw exception, catch higher up!
            except IndexError:
                self._logger.error("MySQL Error: %s" % str(e))
                return None # fixme: throw exception, catch higher up!
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
