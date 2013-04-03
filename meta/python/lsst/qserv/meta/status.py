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
A class that defines errors (codes and their descriptions) for 
qserv metadata. Used by both the qms server and client.
"""


class Status:
    SUCCESS  = 0

    # note: error numbered 1000 - 1200 are used by mysql,
    # see mysqld_ername.h in mysql source code
    ERR_IS_INIT            = 2001
    ERR_QMS_CONNECT        = 2002
    ERR_MYSQL_CONNECT      = 2005
    ERR_MYSQL_DISCONN      = 2010
    ERR_MYSQL_ERROR        = 2015
    ERR_META_EXISTS        = 2020
    ERR_NO_META            = 2025
    ERR_DB_EXISTS          = 2030
    ERR_DB_NOT_EXISTS      = 2035
    ERR_TABLE_EXISTS       = 2040
    ERR_TABLE_NOT_EXISTS   = 2045
    ERR_NO_TABLE_IN_SCHEMA = 2050
    ERR_COL_NOT_FOUND      = 2055
    ERR_SCHEMA_FILE        = 2060
    ERR_INVALID_OPTION     = 2065
    ERR_INVALID_PART       = 2066
    ERR_INVALID_DB_NAME    = 2070
    ERR_NOT_CONNECTED      = 2075
    ERR_CANT_EXEC_SCRIPT   = 2080
    ERR_NOT_IMPLEMENTED    = 9998
    ERR_INTERNAL           = 9999

    errors = { 
        ERR_IS_INIT: "Qserv metadata already initialized.",
        ERR_QMS_CONNECT: "Unable to connect to qms.",
        ERR_MYSQL_CONNECT: "Unable to connect to mysql server.",
        ERR_MYSQL_DISCONN: ("Failed to commit transaction and "
                            "disconnect from mysql server."),
        ERR_MYSQL_ERROR: "Internal MySQL error.",
        ERR_META_EXISTS: "Metadata already installed.",
        ERR_NO_META: "No metadata found.",
        ERR_DB_EXISTS: "The database already exists.",
        ERR_DB_NOT_EXISTS: "The database does not exist.",
        ERR_TABLE_EXISTS: "The table already exists.",
        ERR_TABLE_NOT_EXISTS: "The table does not exist.",
        ERR_NO_TABLE_IN_SCHEMA: ("Can't find 'CREATE TABLE <tableName>.",
                                        "in schema file"),
        ERR_COL_NOT_FOUND: "Column not found in the table.",
        ERR_SCHEMA_FILE: ("The schema file specified in the config file"
                          " can't be access from the client."),
        ERR_INVALID_OPTION: ("Invalid option passed."),
        ERR_INVALID_PART: ("Invalid partitioning."),
        ERR_INVALID_DB_NAME: ("Invalid database name."),
        ERR_NOT_CONNECTED: ("QMS not connected to MySQL."),
        ERR_CANT_EXEC_SCRIPT: ("Can't execute script."),
        ERR_NOT_IMPLEMENTED: ("This feature is not implemented yet."),
        ERR_INTERNAL: "Internal error."
        }

def getErrMsg(errNo):
    s = Status()
    if errNo in s.errors:
        return s.errors[errNo]
    return "Undefinied qms error"

class QmsException(Exception):
    def __init__(self, errNo, extraMsg=None):
        self._errNo = errNo
        self._extraMsg = extraMsg

    def getErrMsg(self):
        msg = getErrMsg(self._errNo)
        if self._extraMsg is not None:
            msg += " (%s)" % self._extraMsg
        return msg

    def getErrNo(self):
        return self._errNo
