# 
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

# qmsErrors.py : Defines error codes and their descriptions.
# Used by both the qms server and client.

class QmsStatus():
    SUCCESS  = 0

    # note: error numbered 1000 - 1200 are used by mysql,
    # see mysqld_ername.h in mysql source code
    ERR_IS_INIT          = 2001
    ERR_MYSQL_CONNECT    = 2002
    ERR_MYSQL_DISCONN    = 2003
    ERR_NO_META          = 2004
    ERR_DB_EXISTS        = 2005
    ERR_DB_NOT_EXISTS    = 2006
    ERR_TABLE_EXISTS     = 2007
    ERR_TABLE_NOT_EXISTS = 2008
    ERR_COL_NOT_FOUND    = 2009
    ERR_SCHEMA_FILE      = 2010
    ERR_INTERNAL         = 9999

    errors = { ERR_IS_INIT: "Qserv metadata already initialized.",
               ERR_MYSQL_CONNECT: "Unable to connect to mysql server.",
               ERR_NO_META: "No metadata found.",
               ERR_DB_EXISTS: "The database already exists.",
               ERR_DB_NOT_EXISTS: "The database does not exist.",
               ERR_TABLE_EXISTS: "The table already exists.",
               ERR_TABLE_NOT_EXISTS: "The table does not exist.",
               ERR_COL_NOT_FOUND: "Column not found in the table.",
               ERR_SCHEMA_FILE: ("The schema file specified in the config file "
                                 "can't be access from the client."),
               ERR_MYSQL_DISCONN: ("Failed to commit transaction and "
                                   "disconnect from mysql server."),
               ERR_INTERNAL: "Internal error."
               }

def getErrMsg(errNo):
    s = QmsStatus()
    if errNo in s.errors:
        return "qms error #%s: %s" % (errNo, s.errors[errNo])
    return "qms error: undefined"
