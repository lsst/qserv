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

    ERR_IS_INIT   = 1001
    ERR_UNDEFINED = 1002

    errors = { ERR_IS_INIT: "Qserv metadata already initialized.",
               ERR_UNDEFINED: "Undefined error."
               }

def getErrMsg(errNo):
    s = QmsStatus()
    if errNo in s.errors:
        return "qms error #%s: %s" % (errNo, s.errors[errNo])
    return "qms error: undefined"
