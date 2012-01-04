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

# metadata module for lsst.qserv.master
#
# The metadata module contains functions, constants, and data related
# strictly to qserv's metadata.  This includes anything in the qserv
# metadata database and name mangling code.

# Pkg imports
import config

class Runtime:
    def __init__(self):
        self.metaDbName = config.config.get("mgmtdb", "db")

# Module data
_myRuntime = None

# External interface
def getIndexNameForTable(tableName):
    global _myRuntime
    if not _myRuntime: _myRuntime = Runtime()
    return _myRuntime.metaDbName + "." + tableName.replace(".","__")

def getMetaDbName():
    global _myRuntime
    if not _myRuntime: _myRuntime = Runtime()
    return _myRuntime.metaDbName 

