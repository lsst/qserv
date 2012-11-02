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

# Standard
from itertools import ifilter
import logging

# Package imports
import qmsImpl
import lsst.qserv.master.config

# Interface for qserv metadata server
class QmsInterface:
    def __init__(self):
        self._loggerName = "qmsLogger"
        self._initLogging()

        okname = ifilter(lambda x: "_" not in x, dir(self))
        self.publishable = filter(lambda x: hasattr(getattr(self,x), 
                                                    'func_doc'), 
                                  okname)

    def installMeta(self):
        """Initializes qserv metadata. It creates persistent structures,
        (it should be called only once)."""
        return qmsImpl.installMeta(self._loggerName)

    def destroyMeta(self):
        """Permanently destroyp qserv metadata."""
        return qmsImpl.destroyMeta(self._loggerName)

    def printMeta(self):
        """Returns string that contains all metadata."""
        return qmsImpl.printMeta(self._loggerName)

    def createDb(self, dbName, crDbOptions):
        """Creates metadata about new database to be managed by qserv."""
        return qmsImpl.createDb(self._loggerName, dbName, crDbOptions)

    def dropDb(self, dbName):
        """Removes metadata about a database managed by qserv."""
        return qmsImpl.dropDb(self._loggerName, dbName)

    def retrieveDbInfo(self, dbName):
        """Retrieves information about a database managed by qserv."""
        return qmsImpl.retrieveDbInfo(self._loggerName, dbName)

    def listDbs(self):
        """Returns string that contains list of databases managed by qserv."""
        return qmsImpl.listDbs(self._loggerName)

    def checkDbExists(self, dbName):
        """Checks if db <dbName> exists, returns 0 (no) or 1 (yes)."""
        return qmsImpl.checkDbExists(self._loggerName, dbName)

    def createTable(self, dbName, crTbOptions, schemaStr):
        """Creates metadata about new table from qserv-managed database."""
        return qmsImpl.createTable(self._loggerName, dbName, 
                                   crTbOptions, schemaStr)

    def dropTable(self, dbName, tableName):
        """Removes metadata about a table."""
        return qmsImpl.dropTable(self._loggerName, dbName, tableName)

    def retrievePartTables(self, dbName):
        """Retrieves list of partitioned tables for a given database."""
        return qmsImpl.retrievePartTables(self._loggerName, dbName)

    def retrieveTableInfo(self, dbName, tableName):
        """Retrieves information about a table."""
        return qmsImpl.retrieveTableInfo(self._loggerName, dbName, tableName)

    def help(self):
        """A brief help message showing available commands"""
        r = "" ## self._handyHeader()
        r += "\n<pre>Available qms commands:\n"
        sorted =  map(lambda x: (x, getattr(self, x)), self.publishable)
        sorted.sort()
        for (k,v) in sorted:
            r += "%-20s : %s\n" %(k, v.func_doc)
        r += "</pre>\n"
        return r

    def _initLogging(self):
        config = lsst.qserv.master.config.config
        outFile = config.get("logging", "outFile")
        levelName = config.get("logging", "level")
        if levelName is None:
            level = logging.ERROR # default
        else:
            ll = {"debug":logging.DEBUG,
                  "info":logging.INFO,
                  "warning":logging.WARNING,
                  "error":logging.ERROR,
                  "critical":logging.CRITICAL}
            level = ll[levelName]
        self.logger = logging.getLogger(self._loggerName)
        hdlr = logging.FileHandler(outFile)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr) 
        self.logger.setLevel(level)
