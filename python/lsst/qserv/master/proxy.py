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

#
## lsst.qserv.master.proxy
# A package providing helper logic to interface with the mysql proxy.
# Code that is related to interaction with the mysqlproxy interface
# should reside here.  This includes code to manage the lock-tables
# that are used to by the qserv frontend to block the proxy while
# queries are being processed, while not blocking the Lua scripting
# layer inside the proxy.  Blocking in the Lua scripting layer
# effectively blocks all query processing in the proxy, since the Lua
# instance is single-threaded.
# 

import lsst.qserv.master.db
import time
import thread

class Lock:
    createTmpl = "CREATE TABLE IF NOT EXISTS %s (err CHAR(255), dummy FLOAT) ENGINE=MEMORY;"
    lockTmpl = "LOCK TABLES %s WRITE;"
    writeTmpl = "INSERT INTO %s VALUES ('%s', %f);"
    unlockTmpl = "UNLOCK TABLES;"

    def __init__(self, tablename):
        self._tableName = tablename
        pass
    def lock(self):
        self.db = lsst.qserv.master.db.Db()
        self.db.activate()
        self.db.applySql((Lock.createTmpl % self._tableName) 
                         + (Lock.lockTmpl % self._tableName)
                         + (Lock.writeTmpl % (self._tableName, "dummy", 
                                              time.time())))
        pass

    def addError(self, error):
        self.db.applySql(Lock.writeTmpl % (self._tableName, "ERR "+ error, 
                                           time.time()))
        pass

    def unlock(self):
        self.db.applySql(Lock.unlockTmpl)
        pass

    def unlockAfter(self, function):
        def waitAndUnlock():
            lock = self
            function()
            lock.unlock()
        threadid = thread.start_new_thread(waitAndUnlock, tuple())

    pass

def clearLocks():
    """Get rid of all the locks in the db.(UNFINISHED)"""
    # Probably need to get a regex for lock table names.
    # Might put this function in db class.
    db = lsst.qserv.master.db.Db()
    db.activate()
    db.applySql("DROP TABLES %s;" 
                % (" ".join(map(lambda t:resultDb+"."+t,
                                ["lock_asdf"]))))
    
