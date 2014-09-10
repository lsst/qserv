#
# LSST Data Management System
# Copyright 2008-2014 LSST Corporation.
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
## lsst.qserv.czar.proxy
# A package providing helper logic to interface with the mysql proxy.
# Code that is related to interaction with the mysqlproxy interface
# should reside here.  This includes code to manage the lock-tables
# that are used to by the qserv frontend to block the proxy while
# queries are being processed, while not blocking the Lua scripting
# layer inside the proxy.  Blocking in the Lua scripting layer
# effectively blocks all query processing in the proxy, since the Lua
# instance is single-threaded.
#

import lsst.qserv.czar.db
import time
import thread

from lsst.qserv.czar import queryMsgGetCount, queryMsgGetMsg, UserQuery_discard


class Lock:
    """The Lock class manages the table locking mechanism used to
    allow the mysql-proxy to effectively wait for a query's completion
    by the czar without blocking. This is achieved by including a
    table-lock acquired by the czar during query submission. The
    mysql-proxy is advised of the name of the locked table, and is
    instructed to query its associated results-hosting mysqld after
    acquiring a lock on the table. mysql-proxy can service other
    requests when it is waiting on mysql queries that it knowingly
    initiates in its C-layer, whereas it treats its Lua plugin code as a
    single-threaded black-box."""
    createTmpl = "CREATE TABLE IF NOT EXISTS %s (chunkId INT, code SMALLINT, message CHAR(255), timeStamp FLOAT) ENGINE=MEMORY;"
    lockTmpl = "LOCK TABLES %s WRITE;"
    writeTmpl = "INSERT INTO {0} VALUES (%s, %s, %s, %s);"
    unlockTmpl = "UNLOCK TABLES;"

    def __init__(self, tablename):
        self._tableName = tablename
        self._sessionId = None
        pass

    def lock(self):
        self.db = lsst.qserv.czar.db.Db()
        if not self.db.check(): # Can't lock.
            return False
        self.db.applySql((Lock.createTmpl % self._tableName)
                         + (Lock.lockTmpl % self._tableName))
        return True

    def setSessionId(self, sessionId):
        self._sessionId = sessionId
        pass

    def unlock(self):
        self._saveQueryMessages()
        self.db.applySql(Lock.unlockTmpl)
        # We should not discard session here, but in the current
        # design the QueryMsg is contained in AsyncQueryMgr, so
        # cannot discard until now.
        if self._sessionId:
            UserQuery_discard(self._sessionId)
        pass

    def unlockAfter(self, threadCreateFunc, function):
        def waitAndUnlock():
            lock = self
            function()
            lock.unlock()
        threadid = thread.start_new_thread(waitAndUnlock, tuple())

    def _saveQueryMessages(self):
        if not self._sessionId: # No object to read.
            return
        msgCount = queryMsgGetCount(self._sessionId)
        for i in range(msgCount):
            msg, chunkId, code, timestamp = queryMsgGetMsg(self._sessionId, i)
            self.db.applySql(Lock.writeTmpl.format(self._tableName), (chunkId, code, msg, timestamp))


def clearLocks():
    """Get rid of all the locks in the db.(UNFINISHED)"""
    # Probably need to get a regex for lock table names.
    # Might put this function in db class.
    db = lsst.qserv.czar.db.Db()
    db.activate()
    db.applySql("DROP TABLES %s;"
                % (" ".join(map(lambda t:resultDb+"."+t,
                                ["lock_asdf"]))))

