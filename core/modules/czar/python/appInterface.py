#
# LSST Data Management System
# Copyright 2009-2014 LSST Corporation.
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
import string
import time
import traceback

# Package imports
import logger
import app
import proxy
import lsst.qserv.czar.config as config

## Helpers:
def parseKillId(killQuery):
    """From a "KILL QUERY 1234" string, return 1234"""
    tokens = map(string.strip, killQuery.split())
    try:
        # Could add validation code here, but shouldn't be necessary.
        return int(tokens[2])
    except:
        pass
    try:
        return int(tokens[1])
    except:
        return None

# Main AppInterface class
#
# We expect front-facing "normal usage" to come through this
# interface, which is intended to make a friendly wrapper around the
# functionality from the app module.
#
# AppInterface instances can underlie an HTTP or XML-RPC server (via
# server.py), or be used directly by test programs or
# development/administrative code.
#
# Ideally, AppInterface objects can be used from standalone Python
# programs, facilitating testing and usage without bringing up a qserv
# czar daemon. It is unclear whether this still works.
class AppInterface:
    """An implemented interface to the Qserv czar application logic. """
    def __init__(self, threadFunc=None):
        self._threadFunc = threadFunc
        self.tracker = app.TaskTracker()
        # set id counter to milliseconds since the epoch, mod 1 year.
        self._idCounter = int((time.time() % (60*60*24*365)) * 1000)
        logger.dbg("_idCounter", self._idCounter)
        self._resultDb = config.config.get("resultdb", "db")
        self._clientToServerId = {}
        pass

    def _maybeCallWithThread(self, function):
        """If we have a designated thread func, use it to execute function in a new thread.  Otherwise,
        execute function inline.
        @returns bool indicating whether function was called in a new thread."""
        if self._threadFunc:
            self._threadFunc(function)
            return True
        else:
            function()
            return False
        pass

    def submitQuery(self, query, conditions):
        """Issue a query.  Params: query, conditions.
        @returns (result table name, lock/message table name, error)

        Does not block for query completion."""
        # FIXME: Need to fix task tracker, and return taskID for tracking

        # Short-circuit the standard proxy/client queries.
        quickResult = app.computeShortCircuitQuery(query, conditions)
        if quickResult: return quickResult
        taskId = self._idCounter # RAW hazard, but this part is single-threaded
        self._idCounter += 1

        logger.dbg("taskId", taskId)

        # resultName should be shorter than 20 characters so it is always
        # shorter than intermediate table names.
        # This allows in-place name replacement optimization while merging.
        resultName = "%s.result_%d" % (self._resultDb, taskId)
        lockName = "%s.message_%d" % (self._resultDb, taskId)
        lock = proxy.Lock(lockName)
        if not lock.lock():
            return ("error", "error",
                    "error locking result, check qserv/db config.")
        context = app.Context(conditions)
        a = app.InbandQueryAction(query, context,
                                  lock.setSessionId, resultName)
        if a.getIsValid():
            self._maybeCallWithThread(a.invoke)
            lock.unlockAfter(self._threadFunc, a.getResult)
        else:
            lock.unlock()
            return ("error","error",a.getError())

        # Remember client context for kill-operations
        proxyName = conditions["client_dst_name"]
        proxyThread = conditions["server_thread_id"]
        self._clientToServerId[(proxyThread, proxyName)] = a.sessionId

        return (resultName, lockName, "")

    def killQuery(self, sessionId):
        """Process a kill query command (experimental).
        @param sessionId : InbandQueryAction session id ."""
        a = app.KillQueryAction(sessionId)
        self._maybeCallWithThread(a.invoke)
        return "Attempt query kill: " + str(sessionId)

    def killQueryUgly(self, killStr, clientId):
        """Process a kill query command (experimental).
        @param killStr : (client)proxy-provided "KILL QUERY ..." string
        @param clientId : client_dst_name from proxy"""
        #lookup sessionId using thread_id (parsed from killStr) and clientId
        try:
            print "killStr, clientId", killStr, clientId
            clientThreadId = parseKillId(killStr)
            print "clientThreadId = ", clientThreadId
            print "mapping:", self._clientToServerId
            sessionId = self._clientToServerId[(clientThreadId, clientId)]
            self.killQuery(sessionId)
        except Exception, e:
            traceStr = traceback.format_exc()
            info = "Error parsing or finding task to kill: %s, %s" % (
                killStr, clientId)
            logger.wrn("Error killing query: " + info + "\n" + traceStr)
            return info

        return True

    ### Deprecated/unused: the Lua interface is single-threaded and doesn't
    ### tolerate blocking well, so we never want it to wait for a query to
    ### complete in this manner. We still want to support this (or
    ### equivalent) in the non proxy interface.
    def joinQuery(self, taskId):
        """Wait for a query to finish, then return its results. Params: taskId."""
        if str(taskId) not in self.actions:
            return None
        a = self.actions[taskId]
        r = a.getResult()
        return r

    def check(self, taskId):
        "Check status of query or task. Params: taskId."
        a = app.CheckAction(self.tracker, taskId)
        a.invoke()
        return a.results

    def results(self, taskId):
        "Get results location for a query or task. Params: taskId."
        return app.results(self.tracker, taskId)

    def resultTableString(self, table):
        """Get contents of a result table."""
        return app.getResultTable(table)

    def cancelEverything(self):
        """Try to kill the threads running underneath, e.g. xrootd or otherwise
        children of app.  No Params."""
        app.stopAll()
