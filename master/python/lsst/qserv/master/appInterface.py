#
# LSST Data Management System
# Copyright 2009-2013 LSST Corporation.
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
import time

# Package imports
import app
import proxy
import lsst.qserv.master.config as config
import lsst.qserv.master.spatial as spatial

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
# master daemon. It is unclear whether this still works.
class AppInterface:
    """An implemented interface to the Qserv master application logic. """
    def __init__(self, reactor=None):
        self.tracker = app.TaskTracker()
        okname = ifilter(lambda x: "_" not in x, dir(self))
        self.publishable = filter(lambda x: hasattr(getattr(self,x),
                                                    'func_doc'),
                                  okname)
        self.reactor = reactor
        self.actions = {}
        # set id counter to seconds since the epoch, mod 1 year.
        self._idCounter = int(time.time() % (60*60*24*365))
        self._resultDb = config.config.get("resultdb", "db")
        pass

    def _callWithThread(self, function):
        if 'lsstRunning' in dir(self.reactor):
            self.reactor.callInThread(function)
        else:
            function()
        pass

    def _getThreadFunc(self):
        if 'lsstRunning' in dir(self.reactor):
            return self.reactor.callInThread
        else:
            return lambda f: thread.start_new_thread(f, tuple())
        pass

    def initMetadataCache(self):
        """Initializes default session for metadata cache.
           Throws QmsExeption on failure."""
        app.MetadataCacheIface().getDefaultSessionId()

    def queryNow(self, q, hints):
        """Issue a query. q=querystring, h=hint list
        @return query result table name
        This executes the query, waits for completion, and returns results.
        (broken)"""
        raise StandardError("Unimplemented")

    def submitQuery(self, query, conditions):
        return self.submitQueryWithLock(query, conditions)

    def submitQueryPlain(self, query, conditions):
        """Simplified mysqlproxy version.  returns table name.
        (broken)"""
        raise StandardError("Unimplemented")

    def submitQueryWithLock(self, query, conditions):
        """Simplified mysqlproxy version.
        @returns result table name, lock table name, but before completion."""
        proxyName = conditions["client_dst_name"]
        proxyThread = conditions["server_thread_id"]
        ## FIXME: Use the proxyId to match up

        # Short-circuit the standard proxy/client queries.
        quickResult = app.computeShortCircuitQuery(query, conditions)
        if quickResult: return quickResult
        taskId = self._idCounter # RAW hazard, but this part is single-threaded
        self._idCounter += 1

        # resultName should be shorter than 20 characters so it is always
        # shorter than intermediate table names.
        # This allows in-place name replacement optimization while merging.
        resultName = "%s.result_%d" % (self._resultDb, taskId)
        lockName = "%s.lock_%d" % (self._resultDb, taskId)
        lock = proxy.Lock(lockName)
        if not lock.lock():
            return ("error", "error",
                    "error locking result, check qserv/db config.")
        a = app.InbandQueryAction(query, conditions,
                                  lambda e: lock.addError(e), resultName)
        if a.getIsValid():
            self._callWithThread(a.invoke)
            lock.unlockAfter(self._getThreadFunc(), a.getResult)
        else:
            lock.unlock()
            return ("error","error",a.getError())
        return (resultName, lockName, "")

    def query(self, q, hints):
        """Issue a query, and return a taskId that can be used for tracking.
        taskId is a 16 byte string, but should be treated as an
        opaque identifier.
        (broken)
        """
        # FIXME: Need to fix task tracker.
        #taskId = self.tracker.track("myquery", a, q)
        #stats = time.qServQueryTimer[time.qServRunningName]
        #stats["appInvokeStart"] = time.time()
        raise StandardError("unimplemented")
        a = app.HintedQueryAction(q, hints, self.pmap)
        key = a.queryHash
        self.actions[key] = a
        self._callWithThread(a.invoke)
        #stats["appInvokeFinish"] = time.time()
        return key

    def killQuery(self, query, taskId):
        """Process a kill query command (experimental)"""
        a = app.KillQueryAction(query)
        self._callWithThread(a.invoke)
        return "Attempt: " + query

    def joinQuery(self, taskId):
        """Wait for a query to finish, then return its results."""
        if str(taskId) not in self.actions:
            return None
        a = self.actions[taskId]
        r = a.getResult()
        return r

    def help(self):
        """A brief help message showing available commands"""
        r = "" ## self._handyHeader()
        r += "\n<pre>available commands:\n"
        sorted =  map(lambda x: (x, getattr(self, x)), self.publishable)
        sorted.sort()
        for (k,v) in sorted:
            r += "%-20s : %s\n" %(k, v.func_doc)
        r += "</pre>\n"
        return r

    def check(self, taskId):
        "Check status of query or task. Params: "
        a = app.CheckAction(self.tracker, taskId)
        a.invoke()
        return a.results

    def results(self, taskId):
        "Get results location for a query or task. Params: taskId"
        return app.results(self.tracker, taskId)

    def resultTableString(self, table):
        """Get contents of a result table."""
        return app.getResultTable(table)

    def reset(self):
        "Resets/restarts server uncleanly. No params."
        if self.reactor:
            args = sys.argv #take the original arguments
            args.insert(0, sys.executable) # add python
            os.execv(sys.executable, args) # replace self with new python.
            print "Reset failed:",sys.executable, str(args)
            return # This will not return.  os.execv should overwrite us.
        else:
            print "<Not resetting: no reactor>"
        pass

    def stop(self):
        "Unceremoniously stop the server."
        if self.reactor:
            self.reactor.stop()
        pass
