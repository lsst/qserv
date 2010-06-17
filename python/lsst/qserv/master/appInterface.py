# Standard
from itertools import ifilter
import time

# Package imports
import app
import proxy
import lsst.qserv.master.config as config

# Main AppInterface class
#
# We expect front-facing "normal usage" to come through this
# interface, which is intended to make a friendly wrapper around the
# functionality from the app module. 
class AppInterface:
    def __init__(self, reactor=None):
        self.tracker = app.TaskTracker()
        okname = ifilter(lambda x: "_" not in x, dir(self))
        self.publishable = filter(lambda x: hasattr(getattr(self,x), 
                                                    'func_doc'), 
                                  okname)
        self.reactor = reactor
        self.pmap = app.makePmap()
        self.actions = {} 
        # set id counter to seconds since the epoch, mod 1 year.
        self._idCounter = int(time.time() % (60*60*24*365))
        self._resultDb = config.config.get("resultdb", "db")

        pass

    def queryNow(self, q, hints):
        """Issue a query. q=querystring, h=hint list
        @return query results
        This executes the query, waits for completion, and returns results."""
        a = app.HintedQueryAction(q, hints, self.pmap)
        if not a.getIsValid():
            return "Error during query parse step." + a.getError()
        a.invoke()
        r = a.getResult()
        return app.getResultTable(r)

    def submitQuery(self, query, conditions):
        return self.submitQuery2(query, conditions)

    def submitQuery1(self,query,conditions):
        """Simplified mysqlproxy version.  returns table name."""
        a = app.HintedQueryAction(query, conditions, self.pmap)
        a.invoke()        
        r = a.getResult()
        return r

    def submitQuery2(self,query,conditions):
        """Simplified mysqlproxy version.  
        @returns result table name, lock table name, but before completion."""
        taskId = self._idCounter # RAW hazard, but this part is single-threaded
        self._idCounter += 1
        resultName = "%s.result_%d" % (self._resultDb, taskId)
        lockName = "%s.lock_%d" % (self._resultDb, taskId)
        lock = proxy.Lock(lockName)
        lock.lock()
        a = app.HintedQueryAction(query, conditions, self.pmap, resultName)
        a.invoke()
        lock.unlockAfter(a.getResult)
        return (resultName, lockName)
    
    def query(self, q, hints):
        """Issue a query, and return a taskId that can be used for tracking.
        taskId is a 16 byte string, but should be treated as an 
        opaque identifier."""
        # FIXME: Need to fix task tracker.
        #taskId = self.tracker.track("myquery", a, q)
        #stats = time.qServQueryTimer[time.qServRunningName]
        #stats["appInvokeStart"] = time.time()
        a = app.HintedQueryAction(q, hints, self.pmap)
        key = a.queryHash
        self.actions[key] = a
        if 'lsstRunning' in dir(self.reactor):
            self.reactor.callInThread(a.invoke)
        else:
            a.invoke()
        #stats["appInvokeFinish"] = time.time()
        return key

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

    def stop(self):
        "Unceremoniously stop the server."
        if self.reactor:
            self.reactor.stop()
    pass
