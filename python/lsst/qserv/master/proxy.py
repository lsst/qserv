#
## lsst.qserv.master.proxy
## A package providing helper logic to interface with the mysql proxy.
import lsst.qserv.master.db
import time
import thread

class Lock:
    createTmpl = "CREATE TABLE IF NOT EXISTS %s (dummy FLOAT) ENGINE=MEMORY;"
    lockTmpl = "LOCK TABLES %s WRITE;"
    writeTmpl = "INSERT INTO %s VALUES (%f);"
    unlockTmpl = "UNLOCK TABLES;"

    def __init__(self, tablename):
        self._tableName = tablename
        pass
    def lock(self):
        self.db = lsst.qserv.master.db.Db()
        self.db.activate()
        self.db.applySql((Lock.createTmpl % self._tableName) 
                         + (Lock.lockTmpl % self._tableName)
                         + (Lock.writeTmpl % (self._tableName, time.time())))
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
