#
## lsst.qserv.master.proxy
## A package providing helper logic to interface with the mysql proxy.
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
    
